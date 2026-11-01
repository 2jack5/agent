// M4 API 入口：接入 MCP 和 A2A，保留已有 Kafka、RAG 和长期记忆能力。
package main

import (
	"context"
	"fmt"
	"my_agent/internal/a2a"
	"my_agent/internal/worker"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/minio/minio-go/v7"

	"my_agent/internal/api"
	"my_agent/internal/config"
	"my_agent/internal/infra"
	"my_agent/internal/llm"
	"my_agent/internal/memory/long_term"
	"my_agent/internal/observability"
	"my_agent/internal/observability/logger"
	"my_agent/internal/rag/engine"
	"my_agent/internal/repo"
	"my_agent/internal/security"
	"my_agent/internal/service"
	"my_agent/internal/tool"
	"my_agent/internal/vector"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "server exited: %v\n", err) // os.Stderr可以区分错误
		os.Exit(1)
	}
}

func run() error {
	// ① 配置、日志和生命周期。
	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger.Init(cfg.Log.Level, cfg.Log.Format)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ② 连接存储；表结构仍由独立迁移命令创建。
	db, err := infra.NewPG(ctx, &cfg.PG)
	if err != nil {
		return fmt.Errorf("pg: %w", err)
	}
	rdb, err := infra.NewRedis(ctx, &cfg.Redis)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	mcli, err := infra.NewMilvus(ctx, &cfg.Milvus)
	if err != nil {
		return fmt.Errorf("milvus: %w", err)
	}
	defer mcli.Close()
	store := vector.NewMilvusStore(mcli)
	mio, err := infra.NewMinIO(ctx, &cfg.MinIO)
	if err != nil {
		return fmt.Errorf("minio: %w", err)
	}
	if err := mio.MakeBucket(ctx, cfg.MinIO.Bucket, minio.MakeBucketOptions{}); err != nil {
		exists, checkErr := mio.BucketExists(ctx, cfg.MinIO.Bucket)
		if checkErr != nil || !exists {
			return fmt.Errorf("minio bucket: %w", err)
		}
	}

	// ③ 共享 Repo、JWT 和认证业务依赖。
	observability.InitMetrics()
	encryptor, err := security.NewEncryptor(cfg.SecretKey)
	if err != nil {
		return fmt.Errorf("encryptor: %w", err)
	}
	gateway := llm.NewGateway(&cfg.LLM)
	repos := repo.New(db)
	reg := tool.NewRegistry()
	tool.RegisterSimple(reg)
	tool.RegisterWeb(reg, tool.SearchConfig{SearxngURL: cfg.Search.SearxngURL})
	tool.RegisterFiles(reg, tool.FileOpsConfig{BaseDir: "./data/files", MaxSize: 10 << 20})
	tool.RegisterDBQuery(reg, db)
	if cfg.Sandbox.Enabled {
		// Docker 沙箱失败不阻断启动：code_exec 不可用而已（降级思维）
		if sandbox, err := tool.NewSandbox(tool.SandboxConfig{
			Image: cfg.Sandbox.Image, Memory: cfg.Sandbox.Memory,
			CPUs: cfg.Sandbox.CPUs, Timeout: cfg.Sandbox.Timeout, PoolSize: cfg.Sandbox.PoolSize,
		}); err != nil {
			observability.LogWarn("sandbox init failed, code_exec disabled", "err", err.Error())
		} else {
			tool.RegisterCodeExec(reg, sandbox, db)
		}
	}
	executor := tool.NewExecutor(repos.Tool, reg, tool.ExecutorConfig{DefaultTimeout: 30 * time.Second})
	jwtMgr := security.NewJWTManager(cfg.JWT.Secret, cfg.JWT.AccessTTL, cfg.JWT.RefreshTTL)

	// 创建 Kafka 消息发送客户端。
	taskWriter := infra.NewKafkaWriter(cfg.Kafka.Brokers)
	defer taskWriter.Close()

	// 创建任务发布者。
	publisher := worker.NewPublisher(taskWriter, &cfg.Kafka)
	svcs, err := service.New(service.Deps{
		Cfg: cfg, Repos: repos, JWT: jwtMgr, RDB: rdb,
		Gateway: gateway, Encryptor: encryptor, Executor: executor, Registry: reg,
		Streams: publisher, Vector: store,
	})
	if err != nil {
		return fmt.Errorf("services: %w", err)
	}
	// MCP 远程工具必须注册到 Executor 共用的注册表中。
	svcs.MCP.SetRegistry(reg)

	// RAG 模型回调必须在 Services 创建之后注入。
	ragEngine := engine.New(gateway, store, repos.KB, &cfg.RAG, cfg.Milvus.Dim)
	ragEngine.SetModels(svcs.DefaultEmbedding, svcs.DefaultRerank)
	svcs.SetRAGEngine(ragEngine)
	svcs.KB.SetMinIO(mio, cfg.MinIO.Bucket)
	memoryMgr := long_term.NewManager(gateway, svcs.DefaultChat, svcs.DefaultEmbedding, repos.Memory, store)
	svcs.Chat.SetLongTermMemory(memoryMgr)
	svcs.Memory.SetManager(memoryMgr)
	tool.RegisterKBSearch(reg, kbSearchFunc(svcs, ragEngine))

	// ④ 中间件和认证路由。写超时保留为 0，供后续 SSE 使用。
	h := server.New(
		server.WithHostPorts(":"+cfg.HTTP.Port),
		server.WithReadTimeout(30*time.Second),
		server.WithWriteTimeout(0),
		server.WithIdleTimeout(120*time.Second),
	)
	// 全局注册也覆盖 OPTIONS 和未匹配路由，使预检及错误响应带上 CORS 头。
	h.Use(api.Recovery(), api.CORS(), api.AccessLog())
	root := h.Group("/")
	root.GET("/healthz", api.Healthz())
	root.GET("/metrics", api.Metrics())
	api.Register(root, api.New(svcs), jwtMgr)

	// ⑤ A2A 对外端点（独立 net/http 端口，回调标准对话链路）。
	if cfg.A2A.Enabled {
		startA2AServer(cfg, svcs)
	}

	// ⑥ 注册退出信号后开始监听，沿用原项目的 Spin/Shutdown 流程。
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)
	observability.LogInfo("my_agent server started", "port", cfg.HTTP.Port, "env", cfg.Env)
	go h.Spin()
	<-quit

	observability.LogInfo("shutting down...")
	shutdownCtx, stop := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer stop()
	cancel()
	h.Shutdown(shutdownCtx)
	return nil
}

// startA2AServer 启动 A2A 对外端点。
func startA2AServer(cfg *config.Config, svcs *service.Services) {
	card := &a2a.AgentCard{
		Name:        cfg.A2A.Name,
		Description: cfg.A2A.Desc,
		URL:         fmt.Sprintf("http://127.0.0.1:%s/a2a", cfg.A2A.Port),
		Version:     "2.0.0",
		Protocol:    "0.2.9",
		Capabilities: struct {
			Streaming bool `json:"streaming"`
		}{Streaming: false},
		Skills: []a2a.AgentSkill{{Name: "chat", Description: "通用对话与工具调用"}},
	}
	handler := func(ctx context.Context, text string) (string, error) {
		return svcs.A2A.InvokeLocal(ctx, 0, text)
	}
	mux := http.NewServeMux()
	a2a.NewServer(card, handler).RegisterRoutes(mux)
	go func() {
		observability.LogInfo("a2a server listening", "port", cfg.A2A.Port)
		if err := http.ListenAndServe(":"+cfg.A2A.Port, mux); err != nil {
			observability.LogError("a2a server exited", "err", err.Error())
		}
	}()
}

// kbSearchFunc 复用同一个 RAG 引擎，为工具执行器提供知识库检索。
func kbSearchFunc(svcs *service.Services, eng *engine.Engine) tool.KBSearchFunc {
	return func(ctx context.Context, kbID int64, query string, topK int) (string, error) {
		kb, err := svcs.Repos.KB.GetKB(ctx, kbID)
		if err != nil {
			return "", err
		}
		refs, err := eng.Retrieve(ctx, kb, query)
		if err != nil {
			return "", err
		}
		var out string
		for _, r := range refs {
			out += fmt.Sprintf("[%s p%d] %s\n", r.Filename, r.Page, r.Content)
		}
		return out, nil
	}
}
