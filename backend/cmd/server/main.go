// NexusAgent API 服务入口：全项目依赖装配（main 是唯一知道所有具体实现的地方）。
//
// 装配顺序（按当前代码的依赖关系）：
//
//	配置 → 日志 → 追踪与指标 → 基础设施(PG/Redis/Milvus/MinIO)与向量库
//	  → 安全组件 → LLM 网关 → 数据访问与工具系统 → Kafka 任务发布者 → service
//	  → RAG 引擎 → 工作流引擎 → 对象存储与长期记忆 → kb_search 工具
//	  → Hertz 路由 → A2A 服务端 → 优雅关停
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
	"my_agent/internal/workflow"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "server exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// ===== ① 配置与日志 =====
	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger.Init(cfg.Log.Level, cfg.Log.Format)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ===== ② 可观测性 =====
	if cfg.Observability.OtelEnabled && cfg.Observability.OtelEndpoint != "" {
		shutdown, err := observability.InitTracer(ctx, cfg.Observability.OtelEndpoint)
		if err != nil {
			observability.LogWarn("otel init failed, tracing disabled", "err", err.Error())
		} else {
			defer shutdown(context.Background())
		}
	}
	observability.InitMetrics()

	// ===== ③ 基础设施 =====
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
	// 向量库复用已连接的 Milvus 客户端。
	store := vector.NewMilvusStore(mcli)
	mio, err := infra.NewMinIO(ctx, &cfg.MinIO)
	if err != nil {
		return fmt.Errorf("minio: %w", err)
	}
	// 对象桶不存在则创建（已存在时忽略）。
	if err := mio.MakeBucket(ctx, cfg.MinIO.Bucket, minio.MakeBucketOptions{}); err != nil {
		exists, checkErr := mio.BucketExists(ctx, cfg.MinIO.Bucket)
		if checkErr != nil || !exists {
			return fmt.Errorf("minio bucket: %w", err)
		}
	}

	// ===== ④ 安全组件 =====
	encryptor, err := security.NewEncryptor(cfg.SecretKey)
	if err != nil {
		return fmt.Errorf("encryptor: %w", err)
	}

	// ===== ⑤ LLM 网关（向量库已在基础设施部分初始化） =====
	gateway := llm.NewGateway(&cfg.LLM)

	// ===== ⑥ 数据访问与工具系统 =====
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
	// JWT 管理器与加密组件一并注入 service 层。
	jwtMgr := security.NewJWTManager(cfg.JWT.Secret, cfg.JWT.AccessTTL, cfg.JWT.RefreshTTL)

	// ===== ⑦ service 层 =====
	// Kafka 任务发布者供业务层投递异步任务，连接随服务退出关闭。
	taskWriter := infra.NewKafkaWriter(cfg.Kafka.Brokers)
	defer taskWriter.Close()

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

	// ===== ⑧ RAG 引擎（注入实时解析函数，改配置无需重启） =====
	ragEngine := engine.New(gateway, store, repos.KB, &cfg.RAG, cfg.Milvus.Dim)
	ragEngine.SetModels(svcs.DefaultEmbedding, svcs.DefaultRerank)
	svcs.SetRAGEngine(ragEngine)

	// ===== ⑨ 工作流引擎（复用既有组件，不重复造轮子） =====
	svcs.Workflow.SetEngine(workflow.New(workflow.Deps{
		Gateway:      gateway,
		Tools:        executor,
		RAG:          ragEngine,
		ResolveModel: svcs.ResolveModelByAlias,
		GetKB:        svcs.Workflow.GetWorkflowLookup(),
		SubRunner:    svcs.Workflow.SubflowRunner(),
	}))

	// 注入知识库上传文件所需的对象存储。
	svcs.KB.SetMinIO(mio, cfg.MinIO.Bucket)

	// ===== ⑩ 长期记忆（抽取用低成本 chat 模型，实时解析） =====
	memoryMgr := long_term.NewManager(gateway, svcs.DefaultChat, svcs.DefaultEmbedding, repos.Memory, store)
	svcs.Chat.SetLongTermMemory(memoryMgr)
	svcs.Memory.SetManager(memoryMgr)

	// ===== ⑪ kb_search 工具（依赖 RAG 引擎，最后注册） =====
	tool.RegisterKBSearch(reg, kbSearchFunc(svcs, ragEngine))

	// ===== ⑫ Hertz 服务 =====
	h := server.New(
		server.WithHostPorts(":"+cfg.HTTP.Port),
		server.WithReadTimeout(30*time.Second),
		server.WithWriteTimeout(0), // SSE 长连接不超时
		server.WithIdleTimeout(120*time.Second),
	)
	// 全局注册也覆盖 OPTIONS 和未匹配路由，使预检及错误响应带上 CORS 头。
	h.Use(api.Recovery(), api.CORS(), api.AccessLog())
	root := h.Group("/")
	root.GET("/healthz", api.Healthz())
	root.GET("/metrics", api.Metrics())
	api.Register(root, api.New(svcs), jwtMgr)

	// ===== ⑬ A2A 对外端点（独立 net/http 端口，回调标准对话链路） =====
	if cfg.A2A.Enabled {
		startA2AServer(cfg, svcs)
	}

	// ===== ⑭ 优雅关停 =====
	// 先注册退出信号，再启动监听，主 goroutine 负责等待退出。
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)
	observability.LogInfo("my_agent server started", "port", cfg.HTTP.Port, "env", cfg.Env)
	// Spin 必须调用：server.New 只做装配不监听端口，Spin 才真正 bind :port。
	// 放在 goroutine 里，主 goroutine 收到信号后调用 Shutdown。
	go h.Spin()
	<-quit

	observability.LogInfo("shutting down...")
	shutdownCtx, stop := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer stop()
	cancel() // 通知后台 goroutine
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

// kbSearchFunc 组装 kb_search 工具的检索回调。
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
