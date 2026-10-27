// NexusAgent 异步任务 Worker 入口：独立进程消费 Redis Streams。
//
// 为什么独立进程（教学点）：
//   - ETL/评估是 CPU/IO 密集任务，与 API 进程隔离避免互相拖累
//   - 独立伸缩：文档上传高峰时只扩 worker 副本
package main

import (
	"context"
	"fmt"
	"my_agent/internal/config"
	"my_agent/internal/infra"
	"my_agent/internal/llm"
	"my_agent/internal/observability"
	"my_agent/internal/observability/logger"
	"my_agent/internal/rag/engine"
	"my_agent/internal/repo"
	"my_agent/internal/security"
	"my_agent/internal/service"
	"my_agent/internal/vector"
	"my_agent/internal/worker"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("")
	if err != nil {
		return err
	}
	logger.Init(cfg.Log.Level, cfg.Log.Format)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 基础设施（API 与 worker 共享同一套存储）
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

	// RAG 引擎（doc-etl 消费方）
	gateway := llm.NewGateway(&cfg.LLM)
	store := vector.NewMilvusStore(mcli)
	repos := repo.New(db)

	// 加密器与 server 同源：模型解析需解密 api_key（缺失会 nil panic）
	encryptor, err := security.NewEncryptor(cfg.SecretKey)
	if err != nil {
		return fmt.Errorf("encryptor: %w", err)
	}

	// 默认模型解析与 server 同源（service.New 仅用于模型解析）
	svcs, err := service.New(service.Deps{
		Cfg: cfg, Repos: repos, Gateway: gateway, RDB: rdb, Vector: store, Encryptor: encryptor,
	})
	if err != nil {
		return fmt.Errorf("services: %w", err)
	}
	ragEngine := engine.New(gateway, store, repos.KB, &cfg.RAG, cfg.Milvus.Dim)
	ragEngine.SetModels(svcs.DefaultEmbedding, svcs.DefaultRerank)

	// 文档 ETL 必须从 MinIO 读取文件，初始化失败就退出。
	mc, err := infra.NewMinIO(ctx, &cfg.MinIO)
	if err != nil {
		return fmt.Errorf("minio: %w", err)
	}

	// 创建 Kafka 文档消费者。
	reader := infra.NewDocKafkaReader(&cfg.Kafka)
	defer reader.Close()

	// 创建 Kafka 写入客户端，用于发送死信。
	writer := infra.NewKafkaWriter(cfg.Kafka.Brokers)
	defer writer.Close()

	// 装配后台 Worker。
	w := worker.New(
		reader,
		writer,
		cfg.Kafka.DLQTopic,
		repos,
		ragEngine,
		mc,
		cfg.MinIO.Bucket,
		&cfg.Worker,
	)

	observability.LogInfo(
		"nexus-agent worker started",
		"topic", cfg.Kafka.DocTopic,
		"group", cfg.Kafka.GroupID,
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		cancel()
	}()
	return w.Run(ctx)
}
