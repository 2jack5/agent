package main

import (
	"context"
	"fmt"
	"my_agent/internal/api"
	"my_agent/internal/config"
	"my_agent/internal/infra"
	"my_agent/internal/observability/logger"
	"my_agent/internal/repo"
	"my_agent/internal/service"
	"os"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "server exited: %v\n", err) // os.Stderr可以区分错误
		os.Exit(1)
	}
}

func run() error {
	// ===== ① 配置与日志 =====
	cfg, err := config.Load("")
	if err != nil {
		return err
	}
	logger.Init(cfg.Log.Level, cfg.Log.Format)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ===== ③ 基础设施 =====
	db, err := infra.NewPG(ctx, &cfg.PG)
	if err != nil {
		return fmt.Errorf("pg: %w", err)
	}
	rdb, err := infra.NewRedis(ctx, &cfg.Redis)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	// ===== ⑥ 数据访问与工具系统 =====
	repos := repo.New(db)

	// ===== ⑦ service 层 =====
	svcs, err := service.New(service.Deps{
		Cfg: cfg, Repos: repos, Gateway: gateway,
		Encryptor: encryptor, JWT: jwtMgr, RDB: rdb,
		Vector: store, Executor: executor, Registry: reg,
		Streams: worker.NewPublisher(rdb),
	})
	if err != nil {
		return fmt.Errorf("services: %w", err)
	}

	// ===== ⑫ Hertz 服务 =====
	h := server.New(
		server.WithHostPorts(":"+cfg.HTTP.Port),
		server.WithReadTimeout(30*time.Second),
		server.WithWriteTimeout(0), // SSE 长连接不超时
		server.WithIdleTimeout(120*time.Second),
	)
	root := h.Group("/")
	root.GET("/healthz", api.Healthz())

}
