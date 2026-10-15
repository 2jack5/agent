// M1 API 入口：配置、日志、PG/Redis、JWT、认证服务与 HTTP。
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"

	"my_agent/internal/api"
	"my_agent/internal/config"
	"my_agent/internal/infra"
	"my_agent/internal/observability"
	"my_agent/internal/observability/logger"
	"my_agent/internal/repo"
	"my_agent/internal/security"
	"my_agent/internal/service"
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

	// ② M1 仅依赖 PG 和 Redis。表结构由独立迁移命令创建。
	db, err := infra.NewPG(ctx, &cfg.PG)
	if err != nil {
		return fmt.Errorf("pg: %w", err)
	}
	rdb, err := infra.NewRedis(ctx, &cfg.Redis)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	// ③ 共享 Repo、JWT 和认证业务依赖。
	repos := repo.New(db)
	jwtMgr := security.NewJWTManager(cfg.JWT.Secret, cfg.JWT.AccessTTL, cfg.JWT.RefreshTTL)
	svcs, err := service.New(service.Deps{
		Cfg: cfg, Repos: repos, JWT: jwtMgr, RDB: rdb,
	})
	if err != nil {
		return fmt.Errorf("services: %w", err)
	}
	// Gateway/Encryptor/Vector/Executor/Registry/Streams 的装配随 M2–M3 补回；
	// 现有完整配置、实体及 StreamPublisher 接口继续保留。

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
	api.Register(root, api.New(svcs), jwtMgr)

	// ⑤ 注册退出信号后开始监听，沿用原项目的 Spin/Shutdown 流程。
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
