# M1 常用命令。后续 Worker/CLI 的实际入口完成后再加入构建。
GO ?= go
DOCKER ?= docker
COMPOSE_FILE := deployments/docker-compose.yaml
BIN_DIR := bin
.DEFAULT_GOAL := help

help:
	@echo "make compose-core  启动 M1 的 PostgreSQL 和 Redis"
	@echo "make migrate       执行升级迁移并初始化管理员"
	@echo "make run           启动认证 API 服务"
	@echo "make build         编译 API 服务和迁移命令"
	@echo "make test          检查当前全部 Go 包"
	@echo "make compose-down  停止 M1 存储，保留数据卷"

compose-core:
	$(DOCKER) compose -f $(COMPOSE_FILE) --profile core up -d postgres redis

compose-down:
	$(DOCKER) compose -f $(COMPOSE_FILE) stop postgres redis

migrate:
	$(GO) run ./cmd/migrate

run:
	$(GO) run ./cmd/server

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/my-agent ./cmd/server
	$(GO) build -o $(BIN_DIR)/my-agent-migrate ./cmd/migrate

test:
	$(GO) test ./...

.PHONY: help compose-core compose-down migrate run build test
