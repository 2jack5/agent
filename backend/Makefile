# M1～M6 常用命令；保留现有 Kafka 任务实现。
GO ?= go
DOCKER ?= docker
COMPOSE_FILE := deployments/docker-compose.yaml
BIN_DIR := bin
.DEFAULT_GOAL := help

help:
	@echo "make compose-core  启动 M3 核心存储（Kafka 另行启动）"
	@echo "make migrate       执行升级迁移并初始化管理员"
	@echo "make run           启动 API 服务"
	@echo "make build         编译 API、Worker、CLI 和迁移命令"
	@echo "make run-worker    启动文档 Kafka Worker"
	@echo "make run-cli       启动原项目的 CLI 入口"
	@echo "make test          检查当前全部 Go 包"
	@echo "make compose-down  停止核心存储，保留数据卷"
	@echo "make compose-up    启动 full/obs 基础设施（Kafka 另行启动）"
	@echo "make cover         运行 race 测试并生成覆盖率（需 C 工具链）"
	@echo "make lint          运行 golangci-lint（需自行安装）"
	@echo "make swag          生成 Swagger 文件（需自行安装 swag）"

compose-core:
	$(DOCKER) compose -f $(COMPOSE_FILE) --profile core up -d

compose-down:
	$(DOCKER) compose -f $(COMPOSE_FILE) stop postgres redis minio etcd milvus milvus-minio

migrate:
	$(GO) run ./cmd/migrate

run:
	$(GO) run ./cmd/server

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/my-agent ./cmd/server
	$(GO) build -o $(BIN_DIR)/my-agent-migrate ./cmd/migrate
	$(GO) build -o $(BIN_DIR)/my-agent-cli ./cmd/cli
	$(GO) build -o $(BIN_DIR)/my-agent-worker ./cmd/worker

run-worker:
	$(GO) run ./cmd/worker

run-cli:
	$(GO) run ./cmd/cli

test:
	$(GO) test ./...

.PHONY: help compose-core compose-down migrate run run-worker run-cli build test

# 保留已有 compose-down 的停止/保留卷行为，不引入原 down -v。
compose-up:
	$(DOCKER) compose -f $(COMPOSE_FILE) --profile full --profile obs up -d

cover:
	$(GO) test ./... -race -coverprofile=coverage.out
	$(GO) tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

swag:
	swag init -g cmd/server/main.go -o api/openapi

.PHONY: compose-up cover lint swag
