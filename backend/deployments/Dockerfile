# 构建上下文为 backend 根目录；TARGET 支持 server/worker/cli/migrate。
# 工具链满足当前 go.mod；镜像拉取和构建请按手动验收说明自行执行。
ARG GO_VERSION=1.26.2
FROM golang:${GO_VERSION}-alpine AS builder
ENV GOTOOLCHAIN=local
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGET=server
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/${TARGET}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /out/app ./app
COPY configs ./configs
# migrate 默认使用 file://migrations，运行镜像需要对应 SQL 文件。
COPY migrations ./migrations
ENTRYPOINT ["./app"]
