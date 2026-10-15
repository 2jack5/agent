# Community MinIO is source-only; build a pinned upstream release locally.
FROM golang:1.25.9 AS builder
ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
RUN go install github.com/minio/minio@${MINIO_VERSION}

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /go/bin/minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data"]
