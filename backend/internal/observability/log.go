package observability

import (
	"my_agent/internal/observability/logger"

	"go.uber.org/zap"
)

// LogInfo 信息日志。
func LogInfo(msg string, kv ...string) {
	fields := kvToFields(kv...)
	logger.L().Info(msg, fields...)
}

// LogWarn 告警日志。
func LogWarn(msg string, kv ...string) {
	fields := kvToFields(kv...)
	logger.L().Warn(msg, fields...)
}

// LogError 错误日志。
func LogError(msg string, kv ...string) {
	fields := kvToFields(kv...)
	logger.L().Error(msg, fields...)
}

func kvToFields(kv ...string) []zap.Field {
	fields := make([]zap.Field, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		fields = append(fields, zap.String(kv[i], kv[i+1]))
	}
	return fields
}
