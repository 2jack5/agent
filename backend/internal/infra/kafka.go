package infra

import (
	"my_agent/internal/config"
	"time"

	"github.com/segmentio/kafka-go"
)

// NewKafkaWriter 创建消息发送客户端。
// 每次发送时由 kafka.Message.Topic 指定目标主题。
func NewKafkaWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(brokers...),

		// 根据消息 Key 分配分区。
		Balancer: &kafka.Hash{},

		// 等待当前同步副本集合的确认。
		RequiredAcks: kafka.RequireAll,

		// 同步发送：调用方可以知道此次发送是否成功。
		Async: false,

		// 教学版每条任务直接形成一个批次。
		BatchSize: 1,

		// 网络发送失败时的尝试次数，不是文档业务重试次数。
		MaxAttempts: 3,

		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,

		// Topic 由部署步骤明确创建。
		AllowAutoTopicCreation: false,
	}
}

// NewDocKafkaReader 创建文档任务消费者。
func NewDocKafkaReader(cfg *config.Kafka) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.DocTopic,
		GroupID: cfg.GroupID,

		// 有消息就尽快返回，不必积攒很大的批次。
		MinBytes: 1,
		MaxBytes: 1024 * 1024,
		MaxWait:  time.Second,

		// 仅在消费组没有有效历史进度时，从最早保留的消息开始。
		StartOffset: kafka.FirstOffset,

		// CommitMessages 调用时同步提交进度。
		CommitInterval: 0,
	})
}
