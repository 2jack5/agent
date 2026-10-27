// Package worker 异步任务消费者（td.md §8.8，Ep 08）。
//
// 基于 Redis Streams 的轻量任务队列（替代 Kafka 的教学取舍）：
//
//	投递：XADD stream:doc-etl * doc_id {id}
//	消费：XREADGROUP（消费者组，多实例水平扩展）
//	收尸：XAUTOCLAIM 取回僵死消息（消费中宕机的遗留）
//	兜底：重试超限 → 死信队列 stream:doc-etl:dlq
//
// 任务类型：doc-etl（文档索引）/ eval（评估，P2 接入）
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"my_agent/internal/config"
	"my_agent/internal/model"
	"my_agent/internal/rag/engine"
	"my_agent/internal/rag/loader"
	"my_agent/internal/repo"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

// Publisher 任务投递者（实现 service.StreamPublisher）。
type Publisher struct {
	writer    *kafka.Writer
	docTopic  string
	evalTopic string
}

// NewPublisher 创建发布者，实现原有 service.StreamPublisher 接口。
func NewPublisher(writer *kafka.Writer, cfg *config.Kafka) *Publisher {
	return &Publisher{
		writer:    writer,
		docTopic:  cfg.DocTopic,
		evalTopic: cfg.EvalTopic,
	}
}

// DocTask 是文档任务的消息格式。
type DocTask struct {
	DocID int64 `json:"doc_id"`
}

// PublishDocETL 发布文档处理任务。
func (p *Publisher) PublishDocETL(ctx context.Context, docID int64) error {
	// 检查任务参数。
	if docID <= 0 {
		return fmt.Errorf("invalid doc_id: %d", docID)
	}

	// 将文档 ID 编码成 JSON。
	body, err := json.Marshal(DocTask{DocID: docID})
	if err != nil {
		return err
	}

	// 限制发布任务的等待时间。
	sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 发送任务，等待 Kafka 返回发送结果。
	return p.writer.WriteMessages(sendCtx, kafka.Message{
		Topic: p.docTopic,
		Key:   []byte(strconv.FormatInt(docID, 10)),
		Value: body,
	})
}

// PublishEvalRun 保留原接口中的评估任务发布方法。
func (p *Publisher) PublishEvalRun(ctx context.Context, runID int64) error {
	if runID <= 0 {
		return fmt.Errorf("invalid run_id: %d", runID)
	}

	body, err := json.Marshal(map[string]int64{
		"run_id": runID,
	})
	if err != nil {
		return err
	}

	sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	return p.writer.WriteMessages(sendCtx, kafka.Message{
		Topic: p.evalTopic,
		Key:   []byte(strconv.FormatInt(runID, 10)),
		Value: body,
	})
}

//// PublishDocETL 投递文档 ETL 任务。
//func (p *Publisher) PublishDocETL(ctx context.Context, docID int64) error {
//	return p.rdb.XAdd(ctx, &redis.XAddArgs{
//		Stream: StreamDocETL,
//		Values: map[string]any{"doc_id": strconv.FormatInt(docID, 10)},
//	}).Err()
//}
//
//// PublishEvalRun 投递评估任务。
//func (p *Publisher) PublishEvalRun(ctx context.Context, runID int64) error {
//	return p.rdb.XAdd(ctx, &redis.XAddArgs{
//		Stream: StreamEval,
//		Values: map[string]any{"run_id": strconv.FormatInt(runID, 10)},
//	}).Err()
//}

// Worker 从 Kafka 读取文档任务并执行。
type Worker struct {
	reader   *kafka.Reader
	writer   *kafka.Writer
	dlqTopic string

	repos  *repo.Repos
	engine *engine.Engine
	minio  *minio.Client
	bucket string
	cfg    *config.Worker
}

// New 创建 Worker；这里仅装配组件，不开始消费。
func New(
	reader *kafka.Reader,
	writer *kafka.Writer,
	dlqTopic string,
	repos *repo.Repos,
	eng *engine.Engine,
	mc *minio.Client,
	bucket string,
	cfg *config.Worker,
) *Worker {
	return &Worker{
		reader:   reader,
		writer:   writer,
		dlqTopic: dlqTopic,
		repos:    repos,
		engine:   eng,
		minio:    mc,
		bucket:   bucket,
		cfg:      cfg,
	}
}

//	卡夫卡
//
// Run 启动消费循环（阻塞，直到 ctx 取消）。
// Run 顺序读取任务，处理完成后手动提交进度。
func (w *Worker) Run(ctx context.Context) error {
	for {
		// 停机时结束消费。
		if ctx.Err() != nil {
			return nil
		}
		// 读取消息，但此时不提交消费进度。
		msg, err := w.reader.FetchMessage(ctx) // ReadMessage() 会自动提交进度;	FetchMessage() 不会自动提交进度
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch task: %w", err)
		}
		// 执行任务，或者将失败任务可靠地写入死信。
		if err := w.process(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// 不继续消费后面的消息，避免越过未完成任务提交进度。
			return fmt.Errorf("process task: %w", err)
		}
		// 处理完成以后，再提交当前消息对应的消费进度。
		commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = w.reader.CommitMessages(commitCtx, msg)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit task: %w", err)
		}
	}
}

// 任务失败
// ↓
// 记录失败状态
// ↓
// 发送死信
// ↓
// 死信发送成功
// ↓
// 返回 Run()
// ↓
// 提交原任务的消费进度
// process 解析任务并执行；失败时转入死信。
func (w *Worker) process(ctx context.Context, msg kafka.Message) error {
	var task DocTask
	// 解析消息中的 JSON。
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		return w.deadLetter(
			ctx,
			msg,
			0,
			fmt.Errorf("decode task: %w", err),
		)
	}
	// 检查文档 ID。
	if task.DocID <= 0 {
		return w.deadLetter(
			ctx,
			msg,
			0,
			fmt.Errorf("invalid doc_id: %d", task.DocID),
		)
	}

	// 给单次文档处理设置超时。
	timeout := w.cfg.TaskTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}

	taskCtx, cancel := context.WithTimeout(ctx, timeout)

	// 继续复用原来的文档处理逻辑。
	err := w.handleDocETL(taskCtx, task.DocID)
	cancel()

	if err == nil {
		return nil
	}

	// 如果是整个进程正在退出，不把任务当成业务失败。
	// 不提交进度，重启后允许再次处理。
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 当前版本：业务失败直接进入死信。
	return w.deadLetter(ctx, msg, task.DocID, err)
}

// deadLetter 重试超限进死信队列（简单策略：按文档失败计数判断）。
// deadLetter 保存失败状态和原始消息。
// 返回 nil 表示失败任务已经交给死信 Topic。
func (w *Worker) deadLetter(ctx context.Context, msg kafka.Message, docID int64, cause error) error {
	// 更新仍然存在的文档状态。
	if docID > 0 {
		doc, err := w.repos.KB.GetDoc(ctx, docID)

		if err == nil {
			doc.Status = model.DocStatusFailed
			doc.Error = truncateErr(cause)

			if err := w.repos.KB.UpdateDoc(ctx, doc); err != nil {
				return fmt.Errorf("save failed status: %w", err)
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			// 数据库暂时不可用：停止当前处理，不提交消费进度。
			return fmt.Errorf("load failed document: %w", err)
		}

		// 文档已经被删除时，仍然可以把任务送入死信。
	}

	// 保存失败原因和原始消息位置，方便排查。
	body, err := json.Marshal(map[string]any{
		"doc_id":           docID,
		"error":            cause.Error(),
		"source_topic":     msg.Topic,
		"source_partition": msg.Partition,
		"source_offset":    msg.Offset,
		"original_value":   string(msg.Value),
	})
	if err != nil {
		return err
	}

	// 限制死信发送时间。
	sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 同步发送，确认死信保存成功。
	if err := w.writer.WriteMessages(sendCtx, kafka.Message{
		Topic: w.dlqTopic,
		Key:   msg.Key,
		Value: body,
	}); err != nil {
		return fmt.Errorf("publish dead letter: %w", err)
	}

	log.Printf(
		"[worker] document %d sent to dead letter: %v",
		docID,
		cause,
	)
	return nil
}

// handleDocETL 文档索引管线（状态机推进由本方法负责）。
func (w *Worker) handleDocETL(ctx context.Context, docID int64) error {
	repos := w.repos

	// ① 取文档与知识库
	doc, err := repos.KB.GetDoc(ctx, docID)
	if err != nil {
		return fmt.Errorf("get doc %d: %w", docID, err)
	}
	if doc.Status == model.DocStatusReady {
		return nil // 幂等：重复投递直接跳过
	}
	kb, err := repos.KB.GetKB(ctx, doc.KBID)
	if err != nil {
		return fmt.Errorf("get kb %d: %w", doc.KBID, err)
	}

	// ② 解析状态推进
	doc.Status = model.DocStatusParsing
	_ = repos.KB.UpdateDoc(ctx, doc)

	// ③ 对象存储取回原文
	data, err := w.getObject(ctx, doc.ObjectKey)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}

	// ④ 解析（PDF/DOCX/MD/TXT/HTML）
	loaded, err := loader.Load(doc.Filename, data)
	if err != nil {
		return fmt.Errorf("parse %s: %w", doc.Filename, err)
	}
	doc.Status = model.DocStatusChunking
	_ = repos.KB.UpdateDoc(ctx, doc)

	// ⑤ 索引：分块落库 + 向量化（engine 内部推进 embedding）
	if err := w.engine.IndexDocument(ctx, kb, doc, loaded); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	// ⑥ 完成
	doc.Status = model.DocStatusReady
	doc.Error = ""
	if err := repos.KB.UpdateDoc(ctx, doc); err != nil {
		return fmt.Errorf("update doc: %w", err)
	}
	log.Printf("[worker] doc-etl %d ready: chunks=%d tokens=%d", doc.ID, doc.ChunkCount, doc.TokenCount)
	return nil
}

// getObject 从 MinIO 拉取对象。
func (w *Worker) getObject(ctx context.Context, key string) ([]byte, error) {
	obj, err := w.minio.GetObject(ctx, w.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

// isBusyGroup 消费组已存在（BUSYGROUP）不算错误。
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func truncateErr(err error) string {
	s := err.Error()
	if len(s) > 500 {
		s = s[:500]
	}
	return s
}
