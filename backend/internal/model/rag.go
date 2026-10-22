// RAG 域实体：知识库 / 文档 / 分块。
package model

import (
	"time"

	"gorm.io/datatypes"
)

// KnowledgeBase 知识库。
type KnowledgeBase struct {
	ID                int64          `gorm:"primaryKey" json:"id"`                         // 知识库主键 ID
	Name              string         `gorm:"size:128" json:"name"`                         // 知识库名称，供列表和管理页面展示
	Description       string         `gorm:"type:text" json:"description"`                 // 知识库说明，描述所收录资料的内容或用途
	UserID            int64          `gorm:"index" json:"user_id"`                         // 所属用户 ID，关联 users.id
	EmbeddingConfigID int64          `json:"embedding_config_id"`                          // 关联的向量化模型配置 ID，对应 model_configs.id
	ChunkSize         int            `gorm:"default:512" json:"chunk_size"`                // 分块目标大小，当前分块器按字符数处理，默认 512
	ChunkOverlap      int            `gorm:"default:64" json:"chunk_overlap"`              // 相邻分块重叠的字符数，保留跨片段上下文，默认 64
	ParserConfig      datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"parser_config"` // 文档解析配置（JSON），预留给解析器使用，当前解析流程未读取
	DocCount          int            `json:"doc_count"`                                    // 知识库中的文档数量统计
	MilvusCollection  string         `gorm:"size:128" json:"milvus_collection"`            // 该知识库对应的 Milvus 向量集合名称，格式为 kb_{id}
	Status            string         `gorm:"size:20;default:active" json:"status"`         // 知识库状态，默认为 active（启用）
	CreatedAt         time.Time      `json:"created_at"`                                   // 知识库创建时间
	UpdatedAt         time.Time      `json:"updated_at"`                                   // 知识库记录最后更新时间
}

// Document 文档（ETL 状态机驱动）。
type Document struct {
	ID          int64          `gorm:"primaryKey" json:"id"`                                                    // 文档主键 ID
	KBID        int64          `gorm:"index:idx_documents_kb,priority:1" json:"kb_id"`                          // 所属知识库 ID，关联 knowledge_bases.id
	Filename    string         `gorm:"size:256" json:"filename"`                                                // 上传时的文件名，包含扩展名
	FileType    string         `gorm:"size:16" json:"file_type"`                                                // 文件类型，例如 pdf、docx、txt，不带扩展名前的点
	ObjectKey   string         `gorm:"size:512" json:"object_key"`                                              // 原始文件在对象存储中的键，用于从 MinIO 读取文件
	FileSize    int64          `json:"file_size"`                                                               // 原始文件大小，单位为字节
	SourceURL   string         `gorm:"size:512" json:"source_url"`                                              // 文档来源地址，可用于记录网页等来源，上传文件时可为空
	Status      string         `gorm:"size:20;index:idx_documents_kb,priority:2;default:pending" json:"status"` // 文档处理状态，取值见下方 DocStatus 常量
	Error       string         `gorm:"size:512" json:"error"`                                                   // 文档处理失败时记录的错误信息
	ChunkCount  int            `json:"chunk_count"`                                                             // 文档解析、切分后生成的片段数量
	TokenCount  int            `json:"token_count"`                                                             // 文档各片段的估算 Token 总数，用于统计文本规模
	EnableGraph bool           `json:"enable_graph"`                                                            // 是否启用 GraphRAG，当前为预留扩展字段
	Metadata    datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"metadata"`                                 // 文档级扩展信息，以 JSON 保存
	CreatedAt   time.Time      `json:"created_at"`                                                              // 文档记录创建时间
	UpdatedAt   time.Time      `json:"updated_at"`                                                              // 文档记录最后更新时间
}

// 文档 ETL 状态机。
const (
	DocStatusPending   = "pending"   // 等待后台任务处理
	DocStatusParsing   = "parsing"   // 正在提取文档文本
	DocStatusChunking  = "chunking"  // 正在将文本切分为片段
	DocStatusEmbedding = "embedding" // 向量化阶段的状态值，当前入库流程未单独写入该状态
	DocStatusReady     = "ready"     // 文档处理完成，可供检索
	DocStatusFailed    = "failed"    // 文档处理失败
)

// DocumentChunk 分块（id 即 Milvus 主键）。
type DocumentChunk struct {
	ID         int64          `gorm:"primaryKey" json:"id"`                               // 片段主键 ID，同时用作 Milvus 中对应向量的主键
	KBID       int64          `gorm:"index" json:"kb_id"`                                 // 所属知识库 ID，便于按知识库检索片段
	DocumentID int64          `gorm:"index:idx_chunks_doc,priority:1" json:"document_id"` // 所属文档 ID，关联 documents.id
	Seq        int            `json:"seq"`                                                // 片段序号，从 0 开始；当前分块器每次处理一页时重新编号
	Content    string         `gorm:"type:text" json:"content"`                           // 片段正文，用于关键词检索和组装模型参考资料
	TokenCount int            `json:"token_count"`                                        // 当前片段的估算 Token 数量
	Meta       datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"meta"`                // 片段定位等扩展信息，例如 {"page":3,"heading":"标题"}
	VectorID   string         `gorm:"size:64" json:"vector_id"`                           // 预留的向量标识字段；当前 Milvus 实际使用片段 ID 作为主键
	Status     int16          `gorm:"default:1" json:"status"`                            // 片段状态标记，默认值为 1，当前未定义其他状态常量
	CreatedAt  time.Time      `gorm:"index:idx_chunks_doc,priority:2" json:"created_at"`  // 片段记录创建时间
}
