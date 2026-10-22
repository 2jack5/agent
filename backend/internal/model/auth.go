package model

import (
	"time"

	"gorm.io/datatypes"
)

// User 用户（P0 仅 admin/user 两级角色，不做 RBAC）。
type User struct {
	ID           int64     `gorm:"primaryKey" json:"id"`                // 用户主键 ID
	Username     string    `gorm:"size:64;uniqueIndex" json:"username"` // 用户名，用于登录，不允许重复
	Email        string    `gorm:"size:128;uniqueIndex" json:"email"`   // 用户邮箱，不允许重复
	PasswordHash string    `gorm:"size:256" json:"-"`                   // bcrypt 密码哈希，不保存明文，不参与 JSON 序列化
	Role         string    `gorm:"size:20;default:user" json:"role"`    // 用户角色：admin 为管理员，user 为普通用户，默认 user
	AvatarURL    string    `gorm:"size:512" json:"avatar_url"`          // 用户头像地址
	Status       int16     `gorm:"default:1" json:"status"`             // 账号状态：1 为正常，其他值禁止登录
	CreatedAt    time.Time `json:"created_at"`                          // 用户创建时间
	UpdatedAt    time.Time `json:"updated_at"`                          // 用户信息最后更新时间
}

// IsAdmin 是否管理员。
func (u *User) IsAdmin() bool { return u.Role == "admin" }

// Session 对话会话（对外 ID 用 UUID 防遍历）。
type Session struct {
	ID           string         `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"` // 会话主键 UUID，默认由数据库生成
	UserID       int64          `gorm:"index" json:"user_id"`                                     // 会话所属用户的 ID
	AgentID      *int64         `json:"agent_id"`                                                 // 关联的 Agent ID，nil 表示未绑定
	Title        string         `gorm:"size:256;default:新对话" json:"title"`                        // 会话标题，默认为“新对话”
	Status       string         `gorm:"size:20;default:active" json:"status"`                     // 会话状态：active 为活跃，archived 为已归档
	Pinned       bool           `json:"pinned"`                                                   // 是否置顶，列表中置顶会话优先显示
	MessageCount int            `json:"message_count"`                                            // 会话累计消息数
	TokenUsage   int64          `json:"token_usage"`                                              // 会话累计消耗的 Token 数
	LastMsgAt    *time.Time     `json:"last_msg_at"`                                              // 最近消息时间，nil 表示尚未记录
	Metadata     datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"metadata"`                  // 会话扩展信息，以 JSONB 存储，默认为空对象
	CreatedAt    time.Time      `json:"created_at"`                                               // 会话创建时间
	UpdatedAt    time.Time      `json:"updated_at"`                                               // 会话信息最后更新时间
}

// Message 消息。role: user/assistant/system/tool
type Message struct {
	ID               int64          `gorm:"primaryKey" json:"id"`                                              // 消息主键 ID
	SessionID        string         `gorm:"type:uuid;index:idx_messages_session,priority:1" json:"session_id"` // 所属会话的 UUID，与创建时间组成联合索引
	Role             string         `gorm:"size:20" json:"role"`                                               // 消息角色：user、assistant、system 或 tool
	Content          string         `gorm:"type:text" json:"content"`                                          // 消息正文
	ReasoningContent string         `gorm:"type:text" json:"reasoning_content,omitempty"`                      // 推理模型返回的思考过程
	ToolCalls        datatypes.JSON `gorm:"type:jsonb" json:"tool_calls,omitempty"`                            // 模型发起的工具调用原始结构，以 JSONB 存储
	ToolCallID       string         `gorm:"size:128" json:"tool_call_id,omitempty"`                            // 工具结果对应的调用 ID，用于关联具体工具调用
	Refs             datatypes.JSON `gorm:"type:jsonb" json:"refs,omitempty"`                                  // RAG 检索引用信息，以 JSONB 存储
	ModelName        string         `gorm:"size:64" json:"model_name,omitempty"`                               // 生成该消息所使用的模型名称
	PromptTokens     int            `json:"prompt_tokens"`                                                     // 输入提示词消耗的 Token 数
	CompletionTokens int            `json:"completion_tokens"`                                                 // 模型输出消耗的 Token 数
	FirstTokenMs     int            `json:"first_token_ms"`                                                    // 首个 Token 返回耗时，单位为毫秒
	LatencyMs        int            `json:"latency_ms"`                                                        // 消息生成总耗时，单位为毫秒
	Status           int16          `gorm:"default:1" json:"status"`                                           // 消息状态：1 为完成，2 为用户中断，3 为失败
	TraceID          string         `gorm:"size:64;default:" json:"trace_id"`                                  // 链路追踪 ID，用于关联本次请求的追踪记录
	Metadata         datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"metadata"`                           // 消息扩展信息，可包含 agent_trace 决策链
	CreatedAt        time.Time      `gorm:"index:idx_messages_session,priority:2" json:"created_at"`           // 消息创建时间，与会话 ID 组成联合索引
}

// 消息状态。
const (
	MsgStatusDone    int16 = 1
	MsgStatusStopped int16 = 2 // 用户中断
	MsgStatusFailed  int16 = 3
)

// Feedback 用户反馈（坏例回流闭环入口）。
type Feedback struct {
	ID         int64     `gorm:"primaryKey" json:"id"`        // 反馈主键 ID
	MessageID  int64     `json:"message_id"`                  // 被反馈消息的 ID
	SessionID  string    `gorm:"type:uuid" json:"session_id"` // 被反馈消息所属会话的 UUID
	UserID     int64     `json:"user_id"`                     // 提交反馈的用户 ID
	Rating     int16     `json:"rating"`                      // 反馈评分：1 为赞，2 为踩
	Reason     string    `gorm:"size:64" json:"reason"`       // 反馈原因
	Comment    string    `gorm:"size:512" json:"comment"`     // 用户补充的反馈说明
	Resolved   bool      `json:"resolved"`                    // 反馈是否已处理
	EvalCaseID *int64    `json:"eval_case_id,omitempty"`      // 关联的评测用例 ID，nil 表示尚未关联
	CreatedAt  time.Time `json:"created_at"`                  // 反馈提交时间
}

// 反馈评分。
const (
	RatingLike    int16 = 1
	RatingDislike int16 = 2
)
