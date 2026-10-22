package api

import (
	"context"
	"my_agent/internal/security"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/route"
)

func Register(g *route.RouterGroup, h *Handler, jwt *security.JWTManager) {
	v1 := g.Group("/api/v1")
	// ---- 公开：认证 ----
	auth := v1.Group("/auth")
	{
		auth.POST("/register", h.Register)
		auth.POST("/login", h.Login)
		auth.POST("/refresh", h.Refresh)
		// 登出需登录态：吊销 refresh token 白名单（td.md 认证四件套均在 /auth 下）
		auth.POST("/logout", Auth(jwt), h.Logout)
	}

	// ---- 认证后的用户接口 ----
	user := v1.Group("", Auth(jwt))

	{
		// 对话与会话
		user.POST("/chat/stream", h.Chat)
		user.GET("/sessions", h.ListSessions)
		user.GET("/sessions/:id/messages", h.ListMessages)
		user.PUT("/sessions/:id", h.RenameSession)
		user.DELETE("/sessions/:id", h.DeleteSession)

		// Agent 与工具
		user.GET("/agents", h.ListAgents)
		user.GET("/agents/:id", h.GetAgent)
		user.POST("/agents", h.CreateAgent)
		user.PUT("/agents/:id", h.UpdateAgent)
		user.DELETE("/agents/:id", h.DeleteAgent)
		user.GET("/tools", h.ListTools)

	}
	// ---- 管理接口（admin）----
	admin := v1.Group("/admin", Auth(jwt), AdminOnly())
	{
		// 模型网关配置
		admin.GET("/models/providers", h.ListProviders)
		admin.POST("/models/providers", h.CreateProvider)
		admin.PUT("/models/providers/:id", h.UpdateProvider)
		admin.DELETE("/models/providers/:id", h.DeleteProvider)
		admin.GET("/models", h.ListModels)
		admin.POST("/models", h.CreateModel)
		admin.PUT("/models/:id", h.UpdateModel)
		admin.DELETE("/models/:id", h.DeleteModel)

	}

	// 当前用户接口使用 Access Token，保留原项目的真实路由。
	user := v1.Group("", Auth(jwt))
	user.GET("/me", h.Me)
}

func Healthz() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	}
}
