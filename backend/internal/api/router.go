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

	// 当前用户接口使用 Access Token，保留原项目的真实路由。
	user := v1.Group("", Auth(jwt))
	user.GET("/me", h.Me)
}

func Healthz() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	}
}
