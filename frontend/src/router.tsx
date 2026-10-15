/**
 * 路由表 —— 含登录守卫与 admin 守卫。
 *
 * 守卫策略：bootstrap 完成前渲染启动屏（防止刷新闪跳登录页）；
 * 未登录 → 重定向 /login；非 admin 访问 /admin/* → 重定向 /chat。
 */
import { Navigate, Route, Routes, useLocation } from 'react-router-dom';
import type { ReactNode } from 'react';
import { useAuth } from '@/stores/auth';
import { AppLayout } from '@/components/layout/AppLayout';
import { Spinner } from '@/components/ui/primitives';
import { LoginPage } from '@/pages/LoginPage';
import { M1WelcomePage, StagePlaceholder } from '@/pages/M1WelcomePage';
import { NotFoundPage } from '@/pages/NotFoundPage';

/** 登录守卫 */
function RequireAuth({ children }: { children: ReactNode }) {
  const { user, bootstrapped } = useAuth();
  const location = useLocation();
  if (!bootstrapped) return <BootScreen />;
  if (!user) return <Navigate to="/login" state={{ from: location.pathname }} replace />;
  return <>{children}</>;
}

/** admin 守卫 */
function RequireAdmin({ children }: { children: ReactNode }) {
  const user = useAuth((s) => s.user);
  if (user?.role !== 'admin') return <Navigate to="/chat" replace />;
  return <>{children}</>;
}

/** 启动屏：会话恢复中 */
function BootScreen() {
  return (
    <div className="flex h-full items-center justify-center gap-3 text-fog">
      <Spinner />
      <span className="text-sm">正在恢复会话…</span>
    </div>
  );
}

export function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />

      <Route
        element={
          <RequireAuth>
            <AppLayout />
          </RequireAuth>
        }
      >
        <Route path="/chat" element={<M1WelcomePage />} />
        <Route path="/agents" element={<StagePlaceholder title="Agent" />} />
        <Route path="/memories" element={<StagePlaceholder title="记忆" />} />
        <Route path="/kbs" element={<StagePlaceholder title="知识库" />} />
        <Route path="/kbs/:id" element={<StagePlaceholder title="知识库详情" />} />
        <Route path="/workflows" element={<StagePlaceholder title="工作流" />} />
        <Route path="/workflows/runs" element={<StagePlaceholder title="工作流运行记录" />} />
        <Route
          path="/admin/models"
          element={
            <RequireAdmin>
              <StagePlaceholder title="模型管理" />
            </RequireAdmin>
          }
        />
        <Route
          path="/admin/integrations"
          element={
            <RequireAdmin>
              <StagePlaceholder title="集成中心" />
            </RequireAdmin>
          }
        />
        <Route path="/" element={<Navigate to="/chat" replace />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
    </Routes>
  );
}
