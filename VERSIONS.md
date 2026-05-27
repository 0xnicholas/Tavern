# Tavern 版本说明

本文档记录 Tavern 各版本的变更历史、功能范围、已知限制与开发规划。

---

## 版本号规则

```
主版本号.次版本号.修订号
```

- **主版本号**：重大架构变更或不兼容 API 修改
- **次版本号**：新增功能，向下兼容
- **修订号**：Bug 修复或性能优化

---

## V0.1.0 — 初始 MVP

> 状态：✅ 已发布  
> 测试：106 passed

### 功能

- **Agent 系统** (`tavern-hero`)：YAML 配置、注册表、目录加载
- **Workflow 引擎** (`tavern-comp`)：Sequential DAG、步骤超时/重试/信号等待、模板渲染
- **Runtime 适配** (`tavern-adapters`)：Pandaria HTTP、Mock
- **HTTP 服务** (`tavern-server`)：Agent/Workflow/Execution CRUD API、Metrics

---

## V0.2.0 — 生产就绪

> 状态：✅ 已完成 (2026-05-26)  
> 测试：172 passed

### 新增功能

| 阶段 | 功能 | Crate |
|------|------|-------|
| Phase 1 | SQLite EventStore（持久化） | `tavern-comp` |
| Phase 1 | PostgreSQL EventStore（可选 feature） | `tavern-comp` |
| Phase 2 | 统一配置管理（figment: TOML + env） | `tavern-config` + `tavern-server` |
| Phase 3 | API Key / Bearer Token 认证 | `tavern-server` |
| Phase 3 | `POST /auth/refresh` Token 刷新 | `tavern-server` |
| Phase 4 | SSE 实时事件流 (`/executions/:id/events/stream`) | `tavern-server` |
| Phase 5 | Agent 热重载（notify watcher） | `tavern-hero` + `tavern-server` |
| Phase 6 | 优雅停机（SIGTERM） + 健康检查增强 | `tavern-server` |
| Phase 7 | Docker 镜像 + docker-compose | 根目录 |
| Phase 8 | 端到端集成测试 + 认证测试 | `tavern-server` |

### 新增架构

| Crate | 说明 |
|-------|------|
| `tavern-config` | figment 统一配置（TOML + `TAVERN_` 环境变量） |
| `tavern-flow-macros` | proc-macro crate: `#[derive(Flow)]`, `#[flow_impl]` |
| `tavern-flow` | 方法级事件驱动编排引擎（FlowEngine / FlowGraph / FlowHandle） |

### 增强

| 功能 | 说明 |
|------|------|
| Hierarchical Process | Manager Agent 动态委派 Task |
| Planning | AgentPlanner 执行前生成结构化计划 |
| Checkpoint Recovery | 进程崩溃后从 EventStore 恢复 Running/WaitingForSignal 实例 |
| PostgreSQLEventStore | 完整实现 + 迁移（`#[cfg(feature = "postgres")]`） |

### Tavern Flow（原型）

| 功能 | 语法 |
|------|------|
| 方法级编排 | `#[start]` / `#[listen("name")]` |
| OR 组合器 | `#[listen(or("a", "b"))]` |
| AND 组合器 | `#[listen(and("a", "b"))]` |
| 条件路由 | `#[router("up")]` → `#[listen("label")]` |
| 事件循环 | `FlowEngine::execute()` |
| 异步执行 | `FlowEngine::start_async()` → `FlowHandle::await_completion()` |

### 测试覆盖

| Crate | 测试数 |
|-------|--------|
| `tavern-core` | 11 |
| `tavern-adapters` | 7 |
| `tavern-config` | 3 |
| `tavern-hero` | 19 |
| `tavern-comp` | 83 |
| `tavern-flow` | 12 |
| `tavern-flow-macros` | — |
| `tavern-server` | 37 (5 unit + 24 integration + 8 e2e) |
| **合计** | **172** |

---

## V0.3.0 — Flow 生产化

> 状态：✅ 已完成 (2026-05-27)  
> 测试：173 passed

### Tavern Flow（生产可用）

| 功能 | 优先级 | 说明 |
|------|:---:|------|
| 并行执行 | P0 | 多个 start 方法 `tokio::spawn` 并发，`JoinSet` + `Semaphore` 控制并发度 |
| REST API 集成 | P0 | `POST /flows/:id/start`, `GET /flows/:id/status`, `POST /flows/:id/cancel` |
| Flow 持久化 | P1 | `FlowEvent` 序列化为 `WorkflowEvent::External`，复用 EventStore 记录执行历史 |
| Router 多 label 返回 | P1 | `#[router]` 返回 `Vec<String>` 触发多个 listener 分支 |
| FlowRegistry | P0 | `FlowFactory` + `StartableFlow` 类型抹除注册表，支持 HTTP 动态实例化 |
| FlowHandleRef | P0 | 轻量可 Clone 引用，Server 层管理活跃 Flow 生命周期 |

### 调试与管控（Planned → 延后到 V0.4.x）

| 里程碑 | 优先级 | 说明 |
|--------|:---:|------|
| 执行回放 API | P0 | `GET /executions/:id/replay` — 从 EventStore 重放事件流 |
| 断点调试 | P1 | 在特定事件处暂停执行 |
| 步骤级审批 | P1 | `wait_for_signal` 强化，支持人工审批节点 |
| 租户级限流 | P1 | `tower::limit::RateLimit`，按 API Key 隔离 |
| 执行克隆 | P2 | 复制历史执行输入，重新触发 |

---

## V0.4.0 — 分布式 + 多租户（Planned）

> 目标：水平扩展、高可用、租户隔离

| 里程碑 | 优先级 | 技术选型 |
|--------|:---:|----------|
| Redis 任务队列 | P0 | `redis` crate |
| 执行器工作节点 | P0 | 新增 crate，从 Server 解耦 |
| 分布式锁 | P1 | Redis Redlock |
| 状态同步 | P1 | 基于 EventStore + 乐观锁 |
| gRPC 内部通信 | P2 | `tonic`（Server ↔ Worker） |
| Kubernetes 部署 | P2 | Helm Chart + HPA |
| 多租户数据隔离 | P1 | PostgreSQL row-level security 或 schema 隔离 |

---

## V1.0.0 — GA 发布（Future）

> API 冻结，长期支持

---

## 技术债务与持续优化

| 任务 | 说明 | 频率 |
|------|------|------|
| 依赖升级 | `cargo update` + `cargo audit` | 每月 |
| 文档同步 | README、API 文档、配置示例 | 每次 PR |
| 测试覆盖率 | 目标 > 80% 集成测试 | 持续 |
| 性能基准 | Workflow 执行延迟 P50/P95/P99 | 每版本 |

---

## 历史归档

| 版本 | 日期 | 测试数 | 说明 |
|------|------|--------|------|
| V0.1.0 | 2026-05-20 | 106 | 初始 MVP |
| V0.2.0 | 2026-05-26 | 172 | 生产就绪：持久化、认证、SSE、Flow 原型、恢复 |
| V0.3.0 | 2026-05-27 | 173 | Flow 生产化：并行执行、REST API、FlowRegistry、持久化、Router 多 label |
