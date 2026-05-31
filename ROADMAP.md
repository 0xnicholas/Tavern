# Tavern Roadmap

## 已完成 (Completed)

### V0.1.0 — 初始 MVP ✅ *2026-05-20*
- Agent 系统：YAML 配置、注册表、目录加载
- Workflow 引擎：Sequential DAG、超时/重试/信号等待、模板渲染
- Runtime 适配：Pandaria HTTP + Mock
- HTTP 服务：Agent/Workflow/Execution CRUD + Metrics
- 测试：106 passed

### V0.2.0 — 生产就绪 ✅ *2026-05-26*
- SQLite / PostgreSQL EventStore 持久化
- 统一配置管理（figment: TOML + 环境变量）
- API Key / Bearer Token 认证 + Token 刷新
- SSE 实时事件流
- Agent / Workflow 热重载（notify watcher）
- 优雅停机 + 健康检查增强
- Docker 镜像 + docker-compose
- Hierarchical Process（Manager Agent 动态委派）
- Planning（AgentPlanner 执行前计划）
- Checkpoint Recovery（崩溃恢复）
- Tavern Flow 原型（方法级编排、OR/AND 组合器、条件路由）
- 测试：172 passed

### V0.3.0 — Flow 生产化 ✅ *2026-05-27*
- Flow 并行执行（JoinSet + Semaphore）
- Flow REST API（`/flows/:id/start`, `/status`, `/cancel`）
- Flow 持久化（EventStore 复用）
- Router 多 label 返回
- FlowRegistry / FlowFactory 类型抹除注册表
- FlowHandleRef 轻量引用
- 测试：173 passed

### V0.3.1 — 执行回放 + Code Review 修复 ✅ *2026-05-30*
- 执行回放 API (`GET /executions/:id/replay`)
- Flow 取消机制实装（`Arc<AtomicBool>` 信号）
- Flow 输入大小校验（1 MiB 限制）
- Step 执行器 `Arc<Step>` 优化、DAG 构建复用
- 直方图指标（7 桶 duration histogram）
- 路径遍历防护、ConfigParse 错误码修正
- Rust edition 2024 迁移
- 测试：188 passed

### V0.3.2 — 审批 + 克隆 + 限流 ✅ *2026-05-30*
- 步骤级审批：`SignalAction` (Approve/Reject)，超时自动驳回
- API：`GET /approvals`、`POST /executions/:id/steps/:step_id/approve|reject`
- 执行克隆：`POST /executions/:id/clone`，仅已完成/失败可克隆
- 租户限流：滑动窗口计数器、`RateLimitConfig`、axum 中间件、429 + Retry-After
- 测试：194 passed

### V0.3.3 — 断点调试 ✅ *2026-05-30*
- Step 新增 `breakpoint: bool` 字段
- 引擎在步骤执行前检查断点，命中时暂停并发射 `BreakpointHit` 事件
- API：`GET /breakpoints` 列出所有断点，通过 `POST /executions/:id/signal` 恢复
- 测试：194 passed

---

## 进行中 (In Progress)

_等待外部项目就绪。无代码开发任务。_

---

## 计划中 (Planned)

### 多租户集成（待外部 crate 就绪）
> 直接引入外部 crate，路由 + 数据隔离

| 任务 | 说明 |
|------|------|
| 引入租户 crate | 替换现有 API Key 认证为租户身份解析 |
| 数据隔离 | SQLite 按租户分文件 / PostgreSQL schema 隔离 |
| 限流绑定 | 现有 `RateLimiter` 按租户 ID 隔离（已完成框架，接入即可） |

### 可观测性集成（待外部项目就绪）
> 直接引入外部 crate，替换现有 tracing/metrics

| 任务 | 说明 |
|------|------|
| 日志 | 替换 `tracing_subscriber::fmt` 为外部实现 |
| Metrics | 替换手写 Prometheus 为外部 exporter |
| 追踪 | 引入 OpenTelemetry 层 |

### V1.0.0 — GA 发布
> API 冻结，长期支持 (LTS)

| 里程碑 | 说明 |
|--------|------|
| API 稳定性承诺 | REST + Flow API 冻结，SemVer 严格遵循 |
| 性能基准 | Workflow 延迟 P50 < 100ms, P99 < 2s (不含 LLM 调用) |
| 安全审计 | 第三方安全审计 + OWASP Top 10 覆盖 |
| 文档完备 | 用户指南、API 参考、部署手册、最佳实践 |
| 测试覆盖率 | > 80% 行覆盖，> 90% 分支覆盖 |

---

## 持续优化 (Ongoing)

| 任务 | 频率 |
|------|------|
| 依赖升级（`cargo update` + `cargo audit`） | 每月 |
| `serde_yaml` → 替代品（等社区 fork 稳定后迁移） | P2 |
| 测试覆盖率提升 | 持续 |
| 性能基准（P50/P95/P99） | 每版本 |
| 文档同步（README、API docs、配置示例） | 每次 PR |

---

## 版本历史

| 版本 | 日期 | 测试 | 主题 |
|------|------|------|------|
| V0.1.0 | 2026-05-20 | 106 | 初始 MVP |
| V0.2.0 | 2026-05-26 | 172 | 生产就绪 |
| V0.3.0 | 2026-05-27 | 173 | Flow 生产化 |
| V0.3.1 | 2026-05-30 | 188 | 执行回放 + Code Review 修复 |
| V0.3.2 | 2026-05-30 | 194 | 审批节点 + 执行克隆 + 租户限流 |
| V0.3.3 | 2026-05-30 | 194 | 断点调试 |
