# Tavern 版本说明

本文档记录 Tavern 各版本的变更历史、功能范围与已知限制。

---

## 版本号规则

Tavern 采用语义化版本控制（SemVer）：

```
主版本号.次版本号.修订号
```

- **主版本号**：重大架构变更或不兼容 API 修改
- **次版本号**：新增功能，向下兼容
- **修订号**：Bug 修复或性能优化

---

## V0.1.0 — 初始版本（Current）

> 状态：开发中（开发分支）  
> 目标：最小可用产品（MVP），验证核心架构

### 新增功能

#### Agent 系统（`tavern-hero`）
- YAML 声明式 Agent 配置（`id`, `name`, `model`, `instructions`, `skills`, `constraints`, `memory`）
- Agent 注册表：内存 HashMap，支持防重复注册
- 批量目录加载：自动识别 `.yaml` / `.yml` 文件
- 配置校验：ID 格式、`temperature` 范围、`max_context_turns` 上限
- 直接向 Agent 提交任务执行

#### Workflow 引擎（`tavern-comp`）
- YAML 声明式 Workflow 配置（`steps`, `inputs`, `outputs`, `process`, `planning`）
- **Sequential 模式**：DAG 拓扑排序执行，支持并行步骤
- **Hierarchical 模式**：Manager Agent 动态委派任务
- **Planning 模式**：AgentPlanner 生成执行计划并注入上下文
- 事件溯源架构：`WorkflowEvent` + `EventStore` + `InstanceState` 重放
- 步骤级特性：超时控制、失败重试（`retries` + `retry_delay`）、信号等待（`wait_for_signal`）
- 模板渲染：`{{var}}` 插值，支持嵌套对象和 Jinja filter
- 并发控制：基于 `tokio::sync::Semaphore` 的 `max_concurrency`

#### Runtime 适配（`tavern-adapters`）
- `Runtime` trait 抽象：与具体 LLM 后端解耦
- `PandariaRuntime`：HTTP 适配，Session 生命周期管理，HMAC-SHA256 Token 自动生成
- `MockRuntime`：同步闭包，用于测试和本地开发

#### HTTP 服务（`tavern-server`）
- **Agent API**：列表、查询、直接执行
- **Workflow API**：列表、查询、创建、删除、同步执行（`run`）、异步执行（`start`）
- **Execution API**：查询状态、查询事件流、发送信号、取消执行
- **管理 API**：Workflow 热重载（`notify` 监听 + debounce）
- **Metrics API**：Prometheus 格式 counter（executions / failures / duration）
- 统一错误响应：`ApiError` + 语义化 HTTP 状态码

#### 示例配置
- `configs/agents/`：researcher、writer、editor
- `configs/workflows/`：content_pipeline（研究 → 写作 → 编辑）

### 架构亮点

```
tavern-server  →  tavern-comp  →  tavern-hero  →  tavern-adapters  →  tavern-core
     HTTP           Workflow        Agent           Runtime            Types
```

- 单向依赖，无循环引用
- 所有共享类型定义在 `tavern-core`
- 所有 trait 抽象定义在 `tavern-core`，实现分布在各 crate

### 测试覆盖

| Crate | 测试数 | 说明 |
|-------|--------|------|
| `tavern-core` | 12 | 配置反序列化、默认值、新类型 |
| `tavern-adapters` | 7 | Runtime 集成、Token 生成 |
| `tavern-hero` | 11 | 注册表、加载器、并发安全 |
| `tavern-comp` | 59 | 引擎集成：DAG、Hierarchical、Planning、重试、超时、并行 |
| `tavern-server` | 17 | 全链路 HTTP API 测试 |
| **合计** | **106** | |

### 已知限制

以下限制在 V0.1.0 中属于**预期内缺口**，将在后续版本迭代：

| 限制 | 说明 | 计划版本 |
|------|------|----------|
| 无持久化存储 | `EventStore` 仅内存实现，重启后执行历史丢失 | V0.2 |
| Agent 无热重载 | 新增 Agent 需重启服务加载 | V0.2 |
| 无认证/授权 | 所有 API 公开可访问 | V0.2 |
| Pandaria 轮询延迟 | `send_message` 采用 30s 间隔轮询，非 SSE | V0.2 |
| 无 OpenAPI 文档 | 仅有外部 Pandaria 的 openapi.yaml | V0.2 |
| 单进程架构 | 无分布式/高可用支持 | V0.4+ |
| 无 Web UI | 纯后端 HTTP 服务 | 待定 |

### 发布前检查清单

- [x] 修复 `test_pandaria_success` 测试失败（`tavern-adapters`）— 补充 GET `/messages` 轮询 mock
- [x] 清理 compiler warning（unused imports / dead code / needless borrow）
- [x] 清理 `derivable_impls` warning（`Process`, `InstanceStatus`）
- [x] `cargo test --workspace` 全绿（106/106 passed）
- [x] `cargo fmt` 无变更
- [x] 确认版本号统一为 `0.1.0`

### 运行方式

```bash
# 1. 检查
cargo check --workspace
cargo test --workspace
cargo clippy --workspace

# 2. 启动服务（连接 Pandaria）
RUNTIME_URL=http://localhost:8080 RUST_LOG=info cargo run -p tavern-server

# 3. 或使用 Mock Runtime 本地测试
cargo test --workspace
```

---

## V0.2.0 — 生产就绪（Planned）

> 目标：支持持久化、热重载、基础安全，达到可用状态

### 计划功能

- **持久化 EventStore**
  - PostgreSQL / SQLite 后端实现
  - 执行历史跨重启保留
  - 快照（snapshot）机制优化重放性能

- **Agent 热重载**
  - 复用 `notify` 机制监听 `configs/agents/`
  - 与 Workflow 热重载行为一致

- **API 认证**
  - API Key / Bearer Token middleware
  - 可配置的白名单/黑名单

- **实时事件流**
  - SSE (`/executions/:id/events/stream`)
  - 替代轮询，降低延迟

- **OpenAPI 自动生成**
  - 集成 `utoipa`，从 axum handler 生成 Swagger UI

- **配置增强**
  - 环境变量统一前缀 `TAVERN_`
  - TOML/JSON 配置文件支持

---

## V0.3.0 — 可观测性（Planned）

> 目标：完善的监控、调试和运维能力

### 计划功能

- **执行追踪**
  - OpenTelemetry / Jaeger 分布式 tracing
  - 步骤级耗时分解

- **日志结构化**
  - JSON 格式日志输出
  - 可配置的日志轮转

- **执行回放与调试**
  - 从 EventStore 完整回放任意执行实例
  - 步骤级输入输出审计

- **限流与配额**
  - 租户级并发执行数限制
  - 每日调用次数配额

---

## V0.4.0 — 分布式（Planned）

> 目标：水平扩展、高可用

### 计划功能

- **执行器集群**
  - Redis/RabbitMQ 任务队列
  - 多实例竞争消费

- **状态共享**
  - 分布式锁（Redis Redlock）
  - 状态一致性保障

- **Workflow 版本管理**
  - 历史版本保留与回滚
  - 执行时锁定 Workflow 版本

---

## 历史归档

| 版本 | 发布日期 | 说明 |
|------|----------|------|
| V0.1.0 | — | 初始 MVP，核心架构验证 |
