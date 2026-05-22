# Plan: Freeze spec-comp-v2-event-driven & Implement Event-Sourced Workflow Orchestrator

> Source spec: `docs/specs/spec-comp-v2-event-driven.md` (frozen at 1726 lines, 2026-05-21)
> Target crates: `tavern-comp`, `tavern-server`

---

## 0. 目标与约束

- **冻结 spec-comp-v2-event-driven.md**：不再修改规格文档本身，所有实现以该 spec 为准。
- **向后兼容**：V1 的 `WorkflowEngine::run()` 保持同步阻塞语义；YAML 配置格式零破坏；现有测试必须通过。
- **零外部依赖（默认）**：事件存储默认内存实现，不强制引入 Kafka/RabbitMQ/SQLite（SQLite 在 Phase 3 实现）。
- **单进程事件循环**：事件在 `tokio::sync::mpsc` 通道内流转。

---

## 1. 总体实施阶段

| 阶段 | 范围 | 核心交付物 | 预估文件数 |
|------|------|-----------|-----------|
| **Phase 1** | `tavern-comp` 引擎重构 | 事件溯源状态机、解释器循环、V1 兼容层 | 6 新 + 3 改 |
| **Phase 2** | `tavern-server` API 演进 | 异步启动、执行查询、信号、取消、审计端点 | 3 改 |
| **Phase 3** | 持久化存储（后续计划） | `SqliteEventStore`、快照、环境变量切换 | 1 新 |
| **Phase 4** | 检查点恢复（后续计划） | `ExecutionSupervisor`、跨进程恢复 | 1 新 |

**本计划覆盖 Phase 1 + Phase 2。** Phase 3/4 在 Phase 2 合并后启动。

---

## 2. Phase 1: `tavern-comp` 引擎重构（详细步骤）

### 2.0 前置：依赖更新

**文件**: `crates/tavern-comp/Cargo.toml`

- 新增依赖：`uuid = { version = "1", features = ["v4"] }`
- `async-trait = "0.1"` 从 `[dev-dependencies]` 移到 `[dependencies]`（`EventStore` trait 需要）
- `chrono` 已存在，`tokio` 已存在。

### 2.1 新增数据模型文件

#### `src/event.rs` — WorkflowEvent 定义

按 spec §4.1 实现 `WorkflowEvent` enum（`#[serde(tag = "type", rename_all = "snake_case")]`）：

- 生命周期：`InstanceCreated`, `InstanceStarted`
- 步骤推进：`StepScheduled`, `StepStarted`, `StepCompleted`, `StepFailed`, `StepRetryScheduled`
- 信号等待：`SignalWaitStarted`, `SignalReceived`
- 定时器：`TimerFired`
- 取消：`CancelRequested`
- 完成：`WorkflowCompleted`, `WorkflowFailed`

注意：spec 中 `StepCompleted` 的 `output_key` 字段、`StepFailed` 的 `will_retry` 字段、`SignalReceived` 的 `received_at` 字段必须保留。

#### `src/instance.rs` — InstanceState + 状态机

按 spec §4.2 实现：

- `InstanceState` struct（含 `id`, `workflow_id`, `status`, `context`, `step_results`, `completed_steps`, `running_steps`, `signal_blocked_steps`, `pending_timers`）
- `InstanceStatus` enum（`Pending`, `Running`, `WaitingForSignal { signal: String }`, `Sleeping { wake_at: DateTime<Utc> }`, `Completed`, `Failed`）
- `InstanceState::apply(&mut self, event: &WorkflowEvent)` 纯函数，无副作用，可安全重放
- **关键修正**：spec §4.2 `SignalReceived` 处理中 `signal_blocked_steps.retain(|s| s != signal_name)` 应改为按 `step_id` 移除（spec 的笔误已在代码示例中暗示：以 step_id 存入集合，应以 step_id 移除。实现时按 step_id 处理）。

#### `src/store.rs` — EventStore trait + MemoryEventStore

按 spec §5.1 实现：

- `EventStore` trait（`async_trait`）：`append`, `read_stream`, `list_by_status`, `save_snapshot`, `load_snapshot`
- `MemoryEventStore`：基于 `RwLock<HashMap<String, Vec<WorkflowEvent>>>` + `RwLock<HashMap<String, InstanceState>>`
- `list_by_status` 采用 spec 文字描述实现：遍历所有流重建状态后筛选（spec 代码示例写 `Ok(vec![])` 是占位，实际采用遍历重建）

### 2.2 新增执行基础设施

#### `src/executor.rs` — StepExecutor

按 spec §5.4 实现：

- `StepExecutor` struct：持有 `Arc<TavernHero>`, `mpsc::Sender<WorkflowEvent>`, `Arc<Semaphore>`
- `new(hero, tx, max_concurrency) -> Self`
- `submit(&self, step: Step, context: Value, attempt: u64)`：获取 semaphore permit 后 `tokio::spawn` 执行
- `execute_once()`：模板渲染 → `hero.execute()` → `tokio::time::timeout` → 结果包装为 `StepCompleted` 或 `StepFailed` 事件发回 channel
- **注意**：`StepFailed` 的 `will_retry` 固定为 `false`，重试由引擎事件驱动管理。

#### `src/timer.rs` — TimerRegistry

按 spec §5.5 实现：

- `TimerRegistry` struct：持有 `mpsc::Sender<WorkflowEvent>`
- `register(&self, timer_id: String, wake_at: DateTime<Utc>)`：每个定时器一个 `tokio::spawn`，`sleep` 后发送 `TimerFired`
- **注意**：若 `wake_at <= now` 则立即发送事件。
- **注意**：spec §5.5 定义为非 async，但 §5.2 调用方写 `.await`，实现时统一为 `async fn register(...)` 与调用方对齐。

### 2.3 重构 `src/engine.rs` — WorkflowEngine 变为解释器

**核心目标**：保留 `run()` 作为 `start() + await_completion()` 的兼容层；新增 `start()` 作为非阻塞入口。

实现内容：

- `WorkflowEngine` struct 新增字段：`store: Arc<dyn EventStore>`
- 构造函数策略（向后兼容）：
  - 保留 `pub fn new(hero: Arc<TavernHero>) -> Self`（内部默认创建 `MemoryEventStore`）
  - 新增 `pub fn with_store(hero: Arc<TavernHero>, store: Arc<dyn EventStore>) -> Self`
  - 这样现有调用点（`handlers.rs`、`main.rs` 测试）无需立即修改
- `start(&self, workflow, inputs) -> Result<ExecutionHandle, CompError>`：
  1. 生成 UUID 作为 `instance_id`
  2. `store.append(InstanceCreated)`
  3. 创建 `(signal_tx, signal_rx)` mpsc channel
  4. 创建 `(completion_tx, completion_rx)` oneshot channel
  5. `tokio::spawn` 解释器循环 `run_interpreter()`
  6. 返回 `ExecutionHandle`
- `run(&self, workflow, inputs)`：调用 `start()` 然后 `handle.await_completion()`
- `run_interpreter()` 私有方法：
  1. `rebuild_state()` 读取 Event Log 并重放
  2. `apply_and_persist(InstanceStarted)`
  3. 创建 `(internal_tx, internal_rx)` mpsc channel
  4. 实例化 `StepExecutor` 和 `TimerRegistry`
  5. 事件循环：`decide_next_action()` → match `Action` → `ScheduleSteps` / `WaitForEvent` / `Complete` / `Fail`
  6. `WaitForEvent` 时用 `tokio::select!` 等待 `internal_rx` 或 `signal_rx`
  7. 事件应用后处理副作用（信号等待检查、重试调度、定时器超时处理）
  8. 终态时通过 `completion_tx` 通知等待者
- `decide_next_action()` 纯函数：
  - 检查终态 → `Complete` / `Fail`
  - 检查 `WaitingForSignal` / `Sleeping` → `WaitForEvent`
  - 构建 DAG 入度表，减去 `completed_steps` 贡献
  - 找出就绪步骤（入度为 0 且未执行、未运行、未被信号阻塞）→ `ScheduleSteps`
  - 全部完成 → 渲染 `outputs` → `Complete`
  - 否则 → `WaitForEvent`
- `apply_and_persist()`：先 `store.append()`，再 `state.apply()`（事件溯源原则）
- `rebuild_state()`：读取 Event Log 逐条 `apply`
- `get_attempt()` / `get_retry_delay()` 辅助函数
- 保留 `validate()` 方法（V1 API，spec 未展示但需保留）

**关键行为约束**：
- `SignalWaitStarted` 在 `StepCompleted` 后由解释器主动生成（非 StepExecutor）。
- 信号超时定时器注册时 `timer_id = format!("signal_timeout_{}", step_id)`。
- 重试定时器注册时 `timer_id = format!("retry_{}_{}", step_id, attempt)`。
- `TimerFired(signal_timeout_*)` → 直接 `WorkflowFailed`。
- `TimerFired(retry_*)` → 解释器循环回到 `decide_next_action`，`pending_timers` 已移除，自动重新 `ScheduleSteps`。
- `Sleeping` 状态保留定义但 V2.0 不主动进入（重试等待通过 `WaitForEvent` + 定时器实现，状态保持 `Running`）。

### 2.4 新增 `src/handle.rs` — ExecutionHandle

将 spec §5.3 的 `ExecutionHandle` 单独成文件：

- `id`, `signal_tx`, `interpreter_handle`, `completion_rx`
- `id()` → `&str`
- `signal(name, payload) -> Result<(), CompError>`
- `await_completion() -> Result<WorkflowResult, CompError>`（**只能调用一次**）
- `query_state(store) -> Result<InstanceState, CompError>`
- `cancel() -> Result<(), CompError>`

### 2.5 修改现有文件

#### `src/workflow.rs`

- `Step` 新增可选字段（V2.0）：`wait_for_signal: Option<String>`, `signal_timeout: Option<u64>`（`#[serde(default)]`）
- `StepResult` 新增字段：`attempt: u64`（默认值 1；所有现有 struct 字面量构造需同步补该字段）
- `WorkflowResult` 新增字段：`outputs: Value`（默认空对象 `{}`；所有现有构造需同步补该字段）
- 更新 `Serialize`/`Deserialize` derive。
- **向后兼容陷阱**：`engine/tests.rs`、`workflow.rs` 测试、`engine.rs` 中大量 `Step { ... }`、`StepResult { ... }`、`WorkflowResult { ... }` 字面量会因新增字段编译失败，需批量补字段。

#### `src/error.rs`

新增 V2 变体（spec §8.1）：

- `InstanceNotFound { id: String }`
- `InstanceClosed { id: String }`
- `SignalRejected { id: String, signal: String }`
- `StoreError(String)`
- `Internal(String)`

#### `src/lib.rs`

新增模块导出：

```rust
pub mod event;
pub mod instance;
pub mod store;
pub mod executor;
pub mod timer;
pub mod handle;

pub use event::WorkflowEvent;
pub use instance::{InstanceState, InstanceStatus};
pub use store::{EventStore, MemoryEventStore};
pub use executor::StepExecutor;
pub use timer::TimerRegistry;
pub use handle::ExecutionHandle;
```

**注意**：保留所有 V1 导出不变。

### 2.6 测试策略

- `engine/tests.rs`（已有）中 `run()` 相关测试**必须继续通过**。
- 新增测试（可放在 `engine/tests.rs` 或各模块 `#[cfg(test)]` 中）：
  - `MemoryEventStore` 追加/读取/快照
  - `InstanceState::apply()` 全事件覆盖
  - `WorkflowEngine::start()` + `await_completion()` 等价于 `run()`
  - 信号等待流程：步骤完成 → `WaitingForSignal` → `signal()` → `Running` → 完成
  - 信号超时：注册超时 → `TimerFired` → `WorkflowFailed`
  - 重试流程：`StepFailed` → `StepRetryScheduled` → `TimerFired` → 重新调度 → 成功
  - 取消：`cancel()` → `CancelRequested` → `WorkflowFailed`
  - 现有测试 struct 字面量更新：`Step` 补 `wait_for_signal: None, signal_timeout: None`；`StepResult` 补 `attempt: 1`；`WorkflowResult` 补 `outputs: Value::Object(Default::default())`

---

## 3. Phase 2: `tavern-server` API 演进（详细步骤）

### 3.1 修改 `src/state.rs`

新增字段：

- `pub event_store: Arc<dyn tavern_comp::EventStore>`（或默认 `MemoryEventStore`）
- `pub execution_handles: Arc<RwLock<HashMap<String, tavern_comp::ExecutionHandle>>>`（可选，用于内存中发信号；若仅通过 EventStore 重建，则不需要）

**决策**：server 层保留 `execution_handles` 以支持向活跃实例发信号；实例状态查询通过 `EventStore::read_stream()` 重建，不依赖句柄。

### 3.2 修改 `src/router.rs`

新增路由（spec §7）：

```rust
.route("/workflows/:id/start", post(handlers::start_workflow_handler))
.route("/executions/:id", get(handlers::get_execution_handler))
.route("/executions/:id/events", get(handlers::get_execution_events_handler))
.route("/executions/:id/signal", post(handlers::signal_execution_handler))
.route("/executions/:id/cancel", post(handlers::cancel_execution_handler))
```

保留现有 `/workflows/:id/run` 不变（同步阻塞，V1 兼容）。

### 3.3 修改 `src/handlers.rs`

#### 新增错误映射（spec §8.2）

```rust
CompError::InstanceNotFound => 404
CompError::InstanceClosed => 409
CompError::SignalRejected => 409
CompError::StoreError => 503
CompError::Internal => 500
```

#### 新增 Handler

1. **`start_workflow_handler`** (`POST /workflows/:id/start`)
   - 查找 workflow
   - `engine.start(&workflow, inputs).await`
   - 将 `ExecutionHandle` 存入 `state.execution_handles`
   - 返回 202 + `{ "execution_id": "..." }`

2. **`get_execution_handler`** (`GET /executions/:id`)
   - 通过 `EventStore::read_stream(id)` 重建 `InstanceState`
   - `started_at` 字段需从 Event Log 中查找首个 `InstanceStarted` 事件提取（`InstanceState` 本身无该字段）
   - 返回 200 + JSON（spec §7.2 格式：execution_id, workflow_id, status, context, outputs, step_results, started_at）
   - 若 stream 为空 → 404 `InstanceNotFound`

3. **`get_execution_events_handler`** (`GET /executions/:id/events`)
   - `EventStore::read_stream(id)` 直接返回事件列表
   - 返回 200 + JSON Array

4. **`signal_execution_handler`** (`POST /executions/:id/signal`)
   - 请求体：`{ "signal_name": "...", "payload": { ... } }`
   - 从 `state.execution_handles` 查找句柄
   - 调用 `handle.signal(name, payload).await`
   - 返回 202
   - 若句柄不存在：尝试通过 `EventStore` 检查实例是否存在 → 存在则返回 409（实例已关闭/非活跃），不存在则 404

5. **`cancel_execution_handler`** (`POST /executions/:id/cancel`)
   - 从 `state.execution_handles` 查找句柄
   - 调用 `handle.cancel().await`
   - 返回 202
   - 若句柄不存在：同 signal 的降级逻辑

#### 修改 `run_workflow_handler`

保持现有逻辑不变（调用 `engine.run()`），但注意 `WorkflowResult` 现在包含 `outputs` 字段，响应组装逻辑可能需要调整以直接透传 `result.outputs`。

---

## 4. 验收标准（Definition of Done）

### Phase 1 验收

- [ ] `cargo check --workspace` 通过，无警告
- [ ] `cargo test --workspace` 通过，现有 `tavern-comp` 测试全部通过
- [ ] `cargo clippy --workspace` 通过
- [ ] 新增模块有单元测试覆盖核心逻辑（事件应用、状态重建、Action 决策、StepExecutor 超时）
- [ ] V1 的 `run()` 行为与重构前一致（输出格式、错误类型、超时重试语义）
- [ ] `start()` 返回的 handle 可通过 `await_completion()` 获得与 `run()` 相同的结果

### Phase 2 验收

- [ ] `cargo test --workspace` 通过
- [ ] `/workflows/:id/run` 保持同步 200 响应（V1 兼容）
- [ ] `/workflows/:id/start` 返回 202 + execution_id
- [ ] `/executions/:id` 可查询 Running/Completed/Failed 状态
- [ ] `/executions/:id/signal` 可向 WaitingForSignal 实例发送信号并恢复执行
- [ ] `/executions/:id/cancel` 可向运行中实例发送取消请求
- [ ] `/executions/:id/events` 返回完整审计日志
- [ ] 所有新端点 HTTP 状态码与 spec §8.2 映射一致

---

## 5. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| `InstanceState::apply()` 与 spec 代码示例不一致 | 状态重建错误 | 严格以 spec 事件应用规则（§10.2 表格）为准，代码示例仅作参考；逐条对照表格实现 |
| `signal_blocked_steps` 的 key 语义模糊 | 信号恢复后 DAG 推进失败 | spec 中集合存 `step_id`（非 `signal_name`），`SignalReceived` 的 `retain` 逻辑按 step_id 移除 |
| `WorkflowResult.outputs` 新增字段破坏现有序列化 | Server 响应格式变化 | `outputs` 默认空对象 `{}`，`run()` 内部填充后返回；server handler 直接透传 |
| `Step`/`StepResult`/`WorkflowResult` 新增字段导致现有测试编译失败 | `cargo test` 无法编译 | 批量更新所有 struct 字面量构造，补全新增字段 |
| 事件循环中 `tokio::select!` 通道关闭导致 panic | 实例异常终止 | `else` 分支返回 `CompError::Internal`，通过 `completion_tx` 通知等待者 |
| Phase 2 的 `execution_handles` 内存泄漏 | 终态实例句柄不释放 | 解释器循环结束时从 `state.execution_handles` 移除；或仅通过 EventStore 重建发信号（更复杂，Phase 2 先保留句柄） |

---

## 6. 文件变更清单

### 新增文件（Phase 1）

```
crates/tavern-comp/src/event.rs       # WorkflowEvent
crates/tavern-comp/src/instance.rs    # InstanceState, InstanceStatus
crates/tavern-comp/src/store.rs       # EventStore, MemoryEventStore
crates/tavern-comp/src/executor.rs    # StepExecutor
crates/tavern-comp/src/timer.rs       # TimerRegistry
crates/tavern-comp/src/handle.rs      # ExecutionHandle
```

### 修改文件（Phase 1）

```
crates/tavern-comp/Cargo.toml         # +uuid, +async-trait
crates/tavern-comp/src/lib.rs         # 新增模块导出
crates/tavern-comp/src/workflow.rs    # Step/StepResult/WorkflowResult 新增字段
crates/tavern-comp/src/error.rs       # 新增 V2 错误变体
crates/tavern-comp/src/engine.rs      # 重构为解释器 + V1 兼容层
crates/tavern-comp/src/engine/tests.rs # 补充 V2 测试
```

### 修改文件（Phase 2）

```
crates/tavern-server/src/router.rs    # 新增执行管理路由
crates/tavern-server/src/handlers.rs  # 新增 handler + 错误映射
crates/tavern-server/src/state.rs     # 新增 event_store, execution_handles
crates/tavern-server/src/main.rs      # AppState 构造补新字段（测试模块中）
```

**注意**：`tavern-server/Cargo.toml` 无需新增依赖（所需类型通过 `tavern-comp` 导出）

---

## 7. 备注

- spec §11.3 的崩溃恢复（Phase 4）和 §15 的 SQLite 持久化（Phase 3）**不在本计划范围内**，本计划仅实现 V2.0 核心事件循环 + Server API。
- spec §16 Event Schema 演进策略中的 `StepCompletedV2` 等未来变体**不在本计划范围内**，当前只实现 V2.0 的基础事件集。
- 本计划假设 TavernHero 的 `execute()` 接口不变（spec §12 确认）。
