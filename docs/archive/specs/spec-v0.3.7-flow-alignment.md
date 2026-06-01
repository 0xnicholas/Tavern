# Tavern V0.3.7: Flow 管控对齐

> **状态：FROZEN** — 2026-05-30  
> **依赖**：复用 V0.3.5 webhook 基础设施、V0.3.3 断点模式  
> **目标**：Flow 引擎支持断点调试和 Webhook 回调

## 1. 概述

### 1.1 问题

V0.3.2 ~ V0.3.6 为 Workflow 引擎（tavern-comp）新增了审批、断点、Webhook、批量、定时等管控功能。Flow 引擎（tavern-flow）仍只有基础执行能力。

### 1.2 范围

- **断点**：Flow 方法级断点，执行前暂停，API 查询/恢复
- **Webhook**：Flow 完成时回调，复用 V0.3.5 的 `send_webhook`
- **不做**：审批（Flow 无 signal 概念）、批量、定时

---

## 2. 断点

### 2.1 设计

Flow 方法元数据新增 `breakpoint` 标记。引擎在执行方法前检查：若为断点方法，暂停并写入事件。复用 Workflow 的 `BreakpointHit` 事件和 `GET /breakpoints` API。

```rust
// tavern-flow 宏扩展
#[start(breakpoint)]         // 启动方法设断点
async fn step_a(&mut self) -> Result<String, FlowError> { ... }

#[listen("step_a", breakpoint)]  // 监听方法设断点
async fn step_b(&mut self, data: String) -> Result<String, FlowError> { ... }
```

### 2.2 实现

- `MethodInfo` 新增 `breakpoint: bool`
- `FlowEngine::execute_inner` 和 `execute_inner_parallel` 在 dispatch 前检查
- 命中时发射 `WorkflowEvent::BreakpointHit`（复用 comp 的事件类型）
- 暂停逻辑：将 Flow 状态标记为等待，通过 EventStore 持久化
- 恢复：外部发送 signal 匹配 `__breakpoint__<method_name>`

> 注：Flow 目前没有 EventStore 写入路径（start_async 里持久化未完全实现）。断点依赖 EventStore，需要先确保 Flow 执行事件写入 EventStore。

---

## 3. Webhook

### 3.1 设计

Flow 完成时触发 webhook 回调，复用 V0.3.5 的 `send_webhook` 函数。配置通过 FlowRegistry 或 Flow 元数据传入。

```rust
// Flow 元数据扩展
#[derive(Flow, WebhookConfig)]  // 或通过 FlowEngine::with_webhook()
struct MyFlow { ... }
```

### 3.2 实现

- `FlowEngine` 新增 `webhook: Option<WebhookConfig>` 字段
- `with_webhook(config)` 构建器方法
- 在 `start_async` / `execute` 完成路径中触发回调
- 复用 `tavern_comp::send_webhook`（改为 `pub`）

---

## 4. API

### `GET /breakpoints`（已有，自动覆盖 Flow 断点）

`BreakpointHit` 事件通过 EventStore 持久化后，现有 `list_breakpoints_handler` 自动包含 Flow 断点。无需新增端点。

### 恢复 Flow 断点

```
POST /executions/:id/signal
{ "signal_name": "__breakpoint__<method_name>" }
```

---

## 5. 分解

### Phase 1: Flow EventStore 持久化（前置任务）

Flow 引擎当前仅在 `start_async` 中写 `FlowStarted` 事件。需要补齐：
- `execute_inner` 中每个方法完成后写 `MethodCompleted` 事件
- `execute_inner_parallel` 同理
- `BreakpointHit` 事件写入（Phase 2 需要）
- 完成后写 `FlowCompleted` / `FlowFailed`

### Phase 2: Flow 断点

- 宏扩展：`#[start(breakpoint)]` / `#[listen(name, breakpoint)]`
- `MethodInfo.breakpoint: bool`
- 引擎检查 + `BreakpointHit` 写入 EventStore
- 信号恢复（复用 Workflow 的信号机制）

### Phase 3: Flow Webhook

- `FlowEngine::with_webhook(config)`
- 完成路径触发 `send_webhook`
- `send_webhook` 改为 `pub(crate)` 或移到共享位置

## 6. 实现文件

| 文件 | 动作 | Phase |
|------|------|:--:|
| `crates/tavern-flow/src/lib.rs` | 修改 | 1,2,3 |
| `crates/tavern-flow-macros/src/lib.rs` | 修改 | 2 |
| `crates/tavern-comp/src/engine.rs` | 修改 | 3 |

---

## 7. 测试

| # | 测试 | 层级 |
|---|------|:--:|
| 1 | 断点方法暂停执行，信号恢复后继续 | unit |
| 2 | 非断点方法正常执行 | unit |
| 3 | Flow webhook 完成时触发，payload 正确 | unit |
