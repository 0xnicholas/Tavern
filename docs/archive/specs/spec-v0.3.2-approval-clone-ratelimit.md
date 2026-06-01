# Tavern V0.3.2: 审批节点 + 执行克隆 + 租户限流

> **状态：FROZEN** — 2026-05-30，review 1 轮，12 个问题全部修复  
> **依赖**：无外部依赖  
> **目标**：三个独立功能，可并行开发

## 1. 概述

V0.3.2 补齐管控层的三个关键能力：

| 功能 | 优先级 | 说明 |
|------|:---:|------|
| 步骤级审批 | P1 | 人工审批节点，强化现有 `wait_for_signal` |
| 执行克隆 | P2 | 复制历史执行输入重新触发 |
| 租户限流 | P1 | 滑动窗口计数器按租户 ID 隔离，为多租户集成做准备 |

### 1.1 设计原则

- **零破坏**：现有 API 和行为不变，188 测试继续通过
- **渐进增强**：审批复用 `SignalWaitStarted` / `SignalReceived` 事件，克隆复用 EventStore，限流用 `AtomicU64` + 定时重置
- **不引入新 crate**：全部在现有 crate 内实现

---

## 2. 步骤级审批（Human-in-the-Loop）

### 2.1 现状

Step 已支持 `wait_for_signal` 字段，步骤完成后引擎发射 `SignalWaitStarted` 事件并进入 `WaitingForSignal` 状态。外部通过 `POST /executions/:id/signal` 发送 `SignalReceived` 事件唤醒。

**缺失**：
- 没有「待审批列表」API，调用方无法查询「哪些步骤在等待审批」
- 审批需要附带决策元信息（审批人、意见、时间戳）
- 没有「驳回」语义——当前 signal 只能推进，无法终止工作流
- 没有审批超时后的默认行为配置

### 2.2 新增 API

#### `GET /approvals`
列出所有等待审批的步骤。查询 EventStore 中所有 `WaitingForSignal` 状态的实例，回放提取 context/step_output。空列表返回 `{"approvals": []}` + `200`。

**Response:**
```json
{
  "approvals": [
    {
      "execution_id": "uuid",
      "workflow_id": "content_pipeline",
      "step_id": "edit",
      "signal_name": "approve_edit",
      "context": { "draft": "...", "topic": "..." },
      "step_output": { "text": "..." },
      "waited_since": "2026-05-30T10:00:00Z",
      "timeout_at": null
    }
  ]
}
```

#### `POST /executions/:id/steps/:step_id/approve`
批准并继续执行。步骤不在 `WaitingForSignal` 状态返回 `409 Conflict`。

**Request:**
```json
{
  "comment": "looks good",
  "reviewer": "alice"
}
```

**Response:** `202 Accepted`，SSE 广播 `SignalReceived { action: "approve", ... }`。

#### `POST /executions/:id/steps/:step_id/reject`
驳回并终止工作流。步骤不在 `WaitingForSignal` 状态返回 `409 Conflict`。

**Request:**
```json
{
  "reason": "needs revision",
  "reviewer": "alice"
}
```

**Response:** `202 Accepted`，工作流以 `StepFailed` 终止，reason 为驳回原因。

#### 错误响应格式
所有新端点遵循现有 `ApiError` 格式：
```json
{ "error": "StepNotWaiting", "message": "step 'edit' is not waiting for approval" }
```

### 2.3 事件模型：扩展现有变体而非新增

`SignalReceived` 增加 `action` 和 `reviewer` 字段，避免事件变体膨胀：

```rust
pub enum WorkflowEvent {
    // ... existing variants unchanged ...

    SignalReceived {
        signal_name: String,
        payload: Value,
        received_at: DateTime<Utc>,
        // ── V0.3.2 新增 ──
        /// 审批动作：None = 传统 signal（向后兼容）
        action: Option<SignalAction>,
        /// 审批人标识
        reviewer: Option<String>,
    },
}

pub enum SignalAction {
    Approve,
    Reject,
}
```

- `action: None` → 传统 signal 行为（向后兼容，继续执行）
- `action: Some(Approve)` → 审批通过，继续执行
- `action: Some(Reject)` → 审批驳回，工作流失败

所有审批事件通过 SSE 广播（复用现有 `execution_events_stream_handler`）。

### 2.4 行为变更

| 场景 | 当前行为 | 新行为 |
|------|----------|--------|
| 发送 signal | 继续执行 | 不变（`POST /executions/:id/signal` 保留） |
| 发送 approve | — | 继续执行 + 记录 reviewer/action |
| 发送 reject | — | 工作流失败，reason = 驳回原因 |
| 审批超时 | 工作流失败 | 可配置：`fail`（默认）或 `reject` |

> `signal_timeout_action: approve` **不提供**，因为人工审批节点自动通过违背审批目的。

### 2.5 Step 配置扩展

```yaml
steps:
  - id: review
    agent_id: reviewer
    task: "审核并给出建议"
    wait_for_signal: "approve_review"
    signal_timeout: 86400           # 24h 超时
    signal_timeout_action: reject   # fail（默认）或 reject
```

### 2.6 实现文件

| 文件 | 动作 | 内容 |
|------|------|------|
| `crates/tavern-comp/src/event.rs` | 修改 | `SignalReceived` 增加 `action`/`reviewer` 字段 |
| `crates/tavern-comp/src/instance.rs` | 修改 | `apply()` 处理 `SignalAction::Reject` |
| `crates/tavern-comp/src/engine.rs` | 修改 | 审批超时 + `signal_timeout_action` 默认动作 |
| `crates/tavern-comp/src/workflow.rs` | 修改 | Step 新增 `signal_timeout_action` 字段 |
| `crates/tavern-comp/src/store.rs` | 确认 | `list_by_status` 已正确实现（SQLite / PG） |
| `crates/tavern-server/src/handlers.rs` | 修改 | 新增 `list_approvals` / `approve_step` / `reject_step` |
| `crates/tavern-server/src/router.rs` | 修改 | 新增 3 条路由 |

---

## 3. 执行克隆

### 3.1 目标

基于历史执行的输入参数，重新触发同一个 Workflow。仅支持**已完成**或**已失败**的执行。

### 3.2 API

#### `POST /executions/:id/clone`

**Response (202):**
```json
{
  "execution_id": "new-uuid",
  "cloned_from": "original-uuid",
  "workflow_id": "content_pipeline",
  "inputs": { "topic": "AI" }
}
```

### 3.3 边界条件

| 场景 | 行为 |
|------|------|
| 源执行不存在 | `404 InstanceNotFound` |
| 源执行正在运行/等待中 | `409 Conflict` + `"cannot clone running execution"` |
| 源执行所属 Workflow 已删除 | `404 WorkflowNotFound` |
| 源执行所属 Workflow 已更新 | 使用**当前版本** Workflow 定义执行 |
| 并发克隆 | 正常，每次生成新的 execution_id |

### 3.4 实现

在 `WorkflowEngine` 上暴露查询方法，避免 server 层直接解析事件：

```rust
impl WorkflowEngine {
    /// 从 EventStore 提取执行实例的输入参数和 workflow_id。
    pub async fn get_execution_inputs(
        &self,
        instance_id: &str,
    ) -> Result<(String, Value), CompError> {
        let events = self.store().read_stream(instance_id).await?;
        if events.is_empty() {
            return Err(CompError::InstanceNotFound {
                id: instance_id.to_string(),
            });
        }
        for event in &events {
            if let WorkflowEvent::InstanceCreated { workflow_id, inputs } = event {
                return Ok((workflow_id.clone(), inputs.clone()));
            }
        }
        Err(CompError::Internal("no InstanceCreated event".into()))
    }
}
```

`clone_execution_handler` 流程：
1. 调用 `engine.get_execution_inputs(id)` 获取 workflow_id + inputs
2. 检查执行状态（已完成/已失败才允许克隆）
3. 查找 Workflow 定义
4. 调用 `engine.start(&workflow, inputs)` 返回新 execution_id

### 3.5 实现文件

| 文件 | 动作 | 内容 |
|------|------|------|
| `crates/tavern-comp/src/engine.rs` | 修改 | 新增 `get_execution_inputs()` |
| `crates/tavern-server/src/handlers.rs` | 修改 | `clone_execution_handler` |
| `crates/tavern-server/src/router.rs` | 修改 | `POST /executions/:id/clone` |

---

## 4. 租户限流

### 4.1 目标

基于租户 ID 做速率限制，为后续多租户集成铺路。限流维度按认证中间件提取的 `tenant_id`（当前阶段映射自 API Key / JWT sub）。

### 4.2 算法

**滑动窗口计数器**（无新依赖，纯 `std`）：

```
tenant_id → AtomicU64 计数器 + 1s 定时重置
```

每秒每租户独立计数，超限返回 429。精度为秒级，满足 V0.3.2 需求。V0.4.0 分布式阶段可替换为 Redis token bucket。

### 4.3 架构

```
Request → Auth Middleware → RateLimit Middleware → Handler
                |                    |
         Extension<tenant_id>   查 HashMap<tenant_id, Counter>
                               超限 → 429 + Retry-After
```

### 4.4 配置

```toml
# tavern.toml
[rate_limit]
enabled = true
default_rps = 10       # 默认每秒请求数

[rate_limit.per_tenant]
admin = { rps = 100 }
user  = { rps = 5 }
```

> `per_tenant` 的 key 是**租户标识**（逻辑名），非 API Key 明文。租户 ID 由认证中间件注入 `Extension`。

### 4.5 行为

| 场景 | 行为 |
|------|------|
| 未超限 | 正常处理 |
| 超限 | `429 Too Many Requests` + `Retry-After: 1` header |
| 未认证请求 | 跳过限流（Auth 中间件先拦截） |
| 限流 `enabled = false` | 直接透传 |
| 未配置的租户 | 使用 `default_rps` |

### 4.6 实现文件

| 文件 | 动作 | 内容 |
|------|------|------|
| `crates/tavern-config/src/lib.rs` | 修改 | `RateLimitConfig` 结构体 |
| `crates/tavern-server/src/ratelimit.rs` | 新建 | 滑动窗口计数器 + axum 中间件 |
| `crates/tavern-server/src/router.rs` | 修改 | 注册限流中间件 |
| `crates/tavern-server/src/state.rs` | 修改 | AppState 新增 `RateLimiter` |

### 4.7 限流器实现概要

```rust
pub struct RateLimiter {
    enabled: bool,
    default_rps: u32,
    tenants: HashMap<String, u32>,
    counters: Arc<RwLock<HashMap<String, AtomicU64>>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        let limiter = Self { /* ... */ };
        // 启动 1s 定时重置任务
        let counters = limiter.counters.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                counters.read().await.values().for_each(|c| c.store(0, Relaxed));
            }
        });
        limiter
    }

    pub fn check(&self, tenant_id: &str) -> Result<(), RateLimitExceeded> { /* ... */ }
}
```

---

## 5. 测试计划

### 5.1 审批

| # | 测试 | 层级 |
|---|------|:---:|
| 1 | `POST /executions/:id/steps/:step_id/approve` 成功后继续执行，SSE 收到事件 | e2e |
| 2 | `POST /executions/:id/steps/:step_id/reject` 后工作流失败，reason 正确 | e2e |
| 3 | `GET /approvals` 列出所有待审批步骤（含空列表） | e2e |
| 4 | 向非等待状态的步骤发送 approve 返回 409 | unit |
| 5 | 向不存在的 execution 发送 approve 返回 404 | unit |
| 6 | 审批超时 + `signal_timeout_action: fail`（默认）工作流失败 | unit |
| 7 | 审批超时 + `signal_timeout_action: reject` 工作流失败 + reason | unit |
| 8 | `POST /executions/:id/signal`（传统 API）向后兼容 | unit |

### 5.2 克隆

| # | 测试 | 层级 |
|---|------|:---:|
| 9 | 克隆已完成执行，新执行正常完成 | e2e |
| 10 | 克隆不存在的执行返回 404 | unit |
| 11 | 克隆运行中的执行返回 409 | unit |
| 12 | 克隆已删除 Workflow 的执行返回 404 | unit |

### 5.3 限流

| # | 测试 | 层级 |
|---|------|:---:|
| 13 | 正常请求通过 | unit |
| 14 | 连续请求超限返回 429 + `Retry-After` header | unit |
| 15 | 不同租户限流独立 | unit |
| 16 | 配置 `enabled = false` 时透传 | unit |
| 17 | 未配置租户使用 `default_rps` | unit |

---

## 6. 版本目标

| 指标 | 目标 |
|------|------|
| 测试总数 | 188 → ~205 |
| 破坏性变更 | 0 |
| 新增依赖 | 0 |
| 预计工期 | 2-3 天 |

---

## 附录 A: Review 修订记录

| # | 级别 | 原问题 | 修订 |
|---|:--:|------|------|
| 2 | CRITICAL | `signal_timeout_action: approve` 语义矛盾 | 去掉 `approve`，只保留 `fail`（默认）和 `reject` |
| 3 | HIGH | `POST /executions/:id/approve` 路由二义性 | 改为 `POST /executions/:id/steps/:step_id/approve` |
| 4 | HIGH | 新增 `StepApproved`/`StepRejected` 事件变体冗余 | 扩展 `SignalReceived` 增加 `action`/`reviewer` 字段 |
| 9 | HIGH | `tower::RateLimit` 不适合多租户 | 改用 `AtomicU64` + 1s 定时重置的滑动窗口计数器 |
| 1 | MEDIUM | `GET /approvals` 每次全量回放成本 | 确认 `list_by_status` 已实现；接受当前成本（审批量不会很大） |
| 7 | MEDIUM | 克隆边界状态未定义 | 明确仅支持 completed/failed；running/waiting 返回 409 |
| 8 | MEDIUM | 事件解析散落在 server 层 | 在 `WorkflowEngine` 暴露 `get_execution_inputs()` |
| 10 | MEDIUM | `per_tenant` 的 key 是 API Key 明文 | 改为租户标识（逻辑名），由认证中间件注入 |
| 5 | LOW | 未定义非等待状态的错误响应 | 补充 409 + ApiError 格式说明 |
| 6 | LOW | `GET /approvals` 空结果未定义 | 明确返回 `{"approvals": []}` + 200 |
| 11 | LOW | 错误响应格式未明确 | 统一使用现有 `ApiError { error, message }` |
| 12 | LOW | SSE 未覆盖审批事件 | 明确审批事件通过现有 SSE 广播 |
