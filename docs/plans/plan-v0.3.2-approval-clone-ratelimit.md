# V0.3.2 Implementation Plan

> **For agentic workers:** 三个功能独立，可并行开发。使用 superpowers:subagent-driven-development 分派。
>
> **Review:** 1 轮，10 个问题全部修复

**Goal:** 审批节点（人工介入）、执行克隆（历史复用）、租户限流（速率控制）。

**Architecture:** 审批扩展现有 signal 事件模型；克隆在 engine 层暴露查询方法（含状态）+ server 层 handler；限流为独立中间件。

**Spec:** `docs/specs/spec-v0.3.2-approval-clone-ratelimit.md`（FROZEN）

**Tech Stack:** Rust, axum, serde_json, chrono, thiserror — 零新增依赖

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/tavern-comp/src/event.rs` | Modify | `SignalReceived` 增加 `action`/`reviewer` 字段 + `SignalAction` 枚举 |
| `crates/tavern-comp/src/lib.rs` | Modify | 导出 `SignalAction`, `SignalTimeoutAction` |
| `crates/tavern-comp/src/instance.rs` | Modify | `apply()` 处理 `SignalAction::Reject` → WorkflowFailed |
| `crates/tavern-comp/src/engine.rs` | Modify | 审批超时默认动作 + `get_execution_info()`（含 inputs + 状态） |
| `crates/tavern-comp/src/workflow.rs` | Modify | `Step` 新增 `signal_timeout_action` 字段 |
| `crates/tavern-config/src/lib.rs` | Modify | `RateLimitConfig` 结构体 |
| `crates/tavern-server/src/lib.rs` | Modify | 声明 `pub mod ratelimit;` |
| `crates/tavern-server/src/ratelimit.rs` | Create | `TenantId` 类型 + 滑动窗口计数器 + axum 中间件 |
| `crates/tavern-server/src/handlers.rs` | Modify | 审批 list/approve/reject + 克隆 |
| `crates/tavern-server/src/router.rs` | Modify | 新增 4 条路由 |
| `crates/tavern-server/src/state.rs` | Modify | AppState 新增 `RateLimiter` |

---

## Track A: 步骤级审批

### Task A1: 扩展事件模型

**Files:** `crates/tavern-comp/src/event.rs`

- [ ] **Step 1: 新增 `SignalAction` 枚举**

```rust
/// V0.3.2: 审批动作类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalAction {
    Approve,
    Reject,
}
```

- [ ] **Step 2: `SignalReceived` 变体增加字段**

```rust
SignalReceived {
    signal_name: String,
    payload: Value,
    received_at: DateTime<Utc>,
    // ── V0.3.2 ──
    action: Option<SignalAction>,
    reviewer: Option<String>,
},
```

- [ ] **Step 3: 全局更新所有 `SignalReceived` 构造和模式匹配**

运行 `grep -rn "SignalReceived" crates/` 列出所有位置，逐处更新：

**构造点**（需要增加新字段）：
| 文件 | 位置 | 操作 |
|------|------|------|
| `engine.rs` | `send_signal` 路径（约 ~L600） | 增加 `action: None, reviewer: None` |
| `instance.rs` | 测试 `test_apply_signal_received_*` | 增加 `action: None, reviewer: None` |
| `handle.rs` | `signal()` 方法 | 增加 `action: None, reviewer: None` |

**模式匹配**（需加 `..` 忽略新字段）：
| 文件 | 位置 | 操作 |
|------|------|------|
| `instance.rs` | `apply()` 方法 | 增加 `..` |
| `engine.rs` | `run_interpreter_loop` 中 `internal_rx.recv()` 匹配 | 增加 `..` |
| `engine.rs` | `run_interpreter_hierarchical` 中 | 增加 `..` |

- [ ] **Step 4: 序列化兼容验证**

新增 `Option` 字段 + `#[serde(default)]` 语义：旧事件反序列化为 `None`。写测试验证旧格式 JSON（无 `action`/`reviewer`）可正常反序列化。

- [ ] **Step 5: 导出到 lib.rs**

在 `crates/tavern-comp/src/lib.rs` 增加：
```rust
pub use event::SignalAction;
```

---

### Task A2: 实例状态处理 Reject

**Files:** `crates/tavern-comp/src/instance.rs`

- [ ] **Step 1: `apply()` 中处理 `SignalReceived`**

当 `action == Some(SignalAction::Reject)` 时：
1. 实例状态转为 `InstanceStatus::Failed`
2. 当前等待信号的 step 标记为 `StepStatus::Failed`
3. `step_result.error` 设为 `"rejected by {reviewer}: {reason}"`（reason 从 `payload["reason"]` 提取）
4. 写入 `failed_at` 时间戳

Approved 行为和传统 signal 一致（继续执行），仅多记录 reviewer。

- [ ] **Step 2: 测试**

```rust
#[test]
fn test_apply_signal_received_reject_fails_workflow() {
    let mut state = InstanceState { status: InstanceStatus::WaitingForSignal { signal: "approve".into() }, .. };
    let event = WorkflowEvent::SignalReceived {
        signal_name: "approve".into(),
        payload: json!({"reason": "needs work"}),
        received_at: Utc::now(),
        action: Some(SignalAction::Reject),
        reviewer: Some("alice".into()),
    };
    state.apply(&event).unwrap();
    assert!(matches!(state.status, InstanceStatus::Failed));
    let step_result = state.step_results.values().find(|r| matches!(r.status, StepStatus::Failed));
    assert!(step_result.unwrap().error.as_ref().unwrap().contains("alice"));
    assert!(step_result.unwrap().error.as_ref().unwrap().contains("needs work"));
}

#[test]
fn test_apply_signal_received_approve_continues() {
    // Approve action: behavior identical to legacy signal — step completes, workflow continues
    // Verify status stays Running, step marked Completed
}
```

---

### Task A3: Step 配置扩展

**Files:** `crates/tavern-comp/src/workflow.rs`

- [ ] **Step 1: `SignalTimeoutAction` 枚举**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalTimeoutAction {
    Fail,
    Reject,
}
```

- [ ] **Step 2: `Step` 结构体新增字段**

```rust
/// 审批超时后默认动作：fail（默认）或 reject
#[serde(default)]
pub signal_timeout_action: Option<SignalTimeoutAction>,
```

- [ ] **Step 3: 导出**

在 `tavern-comp/src/lib.rs` 增加 `pub use workflow::SignalTimeoutAction;`。

- [ ] **Step 4: 反序列化测试**

```rust
#[test]
fn test_step_signal_timeout_action_reject() {
    let yaml = "id: s1\nagent_id: a1\ntask: review\nwait_for_signal: approve\nsignal_timeout: 3600\nsignal_timeout_action: reject";
    let step: Step = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(step.signal_timeout_action, Some(SignalTimeoutAction::Reject));
}

#[test]
fn test_step_signal_timeout_action_default_none() {
    let step: Step = serde_yaml::from_str("id: s1\nagent_id: a1\ntask: x").unwrap();
    assert_eq!(step.signal_timeout_action, None);
}
```

---

### Task A4: 引擎超时默认动作

**Files:** `crates/tavern-comp/src/engine.rs`

- [ ] **Step 1: 定位信号超时处理逻辑**

目标代码在 `run_interpreter_loop` 方法内约 ~L610，匹配 `WorkflowEvent::TimerFired { timer_id }`。

当前逻辑（简化）：
```rust
if timer_id.starts_with("signal_timeout_") {
    // 直接 WorkflowFailed
}
```

- [ ] **Step 2: 改造为读取 `signal_timeout_action`**

```rust
if timer_id.starts_with("signal_timeout_") {
    let step_id = timer_id.strip_prefix("signal_timeout_").unwrap();
    let action = workflow.steps.iter()
        .find(|s| s.id == step_id)
        .and_then(|s| s.signal_timeout_action.clone());

    match action {
        Some(SignalTimeoutAction::Reject) => {
            // 发射 SignalReceived { action: Reject, reviewer: "system" }
            let reject_event = WorkflowEvent::SignalReceived {
                signal_name: format!("signal_timeout_{}", step_id),
                payload: json!({"reason": "approval timed out"}),
                received_at: Utc::now(),
                action: Some(SignalAction::Reject),
                reviewer: Some("system".to_string()),
            };
            self.apply_and_persist(&instance_id, reject_event, &mut state).await?;
        }
        _ => {
            // 默认 Fail 行为（与现在一致）
            let fail_event = WorkflowEvent::WorkflowFailed { ... };
            self.apply_and_persist(&instance_id, fail_event, &mut state).await?;
            break Err(...);
        }
    }
}
```

- [ ] **Step 3: 测试**

```rust
#[tokio::test]
async fn test_signal_timeout_action_reject() {
    // Workflow: step with wait_for_signal, signal_timeout: 1, signal_timeout_action: reject
    // engine.run() → await timeout
    // assert: state == Failed, step_result.error contains "approval timed out"
}

#[tokio::test]
async fn test_signal_timeout_action_fail_default() {
    // Workflow: step with wait_for_signal, signal_timeout: 1, no signal_timeout_action
    // engine.run() → await timeout
    // assert: state == Failed (existing behavior unchanged)
}
```

---

### Task A5: Server 层审批 API

**Files:** `crates/tavern-server/src/handlers.rs`, `router.rs`

- [ ] **Step 1: `GET /approvals` — `list_approvals_handler`**

1. 调用 `event_store.list_by_status(...)` 查询所有 WaitingForSignal 实例
   - **注意**：需确认 SQLite `list_by_status` 实现。`WaitingForSignal { signal }` 若为精确匹配，需传空字符串查询所有，或单独实现 `list_by_status_wildcard`。实现时先查看 `store.rs` 的 `list_by_status` SQL 语句。
2. 对每个 instance_id，回放事件流提取：
   - `InstanceCreated` → workflow_id
   - `SignalWaitStarted` → step_id, signal_name, waited_since
   - 当前 context 和最后一个 step_output
3. 返回 `Json(ApprovalListResponse { approvals })`，空列表返回 `{"approvals": []}` + 200

- [ ] **Step 2: `POST /executions/:id/steps/:step_id/approve`**

1. 读取 `ApproveRequest { comment, reviewer }`
2. 构造 `SignalReceived { signal_name, action: Some(Approve), reviewer, payload: { "comment": "..." } }`
3. 查找 `execution_handles` 中的 signal_tx 并发送
4. **若 `execution_handles` 中无该实例**（进程重启后恢复的实例）：
   - 从 `event_store` 读取确认实例存在
   - 若实例状态为 WaitingForSignal → 直接将事件 append 到 EventStore，由解释器循环接收（解释器循环同时监听 `internal_rx` 和 `signal_rx`，但恢复后的实例 signal_rx 来自 recovery 创建的 channel）
   - 若实例不在 WaitingForSignal → 409
5. 若 step 不在 WaitingForSignal → 409

- [ ] **Step 3: `POST /executions/:id/steps/:step_id/reject`**

同上，`action: Some(Reject)`，payload 含 `reason`。

- [ ] **Step 4: 错误响应**

```rust
// step 不在等待
(StatusCode::CONFLICT, ApiError::new("StepNotWaiting", "step 'xxx' is not waiting for approval"))
// 实例不存在
(StatusCode::NOT_FOUND, ApiError::new("InstanceNotFound", "..."))
```

- [ ] **Step 5: 路由注册**

```rust
.route("/approvals", get(handlers::list_approvals_handler))
.route("/executions/:id/steps/:step_id/approve", post(handlers::approve_step_handler))
.route("/executions/:id/steps/:step_id/reject", post(handlers::reject_step_handler))
```

- [ ] **Step 6: 集成测试**

`POST /workflows/signal_flow/start` → 等待 WaitingForSignal → `POST /executions/:id/steps/s1/approve` → 轮询确认 completed。

---

## Track B: 执行克隆

### Task B1: Engine 层查询方法（合并 inputs + 状态）

**Files:** `crates/tavern-comp/src/engine.rs`

- [ ] **Step 1: 实现 `get_execution_info()`**

一次 EventStore 读取，同时返回 inputs 和当前状态，避免 handler 层二次读取：

```rust
pub struct ExecutionInfo {
    pub workflow_id: String,
    pub inputs: Value,
    pub status: InstanceStatus,
}

pub async fn get_execution_info(
    &self,
    instance_id: &str,
) -> Result<ExecutionInfo, CompError> {
    let events = self.store.read_stream(instance_id).await?;
    if events.is_empty() {
        return Err(CompError::InstanceNotFound { id: instance_id.to_string() });
    }

    let mut workflow_id = None;
    let mut inputs = None;
    let mut state = InstanceState { id: instance_id.to_string(), ..Default::default() };

    for event in &events {
        match event {
            WorkflowEvent::InstanceCreated { workflow_id: wid, inputs: inp } => {
                workflow_id = Some(wid.clone());
                inputs = Some(inp.clone());
            }
            _ => {}
        }
        let _ = state.apply(event);
    }

    let workflow_id = workflow_id.ok_or(CompError::Internal("no InstanceCreated event".into()))?;
    let inputs = inputs.unwrap_or(Value::Null);

    Ok(ExecutionInfo { workflow_id, inputs, status: state.status })
}
```

- [ ] **Step 2: 单元测试**

```rust
#[tokio::test]
async fn test_get_execution_info_success() {
    // start workflow → engine.get_execution_info(id)
    // assert: workflow_id correct, inputs correct, status == Completed
}
#[tokio::test]
async fn test_get_execution_info_not_found() {
    // engine.get_execution_info("nonexistent") → Err(InstanceNotFound)
}
```

---

### Task B2: Server 层克隆 API

**Files:** `crates/tavern-server/src/handlers.rs`, `router.rs`

- [ ] **Step 1: `POST /executions/:id/clone` — `clone_execution_handler`**

1. 调用 `engine.get_execution_info(id)` → `ExecutionInfo { workflow_id, inputs, status }`
2. 检查 status：仅 `Completed` 或 `Failed` 允许克隆；`Running` / `WaitingForSignal` / `Pending` 返回 409
3. 查找 Workflow 定义（不存在 → 404）
4. 调用 `engine.start(&workflow, inputs)` → 新 execution_id
5. 返回 `202` + `{ execution_id, cloned_from, workflow_id, inputs }`

- [ ] **Step 2: 路由注册**

```rust
.route("/executions/:id/clone", post(handlers::clone_execution_handler))
```

- [ ] **Step 3: 集成测试**

运行 content_pipeline → 确认 completed → 克隆 → 验证新 execution 有相同 inputs 且独立执行完成。

---

## Track C: 租户限流

### Task C1: 配置定义

**Files:** `crates/tavern-config/src/lib.rs`

- [ ] **Step 1: `RateLimitConfig` 结构体**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_rps")]
    pub default_rps: u32,
    #[serde(default)]
    pub per_tenant: HashMap<String, TenantRateLimit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantRateLimit {
    pub rps: u32,
}

fn default_rps() -> u32 { 10 }
```

- [ ] **Step 2: 加入 `TavernConfig`**

```rust
#[serde(default)]
pub rate_limit: RateLimitConfig,
```

- [ ] **Step 3: 测试**

```rust
#[test]
fn test_rate_limit_config_default() {
    let config: TavernConfig = toml::from_str("").unwrap();
    assert!(!config.rate_limit.enabled);
    assert_eq!(config.rate_limit.default_rps, 10);
}

#[test]
fn test_rate_limit_config_full() {
    let toml = r#"
[rate_limit]
enabled = true
default_rps = 20
[rate_limit.per_tenant.admin]
rps = 100
"#;
    let config: TavernConfig = toml::from_str(toml).unwrap();
    assert!(config.rate_limit.enabled);
    assert_eq!(config.rate_limit.per_tenant["admin"].rps, 100);
}
```

---

### Task C2: 限流器实现

**Files:** `crates/tavern-server/src/ratelimit.rs`（新建）

- [ ] **Step 1: 定义 `TenantId` 新类型**

```rust
/// 从认证中间件注入的租户标识。
#[derive(Clone, Debug)]
pub struct TenantId(pub String);
```

- [ ] **Step 2: `RateLimiter` 结构体 + 构造函数**

```rust
pub struct RateLimiter {
    enabled: bool,
    default_rps: u32,
    tenants: HashMap<String, u32>,
    counters: Arc<tokio::sync::RwLock<HashMap<String, AtomicU64>>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        let tenants: HashMap<String, u32> = config.per_tenant
            .into_iter()
            .map(|(k, v)| (k, v.rps))
            .collect();

        let counters = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let counters_clone = counters.clone();

        // 每 1 秒重置所有计数器
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let c = counters_clone.read().await;
                for counter in c.values() {
                    counter.store(0, Ordering::Relaxed);
                }
            }
        });

        Self { enabled: config.enabled, default_rps, tenants, counters }
    }

    pub fn check(&self, tenant_id: &str) -> Result<(), RateLimitExceeded> {
        if !self.enabled {
            return Ok(());
        }
        let limit = self.tenants.get(tenant_id).copied().unwrap_or(self.default_rps) as u64;
        // ... fetch_add + check
    }
}
```

> **已知限制**：重置任务和请求线程间无锁同步。t=0.999s 的计数可能被 t=1.000s 的重置覆盖，单秒实际放行最多约 2× 限额。V0.3.2 接受此精度；V0.4.0 用 Redis token bucket 替换。

- [ ] **Step 3: axum 中间件**

```rust
pub async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, (StatusCode, [(HeaderName, &'static str); 1], Json<ApiError>)> {
    let tenant_id = request.extensions().get::<TenantId>()
        .map(|t| t.0.as_str())
        .unwrap_or("default");

    match state.rate_limiter.check(tenant_id) {
        Ok(()) => Ok(next.run(request).await),
        Err(_) => Err((
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, "1")],
            Json(ApiError {
                error: "RateLimited".into(),
                message: format!("rate limit exceeded for tenant '{}'", tenant_id),
                status: StatusCode::TOO_MANY_REQUESTS,
            }),
        )),
    }
}
```

- [ ] **Step 4: 单元测试**

```rust
#[tokio::test]
async fn test_rate_limiter_allows_within_limit() {
    let config = RateLimitConfig { enabled: true, default_rps: 10, .. };
    let limiter = RateLimiter::new(config);
    for _ in 0..10 { assert!(limiter.check("default").is_ok()); }
}

#[tokio::test]
async fn test_rate_limiter_blocks_exceeded() {
    let config = RateLimitConfig { enabled: true, default_rps: 5, .. };
    let limiter = RateLimiter::new(config);
    for _ in 0..5 { limiter.check("default").unwrap(); }
    assert!(limiter.check("default").is_err());
}

#[tokio::test]
async fn test_rate_limiter_per_tenant_isolation() {
    let mut tenants = HashMap::new();
    tenants.insert("admin".into(), TenantRateLimit { rps: 100 });
    tenants.insert("user".into(), TenantRateLimit { rps: 2 });
    let config = RateLimitConfig { enabled: true, default_rps: 10, per_tenant: tenants };
    let limiter = RateLimiter::new(config);
    // admin can burst
    for _ in 0..100 { assert!(limiter.check("admin").is_ok()); }
    // user limited to 2
    limiter.check("user").unwrap();
    limiter.check("user").unwrap();
    assert!(limiter.check("user").is_err());
}

#[tokio::test]
async fn test_rate_limiter_disabled_passes() {
    let config = RateLimitConfig { enabled: false, .. };
    let limiter = RateLimiter::new(config);
    for _ in 0..1000 { assert!(limiter.check("default").is_ok()); }
}

#[tokio::test]
async fn test_rate_limiter_unknown_tenant_uses_default() {
    let config = RateLimitConfig { enabled: true, default_rps: 3, .. };
    let limiter = RateLimiter::new(config);
    limiter.check("unknown").unwrap();
    limiter.check("unknown").unwrap();
    limiter.check("unknown").unwrap();
    assert!(limiter.check("unknown").is_err()); // 4th blocked
}
```

---

### Task C3: 模块注册 + AppState 构造点更新

**Files:** `crates/tavern-server/src/lib.rs`, `state.rs`, `router.rs`, `main.rs`, `tests/e2e.rs`

- [ ] **Step 1: 声明模块**

`crates/tavern-server/src/lib.rs` 增加：
```rust
pub mod ratelimit;
```

- [ ] **Step 2: `AppState` 新增字段**

```rust
pub rate_limiter: RateLimiter,
```

- [ ] **Step 3: `main.rs` 初始化**

从 `config.rate_limit` 构造 `RateLimiter::new()` 注入 `AppState`。

- [ ] **Step 4: 注册中间件**

在 `router.rs` 中，auth 中间件之后插入：
```rust
if state.config.rate_limit.enabled {
    protected_routes = protected_routes.layer(middleware::from_fn_with_state(
        state.clone(),
        crate::ratelimit::rate_limit_middleware,
    ));
}
```

- [ ] **Step 5: 更新所有 `AppState` 构造点——共需增加 `rate_limiter` 字段**

| 位置 | 文件 | 行 |
|------|------|-----|
| 生产入口 | `main.rs` ~L121 | 1 处 |
| 测试辅助函数 | `main.rs` `create_test_app` ~L765 | 1 处 |
| 测试辅助函数 | `main.rs` `create_test_app_with_workflow` ~L1290 | 1 处 |
| cancel 测试 | `main.rs` `test_cancel_workflow_execution` ~L1370 | 1 处 |
| e2e 测试 helper | `tests/e2e.rs` ~L133 | 1 处 |
| e2e 测试 auth | `tests/e2e.rs` ~L637 | 1 处 |

所有构造点插入：
```rust
rate_limiter: RateLimiter::new(RateLimitConfig::default()),
```

---

## 最终集成 & 验证

- [ ] `cargo check --workspace` — 0 errors
- [ ] `cargo test --workspace` — 全部通过（188 + 新增 ~19）
- [ ] `cargo clippy --workspace` — 无新增 warning
- [ ] 手动 e2e：审批完整流程（start → wait → approve → complete）
- [ ] 手动 e2e：审批驳回流程（start → wait → reject → failed）
- [ ] 手动 e2e：克隆流程（run → clone → 验证结果）
- [ ] 手动 e2e：限流 429 验证
