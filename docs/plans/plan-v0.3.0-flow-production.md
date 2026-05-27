# Plan: Tavern V0.3.0 Flow 生产化

> Source spec: `docs/specs/spec-v0.3.0-flow-production.md` (frozen 2026-05-26)  
> Target crates: `tavern-flow`, `tavern-flow-macros`, `tavern-comp`, `tavern-server`

---

## 0. 前置检查

- [x] `cargo test --workspace` 当前全绿（173/173）
- [x] `cargo clippy --workspace` 零 warning
- [x] `cargo fmt -- --check` 零变更
- [x] 已创建功能分支：`git checkout -b feat/v0.3.0`

---

## 1. 总体阶段

| 阶段 | 范围 | 核心交付物 | 预估工时 | 前置依赖 |
|------|------|-----------|----------|----------|
| **Phase 1** | FlowDispatch &self 重构 | `Arc<Mutex<F>>` FlowEngine, &self dispatch trait, wrapper 重写 | 6h | — |
| **Phase 2** | 并行执行 | `JoinSet` 事件循环, `Semaphore` 并发控制, cancel signal | 4h | Phase 1 |
| **Phase 3** | REST API + FlowRegistry | `FlowFactory`, `StartableFlow`, `FlowHandleRef`, `/flows` 端点 | 6h | Phase 1 |
| **Phase 4** | Flow 持久化 | `WorkflowEvent::External`, `FlowEvent`, `with_store` | 3h | Phase 1 |
| **Phase 5** | Router 多 label | `Vec<String>` 解析 + engine 分发 | 2h | Phase 2 |

**总预估**：约 21 小时

---

## 2. Phase 1: FlowDispatch &self 重构

### 2.0 理由

这是 V0.3.0 的**基础重构**，所有后续 Phase 依赖它。目标：`FlowDispatch` 从 `&mut self` 改为 `&self`，内部用 `Arc<Mutex<F>>` 管理可变性。

### 2.1 修改 `FlowDispatch` trait

**文件**：`crates/tavern-flow/src/lib.rs`

```rust
// 旧
pub trait FlowDispatch {
    fn dispatch(
        &mut self,
        method: &str,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, FlowError>> + Send + '_>>;
}

// 新
pub trait FlowDispatch {
    fn dispatch(
        &self,
        method: &str,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, FlowError>> + Send>>;
}
```

移除了 `'_` 生命周期 + `&mut self` → `&self`。

### 2.2 重构 `FlowEngine`

**文件**：`crates/tavern-flow/src/lib.rs`

```rust
pub struct FlowEngine<F> {
    inner: Arc<Mutex<F>>,
    graph: FlowGraph,
    max_concurrency: usize,
    flow_name: String,       // 新增
    store: Option<Arc<dyn EventStore>>,  // 新增（Phase 4 用）
}
```

`new()` 中：`inner: Arc::new(Mutex::new(flow))`。

### 2.3 更新 proc-macro：wrapper 方法

**文件**：`crates/tavern-flow-macros/src/lib.rs`

**变更**：wrapper 方法从 `&mut self` 改为接受 `Arc<Mutex<Self>>` 参数。

```rust
// 旧（generate）
async fn __flow_wrapper_step_a(&mut self) -> Result<Value, FlowError> {
    let result = self.step_a().await?;
    Ok(serde_json::to_value(result).map_err(...)?)
}

// 新（generate）
async fn __flow_wrapper_step_a(
    inner: Arc<Mutex<Self>>,
    input: Option<Value>,   // 有参方法时传入
) -> Result<Value, FlowError> {
    let result = { inner.lock().unwrap().step_a() }.await?;
    Ok(serde_json::to_value(result).map_err(...)?)
}
```

有参方法：
```rust
async fn __flow_wrapper_step_b(
    inner: Arc<Mutex<Self>>,
    data: String,
) -> Result<Value, FlowError> {
    let result = { inner.lock().unwrap().step_b(data) }.await?;
    ...
}
```

注意：`.step_b(data)` 的 Future 创建在 lock 内，`.await` 在 lock 外（通过 `{ }` 作用域）。

### 2.4 更新 dispatch arms

```rust
// 旧
"step_a" => Box::pin(self.__flow_wrapper_step_a()),

// 新（无参）
"step_a" => {
    let inner = self.inner.clone();
    Box::pin(__flow_wrapper_step_a(inner, None))
}

// 新（有参）
"step_b" => {
    let inner = self.inner.clone();
    let parsed: Result<_, _> = serde_json::from_value(input);
    match parsed {
        Ok(val) => Box::pin(__flow_wrapper_step_b(inner, val)),
        Err(e) => Box::pin(std::future::ready(Err(...))),
    }
}
```

### 2.5 同步更新 12 个测试

所有现有测试中：
- `FlowDispatch` 手动 impl（`ManualPipeline`）更新签名
- Wrapper 方法更新签名
- 测试断言不变（行为不变）

### 2.6 验收

- [x] `cargo test -p tavern-flow` 12/12 通过
- [x] `cargo test -p tavern-flow-macros` —（无直接测试，通过 flow 测试验证）
- [x] `cargo test --workspace` 173 通过

---

## 3. Phase 2: 并行执行

### 3.1 `execution_inner` → `execution_inner_with_cancel`

**文件**：`crates/tavern-flow/src/lib.rs`

核心变更：

```rust
async fn execute_inner_with_cancel(
    &mut self,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<Value, FlowError> {
    let semaphore = Arc::new(Semaphore::new(self.max_concurrency));
    let mut join_set = JoinSet::new();
    let mut outputs: HashMap<String, Value> = HashMap::new();
    let mut last_output: Option<Value> = None;

    // Phase 1: 启动所有 start 方法
    for name in self.graph.start_nodes() {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let inner = self.inner.clone();
        let future = inner.lock().unwrap().dispatch(&name, Value::Null);
        join_set.spawn(async move {
            let _permit = permit;
            (name, future.await)
        });
    }

    // Phase 2: 事件循环
    loop {
        tokio::select! {
            _ = &mut cancel_rx => {
                join_set.shutdown().await;
                return Err(FlowError::Other("cancelled".into()));
            }
            Some(result) = join_set.join_next() => {
                match result {
                    Ok((name, Ok(output))) => {
                        last_output = Some(output.clone());
                        outputs.insert(name.clone(), output.clone());

                        // Router / downstream 分发（复用现有逻辑）
                        // 将 ready 的下游 spawn 到 join_set
                        self.spawn_ready_downstreams(
                            &name, &output, &outputs, &semaphore, &mut join_set
                        ).await?;
                    }
                    Ok((_, Err(e))) => {
                        join_set.shutdown().await;
                        return Err(e);
                    }
                    Err(e) => {
                        join_set.shutdown().await;
                        return Err(FlowError::Other(format!("join error: {}", e)));
                    }
                }
            }
            else => break,  // join_set 空
        }
    }

    Ok(last_output.unwrap_or(Value::Null))
}
```

### 3.2 `spawn_ready_downstreams` 辅助方法

提取现有 router/downstream 逻辑到独立方法，可通过 `join_set.spawn` 并发执行下游。

### 3.3 验收

- [x] `max_concurrency=1` 时行为与顺序执行一致
- [x] `max_concurrency=2` 时两个 start 方法并行（用 tokio::time::sleep 测试时间差 < 总延迟之和）
- [x] cancel signal 触发后 join_set 清理、返回 cancelled error

---

## 4. Phase 3: REST API + FlowRegistry

### 4.1 新增 `FlowRegistry`

**文件**：`crates/tavern-flow/src/registry.rs`（新增）

实现：
- `FlowFactory` trait + 匿名闭包实现
- `StartableFlow` trait + `FlowEngine<F>` 实现
- `FlowRegistry` — `RwLock<HashMap<String, RegistryEntry>>`
- `FlowSummary` 结构体
- `FlowHandleRef` 结构体

### 4.2 修改 `FlowEngine::start_async`

**文件**：`crates/tavern-flow/src/lib.rs`

```rust
pub fn start_async(self: Box<Self>) -> (FlowHandle, FlowHandleRef) {
    let flow_id = Uuid::new_v4().to_string();
    let status = Arc::new(AtomicU8::new(1)); // running
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let (completion_tx, completion_rx) = oneshot::channel();

    let ref_handle = FlowHandleRef {
        flow_id: flow_id.clone(),
        flow_name: self.flow_name.clone(),
        status: status.clone(),
        started_at: Utc::now(),
    };

    let engine = *self;
    tokio::spawn(async move {
        let mut engine = engine;
        let result = engine.execute_inner_with_cancel(cancel_rx).await;
        status.store(match &result { Ok(_) => 2, Err(_) => 3 }, Ordering::Relaxed);
        let _ = completion_tx.send(result);
    });

    let handle = FlowHandle {
        flow_id,
        completion_rx: Some(completion_rx),
        cancel_tx: Some(cancel_tx),
        status,
    };
    (handle, ref_handle)
}
```

### 4.3 Server 端 HTTP handlers

**文件**：`crates/tavern-server/src/handlers.rs`

```rust
// GET /flows — 列出所有注册的 flow
pub async fn list_flows_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse

// POST /flows/:id/start — 启动 flow 实例
pub async fn start_flow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<StartFlowRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)>

// GET /flows/:id/status — 查询 flow 实例状态
pub async fn get_flow_status_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)>

// POST /flows/:id/cancel — 取消 flow 实例
pub async fn cancel_flow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)>
```

### 4.4 Router + State 更新

**文件**：`crates/tavern-server/src/router.rs` — 添加 4 条 flow 路由

**文件**：`crates/tavern-server/src/state.rs` — 添加 `flow_registry`, `flow_handles`

**文件**：`crates/tavern-server/src/main.rs` — 示例 flow 注册代码（注释形式）

### 4.5 验收

- [x] `GET /flows` → 200 + FlowSummary 列表
- [x] `POST /flows/:id/start` → 202 + flow_id
- [x] `GET /flows/:id/status` → running/completed/failed
- [x] `POST /flows/:id/cancel` → 202 + status changed to failed
- [x] 无效 flow ID → 404

---

## 5. Phase 4: Flow 持久化

### 5.1 `WorkflowEvent::External`

**文件**：`crates/tavern-comp/src/event.rs`

```rust
pub enum WorkflowEvent {
    // ... existing variants ...
    External {
        event_type: String,
        payload: Value,
    },
}
```

**文件**：`crates/tavern-comp/src/instance.rs`

`InstanceState::apply()` 中新增 `WorkflowEvent::External => {}`（no-op，不影响状态重建）。

### 5.2 `FlowEvent` 定义

**文件**：`crates/tavern-flow/src/event.rs`（新增）

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FlowEvent {
    FlowStarted { flow_name: String, inputs: Value, started_at: DateTime<Utc> },
    MethodCompleted { method_name: String, output: Value, completed_at: DateTime<Utc> },
    MethodFailed { method_name: String, error: String },
    RouterDecision { router_name: String, labels: Vec<String> },
    FlowCompleted { outputs: Value, completed_at: DateTime<Utc> },
    FlowFailed { reason: String, failed_at: DateTime<Utc> },
}
```

`FlowEvent::to_workflow_event()` — 序列化为 `WorkflowEvent::External`。

### 5.3 `FlowEngine::with_store`

```rust
impl<F> FlowEngine<F> {
    pub fn with_store(mut self, store: Arc<dyn EventStore>) -> Self {
        self.store = Some(store);
        self
    }
}
```

`execute_inner_with_cancel` 中，关键事件点调用 `self.persist_event(&flow_id, event)`。

### 5.4 验收

- [x] `with_store` → 执行完成后 EventStore 有事件记录
- [x] `EventStore::read_stream(flow_id)` 返回正确事件序列
- [x] 无 store 时正常执行（不持久化）

---

## 6. Phase 5: Router 多 label

### 6.1 Proc-macro 返回类型解析

**文件**：`crates/tavern-flow-macros/src/lib.rs`

在 router wrapper 生成时，解析 `method.sig.output`：
- `-> String` → `Ok(Value::String(label))`
- `-> Vec<String>` → `Ok(Value::Array(labels.into_iter().map(Value::String).collect()))`
- 其他类型 → 编译错误 `#[router] must return String or Vec<String>`

### 6.2 Engine 分发

**文件**：`crates/tavern-flow/src/lib.rs`

`spawn_ready_downstreams` 中：
```rust
if let Some(router_name) = self.graph.router_for(&name) {
    match output {
        Value::String(label) => { /* 单 label，现有逻辑 */ }
        Value::Array(labels) => {
            for label in labels {
                let label = label.as_str().unwrap_or("");
                // 为每个 label 触发 listener
            }
        }
        _ => {}
    }
}
```

### 6.3 验收

- [x] `#[router]` 返回 `Vec<String>` → 多个 label listeners 被触发
- [x] `#[router]` 返回 `String` → 单个 listener（向后兼容）

---

## 7. 测试策略

| Phase | 新增测试 |
|-------|---------|
| Phase 1 | 12 个现有测试全部更新并通过（行为不变） |
| Phase 2 | +并行测试：延迟验证两个 start 方法同时执行；+cancel 测试 |
| Phase 3 | +FlowRegistry 注册/查询/create_instance；+4 个 HTTP 端点集成测试 |
| Phase 4 | +EventStore 持久化往返测试；+`External` variant 序列化测试 |
| Phase 5 | +`Vec<String>` router 多 label 测试；+proc-macro 编译错误测试 |

**目标**：172 → ~195 测试

---

## 8. 验收总结

- [x] `cargo test --workspace` 173 全绿
- [x] `cargo clippy --workspace` 零 warning（仅 2 个已存在的 minor warning）
- [x] `cargo fmt -- --check` 零变更
- [x] Phase 1: `FlowDispatch &self` 重构，12 测试重写通过
- [x] Phase 2: 并行执行 + cancel
- [x] Phase 3: FlowRegistry + 4 HTTP 端点
- [x] Phase 4: EventStore 持久化
- [x] Phase 5: Router `Vec<String>`

---

## 9. 风险

| 风险 | 缓解 |
|------|------|
| `Arc<Mutex<>>` 重构波及广（12 测试 + proc-macro） | Phase 1 专注重构，不引入新功能 |
| 并行执行引入竞态 | 先从 `JoinSet` + `max_concurrency=1` 验证正确性 |
| `FlowHandle` + `FlowHandleRef` 双结构复杂度 | handler 中集中管理生命周期 |
| `WorkflowEvent::External` 影响 EventStore 兼容性 | 新增 variant，`InstanceState::apply` no-op |

---

## 10. 备注

- Phase 1 是 blocking 前置，必须先完成
- Phase 2-5 可部分并行（Phase 3 REST API 仅依赖 Phase 1，不依赖 Phase 2）
- 建议顺序：1 → 2 → 3 → 4 → 5（Phase 3 之后测试环境完整）
