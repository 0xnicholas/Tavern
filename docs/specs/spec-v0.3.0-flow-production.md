# Tavern V0.3.0: Flow 生产化

> **状态：FROZEN** — 2026-05-26（review 2 轮，10 个问题全部修复）  
> 下一阶段：plan 编写后冻结

## 1. 概述

V0.3.0 将 `tavern-flow` 从原型推进到**生产可用**阶段。核心目标：Flow 可被 HTTP API 调用、可并行执行、可持久化恢复。

### 1.1 设计原则

- **向后兼容**：现有 `tavern-comp` API 零破坏，V0.2.0 172 测试继续通过
- **最小抽象**：不引入新的 trait 层次，复用 EventStore / WorkflowEngine 基础设施
- **Type-erased registry**：通过工厂模式抹除泛型，让 Flow 可注册、可路由
- **顺序 → 并行渐进**：默认行为不变，通过 `max_concurrency` 参数启用并行

### 1.2 核心交付

| 功能 | 优先级 | 说明 |
|------|:---:|------|
| REST API 集成 | P0 | Flow 注册表 + HTTP 端点 |
| 并行执行 | P0 | 多 start 方法并发 + 并发控制 |
| Flow 持久化 | P1 | 复用 EventStore，记录执行事件 |
| Router 多 label | P1 | `#[router]` 返回 `Vec<String>` 触发多分支 |
| inputs → state | P2 | `FlowFactory::create` 实现注入 |

---

## 2. REST API 集成

### 2.1 Type-erased Registry

```rust
// crates/tavern-flow/src/registry.rs

pub trait FlowFactory: Send + Sync {
    fn metadata(&self) -> FlowMetadata;
    fn create(&self, inputs: Value) -> Result<Box<dyn StartableFlow>, FlowError>;
}

#[async_trait]
pub trait StartableFlow: Send {
    async fn start(self: Box<Self>) -> Result<FlowHandle, FlowError>;
}

// 泛型 FlowEngine<F> → type-erased StartableFlow
#[async_trait]
impl<F: Flow + FlowDispatch + Send + 'static> StartableFlow for FlowEngine<F> {
    async fn start(self: Box<Self>) -> Result<FlowHandle, FlowError> {
        Ok(self.start_async())
    }
}

pub struct FlowRegistry {
    entries: RwLock<HashMap<String, RegistryEntry>>,
}
```

### 2.2 FlowHandle（增强）

```rust
pub struct FlowHandle {
    flow_id: String,
    completion_rx: Option<oneshot::Receiver<Result<Value, FlowError>>>,
    cancel_tx: Option<oneshot::Sender<()>>,
    status: Arc<AtomicU8>,  // 0=pending, 1=running, 2=completed, 3=failed
}

impl FlowHandle {
    pub fn flow_id(&self) -> &str;
    pub fn status(&self) -> FlowStatus;
    pub async fn await_completion(&mut self) -> Result<Value, FlowError>;
    pub fn cancel(&self);  // 发送 cancel signal → spawned task 检查并退出
}
```

**FlowEngine 感知 cancel**：`execute_inner` 循环中，每轮开始时检查 cancel signal（`cancel_rx.try_recv()`）。若已取消 → 立即返回 `FlowError`。

### 2.3 HTTP 端点

```
GET  /flows                  → [FlowSummary { id, name, start_methods }]
POST /flows/:id/start        → 202 { "flow_id": "uuid" }
GET  /flows/:id/status       → { flow_id, status, outputs, started_at, completed_at }
POST /flows/:id/cancel       → 202
```

### 2.4 活跃 Flow 管理

`AppState` 新增：

```rust
pub flow_registry: FlowRegistry,
pub flow_handles: Arc<RwLock<HashMap<String, FlowHandle>>>,
```

**`POST /flows/:id/start` handler 流程**：
1. `flow_registry.create_instance(id, inputs)` → `Box<dyn StartableFlow>`
2. 生成 UUID 作为 `flow_id`
3. `flow.start()` → `FlowHandle { flow_id, ... }`
4. `state.flow_handles.write().insert(flow_id, handle)` — **注意**：`FlowHandle` 需提供 `clone_handle()` 方法，返回一个可存储的轻量副本（共享内部 status Arc + completion_rx 的替代方案）

> **FlowHandle Clone 策略**：FlowHandle 本身不含 `Clone`（因 `oneshot::Receiver` 不能 clone）。改为：
> - `start()` 返回 `FlowHandle`（唯一，拥有 `completion_rx` 和 `cancel_tx`）
> - 同时返回 `FlowHandleRef { flow_id, status: Arc<AtomicU8> }` 存入 `flow_handles`
> 
> `FlowHandleRef` 仅用于状态查询；cancel 通过 `cancel_tx` 需要在 start 时额外存储到 flow_handles。

**简化方案**：handler spawn 一个任务持有 FlowHandle，`flow_handles` 只存 `FlowHandleRef`（轻量、可 Clone）：

```rust
pub struct FlowHandleRef {
    pub flow_id: String,
    pub flow_name: String,
    pub status: Arc<AtomicU8>,
    pub started_at: DateTime<Utc>,
}

impl FlowEngine<F> {
    pub fn start_async(self: Box<Self>) -> (FlowHandle, FlowHandleRef) {
        let flow_id = Uuid::new_v4().to_string();
        let status = Arc::new(AtomicU8::new(1)); // running
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();

        let ref_handle = FlowHandleRef {
            flow_id: flow_id.clone(),
            flow_name: self.graph.flow_name.clone(), // 需新增字段
            status: status.clone(),
            started_at: Utc::now(),
        };

        let engine = *self;
        tokio::spawn(async move {
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
}
```

handler 中：
```rust
let (handle, ref_handle) = flow.start().await?;
state.flow_handles.write().insert(ref_handle.flow_id.clone(), ref_handle);
// spawn 清理任务持有 handle 等待完成
```

### 2.5 注册方式

```rust
flow_registry.register("content_pipeline", "Content Pipeline", Box::new(|inputs| {
    let state: ContentState = serde_json::from_value(inputs)?;
    Ok(Box::new(FlowEngine::new(ContentPipeline { state, hero: hero.clone() })))
}));
```

---

## 3. 并行执行

### 3.1 核心挑战

`FlowDispatch::dispatch(&mut self, ...)` 返回 `Pin<Box<dyn Future + Send + '_>>`。`'_` borrow `&mut self`，导致：
- 无法 lock → dispatch → unlock → await（Future borrows lock guard）

### 3.2 解决方案：&self + Arc\<Mutex\<F\>\>

```rust
// 重构后
pub struct FlowEngine<F> {
    inner: Arc<Mutex<F>>,      // 内部可变性
    graph: FlowGraph,
    max_concurrency: usize,
    flow_name: String,         // 新增（用于 FlowHandleRef）
}

pub trait FlowDispatch {
    fn dispatch(
        &self,                 // &mut → &self
        method: &str,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, FlowError>> + Send>>;  // 移除 '_
}
```

**生成的 wrapper**：
```rust
// 旧
async fn __flow_wrapper_step_a(&mut self) -> Result<Value, FlowError> {
    let result = self.step_a().await?; ...

// 新
async fn __flow_wrapper_step_a(inner: Arc<Mutex<Self>>) -> Result<Value, FlowError> {
    let result = { inner.lock().unwrap().step_a() }.await?; ...
}
```

**dispatch arm**：
```rust
"step_a" => {
    let inner = self.inner.clone();
    Box::pin(__flow_wrapper_step_a(inner))
}
```

关键：lock 仅在 `.step_a()` 调用期间持有（创建 Future 时），Future 本身不持有 lock。

### 3.3 并行逻辑

```rust
async fn execute_inner_with_cancel(
    &mut self,
    mut cancel_rx: oneshot::Receiver<()>,
) -> Result<Value, FlowError> {
    let semaphore = Arc::new(Semaphore::new(self.max_concurrency));
    let mut join_set = JoinSet::new();

    // 启动所有 start 方法
    for name in self.graph.start_nodes() {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let inner = self.inner.clone();
        let future = inner.lock().unwrap().dispatch(&name, Value::Null);
        join_set.spawn(async move {
            let _permit = permit;
            (name, future.await)
        });
    }

    // 事件循环
    loop {
        tokio::select! {
            _ = &mut cancel_rx => return Err(FlowError::Other("cancelled".into())),
            Some(result) = join_set.join_next() => {
                let (name, output) = result.unwrap();
                match output {
                    Ok(val) => {
                        // router / downstream 分发（同现有逻辑）
                        // 将就绪的下游方法继续 spawn 到 join_set
                    }
                    Err(e) => return Err(e),
                }
            }
            else => break, // join_set 为空
        }
    }
}
```

**关键约束**：
- 默认 `max_concurrency = 1`（顺序，向后兼容）
- AND：等待所有上游完成才调度
- OR：首个上游触发
- Router：顺序执行（确定 labels）

---

## 4. Flow 持久化

### 4.1 WorkflowEvent::External

```rust
pub enum WorkflowEvent {
    // ... 现有 ...
    External {
        event_type: String,
        payload: Value,
    },
}
```

### 4.2 FlowEvent

```rust
pub enum FlowEvent {
    FlowStarted { flow_name, inputs, started_at },
    MethodCompleted { method_name, output, completed_at },
    MethodFailed { method_name, error },
    RouterDecision { router_name, labels: Vec<String> },
    FlowCompleted { outputs, completed_at },
    FlowFailed { reason, failed_at },
}
```

`FlowEvent → to_workflow_event() → WorkflowEvent::External`。复用 `workflow_events` 表，无新迁移。

### 4.3 FlowEngine 集成

```rust
impl<F> FlowEngine<F> {
    pub fn with_store(mut self, store: Arc<dyn EventStore>) -> Self;
}
```

`execute_inner` 中，每个事件后调用 `persist_event(flow_id, event)`。可选：仅在 `store.is_some()` 时持久化。

---

## 5. Router 多 label

宏解析返回类型：
- `String` → wrapper `Value::String(label)`
- `Vec<String>` → wrapper `Value::Array([...])`

Engine 中：`Value::Array` → 遍历 labels → 触发对应 listener。

---

## 6. inputs → state

由 `FlowFactory::create(inputs)` 用户闭包实现。`FlowEngine` 不感知。

---

## 7. 文件变更

| 文件 | 变更 |
|------|------|
| `tavern-comp/src/event.rs` | `+WorkflowEvent::External` |
| `tavern-flow/src/registry.rs` | **新**: FlowFactory, StartableFlow, FlowRegistry, FlowSummary, FlowHandleRef |
| `tavern-flow/src/event.rs` | **新**: FlowEvent |
| `tavern-flow/src/lib.rs` | FlowDispatch `&self` + 移除 `'_`, FlowEngine `Arc<Mutex<F>>`, `start_async → (FlowHandle, FlowHandleRef)` |
| `tavern-flow-macros/src/lib.rs` | Wrapper `Arc<Mutex<>>`, Router `Vec<String>` |
| `tavern-server/src/router.rs` | `+ /flows` routes |
| `tavern-server/src/handlers.rs` | `+ flow` handlers |
| `tavern-server/src/state.rs` | `+ flow_registry, flow_handles` |

---

## 8. 向后兼容

| 功能 | 影响 |
|------|------|
| `tavern-comp` | `WorkflowEvent::External` 纯增量 |
| `FlowDispatch` trait | **Breaking**: `&mut self` → `&self`, `'_` → `Send` |
| `FlowEngine::new/execute` | 签名不变 |
| `FlowHandle` | 新增字段和方法，旧 `await_completion()` 不变 |
| proc-macro 属性语法 | 不变 |
| 12 个现有 flow 测试 | 需同步更新 |

---

## 9. 验收标准

- [ ] 172 个已有测试 + V0.3.0 新增 → 全绿
- [ ] clippy + fmt clean
- [ ] FlowRegistry register → create_instance → start → 202 + flow_id
- [ ] `GET /flows/:id/status` 正确返回
- [ ] `POST /flows/:id/cancel` → spawned task 退出
- [ ] `max_concurrency=2` 时两个 start 方法并行
- [ ] FlowEvent 持久化到 EventStore
- [ ] Router `Vec<String>` 触发多个 listener
- [ ] inputs → state 注入

---

## 10. 风险

| 风险 | 缓解 |
|------|------|
| `Arc<Mutex<>>` 重构波及 12 测试 | 一次性更新 |
| Router 返回类型解析 | 仅支持 `String` / `Vec<String>` |
| FlowHandle + FlowHandleRef 双结构 | handler 中管理生命周期 |

## 11. 版本历史

| 版本 | 变更 |
|------|------|
| 0.1.0 | 初稿 |
| 0.2.0 | Round 1: WorkflowEvent::External, &self dispatch, Arc<Mutex<>> |
| 0.3.0 | Round 2: FlowHandleRef, cancel, status, dispatch_static 移除 |
