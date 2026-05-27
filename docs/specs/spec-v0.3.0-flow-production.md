# Tavern V0.3.0: Flow 生产化

> **状态：DRAFT** — 2026-05-26（review round 1 修正）  
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
| inputs → state | P2 | `engine.execute(json!({...}))` 注入 state |

---

## 2. REST API 集成

### 2.1 问题

`FlowEngine<F>` 是泛型，无法存入 `HashMap<String, FlowEngine<F>>`。需要 type-erased wrapper。

### 2.2 设计：FlowFactory + StartableFlow

```rust
// crates/tavern-flow/src/registry.rs

/// Flow 工厂：从 JSON inputs 创建 type-erased flow 实例。
pub trait FlowFactory: Send + Sync {
    fn metadata(&self) -> FlowMetadata;
    fn create(&self, inputs: serde_json::Value) -> Result<Box<dyn StartableFlow>, FlowError>;
}

/// Type-erased 可启动 flow。使用 async_trait 处理 Pin<Box<dyn Future>>。
#[async_trait::async_trait]
pub trait StartableFlow: Send {
    /// 异步启动，返回 FlowHandle。
    async fn start(self: Box<Self>) -> Result<FlowHandle, FlowError>;
}

/// 为任意 FlowEngine<F> 实现 StartableFlow。
/// 利用 Box<Self> → *self 提取 FlowEngine，调用 start_async。
#[async_trait::async_trait]
impl<F: Flow + FlowDispatch + Send + 'static> StartableFlow for FlowEngine<F> {
    async fn start(self: Box<Self>) -> Result<FlowHandle, FlowError> {
        Ok(self.start_async())  // FlowEngine::start_async takes Box<Self>
    }
}

/// FlowRegistry：线程安全的 type-erased 注册表。
pub struct FlowRegistry {
    entries: tokio::sync::RwLock<HashMap<String, RegistryEntry>>,
}

struct RegistryEntry {
    name: String,
    factory: Box<dyn FlowFactory>,
}

impl FlowRegistry {
    pub fn new() -> Self;
    pub fn register(&self, id: &str, name: &str, factory: Box<dyn FlowFactory>);
    pub fn get(&self, id: &str) -> Option<FlowSummary>;
    pub fn list(&self) -> Vec<FlowSummary>;
    pub fn create_instance(
        &self,
        id: &str,
        inputs: serde_json::Value,
    ) -> Result<Box<dyn StartableFlow>, FlowError>;
}

pub struct FlowSummary {
    pub id: String,
    pub name: String,
    pub start_methods: Vec<String>,
}
```

**`FlowEngine::start_async` 签名变更**：

```rust
impl<F> FlowEngine<F> {
    /// 异步启动（从 Box<Self> 中提取引擎并 spawn）。
    pub fn start_async(self: Box<Self>) -> FlowHandle;
}
```

理由：`StartableFlow::start(self: Box<Self>)` 传入 `Box<dyn StartableFlow>`。在 trait impl 中 `Self = FlowEngine<F>`，调用 `self.start_async()` 其中 `self: Box<FlowEngine<F>>`。Rust auto-deref 匹配 `start_async(Box<Self>)` 签名。

### 2.3 HTTP 端点

```
GET  /flows                  → [FlowSummary]
POST /flows/:id/start        → 202 { "flow_id": "uuid" }
GET  /flows/:id/status       → { flow_id, status, outputs, started_at, completed_at }
POST /flows/:id/cancel       → 202
```

**`POST /flows/:id/start`**：
```json
// Request
{ "inputs": { "topic": "AI trends" } }

// Response 202
{ "flow_id": "550e8400-e29b-41d4-a716-446655440000" }
```

**`GET /flows/:id/status`**：
```json
{
  "flow_id": "550e8400-...",
  "flow_name": "content_pipeline",
  "status": "running",
  "outputs": null,
  "started_at": "2026-05-26T10:00:00Z",
  "completed_at": null
}
```

**活跃 Flow 管理**：

`AppState` 新增 `flow_handles` 字段（类似 `execution_handles`）：

```rust
// tavern-server/src/state.rs
pub flow_handles: Arc<RwLock<HashMap<String, FlowHandle>>>,
```

handler 在 `POST /flows/:id/start` 时：
1. 通过 `FlowRegistry::create_instance` 创建 `Box<dyn StartableFlow>`
2. 生成 UUID 作为 `flow_id`
3. 调用 `flow.start()` 获取 `FlowHandle`
4. 将 `FlowHandle` 存入 `state.flow_handles`
5. spawn 清理任务（interpreter 完成时移除）
6. 返回 202 + flow_id

`GET /flows/:id/status` 和 `POST /flows/:id/cancel` 从 `flow_handles` 和/或 EventStore 查询状态。

### 2.4 注册方式

在 `tavern-server/src/main.rs` 或独立 setup 模块中：

```rust
let flow_registry = FlowRegistry::new();

flow_registry.register(
    "content_pipeline",
    "Content Pipeline",
    Box::new(|inputs: Value| {
        let state: ContentState = serde_json::from_value(inputs)?;
        let pipeline = ContentPipeline { state, hero: hero.clone() };
        Ok(Box::new(FlowEngine::new(pipeline)))
    }),
);
```

---

## 3. 并行执行

### 3.1 当前状态

`FlowEngine::execute_inner()` 使用 `VecDeque<String>` 顺序执行。同一时刻只有一个方法在运行。

### 3.2 目标

多个 `#[start]` 方法并发执行（`tokio::spawn`），通过 `max_concurrency` 限制并发数。

### 3.3 核心挑战

`FlowDispatch::dispatch(&mut self, ...)` 返回 `Pin<Box<dyn Future + Send + '_>>`。`'_` 生命周期 borrow `&mut self`，意味着：
- 无法在持有 `&mut self` 的同时 await Future
- 无法用 `Mutex` lock → dispatch → unlock → await（Future borrows guard）

**解决方案**：dispatch 内部使用 `Arc<Mutex<F>>`。`FlowDispatch::dispatch` 改为接受 `&self`。生成的 wrapper 方法通过 `self.inner.lock().unwrap()` 获取 `&mut F` 来调用原始方法。

```rust
// 重构后的 FlowEngine
pub struct FlowEngine<F> {
    inner: Arc<Mutex<F>>,     // 新增：内部可变性
    graph: FlowGraph,
    max_concurrency: usize,
}

// 重构后的 dispatch trait
pub trait FlowDispatch {
    fn dispatch(
        &self,               // 改为 &self（不要求 &mut）
        method: &str,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, FlowError>> + Send>>;  // 移除 '_ 
}
```

**proc-macro 生成的 wrapper 方法变更**：

```rust
// 旧（&mut self）
async fn __flow_wrapper_step_a(&mut self) -> Result<Value, FlowError> {
    let result = self.step_a().await?;
    ...
}

// 新（&self + 内部 Mutex）
async fn __flow_wrapper_step_a(
    inner: Arc<Mutex<Self>>,
) -> Result<Value, FlowError> {
    let result = inner.lock().unwrap().step_a().await?;
    ...
}
```

**影响范围**：
- `#[flow_impl]` 宏：wrapper 方法签名 + dispatch arms 全部重写
- `FlowDispatch` trait：`&mut self` → `&self`，移除 `'_`
- 12 个现有 flow 测试需同步更新

### 3.4 并行执行逻辑

```rust
async fn execute_inner(&mut self) -> Result<Value, FlowError> {
    let semaphore = Arc::new(Semaphore::new(self.max_concurrency));
    let mut pending = VecDeque::from(self.graph.start_nodes());
    let mut handles = Vec::new();

    // Phase 1: 并行启动 start 方法
    for name in &pending {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let inner = self.inner.clone();
        let name = name.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let future = FlowEngine::<F>::dispatch_static(&inner, &name, Value::Null);
            (name, future.await)
        }));
    }

    // Phase 2: 收集结果 → 触发下游（继续并行循环）
    // ...
}
```

**关键约束**：
- 默认 `max_concurrency = 1`（顺序执行，向后兼容）
- AND 语义：等待所有上游完成后才调度（通过 in_degree 计数）
- OR 语义：首个完成的上游即触发（in_degree 初始为 1）
- Router 方法顺序执行（确定 label 后才能分发多分支）

---

## 4. Flow 持久化

### 4.1 问题

`EventStore::append(instance_id, WorkflowEvent)` 接受 `WorkflowEvent` 类型。`FlowEvent` 是不同 enum。

### 4.2 设计：WorkflowEvent 新增 External variant

在 `tavern-comp` 的 `WorkflowEvent` 中增加一个通用 variant：

```rust
pub enum WorkflowEvent {
    // ... 现有 variants ...
    
    /// 外部事件（Flow 事件、自定义事件等）
    External {
        /// 事件类型标签（如 "flow_started", "method_completed"）
        event_type: String,
        /// 事件 payload（JSON）
        payload: Value,
    },
}
```

### 4.3 FlowEvent 定义

```rust
// crates/tavern-flow/src/event.rs

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FlowEvent {
    FlowStarted {
        flow_name: String,
        inputs: Value,
        started_at: DateTime<Utc>,
    },
    MethodScheduled {
        method_name: String,
    },
    MethodCompleted {
        method_name: String,
        output: Value,
        completed_at: DateTime<Utc>,
    },
    MethodFailed {
        method_name: String,
        error: String,
    },
    RouterDecision {
        router_name: String,
        labels: Vec<String>,
    },
    FlowCompleted {
        outputs: Value,
        completed_at: DateTime<Utc>,
    },
    FlowFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl FlowEvent {
    /// 包装为 WorkflowEvent::External 以存入 EventStore。
    pub fn to_workflow_event(&self) -> WorkflowEvent {
        WorkflowEvent::External {
            event_type: self.event_type(),
            payload: serde_json::to_value(self).unwrap(),
        }
    }

    fn event_type(&self) -> String {
        // 利用 serde(tag = "type") 提取 tag
        match self {
            FlowEvent::FlowStarted { .. } => "flow_started",
            FlowEvent::MethodCompleted { .. } => "method_completed",
            // ...
        }.to_string()
    }
}
```

### 4.4 FlowEngine 集成

```rust
impl<F> FlowEngine<F> {
    pub fn with_store(mut self, store: Arc<dyn EventStore>) -> Self;

    /// 持久化一个 Flow 事件。
    async fn persist_event(&self, flow_id: &str, event: FlowEvent) -> Result<(), CompError> {
        if let Some(store) = &self.store {
            store.append(flow_id, event.to_workflow_event()).await
        } else {
            Ok(())
        }
    }
}
```

**事件存储**：
- 所有 Flow 持久化复用现有 `workflow_events` 表
- `instance_id` 列存 `flow_id`（UUID）
- `payload` 列存 JSON：`{"type": "flow_started", ...}` （与 `WorkflowEvent` 通过 `serde(tag = "type")` 共存）
- 无新表、无新迁移

---

## 5. Router 多 label 返回

### 5.1 当前

`#[router]` 返回 `String` → 触发单个 label。

### 5.2 目标

```rust
#[router("process")]
async fn classifier(&mut self, data: String) -> Vec<String> {
    vec!["publish".to_string(), "notify".to_string()]
}
```

### 5.3 实现

- **宏**：proc-macro 解析 router 方法的返回类型
  - `String` → wrapper 生成 `Value::String(label)`
  - `Vec<String>` → wrapper 生成 `Value::Array([Value::String(l1), ...])`
- **Engine**：`execute_inner` 中识别 `Value::Array` → 遍历 labels，为每个 label 触发对应 listener
- **向后兼容**：单个 `String` 仍然支持

---

## 6. inputs → state 注入

### 6.1 设计

`FlowFactory::create(inputs)` 接收 JSON inputs，由用户闭包实现反序列化到 state：

```rust
flow_registry.register("content_pipeline", "Content Pipeline", Box::new(|inputs| {
    let state: ContentState = serde_json::from_value(inputs)?;
    Ok(Box::new(FlowEngine::new(ContentPipeline { state })))
}));
```

`FlowEngine` 本身保持泛型，不强制 inputs 约定。注入策略由 `FlowFactory` 实现自由决定。

---

## 7. 文件变更

### 新增文件

```
crates/tavern-flow/src/registry.rs     # FlowFactory, FlowRegistry, StartableFlow
crates/tavern-flow/src/event.rs        # FlowEvent
```

### 修改文件

```
crates/tavern-comp/src/event.rs        # +WorkflowEvent::External
crates/tavern-flow/src/lib.rs          # FlowDispatch &self, FlowEngine: Arc<Mutex<F>>, with_store, max_concurrency
crates/tavern-flow/src/engine.rs       # 并行执行逻辑 (dispatch_static)
crates/tavern-flow-macros/src/lib.rs   # Wrapper &self + Arc<Mutex<>>, Router Vec<String> 返回类型解析
crates/tavern-server/src/router.rs     # + /flows routes
crates/tavern-server/src/handlers.rs   # + flow handlers
crates/tavern-server/src/state.rs      # + flow_registry, flow_handles
crates/tavern-server/src/main.rs       # flow 注册 + 恢复
```

---

## 8. 向后兼容保证

| 现有功能 | V0.3.0 后 |
|----------|-----------|
| `tavern-comp` 所有 API | `WorkflowEvent::External` 为纯增量，零破坏 |
| `tavern-flow` `FlowDispatch` trait | **Breaking**: `&mut self` → `&self`，移除 `'_` |
| `tavern-flow` `FlowEngine::new/execute` | 签名不变，内部 `Arc<Mutex<F>>` 对调用方透明 |
| `FlowHandle::await_completion()` | 不变 |
| proc-macro 属性语法 | 语法不变，wrapper 生成内容变化 |
| 现有 YAML 配置 | 不变 |
| V0.2.0 全部 172 测试 | 12 个 flow 测试需同步更新（wrapper &self 变更） |

> **注意**：`FlowDispatch` trait 的 `&mut self` → `&self` 是 V0.3.0 中唯一的 breaking change。影响范围：`tavern-flow` crate 内部 12 个测试和 proc-macro 生成代码。外部调用方（如有）需同步更新。

---

## 9. 验收标准

- [ ] `cargo test --workspace` 全绿（172 + V0.3.0 新增）
- [ ] `cargo clippy --workspace` 零 warning
- [ ] `cargo fmt -- --check` 零变更
- [ ] FlowRegistry 可注册、查询、创建实例
- [ ] `POST /flows/:id/start` → 202 + flow_id
- [ ] `GET /flows/:id/status` → 正确状态
- [ ] 2 个 start 方法可通过 `max_concurrency=2` 并行执行
- [ ] `max_concurrency=1` 保持顺序执行
- [ ] Flow 完成后 EventStore 有完整 `FlowEvent` 记录（通过 `WorkflowEvent::External`）
- [ ] `#[router]` 返回 `Vec<String>` 触发多个 label listener
- [ ] inputs → state 通过 `FlowFactory::create` 正确注入

---

## 10. 风险与缓解

| 风险 | 缓解 |
|------|------|
| `FlowDispatch` &self 重构破坏现有 12 个测试 | 一次性更新所有测试 + proc-macro 生成代码 |
| `Arc<Mutex<F>>` 引入性能开销 | 默认 `max_concurrency=1`（无并发，lock 无竞争） |
| `WorkflowEvent::External` 可能干扰现有 EventStore 查询 | 新增 variant 为增量，不影响 `read_stream` 反序列化（serde tag 自动识别） |
| proc-macro Router 返回类型解析复杂 | 先支持 `String` / `Vec<String>` 两种，文档说明 |

---

## 11. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 0.1.0-draft | 2026-05-26 | 初稿：5 项功能设计 |
| 0.2.0-draft | 2026-05-26 | Review round 1: 修复 C1-C3 + I4-I6 |
