# Tavern V0.3.0: Flow 生产化

> **状态：DRAFT** — 2026-05-26  
> 下一阶段：plan 编写后冻结

## 1. 概述

V0.3.0 将 `tavern-flow` 从原型推进到**生产可用**阶段。核心目标：Flow 可被 HTTP API 调用、可并行执行、可持久化恢复。

### 1.1 设计原则

- **向后兼容**：现有 `tavern-comp` API 零破坏，V0.2.0 测试继续通过
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

/// Type-erased 可启动 flow。
#[async_trait::async_trait]
pub trait StartableFlow: Send {
    async fn start(self: Box<Self>) -> Result<FlowHandle, FlowError>;
    async fn execute(
        &mut self,
        inputs: serde_json::Value,
    ) -> Result<serde_json::Value, FlowError>;
}

/// 为任意 FlowEngine<F> 实现 StartableFlow。
#[async_trait::async_trait]
impl<F: Flow + FlowDispatch + Send + 'static> StartableFlow for FlowEngine<F> {
    async fn start(self: Box<Self>) -> Result<FlowHandle, FlowError> {
        Ok((*self).start_async())
    }
    async fn execute(
        &mut self,
        inputs: serde_json::Value,
    ) -> Result<serde_json::Value, FlowError> {
        self.execute(inputs).await
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

### 2.3 HTTP 端点

```
GET  /flows                  → [FlowSummary]
POST /flows/:id/start        → 202 { "flow_id": "uuid" }
GET  /flows/:id/status       → { flow_id, status, outputs, step_results }
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
  "status": "running",   // pending | running | completed | failed
  "outputs": null,
  "started_at": "2026-05-26T10:00:00Z",
  "completed_at": null
}
```

**状态存储**：基于事件源。每个 flow 执行产生 `FlowEvent` 序列（`FlowStarted`, `MethodCompleted`, `FlowCompleted`, `FlowFailed`），可通过 EventStore 查询和重建状态。

### 2.4 注册方式

在 `tavern-server/src/main.rs` 或独立 setup 模块中：

```rust
let flow_registry = FlowRegistry::new();

// 匿名工厂闭包
flow_registry.register(
    "content_pipeline",
    "Content Pipeline",
    Box::new(|inputs: Value| {
        let pipeline = ContentPipeline::from_inputs(inputs)?;
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

### 3.3 设计

```rust
pub struct FlowEngine<F> {
    flow: F,
    graph: FlowGraph,
    max_concurrency: usize,  // 新增
}

impl<F: Flow + FlowDispatch + Send + 'static> FlowEngine<F> {
    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }
}
```

**并行执行逻辑**：

```rust
async fn execute_inner(&mut self) -> Result<Value, FlowError> {
    let semaphore = Arc::new(Semaphore::new(self.max_concurrency));
    let mut pending = VecDeque::from(self.graph.start_nodes());
    let mut handles = Vec::new();

    // Phase 1: 并行启动 start 方法
    while let Some(name) = pending.pop_front() {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let fut = self.flow.dispatch(&name, Value::Null);
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            (name, fut.await)
        }));
    }

    // Phase 2: 收集结果 → 触发下游（继续并行）
    // ...（类似 Phase 1，但 input 来自上游 output）
}
```

**关键约束**：
- 下游方法依赖多个上游时（AND 语义），等待所有上游完成后才执行
- OR 语义：首个完成的下游立即触发
- Router 方法始终顺序执行（确定 label 后才能分发）
- 默认 `max_concurrency = 1`（顺序执行，保持向后兼容）

---

## 4. Flow 持久化

### 4.1 设计

复用 `tavern-comp` 的 `EventStore` trait。新增 `FlowEvent` 类型记录 Flow 执行历史。

```rust
// crates/tavern-flow/src/event.rs

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FlowEvent {
    FlowStarted {
        flow_id: String,
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
        label: String,
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
```

### 4.2 FlowEngine 集成

```rust
impl<F> FlowEngine<F> {
    pub fn with_store(mut self, store: Arc<dyn EventStore>) -> Self;

    /// 从 flow_id 恢复执行（崩溃后）。
    pub async fn recover(
        flow_id: &str,
        store: Arc<dyn EventStore>,
        factory: &dyn FlowFactory,
    ) -> Result<FlowHandle, FlowError>;
}
```

**注意**：Flow 持久化复用 `tavern-comp` 的 `EventStore` trait（`append` / `read_stream`），不需要新建表。`FlowEvent` 作为 JSON 存入 `workflow_events.payload` 列（通过 `serde(tag = "type")` 区分）。

---

## 5. Router 多 label 返回

### 5.1 当前

`#[router]` 返回 `String` → 触发单个 label。

### 5.2 目标

返回 `Vec<String>` → 触发多个 branch。

```rust
#[router("process")]
async fn classifier(&mut self, data: String) -> Vec<String> {
    vec!["publish".to_string(), "notify".to_string()]
}

#[listen("publish")]
async fn on_publish(&mut self, data: String) -> Result<(), FlowError> { ... }

#[listen("notify")]
async fn on_notify(&mut self, data: String) -> Result<(), FlowError> { ... }
```

### 5.3 实现

- 宏：router 返回 `Vec<String>` 时，wrapper 序列化为 `Value::Array`
- Engine：`execute_inner` 中，识别 `Value::Array` → 遍历 labels，触发多个 label listeners
- 向后兼容：单个 `String` 仍然支持

---

## 6. inputs → state 注入

### 6.1 当前

`FlowEngine::execute(inputs)` 的 `inputs` 被忽略（始终传 `Value::Null`）。

### 6.2 目标

```rust
let mut engine = FlowEngine::new(pipeline);
let result = engine.execute(json!({"topic": "AI", "depth": 3})).await?;
```

`inputs` 注入到 `self.state` 中对应的字段。

### 6.3 实现

通过 `serde_json::from_value` 反序列化 inputs 到 state：

```rust
pub fn new_with_inputs(flow: F, inputs: Value) -> Result<Self, FlowError> {
    let mut engine = Self::new(flow);
    // 尝试将 inputs 合并到 state（如果 state 实现 Deserialize）
    if !inputs.is_null() {
        // 使用 serde_json 的 merge 或直接设值
        // 具体策略取决于 state 类型
    }
    Ok(engine)
}
```

**限制**：泛型 state 不支持自动反序列化。`FlowFactory::create(inputs)` 由用户实现具体注入逻辑（在闭包中手动 `serde_json::from_value`）。

---

## 7. 文件变更

### 新增文件

```
crates/tavern-flow/src/registry.rs     # FlowFactory, FlowRegistry, StartableFlow
crates/tavern-flow/src/event.rs        # FlowEvent
```

### 修改文件

```
crates/tavern-flow/src/lib.rs          # +FlowEngine: with_store, with_max_concurrency, recover
crates/tavern-flow/src/engine.rs       # 并行执行逻辑
crates/tavern-flow-macros/src/lib.rs   # Router 返回 Vec<String> 支持
crates/tavern-server/src/router.rs     # + /flows routes
crates/tavern-server/src/handlers.rs   # + flow handlers
crates/tavern-server/src/state.rs      # + flow_registry: FlowRegistry
crates/tavern-server/src/main.rs       # flow 注册 + 恢复
```

---

## 8. 向后兼容保证

| 现有功能 | V0.3.0 后 |
|----------|-----------|
| `tavern-comp` 所有 API | 零变化 |
| `tavern-flow` 现有 API | `FlowEngine::new/execute/start_async` 签名不变 |
| `FlowHandle::await_completion()` | 不变 |
| 所有 proc-macro 属性 | 语法不变，扩展 Router 返回类型 |
| 现有 YAML 配置 | 不变 |
| V0.2.0 全部 172 测试 | 继续通过 |

---

## 9. 验收标准

- [ ] `cargo test --workspace` 全绿（V0.2.0 172 + V0.3.0 新增）
- [ ] `cargo clippy --workspace` 零 warning
- [ ] `cargo fmt -- --check` 零变更
- [ ] FlowRegistry 可注册、查询、创建实例
- [ ] `POST /flows/:id/start` → 202 + flow_id
- [ ] `GET /flows/:id/status` → 正确状态
- [ ] 2 个 start 方法并行执行（通过延迟测试验证）
- [ ] `max_concurrency` 限制并发数
- [ ] Flow 完成后 EventStore 有完整事件记录
- [ ] 崩溃后可从 EventStore 恢复 Flow 状态
- [ ] `#[router]` 返回 `Vec<String>` 触发多个 label listener
- [ ] inputs → state 正确注入

---

## 10. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 0.1.0-draft | 2026-05-26 | 初稿：5 项功能设计 |
