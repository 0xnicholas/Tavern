# Tavern Flow Layer — Spec

> **状态：FROZEN** — 2026-05-26  
> 原型验证通过（172 tests, 0 failures）。实际实现与初始设计存在差异，详见 §6 验证结果。

## 1. 概述

`tavern-flow` 提供与 CrewAI Flow 等价的能力：**方法级事件驱动编排**。与 tavern-comp Workflow 的核心差异：

| | Workflow (tavern-comp) | Flow (tavern-flow) |
|---|---|---|
| 定义方式 | YAML 配置文件 | Rust struct + proc-macro |
| 步骤类型 | 必须绑定 Agent (`agent_id`) | 任意异步 Rust 方法 |
| 状态类型 | JSON `Value`（弱类型） | 用户自定义 struct（手动声明） |
| 依赖声明 | `depends_on: [step_id]` | `#[listen("method_name")]` |
| 条件分支 | ❌ | `#[router]` → label → `#[listen(label)]` |
| 控制流 | 线性 DAG | `or("a","b")` / `and("a","b")` 组合器 |
| 嵌入 Comp | ❌ | 方法内调用 `hero.execute()` 或 `engine.run()` |

### 1.1 设计原则

- **Rust proc-macro DSL**：声明式属性宏标记方法依赖关系
- **状态由用户定义**：proc-macro 无法注入字段，用户手动声明 `state: T` 字段
- **零外部依赖**（core 之外）：纯内存执行，依赖 tokio + serde
- **Wrapper 方法模式**：绕过 `Box::pin(async { self.foo().await })` 生命周期限制
- **`#[flow_impl(crate = "...")]`**：支持 crate 内部和外部使用

---

## 2. 架构（实际）

```
tavern-flow/                          (crate)
├── Cargo.toml                        
└── src/
    └── lib.rs                        Flow, FlowDispatch, FlowEngine, FlowGraph, FlowHandle, FlowError

tavern-flow-macros/                   (crate, proc-macro = true)
├── Cargo.toml
└── src/
    └── lib.rs                        proc-macro 实现:
        ├── #[derive(Flow)]           标记 struct（空实现，实际 trait 由 flow_impl 生成）
        ├── #[start]                  pass-through 标记入口方法
        ├── #[listen(...)]            pass-through 标记监听方法
        ├── #[router("name")]         pass-through 标记路由方法
        └── #[flow_impl(crate="...")]  生成 FlowDispatch + Flow trait impl
```

---

## 3. proc-macro DSL（实际形态）

### 3.1 基本定义

```rust
use tavern_flow::{Flow, flow_impl, start, listen, FlowEngine, FlowError};

// 用户手动定义状态 struct 和 pipeline struct
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ContentState {
    topic: String,
    research_notes: Option<String>,
}

#[derive(Flow)]
struct ContentPipeline {
    state: ContentState,  // 用户手动声明 state 字段
    hero: Arc<TavernHero>,
}

#[flow_impl(crate = "tavern_flow")]
impl ContentPipeline {
    #[start]
    async fn research(&mut self) -> Result<String, FlowError> {
        let result = self.hero
            .execute("researcher", &self.state.topic, None)
            .await?;
        self.state.research_notes = Some(result.to_string());
        Ok(result.to_string())
    }

    #[listen("research")]
    async fn write(&mut self, notes: String) -> Result<String, FlowError> {
        // notes = research 的返回值（自动从上游输出注入）
        Ok(format!("article about: {}", notes))
    }
}

// 同步执行
let mut engine = FlowEngine::new(pipeline);
let result = engine.execute(json!({})).await?;

// 异步执行
let engine = FlowEngine::new(pipeline);
let mut handle = engine.start_async();
let result = handle.await_completion().await?;
```

### 3.2 条件路由

```rust
#[router("upstream_method")]
async fn quality_gate(&mut self, data: String) -> String {
    if data.len() > 500 { "approved".to_string() } else { "needs_revision".to_string() }
}

#[listen("approved")]
async fn on_approved(&mut self, data: String) -> Result<String, FlowError> {
    // data = router 接收的输入（即 upstream 的输出）
    Ok(format!("approved: {}", data))
}

#[listen("rejected")]   // label listeners 只接收匹配 label 的输入
async fn on_rejected(&mut self, data: String) -> Result<String, FlowError> {
    Ok(format!("rejected: {}", data))
}
```

`#[router]` 方法返回 `String`（label），引擎执行 router 后根据 label 触发对应的 `#[listen(label)]` 方法。Label listener 接收 router 的**输入**（上游输出），而非 router 的 label 输出。

### 3.3 控制流组合器

```rust
// OR: 任一上游完成即触发（传入第一个完成的值，仅执行一次）
#[listen(or("source_a", "source_b"))]
async fn consumer(&mut self, data: String) -> Result<String, FlowError> { ... }

// AND: 全部上游完成才触发
#[listen(and("first", "second"))]
async fn after_both(&mut self) -> Result<String, FlowError> { ... }
```

---

## 4. 宏展开机制（已实现）

### 4.1 `#[derive(Flow)]`

空 pass-through。`Flow` trait 的实际实现由 `#[flow_impl]` 生成。

### 4.2 `#[flow_impl(crate = "...")]` 展开

`#[flow_impl]` 对每个 flow 方法生成一个 **wrapper async fn**，解决 `Box::pin(async { self.method().await })` 的生命周期问题。

**输入：**
```rust
#[flow_impl(crate = "crate")]
impl MyPipeline {
    #[start]
    async fn step_a(&mut self) -> Result<String, FlowError> { ... }

    #[listen("step_a")]
    async fn step_b(&mut self, data: String) -> Result<String, FlowError> { ... }
}
```

**输出：**
```rust
impl MyPipeline {
    // 原始方法（已剥离 flow 属性）
    async fn step_a(&mut self) -> Result<String, FlowError> { ... }
    async fn step_b(&mut self, data: String) -> Result<String, FlowError> { ... }

    // 生成的 wrapper 方法
    async fn __flow_wrapper_step_a(&mut self) -> Result<Value, FlowError> {
        let result = self.step_a().await?;
        Ok(serde_json::to_value(result).map_err(|e| FlowError::Serialization(e.to_string()))?)
    }
    async fn __flow_wrapper_step_b(&mut self, data: String) -> Result<Value, FlowError> {
        let result = self.step_b(data).await?;
        Ok(serde_json::to_value(result).map_err(|e| FlowError::Serialization(e.to_string()))?)
    }
}

impl FlowDispatch for MyPipeline {
    fn dispatch(&mut self, method: &str, input: Value)
        -> Pin<Box<dyn Future<Output = Result<Value, FlowError>> + Send + '_>>
    {
        match method {
            "step_a" => Box::pin(self.__flow_wrapper_step_a()),
            "step_b" => {
                let parsed = serde_json::from_value(input);
                match parsed {
                    Ok(val) => Box::pin(self.__flow_wrapper_step_b(val)),
                    Err(e) => Box::pin(std::future::ready(Err(FlowError::Serialization(e.to_string())))),
                }
            }
            _ => Box::pin(std::future::ready(Err(FlowError::MethodNotFound { name: method.into() }))),
        }
    }
}

impl Flow for MyPipeline {
    fn metadata() -> FlowMetadata {
        FlowMetadata {
            methods: vec![
                MethodInfo { name: "step_a", is_start: true, ... },
                MethodInfo { name: "step_b", is_start: false, listen_type: ListenType::Single("step_a"), ... },
            ],
        }
    }
}
```

**关键设计决策：wrapper 方法模式**

直接 `Box::pin(async { self.method().await? })` 会导致生命周期错误（`self` 被 borrow 但 async block 需要 `'static`）。Wrapper 方法将 `self` 的 borrow 封装在 `async fn(&mut self)` 中，dispatch 只需调用 `Box::pin(self.wrapper())`，生命周期自动对齐。

### 4.3 OR/AND 语法

最终采用**扩展 `#[listen]` 属性语法**（spec 原 Option B 变体）：

```rust
#[listen("single_method")]          // 单项监听
#[listen(or("a", "b"))]             // OR 组合
#[listen(and("a", "b"))]            // AND 组合
```

`or()` 和 `and()` 不是独立的 proc-macro，而是在 `#[listen]` 属性内部解析的语法。这避免了双宏展开顺序问题（spec 原 Option A）。

---

## 5. Runtime API（实际）

### 5.1 Flow trait

```rust
pub trait Flow {
    fn metadata() -> FlowMetadata;
}
```

### 5.2 FlowDispatch trait

```rust
pub trait FlowDispatch {
    fn dispatch(
        &mut self,
        method: &str,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, FlowError>> + Send + '_>>;
}
```

### 5.3 FlowMetadata + MethodInfo + ListenType

```rust
pub struct FlowMetadata {
    pub methods: Vec<MethodInfo>,
}

pub struct MethodInfo {
    pub name: String,
    pub is_start: bool,
    pub is_router: bool,
    pub router_for: Option<String>,
    pub listen_type: ListenType,
}

pub enum ListenType {
    Single(String),
    Or(Vec<String>),
    And(Vec<String>),
}
```

### 5.4 FlowEngine

```rust
pub struct FlowEngine<F> {
    flow: F,
    graph: FlowGraph,
}

impl<F: Flow + FlowDispatch + Send + 'static> FlowEngine<F> {
    pub fn new(flow: F) -> Self;
    pub async fn execute(&mut self, inputs: Value) -> Result<Value, FlowError>;
    pub fn start_async(self) -> FlowHandle;      // 异步执行
}
```

**执行模型：**
- 顺序执行（同一时刻只执行一个方法）
- 从 `#[start]` 方法开始
- 方法完成后检查 router → 执行 router → 触发 label listeners
- 无 router 时，通过 DAG 入度计数触发下游
- OR: in_degree 初始为 1，首个上游完成触发
- AND: in_degree = 上游数，全部完成触发
- 触发后从 in_degree 移除，防止重复触发

### 5.5 FlowHandle

```rust
pub struct FlowHandle { ... }
impl FlowHandle {
    pub async fn await_completion(&mut self) -> Result<Value, FlowError>;
}
```

### 5.6 FlowError

```rust
pub enum FlowError {
    MethodNotFound { name: String },
    Serialization(String),
    Other(String),
}
```

---

## 6. 验证结果（原 §6 待验证问题）

| 问题 | 原始状态 | 结果 |
|---|---|---|
| `or!()` / `#[listen()]` 双宏展开顺序 | ❓ | ✅ 采用扩展 `#[listen(or(...))]` 语法，回避独立 proc-macro 展开问题 |
| `#[flow_impl]` 同时保留原 `impl` + 生成 `FlowDispatch` | ❓ | ✅ `#[flow_impl]` 生成两个独立 `impl` 块（原始方法 + dispatch wrapper） |
| `#[derive(Flow)]` 注入 `state` 字段 | ❓ | ❌ proc-macro 无法向 struct 注入字段。用户需手动声明 `state: T` |
| `Pin<Box<dyn Future>>` 生命周期 | ❓ | ✅ wrapper 方法模式：`async fn wrapper(&mut self) → dispatch` 调 `Box::pin(self.wrapper())` |
| serde 往返 | ❓ | ✅ 12 个测试覆盖: manual + macro dispatch, event loop, router, OR, AND, async start |
| 并行执行 | — | ❌ 当前顺序执行。并发执行（多个 start 方法 tokio::spawn）留待未来 |

---

## 7. 已知限制

| 限制 | 说明 |
|------|------|
| 顺序执行 | `FlowEngine` 一次只执行一个方法，不并行 |
| Router 方法必须返回 `String` | 返回类型硬编码为 `Value::String(label)` |
| Rust 类型限制 | 不支持 `Vec<String>` 多 label 返回（可后期扩展） |
| 无持久化 | 崩溃后无法恢复 flow 状态 |
| 无 REST API 集成 | Flow 尚未暴露为 HTTP 端点 |
| `Sleeping` / `Planner` 不与 Flow 交互 | 暂不集成 comp 的 Planning/Hierarchical |

---

## 8. 文件结构（实际）

```
crates/tavern-flow-macros/
├── Cargo.toml         # proc-macro = true, deps: syn, quote, proc-macro2
└── src/lib.rs         # 344 lines: derive(Flow), flow_impl, start, listen, router

crates/tavern-flow/
├── Cargo.toml         # deps: tavern-flow-macros, tavern-core, tokio, serde_json
└── src/lib.rs         # 779 lines: Flow, FlowDispatch, FlowEngine, FlowGraph, FlowHandle, FlowError, tests
```

---

## 9. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 0.1.0-draft | 2026-05-21 | 初稿：设计方向、待验证问题 |
| 0.1.0-frozen | 2026-05-26 | 原型验证通过，文档修订为实际实现：wrapper 方法模式、`#[listen(or/and)]` 语法、FlowHandle、已知限制 |
