# Tavern Flow Layer — Draft Spec

> **状态：DRAFT** — 此 spec 描述 Phase 2 的目标形态，但 proc-macro DSL 尚未通过原型验证。
> 本文档作为设计方向指引，具体实现细节需在原型验证后修订。

## 1. 概述

`tavern-flow` 提供与 CrewAI Flow 等价的能力：**方法级事件驱动编排**。与 tavern-comp Workflow 的核心差异：

| | Workflow (tavern-comp) | Flow (tavern-flow) |
|---|---|---|
| 定义方式 | YAML 配置文件 | Rust struct + proc-macro |
| 步骤类型 | 必须绑定 Agent (`agent_id`) | 任意异步 Rust 方法 |
| 状态类型 | JSON `Value`（弱类型） | 泛型 `FlowState`（编译时类型安全） |
| 依赖声明 | `depends_on: [step_id]` | `#[listen("method_name")]` |
| 条件分支 | ❌ | `#[router]` → label → `#[listen(label)]` |
| 流式输出 | ❌ | `#[stream]` |
| 嵌入 Crew | ❌ | 方法内调用 `hero.execute()` 或 `engine.run()` |

### 1.1 设计原则

- **Rust proc-macro DSL**：用宏模拟 Python 装饰器的声明风格
- **零外部依赖**：默认内存执行 + 可选 SQLite 持久化
- **与 comp 复用基础设施**：`ExecutionHandle`、`EventStore` 共享接口
- **Phase 2 暂不实现** `[persist]`、`[stream]` 等高级特性——先验证核心宏 DSL

---

## 2. 架构

```
tavern-flow/                          (crate)
├── Cargo.toml                        dependencies: tavern-flow-macros, tavern-core, tokio, serde
└── src/
    ├── lib.rs
    ├── engine.rs                     FlowEngine: 方法图事件循环
    ├── graph.rs                      FlowGraph: 方法节点 + 边 + 拓扑排序
    ├── state.rs                      FlowState trait
    ├── handle.rs                     FlowHandle (仿 ExecutionHandle)
    └── error.rs                      FlowError

tavern-flow-macros/                   (crate, proc-macro = true)
├── Cargo.toml
└── src/
    └── lib.rs                        proc-macro 实现:
        ├── #[derive(Flow)]           展开为 Flow trait impl
        ├── #[start]                  标记入口方法
        ├── #[listen("name")]         标记监听方法
        ├── #[router("name")]         标记路由方法
        └── or!() / and!()           控制流组合器
```

---

## 3. proc-macro DSL（目标形态）

### 3.1 基本定义

```rust
use tavern_flow_macros::{Flow, start, listen};
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
struct ContentState {
    topic: String,
    research_notes: Option<String>,
    draft: Option<String>,
}

// #[derive(Flow)] 展开为 impl Flow for ContentPipeline
#[derive(Flow)]
#[flow(state = ContentState)]
struct ContentPipeline {
    hero: Arc<TavernHero>,
}

impl FlowImpl for ContentPipeline {
    fn init(&mut self) -> &mut ContentState {
        &mut self.state  // #[derive(Flow)] 注入的 state 字段
    }
}

#[flow_impl]
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
        let result = self.hero
            .execute("writer", &notes, None)
            .await?;
        self.state.draft = Some(result.to_string());
        Ok(result.to_string())
    }
}

// 使用
let flow = ContentPipeline::new(hero);
let result = flow.execute(json!({"topic": "AI trends"})).await?;
```

### 3.2 条件路由

```rust
#[router("write")]
async fn quality_gate(&mut self, draft: String) -> String {
    if draft.len() > 500 { "approved".into() } else { "needs_revision".into() }
}

#[listen("approved")]
async fn finalize(&mut self, draft: String) -> Result<(), FlowError> { Ok(()) }

#[listen("needs_revision")]
async fn revise(&mut self, draft: String) -> Result<String, FlowError> { ... }
```

`#[router]` 返回值是 label 字符串，触发对应 `#[listen(label)]`。返回值支持多个 label（分叉执行），此时返回 `Vec<String>`。

### 3.3 控制流组合器

```rust
// or!: 任一上游完成即触发（传入第一个完成的值）
#[listen(or!("load_cache", "fetch_remote"))]
async fn process(&mut self, data: String) -> Result<(), FlowError>;

// and!: 全部完成才触发（不传参数，从 state 读取）
#[listen(and!("validate_input", "authenticate_user"))]
async fn execute(&mut self) -> Result<(), FlowError>;
```

---

## 4. 宏展开机制（待原型验证）

### 4.1 `#[derive(Flow)]` 展开

```
输入:                                   输出:
#[derive(Flow)]                         
#[flow(state = ContentState)]           impl Flow for ContentPipeline {
struct ContentPipeline {                    fn metadata() -> FlowMetadata { ... }
    hero: Arc<TavernHero>,              }
}                                       
                                        impl ContentPipeline {
                                            // 注入 state 字段
                                            pub state: ContentState,

                                            fn new(hero: Arc<...>, state: ContentState) -> Self { ... }
                                        }
```

`content_pipeline.rs` → macro expansion → generated `impl Flow`

### 4.2 `#[flow_impl]` + `#[start]` / `#[listen]` 展开

```
输入:
#[flow_impl]
impl ContentPipeline {
    #[start]
    async fn research(&mut self) -> Result<String, FlowError> { ... }
    #[listen("research")]
    async fn write(&mut self, data: String) -> Result<String, FlowError> { ... }
}

输出:
impl ContentPipeline {
    // 原始方法体保留
    async fn research(&mut self) -> Result<String, FlowError> { ... }
    async fn write(&mut self, data: String) -> Result<String, FlowError> { ... }
}

impl FlowDispatch for ContentPipeline {
    fn dispatch(
        &mut self,
        method: &str,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, FlowError>> + '_>> {
        match method {
            "research" => Box::pin(async {
                Ok(serde_json::to_value(self.research().await?)?)
            }),
            "write" => Box::pin(async {
                let data: String = serde_json::from_value(input)?;
                Ok(serde_json::to_value(self.write(data).await?)?)
            }),
            _ => Err(FlowError::MethodNotFound { name: method.into() }),
        }
    }
}
```

### 4.3 `or!()` / `and!()` 与 `#[listen]` 的双宏协作

这是 Phase 2 最大技术挑战。`or!("a", "b")` 必须先展开为某种 token 表示，然后 `#[listen(...)]` 属性宏才能解析：

```
选项 A: or!("a", "b") 展开为字符串 "or::a::b"
        #[listen("or::a::b")] → FlowGraph 中标记为 OrEdge

选项 B: 使用 helper attribute #[listen(or = ["a", "b"])]
        避免嵌套宏问题，但语法不如 CrewAI 优雅

选项 C: or!("a", "b") 展开为特定类型标记
        proc-macro 中的 `TokenStream` 替换
```

**验证结论**选项 B 最可行，选项 A 需要确认 proc-macro 展开顺序。原型验证后再确定。

---

## 5. Flow Engine

```rust
// tavern-flow/src/engine.rs

pub struct FlowEngine<F: Flow> {
    flow: F,
    graph: FlowGraph,
}

impl<F: Flow + FlowDispatch> FlowEngine<F> {
    pub fn new(flow: F) -> Self {
        let graph = FlowGraph::from_metadata(F::metadata());
        Self { flow, graph }
    }

    /// 同步执行
    pub async fn execute(&mut self, inputs: serde_json::Value) -> Result<serde_json::Value, FlowError> {
        // 1. 初始化 state
        // 2. 找到所有 @start 方法 → 并行执行
        // 3. 事件循环：方法完成 → 检查依赖 → 触发下游
        // 4. 最后一个方法完成 → 返回结果
        todo!()
    }

    /// 异步启动
    pub async fn start(self, inputs: serde_json::Value) -> Result<FlowHandle, FlowError>;
}
```

```rust
// tavern-flow/src/graph.rs

struct FlowGraph {
    nodes: HashMap<String, MethodNode>,
}

struct MethodNode {
    name: String,
    is_start: bool,
    is_router: bool,
    listeners: Vec<Listener>,
}

enum Listener {
    Single(String),          // #[listen("name")]
    Or(Vec<String>),         // or!("a", "b")
    And(Vec<String>),        // and!("a", "b")
    RouterLabel(String),     // #[router] → label
}
```

---

## 6. 待验证问题

| 问题 | 状态 | 验证方式 |
|---|---|---|
| `or!()` / `#[listen()]` 双宏展开顺序 | ❓ | 写最小 proc-macro 测试 |
| `#[flow_impl]` 是否可同时保留原 `impl` 块 + 生成 `FlowDispatch` | ❓ | 确认 syn/quote 能否导出两个 `impl` |
| `#[derive(Flow)]` 注入 `state` 字段到 struct | ❓ | proc-macro attribute 可否修改 struct 定义 |
| 异步方法重载（`execute_method` 中 `Pin<Box<dyn Future>>`）的生命周期 | ❓ | 编译验证 |
| `serde_json::from_value` / `to_value` 的序列化往返是否满足所有类型 | ❓ | 单元测试覆盖 |

---

## 7. 下一步

1. **创建 `tavern-flow-macros` playground crate**（最小可编译 proc-macro）
2. 实现 `#[start]` + `#[listen("single_method")]` 的基本展开
3. 验证展开后的代码可编译、可执行
4. 根据原型结果修订此 spec
5. 补充 REST API、错误处理、持久化等章节
