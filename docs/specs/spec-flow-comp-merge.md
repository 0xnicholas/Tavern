# Tavern Flow-Comp 合并 — Spec

> **状态：REVIEWED** — 2026-06-03 (2 轮审查，0 阻塞，8 个细节修复)  
> 将 `tavern-flow` 从独立编排引擎重构为 `tavern-comp` 的 Rust proc-macro DSL 前端，消除 DAG 调度、事件循环、持久化的代码重复。

## 1. 动机

### 1.1 当前问题

`tavern-comp`（YAML Workflow 引擎）和 `tavern-flow`（proc-macro 方法编排）是两个独立的编排引擎，共享以下重叠：

| 重叠区 | Comp 实现 | Flow 实现 | 代码量 |
|--------|-----------|-----------|--------|
| DAG 调度 | `decide_next_action` (40 行) | `FlowGraph::on_complete` (15 行) | ~55 行 |
| 事件循环 | `run_interpreter_loop` (200 行) | `execute_inner` (70 行) | ~270 行 |
| 断点 | `Step.breakpoint` + event | `MethodInfo.breakpoint` + event | 2× |
| Webhook | `workflow.webhook` + `send_webhook()` | `engine.webhook` → 同一函数 | 2× |
| 持久化 | `EventStore` trait | 通过 `WorkflowEvent::External` 复用 | 寄生 |
| 注册表 | `WorkflowRegistry` | `FlowRegistry` | 2× |
| 执行句柄 | `ExecutionHandle` | `FlowHandle` + `FlowHandleRef` | 3× 类型 |

此外 Flow 已依赖 Comp（`EventStore`、`WebhookConfig`、`send_webhook`），形成循环寄生关系。

### 1.2 目标

**一个执行引擎，两种定义方式：**

- **YAML 路径**（不变）：用户写 YAML → `Workflow` → `WorkflowEngine`
- **Rust DSL 路径**（新增）：用户写 `#[start]`/`#[listen]` → `#[flow_impl]` 展开为 `Workflow` + `FlowStepExecutor` → 同一个 `WorkflowEngine`

删除 `FlowEngine`、`FlowGraph`、`FlowEvent`、`FlowHandle`/`FlowHandleRef`、`FlowRegistry`，将 `tavern-flow` 缩减为 proc-macro DSL + `FlowStepExecutor` trait（~200 行，当前 1137 行）。

---

## 2. 架构

```
                    ┌──────────────────────────────┐
                    │     tavern-flow (thin)        │
                    │  ┌──────────────────────────┐ │
                    │  │ tavern-flow-macros       │ │
                    │  │ #[start] #[listen]       │ │
                    │  │ #[router] #[flow_impl]   │ │
                    │  └──────────┬───────────────┘ │
                    │             │ 展开为            │
                    │  ┌──────────▼───────────────┐ │
                    │  │ Workflow + Step 定义      │ │
                    │  │ FlowStepExecutor impl     │ │
                    │  └──────────┬───────────────┘ │
                    └─────────────┼─────────────────┘
                                  │ 委托
                    ┌─────────────▼─────────────────┐
                    │        tavern-comp             │
                    │  WorkflowEngine                │
                    │  decide_next_action (增强)     │
                    │  EventStore / Signals / Cron   │
                    └───────────────────────────────┘
```

**数据流**：
```
用户写 #[start] async fn research(&mut self) → 
  #[flow_impl] 展开为:
    Step { id: "research", agent_id: "__flow__", depends_on: [], ... }
    FlowStepExecutor::execute_step("research", input)
  →
WorkflowEngine::start(generated_workflow, inputs) →
  decide_next_action 发现 ready step →
    agent_id == "__flow__" → 调 FlowStepExecutor 而非 Hero →
  方法返回 → StepCompleted event → 更新入度 → 下一批
```

---

## 3. 数据模型变化

### 3.1 Step 新增字段（`tavern-comp`）

```rust
/// 哨兵值：标记此 step 由 Flow 方法执行，非 Agent 调用。
pub const FLOW_AGENT_ID: &str = "__flow__";

pub struct Step {
    // ── 现有字段（不变）──
    pub id: String,
    pub agent_id: String,
    pub task: String,
    pub depends_on: Vec<String>,
    pub output_key: Option<String>,
    pub timeout: Option<u64>,
    pub retries: Option<u64>,
    pub retry_delay: Option<u64>,
    pub wait_for_signal: Option<String>,
    pub signal_timeout: Option<u64>,
    pub signal_timeout_action: Option<SignalTimeoutAction>,
    pub breakpoint: bool,
    pub model_override: Option<tavern_core::ModelConfig>,
    pub expected_output: Option<String>,

    // ── V0.4 新增 ──
    /// OR 依赖：任一上游完成即触发。与 depends_on 互斥。
    #[serde(default)]
    pub or_depends_on: Vec<String>,

    /// Router 配置。非 None 时此 step 执行后产生 label(s) 触发下游。
    #[serde(default)]
    pub router: Option<RouterConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    /// 路由输入来源 step_id
    pub upstream: String,
}
```

**约束**（在 `validate_static()` + `validate_dag()` 中强制）：
- `depends_on` 和 `or_depends_on` 互斥——两者都非空时报 `ConfigParse` 错
- `or_depends_on` 中引用的 step_id 必须存在于 Workflow 的 steps 中（存在性检查）——**但 `__label__` 前缀的条目跳过此检查**（它们是 Router 运行时产生的 label，非静态 step ID）
- OR 边参与 DAG 环检测：`build_dag_maps()` 需同时处理 `depends_on` 和 `or_depends_on` 中的非 label 条目，防止 OR 边产生死锁。`__label__` 条目跳过环检测
- `router` 非 None 时，`depends_on` 必须包含 `router.upstream`（router 依赖其上游）

### 3.2 新增 `FlowStepExecutor` trait（`tavern-comp`）

```rust
use std::pin::Pin;
use std::future::Future;
use serde_json::Value;

/// Flow 方法步骤执行器。由 #[flow_impl] proc-macro 自动实现。
/// 引擎遇到 agent_id == FLOW_AGENT_ID 时，通过此 trait 执行步骤。
pub trait FlowStepExecutor: Send + 'static {
    fn execute_step(
        &mut self,
        step_id: &str,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>>;
}
```

### 3.3 `WorkflowEngine` — hero 改为 Option

```rust
pub struct WorkflowEngine {
    /// Agent 执行器。None = Flow 模式（不使用 Hero）。
    hero: Option<Arc<TavernHero>>,
    store: Arc<dyn EventStore>,
    max_concurrency: usize,
    /// Flow 方法执行器（None = 纯 Comp 模式）
    flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
}
```

- **Comp 模式**：`hero` 为 `Some(...)`, `flow_executor` 为 `None`
- **Flow 模式**：`hero` 为 `None`, `flow_executor` 为 `Some(...)`
- **混合模式**（同一 Workflow 含 Flow step 和 Hero step）：两者都为 `Some(...)`
- `validate()` 中，按 step 粒度跳过 Agent 检查：仅当 `step.agent_id != FLOW_AGENT_ID` 且 `hero.is_some()` 时验证 Agent 存在性
- `StepExecutor::execute_once()` 中，当 `agent_id == FLOW_AGENT_ID` 时走 Flow 路径，不会访问 `hero`

这同时解决了 **B1**（`validate()` 拒绝 `__flow__` steps）和 **B2**（不需要 `TavernHero::empty()`）。

### 3.4 删除的 Flow 类型

| 删除 | 原因 |
|------|------|
| `FlowEngine<F>` | 执行逻辑合并到 `WorkflowEngine` |
| `FlowGraph` | DAG 逻辑合并到 `decide_next_action` |
| `FlowHandle` / `FlowHandleRef` | 用 `ExecutionHandle` 替代 |
| `FlowEvent` (7 variants) | 不再需要独立的 Flow 事件类型 |
| `FlowMetadata` / `MethodInfo` / `ListenType` | 展开时直接映射到 Comp 数据结构 |
| `FlowRegistry` / `FlowFactory` / `StartableFlow` / `FlowSummary` | 用 `WorkflowRegistry` 替代 |
| `Flow` trait | 删除，`__workflow_definition()` 替代 |
| `FlowDispatch` trait | 重命名为 `FlowStepExecutor`（语义更明确） |
| `FlowError` | 保留，作为 Flow 方法的错误类型 |

---

## 4. 引擎改造

### 4.1 `decide_next_action` — OR 依赖支持

当前逻辑仅处理 `depends_on`（AND 语义）。改造后使用 `validator::build_dag_maps()` 的增强版作为单一真相源：

```rust
fn decide_next_action(
    &self,
    workflow: &Workflow,
    state: &InstanceState,
) -> Result<Action, CompError> {
    // ... 终态检查不变 ...

    // 使用增强后的 build_dag_maps（已处理 depends_on + or_depends_on）
    let dag = crate::validator::build_dag_maps(workflow);
    let mut in_degree = dag.in_degree;
    let or_steps = dag.or_steps; // HashSet<String>: step_id 是 OR step

    // 减去已完成步骤的贡献
    for completed in &state.completed_steps {
        for step in &workflow.steps {
            if step.depends_on.contains(completed) {
                if let Some(d) = in_degree.get_mut(&step.id) {
                    *d = d.saturating_sub(1);
                }
            }
            if step.or_depends_on.contains(completed) {
                // OR：任一上游完成即清零（触发执行）
                in_degree.insert(step.id.clone(), 0);
            }
        }
    }

    // 就绪步骤 = 入度为 0 + 未完成/未运行/未被阻塞/未被调度
    let ready: Vec<String> = workflow.steps.iter()
        .filter(|s| {
            in_degree.get(&s.id).copied().unwrap_or(0) == 0
                && !state.completed_steps.contains(&s.id)
                && !state.running_steps.contains(&s.id)
                && !state.signal_blocked_steps.contains(&s.id)
                && !state.scheduled_steps.contains(&s.id)
        })
        .map(|s| s.id.clone())
        .collect();

    // ... all_done 检查不变 ...
}
```

**注意**：由于 `depends_on` 和 `or_depends_on` 互斥（§3.1 约束），subtraction 循环中的两个 `if` 分支不会同时命中同一个 step。OR step 的 `in_degree` 初始值为 1，完成时直接置零；AND step 的 `in_degree` = `depends_on.len()`，逐步递减。

### 4.2 Router 处理

Router step 执行完成后，其输出（String 或 `Vec<String>`）作为 label(s) 注入 `completed_steps`，触发 `or_depends_on` 中包含该 label 的下游 step。

**Router 数据流**（重要）：
1. Router 方法的**输入**来自 `router.upstream` 的 step 输出
2. Router 方法的**输出**是 label(s)——仅用于路由决策
3. 下游 label-listener 方法的**输入**是 `router.upstream` 的原始输出（非 router 输出）

这意味着 Router 只能路由，不能转换数据。如需转换+路由，应在上游和 Router 之间插入独立方法。

**Label 命名约定**：Router 注入 `completed_steps` 时使用 `__label__<label>` 前缀，避免与真实 step ID 冲突。**同时必须将上游输出写入 context**，因为 `resolve_flow_input()` 从 context 读取下游输入：

```rust
const ROUTER_LABEL_PREFIX: &str = "__label__";

// 在 run_interpreter_loop 的 StepCompleted 事件处理中：
WorkflowEvent::StepCompleted { step_id, output, .. } => {
    // ... 现有 context 更新不变 ...

    // V0.4: Router 路由
    let step = workflow.steps.iter().find(|s| s.id == step_id);
    if let Some(step) = step {
        if let Some(ref router) = step.router {
            // 获取上游原始输出（下游 listener 需要此数据）
            let upstream_output = state.context.get(&router.upstream).cloned()
                .unwrap_or(Value::Null);
            let labels: Vec<String> = extract_labels_from_output(output);
            for label in &labels {
                let label_key = format!("{}{}", ROUTER_LABEL_PREFIX, label);
                // ① 将上游数据存入 context（供 resolve_flow_input 读取）
                if let Some(obj) = state.context.as_object_mut() {
                    obj.insert(label_key.clone(), upstream_output.clone());
                }
                // ② 注入 completed_steps（触发 OR 入度归零）
                state.completed_steps.insert(label_key);
            }
        }
    }
}
```

相应的，`#[listen("approved")]` 展开为 `or_depends_on: ["__label__approved"]`，确保 router label 命中正确的下游。

`extract_labels_from_output` 从 `Value::String` 提取单 label，从 `Value::Array` 提取多 label：

```rust
fn extract_labels_from_output(output: &Value) -> Vec<String> {
    match output {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        _ => vec![],
    }
}
```

### 4.3 `StepExecutor` — Flow 哨兵路由

`StepExecutor::execute_once` 检查 `agent_id`：

```rust
async fn execute_once(
    step: &Step,
    context: &Value,
    hero: Option<&TavernHero>,  // Flow 模式下为 None
    flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
) -> Result<Value, String> {
    if step.agent_id == FLOW_AGENT_ID {
        let executor = flow_executor.as_ref()
            .ok_or_else(|| "flow executor not configured".to_string())?;
        let mut guard = executor.lock().await;

        // Flow step 的 task 字段存储方法名（如 "research"）
        // input 来自上游 step 的输出（从 context 取）
        let input = resolve_flow_input(step, context);
        guard.execute_step(&step.task, input).await
    } else {
        // 原有 Hero 调用逻辑不变
        let hero = hero.ok_or_else(|| "hero not configured".to_string())?;
        let task = render_template(&step.task, context)?;
        let timeout = step.timeout.unwrap_or(300);
        let fut = hero.execute(&step.agent_id, &task, Some(context.clone()));
        match tokio::time::timeout(Duration::from_secs(timeout), fut).await {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!("step timed out after {}s", timeout)),
        }
    }
}

/// 为 Flow step 解析输入：取第一个依赖 step 的输出。
fn resolve_flow_input(step: &Step, context: &Value) -> Value {
    // Router step: 输入来自 router.upstream
    if let Some(ref router) = step.router {
        return context.get(&router.upstream).cloned().unwrap_or(Value::Null);
    }
    // 普通 step: AND 取 depends_on，OR 取 or_depends_on
    let upstreams: Vec<&str> = if !step.depends_on.is_empty() {
        step.depends_on.iter().map(|s| s.as_str()).collect()
    } else {
        step.or_depends_on.iter().map(|s| s.as_str()).collect()
    };
    upstreams.first()
        .and_then(|id| context.get(id))
        .cloned()
        .unwrap_or(Value::Null)
}
```

### 4.4 `WorkflowEngine` 构造器

```rust
impl WorkflowEngine {
    /// Comp 模式（现有 API，签名不变）。
    pub fn new(hero: Arc<TavernHero>) -> Self {
        Self {
            hero: Some(hero),
            store: Arc::new(MemoryEventStore::new()),
            max_concurrency: usize::MAX,
            flow_executor: None,
        }
    }

    /// Flow 模式：不依赖 TavernHero，使用 FlowStepExecutor。
    pub fn new_with_flow_executor(
        executor: Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>,
    ) -> Self {
        Self {
            hero: None,
            store: Arc::new(MemoryEventStore::new()),
            max_concurrency: 1,
            flow_executor: Some(executor),
        }
    }
}
```

`validate()` 调整：当 `self.hero.is_none()` 时跳过 Agent 存在性检查。

---

## 5. Proc-macro 展开变化

### 5.1 当前展开 vs V0.4 展开

| 当前生成 | V0.4 生成 |
|----------|-----------|
| `FlowDispatch` impl | `FlowStepExecutor` impl |
| `Flow` trait impl（metadata） | `__workflow_definition()` 静态方法 |
| Wrapper 方法 | Wrapper 方法（不变） |
| — | `run(inputs)` 便捷方法 |
| — | `start_async()` 便捷方法 |

### 5.2 属性映射规则

| Proc-macro 属性 | 展开为 |
|-----------------|--------|
| `#[start]` | `Step { depends_on: [], or_depends_on: [], router: None }` |
| `#[start]` | `Step { depends_on: [], or_depends_on: [], router: None }` |
| `#[listen("method_name")]`（引用方法名） | `Step { depends_on: [], or_depends_on: ["method_name"], router: None }` |
| `#[listen("label")]`（引用 Router label） | `Step { depends_on: [], or_depends_on: ["__label__label"], router: None }` |
| `#[listen(or("a","b"))]` | `Step { depends_on: [], or_depends_on: ["__label__a","__label__b"], router: None }` |
| `#[listen(and("a","b"))]` | `Step { depends_on: ["a","b"], or_depends_on: [], router: None }` |
| `#[router("upstream")]` | `Step { id: "__router__<name>", depends_on: ["upstream"], router: Some(RouterConfig { upstream: "upstream" }) }` |

**前缀自动判断规则**：proc-macro 展开时扫描同一 `#[flow_impl]` 块中的所有方法名。`#[listen("x")]` 若 x 是方法名 → 无前缀；若 x 不是方法名 → `__label__` 前缀（视为 Router label）。`or()` / `and()` 中的参数同理。

**注意**：
- `#[listen("method_name")]` 用 OR（非 AND），因为上游只有 1 个，OR/AND 行为等价。
- `#[listen(and("a","b"))]` 引用的是真实 step ID（无前缀），因为这些是直接方法依赖。
- 所有非 Router 的 Flow step 设 `output_key: Some(step_id.clone())`，确保上游输出进入 context 供下游 `resolve_flow_input()` 读取（**解决 B3**）。
- Router step 不设 `output_key`（Router 输出仅用于路由，不存入 context）。
- **静态验证限制**：`#[listen("label")]` 无法在编译时验证 label 是否会被某个 Router 产生。运行时若 label 从未出现，下游步骤永不触发（Workflow 挂起）。这是已知限制。

### 5.3 展开示例

**输入：**
```rust
#[derive(Flow)]
struct ContentPipeline { state: ContentState }

#[flow_impl(crate = "tavern_flow")]
impl ContentPipeline {
    #[start]
    async fn research(&mut self) -> Result<String, FlowError> { ... }

    #[listen("research")]
    async fn write(&mut self, notes: String) -> Result<String, FlowError> { ... }

    #[router("write")]
    async fn quality_gate(&mut self, data: String) -> String {
        if data.len() > 500 { "approved".into() } else { "rejected".into() }
    }

    #[listen("approved")]
    async fn publish(&mut self, data: String) -> Result<String, FlowError> { ... }
}
```

**输出 — `__workflow_definition()`：**
```rust
impl ContentPipeline {
    fn __workflow_definition() -> tavern_comp::Workflow {
        tavern_comp::Workflow {
            id: "ContentPipeline".to_string(),
            name: "ContentPipeline".to_string(),
            steps: vec![
                // #[start]
                Step {
                    id: "research".into(),
                    agent_id: tavern_comp::FLOW_AGENT_ID.into(),
                    task: "research".into(),
                    depends_on: vec![],
                    or_depends_on: vec![],
                    output_key: Some("research".into()),
                    router: None,
                    ..Default::default()
                },
                // #[listen("research")]
                Step {
                    id: "write".into(),
                    agent_id: tavern_comp::FLOW_AGENT_ID.into(),
                    task: "write".into(),
                    depends_on: vec![],
                    or_depends_on: vec!["research".into()],  // 直接方法依赖，无前缀
                    output_key: Some("write".into()),
                    router: None,
                    ..Default::default()
                },
                // #[router("write")]
                Step {
                    id: "__router__quality_gate".into(),
                    agent_id: tavern_comp::FLOW_AGENT_ID.into(),
                    task: "quality_gate".into(),
                    depends_on: vec!["write".into()],
                    or_depends_on: vec![],
                    router: Some(RouterConfig { upstream: "write".into() }),
                    ..Default::default()
                },
                // #[listen("approved")] → Router label，带 __label__ 前缀
                Step {
                    id: "publish".into(),
                    agent_id: tavern_comp::FLOW_AGENT_ID.into(),
                    task: "publish".into(),
                    depends_on: vec![],
                    or_depends_on: vec!["__label__approved".into()],
                    output_key: Some("publish".into()),
                    router: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }
}
```

**输出 — `FlowStepExecutor` impl：**
```rust
impl tavern_comp::FlowStepExecutor for ContentPipeline {
    fn execute_step(
        &mut self,
        step_id: &str,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        match step_id {
            "research" => Box::pin(async {
                self.__flow_wrapper_research().await.map_err(|e| e.to_string())
            }),
            "write" => {
                let data: String = serde_json::from_value(input).map_err(|e| e.to_string())?;
                Box::pin(async move {
                    self.__flow_wrapper_write(data).await.map_err(|e| e.to_string())
                })
            }
            "__router__quality_gate" => {
                let data: String = serde_json::from_value(input).map_err(|e| e.to_string())?;
                Box::pin(async move {
                    self.__flow_wrapper_quality_gate(data).await.map_err(|e| e.to_string())
                })
            }
            "publish" => {
                let data: String = serde_json::from_value(input).map_err(|e| e.to_string())?;
                Box::pin(async move {
                    self.__flow_wrapper_publish(data).await.map_err(|e| e.to_string())
                })
            }
            _ => Box::pin(std::future::ready(Err(format!("method not found: {}", step_id)))),
        }
    }
}
```

**输出 — 便捷方法：**
```rust
impl ContentPipeline {
    pub async fn run(self, inputs: Value) -> Result<Value, FlowError> {
        let workflow = Self::__workflow_definition();
        let executor = Arc::new(tokio::sync::Mutex::new(self));
        let engine = tavern_comp::WorkflowEngine::new_with_flow_executor(executor);
        let result = engine.run(&workflow, inputs).await
            .map_err(|e| FlowError::Other(e.to_string()))?;
        Ok(result.outputs)
    }

    pub fn start_async(self) -> (/* ... */) {
        // 内部调用 WorkflowEngine::start() + ExecutionHandle
    }

    pub fn with_store(self, store: Arc<dyn EventStore>) -> Self { ... }
    pub fn with_webhook(self, webhook: WebhookConfig) -> Self { ... }
    pub fn with_max_concurrency(self, n: usize) -> Self { ... }
}
```

---

## 6. 公开 API 变化

### 6.1 用户代码迁移

```rust
// Before (V0.3.x):
let mut engine = FlowEngine::new(pipeline);
let result = engine.execute(json!({})).await?;

let (mut handle, _ref) = engine.start_async();
let result = handle.await_completion().await?;

// After (V0.4):
let result = pipeline.run(json!({})).await?;

// 带配置：
let result = pipeline
    .with_store(store)
    .with_max_concurrency(4)
    .run(json!({}))
    .await?;
```

### 6.2 删除的公共 API

`tavern-flow` 不再导出：
- `FlowEngine`、`FlowGraph`、`FlowHandle`、`FlowHandleRef`
- `FlowEvent`、`FlowMetadata`、`MethodInfo`、`ListenType`
- `FlowRegistry`、`FlowFactory`、`StartableFlow`、`FlowSummary`
- `Flow` trait、`FlowDispatch` trait
- `FlowError::MethodNotFound`（未使用的方法直接在 match 中报错）

`tavern-flow` 保留导出：
- `FlowError`（方法返回类型）
- `#[derive(Flow)]`（空 derive macro，向后兼容）
- `#[flow_impl]`、`#[start]`、`#[listen]`、`#[router]`

`tavern-comp` 新增导出：
- `FLOW_AGENT_ID`
- `FlowStepExecutor` trait
- `RouterConfig`
- `WorkflowEngine::new_with_flow_executor()`

### 6.3 服务器端点

`/flows/:id/start`、`/flows/:id/status`、`/flows/:id/cancel` — 保留，实现改为通过 `WorkflowRegistry` 注册 Flow 生成的 Workflow 定义，执行走 `WorkflowEngine`。

---

## 7. 测试策略

### 7.1 `tavern-comp` 新增测试

| 测试 | 覆盖 |
|------|------|
| `test_or_depends_single_upstream` | 一个 OR 上游完成即触发下游 |
| `test_or_depends_first_of_two` | 两个 OR 上游，第一个完成触发，第二个被忽略 |
| `test_and_depends_both_required` | 两个 AND 上游，全部完成才触发 |
| `test_router_single_label` | Router 返回单 String，触发对应 label listener |
| `test_router_multi_label` | Router 返回 `Vec<String>`，触发多个 label listener |
| `test_flow_agent_id_routes_to_executor` | `agent_id == "__flow__"` 走 FlowStepExecutor |
| `test_mixed_flow_and_hero_steps` | 同一 Workflow 含 Flow step 和普通 Hero step |
| `test_or_and_mutual_exclusion_validation` | `depends_on` 和 `or_depends_on` 都非空时报错 |

### 7.2 `tavern-flow` 删除的测试

当前 12 个测试全部删除（测试 FlowEngine/FlowGraph 逻辑，合并后由 Comp 测试覆盖）。

### 7.3 `tavern-flow-macros` 新增测试

| 测试 | 覆盖 |
|------|------|
| `test_macro_generates_workflow_definition` | 验证 `__workflow_definition()` 输出正确的 Step 列表 |
| `test_macro_generates_flow_step_executor` | 验证 `FlowStepExecutor::execute_step` 路由正确 |
| `test_macro_generates_run_method` | 验证 `pipeline.run()` 端到端可调用 |
| `test_or_and_router_attribute_parsing` | 各种 proc-macro 属性组合解析正确 |

### 7.4 回归测试

Comp 现有所有测试必须保持通过（AND DAG 逻辑不变，`decide_next_action` 改动是纯增量的）。

---

## 8. 实施顺序

| Phase | Crate | 内容 | 破坏性 |
|-------|-------|------|:---:|
| **A** | `tavern-comp` | `Step` 加 `or_depends_on` + `RouterConfig` 字段 | 否 |
| **A2** | `tavern-comp` | `build_dag_maps()` 增强：处理 `or_depends_on` 边 + 存在性检查 + 环检测 + 互斥检查 | 否 |
| **B** | `tavern-comp` | `decide_next_action` 使用增强后的 `build_dag_maps` 支持 OR 依赖 | 否 |
| **C** | `tavern-comp` | 新增 `FLOW_AGENT_ID` + `FlowStepExecutor` trait | 否 |
| **D** | `tavern-comp` | `WorkflowEngine.hero` 改为 `Option<Arc<TavernHero>>` + `new_with_flow_executor()` 构造器，`validate()` 中 `hero.is_none()` 时跳过 Agent 检查 | 否 |
| **E** | `tavern-comp` | `StepExecutor::execute_once` Flow 哨兵路由 | 否 |
| **F** | `tavern-comp` | Router 事件处理逻辑（`StepCompleted` 分支 + `__label__` 前缀） | 否 |
| **🔍** | **CHECKPOINT** | 手写 `Workflow`（含 `agent_id: "__flow__"`）+ mock `FlowStepExecutor` → `engine.run()` 端到端通过 | — |
| **G** | `tavern-flow-macros` | `#[flow_impl]` 展开改为 Workflow + FlowStepExecutor（含 `output_key` 设置 + label 前缀映射） | **是** |
| **H** | `tavern-flow` | 删除 `FlowEngine`/`FlowGraph`/`FlowEvent`/`FlowHandle` | **是** |
| **I** | `tavern-flow` | 新增 `run()`/`start_async()`/`with_*()` 便捷方法 | 是 |
| **J** | `tavern-server` | `/flows` 端点适配 WorkflowRegistry | 否 |
| **K** | 全仓 | 删除旧 Flow 测试，添加新测试，验证全仓通过 | — |

**Checkpoint 说明**：Phase F 完成后、Phase G 开始前，必须用一个手写的 `Workflow` + mock `FlowStepExecutor` 验证 Comp 引擎能正确执行 Flow 语义的 step。这是 go/no-go 门槛——如果验证失败，Phase G 不应开始。

---

## 9. 已知限制

### 9.1 Flow 步骤间串行执行

`FlowStepExecutor` 使用 `tokio::sync::Mutex`，因此同一 Flow pipeline 的多个方法**不能并行执行**——引擎持有 `&mut self` 通过 Mutex 串行化所有 Flow 方法调用。`with_max_concurrency(n)` 仅在混合模式（部分 Flow step + 部分 Hero step）下有效。纯 Flow pipeline 始终串行。

### 9.2 Router 不能转换数据

Router 方法的返回值仅用于路由决策（label），不传递给下游。下游 Listener 方法接收的是 `router.upstream` 的原始输出。如需数据转换+路由，应在 Router 前插入独立方法。

### 9.3 `FlowError` 保留但缩窄

`FlowError` 保留为 Flow 方法的返回类型，但在 `run()` 便捷方法中仅作为 `Other(String)` 包装 `CompError`。未来可考虑直接使用 `CompError`。

## 10. 风险与缓解

| 风险 | 缓解 |
|------|------|
| Flow 的 OR 语义（仅执行一次）与 Comp 的事件重放冲突 | OR step 触发后立即从 `in_degree` 中移除，事件重放时 `on_complete` 不再返回它 |
| Router step 作为虚拟 step 引入额外的 `InstanceCreated`/`StepCompleted` 事件 | Router step 与其他 step 地位相同，事件序列一致 |
| 用户使用 `FlowHandleRef::cancel()` 的代码被破坏 | 提供 `ExecutionHandle::cancel()` 作为替代，迁移指南写明 |
| `#[derive(Flow)]` 变成空壳可能让用户困惑 | 保留 derive macro 作为空实现（无操作），文档注明实际逻辑在 `#[flow_impl]` |

---

## 11. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 0.1.0-draft | 2026-06-03 | 初稿 |
