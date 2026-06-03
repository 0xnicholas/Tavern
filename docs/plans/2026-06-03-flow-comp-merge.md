# Flow-Comp 合并实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 tavern-flow 从独立编排引擎重构为 tavern-comp 的 proc-macro DSL 前端，删除 ~900 行重复代码。

**Architecture:** 唯一执行引擎是 tavern-comp 的 WorkflowEngine。Flow 的 proc-macro（`#[start]`/`#[listen]`/`#[router]`）展开为 Workflow + Step + FlowStepExecutor，通过 `agent_id="__flow__"` 哨兵和 `FlowStepExecutor` trait 桥接到引擎。FlowEngine、FlowGraph、FlowEvent、FlowHandle 全部删除。

**Tech Stack:** Rust 2024 edition, tokio, serde, syn/quote (proc-macro), sqlx (EventStore)

**Spec:** `docs/specs/spec-flow-comp-merge.md`

---

## 文件变更总览

| 文件 | 操作 | 说明 |
|------|------|------|
| `crates/tavern-comp/src/workflow.rs` | 修改 | 加 `or_depends_on`, `RouterConfig`, `FLOW_AGENT_ID`, `Step` Default impl |
| `crates/tavern-comp/src/validator.rs` | 修改 | `build_dag_maps` 增强 + `DagMaps.or_steps` |
| `crates/tavern-comp/src/engine.rs` | 修改 | `hero→Option`, `decide_next_action` OR, Router handler, `new_with_flow_executor` |
| `crates/tavern-comp/src/executor.rs` | 修改 | `flow_executor` 字段, `execute_once` Flow 路由 |
| `crates/tavern-comp/src/lib.rs` | 修改 | 导出新类型 |
| `crates/tavern-flow-macros/src/lib.rs` | 重写 | 展开目标改为 Workflow + FlowStepExecutor |
| `crates/tavern-flow/src/lib.rs` | 重写 | 删除 FlowEngine/FlowGraph, 保留 FlowStepExecutor 相关 |
| `crates/tavern-flow/src/event.rs` | **删除** | FlowEvent 不再需要 |
| `crates/tavern-flow/src/registry.rs` | **删除** | FlowRegistry 不再需要 |
| `crates/tavern-flow/Cargo.toml` | 修改 | 清理依赖 |
| `crates/tavern-server/src/routes/flows.rs` | 修改 | 适配 WorkflowRegistry（如有需要） |

---

## Phase A: Step 新增 OR 依赖字段 (`tavern-comp`)

### Task A1: 添加 `RouterConfig` 和 `FLOW_AGENT_ID` 常量

**Files:**
- Modify: `crates/tavern-comp/src/workflow.rs`

- [ ] **Step 1: 写测试 — RouterConfig 序列化/反序列化**

在 `crates/tavern-comp/src/workflow.rs` 测试模块末尾加：

```rust
#[test]
fn test_router_config_serialize() {
    let cfg = RouterConfig { upstream: "step_a".into() };
    let json = serde_json::to_string(&cfg).unwrap();
    assert!(json.contains("upstream"));
    assert!(json.contains("step_a"));
    let back: RouterConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(back.upstream, "step_a");
}

#[test]
fn test_step_with_or_depends_on_deserialize() {
    let yaml = r#"
id: s1
agent_id: a1
task: do something
or_depends_on:
  - upstream_a
  - upstream_b
"#;
    let step: Step = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(step.or_depends_on, vec!["upstream_a", "upstream_b"]);
    assert!(step.depends_on.is_empty());
}

#[test]
fn test_step_with_router_deserialize() {
    let yaml = r#"
id: s1
agent_id: a1
task: route
depends_on:
  - source
router:
  upstream: source
"#;
    let step: Step = serde_yaml::from_str(yaml).unwrap();
    assert!(step.router.is_some());
    assert_eq!(step.router.unwrap().upstream, "source");
}
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cargo test -p tavern-comp -- test_router_config_serialize test_step_with_or_depends_on test_step_with_router
```
Expected: FAIL — `RouterConfig` not defined, Step fields missing

- [ ] **Step 3: 实现 `RouterConfig` + Step 新字段 + Step Default**

在 `crates/tavern-comp/src/workflow.rs` 中，`Step` struct 上方加：

```rust
/// V0.4: Router 路由配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    /// 路由输入来源 step_id
    pub upstream: String,
}

/// 哨兵值：标记此 step 由 Flow 方法执行，非 Agent 调用。
pub const FLOW_AGENT_ID: &str = "__flow__";
```

在 `Step` struct 的 `expected_output` 字段后加：

```rust
    /// V0.4: OR 依赖——任一上游完成即触发。与 depends_on 互斥。
    #[serde(default)]
    pub or_depends_on: Vec<String>,

    /// V0.4: Router 配置——非 None 时此 step 执行后产生 label(s) 触发下游。
    #[serde(default)]
    pub router: Option<RouterConfig>,
```

为 `Step` 添加 `Default` impl（供测试使用 `..Step::default()`）：

```rust
impl Default for Step {
    fn default() -> Self {
        Self {
            id: String::new(),
            agent_id: String::new(),
            task: String::new(),
            depends_on: vec![],
            output_key: None,
            timeout: None,
            retries: None,
            retry_delay: None,
            wait_for_signal: None,
            signal_timeout: None,
            signal_timeout_action: None,
            breakpoint: false,
            model_override: None,
            expected_output: None,
            or_depends_on: vec![],
            router: None,
        }
    }
}
```

在 `make_step` 测试辅助函数中加两个字段（validator.rs 的测试也会用到，下个 task 处理）：

```rust
    or_depends_on: vec![],
    router: None,
```

- [ ] **Step 4: 运行测试确认通过**

```bash
cargo test -p tavern-comp -- test_router_config_serialize test_step_with_or_depends_on test_step_with_router
```
Expected: PASS

- [ ] **Step 5: 修复所有编译错误（现有 make_step 调用缺新字段）**

```bash
cargo check -p tavern-comp 2>&1 | head -20
```
Expected: 如有编译错误，在 `validator.rs` 测试的 `make_step` 函数中加 `or_depends_on: vec![], router: None`。

- [ ] **Step 6: Commit**

```bash
git add crates/tavern-comp/src/workflow.rs
git commit -m "feat(comp): add or_depends_on, RouterConfig, FLOW_AGENT_ID to Step"
```

---

## Phase A2: `build_dag_maps` 增强 + 验证 (`tavern-comp`)

### Task A2: 增强 DAG 分析和验证

**Files:**
- Modify: `crates/tavern-comp/src/validator.rs`

- [ ] **Step 1: 写测试 — OR 依赖、互斥检查、环检测**

在 `crates/tavern-comp/src/validator.rs` 测试模块末尾加：

```rust
// ── V0.4: OR dependency tests ──

fn make_step_or(id: &str, or_deps: Vec<&str>) -> crate::workflow::Step {
    crate::workflow::Step {
        or_depends_on: or_deps.into_iter().map(|s| s.to_string()).collect(),
        router: None,
        ..make_step(id, vec![])
    }
}

#[test]
fn test_or_dep_build_dag_maps() {
    let workflow = Workflow {
        steps: vec![
            make_step("a", vec![]),
            make_step("b", vec![]),
            make_step_or("c", vec!["a", "b"]),
        ],
        ..base_workflow()
    };
    let dag = build_dag_maps(&workflow);
    // OR step: in_degree should be 1 (not 2)
    assert_eq!(dag.in_degree.get("c").copied(), Some(1));
    assert!(dag.or_steps.contains("c"));
}

#[test]
fn test_or_dep_missing_dependency() {
    let workflow = Workflow {
        steps: vec![
            make_step("a", vec![]),
            make_step_or("b", vec!["nonexistent"]),
        ],
        ..base_workflow()
    };
    let err = validate_dag(&workflow).unwrap_err();
    assert!(matches!(err, CompError::StepNotFound { id } if id == "nonexistent"));
}

#[test]
fn test_or_dep_mutual_exclusion_rejected() {
    let mut step = make_step("a", vec!["x"]);
    step.or_depends_on = vec!["y".into()];
    let workflow = Workflow {
        steps: vec![step],
        ..base_workflow()
    };
    let err = validate_dag(&workflow).unwrap_err();
    assert!(matches!(err, CompError::ConfigParse { .. }));
}

#[test]
fn test_or_dep_cycle_detected() {
    let workflow = Workflow {
        steps: vec![
            make_step_or("a", vec!["b"]),
            make_step("b", vec!["a"]),
        ],
        ..base_workflow()
    };
    let err = validate_dag(&workflow).unwrap_err();
    assert!(matches!(err, CompError::CyclicDependency));
}

#[test]
fn test_label_prefixed_or_dep_skipped_in_validation() {
    let workflow = Workflow {
        steps: vec![
            make_step("a", vec![]),
            make_step_or("b", vec!["__label__approved"]),
        ],
        ..base_workflow()
    };
    // __label__ entries should be skipped in existence check
    assert!(validate_dag(&workflow).is_ok());
}

fn base_workflow() -> Workflow {
    Workflow {
        id: "w1".into(),
        name: "test".into(),
        description: None,
        steps: vec![],
        inputs: vec![],
        outputs: vec![],
        process: Process::Sequential,
        planning: None,
        webhook: None,
        schedule: None,
        schedule_inputs: serde_json::Value::Null,
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cargo test -p tavern-comp -- test_or_dep_
```
Expected: 多个 FAIL

- [ ] **Step 3: 增强 `build_dag_maps`**

替换现有 `build_dag_maps` 函数：

```rust
/// 构建 DAG 的入度表和邻接表（增强版：处理 depends_on + or_depends_on）。
pub fn build_dag_maps(workflow: &Workflow) -> DagMaps {
    let step_ids: HashSet<String> = workflow.steps.iter().map(|s| s.id.clone()).collect();
    let mut in_degree: HashMap<String, usize> =
        workflow.steps.iter().map(|s| (s.id.clone(), 0)).collect();
    let mut adj: HashMap<String, Vec<String>> = workflow
        .steps
        .iter()
        .map(|s| (s.id.clone(), Vec::new()))
        .collect();
    let mut or_steps: HashSet<String> = HashSet::new();

    for step in &workflow.steps {
        if !step.or_depends_on.is_empty() {
            // OR: in_degree = 1（任一上游完成即触发）
            in_degree.insert(step.id.clone(), 1);
            or_steps.insert(step.id.clone());
            for upstream in &step.or_depends_on {
                // 非 label 边加入邻接表（用于环检测）
                if !upstream.starts_with("__label__") {
                    adj.entry(upstream.clone()).or_default().push(step.id.clone());
                }
            }
        } else {
            // AND: in_degree = depends_on.len()
            for dep in &step.depends_on {
                adj.entry(dep.clone()).or_default().push(step.id.clone());
                *in_degree.get_mut(&step.id).unwrap() += 1;
            }
        }
    }
    DagMaps {
        in_degree,
        adj,
        step_ids,
        or_steps,
    }
}
```

- [ ] **Step 4: 更新 `DagMaps` struct 加 `or_steps` 字段**

```rust
pub struct DagMaps {
    pub in_degree: HashMap<String, usize>,
    pub adj: HashMap<String, Vec<String>>,
    pub step_ids: HashSet<String>,
    /// V0.4: OR step 集合
    pub or_steps: HashSet<String>,
}
```

更新 `validate_dag` 中的解构：

```rust
let DagMaps {
    mut in_degree,
    adj,
    step_ids,
    or_steps: _,
} = build_dag_maps(workflow);
```

- [ ] **Step 5: 增强 `validate_dag` — OR 依赖存在性 + 互斥检查 + router 约束 + label 跳过**

在 `validate_dag` 的存在性检查循环中加：

```rust
// V0.4: 互斥检查
if !step.depends_on.is_empty() && !step.or_depends_on.is_empty() {
    return Err(CompError::ConfigParse {
        path: "<workflow>".to_string(),
        reason: format!(
            "step '{}' has both depends_on and or_depends_on — must be mutually exclusive",
            step.id
        ),
    });
}
// V0.4: router upstream 必须在 depends_on 中
if let Some(ref router) = step.router {
    if !step.depends_on.contains(&router.upstream) {
        return Err(CompError::ConfigParse {
            path: "<workflow>".to_string(),
            reason: format!(
                "step '{}' has router.upstream '{}' which is not in depends_on",
                step.id, router.upstream
            ),
        });
    }
}
// V0.4: or_depends_on 存在性检查（跳过 __label__ 条目）
for dep in &step.or_depends_on {
    if !dep.starts_with("__label__") && !step_ids.contains(dep) {
        return Err(CompError::StepNotFound { id: dep.clone() });
    }
}
```

> **注意**：上述校验同样需要在 `validate_static()` 的 Hierarchical 分支手动依赖检查循环中加入（跳过 DAG 环检测，但存在性/互斥/router 约束仍需检查）。

- [ ] **Step 6: 全量测试**

```bash
cargo test -p tavern-comp
```
Expected: 所有测试 PASS（包括新增的 5 个 OR 测试 + 所有现有测试）

- [ ] **Step 7: Commit**

```bash
git add crates/tavern-comp/src/validator.rs
git commit -m "feat(comp): enhance build_dag_maps for or_depends_on — cycle detection, mutual exclusion, label skip"
```

---

## Phase B: `decide_next_action` OR 支持 (`tavern-comp`)

### Task B: 引擎调度逻辑支持 OR

**Files:**
- Modify: `crates/tavern-comp/src/engine.rs:decide_next_action`

- [ ] **Step 1: 写集成测试 — OR 依赖 Workflow 端到端执行**

在 `crates/tavern-comp/src/engine/tests.rs` 中加（或开启一个新测试文件）：

```rust
#[tokio::test]
async fn test_or_dependency_first_upstream_triggers() {
    let hero = test_hero();
    let engine = WorkflowEngine::new(hero.clone());
    let workflow = Workflow {
        steps: vec![
            Step { id: "a".into(), agent_id: "echo".into(), task: "a".into(), output_key: Some("a".into()), ..Step::default() },
            Step { id: "b".into(), agent_id: "echo".into(), task: "b".into(), output_key: Some("b".into()), ..Step::default() },
            Step { id: "c".into(), agent_id: "echo".into(), task: "c".into(), or_depends_on: vec!["a".into(), "b".into()], ..Step::default() },
        ],
        ..test_workflow("or_test")
    };
    let result = engine.run(&workflow, json!({})).await.unwrap();
    // Step c 应该被触发（任一 a 或 b 完成）
    assert!(result.step_results.get("c").unwrap().output.is_some());
}
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cargo test -p tavern-comp -- test_or_dependency_first_upstream_triggers
```
Expected: FAIL — OR dependency not yet handled

- [ ] **Step 3: 修改 `decide_next_action` 使用增强后的 `build_dag_maps`**

替换现有 `decide_next_action` 中构建 `in_degree` 的逻辑（约从 `let mut in_degree = crate::validator::build_dag_maps(workflow).in_degree;` 开始）：

```rust
let dag = crate::validator::build_dag_maps(workflow);
let mut in_degree = dag.in_degree;

for completed in &state.completed_steps {
    for step in &workflow.steps {
        if step.depends_on.contains(completed) {
            if let Some(d) = in_degree.get_mut(&step.id) {
                *d = d.saturating_sub(1);
            }
        }
        if step.or_depends_on.contains(completed) {
            // OR: 任一上游完成即清零
            in_degree.insert(step.id.clone(), 0);
        }
    }
}
```

- [ ] **Step 4: 修复 `decide_next_action` 中的解构错误**

确保 `decide_next_action` 函数内的 filter 逻辑保持不变（已存在），但由于 OR step 的 `in_degree` 初始值可能为 1 且在完成时置零，filter 中的 `in_degree.get(&s.id).copied().unwrap_or(0) == 0` 自然适用。

- [ ] **Step 5: 运行集成测试**

```bash
cargo test -p tavern-comp -- test_or_dependency_
```
Expected: PASS

- [ ] **Step 6: 全量回归**

```bash
cargo test -p tavern-comp
```
Expected: 所有现有测试 PASS

- [ ] **Step 7: Commit**

```bash
git add crates/tavern-comp/src/engine.rs
git commit -m "feat(comp): decide_next_action supports or_depends_on"
```

---

## Phase C: `FlowStepExecutor` trait (`tavern-comp`)

### Task C: 新增 Flow 方法执行抽象

**Files:**
- Create: `crates/tavern-comp/src/flow_executor.rs`
- Modify: `crates/tavern-comp/src/lib.rs`

- [ ] **Step 1: 写测试 — mock FlowStepExecutor**

```bash
# 新文件: crates/tavern-comp/src/flow_executor.rs
```

```rust
use std::pin::Pin;
use std::future::Future;
use serde_json::Value;

/// Flow 方法步骤执行器。由 #[flow_impl] proc-macro 自动实现。
pub trait FlowStepExecutor: Send + 'static {
    fn execute_step(
        &mut self,
        step_id: &str,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExecutor;
    impl FlowStepExecutor for MockExecutor {
        fn execute_step(
            &mut self,
            step_id: &str,
            _input: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
            let id = step_id.to_string();
            Box::pin(async move {
                Ok(Value::String(format!("result_{}", id)))
            })
        }
    }

    #[tokio::test]
    async fn test_mock_flow_executor() {
        let mut exec = MockExecutor;
        let output = exec.execute_step("research", Value::Null).await.unwrap();
        assert_eq!(output, Value::String("result_research".into()));
    }
}
```

- [ ] **Step 2: 运行测试确认通过**

```bash
cargo test -p tavern-comp -- test_mock_flow_executor
```
Expected: PASS（新文件，无其他依赖）

- [ ] **Step 3: 导出新模块**

在 `crates/tavern-comp/src/lib.rs` 加：

```rust
pub mod flow_executor;
pub use flow_executor::FlowStepExecutor;
```

- [ ] **Step 4: Commit**

```bash
git add crates/tavern-comp/src/flow_executor.rs crates/tavern-comp/src/lib.rs
git commit -m "feat(comp): add FlowStepExecutor trait for Flow method dispatch"
```

---

## Phase D: `WorkflowEngine` hero→Option (`tavern-comp`)

### Task D: 引擎支持 Flow-only 模式

**Files:**
- Modify: `crates/tavern-comp/src/engine.rs`

- [ ] **Step 1: 改 `hero` 字段为 `Option<Arc<TavernHero>>`**

```rust
pub struct WorkflowEngine {
    hero: Option<Arc<tavern_hero::TavernHero>>,
    store: Arc<dyn EventStore>,
    max_concurrency: usize,
    flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
}
```

- [ ] **Step 2: 更新 `new()` 构造器**

```rust
pub fn new(hero: Arc<tavern_hero::TavernHero>) -> Self {
    Self {
        hero: Some(hero),
        store: Arc::new(MemoryEventStore::new()),
        max_concurrency: usize::MAX,
        flow_executor: None,
    }
}
```

- [ ] **Step 3: 添加 `new_with_flow_executor()` 构造器**

```rust
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
```

- [ ] **Step 4: 修复所有 `self.hero.` 调用点为 `self.hero.as_ref().unwrap()` 或 guard**

在 `validate()` 中：
```rust
// 按 step 跳过 Agent 存在性检查
for step in &workflow.steps {
    if step.agent_id != FLOW_AGENT_ID {
        if let Some(ref hero) = self.hero {
            if hero.get_agent(&step.agent_id).await.is_none() {
                return Err(CompError::AgentNotFound { id: step.agent_id.clone() });
            }
        }
    }
}
// Hierarchical Manager 检查
if let Process::Hierarchical(cfg) = &workflow.process {
    if let Some(ref hero) = self.hero {
        if hero.get_agent(&cfg.agent_id).await.is_none() { ... }
    }
}
// Planning agent 检查 — 需要 hero
if let Some(ref planning) = workflow.planning {
    if planning.enabled && self.hero.is_none() {
        return Err(CompError::ConfigParse {
            path: "<workflow>".into(),
            reason: "Planning requires hero (not available in Flow-only mode)".into(),
        });
    }
}
```

在 `run_interpreter_loop` / `run_interpreter_hierarchical` 中，`StepExecutor::new` 调用改为传递 `self.hero.clone()`：

```rust
let executor = StepExecutor::new(
    self.hero.clone(),
    self.flow_executor.clone(),
    internal_tx.clone(),
    self.max_concurrency,
);
```

- [ ] **Step 5: 更新 `StepExecutor::new` 签名**（see Phase E）

此时 `StepExecutor::new` 仍用旧签名。先编译通过——在 Phase E 中统一修改。

- [ ] **Step 6: 编译检查**

```bash
cargo check -p tavern-comp 2>&1
```
Expected: 如有编译错误，逐处修复 `self.hero` 访问。

- [ ] **Step 7: 全量测试**

```bash
cargo test -p tavern-comp
```
Expected: 所有现有测试 PASS

- [ ] **Step 8: Commit**

```bash
git add crates/tavern-comp/src/engine.rs
git commit -m "feat(comp): WorkflowEngine.hero→Option + new_with_flow_executor constructor"
```

---

## Phase E: `StepExecutor` Flow 路由 (`tavern-comp`)

### Task E: StepExecutor 执行 Flow 方法

**Files:**
- Modify: `crates/tavern-comp/src/executor.rs`

- [ ] **Step 1: 添加 `flow_executor` 字段**

```rust
use crate::flow_executor::FlowStepExecutor;

pub struct StepExecutor {
    hero: Option<Arc<tavern_hero::TavernHero>>,
    flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
    tx: mpsc::Sender<WorkflowEvent>,
    semaphore: Arc<tokio::sync::Semaphore>,
}
```

- [ ] **Step 2: 更新构造函数**

```rust
impl StepExecutor {
    pub fn new(
        hero: Option<Arc<tavern_hero::TavernHero>>,
        flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
        tx: mpsc::Sender<WorkflowEvent>,
        max_concurrency: usize,
    ) -> Self {
        Self { hero, flow_executor, tx, semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrency.min(65536))) }
    }
```

- [ ] **Step 3: 更新 `submit` 中的 clone**

```rust
pub async fn submit(&self, step: Arc<Step>, context: Value, attempt: u64, will_retry: bool) {
    let hero = self.hero.clone();
    let flow_executor = self.flow_executor.clone();
    let tx = self.tx.clone();
    // ... rest unchanged, pass flow_executor to execute_once
```

- [ ] **Step 4: 改造 `execute_once` — FLOW_AGENT_ID 哨兵路由**

```rust
async fn execute_once(
    step: &Step,
    context: &Value,
    hero: Option<Arc<tavern_hero::TavernHero>>,
    flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
    tx: &mpsc::Sender<WorkflowEvent>,
) -> Result<Value, String> {
    if step.agent_id == crate::workflow::FLOW_AGENT_ID {
        let executor = flow_executor.as_ref()
            .ok_or_else(|| "flow executor not configured".to_string())?;
        let mut guard = executor.lock().await;
        let input = resolve_flow_input(step, context);
        return guard.execute_step(&step.task, input).await;
    }
    // ... existing Hero logic (unchanged)
}

/// 为 Flow step 解析输入：取第一个依赖 step 的输出。
fn resolve_flow_input(step: &Step, context: &Value) -> Value {
    if let Some(ref router) = step.router {
        return context.get(&router.upstream).cloned().unwrap_or(Value::Null);
    }
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

- [ ] **Step 5: 更新 engine.rs 中所有 `StepExecutor::new` 调用**

所有 `StepExecutor::new(self.hero.clone(), ...)` → `StepExecutor::new(self.hero.clone(), self.flow_executor.clone(), ...)`

- [ ] **Step 6: 写测试 — Flow 哨兵路由**

在 `executor.rs` tests 中（或 engine tests）：

```rust
#[tokio::test]
async fn test_flow_step_routes_to_executor() {
    use crate::flow_executor::FlowStepExecutor;
    struct EchoExec;
    impl FlowStepExecutor for EchoExec {
        fn execute_step(&mut self, _id: &str, input: Value) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
            Box::pin(async move { Ok(input) })
        }
    }
    let engine = WorkflowEngine::new_with_flow_executor(
        Arc::new(tokio::sync::Mutex::new(EchoExec))
    );
    let workflow = Workflow {
        steps: vec![Step {
            id: "s1".into(), agent_id: FLOW_AGENT_ID.into(),
            task: "s1".into(), output_key: Some("s1".into()),
            ..Step::default()
        }],
        ..test_workflow("flow_test")
    };
    let result = engine.run(&workflow, json!({})).await.unwrap();
    assert!(result.step_results["s1"].output.is_some());
}
```

- [ ] **Step 7: 编译 + 测试**

```bash
cargo check -p tavern-comp && cargo test -p tavern-comp
```
Expected: 所有测试 PASS

- [ ] **Step 8: Commit**

```bash
git add crates/tavern-comp/src/executor.rs crates/tavern-comp/src/engine.rs
git commit -m "feat(comp): StepExecutor routes FLOW_AGENT_ID to FlowStepExecutor"
```

---

## Phase F: Router 事件处理 (`tavern-comp`)

### Task F: Router step 完成后触发下游

**Files:**
- Modify: `crates/tavern-comp/src/engine.rs` — `run_interpreter_loop` 的 `StepCompleted` 分支

- [ ] **Step 1: 写测试 — Router 触发下游**

```rust
#[tokio::test]
async fn test_router_triggers_labeled_downstream() {
    struct RouterExec {
        labels: Vec<String>,
    }
    impl FlowStepExecutor for RouterExec {
        fn execute_step(&mut self, step_id: &str, _input: Value) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
            if step_id.starts_with("__router__") {
                let labels: Value = self.labels.iter().map(|l| Value::String(l.clone())).collect();
                return Box::pin(async move { Ok(Value::Array(labels.as_array().unwrap().clone())) });
            }
            Box::pin(async move { Ok(Value::String(format!("done_{}", step_id))) })
        }
    }
    let executor = Arc::new(tokio::sync::Mutex::new(RouterExec { labels: vec!["approved".into()] }));
    let engine = WorkflowEngine::new_with_flow_executor(executor);
    let workflow = Workflow {
        steps: vec![
            Step { id: "source".into(), agent_id: FLOW_AGENT_ID.into(), task: "source".into(), output_key: Some("source".into()), ..Step::default() },
            Step { id: "__router__gate".into(), agent_id: FLOW_AGENT_ID.into(), task: "__router__gate".into(), depends_on: vec!["source".into()], router: Some(RouterConfig { upstream: "source".into() }), ..Step::default() },
            Step { id: "publish".into(), agent_id: FLOW_AGENT_ID.into(), task: "publish".into(), or_depends_on: vec!["__label__approved".into()], output_key: Some("publish".into()), ..Step::default() },
        ],
        ..test_workflow("router_test")
    };
    let result = engine.run(&workflow, json!({})).await.unwrap();
    assert!(result.step_results["publish"].output.is_some());
}
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cargo test -p tavern-comp -- test_router_triggers_labeled_downstream
```
Expected: FAIL — Router logic not yet implemented

- [ ] **Step 3: 在 `StepCompleted` 事件处理中添加 Router 逻辑**

在 `run_interpreter_loop` 中 `WorkflowEvent::StepCompleted { step_id, output, .. }` 的处理分支末尾（`apply_and_persist` 之后，`signal` 检查之前）加：

```rust
// V0.4: Router 路由处理
if let Some(step) = workflow.steps.iter().find(|s| &s.id == step_id) {
    if let Some(ref router) = step.router {
        let upstream_output = state.context.get(&router.upstream).cloned().unwrap_or(Value::Null);
        let labels = extract_labels_from_output(output);
        for label in &labels {
            let label_key = format!("__label__{}", label);
            if let Some(obj) = state.context.as_object_mut() {
                obj.insert(label_key.clone(), upstream_output.clone());
            }
            state.completed_steps.insert(label_key);
        }
    }
}
```

在 `engine.rs` 顶部加辅助函数：

```rust
const ROUTER_LABEL_PREFIX: &str = "__label__";

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

- [ ] **Step 4: 运行测试**

```bash
cargo test -p tavern-comp -- test_router_triggers_labeled_downstream
```
Expected: PASS

- [ ] **Step 5: 全量回归**

```bash
cargo test -p tavern-comp
```
Expected: 所有测试 PASS

- [ ] **Step 6: Commit**

```bash
git add crates/tavern-comp/src/engine.rs
git commit -m "feat(comp): router label injection in StepCompleted handler"
```

---

## 🔍 CHECKPOINT — 手写 Workflow + mock FlowStepExecutor 端到端验证

- [ ] **Step 1: 运行 Comp 全量测试**

```bash
cargo test -p tavern-comp
```
Expected: 全部 PASS（包括 Flow 相关的 3 个新增测试）

- [ ] **Step 2: 运行全仓编译检查**

```bash
cargo check --workspace
```
Expected: 0 errors

- [ ] **Step 3: 手动验证 — `cargo test -p tavern-comp` 中包含至少一个纯 Flow 路径的端到端测试**

确认 `test_flow_step_routes_to_executor` 和 `test_router_triggers_labeled_downstream` 都通过。

- [ ] **Step 4: Commit checkpoint**

```bash
git commit --allow-empty -m "CHECKPOINT: Comp-side Flow infrastructure complete — all tests pass"
```

**Go/No-Go 条件**：所有 Comp 测试通过，Flow 哨兵路由可用。若失败，不回退，在此修复。通过后进入 Phase G。

---

## Phase G: Proc-macro 展开重写 (`tavern-flow-macros`)

### Task G: `#[flow_impl]` 展开为 Workflow + FlowStepExecutor

**Files:**
- Modify: `crates/tavern-flow-macros/src/lib.rs`（重写 flow_impl 展开逻辑）

这是最大的单体任务，分解为子步骤：

- [ ] **G1: 展开 `FlowStepExecutor` impl（替代当前 `FlowDispatch` impl）**

当前生成：
```rust
impl FlowDispatch for StructName { fn dispatch(...) -> Pin<Box<...>> { match method { ... } } }
```
改为生成：
```rust
impl tavern_comp::FlowStepExecutor for StructName {
    fn execute_step(&mut self, step_id: &str, input: Value) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        match step_id { ... }
    }
}
```

关键：`FlowStepExecutor::execute_step` 返回 `Result<Value, String>`（非 `FlowError`），wrapper 方法返回类型改为 `Result<Value, String>`。

- [ ] **G2: 展开 `__workflow_definition()` 静态方法**

生成 `Workflow` struct，每个方法映射为一个 `Step`：
- `#[start]` → `depends_on: [], or_depends_on: []`
- `#[listen("name")]`，name 是方法名 → `or_depends_on: ["name"]`（无前缀）
- `#[listen("label")]`，label 不是方法名 → `or_depends_on: ["__label__label"]`（有前缀）
- `#[listen(or("a","b"))]` → `or_depends_on: ["__label__a", "__label__b"]`
- `#[listen(and("a","b"))]` → `depends_on: ["a", "b"]`
- `#[router("upstream")]` → `id: "__router__<name>", depends_on: ["upstream"], router: Some(RouterConfig { upstream })`

所有非 Router step 设 `output_key: Some(step_id)`。所有 step 设 `agent_id: tavern_comp::FLOW_AGENT_ID`。

- [ ] **G3: 展开 `run()` 便捷方法**

```rust
pub async fn run(self, inputs: Value) -> Result<Value, FlowError> {
    let workflow = Self::__workflow_definition();
    let executor = Arc::new(tokio::sync::Mutex::new(self));
    let engine = tavern_comp::WorkflowEngine::new_with_flow_executor(executor);
    let result = engine.run(&workflow, inputs).await
        .map_err(|e| FlowError::Other(e.to_string()))?;
    Ok(result.outputs)
}
```

- [ ] **G4: 前缀自动判断逻辑**

在 `extract_flow_attr` 阶段收集所有方法名到一个 `HashSet`。展开 `#[listen("x")]` 时检查 x 是否在方法名集合中，决定是否加 `__label__` 前缀。

- [ ] **G5: 删除旧生成逻辑**

删除 `FlowDispatch` trait impl、`Flow` trait impl（metadata）、`FlowMetadata`/`MethodInfo` 引用。

- [ ] **G6: 编译检查**

```bash
cargo check -p tavern-flow-macros
```

- [ ] **G7: 写快照测试 — 验证生成的结构**

```rust
// 在 proc-macro crate 中无法直接测试展开输出。
// 替代方案：在 tavern-flow 中写集成测试，用 trybuild 验证宏展开。
```

- [ ] **G8: Commit**

```bash
git add crates/tavern-flow-macros/src/lib.rs
git commit -m "feat(flow-macros): rewrite #[flow_impl] to expand Workflow + FlowStepExecutor"
```

---

## Phase H: 删除 Flow 旧引擎 (`tavern-flow`)

### Task H1: 删除文件

- [ ] **Step 1: 删除 event.rs 和 registry.rs**

```bash
rm crates/tavern-flow/src/event.rs
rm crates/tavern-flow/src/registry.rs
```

- [ ] **Step 2: 清理 Cargo.toml — 移除不必要的依赖**

检查 `crates/tavern-flow/Cargo.toml`，移除：
- `tavern-core`（如果不再直接使用）
- `tavern-hero`（如果不再直接使用）
- `uuid`、`chrono`（不再生成 flow_id）

保留：
- `tavern-comp`（FlowStepExecutor trait + WorkflowEngine）
- `tavern-flow-macros`（proc-macro）
- `serde`、`serde_json`、`tokio`、`thiserror`

### Task H2: 重写 lib.rs

- [ ] **Step 1: 替换 `crates/tavern-flow/src/lib.rs`**

新内容 ~100 行：

```rust
//! tavern-flow — Rust proc-macro DSL for method-level workflow orchestration.
//! Expands `#[start]`/`#[listen]`/`#[router]` into `tavern_comp::Workflow` +
//! `tavern_comp::FlowStepExecutor`, executed by `tavern_comp::WorkflowEngine`.

pub use tavern_flow_macros::{Flow, flow_impl, listen, router, start};

// Re-export from tavern-comp for user convenience
pub use tavern_comp::FlowStepExecutor;

/// Flow 方法错误类型。
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("flow error: {0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    // Tests that use proc-macros live here or in integration tests.
    // Existing Flow tests need to be migrated:
    // - Keep: dispatch behavior tests (via FlowStepExecutor)
    // - Delete: FlowEngine/FlowGraph tests
}
```

- [ ] **Step 2: 迁移保留的测试**

保留并适配以下测试（从旧的 lib.rs tests 迁移）：
- `test_macro_dispatch_start` → 通过 `FlowStepExecutor::execute_step` 调用
- `test_macro_dispatch_listen` → 同上
- `test_macro_flow_metadata` → 改为验证 `__workflow_definition()` 输出
- `test_flow_engine_event_loop` → 改为调用 `pipeline.run()`
- `test_flow_engine_with_router` → 改为调用 `pipeline.run()`
- `test_flow_or_combinator` → 改为调用 `pipeline.run()`
- `test_flow_and_combinator` → 改为调用 `pipeline.run()`

删除的测试：
- `test_flow_graph_from_metadata`（FlowGraph 已删除）
- `test_flow_handle_start_and_await`（FlowHandle 已删除）
- `test_parallel_path_enabled`（FlowEngine 已删除）

- [ ] **Step 3: 编译 + 测试**

```bash
cargo check -p tavern-flow && cargo test -p tavern-flow
```
Expected: 编译通过，迁移后的测试 PASS

- [ ] **Step 4: Commit**

```bash
git add crates/tavern-flow/
git commit -m "feat(flow): delete FlowEngine/FlowGraph/FlowEvent/FlowHandle, keep FlowStepExecutor + macros"
```

---

## Phase I: 新便捷 API (`tavern-flow`)

### Task I: `run()` / `start_async()` / builder 方法

这些已在 Phase G 的 proc-macro 展开中生成。Phase I 主要确保暴露正确，并添加文档测试。

- [ ] **Step 1: 验证 `run()` 可用**

```bash
cargo test -p tavern-flow -- test_flow_engine_event_loop
```
Expected: PASS（使用 `pipeline.run()` 语法）

- [ ] **Step 2: 添加 `with_store` / `with_webhook` / `with_max_concurrency` builder**

在 proc-macro 展开的 `run()` 方法中支持 builder 模式（需要在 `#[flow_impl]` 生成的 struct 上加字段）：

该功能可通过在 `#[derive(Flow)]` struct 上手动加字段实现（用户侧），或通过额外的 proc-macro 生成。当前阶段保持 YAGNI——用户可在 struct 中自行添加 `store: Option<Arc<dyn EventStore>>` 等字段并在 `run()` 中使用。

- [ ] **Step 3: Commit**

```bash
git add crates/tavern-flow/
git commit -m "feat(flow): pipeline.run() convenience method via proc-macro"
```

---

## Phase J: Server `/flows` 端点适配 (`tavern-server`)

### Task J: Flow 端点通过 WorkflowRegistry 注册

**Files:**
- Modify: `crates/tavern-server/src/routes/flows.rs`（或等效文件）

- [ ] **Step 1: 确认当前 `/flows` 端点实现**

```bash
grep -rn "flows" crates/tavern-server/src/ --include="*.rs" | head -20
```

- [ ] **Step 2: 适配 — Flow 的 `__workflow_definition()` 注册到 WorkflowRegistry**

如果当前 `/flows` 使用独立的 `FlowRegistry`，改为使用 `WorkflowRegistry` + `FlowStepExecutor` 包装。

如果当前 `/flows` 已经委托给 `WorkflowEngine`（通过 `FlowRegistry` 内部调用 `start_async`），则只需确保新的 `pipeline.run()` 路径可用。

由于 Tavern 当前可能是 mock runtime 或无实际 `/flows` 端点（需确认），此 Phase 可能是空操作或小改动。

- [ ] **Step 3: 编译 + 测试**

```bash
cargo check -p tavern-server && cargo test -p tavern-server
```
Expected: 编译通过，现有测试 PASS

- [ ] **Step 4: Commit**

```bash
git add crates/tavern-server/
git commit -m "feat(server): adapt /flows endpoints for merged Flow-Comp engine"
```

---

## Phase K: 最终验证

### Task K1: 全仓编译 + 测试

- [ ] **Step 1: 全仓编译**

```bash
cargo check --workspace
```
Expected: 0 errors

- [ ] **Step 2: 全仓测试**

```bash
cargo test --workspace
```
Expected: 所有测试 PASS

- [ ] **Step 3: Clippy**

```bash
cargo clippy --workspace -- -D warnings
```
Expected: 0 warnings

- [ ] **Step 4: Format**

```bash
cargo fmt -- --check
```
Expected: 无格式差异

### Task K2: 代码行数对比

- [ ] **Step 1: 统计行数**

```bash
echo "=== tavern-flow ===" && wc -l crates/tavern-flow/src/*.rs
echo "=== tavern-comp ===" && wc -l crates/tavern-comp/src/*.rs
```

确认 tavern-flow 从 ~1320 行缩减到 ~200 行。

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "chore: final verification — all tests pass, clippy clean, fmt clean"
```

---

## 完工检查清单

- [ ] `cargo check --workspace` — 0 errors
- [ ] `cargo test --workspace` — 所有测试 PASS
- [ ] `cargo clippy --workspace -- -D warnings` — 0 warnings
- [ ] `cargo fmt -- --check` — 无格式差异
- [ ] `tavern-flow` 代码量 < 300 行（从 ~1320 行缩减）
- [ ] `tavern-comp` 新增 `or_depends_on`/`RouterConfig`/`FlowStepExecutor` 导出
- [ ] 旧 Flow 测试中保留的 7 个已迁移通过
- [ ] README/VERSIONS.md 已更新（可选，不影响功能）
