# Tavern Refactor: CrewAI Alignment — Phase 1

## 1. 概述

Phase 1 改造 `tavern-comp`，增加三项 Crew 层能力：

- **Hierarchical Process**：Manager Agent（LLM 驱动）动态委派 Task
- **Planning**：AgentPlanner 执行前生成结构化执行计划
- **Checkpoint**：状态快照加速 Event Sourcing 恢复

Agent 层（Tools、Skills、Knowledge）和 Flow 层（事件驱动方法编排）不在 Phase 1 范围。

### 1.1 设计原则

- **向后兼容**：现有 DAG Workflow YAML 不变，`WorkflowEngine::run()` / `start()` 行为不变
- **Pandaria 不变**：LLM 调用持续通过 `Runtime` trait → Pandaria 代理
- **Memory 外接**：Memory 系统通过外部服务实现，Tavern 不内置 Memory crate
- **默认 Sequential**：`Process::Sequential` 为默认值，旧配置无需修改

### 1.2 Crate 变更

| Crate | 变更 | 说明 |
|---|---|---|
| `tavern-core` | 修改 | 新增 `Process`、`ManagerConfig`、`PlanningConfig`、`Plan` |
| `tavern-comp` | 修改 | 新增 `run_hierarchical`、Planner、Checkpoint；`validate_static` 增加 process 感知 |
| `tavern-server` | 修改 | 新增 `?process=hierarchical` 查询参数；错误映射扩展 |

---

## 2. 术语定义

| 术语 | 定义 |
|---|---|
| Process | 执行策略：`Sequential`（DAG）或 `Hierarchical`（Manager Agent） |
| Manager Agent | Hierarchical 模式中的调度 Agent，由 LLM 驱动，动态委派 Task |
| AgentPlanner | Planning 模式中的规划 Agent，执行前输出 Plan |
| Plan | AgentPlanner 生成的执行计划，注入到 Task 描述中 |
| Checkpoint | `InstanceState` 快照 + 事件序号，用于加速恢复 |
| CompletedTask | Manager 循环中已完成步骤的摘要记录 |

---

## 3. Hierarchical Process

### 3.1 概念

DAG 模式下步骤按静态依赖拓扑排序。Hierarchical 模式引入 **Manager Agent**（LLM 驱动的调度器），运行时动态决定 Task 委派顺序，`depends_on` 字段被忽略。

```
Sequential (现有):               Hierarchical (新增):

  [Step A] ──→ [Step B]           Manager LLM
       ↘   ↗                       │ 分析所有 Task + Agent 清单
        [C]                         │ 返回 JSON 决策
  (静态 DAG)                        │ 执行 → 收集结果 → 循环
                                    │ 直到 Manager 返回 done
```

### 3.2 数据模型

```rust
// tavern-core/src/config.rs

/// 执行策略，存储在 Workflow 上
pub enum Process {
    /// 默认：DAG 拓扑排序 + 事件溯源（现有行为）
    Sequential,
    /// Manager Agent 动态委派
    Hierarchical(ManagerConfig),
}

impl Default for Process {
    fn default() -> Self {
        Process::Sequential
    }
}

pub struct ManagerConfig {
    /// Manager Agent 的 ID（必须在 registry 中注册）
    pub agent_id: String,
    /// 可选：覆盖 Manager Agent 的 instructions
    /// 如果为 None，使用 agent 注册时的默认 instructions
    pub instructions: Option<String>,
}
```

`Workflow` 结构体新增 `process` 字段（`tavern-comp/src/workflow.rs`）：

```rust
pub struct Workflow {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]                    // YAML 缺失时默认 Sequential
    pub process: Process,
    pub steps: Vec<Step>,
    pub inputs: Vec<InputDef>,
    pub outputs: Vec<OutputDef>,
    // planning 字段见第 4 节
}
```

### 3.3 Workflow YAML

```yaml
id: research_crew
name: Research Crew
process: hierarchical
manager:
  agent_id: manager
  instructions: |
    You are a project manager. Available agents:
    - researcher: gathers information
    - writer: writes articles
    - editor: polishes content

    Assign tasks one at a time. After receiving an output, validate quality
    before deciding the next task. Output JSON:
    {"action":"delegate","task_id":"...","agent_id":"..."} or {"action":"done"}

steps:
  - id: gather_info
    agent_id: researcher
    task: "Research the latest trends in {{topic}}"
    retries: 1
  - id: write_article
    agent_id: writer
    task: "Write about: {{research_notes}}"
    retries: 1
  - id: polish
    agent_id: editor
    task: "Polish: {{draft}}"
    retries: 1

inputs:
  - name: topic
    required: true
```

**向后兼容**：不指定 `process` 字段时默认 `sequential`。

### 3.4 Manager LLM 调用链路

Manager 的 LLM 决策通过 `TavernHero::execute()` 完成：

```
1. 构建 Manager Prompt（由 build_manager_prompt 生成）
   ┌──────────────────────────────────────────────────┐
   │ System: {manager.instructions}                    │
   │                                                   │
   │ Available Agents:                                 │
   │ - researcher: gathers information                 │
   │ - writer: writes articles                         │
   │ - editor: polishes content                        │
   │                                                   │
   │ Pending Tasks:                                    │
   │ - gather_info: Research {{topic}}                 │
   │ - write_article: Write about research             │
   │ - polish: Polish the draft                        │
   │                                                   │
   │ Completed Tasks: []                               │
   │                                                   │
   │ Decide next action. Output JSON only:             │
   │ {"action":"delegate","task_id":"...","agent":"..."}│
   │ or {"action":"done"}                              │
   └──────────────────────────────────────────────────┘

2. 调用：hero.execute("manager", &manager_prompt, None)
   → Pandaria → LLM → 返回 JSON 字符串

3. 解析决策：parse_manager_response(json_str)
   → ManagerDecision::Delegate { task_id, agent_id }
   → ManagerDecision::Done

4. 如果是 Delegate:
   - 渲染 task 模板（注入 context 中的 step outputs）
   - hero.execute(agent_id, &rendered_task, Some(context))
   - 记录到 CompletedTask 列表
   - 返回步骤 1（循环）

5. 如果是 Done:
   - 汇总所有 CompletedTask → WorkflowResult
```

**关键点**：Manager Agent 必须在 `AgentRegistry` 中注册（配置 model/provider）。`ManagerConfig.instructions` 用于覆盖 Manager Agent 默认 instructions，自动注入 CLI 指令（JSON 输出格式、Agent 清单等）。

### 3.5 Engine API

```rust
// tavern-comp/src/engine.rs

impl WorkflowEngine {

    // V1 同步执行 (保留，内部自动分发)
    pub async fn run(&self, workflow: &Workflow, inputs: Value)
        -> Result<WorkflowResult, CompError>;

    // V2 异步启动 (保留，内部自动分发)
    pub async fn start(&self, workflow: &Workflow, inputs: Value)
        -> Result<ExecutionHandle, CompError>;

    // ── 内部：run 和 start 都调用此方法分发 ──
    // Sequential: 现有 run_interpreter（V2 事件溯源引擎）
    // Hierarchical: 新的 run_hierarchical
    async fn dispatch(&self, workflow: &Workflow, inputs: Value)
        -> Result<WorkflowResult, CompError> {
        match &workflow.process {
            Process::Sequential => self.run_interpreter(...),
            Process::Hierarchical(cfg) => self.run_hierarchical(cfg, ...),
        }
    }

    /// Hierarchical: Manager Agent 驱动的执行循环
    async fn run_hierarchical(
        &self,
        workflow: &Workflow,
        manager_config: &ManagerConfig,
        execution_id: &str,
        signal_rx: mpsc::Receiver<WorkflowEvent>,
    ) -> Result<WorkflowResult, CompError>;

    /// 构建 Manager prompt
    fn build_manager_prompt(
        &self,
        workflow: &Workflow,
        manager_config: &ManagerConfig,
        completed: &[CompletedTask],
        pending: &[&Step],
    ) -> String;

    /// 解析 Manager LLM 的 JSON 响应
    fn parse_manager_response(response: &str)
        -> Result<ManagerDecision, CompError>;
}

/// 已完成步骤的摘要
struct CompletedTask {
    task_id: String,
    agent_id: String,
    output: Value,
    /// 是否有错误（非致命，用于 Manager 决策）
    error: Option<String>,
}

/// Manager 的决策
enum ManagerDecision {
    /// 委派一个 task 给一个 agent
    Delegate {
        task_id: String,
        agent_id: String,
    },
    /// 所有 task 完成
    Done,
}
```

### 3.6 `start()` 与 Hierarchical 的兼容

`start()` 异步路径同样支持 Hierarchical：

```
start(workflow, inputs)
  └─ dispatch(workflow, inputs)
       ├─ Sequential → spawn run_interpreter(instance_id, workflow, signal_rx, completion_tx)
       └─ Hierarchical → spawn run_hierarchical(manager_config, workflow, signal_rx, completion_tx)

两种模式都产生完整的 Event Log（InstanceCreated → StepScheduled → StepCompleted → ...），
都返回 ExecutionHandle，支持 signal/cancel/query_state。
```

Hierarchical 模式的事件流：

```
InstanceCreated → InstanceStarted
  → StepScheduled { step_id: "gather_info", attempt: 1 }
  → StepStarted { step_id: "gather_info" }
  → StepCompleted { step_id: "gather_info", output_key: "research_notes", ... }
  → [checkpoint saved]
  → StepScheduled { step_id: "write_article", attempt: 1 }
  → ...
  → WorkflowCompleted
```

每个 Task 的执行产生与 Sequential 相同的事件序列，Checkpoint 机制同样适用。

### 3.7 约束与限制

| 规则 | 值 |
|---|---|
| Manager Agent 必须在 registry 注册 | 否则返回 `AgentNotFound` |
| `process: hierarchical` 时 `validate_static` 跳过 DAG 校验 | 仍校验 ID 格式、步骤唯一性、task 长度等 |
| `depends_on` 在 hierarchical 模式下忽略 | Manager 动态决定顺序 |
| 最大 Task 数量 | `MAX_HIERARCHICAL_TASKS = 50` |
| Manager 最大循环次数 | `MAX_MANAGER_LOOPS = 100`（防无限） |
| Manager Prompt 最大长度 | `MAX_MANAGER_PROMPT_LEN = 100_000` |

### 3.8 `validate_static()` 变更

```rust
impl Workflow {
    pub fn validate_static(&self) -> Result<(), CompError> {
        // 通用校验（所有 process 类型）
        validate_id(&self.id)?;
        validate_step_count(self.steps.len())?;
        for step in &self.steps {
            validate_step_id(&step.id)?;
            validate_task_length(&step.task)?;
            // ...
        }

        // Process 相关校验
        match &self.process {
            Process::Sequential => {
                // 现有 DAG 校验：依赖存在性、无环、拓扑排序
                validate_dag(&self.steps)?;
            }
            Process::Hierarchical(cfg) => {
                // 跳过 DAG 校验（依赖由 Manager 动态决定）
                validate_manager_agent(&cfg.agent_id)?;
            }
        }

        Ok(())
    }
}
```

---

## 4. Planning

### 4.1 概念

Planning 在执行前插入 **AgentPlanner** 步骤。`planning_llm` 分析所有 Task，输出结构化 `Plan`，Plan Context 被注入到每个 Task 的描述中。

Planning 可与 Sequential 或 Hierarchical 任意组合。

```
┌──────────┐    ┌───────────────┐    ┌──────────────────┐    ┌──────────┐
│ Task 列表 │ → │ AgentPlanner  │ → │ Plan Context 注入 │ → │ 正常执行  │
└──────────┘    └───────────────┘    └──────────────────┘    └──────────┘
```

### 4.2 数据模型

```rust
// tavern-core/src/config.rs

pub struct PlanningConfig {
    pub enabled: bool,
    /// None 时使用第一个 step 的 agent model 作为回退
    pub planning_llm: Option<String>,
}

pub struct Plan {
    pub steps: Vec<PlanStep>,
    pub overall_strategy: String,
}

pub struct PlanStep {
    pub task_id: String,
    pub agent_id: String,
    pub reasoning: String,
    pub expected_output: String,
    pub dependencies: Vec<String>,
}
```

### 4.3 Workflow YAML

```yaml
id: complex_research
planning:
  enabled: true
  planning_llm: "gpt-4o"

process: sequential    # 或 hierarchical

steps:
  - id: research
    agent_id: researcher
    task: "Research {{topic}}"
  # ...
```

### 4.4 执行流程

```
1. 检查 workflow.planning.enabled —— false 则跳过

2. 构建 Planner Prompt（通过 hero.execute 发送给 planning_llm）:
   "You are a planning agent. Workflow: {description}.
    Tasks: [{id, agent, task}].
    Output a JSON plan with overall_strategy and per-step reasoning,
    expected_output, and suggested dependencies."

3. 解析 Plan JSON → Plan struct

4. 注入 Plan Context 到每个 step 的 task:
   原始: "Research {{topic}}"
   增强: "Research {{topic}}

          [Plan Context]
          Strategy: {plan.overall_strategy}
          Your role in plan: {plan_step.reasoning}
          Expected: {plan_step.expected_output}"

5. 执行增强后的 Workflow（dispatch 到 DAG 或 Hierarchical）
```

### 4.5 Planning + Hierarchical 交互

Plan 以 **额外 system context** 形式注入 Manager Prompt：

```
Manager Prompt:
   System: {manager.instructions}
           [Plan] {plan.overall_strategy}
           Per-task instructions: {serialized_plan_steps}
   Available Agents: ...
   Pending Tasks: ...
   Completed Tasks: ...

Manager 可偏离 Plan（Plan 是建议，非强制约束）。
```

### 4.6 配置约束

| 规则 | 值 |
|---|---|
| `planning.planning_llm` 为 None | 使用第一个 step agent 的 model |
| Planning 超时 | `PLANNING_TIMEOUT_SECS = 60` |
| Plan 最大 token | `PLANNING_MAX_TOKENS = 2000` |

---

## 5. Checkpoint

### 5.1 定位

Checkpoint 是 Event Sourcing 的**性能优化层**，不替代事件日志。

```
Event Sourcing              Checkpoint
──────────────              ──────────
完整重放 N 个事件            加载 1 个快照 + 重放 M 个增量
精确但 N 大时慢              快速恢复
用于审计                    用于 crash recovery
```

### 5.2 数据模型

```rust
// tavern-comp/src/checkpoint.rs (新文件)

use chrono::{DateTime, Utc};
use crate::instance::InstanceState;

pub struct Checkpoint {
    pub instance_id: String,
    pub workflow_id: String,
    /// 已应用的事件数量（零基索引：event_sequence = N 表示
    /// 事件 [0..N) 已通过 apply() 反映在此快照中。
    /// 恢复时从 all_events[event_sequence..] 开始重放）
    pub event_sequence: u64,
    pub state: InstanceState,
    pub created_at: DateTime<Utc>,
}
```

### 5.3 EventStore 扩展

```rust
#[async_trait]
pub trait EventStore {
    // 现有方法不变
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> ...;
    async fn read_stream(&self, instance_id: &str) -> ...;
    // ...

    /// Phase 1 新增：保存 checkpoint
    async fn save_checkpoint(&self, checkpoint: Checkpoint) -> Result<(), CompError>;

    /// Phase 1 新增：加载最新 checkpoint
    async fn load_latest_checkpoint(
        &self,
        instance_id: &str,
    ) -> Result<Option<Checkpoint>, CompError>;
}
```

注：现有 `save_snapshot` / `load_snapshot`（存储裸 `InstanceState`）保持不变，与 checkpoint 独立。

### 5.4 快照策略

```
触发时机:  每个 StepCompleted 事件后
保留策略:  每个 instance 只保留最新 1 个 (覆盖写入)
存储:      MemoryEventStore.checkpoints: RwLock<HashMap<String, Checkpoint>>
```

### 5.5 恢复逻辑

```rust
impl WorkflowEngine {
    async fn rebuild_state(&self, instance_id: &str) -> Result<InstanceState, CompError> {
        match self.store.load_latest_checkpoint(instance_id).await? {
            Some(checkpoint) => {
                // 1. 从快照恢复
                let mut state = checkpoint.state;
                state.id = instance_id.to_string();

                // 2. 获取快照之后的所有事件
                let all_events = self.store.read_stream(instance_id).await?;
                // event_sequence = N → 事件 [0..N) 已在快照中
                // 需要重放 [N..)
                let delta_events = &all_events[checkpoint.event_sequence as usize..];

                // 3. 重放增量
                for event in delta_events {
                    state.apply(event)?;
                }
                Ok(state)
            }
            None => {
                // 无快照，完整重放
                let events = self.store.read_stream(instance_id).await?;
                let mut state = InstanceState {
                    id: instance_id.to_string(),
                    ..Default::default()
                };
                for event in events {
                    state.apply(&event)?;
                }
                Ok(state)
            }
        }
    }

    async fn maybe_checkpoint(
        &self,
        instance_id: &str,
        state: &InstanceState,
        event_sequence: u64,
    ) {
        let checkpoint = Checkpoint {
            instance_id: instance_id.to_string(),
            workflow_id: state.workflow_id.clone(),
            event_sequence,
            state: state.clone(),
            created_at: Utc::now(),
        };
        if let Err(e) = self.store.save_checkpoint(checkpoint).await {
            tracing::warn!(error = %e, "checkpoint save failed, continuing");
        }
    }
}
```

---

## 6. 向后兼容保证

| 现有功能 | Phase 1 后 |
|---|---|
| `Workflow::validate_static()` | 不含 `process` 字段时行为不变 |
| `WorkflowEngine::run()` | 签名不变，内部 dispatch 到 DAG 或 Hierarchical |
| `WorkflowEngine::start()` | 签名不变，支持两种 process |
| V2 ExecutionHandle / EventStore | 不变 |
| 现有 YAML 配置 | 无需修改 |
| 现有 REST API | 新增可选 `?process=` 参数 |
| 现有 81 tests | 全部通过 |

---

## 7. 错误处理

### CompError 新增变体

```rust
pub enum CompError {
    // 现有 20 个变体保持不变 ...

    /// Manager Agent 调用失败（LLM 返回非 JSON 或 Runtime 错误）
    ManagerError { reason: String },

    /// AgentPlanner 调用失败
    PlanningError { reason: String },

    /// Checkpoint 读写失败
    CheckpointError { reason: String },

    /// Manager 循环超过上限
    ManagerLoopExceeded { max_loops: usize },
}
```

### HTTP 映射

| Error | HTTP |
|---|---|
| `ManagerError` | 502 |
| `PlanningError` | 502 |
| `CheckpointError` | 500 |
| `ManagerLoopExceeded` | 500 |

---

## 8. 验收标准

- [ ] `cargo build --workspace` 通过
- [ ] `cargo test --workspace` 全部通过，现有 81 tests 零破坏
- [ ] `cargo clippy --workspace` 零新增警告
- [ ] Hierarchical：content_pipeline 通过 Manager Agent 完成三个 agent 的协调执行
- [ ] Hierarchical test：Manager 返回 `done` 时正确终止（用 MockRuntime 模拟）
- [ ] Hierarchical test：Manager 循环超过上限返回 `ManagerLoopExceeded`
- [ ] Hierarchical test：Manager agent 未注册返回 `AgentNotFound`
- [ ] Planning：planning_llm 生成有效 Plan，Plan Context 注入到 task
- [ ] Planning test：`enabled: false` 跳过 Planner
- [ ] Checkpoint test：模拟崩溃后从快照恢复（drop engine, rebuild_state）
- [ ] Checkpoint test：无快照时回退到完整事件重放
- [ ] YAML 兼容：不含 `process` 的旧 content_pipeline.yaml 正常执行
- [ ] API 兼容：`POST /workflows/:id/run` 不加参数正常

### 测试策略

- **Manager/Planner 行为测试**：使用 `MockRuntime` 注入预设 JSON 响应，验证解析和流程控制逻辑
- **Integration 测试**：使用真实 Pandaria（或 wiremock stub）验证完整 LLM 调用链路
- **Checkpoint 测试**：构造 event log → save checkpoint → rebuild → 验证状态一致性

---

## 9. 风险与缓解

| 风险 | 缓解 |
|---|---|
| Manager LLM 返回非 JSON | Prompt 强调 JSON-only；`parse_manager_response` 容错解析；最多重试 1 次 |
| Manager 无限循环 | `MAX_MANAGER_LOOPS = 100` 硬限制 |
| Hierarchical 忽略 `depends_on` 引发用户混淆 | 文档明确说明；`validate_static` 区分 process 类型 |
| `event_sequence` 语义错误导致状态恢复不一致 | 恢复逻辑单元测试覆盖边界：空事件流、单事件、跨 checkpoint 恢复 |
| Checkpoint 保存失败不阻塞执行 | `tracing::warn!` 记录，继续执行 |
