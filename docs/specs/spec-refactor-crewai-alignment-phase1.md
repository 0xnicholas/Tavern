# Tavern Refactor: CrewAI Alignment — Phase 1

## 1. 概述

Phase 1 改造 `tavern-comp`，增加两项 Crew 层能力：

- **Hierarchical Process**：Manager Agent（LLM 驱动）动态委派 Task
- **Planning**：AgentPlanner 执行前生成结构化执行计划

Agent 层（Tools、Skills、Knowledge）和 Flow 层（事件驱动方法编排）不在 Phase 1 范围。

> **注意**：Checkpoint（Event Sourcing 快照加速恢复）原在此 spec 中，已移至独立的基础设施演进任务。见 `plan-comp-v2-event-driven.md` Phase 4。

### 1.1 设计原则

- **向后兼容**：现有 DAG Workflow YAML 不变，`WorkflowEngine::run()` / `start()` 行为不变
- **Pandaria 不变**：LLM 调用持续通过 `Runtime` trait → Pandaria 代理
- **Memory 外接**：Memory 系统通过外部服务实现，Tavern 不内置 Memory crate
- **默认 Sequential**：`Process::Sequential` 为默认值，旧配置无需修改
- **所有 LLM 调用经 Hero**：Manager 和 Planner 都必须是 `AgentRegistry` 中注册的 Agent，不直接构造裸 LLM 请求

### 1.2 Crate 变更

| Crate | 变更 | 说明 |
|---|---|---|
| `tavern-core` | 修改 | 新增 `Process`、`ManagerConfig`、`PlanningConfig`、`Plan`、`Step.expected_output` |
| `tavern-comp` | 修改 | 新增 `run_interpreter_hierarchical`、Planner；`validate_static` 增加 process 感知 |
| `tavern-server` | 修改 | 新增 `?process=hierarchical` 查询参数（仅用于 API 覆盖静态配置）；错误映射扩展 |

---

## 2. 术语定义

| 术语 | 定义 |
|---|---|
| Process | 执行策略：`Sequential`（DAG）或 `Hierarchical`（Manager Agent） |
| Manager Agent | Hierarchical 模式中的调度 Agent，由 LLM 驱动，动态委派 Task。**必须是 registry 中的已注册 Agent** |
| AgentPlanner | Planning 模式中的规划 Agent，执行前输出 Plan。**由 `planning_agent` 字段指定，必须是 registry 中的已注册 Agent** |
| Plan | AgentPlanner 生成的执行计划，注入到 Task 描述中 |
| Planning Phase | 执行 Workflow 之前的规划阶段，若失败则 Workflow 不启动（Fails fast） |
| CompletedTask | Manager 循环中已完成步骤的摘要记录 |

### 2.1 Process 类型速览

| Process | 调度方式 | `depends_on` | 适用场景 |
|---|---|---|---|
| `Sequential`（默认） | DAG 拓扑排序 | 被读取 | 确定性流程：研究→写作→审核→发布 |
| `Hierarchical` | Manager LLM 动态委派 | **被忽略** | 非确定性流程：复杂问题分解、自适应任务分配 |

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
    #[serde(default)]
    pub planning: Option<PlanningConfig>,
}
```

**YAML serde 策略**：`process` 和 `manager` 是两个独立的 YAML key，需自定义反序列化：

```rust
// 反序列化时：
// process: "hierarchical" → 标记 variant
// manager: { agent_id: "...", instructions: "..." } → ManagerConfig
// 二者组合为 Process::Hierarchical(ManagerConfig { ... })
// 
// 方案：在 Workflow 的 Deserialize 实现中，先读取中间结构体
// #[serde(rename = "process")] process_tag: String
// #[serde(default)] manager: Option<ManagerConfig>,
// 然后根据 process_tag 构造 Process enum
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
    expected_output: "A structured research report with key findings and sources"
    retries: 1
  - id: write_article
    agent_id: writer
    task: "Write about: {{research_notes}}"
    expected_output: "A well-written article of 500-1000 words"
    retries: 1
  - id: polish
    agent_id: editor
    task: "Polish: {{draft}}"
    expected_output: "A publication-ready final draft"
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
   │ ## Output Format                                  │
   │ You MUST respond with valid JSON only.            │
   │ {"action":"delegate","task_id":"...","agent_id":"..."}
   │ or {"action":"done"}                              │
   │                                                   │
   │ User:                                             │
   │ ## Available Agents                               │
   │ - {agent_id}: {agent.description}                 │
   │   Skills: {agent.skills}                          │
   │   Instructions summary: {前 300 字符}              │
   │                                                   │
   │ ## Pending Tasks                                  │
   │ - {task_id}: {task_description}                   │
   │   Expected: {expected_output}                     │
   │                                                   │
   │ ## Completed Tasks                                │
   │ {task_id} → {agent_id}: {output_summary} (前500字符)│
   │                                                   │
   │ Decide the next action. Output JSON only.         │
   └──────────────────────────────────────────────────┘

2. 调用：hero.execute(manager.agent_id, &manager_prompt, None)
   → Pandaria → LLM → 返回 JSON 字符串

3. 解析决策：parse_manager_response(json_str)
   → 容错策略见下文

4. 如果是 Delegate { task_id, agent_id }:
   - 渲染 task 模板（注入 context 中的 step outputs）
   - 检查 agent_id 是否在 registry 中（防御性检查）
   - hero.execute(agent_id, &rendered_task, Some(context))
   - 记录到 CompletedTask 列表
   - 返回步骤 1（循环）

5. 如果是 Done:
   - 汇总所有 CompletedTask → WorkflowResult
```

**关键点**：Manager Agent 必须在 `AgentRegistry` 中注册（配置 model/provider）。提示词中的 agent 信息包含 `description`、`skills` 列表和 `instructions` 摘要，帮助 Manager 做出准确的委派决策。

**JSON 解析容错策略**（`parse_manager_response`）：

```
1. 尝试直接 serde_json::from_str
2. 失败 → 搜索 ```json ... ``` code block，提取内容后重试
3. 失败 → 搜索第一个 { 到最后一个 }，截取后重试
4. 失败 → 如果是第一次解析失败，将原始响应和错误信息注入下一轮 Manager Prompt
          （告知 Manager 格式错误并重新请求），然后递归调用 hero.execute
5. 第二次仍失败 → 返回 ManagerError { reason: "failed to parse response after retry" }
```

### 3.5 Engine API

```rust
// tavern-comp/src/engine.rs

impl WorkflowEngine {

    // V1 同步执行 (保留，内部委托给 start + await_completion)
    pub async fn run(&self, workflow: &Workflow, inputs: Value)
        -> Result<WorkflowResult, CompError>;

    // V2 异步启动 (保留，内部根据 process 选择解释器 spawn)
    pub async fn start(&self, workflow: &Workflow, inputs: Value)
        -> Result<ExecutionHandle, CompError>;

    // ── 内部解释器（签名一致，由 start() 的 spawn closure 路由） ──

    /// Sequential 解释器（现有）
    async fn run_interpreter(
        &self,
        instance_id: String,
        workflow: Workflow,
        signal_rx: mpsc::Receiver<WorkflowEvent>,
        completion_tx: tokio::sync::oneshot::Sender<Result<WorkflowResult, CompError>>,
    ) -> Result<(), CompError>;

    /// Hierarchical 解释器（新增，签名一致）
    async fn run_interpreter_hierarchical(
        &self,
        instance_id: String,
        workflow: Workflow,
        manager_config: ManagerConfig,
        mut signal_rx: mpsc::Receiver<WorkflowEvent>,
        completion_tx: tokio::sync::oneshot::Sender<Result<WorkflowResult, CompError>>,
    ) -> Result<(), CompError>;

    /// 构建 Manager prompt
    fn build_manager_prompt(
        &self,
        workflow: &Workflow,
        manager_config: &ManagerConfig,
        completed: &[CompletedTask],
        pending_step_ids: &[String],
    ) -> String;

    /// 解析 Manager LLM 的 JSON 响应（含重试）
    async fn parse_manager_response(
        &self,
        manager_agent_id: &str,
        workflow: &Workflow,
        manager_config: &ManagerConfig,
        completed: &[CompletedTask],
        pending_step_ids: &[String],
        raw_response: &str,
    ) -> Result<ManagerDecision, CompError>;
}

/// 已完成步骤的摘要（从 Event Log 推导，不独立存储）
struct CompletedTask {
    task_id: String,
    agent_id: String,
    output: Value,
    /// 非致命错误信息（如 Agent 执行成功但 Manager 认为质量不足）
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

### 3.6 Hierarchical 事件循环（新增）

`run_interpreter_hierarchical` 的事件循环与 Sequential 共享相同的 Event Sourcing 机制：

```rust
async fn run_interpreter_hierarchical(
    &self,
    instance_id: String,
    workflow: Workflow,
    manager_config: ManagerConfig,
    mut signal_rx: mpsc::Receiver<WorkflowEvent>,
    completion_tx: tokio::sync::oneshot::Sender<Result<WorkflowResult, CompError>>,
) -> Result<(), CompError> {
    let mut state = self.rebuild_state(&instance_id).await?;
    self.apply_and_persist(&instance_id, WorkflowEvent::InstanceStarted, &mut state).await?;

    let (internal_tx, mut internal_rx) = mpsc::channel::<WorkflowEvent>(64);
    let executor = StepExecutor::new(self.hero.clone(), internal_tx.clone(), self.max_concurrency);
    let timer_registry = TimerRegistry::new(internal_tx.clone());

    // 追踪 Manager 循环状态
    let mut completed_tasks: Vec<CompletedTask> = Vec::new();
    let mut manager_loops: usize = 0;

    let result: Result<WorkflowResult, CompError> = async {
        loop {
            manager_loops += 1;
            if manager_loops > MAX_MANAGER_LOOPS {
                break Err(CompError::ManagerLoopExceeded { max_loops: MAX_MANAGER_LOOPS });
            }

            // 计算 pending steps（未被 complete 覆盖的 step）
            let pending_ids: Vec<String> = workflow.steps.iter()
                .map(|s| s.id.clone())
                .filter(|id| !completed_tasks.iter().any(|ct| ct.task_id == *id))
                .collect();

            if pending_ids.is_empty() {
                // 所有 step 完成 → 构建 outputs 并结束
                let outputs = self.build_workflow_outputs(&workflow, &state)?;
                let event = WorkflowEvent::WorkflowCompleted {
                    outputs: outputs.clone(),
                    completed_at: Utc::now(),
                };
                self.apply_and_persist(&instance_id, event, &mut state).await?;
                break Ok(WorkflowResult {
                    context: state.context.clone(),
                    outputs,
                    step_results: state.step_results.clone(),
                });
            }

            // ① 构建 Manager prompt
            let prompt = self.build_manager_prompt(
                &workflow, &manager_config,
                &completed_tasks, &pending_ids,
            );

            // ② 调用 Manager Agent
            //    注意：Manager 也是通过 StepExecutor 执行（不是特殊路径），
            //    但 Manager 的调用直接通过 hero.execute，不走 StepExecutor，
            //    因为 Manager 决策不产生 StepScheduled/StepStarted/StepCompleted 事件
            let manager_result = self.hero.execute(
                &manager_config.agent_id,
                &prompt,
                None,  // Manager 不需要 context
            ).await;

            match manager_result {
                Ok(raw_response) => {
                    let response_str = raw_response.as_str()
                        .unwrap_or(&raw_response.to_string());

                    let decision = self.parse_manager_response(
                        &manager_config.agent_id,
                        &workflow,
                        &manager_config,
                        &completed_tasks,
                        &pending_ids,
                        response_str,
                    ).await?;

                    match decision {
                        ManagerDecision::Delegate { task_id, agent_id } => {
                            // 验证 step 和 agent 存在
                            let step = workflow.steps.iter()
                                .find(|s| s.id == task_id)
                                .ok_or(CompError::ManagerError {
                                    reason: format!("Manager returned unknown task_id: {}", task_id),
                                })?;

                            if self.hero.get_agent(&agent_id).is_none() {
                                return Err(CompError::ManagerError {
                                    reason: format!("Manager returned unknown agent_id: {}", agent_id),
                                });
                            }

                            // ③ 通过事件溯源执行 step（与 Sequential 相同的事件流）
                            let attempt = self.get_attempt(&state, &task_id);
                            let max_retries = step.retries.unwrap_or(0);

                            // 持久化调度事件
                            let event = WorkflowEvent::StepScheduled {
                                step_id: task_id.clone(),
                                attempt,
                            };
                            self.apply_and_persist(&instance_id, event, &mut state).await?;

                            // 提交执行（StepExecutor 会在完成后 emit StepCompleted/StepFailed）
                            executor.submit(step.clone(), state.context.clone(), attempt, max_retries).await;

                            // ④ 等待步骤结果
                            let step_result = loop {
                                tokio::select! {
                                    Some(event) = internal_rx.recv() => {
                                        self.apply_and_persist(&instance_id, event.clone(), &mut state).await?;

                                        match &event {
                                            WorkflowEvent::StepCompleted { step_id, output, attempt, .. } => {
                                                break Ok(CompletedTask {
                                                    task_id: step_id.clone(),
                                                    agent_id: step.agent_id.clone(),
                                                    output: output.clone(),
                                                    error: None,
                                                });
                                            }
                                            WorkflowEvent::StepFailed { step_id, error, will_retry: false, .. } => {
                                                // 不重试：将错误记录到 CompletedTask
                                                break Ok(CompletedTask {
                                                    task_id: step_id.clone(),
                                                    agent_id: step.agent_id.clone(),
                                                    output: Value::Null,
                                                    error: Some(error.clone()),
                                                });
                                            }
                                            WorkflowEvent::StepFailed { step_id, attempt, will_retry: true, .. } => {
                                                // 引擎自动重试：注册定时器
                                                let delay = self.get_retry_delay(&workflow, step_id);
                                                let scheduled_at = Utc::now() + chrono::Duration::seconds(delay as i64);
                                                let retry_event = WorkflowEvent::StepRetryScheduled {
                                                    step_id: step_id.clone(),
                                                    attempt: attempt + 1,
                                                    scheduled_at,
                                                };
                                                self.apply_and_persist(&instance_id, retry_event, &mut state).await?;
                                                timer_registry.register(
                                                    format!("retry_{}_{}", step_id, attempt + 1),
                                                    scheduled_at,
                                                ).await;
                                                // continue waiting for the retry result
                                            }
                                            _ => {}
                                        }
                                    }
                                    Some(event) = signal_rx.recv() => {
                                        // 外部信号（如 cancel）
                                        self.apply_and_persist(&instance_id, event, &mut state).await?;
                                    }
                                    else => {
                                        break Err(CompError::Internal("event channels closed".into()));
                                    }
                                }
                            }?;

                            completed_tasks.push(step_result);
                            // 循环回到 Manager 决策
                        }
                        ManagerDecision::Done => {
                            let outputs = self.build_workflow_outputs(&workflow, &state)?;
                            let event = WorkflowEvent::WorkflowCompleted {
                                outputs: outputs.clone(),
                                completed_at: Utc::now(),
                            };
                            self.apply_and_persist(&instance_id, event, &mut state).await?;
                            break Ok(WorkflowResult {
                                context: state.context.clone(),
                                outputs,
                                step_results: state.step_results.clone(),
                            });
                        }
                    }
                }
                Err(e) => {
                    break Err(CompError::ManagerError {
                        reason: format!("Manager agent execution failed: {}", e),
                    });
                }
            }
        }
    }.await;

    let _ = completion_tx.send(result.clone());
    result.map(|_| ())
}
```

**关键设计决策**：

- **Manager 不产生独立事件**：Manager 的 LLM 调用不写入 Event Log（Manager 是调度逻辑，不是工作流步骤）。只有被委派的 Step 产生事件。
- **Step 执行复用事件溯源**：每个被委派的 Step 走 `StepScheduled → StepStarted → StepCompleted/Failed` 完整序列，与 Sequential 完全一致。
- **重试在 Step 内部处理**：`StepFailed(will_retry=true)` 触发定时器重试，Manager 不参与重试决策。
- **信号支持**：`signal_rx` 通道确保 `cancel` 等外部信号可中断 Hierarchical 执行。
- **`CompletedTask` 从 Event Log 推导**：不独立存储，从 `state.step_results` 重建。`error` 字段仅在 Manager 需要判断质量时非 None。

### 3.7 `start()` 与 Hierarchical 的兼容

`start()` 的 spawn closure 根据 `workflow.process` 选择解释器：

```rust
let interpreter_future = match &workflow.process {
    Process::Sequential => {
        engine.run_interpreter(id_clone, workflow, signal_rx, completion_tx)
    }
    Process::Hierarchical(cfg) => {
        engine.run_interpreter_hierarchical(
            id_clone, workflow, cfg.clone(), signal_rx, completion_tx
        )
    }
};
let interpreter_handle = tokio::spawn(interpreter_future);
```

两种解释器的签名一致（都接收 `completion_tx`，返回 `Result<(), CompError>`），
`start()` 的 `ExecutionHandle` 构造逻辑完全复用。

`run()` 同步方法内部：
```rust
let mut handle = self.start(workflow, inputs).await?;
handle.await_completion().await
```
同样无需修改。

Hierarchical 模式的事件流：

```
InstanceCreated → InstanceStarted
  → StepScheduled { step_id: "gather_info", attempt: 1 }
  → StepStarted { step_id: "gather_info" }
  → StepCompleted { step_id: "gather_info", output_key: "research_notes", ... }
  → StepScheduled { step_id: "write_article", attempt: 1 }
  → ...
  → WorkflowCompleted
```

每个 Task 的执行产生与 Sequential 相同的事件序列。

### 3.8 Hierarchical 模式的 Outputs 构建

Hierarchical 模式下 `outputs` 字段的处理逻辑与 Sequential 一致（从 `workflow.outputs` 模板渲染），
但需注意：

```
- Manager 可能选择不执行某些 step
- 若某个 OutputDef 的 value 引用了未执行 step 的 output_key
  → 模板渲染报错 → WorkflowFailed
- 建议：Hierarchical 模式下 outputs 引用「一定会被执行的 step」的输出，
  或使用 inputs 中的变量（inputs 始终可用）
```

若 `workflow.outputs` 为空，`outputs` 返回空对象 `{}`，调用方从 `context` 中提取所需数据。

### 3.9 约束与限制

| 规则 | 值 |
|---|---|
| Manager Agent 必须在 registry 注册 | `validate()` 检查，否则 `AgentNotFound` |
| `process: hierarchical` 时 `validate_static` 跳过 DAG 校验 | 仍校验 ID 格式、步骤唯一性、task 长度等 |
| `depends_on` 在 hierarchical 模式下忽略 | Manager 动态决定顺序 |
| 最大 Task 数量（Hierarchical） | `MAX_HIERARCHICAL_TASKS = 50`（覆盖 `MAX_STEPS = 100`，`validate_static` 按 process 分支检查） |
| Manager 最大循环次数 | `MAX_MANAGER_LOOPS = 100`（防无限） |
| Manager Prompt 最大长度 | `MAX_MANAGER_PROMPT_LEN = 100_000` |

`validate_static` 中步骤数检查按 process 分支：

```rust
match &self.process {
    Process::Sequential => {
        if self.steps.len() > MAX_STEPS { return Err(...); }
        validate_dag(&self.steps)?;
    }
    Process::Hierarchical(_) => {
        if self.steps.len() > MAX_HIERARCHICAL_TASKS { return Err(...); }
        // 不校验 DAG
    }
}
```

### 3.10 `validate_static()` 变更

`validate_static` 仅做**静态**检查（不依赖 Hero/Registry）。Manager agent 的注册存在性检查移至 `validate()`。

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
                // 跳过 DAG 校验
                // Manager agent_id 只做 ID 格式检查，注册存在性由 validate() 检查
                if !is_valid_id(&cfg.agent_id) {
                    return Err(CompError::ConfigParse { ... });
                }
            }
        }

        Ok(())
    }
}
```

```rust
// engine.rs: validate() 扩展 Manager agent + Planner agent 检查
impl WorkflowEngine {
    pub fn validate(&self, workflow: &Workflow) -> Result<(), CompError> {
        workflow.validate_static()?;

        // 检查每个 step 的 agent
        for step in &workflow.steps {
            if self.hero.get_agent(&step.agent_id).is_none() {
                return Err(CompError::AgentNotFound { id: step.agent_id.clone() });
            }
        }

        // Hierarchical: 额外检查 Manager agent
        if let Process::Hierarchical(cfg) = &workflow.process {
            if self.hero.get_agent(&cfg.agent_id).is_none() {
                return Err(CompError::AgentNotFound { id: cfg.agent_id.clone() });
            }
        }

        // Planning: 检查 planning_agent
        if let Some(ref planning) = workflow.planning {
            if planning.enabled {
                let agent_id = planning.planning_agent.as_deref()
                    .unwrap_or(&workflow.steps[0].agent_id);
                if self.hero.get_agent(agent_id).is_none() {
                    return Err(CompError::AgentNotFound { id: agent_id.to_string() });
                }
            }
        }

        Ok(())
    }
}
```

---

## 4. Planning

### 4.1 概念

Planning 在执行前插入 **Planning Phase**。AgentPlanner（registry 中的已注册 Agent）分析所有 Task，输出结构化 `Plan`，Plan Context 被注入到每个 Task 的描述中。

Planning 可与 Sequential 或 Hierarchical 任意组合。

```
┌──────────┐    ┌───────────────┐    ┌──────────────────┐    ┌──────────┐
│ Task 列表 │ → │ AgentPlanner  │ → │ Plan Context 注入 │ → │ 正常执行  │
└──────────┘    └───────────────┘    └──────────────────┘    └──────────┘
      ↑                ↑                     ↑
   (step 定义)   (hero.execute)        (修改 task 字符串)
```

**Planning 失败即失败（Fails fast）**：如果 Planner 超时、返回无效 JSON、或 Agent 执行出错，工作流不启动，直接返回 `PlanningError`。

### 4.2 数据模型

```rust
// tavern-core/src/config.rs

pub struct PlanningConfig {
    pub enabled: bool,
    /// AgentPlanner 的 agent_id。
    /// 如果为 None 且 enabled=true，使用 workflow.steps[0].agent_id 作为 Planner。
    /// 该 Agent 必须已在 registry 中注册。
    pub planning_agent: Option<String>,
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
    /// Planner 建议的依赖关系。
    /// Sequential 模式：若 Planner 返回非空 dependencies，覆盖 Workflow 的静态 depends_on。
    /// Hierarchical 模式：被忽略，Manager 动态决定执行顺序。
    pub dependencies: Vec<String>,
}

// ── 新增字段：Step.expected_output ──

pub struct Step {
    // ... 现有字段不变 ...

    /// 可选的预期输出描述，帮助 LLM 理解任务目标。
    /// 在 Manager prompt 和 Planning Context 注入时使用。
    /// YAML 中可省略，默认 None。
    #[serde(default)]
    pub expected_output: Option<String>,
}
```

### 4.3 Workflow YAML

```yaml
id: complex_research
planning:
  enabled: true
  planning_agent: "planner"   # agent_id，必须在 registry 注册

process: sequential    # 或 hierarchical

steps:
  - id: research
    agent_id: researcher
    task: "Research {{topic}}"
    expected_output: "A comprehensive research document with citations"
  # ...
```

**`planning_agent` 省略时的回退**：

```yaml
planning:
  enabled: true
  # planning_agent 未指定 → 使用 workflow.steps[0].agent_id 作为 Planner
  # 等价于 planning_agent: "researcher"
```

### 4.4 执行流程

```
0. validate() 阶段：检查 planning_agent（或回退 agent）在 registry 中存在

1. 检查 workflow.planning.enabled —— false 则跳过 Planning Phase

2. 确定 planner_agent_id:
   let planner_agent_id = planning.planning_agent.as_deref()
       .unwrap_or(&workflow.steps[0].agent_id);

3. 构建 Planner Prompt（System = agent instructions, User = task list）:
   System: {agent.instructions}
   User:
     "You are a planning agent for workflow: {workflow.description}.
      Tasks to plan:
      {for each step: id, agent_id, task, expected_output}

      Output a JSON plan with:
      - overall_strategy: string
      - steps: [{
          task_id: string,
          agent_id: string,
          reasoning: string (why this step is needed),
          expected_output: string,
          dependencies: [string] (task_ids that must complete before this task)
        }]"

4. 发送给 AgentPlanner：
   hero.execute(planner_agent_id, &planner_prompt, None)
   → 超时：PLANNING_TIMEOUT_SECS = 60 秒
   → JSON 解析容错：同 Manager 的容错策略（code block 提取 + 子串截取 + 一次重试）

5. 解析 Plan JSON → Plan struct
   验证：Plan 中每个 task_id 必须在 workflow.steps 中存在
   验证：Plan 中每个 agent_id 必须在 registry 中存在
   任一验证失败 → PlanningError

6. 注入 Plan Context：

   a) Sequential 模式：
      - 若 PlanStep.dependencies 非空，**覆盖**对应 Step 的 depends_on
      - 将 Plan Context 注入到每个 step.task 的**原始模板**中
        （模板渲染在 StepExecutor 内部进行）

   原始 task 模板:
     "Research {{topic}}"

   注入后:
     "Research {{topic}}

      [Plan Context]
      Overall Strategy: {plan.overall_strategy}
      Your role in this plan: {plan_step.reasoning}
      Expected output: {plan_step.expected_output}"

   b) Hierarchical 模式：
      - Plan 写入 state.context._plan（JSON Value），不在 step.task 层面注入
      - build_manager_prompt 读取 state.context._plan 并注入 Manager Prompt

7. 注入了 Plan 的 Workflow 正常执行（dispatch 到 DAG 或 Hierarchical）
```

### 4.5 Planning + Hierarchical 交互

Plan 以 **额外 User section** 形式注入 Manager Prompt：

```
Manager Prompt:
  System: (同上)
  User:
    ## Execution Plan
    Strategy: {plan.overall_strategy}
    Per-step plan:
    - {task_id}: assigned to {agent_id}
      Reasoning: {plan_step.reasoning}
      Expected: {plan_step.expected_output}

    ## Available Agents
    ...

    ## Pending Tasks
    ...

    ## Completed Tasks
    ...

    Decide the next action. Output JSON only.
```

Manager 可偏离 Plan（Plan 是建议，非强制约束）。

**与 worker agent 的区别**：worker agent（被委派的 Step）的 task 描述在 §4.4 步骤 6 中通过字符串拼接直接注入 Plan Context。Manager 的 Plan 信息是通过 `build_manager_prompt` 的 User section 传入，二者机制统一为「将 Plan 信息以结构化文本注入到调用 prompt 中」。

### 4.6 Planning Phase 失败处理

```
Planning Phase 发生的错误不进入 Event Log（Workflow 未启动）：

- Planner Agent 不存在 → AgentNotFound（validate 阶段捕获）
- Planner 超时 → PlanningError { reason: "planner timed out after 60s" }
- Planner 返回无效 JSON → PlanningError { reason: "..." }（含一次重试）
- Plan 引用不存在的 task_id/agent_id → PlanningError { reason: "..." }
- Planner Agent 执行错误（Runtime 502） → PlanningError { reason: "..." }

所有 PlanningError 映射为 HTTP 502
```

### 4.7 配置约束

| 规则 | 值 |
|---|---|
| `planning.enabled` | 默认 false（不指定 planning 节点时不启动） |
| `planning.planning_agent` 为 None | 使用 `workflow.steps[0].agent_id` 作为 Planner |
| Planning 超时 | `PLANNING_TIMEOUT_SECS = 60` |
| Plan 最大 token | `PLANNING_MAX_TOKENS = 2000` |
| Planner JSON 解析重试 | 最多 1 次 |

---

## 5. 向后兼容保证

| 现有功能 | Phase 1 后 |
|---|---|
| `Workflow::validate_static()` | 不含 `process` 字段时行为不变 |
| `WorkflowEngine::run()` | 签名不变，内部 dispatch 到 DAG 或 Hierarchical |
| `WorkflowEngine::start()` | 签名不变，支持两种 process |
| V2 ExecutionHandle / EventStore | 不变（Hierarchical 复用相同事件类型） |
| 现有 YAML 配置 | 无需修改 |
| 现有 REST API | `?process=hierarchical` 仅用于覆盖 YAML 的静态 process 配置（可选参数） |
| 现有全部 tests | 全部通过（Sequential 的不含 `process` 字段） |

---

## 6. 错误处理

### CompError 新增变体

```rust
pub enum CompError {
    // 现有 20 个变体保持不变 ...

    #[error("manager agent error: {reason}")]
    ManagerError { reason: String },

    #[error("manager loop exceeded max loops ({max_loops})")]
    ManagerLoopExceeded { max_loops: usize },

    #[error("planning error: {reason}")]
    PlanningError { reason: String },

    #[error("planning agent '{id}' not registered")]
    PlanningAgentNotRegistered { id: String },
}
```

### HTTP 映射

| Error | HTTP |
|---|---|
| `ManagerError` | 502 |
| `ManagerLoopExceeded` | 500 |
| `PlanningError` | 502 |
| `PlanningAgentNotRegistered` | 500 |

---

## 7. 验收标准

- [ ] `cargo build --workspace` 通过
- [ ] `cargo test --workspace` 全部通过，现有全部 tests 零破坏
- [ ] `cargo clippy --workspace` 零新增警告
- [ ] Hierarchical：content_pipeline 通过 Manager Agent 完成三个 agent 的协调执行
- [ ] Hierarchical test：Manager 返回 `done` 时正确终止（用 MockRuntime 模拟）
- [ ] Hierarchical test：Manager 循环超过上限返回 `ManagerLoopExceeded`
- [ ] Hierarchical test：Manager agent 未注册返回 `AgentNotFound`
- [ ] Hierarchical test：Manager 返回非 JSON → 容错解析成功 + 一次重试后失败
- [ ] Hierarchical test：Manager 委派不存在的 task_id → `ManagerError`
- [ ] Hierarchical test：Hierarchical 事件流与 Sequential 一致（StepScheduled → Completed）
- [ ] Planning：planning_agent 生成有效 Plan，Plan Context 注入到 task
- [ ] Planning test：`enabled: false` 跳过 Planner
- [ ] Planning test：`planning_agent` 省略时回退到 `steps[0].agent_id`
- [ ] Planning test：Plan 引用不存在 task_id → `PlanningError`，Workflow 不启动
- [ ] Planning test：Planning 超时 → `PlanningError`
- [ ] Planning + Hierarchical：Plan 注入 Manager Prompt
- [ ] YAML 兼容：不含 `process` 的旧 content_pipeline.yaml 正常执行
- [ ] API 兼容：`POST /workflows/:id/run` 不加参数正常
- [ ] `Step.expected_output` 新增字段不影响现有 YAML（`#[serde(default)]`）

### 测试策略

- **Manager/Planner 行为测试**：使用 `MockRuntime` 注入预设 JSON 响应，验证解析和流程控制逻辑
- **Hierarchical 事件循环测试**：验证 Manager 决策 → Step 执行 → 结果收集的完整循环
- **Integration 测试**：使用真实 Pandaria（或 wiremock stub）验证完整 LLM 调用链路
- **容错解析测试**：验证 code block 提取、子串截取、重试逻辑

---

## 8. 风险与缓解

| 风险 | 缓解 |
|---|---|
| Manager LLM 返回非 JSON | 三层容错解析 + 一次重试告知 Manager 格式错误 |
| Manager 无限循环 | `MAX_MANAGER_LOOPS = 100` 硬限制 |
| Hierarchical 忽略 `depends_on` 引发用户混淆 | 文档明确说明；`validate_static` 区分 process 类型 |
| Planner 生成的依赖与 YAML 定义的冲突 | Sequential 模式下 Planner 的 `dependencies` 覆盖 YAML 的 `depends_on`（Plan 优先；设计意图如此） |
| `planning_agent` 回退到 step[0] 的 agent 产生低质量 Plan | 文档建议为关键 Workflow 显式配置专用 Planner Agent |
| Hierarchical outputs 因未执行 step 导致模板渲染失败 | 文档建议 outputs 仅引用必定执行的 step 的 output_key |
| Manager 委派已完成的 step_id | 防御性检查：`pending_ids` 只包含未完成的 step；如果 Manager 返回已完成 step_id → `ManagerError` |
