use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::instrument;
use uuid::Uuid;

use crate::context::render_template;
use crate::error::CompError;
use crate::event::WorkflowEvent;
use crate::executor::StepExecutor;
use crate::instance::{InstanceState, InstanceStatus};
use crate::store::{EventStore, MemoryEventStore};
use crate::timer::TimerRegistry;
use crate::workflow::{ManagerConfig, Process, StepStatus, Workflow, WorkflowResult};

use super::handle::ExecutionHandle;

// ── Phase 1 常量 ──
const MAX_MANAGER_LOOPS: usize = 100;
const PLANNING_TIMEOUT_SECS: u64 = 60;

// ── 辅助类型 ──

pub(crate) struct CompletedTask {
    pub task_id: String,
    pub agent_id: String,
    pub output: Value,
    #[allow(dead_code)]
    pub error: Option<String>,
}

pub(crate) enum ManagerDecision {
    Delegate { task_id: String, agent_id: String },
    Done,
}

/// 从 LLM 响应中解析 Manager JSON 决策（含 code block 提取和子串截取容错）。
fn parse_manager_json(raw: &str) -> Result<ManagerDecision, String> {
    let json_str = extract_json(raw);
    let val: Value = serde_json::from_str(&json_str).map_err(|e| format!("invalid JSON: {}", e))?;

    let action = val["action"]
        .as_str()
        .ok_or_else(|| "missing 'action' field".to_string())?;

    match action {
        "delegate" => {
            let task_id = val["task_id"]
                .as_str()
                .ok_or_else(|| "missing 'task_id'".to_string())?
                .to_string();
            let agent_id = val["agent_id"]
                .as_str()
                .ok_or_else(|| "missing 'agent_id'".to_string())?
                .to_string();
            Ok(ManagerDecision::Delegate { task_id, agent_id })
        }
        "done" => Ok(ManagerDecision::Done),
        other => Err(format!("unknown action: '{}'", other)),
    }
}

/// 从 LLM 响应中提取 JSON：直接解析 → ```json block → 首尾 {} 截取。
fn extract_json(raw: &str) -> String {
    // 尝试直接解析
    if serde_json::from_str::<Value>(raw).is_ok() {
        return raw.to_string();
    }
    // 搜索 ```json ... ```
    if let Some(start) = raw.find("```json") {
        let after = &raw[start + 7..];
        if let Some(end) = after.find("```") {
            return after[..end].trim().to_string();
        }
    }
    // 搜索 ``` ... ```
    if let Some(start) = raw.find("```") {
        let after = &raw[start + 3..];
        if let Some(end) = after.find("```") {
            return after[..end].trim().to_string();
        }
    }
    // 截取首 { 到尾 }
    if let Some(start) = raw.find('{') {
        if let Some(end) = raw.rfind('}') {
            return raw[start..=end].to_string();
        }
    }
    raw.to_string()
}

/// 解析 JSON 并支持一次重试（用于 Planning）。
fn parse_json_with_retry<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, String> {
    let json_str = extract_json(raw);
    serde_json::from_str(&json_str).map_err(|e| format!("invalid JSON: {}", e))
}

#[derive(Debug)]
pub enum Action {
    ScheduleSteps(Vec<String>),
    WaitForEvent,
    Complete(Value),
    Fail(String),
}

/// Workflow 执行引擎，V2 重构为事件溯源状态机解释器。
#[derive(Clone)]
pub struct WorkflowEngine {
    hero: Arc<tavern_hero::TavernHero>,
    store: Arc<dyn EventStore>,
    max_concurrency: usize,
}

impl WorkflowEngine {
    /// 初始化，注入 TavernHero，默认使用内存事件存储。
    pub fn new(hero: Arc<tavern_hero::TavernHero>) -> Self {
        Self {
            hero,
            store: Arc::new(MemoryEventStore::new()),
            max_concurrency: usize::MAX,
        }
    }

    /// 使用自定义 EventStore 初始化。
    pub fn with_store(mut self, store: Arc<dyn EventStore>) -> Self {
        self.store = store;
        self
    }

    /// 设置最大并发数（默认不限制）。
    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    /// 验证 Workflow 的合法性（含动态检查）。
    pub fn validate(&self, workflow: &Workflow) -> Result<(), CompError> {
        workflow.validate_static()?;

        for step in &workflow.steps {
            if self.hero.get_agent(&step.agent_id).is_none() {
                return Err(CompError::AgentNotFound {
                    id: step.agent_id.clone(),
                });
            }
        }

        // Hierarchical: 额外检查 Manager agent
        if let Process::Hierarchical(cfg) = &workflow.process {
            if self.hero.get_agent(&cfg.agent_id).is_none() {
                return Err(CompError::AgentNotFound {
                    id: cfg.agent_id.clone(),
                });
            }
        }

        // Planning: 检查 planning_agent
        if let Some(ref planning) = workflow.planning {
            if planning.enabled {
                let agent_id = planning
                    .planning_agent
                    .as_deref()
                    .unwrap_or(&workflow.steps[0].agent_id);
                if self.hero.get_agent(agent_id).is_none() {
                    return Err(CompError::PlanningAgentNotRegistered {
                        id: agent_id.to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// 启动工作流实例（非阻塞）。
    #[instrument(skip(self, workflow, inputs), fields(workflow_id = %workflow.id))]
    pub async fn start(
        &self,
        workflow: &Workflow,
        inputs: Value,
    ) -> Result<ExecutionHandle, CompError> {
        self.validate(workflow)?;
        let inputs = normalize_inputs(workflow, &inputs)?;

        // ── Planning Phase ──
        let workflow = if let Some(ref planning) = workflow.planning {
            if planning.enabled {
                self.run_planning_phase(workflow).await?
            } else {
                workflow.clone()
            }
        } else {
            workflow.clone()
        };

        let id = Uuid::new_v4().to_string();

        self.store
            .append(
                &id,
                WorkflowEvent::InstanceCreated {
                    workflow_id: workflow.id.clone(),
                    inputs: inputs.clone(),
                },
            )
            .await?;

        let (signal_tx, signal_rx) = mpsc::channel::<WorkflowEvent>(64);
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let engine = self.clone();
        let id_clone = id.clone();

        let process = workflow.process.clone();
        let interpreter_handle = tokio::spawn(async move {
            let result = match &process {
                Process::Sequential => {
                    engine
                        .run_interpreter(id_clone, workflow, signal_rx, completion_tx)
                        .await
                }
                Process::Hierarchical(cfg) => {
                    engine
                        .run_interpreter_hierarchical(
                            id_clone,
                            workflow,
                            cfg.clone(),
                            signal_rx,
                            completion_tx,
                        )
                        .await
                }
            };
            if let Err(ref e) = result {
                tracing::error!(error = %e, "interpreter failed");
            }
            result
        });

        Ok(ExecutionHandle {
            id,
            signal_tx,
            interpreter_handle,
            completion_rx: Some(completion_rx),
        })
    }

    /// V1 兼容层：同步阻塞执行。
    pub async fn run(
        &self,
        workflow: &Workflow,
        inputs: Value,
    ) -> Result<WorkflowResult, CompError> {
        let mut handle = self.start(workflow, inputs).await?;
        handle.await_completion().await
    }

    // ── Planning Phase ──

    async fn run_planning_phase(&self, workflow: &Workflow) -> Result<Workflow, CompError> {
        let planning = workflow.planning.as_ref().unwrap();
        let planner_agent_id = planning
            .planning_agent
            .as_deref()
            .unwrap_or(&workflow.steps[0].agent_id);

        let planner_prompt = self.build_planner_prompt(workflow);

        let raw = tokio::time::timeout(
            std::time::Duration::from_secs(PLANNING_TIMEOUT_SECS),
            self.hero.execute(planner_agent_id, &planner_prompt, None),
        )
        .await
        .map_err(|_| CompError::PlanningError {
            reason: format!("planner timed out after {}s", PLANNING_TIMEOUT_SECS),
        })?
        .map_err(|e| CompError::PlanningError {
            reason: format!("planner agent execution failed: {}", e),
        })?;

        let response_owned = raw.to_string();
        let response_str = raw.as_str().unwrap_or(&response_owned);
        let plan: crate::workflow::Plan =
            parse_json_with_retry(response_str).map_err(|e| CompError::PlanningError {
                reason: format!("failed to parse plan JSON: {}", e),
            })?;

        // Validate plan references
        let step_ids: std::collections::HashSet<&str> =
            workflow.steps.iter().map(|s| s.id.as_str()).collect();
        for ps in &plan.steps {
            if !step_ids.contains(ps.task_id.as_str()) {
                return Err(CompError::PlanningError {
                    reason: format!("plan references unknown task_id: {}", ps.task_id),
                });
            }
        }

        // Inject plan into workflow steps
        let mut new_workflow = workflow.clone();
        for step in &mut new_workflow.steps {
            if let Some(plan_step) = plan.steps.iter().find(|ps| ps.task_id == step.id) {
                let plan_context = format!(
                    "\n\n[Plan Context]\nOverall Strategy: {}\nYour role in this plan: {}\nExpected output: {}",
                    plan.overall_strategy, plan_step.reasoning, plan_step.expected_output
                );
                step.task = format!("{}{}", step.task, plan_context);

                // Sequential: override depends_on with planner's suggested dependencies
                if matches!(new_workflow.process, Process::Sequential)
                    && !plan_step.dependencies.is_empty()
                {
                    step.depends_on = plan_step.dependencies.clone();
                }
            }
        }

        Ok(new_workflow)
    }

    fn build_planner_prompt(&self, workflow: &Workflow) -> String {
        let mut tasks_desc = String::new();
        for step in &workflow.steps {
            tasks_desc.push_str(&format!(
                "- id: {}\n  agent: {}\n  task: {}\n",
                step.id, step.agent_id, step.task
            ));
            if let Some(ref expected) = step.expected_output {
                tasks_desc.push_str(&format!("  expected_output: {}\n", expected));
            }
        }

        format!(
            "You are a planning agent for workflow: {}\n\n\
             Tasks to plan:\n{}\n\n\
             Output a JSON plan with:\n\
             - overall_strategy: string\n\
             - steps: [\n\
                 {{\"task_id\": \"...\", \"agent_id\": \"...\", \"reasoning\": \"...\", \n\
                   \"expected_output\": \"...\", \"dependencies\": [...]}}\n\
               ]",
            workflow.description.as_deref().unwrap_or(&workflow.name),
            tasks_desc
        )
    }

    // ── Hierarchical Process ──

    fn build_manager_prompt(
        &self,
        workflow: &Workflow,
        manager_config: &ManagerConfig,
        completed: &[CompletedTask],
        pending_ids: &[String],
    ) -> String {
        // Agent descriptions
        let mut agent_desc = String::new();
        let seen: std::collections::HashSet<&str> =
            workflow.steps.iter().map(|s| s.agent_id.as_str()).collect();
        for agent_id in &seen {
            if let Some(agent) = self.hero.get_agent(agent_id) {
                let instr_summary: String = agent.instructions.chars().take(300).collect();
                let skills: Vec<String> = agent.skills.iter().map(|s| s.id.clone()).collect();
                agent_desc.push_str(&format!(
                    "- {}: {}\n  Skills: {}\n  Instructions summary: {}\n",
                    agent_id,
                    agent.description.as_deref().unwrap_or("no description"),
                    skills.join(", "),
                    instr_summary
                ));
            }
        }

        // Pending tasks
        let mut pending_desc = String::new();
        for step in &workflow.steps {
            if pending_ids.contains(&step.id) {
                pending_desc.push_str(&format!("- {}: {}", step.id, step.task));
                if let Some(ref expected) = step.expected_output {
                    pending_desc.push_str(&format!("\n  Expected: {}", expected));
                }
                pending_desc.push('\n');
            }
        }

        // Completed tasks
        let mut completed_desc = String::new();
        for ct in completed {
            let output_str = ct.output.to_string();
            let summary: String = output_str.chars().take(500).collect();
            completed_desc.push_str(&format!("{} → {}: {}\n", ct.task_id, ct.agent_id, summary));
        }

        let system_instructions = manager_config
            .instructions
            .as_deref()
            .unwrap_or("You are a project manager. Delegate tasks to agents.");

        format!(
            "{}\n\n## Output Format\n\
             You MUST respond with valid JSON only. No markdown, no explanation.\n\
             Schema: {{\"action\": \"delegate\", \"task_id\": \"<id>\", \"agent_id\": \"<id>\"}}\n\
                     or {{\"action\": \"done\"}}\n\n\
             ## Available Agents\n{}\n\
             ## Pending Tasks\n{}\n\
             ## Completed Tasks\n{}\n\
             Decide the next action. Output JSON only.",
            system_instructions, agent_desc, pending_desc, completed_desc
        )
    }

    async fn parse_manager_response(
        &self,
        manager_agent_id: &str,
        _workflow: &Workflow,
        _manager_config: &ManagerConfig,
        _completed: &[CompletedTask],
        _pending_ids: &[String],
        raw_response: &str,
    ) -> Result<ManagerDecision, CompError> {
        match parse_manager_json(raw_response) {
            Ok(decision) => Ok(decision),
            Err(first_err) => {
                // 一次重试：告知 Manager 格式错误
                let retry_prompt = format!(
                    "Your previous response was not valid JSON. Error: {}\n\
                     You MUST output ONLY valid JSON.\n\
                     {{\"action\": \"delegate\", \"task_id\": \"<id>\", \"agent_id\": \"<id>\"}}\n\
                     or {{\"action\": \"done\"}}",
                    first_err
                );
                let retry_raw = self
                    .hero
                    .execute(manager_agent_id, &retry_prompt, None)
                    .await
                    .map_err(|e| CompError::ManagerError {
                        reason: format!("manager retry failed: {}", e),
                    })?;
                let retry_owned = retry_raw.to_string();
                let retry_str = retry_raw.as_str().unwrap_or(&retry_owned);
                parse_manager_json(retry_str).map_err(|e| CompError::ManagerError {
                    reason: format!("failed to parse manager response after retry: {}", e),
                })
            }
        }
    }

    /// 核心：事件循环解释器
    async fn run_interpreter(
        &self,
        instance_id: String,
        workflow: Workflow,
        mut signal_rx: mpsc::Receiver<WorkflowEvent>,
        completion_tx: tokio::sync::oneshot::Sender<Result<WorkflowResult, CompError>>,
    ) -> Result<(), CompError> {
        let mut state = self.rebuild_state(&instance_id).await?;

        self.apply_and_persist(&instance_id, WorkflowEvent::InstanceStarted, &mut state)
            .await?;

        let (internal_tx, mut internal_rx) = mpsc::channel::<WorkflowEvent>(64);

        let executor =
            StepExecutor::new(self.hero.clone(), internal_tx.clone(), self.max_concurrency);

        let timer_registry = TimerRegistry::new(internal_tx.clone());

        let result: Result<WorkflowResult, CompError> = async {
            loop {
                let action = self.decide_next_action(&workflow, &state)?;

                match action {
                    Action::ScheduleSteps(step_ids) => {
                        for step_id in step_ids {
                            let step = workflow
                                .steps
                                .iter()
                                .find(|s| s.id == step_id)
                                .ok_or(CompError::StepNotFound { id: step_id.clone() })?;

                            let attempt = self.get_attempt(&state, &step_id);
                            let max_retries = step.retries.unwrap_or(0);
                            let will_retry = attempt <= max_retries;
                            let event = WorkflowEvent::StepScheduled {
                                step_id: step_id.clone(),
                                attempt,
                            };
                            self.apply_and_persist(&instance_id, event, &mut state)
                                .await?;

                            executor.submit(step.clone(), state.context.clone(), attempt, will_retry)
                                .await;
                        }
                    }
                    Action::WaitForEvent => {
                        tokio::select! {
                            Some(event) = internal_rx.recv() => {
                                self.apply_and_persist(&instance_id, event.clone(), &mut state).await?;

                                if let WorkflowEvent::StepCompleted { step_id, .. } = &event {
                                    if let Some(step) = workflow.steps.iter().find(|s| &s.id == step_id) {
                                        if let Some(ref signal_name) = step.wait_for_signal {
                                            let wait_event = WorkflowEvent::SignalWaitStarted {
                                                step_id: step_id.clone(),
                                                signal_name: signal_name.clone(),
                                            };
                                            self.apply_and_persist(&instance_id, wait_event, &mut state).await?;

                                            if let Some(timeout_secs) = step.signal_timeout {
                                                let timer_id = format!("signal_timeout_{}", step_id);
                                                let wake_at = Utc::now() + chrono::Duration::seconds(timeout_secs as i64);
                                                timer_registry.register(timer_id, wake_at).await;
                                            }
                                        }
                                    }
                                }

                                if let WorkflowEvent::StepFailed { step_id, will_retry: true, attempt, .. } = &event {
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
                                }

                                if let WorkflowEvent::StepFailed { step_id, will_retry: false, .. } = &event {
                                    let reason = state.step_results.get(step_id)
                                        .and_then(|r| r.error.clone())
                                        .unwrap_or_else(|| "step failed".to_string());
                                    break Err(CompError::StepFailed { step_id: step_id.clone(), reason });
                                }

                                if let WorkflowEvent::TimerFired { timer_id } = &event {
                                    if timer_id.starts_with("signal_timeout_") {
                                        let step_id = timer_id.strip_prefix("signal_timeout_").unwrap();
                                        let reason = format!("signal '{}' timeout", step_id);
                                        let fail_event = WorkflowEvent::WorkflowFailed {
                                            reason: reason.clone(),
                                            failed_at: Utc::now(),
                                        };
                                        self.apply_and_persist(&instance_id, fail_event, &mut state).await?;
                                        break Err(CompError::StepFailed {
                                            step_id: step_id.to_string(),
                                            reason,
                                        });
                                    }
                                }
                            }
                            Some(event) = signal_rx.recv() => {
                                self.apply_and_persist(&instance_id, event, &mut state).await?;
                            }
                            else => {
                                break Err(CompError::Internal("event channels closed".into()));
                            }
                        }
                    }
                    Action::Complete(outputs) => {
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
                    Action::Fail(reason) => {
                        let event = WorkflowEvent::WorkflowFailed {
                            reason: reason.clone(),
                            failed_at: Utc::now(),
                        };
                        self.apply_and_persist(&instance_id, event, &mut state).await?;
                        let step_id = state.step_results.iter()
                            .find(|(_, r)| matches!(r.status, StepStatus::Failed))
                            .map(|(id, _)| id.clone())
                            .unwrap_or_else(|| "unknown".to_string());
                        break Err(CompError::StepFailed { step_id, reason });
                    }
                }
            }
        }.await;

        let _ = completion_tx.send(result.clone());
        result.map(|_| ())
    }

    async fn rebuild_state(&self, instance_id: &str) -> Result<InstanceState, CompError> {
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

    async fn apply_and_persist(
        &self,
        instance_id: &str,
        event: WorkflowEvent,
        state: &mut InstanceState,
    ) -> Result<(), CompError> {
        self.store.append(instance_id, event.clone()).await?;
        state.apply(&event)?;
        Ok(())
    }

    fn decide_next_action(
        &self,
        workflow: &Workflow,
        state: &InstanceState,
    ) -> Result<Action, CompError> {
        match &state.status {
            InstanceStatus::Completed => {
                return Ok(Action::WaitForEvent);
            }
            InstanceStatus::Failed => {
                let reason = state
                    .step_results
                    .values()
                    .find(|r| matches!(r.status, StepStatus::Failed))
                    .and_then(|r| r.error.clone())
                    .unwrap_or_else(|| "workflow failed".to_string());
                return Ok(Action::Fail(reason));
            }
            InstanceStatus::WaitingForSignal { .. } | InstanceStatus::Sleeping { .. } => {
                return Ok(Action::WaitForEvent);
            }
            _ => {}
        }

        let mut in_degree: HashMap<String, usize> = HashMap::new();
        for step in &workflow.steps {
            in_degree.entry(step.id.clone()).or_insert(0);
            for _dep in &step.depends_on {
                *in_degree.entry(step.id.clone()).or_insert(0) += 1;
            }
        }

        for completed in &state.completed_steps {
            for step in &workflow.steps {
                if step.depends_on.contains(completed) {
                    if let Some(d) = in_degree.get_mut(&step.id) {
                        *d = d.saturating_sub(1);
                    }
                }
            }
        }

        let ready: Vec<String> = workflow
            .steps
            .iter()
            .filter(|s| {
                in_degree.get(&s.id).copied().unwrap_or(0) == 0
                    && !state.completed_steps.contains(&s.id)
                    && !state.running_steps.contains(&s.id)
                    && !state.signal_blocked_steps.contains(&s.id)
                    && !state.scheduled_steps.contains(&s.id)
            })
            .map(|s| s.id.clone())
            .collect();

        if !ready.is_empty() {
            return Ok(Action::ScheduleSteps(ready));
        }

        let all_done = workflow.steps.iter().all(|s| {
            state.completed_steps.contains(&s.id) && !state.signal_blocked_steps.contains(&s.id)
        });

        if all_done {
            return Ok(Action::Complete(
                self.build_workflow_outputs(workflow, state)?,
            ));
        }

        Ok(Action::WaitForEvent)
    }

    /// 从 Workflow 的 outputs 定义和当前 context 渲染最终输出。
    pub(crate) fn build_workflow_outputs(
        &self,
        workflow: &Workflow,
        state: &InstanceState,
    ) -> Result<Value, CompError> {
        let mut outputs = serde_json::Map::new();
        for output_def in &workflow.outputs {
            let value = render_template(&output_def.value, &state.context)?;
            outputs.insert(output_def.name.clone(), Value::String(value));
        }
        Ok(Value::Object(outputs))
    }

    fn get_attempt(&self, state: &InstanceState, step_id: &str) -> u64 {
        state
            .step_results
            .get(step_id)
            .map(|r| r.attempt + 1)
            .unwrap_or(1)
    }

    fn get_retry_delay(&self, workflow: &Workflow, step_id: &str) -> u64 {
        workflow
            .steps
            .iter()
            .find(|s| s.id == step_id)
            .and_then(|s| s.retry_delay)
            .unwrap_or(0)
    }

    /// Hierarchical 解释器：Manager Agent 动态委派 Task。
    async fn run_interpreter_hierarchical(
        &self,
        instance_id: String,
        workflow: Workflow,
        manager_config: ManagerConfig,
        mut signal_rx: mpsc::Receiver<WorkflowEvent>,
        completion_tx: tokio::sync::oneshot::Sender<Result<WorkflowResult, CompError>>,
    ) -> Result<(), CompError> {
        let mut state = self.rebuild_state(&instance_id).await?;
        self.apply_and_persist(&instance_id, WorkflowEvent::InstanceStarted, &mut state)
            .await?;

        let (internal_tx, mut internal_rx) = mpsc::channel::<WorkflowEvent>(64);
        let executor =
            StepExecutor::new(self.hero.clone(), internal_tx.clone(), self.max_concurrency);
        let timer_registry = TimerRegistry::new(internal_tx.clone());

        let mut completed_tasks: Vec<CompletedTask> = Vec::new();
        let mut manager_loops: usize = 0;

        let result: Result<WorkflowResult, CompError> = async {
            loop {
                manager_loops += 1;
                if manager_loops > MAX_MANAGER_LOOPS {
                    break Err(CompError::ManagerLoopExceeded {
                        max_loops: MAX_MANAGER_LOOPS,
                    });
                }

                let pending_ids: Vec<String> = workflow
                    .steps
                    .iter()
                    .map(|s| s.id.clone())
                    .filter(|id| !completed_tasks.iter().any(|ct| ct.task_id == *id))
                    .collect();

                if pending_ids.is_empty() {
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

                let prompt = self.build_manager_prompt(
                    &workflow,
                    &manager_config,
                    &completed_tasks,
                    &pending_ids,
                );

                let manager_result = self
                    .hero
                    .execute(&manager_config.agent_id, &prompt, None)
                    .await;

                match manager_result {
                    Ok(raw) => {
                        let tmp_owned = raw.to_string();
                        let response_str = raw.as_str().unwrap_or(&tmp_owned);
                        let decision = self
                            .parse_manager_response(
                                &manager_config.agent_id,
                                &workflow,
                                &manager_config,
                                &completed_tasks,
                                &pending_ids,
                                response_str,
                            )
                            .await?;

                        match decision {
                            ManagerDecision::Delegate { task_id, agent_id } => {
                                let step = workflow
                                    .steps
                                    .iter()
                                    .find(|s| s.id == task_id)
                                    .ok_or(CompError::ManagerError {
                                        reason: format!(
                                            "Manager returned unknown task_id: {}",
                                            task_id
                                        ),
                                    })?;

                                if self.hero.get_agent(&agent_id).is_none() {
                                    return Err(CompError::ManagerError {
                                        reason: format!(
                                            "Manager returned unknown agent_id: {}",
                                            agent_id
                                        ),
                                    });
                                }

                                let attempt = self.get_attempt(&state, &task_id);
                                let max_retries = step.retries.unwrap_or(0);
                                let will_retry = attempt <= max_retries;

                                let event = WorkflowEvent::StepScheduled {
                                    step_id: task_id.clone(),
                                    attempt,
                                };
                                self.apply_and_persist(&instance_id, event, &mut state)
                                    .await?;

                                executor
                                    .submit(
                                        step.clone(),
                                        state.context.clone(),
                                        attempt,
                                        will_retry,
                                    )
                                    .await;

                                // 等待步骤结果
                                let step_result: CompletedTask = loop {
                                    tokio::select! {
                                        Some(event) = internal_rx.recv() => {
                                            self.apply_and_persist(&instance_id, event.clone(), &mut state).await?;

                                            match &event {
                                                WorkflowEvent::StepCompleted { step_id, output, .. } => {
                                                    break CompletedTask {
                                                        task_id: step_id.clone(),
                                                        agent_id: step.agent_id.clone(),
                                                        output: output.clone(),
                                                        error: None,
                                                    };
                                                }
                                                WorkflowEvent::StepFailed { step_id, error, will_retry: false, .. } => {
                                                    break CompletedTask {
                                                        task_id: step_id.clone(),
                                                        agent_id: step.agent_id.clone(),
                                                        output: Value::Null,
                                                        error: Some(error.clone()),
                                                    };
                                                }
                                                WorkflowEvent::StepFailed { step_id, attempt, will_retry: true, .. } => {
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
                                                }
                                                _ => {}
                                            }
                                        }
                                        Some(event) = signal_rx.recv() => {
                                            self.apply_and_persist(&instance_id, event, &mut state).await?;
                                        }
                                        else => {
                                            break CompletedTask {
                                                task_id: task_id.clone(),
                                                agent_id: step.agent_id.clone(),
                                                output: Value::Null,
                                                error: Some("event channels closed".to_string()),
                                            };
                                        }
                                    }
                                };

                                completed_tasks.push(step_result);
                            }
                            ManagerDecision::Done => {
                                let outputs = self.build_workflow_outputs(&workflow, &state)?;
                                let event = WorkflowEvent::WorkflowCompleted {
                                    outputs: outputs.clone(),
                                    completed_at: Utc::now(),
                                };
                                self.apply_and_persist(&instance_id, event, &mut state)
                                    .await?;
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
        }
        .await;

        let _ = completion_tx.send(result.clone());
        result.map(|_| ())
    }
}

/// 校验外部输入参数，应用默认值，并构建初始 Context。
fn normalize_inputs(workflow: &Workflow, inputs: &Value) -> Result<Value, CompError> {
    let mut obj = match inputs {
        Value::Object(map) => map.clone(),
        other => {
            return Err(CompError::InvalidInputType {
                got: other.to_string(),
            });
        }
    };

    for input_def in &workflow.inputs {
        match obj.get(&input_def.name) {
            Some(_) => { /* provided */ }
            None => {
                if input_def.required {
                    return Err(CompError::MissingInput {
                        name: input_def.name.clone(),
                    });
                }
                if let Some(ref default) = input_def.default {
                    obj.insert(input_def.name.clone(), default.clone());
                }
            }
        }
    }

    Ok(Value::Object(obj))
}

#[cfg(test)]
mod tests;
