# Tavern Comp V2: Event-Sourced Workflow Orchestrator

## 1. 概述

Tavern Comp V2 将编排引擎从**同步内存调度器**演进为**事件溯源状态机（Event-Sourced State Machine）**。核心目标是在不引入外部消息队列（如 Kafka）的前提下，实现：

- **有状态的工作流实例**：实例可查询、可暂停、可从崩溃中恢复
- **异步执行**：HTTP API 不再阻塞等待工作流完成
- **长时间运行支持**：步骤可等待外部信号（人工审批、Webhook 回调）而不占用内存
- **审计与可观测性**：事件流即审计日志，支持精确重放

### 1.1 设计原则

- **事件即真相源（Event Sourcing）**：`InstanceState` 完全由 `WorkflowEvent` 流推导，不独立维护可变状态
- **零外部依赖（默认）**：事件存储默认内存实现，可选 SQLite/Redis，不强制 Kafka/RabbitMQ
- **向后兼容**：保留 V1 的 `WorkflowEngine::run()` 作为阻塞式兼容层
- **单进程事件循环**：事件在 `tokio::sync::mpsc` 通道内流转，调度器仍是单进程解释器
- **配置不变**：Workflow YAML 格式完全兼容 V1，无需迁移配置

### 1.2 与 V1 的核心差异

| 维度 | V1（当前） | V2（目标） |
|------|-----------|-----------|
| 执行模型 | 同步 `run()`，内存 `FuturesUnordered` | 异步 `start()`，事件循环驱动状态机 |
| 状态位置 | `run()` 内的堆变量 | `InstanceState`，由 EventStore 持久化 |
| 错误恢复 | 进程崩溃 = 工作流丢失 | 重放 Event Log = 精确恢复 |
| 外部信号 | 不支持 | `ExecutionHandle::signal()` + `wait_for_signal` YAML 字段 |
| 定时器 | `tokio::time::sleep`（占内存） | `TimerFired` 事件（零内存占用） |
| 审计日志 | 无 | Event Log 即完整审计 |
| YAML 兼容性 | - | V1 配置无需修改；V2 新增可选字段 |

---

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| Workflow | 工作流**定义**（静态配置），描述步骤、依赖、输入输出 |
| Instance / Execution | 工作流的**运行实例**（动态状态），每个 `start()` 产生一个 |
| Event | 状态机事件，如 `StepScheduled`、`StepCompleted`、`SignalReceived` |
| Event Log | 某实例按序排列的全部事件，是状态的真相源 |
| EventStore | 事件持久化抽象，负责追加和读取事件流 |
| State | `InstanceState`，由 Event Log 重放推导出的当前状态 |
| Action | 引擎基于当前状态决定的下一步操作，如 `ScheduleSteps`、`WaitForEvent` |
| Signal | 外部系统向运行中实例发送的异步输入（人工审批、Webhook） |
| Timer | 引擎安排的定时器，到期后产生 `TimerFired` 事件 |

---

## 3. 架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Tavern Server                             │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────────┐ │
│  │ POST /run   │  │GET /exec/:id │  │ POST /exec/:id/signal   │ │
│  │ (202 + id)  │  │ 查询状态      │  │ 发送外部信号             │ │
│  └──────┬──────┘  └──────────────┘  └─────────────────────────┘ │
│         │                                                        │
│  ┌──────▼────────────────────────────────────────────────────┐  │
│  │              WorkflowEngine (Interpreter)                  │  │
│  │                                                            │  │
│  │   EventStore ──→ read_stream() ──→ Event Log ──→ State   │  │
│  │        ↑                                    │              │  │
│  │   append()                                  ▼              │  │
│  │   (persist)                           decide_next_action() │  │
│  │        ↑                                    │              │  │
│  │        └──────────────┬─────────────────────┘              │  │
│  │                       │ Action                             │  │
│  │              ┌────────┴────────┐                           │  │
│  │              ▼                 ▼                           │  │
│  │   ┌─────────────────┐  ┌─────────────────┐                │  │
│  │   │  StepExecutor   │  │  TimerRegistry  │                │  │
│  │   │  (mpsc channel) │  │  (sleep + emit) │                │  │
│  │   └────────┬────────┘  └────────┬────────┘                │  │
│  │            │                    │                          │  │
│  │            ▼                    ▼                          │  │
│  │   StepCompleted ───── TimerFired ────→ mpsc rx            │  │
│  │                                                            │  │
│  └────────────────────────────────────────────────────────────┘  │
│                              │                                   │
│                              ▼ Arc<TavernHero>                   │
│                      ┌───────────────┐                           │
│                      │  Tavern Hero  │                           │
│                      └───────┬───────┘                           │
│                              ▼ Arc<dyn Runtime>                  │
│                      ┌───────────────┐                           │
│                      │   Adapters    │                           │
│                      └───────────────┘                           │
└─────────────────────────────────────────────────────────────────┘
```

### 3.1 Crate 职责（变化）

| Crate | V2 变化 |
|-------|---------|
| `tavern-comp` | 新增 `event.rs`, `instance.rs`, `store.rs`, `executor.rs`；`engine.rs` 重构为解释器 |
| `tavern-server` | 新增执行管理 API (`/executions`)，`/workflows/:id/run` 改为 202 异步 |

---

## 4. 数据模型

### 4.1 WorkflowEvent（事件）

真相源。所有状态变更必须通过事件表达。

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowEvent {
    // ── 生命周期 ──
    InstanceCreated {
        workflow_id: String,
        inputs: Value,
    },
    InstanceStarted,

    // ── 步骤推进 ──
    StepScheduled {
        step_id: String,
        attempt: u64,
    },
    StepStarted {
        step_id: String,
        started_at: DateTime<Utc>,
    },
    StepCompleted {
        step_id: String,
        output: Value,
        /// 步骤尝试次数（从 1 开始）
        attempt: u64,
        /// 输出写入 context 的键名（来自 Workflow 定义，事件自包含）
        output_key: Option<String>,
        completed_at: DateTime<Utc>,
    },
    StepFailed {
        step_id: String,
        error: String,
        /// 步骤尝试次数（从 1 开始）
        attempt: u64,
        /// 引擎是否计划重试
        will_retry: bool,
    },
    StepRetryScheduled {
        step_id: String,
        attempt: u64,
        /// 计划执行时间
        scheduled_at: DateTime<Utc>,
    },

    // ── 信号等待 ──
    SignalWaitStarted {
        step_id: String,
        signal_name: String,
    },
    SignalReceived {
        signal_name: String,
        payload: Value,
        received_at: DateTime<Utc>,
    },

    // ── 定时器 ──
    TimerFired {
        timer_id: String,
    },

    // ── 取消 ──
    CancelRequested {
        requested_at: DateTime<Utc>,
    },

    // ── 完成 ──
    WorkflowCompleted {
        outputs: Value,
        completed_at: DateTime<Utc>,
    },
    WorkflowFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
}
```

### 4.1a Step（V2 更新）

在 V1 `Step` 基础上新增两个可选字段，其余字段不变。完整定义见 V1 规格 `spec-comp.md` 第 4.2 节。

```rust
pub struct Step {
    // ── V1 字段（不变）──
    pub id: String,
    pub agent_id: String,
    pub task: String,
    pub depends_on: Vec<String>,
    pub output_key: Option<String>,
    pub timeout: Option<u64>,
    pub retries: Option<u64>,
    pub retry_delay: Option<u64>,
    
    // ── V2.0 新增（可选）──
    /// 等待的外部信号名称
    /// 若不为 null，步骤执行完成后引擎进入 WaitingForSignal 状态
    pub wait_for_signal: Option<String>,
    
    /// 信号等待超时（秒）
    /// 默认：null（无超时，永久等待）
    pub signal_timeout: Option<u64>,
}
```

### 4.2 InstanceState（实例状态）

由 Event Log 重放推导出的**纯派生状态**。不独立持久化，崩溃后通过 `EventStore::read_stream()` 重建。

```rust
use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct InstanceState {
    pub id: String,
    pub workflow_id: String,
    pub status: InstanceStatus,

    /// 当前上下文（逐步由 StepCompleted 事件构建）
    pub context: Value,

    /// 步骤结果
    pub step_results: HashMap<String, StepResult>,

    /// 已完成的步骤 ID（用于 DAG 入度计算）
    pub completed_steps: HashSet<String>,

    /// 当前正在运行的步骤
    pub running_steps: HashSet<String>,

    /// 已完成但信号未到的步骤（阻塞后续步骤调度）
    pub signal_blocked_steps: HashSet<String>,

    /// 活跃定时器（timer_id → wake_at）
    pub pending_timers: HashMap<String, DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InstanceStatus {
    Pending,
    Running,
    WaitingForSignal { signal: String },
    Sleeping { wake_at: DateTime<Utc> },
    Completed,
    Failed,
}

impl Default for InstanceStatus {
    fn default() -> Self { InstanceStatus::Pending }
}
```

**状态推导（纯函数）**：

```rust
impl InstanceState {
    /// 应用单个事件到状态。无副作用，可安全重放。
    pub fn apply(&mut self, event: &WorkflowEvent) -> Result<(), CompError> {
        match event {
            WorkflowEvent::InstanceCreated { workflow_id, inputs } => {
                self.workflow_id = workflow_id.clone();
                self.context = inputs.clone();
                self.status = InstanceStatus::Pending;
            }
            WorkflowEvent::InstanceStarted => {
                self.status = InstanceStatus::Running;
            }
            WorkflowEvent::StepStarted { step_id, started_at } => {
                if self.running_steps.contains(step_id) {
                    tracing::warn!(step_id = %step_id, "StepStarted for already-running step");
                }
                self.running_steps.insert(step_id.clone());
                // 初始化步骤结果的 started_at
                self.step_results.entry(step_id.clone())
                    .and_modify(|r| r.started_at = Some(*started_at))
                    .or_insert(StepResult {
                        status: StepStatus::Running,
                        output: None,
                        error: None,
                        started_at: Some(*started_at),
                        completed_at: None,
                        attempt: 0,
                    });
            }
            WorkflowEvent::StepCompleted { step_id, output, output_key, attempt, completed_at } => {
                self.running_steps.remove(step_id);
                self.completed_steps.insert(step_id.clone());
                // 若事件包含 output_key，写入 context
                if let Some(ref key) = output_key {
                    if let Some(obj) = self.context.as_object_mut() {
                        obj.insert(key.clone(), output.clone());
                    }
                }
                // 记录步骤结果
                self.step_results.insert(step_id.clone(), StepResult {
                    status: StepStatus::Completed,
                    output: Some(output.clone()),
                    error: None,
                    started_at: self.step_results.get(step_id)
                        .and_then(|r| r.started_at),
                    completed_at: Some(*completed_at),
                    attempt: *attempt,
                });
            }
            WorkflowEvent::SignalWaitStarted { step_id, signal_name } => {
                self.signal_blocked_steps.insert(step_id.clone());
                self.status = InstanceStatus::WaitingForSignal { 
                    signal: signal_name.clone() 
                };
            }
            WorkflowEvent::StepFailed { step_id, error, attempt, will_retry: false, .. } => {
                self.running_steps.remove(step_id);
                self.status = InstanceStatus::Failed;
                self.step_results.insert(step_id.clone(), StepResult {
                    status: StepStatus::Failed,
                    output: None,
                    error: Some(error.clone()),
                    started_at: self.step_results.get(step_id)
                        .and_then(|r| r.started_at),
                    completed_at: Some(Utc::now()),
                    attempt: *attempt,
                });
            }
            WorkflowEvent::StepFailed { step_id, error, attempt, will_retry: true, .. } => {
                self.running_steps.remove(step_id);
                // will_retry=true 时状态保持 Running，等待重试定时器
                self.step_results.insert(step_id.clone(), StepResult {
                    status: StepStatus::Failed,
                    output: None,
                    error: Some(error.clone()),
                    started_at: self.step_results.get(step_id)
                        .and_then(|r| r.started_at),
                    completed_at: Some(Utc::now()),
                    attempt: *attempt,
                });
            }
            WorkflowEvent::SignalReceived { signal_name, payload, .. } => {
                // 防御：只有真正在等待该信号时才处理
                let expected = matches!(self.status, InstanceStatus::WaitingForSignal { ref signal } if signal == signal_name);
                if !expected {
                    tracing::warn!(current = ?self.status, signal = %signal_name, "SignalReceived in unexpected state, ignored");
                    return Ok(());
                }
                self.signal_blocked_steps.retain(|s| s != signal_name);
                self.status = InstanceStatus::Running;
                // 信号 payload 写入 context.signals.{signal_name}
                if let Some(obj) = self.context.as_object_mut() {
                    let signals = obj.entry("signals".to_string())
                        .or_insert_with(|| Value::Object(serde_json::Map::new()))
                        .as_object_mut()
                        .unwrap();
                    signals.insert(signal_name.clone(), payload.clone());
                }
            }
            WorkflowEvent::TimerFired { timer_id } => {
                self.pending_timers.remove(timer_id);
                // 不直接修改 status，由引擎根据 timer_id 含义决定后续行为
            }
            WorkflowEvent::CancelRequested { .. } => {
                self.status = InstanceStatus::Failed;
            }
            WorkflowEvent::WorkflowCompleted { .. } => {
                self.status = InstanceStatus::Completed;
            }
            WorkflowEvent::WorkflowFailed { .. } => {
                self.status = InstanceStatus::Failed;
            }
            _ => {}
        }
        Ok(())
    }
}
```

### 4.3 StepResult（增强）

V1 的 `StepResult` 基础上增加重试计数。

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepResult {
    pub status: StepStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    /// 当前尝试次数（从 1 开始）
    pub attempt: u64,
}
```

### 4.3a WorkflowResult（V2 更新）

在 V1 基础上增加 `outputs` 字段。

```rust
pub struct WorkflowResult {
    /// 最终上下文（包含所有输入、步骤输出和信号）
    pub context: serde_json::Value,
    
    /// 工作流最终输出（由 `OutputDef` 模板渲染）
    pub outputs: serde_json::Value,
    
    /// 每个步骤的详细执行结果
    pub step_results: HashMap<String, StepResult>,
}
```

### 4.4 Action（引擎决策）

引擎基于 `InstanceState` 和 `Workflow` 定义决定的下一步操作。

```rust
#[derive(Debug)]
pub enum Action {
    /// 调度一组步骤执行
    ScheduleSteps(Vec<String>),
    /// 状态机无即刻动作，阻塞等待外部事件
    WaitForEvent,
    /// 工作流成功完成
    Complete(Value),
    /// 工作流失败（不可恢复）
    Fail(String),
}
```

---

## 5. 接口定义

### 5.1 EventStore

事件持久化抽象。唯一写操作是 `append`（追加），读操作是 `read_stream`（按序读取）。

```rust
use async_trait::async_trait;

#[async_trait]
pub trait EventStore: Send + Sync {
    /// 追加事件到指定实例的事件流。
    /// 实现必须保证：同一 instance_id 的事件按调用顺序存储。
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError>;

    /// 读取实例的完整事件流，按发生顺序返回。
    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError>;

    /// 列出指定状态的实例 ID（用于 ExecutionSupervisor 恢复）
    /// 默认实现：遍历所有实例重建状态后筛选（性能差，生产环境应覆盖）
    async fn list_by_status(&self, status: InstanceStatus) -> Result<Vec<String>, CompError> {
        Ok(vec![]) // 默认空实现，内存/SQLite 实现应覆盖
    }

    /// 保存状态快照（可选优化，避免每次重放全部事件）
    async fn save_snapshot(&self, instance_id: &str, state: &InstanceState) -> Result<(), CompError>;
    async fn load_snapshot(&self, instance_id: &str) -> Result<Option<InstanceState>, CompError>;
}
```

**默认实现：内存存储**

```rust
use std::collections::HashMap;
use tokio::sync::RwLock;

pub struct MemoryEventStore {
    streams: RwLock<HashMap<String, Vec<WorkflowEvent>>>,
    snapshots: RwLock<HashMap<String, InstanceState>>,
}

#[async_trait]
impl EventStore for MemoryEventStore {
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError> {
        let mut streams = self.streams.write().await;
        streams.entry(instance_id.to_string())
            .or_default()
            .push(event);
        Ok(())
    }

    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError> {
        let streams = self.streams.read().await;
        Ok(streams.get(instance_id).cloned().unwrap_or_default())
    }

    async fn list_by_status(&self, status: InstanceStatus) -> Result<Vec<String>, CompError> {
        let streams = self.streams.read().await;
        let mut result = Vec::new();
        for (id, events) in streams.iter() {
            let mut state = InstanceState {
                id: id.clone(),
                ..Default::default()
            };
            for event in events {
                let _ = state.apply(event);
            }
            if std::mem::discriminant(&state.status) == std::mem::discriminant(&status) {
                result.push(id.clone());
            }
        }
        Ok(result)
    }

    // snapshot 可选实现...
}
```

### 5.2 WorkflowEngine（V2）

引擎从"执行器"变为"解释器"。

```rust
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone)]
pub struct WorkflowEngine {
    hero: Arc<TavernHero>,
    store: Arc<dyn EventStore>,
    /// 实例级并发限制：每个工作流实例内同时运行的最大步骤数
    /// 
    /// **注意**：这是实例级限制，非全局限制。100 个实例 × max_concurrency=10
    /// 可能产生 1000 个并发步骤调用。若需全局限流，应在 Runtime 层或 Hero 层实现。
    max_concurrency: usize,
}

impl WorkflowEngine {
    pub fn new(hero: Arc<TavernHero>, store: Arc<dyn EventStore>) -> Self {
        Self { hero, store, max_concurrency: usize::MAX }
    }

    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    // ── V2 核心 API ──

    /// 启动工作流实例（非阻塞）。
    /// 返回 ExecutionHandle，可用于查询状态、发送信号和等待完成。
    pub async fn start(
        &self,
        workflow: &Workflow,
        inputs: Value,
    ) -> Result<ExecutionHandle, CompError> {
        let id = Uuid::new_v4().to_string();

        // 1. 持久化创建事件
        self.store.append(&id, WorkflowEvent::InstanceCreated {
            workflow_id: workflow.id.clone(),
            inputs: inputs.clone(),
        }).await?;

        // 2. 启动解释器循环（后台任务）
        let (signal_tx, signal_rx) = mpsc::channel::<WorkflowEvent>(64);
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let engine = self.clone();
        let workflow = workflow.clone();
        let id_clone = id.clone();

        let interpreter_handle = tokio::spawn(async move {
            let result = engine.run_interpreter(id_clone, workflow, signal_rx, completion_tx).await;
            if let Err(ref e) = result {
                tracing::error!(error = %e, "interpreter failed");
            }
            result
        });

        Ok(ExecutionHandle { id, signal_tx, interpreter_handle, completion_rx: Some(completion_rx) })
    }

    /// V1 兼容层：同步阻塞执行。
    /// 内部调用 start() + oneshot 等待完成。
    pub async fn run(
        &self,
        workflow: &Workflow,
        inputs: Value,
    ) -> Result<WorkflowResult, CompError> {
        let handle = self.start(workflow, inputs).await?;
        handle.await_completion().await
    }

    /// 核心：事件循环解释器
    async fn run_interpreter(
        &self,
        instance_id: String,
        workflow: Workflow,
        mut signal_rx: mpsc::Receiver<WorkflowEvent>,
        completion_tx: tokio::sync::oneshot::Sender<Result<WorkflowResult, CompError>>,
    ) -> Result<(), CompError> {
        // 重建状态
        let mut state = self.rebuild_state(&instance_id).await?;

        // emit InstanceStarted
        self.apply_and_persist(&instance_id, WorkflowEvent::InstanceStarted, &mut state).await?;

        // 内部事件通道（StepExecutor 和 TimerRegistry 向此处发事件）
        let (internal_tx, mut internal_rx) = mpsc::channel::<WorkflowEvent>(64);

        // 启动步骤执行器（只做单次执行，重试由引擎事件驱动）
        let executor = StepExecutor::new(
            self.hero.clone(),
            internal_tx.clone(),
            self.max_concurrency,
        );

        // 启动定时器注册器
        let timer_registry = TimerRegistry::new(internal_tx.clone());

        let result = loop {
            let action = self.decide_next_action(&workflow, &state)?;

            match action {
                Action::ScheduleSteps(step_ids) => {
                    for step_id in step_ids {
                        let step = workflow.steps.iter()
                            .find(|s| s.id == step_id)
                            .ok_or(CompError::StepNotFound { id: step_id.clone() })?;

                        let attempt = self.get_attempt(&state, &step_id);
                        let event = WorkflowEvent::StepScheduled {
                            step_id: step_id.clone(),
                            attempt,
                        };
                        self.apply_and_persist(&instance_id, event, &mut state).await?;

                        executor.submit(step.clone(), state.context.clone(), attempt).await;
                    }
                }
                Action::WaitForEvent => {
                    // 多路等待：内部事件（步骤完成、定时器）或外部信号
                    tokio::select! {
                        Some(event) = internal_rx.recv() => {
                            self.apply_and_persist(&instance_id, event.clone(), &mut state).await?;

                            // StepCompleted 后检查是否需要等待信号
                            if let WorkflowEvent::StepCompleted { step_id, .. } = &event {
                                if let Some(step) = workflow.steps.iter().find(|s| &s.id == step_id) {
                                    if let Some(ref signal_name) = step.wait_for_signal {
                                        let wait_event = WorkflowEvent::SignalWaitStarted {
                                            step_id: step_id.clone(),
                                            signal_name: signal_name.clone(),
                                        };
                                        self.apply_and_persist(&instance_id, wait_event, &mut state).await?;

                                        // 注册信号超时定时器
                                        if let Some(timeout_secs) = step.signal_timeout {
                                            let timer_id = format!("signal_timeout_{}", step_id);
                                            let wake_at = Utc::now() + chrono::Duration::seconds(timeout_secs as i64);
                                            timer_registry.register(timer_id, wake_at).await;
                                        }
                                    }
                                }
                            }

                            // StepFailed 后检查是否需重试
                            if let WorkflowEvent::StepFailed { step_id, attempt, .. } = &event {
                                let max_retries = workflow.steps.iter()
                                    .find(|s| &s.id == step_id)
                                    .and_then(|s| s.retries)
                                    .unwrap_or(0);
                                if *attempt <= max_retries {
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
                            }

                            // TimerFired 处理信号超时
                            if let WorkflowEvent::TimerFired { timer_id } = &event {
                                if timer_id.starts_with("signal_timeout_") {
                                    // 信号超时直接失败工作流（不消耗步骤重试次数）
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
                                // 重试定时器：TimerFired 后 pending_timers 移除，
                                // decide_next_action 会在下一循环调度就绪步骤
                            }
                        }
                        Some(event) = signal_rx.recv() => {
                            self.apply_and_persist(&instance_id, event, &mut state).await?;
                        }
                        else => {
                            // 所有通道关闭，实例终止
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
                    break Err(CompError::StepFailed { step_id: "unknown".to_string(), reason });
                }
            }
        };

        // 通知等待者（V1 兼容层或外部查询）
        let _ = completion_tx.send(result.clone());
        result.map(|_| ())
    }

    /// 重建状态：读取 Event Log 并重放
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

    /// 原子操作：先持久化事件，再更新内存状态
    /// 事件溯源原则：Event Store 是真相源，内存状态是投影
    async fn apply_and_persist(
        &self,
        instance_id: &str,
        event: WorkflowEvent,
        state: &mut InstanceState,
    ) -> Result<(), CompError> {
        self.store.append(instance_id, event.clone()).await?;  // ① 先持久化
        state.apply(&event)?;                                   // ② 再更新内存
        Ok(())
    }

    /// 基于当前状态决定下一步 Action
    fn decide_next_action(
        &self,
        workflow: &Workflow,
        state: &InstanceState,
    ) -> Result<Action, CompError> {
        // 如果已在终态，返回对应 Action
        match &state.status {
            InstanceStatus::Completed => {
                // 不应被调用，但安全处理
                return Ok(Action::WaitForEvent);
            }
            InstanceStatus::Failed => {
                let reason = state.step_results.values()
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

        // 构建 DAG 入度表
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        for step in &workflow.steps {
            in_degree.entry(step.id.clone()).or_insert(0);
            for dep in &step.depends_on {
                *in_degree.entry(step.id.clone()).or_insert(0) += 1;
            }
        }

        // 减去已完成步骤的贡献
        for completed in &state.completed_steps {
            for step in &workflow.steps {
                if step.depends_on.contains(completed) {
                    if let Some(d) = in_degree.get_mut(&step.id) {
                        *d = d.saturating_sub(1);
                    }
                }
            }
        }

        // 找出就绪步骤（入度为 0 且未执行、未运行、未被信号阻塞）
        let ready: Vec<String> = workflow.steps.iter()
            .filter(|s| {
                in_degree.get(&s.id).copied().unwrap_or(0) == 0
                    && !state.completed_steps.contains(&s.id)
                    && !state.running_steps.contains(&s.id)
                    && !state.signal_blocked_steps.contains(&s.id)
            })
            .map(|s| s.id.clone())
            .collect();

        if !ready.is_empty() {
            return Ok(Action::ScheduleSteps(ready));
        }

        // 无就绪步骤，检查是否全部完成（完成 = 步骤执行完 + 信号阻塞解除）
        let all_done = workflow.steps.iter()
            .all(|s| state.completed_steps.contains(&s.id)
                && !state.signal_blocked_steps.contains(&s.id));

        if all_done {
            // 渲染 outputs
            let mut outputs = serde_json::Map::new();
            for output_def in &workflow.outputs {
                let value = crate::context::render_template(&output_def.value, &state.context)?;
                outputs.insert(output_def.name.clone(), Value::String(value));
            }
            return Ok(Action::Complete(Value::Object(outputs)));
        }

        // 有步骤在运行或等待中
        Ok(Action::WaitForEvent)
    }

    fn get_attempt(&self, state: &InstanceState, step_id: &str) -> u64 {
        state.step_results.get(step_id)
            .map(|r| r.attempt + 1)
            .unwrap_or(1)
    }

    fn get_retry_delay(&self, workflow: &Workflow, step_id: &str) -> u64 {
        workflow.steps.iter()
            .find(|s| s.id == step_id)
            .and_then(|s| s.retry_delay)
            .unwrap_or(0)
    }
}
```

### 5.3 ExecutionHandle

外部系统与工作流实例交互的句柄。

```rust
use tokio::sync::mpsc;

pub struct ExecutionHandle {
    pub id: String,
    signal_tx: mpsc::Sender<WorkflowEvent>,
    interpreter_handle: tokio::task::JoinHandle<Result<(), CompError>>,
    completion_rx: Option<tokio::sync::oneshot::Receiver<Result<WorkflowResult, CompError>>>,
}

impl ExecutionHandle {
    pub fn id(&self) -> &str { &self.id }

    /// 向运行中的实例发送外部信号
    pub async fn signal(&self, name: &str, payload: Value) -> Result<(), CompError> {
        self.signal_tx.send(WorkflowEvent::SignalReceived {
            signal_name: name.to_string(),
            payload,
            received_at: Utc::now(),
        }).await.map_err(|_| CompError::InstanceClosed {
            id: self.id.clone(),
        })
    }

    /// 阻塞等待实例完成（V1 兼容层使用）
    /// 优先使用 oneshot 通道，避免轮询 EventStore
    /// **只能调用一次**，重复调用会返回错误。
    pub async fn await_completion(&mut self) -> Result<WorkflowResult, CompError> {
        if let Some(rx) = self.completion_rx.take() {
            rx.await.map_err(|_| CompError::Internal("completion channel closed".into()))?
        } else {
            Err(CompError::Internal("await_completion already called".into()))
        }
    }

    /// 查询实例当前状态（通过 EventStore 重放重建）
    pub async fn query_state(&self, store: &dyn EventStore) -> Result<InstanceState, CompError> {
        let events = store.read_stream(&self.id).await?;
        let mut state = InstanceState {
            id: self.id.clone(),
            ..Default::default()
        };
        for event in events {
            state.apply(&event)?;
        }
        Ok(state)
    }

    /// 优雅关闭解释器（发送 Cancel 信号）
    pub async fn cancel(&self) -> Result<(), CompError> {
        self.signal_tx.send(WorkflowEvent::CancelRequested {
            requested_at: Utc::now(),
        }).await.map_err(|_| CompError::InstanceClosed {
            id: self.id.clone(),
        })
    }
}
```

### 5.4 StepExecutor

负责实际调用 Hero **单次执行**步骤（含超时），将结果通过通道回传给解释器。

> **重试由引擎通过事件驱动管理**，`StepExecutor` 不做重试。这样每次重试都有完整的 `StepScheduled → StepStarted → StepCompleted/Failed` 事件序列，审计更完整。

```rust
use tokio::sync::mpsc;

pub struct StepExecutor {
    hero: Arc<TavernHero>,
    tx: mpsc::Sender<WorkflowEvent>,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl StepExecutor {
    pub fn new(
        hero: Arc<TavernHero>,
        tx: mpsc::Sender<WorkflowEvent>,
        max_concurrency: usize,
    ) -> Self {
        Self {
            hero,
            tx,
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrency)),
        }
    }

    pub async fn submit(&self, step: Step, context: Value, attempt: u64) {
        let hero = self.hero.clone();
        let tx = self.tx.clone();
        let output_key = step.output_key.clone();
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();

        tokio::spawn(async move {
            let _permit = permit; // 持有直到任务完成

            let started = WorkflowEvent::StepStarted {
                step_id: step.id.clone(),
                started_at: Utc::now(),
            };
            if let Err(e) = tx.send(started).await {
                tracing::error!(error = %e, "interpreter closed, step start event dropped");
                return;
            }

            let result = Self::execute_once(&step, &context, &hero).await;

            let event = match result {
                Ok(output) => WorkflowEvent::StepCompleted {
                    step_id: step.id.clone(),
                    output,
                    attempt,
                    output_key,
                    completed_at: Utc::now(),
                },
                Err(error) => WorkflowEvent::StepFailed {
                    step_id: step.id.clone(),
                    error,
                    attempt,
                    will_retry: false, // 引擎根据 retries 配置独立决策
                },
            };
            if let Err(e) = tx.send(event).await {
                tracing::error!(error = %e, "interpreter closed, step result dropped");
            }
        });
    }

    /// 单次执行（含超时），不做重试
    async fn execute_once(
        step: &Step,
        context: &Value,
        hero: &TavernHero,
    ) -> Result<Value, String> {
        let task = match crate::context::render_template(&step.task, context) {
            Ok(t) => t,
            Err(e) => return Err(format!("template render failed: {}", e)),
        };

        let timeout = step.timeout.unwrap_or(300);

        let fut = hero.execute(&step.agent_id, &task, Some(context.clone()));
        match tokio::time::timeout(Duration::from_secs(timeout), fut).await {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!("step timed out after {}s", timeout)),
        }
    }
}
```

### 5.5 TimerRegistry

轻量定时器注册器，每个定时器一个 `tokio::spawn` 任务。不依赖外部定时服务。

```rust
use tokio::sync::mpsc;

pub struct TimerRegistry {
    tx: mpsc::Sender<WorkflowEvent>,
}

impl TimerRegistry {
    pub fn new(tx: mpsc::Sender<WorkflowEvent>) -> Self {
        Self { tx }
    }

    /// 注册一个定时器，到期后发送 TimerFired 事件
    pub fn register(&self, timer_id: String, wake_at: DateTime<Utc>) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let now = Utc::now();
            if wake_at > now {
                if let Ok(duration) = (wake_at - now).to_std() {
                    tokio::time::sleep(duration).await;
                }
            }
            let event = WorkflowEvent::TimerFired { timer_id };
            if let Err(e) = tx.send(event).await {
                tracing::error!(error = %e, "interpreter closed, timer event dropped");
            }
        });
    }
}
```

---

## 6. Library API

### 6.1 Workflow（不变）

V1 的 `Workflow`、`Step`、`InputDef`、`OutputDef` 数据模型完全保留，YAML 配置无需迁移。

### 6.2 WorkflowRegistry（不变）

注册表逻辑不变，仍负责 Workflow 定义的加载和查询。

### 6.3 新增：ExecutionRegistry（可选）

若需要在内存中跟踪活跃实例句柄，可引入：

```rust
pub struct ExecutionRegistry {
    handles: RwLock<HashMap<String, ExecutionHandle>>,
}
```

> 注：这不是必需组件，因为实例状态可通过 `EventStore` 重建。仅当需要向特定实例发信号时才需要保留句柄。

---

## 7. REST API（tavern-server）

### 7.1 启动 Workflow（推荐新增端点）

```
POST /workflows/:id/start
```

异步启动工作流，立即返回 `execution_id`。

**响应 202 Accepted:**
```json
{
  "execution_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**响应 404:** Workflow 不存在
**响应 400:** 输入参数缺失

> **V1 兼容**：`POST /workflows/:id/run` 保持同步阻塞行为不变（内部调用 `start()` + `await_completion()`）。V2.0 不破坏此端点。


### 7.2 查询执行状态（新增）

```
GET /executions/:execution_id
```

**响应 200:**
```json
{
  "execution_id": "550e8400-e29b-41d4-a716-446655440000",
  "workflow_id": "content_pipeline",
  "status": "Running",
  "context": {
    "topic": "AI Agents",
    "research_notes": "...",
    "signals": {}
  },
  "outputs": {},
  "step_results": {
    "research": { "status": "Completed", "attempt": 1 },
    "write": { "status": "Running", "attempt": 1 }
  },
  "started_at": "2026-05-21T10:00:00Z"
}
```

**响应 404:** execution_id 不存在

### 7.3 发送信号（新增）

```
POST /executions/:execution_id/signal
```

**请求体:**
```json
{
  "signal_name": "approval",
  "payload": {
    "approved": true,
    "comments": "Go ahead"
  }
}
```

**响应 202:** 信号已接收
**响应 404:** execution_id 不存在
**响应 409:** 实例未处于等待信号状态，或信号名不匹配当前等待的信号

### 7.4 查询执行历史 / 审计日志（新增）

```
GET /executions/:execution_id/events
```

**响应 200:**
```json
[
  { "type": "instance_created", "workflow_id": "content_pipeline", ... },
  { "type": "instance_started" },
  { "type": "step_scheduled", "step_id": "research", "attempt": 1 },
  { "type": "step_started", "step_id": "research", ... },
  { "type": "step_completed", "step_id": "research", "output": "...", "attempt": 1, "output_key": "research_notes" },
  ...
]
```

### 7.5 取消执行（新增）

```
POST /executions/:execution_id/cancel
```

向运行中的实例发送取消请求。

**响应 202:** 取消请求已接受
**响应 404:** execution_id 不存在
**响应 409:** 实例已处于终态（Completed / Failed）

---

## 8. 错误定义

### 8.1 新增 CompError 变体

```rust
#[derive(Debug, thiserror::Error)]
pub enum CompError {
    // ── V1 保留（全部保留） ──
    #[error("workflow '{id}' not found")]
    WorkflowNotFound { id: String },
    // ... (其余 V1 变体不变)

    // ── V2 新增 ──
    #[error("execution instance '{id}' not found")]
    InstanceNotFound { id: String },

    #[error("execution instance '{id}' is closed")]
    InstanceClosed { id: String },

    #[error("instance '{id}' is not waiting for signal '{signal}'")]
    SignalRejected { id: String, signal: String },

    #[error("event store error: {0}")]
    StoreError(String),

    #[error("internal error: {0}")]
    Internal(String),
}
```

### 8.2 HTTP 状态码映射（新增）

| CompError | HTTP Status |
|-----------|-------------|
| InstanceNotFound | 404 Not Found |
| InstanceClosed | 409 Conflict |
| SignalRejected | 409 Conflict |
| StoreError | 503 Service Unavailable |
| Internal | 500 Internal Server Error |

---

## 9. YAML 配置格式

### 9.1 V2.0 新增字段

在 V1 基础上新增以下可选字段，**不破坏现有配置**：

| 字段 | 类型 | 必需 | 默认 | 约束 |
|------|------|------|------|------|
| `steps[].wait_for_signal` | string | 否 | null | 非空，声明此步骤需要等待的外部信号名称 |
| `steps[].signal_timeout` | integer | 否 | null | ≥ 1，单位：秒。信号未在超时时间内到达则步骤失败 |

### 9.2 Human-in-the-loop 示例

```yaml
id: content_pipeline
name: 内容生产流水线

steps:
  - id: research
    agent_id: researcher
    task: "研究以下主题并整理关键信息: {{topic}}"
    output_key: research_notes

  - id: write
    agent_id: writer
    task: "根据以下研究资料撰写文章: {{research_notes}}"
    depends_on: [research]
    output_key: draft

  - id: review
    agent_id: reviewer
    task: "请审核以下文章并提供修改意见: {{draft}}"
    depends_on: [write]
    output_key: approved_draft
    wait_for_signal: editorial_approval      # 等待人工审批信号
    signal_timeout: 86400                    # 24 小时超时

  - id: publish
    agent_id: publisher
    task: "发布已审批的文章: {{approved_draft}}"
    depends_on: [review]

inputs:
  - name: topic
    required: true
```

### 9.3 字段约束（完整）

| 字段 | 类型 | 必需 | 默认 | 约束 |
|------|------|------|------|------|
| id | string | 是 | - | `^[a-zA-Z0-9_-]+$`, 1-64 字符 |
| name | string | 是 | - | 非空 |
| description | string | 否 | null | - |
| steps | array | 是 | - | 至少 1 个元素 |
| steps[].id | string | 是 | - | workflow 内唯一 |
| steps[].agent_id | string | 是 | - | 引用 Hero 注册表中的 Agent |
| steps[].task | string | 是 | - | 支持 `{{var}}` 模板插值 |
| steps[].depends_on | array | 否 | [] | 元素为已定义的 step id |
| steps[].output_key | string | 否 | null | 非空，workflow 内唯一 |
| steps[].timeout | integer | 否 | 300 | ≥ 1，单位：秒 |
| steps[].retries | integer | 否 | 0 | ≥ 0 |
| steps[].retry_delay | integer | 否 | 0 | ≥ 0，单位：秒 |
| steps[].wait_for_signal | string | 否 | null | 非空 |
| steps[].signal_timeout | integer | 否 | null | ≥ 1，单位：秒 |
| inputs | array | 否 | [] | - |
| inputs[].name | string | 是 | - | 非空 |
| inputs[].required | bool | 否 | true | - |
| inputs[].default | any | 否 | null | 任意 JSON 值 |
| outputs | array | 否 | [] | - |
| outputs[].name | string | 是 | - | 非空 |
| outputs[].value | string | 是 | - | 支持 `{{var}}` 模板插值 |

### 9.4 未来扩展

V2.x 可能引入通用条件表达式字段 `steps[].when`（如 `"signal('approval') && context.score > 0.8"`），届时 `wait_for_signal` 可作为 `when` 的语法糖保留。

---

## 10. 状态机行为

### 10.1 状态转换图

```
                         InstanceCreated
                               │
                               ▼
                            Pending
                               │
                         InstanceStarted
                               │
                               ▼
          ┌───────────────── Running ──────────────────┐
          │                      │                      │
          ▼                      ▼                      ▼
   StepScheduled      SignalWaitStarted         TimerFired
          │                      │                      │
          ▼                      ▼                      ▼
   StepStarted    WaitingForSignal ──SignalReceived──→ Running
          │                      │                      │
          ▼                      │                      │
   StepCompleted                │                      │
          │                      │                      │
          └──────────────────────┘                      │
                                                        │
    ┌───────────────────────────────────────────────────┘
    │
    ▼
┌───────────────┐    ┌────────────────┐   ┌────────────────┐
│WorkflowCompleted│   │ WorkflowFailed │   │ WorkflowFailed │
│   (正常完成)    │   │  (步骤失败/超时) │   │ (信号超时/其他) │
└───────────────┘    └────────────────┘   └────────────────┘
```

**状态转换说明**：
- `Running` → `WaitingForSignal`：`StepCompleted` 后若步骤声明 `wait_for_signal`，引擎生成 `SignalWaitStarted` 事件
- `WaitingForSignal` → `Running`：`SignalReceived` 事件到达，信号 payload 写入 context
- `WaitingForSignal` → `Failed`：`signal_timeout` 到期（`TimerFired`），引擎生成 `StepFailed` → `WorkflowFailed`

### 10.2 事件应用规则

| 事件 | 状态要求 | 状态变更 | Context 变更 |
|------|---------|---------|-------------|
| `InstanceCreated` | 无 | `Pending` | 初始化为 `inputs` |
| `InstanceStarted` | `Pending` | `Running` | 无 |
| `StepScheduled` | `Running` | 无 | 无 |
| `StepStarted` | `Running` | `running_steps` +1 | 无 |
| `StepCompleted` | `Running` | `running_steps` -1, `completed_steps` +1 | 若 `output_key` 存在，写入 context |
| `SignalWaitStarted` | `Running` | `WaitingForSignal`, `signal_blocked_steps` +1 | 无 |
| `StepFailed` (will_retry=true) | `Running` | `running_steps` -1 | 记录 StepResult |
| `StepFailed` (will_retry=false) | `Running` | `Failed` | 记录 StepResult |
| `StepRetryScheduled` | `Running` | 无 | 无 |
| `SignalReceived` | `WaitingForSignal` | `Running`, `signal_blocked_steps` -1 | 信号 payload 写入 `context.signals.{name}` |
| `TimerFired` | `Sleeping` / `WaitingForSignal` | 移除定时器 | 状态由引擎根据 timer_id 决定 |
| `CancelRequested` | 任意 | `Failed` | 无 |
| `WorkflowCompleted` | `Running` | `Completed` | 无 |
| `WorkflowFailed` | `Running` | `Failed` | 无 |

### 10.3 重试策略

重试由引擎事件驱动管理：
1. `StepFailed` 事件包含当前 `attempt` 次数
2. 引擎检查 `attempt <= step.retries`：
   - 是：生成 `StepRetryScheduled` + 注册定时器，`TimerFired` 后重新 `ScheduleSteps`
   - 否：生成最终 `StepFailed`（`will_retry: false`），工作流失败
3. 每次重试都有完整的 `StepScheduled → StepStarted → StepCompleted/Failed` 事件序列

重试间隔由 `step.retry_delay`（秒）决定，通过 `TimerRegistry` 实现零内存等待。

### 10.4 信号等待策略

步骤声明 `wait_for_signal` 时的执行流程：

1. `StepScheduled` → `StepStarted` → Agent 执行（Agent 返回提示信息，如"等待人工审批"）
2. `StepCompleted`：Agent 执行完成，输出写入 context
3. 引擎检查步骤是否声明 `wait_for_signal`：
   - 是：生成 `SignalWaitStarted` 事件，状态变为 `WaitingForSignal { signal: "xxx" }`，`signal_blocked_steps` 记录该步骤，注册信号超时定时器（若 `signal_timeout` 存在）
   - 否：正常推进 DAG
4. `SignalReceived` 到达：验证信号名匹配，应用 payload 到 `context.signals.{name}`，状态恢复 `Running`
5. `signal_timeout` 到期（`TimerFired`）：引擎生成 `StepFailed`（reason: "signal timeout"），按 `retries` 配置决定是否重试

> **注意**：`SignalWaitStarted` 是持久化事件，崩溃恢复时重放此事件可精确重建 `WaitingForSignal` 状态，无需依赖 Workflow 静态配置。

**信号匹配规则**：
- `POST /executions/:id/signal` 中的 `signal_name` 必须与步骤的 `wait_for_signal` 精确匹配
- 若实例未处于 `WaitingForSignal` 状态，或信号名不匹配 → 返回 `SignalRejected`
- 并发信号：第二个及以后的信号会被缓冲在 channel 中，但解释器处理第一个信号后状态已恢复 `Running`，后续信号到达时 `apply` 中无操作（静默丢弃）

---

## 11. 执行引擎行为

### 11.1 事件循环（Interpreter Loop）

```
重建状态 ──→ 决定 Action ──→ 执行 Action ──→ 等待事件 ──→ 应用事件 ──→ 循环
                │                              ↑
                └──── ScheduleSteps ───────────┘
                     (StepExecutor 完成后发事件)
```

### 11.2 并发模型

- **解释器**：单任务循环（每个实例一个 `tokio::task`）
- **StepExecutor**：通过 `tokio::sync::Semaphore` 限制并发数
- **TimerRegistry**：每个定时器一个 `tokio::spawn` + `tokio::time::sleep`

### 11.3 崩溃恢复策略

V2.0 采用**保守策略**，明确分阶段演进：

#### V2.0 ~ V2.2：保守策略（进程重启 = Running 实例标记为 Failed）

```
进程崩溃前：InstanceState = Running, Step "research" 正在执行
进程重启后：
  1. 重放 Event Log 到 StepStarted("research")
  2. 无 StepCompleted/StepFailed 事件
  3. ExecutionSupervisor 追加 WorkflowFailed { reason: "process restarted" }
  4. 实例终态：Failed
```

**原因**：
- 避免无幂等性保证的重复执行（Agent 调用可能产生副作用：发送邮件、扣费、创建订单）
- V2.0 先验证事件溯源架构的正确性，不引入分布式系统复杂度

#### Phase 4：检查点恢复（Checkpoint Recovery）

```rust
#[async_trait]
pub trait StepExecutor {
    /// 正常提交步骤执行
    async fn submit(&self, step: Step, context: Value);
    
    /// 恢复时查询：此步骤是否已有结果（幂等性检查）
    /// 默认实现返回 None，表示不支持幂等查询
    async fn query_result(&self, step_id: &str, attempt: u64) -> Option<StepResult> {
        None
    }
}
```

恢复逻辑：
1. 重放 Event Log 到 `StepStarted` 事件
2. 调用 `executor.query_result(step_id, attempt)`
3. 若有结果 → 直接 emit `StepCompleted`/`StepFailed`，无需重新执行
4. 若无结果 → 重新 `submit()`

**前提条件**：
- Runtime 或 Hero 层提供"查询任务状态"接口，或引擎内部维护"已提交未确认"集合
- 使用共享 EventStore（SQLite/Redis），内存存储不支持跨进程恢复

---

## 12. 与 Hero 的集成

集成方式**不变**。`StepExecutor` 仍通过 `TavernHero::execute()` 提交任务。

```
StepExecutor::execute_with_retry()
    → render_template()          # Comp 内部
    → hero.execute(agent_id, task, context)  # 提交任务
    → 结果通过 mpsc channel 回传 Interpreter
```

---

## 13. 目录结构

```
tavern/
├── crates/
│   └── tavern-comp/
│       └── src/
│           ├── lib.rs              # 模块导出
│           ├── workflow.rs         # Workflow/Step 数据模型（V2 新增可选字段）
│           ├── registry.rs         # WorkflowRegistry（不变）
│           ├── engine.rs           # WorkflowEngine（重构为解释器）
│           ├── engine/
│           │   └── tests.rs        # 引擎测试
│           ├── context.rs          # 模板渲染（不变）
│           ├── validator.rs        # DAG 验证（不变）
│           ├── error.rs            # CompError（新增 V2 变体）
│           ├── event.rs            # [新增] WorkflowEvent 定义
│           ├── instance.rs         # [新增] InstanceState + 状态机
│           ├── store.rs            # [新增] EventStore trait + MemoryEventStore
│           ├── executor.rs         # [新增] StepExecutor
│           └── timer.rs            # [新增] TimerRegistry
└── docs/
    └── specs/
        ├── spec-comp.md            # V1 规格（归档）
        └── spec-comp-v2-event-driven.md  # 本文件
```

---

## 14. 环境变量

| 变量 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `WORKFLOW_CONFIG_DIR` | 否 | `./configs/workflows` | Workflow YAML 配置目录 |
| `EVENT_STORE_TYPE` | 否 | `memory` | 事件存储类型：`memory` / `sqlite` |
| `EVENT_STORE_PATH` | 否 | `./tavern-events.db` | SQLite 存储路径（仅 sqlite 时有效） |
| `EVENT_RETENTION_DAYS` | 否 | `30` | 已完成实例的事件保留天数（仅 sqlite 时有效，V2.x 实现） |
| `GLOBAL_MAX_CONCURRENCY` | 否 | `usize::MAX` | 全局最大并发步骤数（所有实例合计，V2.x 实现） |

---

## 15. 依赖矩阵

| Crate | core | adapters | hero | comp | server |
|-------|:----:|:--------:|:----:|:----:|:------:|
| `serde` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `serde_json` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `serde_yaml` | - | - | ✓ | ✓ | - |
| `thiserror` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `tokio` | - | - | ✓ | ✓ | ✓ |
| `chrono` | - | - | - | ✓ | - |
| `uuid` | - | - | - | ✓ | - |
| `async-trait` | ✓ | ✓ | ✓ | ✓ | - |
| `tracing` | - | - | - | ✓ | ✓ |
| `tavern-core` | - | ✓ | ✓ | ✓ | ✓ |
| `tavern-hero` | - | - | - | ✓ | ✓ |
| `tavern-comp` | - | - | - | - | ✓ |

---

## 16. Event Schema 演进策略

事件溯源的核心约束是**事件日志不可变**。因此 `WorkflowEvent` 的演进遵循以下原则：

### 16.1 基本原则：只扩展，不修改

- **永不修改已有变体的字段**（如给 `StepCompleted` 加字段）
- **新需求通过添加新变体实现**（如 `StepCompletedV2`）
- **状态机同时处理新旧变体**，语义相同的变体走同一分支

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowEvent {
    // V2.0 事件（冻结，永不修改字段）
    InstanceCreated { workflow_id: String, inputs: Value },
    StepCompleted { step_id: String, output: Value, completed_at: DateTime<Utc> },
    
    // V2.1 新增：带 Metrics 的步骤完成事件
    StepCompletedV2 {
        step_id: String,
        output: Value,
        completed_at: DateTime<Utc>,
        metrics: StepMetrics,  // 新增字段
    },
    
    // V2.2 新增：Saga 补偿事件
    StepCompensated {
        step_id: String,
        compensation_output: Value,
    },
    
    // ... 未来更多
}
```

### 16.2 状态机兼容性处理

```rust
impl InstanceState {
    pub fn apply(&mut self, event: &WorkflowEvent) -> Result<(), CompError> {
        match event {
            // 旧变体
            WorkflowEvent::StepCompleted { step_id, output, .. } => {
                self.complete_step(step_id, output.clone());
            }
            // 新变体：语义相同，只是多了 metrics
            WorkflowEvent::StepCompletedV2 { step_id, output, .. } => {
                self.complete_step(step_id, output.clone());
                // metrics 不进入核心状态，可单独存储到 metrics store
            }
            _ => {}
        }
        Ok(())
    }
}
```

### 16.3 写策略

**引擎总是写入最新版本的事件**：

```rust
// StepExecutor 总是 emit 最新版本
tx.send(WorkflowEvent::StepCompletedV2 { ... }).await?;
```

### 16.4 为什么不使用迁移脚本

事件溯源的"真相源"属性要求历史不可变。若确实需要清理旧格式：
1. 保留原始 Event Log（归档）
2. 创建新的 Event Log 副本（转换后）
3. 引擎读取新副本，旧副本保留用于审计追溯

---

## 17. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 0.1.0 | 2026-05-20 | 初始版本：串行执行、DAG 验证、模板渲染 |
| 0.2.0 | 2026-05-21 | 并行 wave-front 调度、重试、超时、minijinja、Prometheus 指标、管理 API |
| **0.3.0** | - | **事件溯源状态机：异步执行、EventStore、信号、定时器、审计日志** |

---

## 18. 迁移计划

### Phase 1：引擎重构（零 API 破坏）
1. 新增 `event.rs`, `instance.rs`, `store.rs`, `executor.rs`, `timer.rs`
2. 重构 `WorkflowEngine`：保留 `run()` 作为 `start() + await_completion()` 的语法糖
3. 所有现有测试应继续通过（`run()` 行为不变）

### Phase 2：Server API 演进（零破坏）
1. 保留 `POST /workflows/:id/run` 不变（同步阻塞，V1 兼容）
2. 新增 `POST /workflows/:id/start`（异步启动，202 + execution_id）
3. 新增 `GET /executions/:id`（查询状态）
4. 新增 `POST /executions/:id/signal`（发送信号）
5. 新增 `POST /executions/:id/cancel`（取消执行）

### Phase 3：持久化存储
1. 实现 `SqliteEventStore`
2. 通过环境变量切换存储后端
3. 引入状态快照机制优化重放性能

### Phase 4：检查点恢复与跨进程恢复

**目标**：进程重启后，运行中的实例能够自动恢复，不丢失已执行步骤的进度。

**前提**：使用共享 EventStore（SQLite 或 Redis），内存存储不支持跨进程恢复。

### Phase 5：Event Log 归档与清理（V2.x）

**目标**：防止 Event Store 无限增长导致磁盘耗尽。

1. 定时任务扫描终态实例（Completed/Failed）
2. 超过 `EVENT_RETENTION_DAYS` 的实例：
   - 可选：将 Event Log 压缩归档到对象存储
   - 删除原始事件，保留实例元数据（id、workflow_id、status、完成时间）
3. 归档后实例仍可查询状态（通过最终快照），但不可重放完整历史

#### 4.1 ExecutionSupervisor

```rust
pub struct ExecutionSupervisor {
    store: Arc<dyn EventStore>,
    engine: WorkflowEngine,
    executor: Arc<dyn StepExecutor>,
}

impl ExecutionSupervisor {
    /// 启动时调用：扫描所有 Running 实例并恢复
    pub async fn recover(&self) -> Result<(), CompError> {
        let running_ids = self.store.list_by_status(InstanceStatus::Running).await?;
        for id in running_ids {
            // 重放事件重建状态
            let state = self.engine.rebuild_state(&id).await?;
            
            // 检查当前是否有运行中的步骤
            for step_id in &state.running_steps {
                // 查询步骤是否已有结果（幂等性检查）
                let attempt = state.step_results.get(step_id)
                    .map(|r| r.attempt).unwrap_or(1);
                if let Some(result) = self.executor.query_result(step_id, attempt).await {
                    // 步骤在崩溃前已完成，只是事件未写入
                    let event = result.to_event();
                    self.store.append(&id, event).await?;
                } else {
                    // 步骤未执行完，重新提交（需要 Workflow 定义）
                    // 实际实现需从 WorkflowRegistry 查询 Workflow
                }
            }
            
            // 重新启动解释器循环
            // self.engine.resume_instance(id, state).await?;
        }
        Ok(())
    }
}
```

#### 4.2 StepExecutor 幂等接口

```rust
#[async_trait]
pub trait StepExecutor {
    async fn submit(&self, step: Step, context: Value);
    
    /// 恢复时查询步骤执行结果
    /// 返回 Some = 步骤已完成，无需重新执行
    /// 返回 None = 步骤未执行或执行中，需要重新 submit
    async fn query_result(&self, step_id: &str, attempt: u64) -> Option<StepResult> {
        None // 默认不支持
    }
}
```

#### 4.3 恢复语义

| 崩溃前状态 | 恢复行为 |
|-----------|---------|
| `StepScheduled` 未开始 | 重新调度 |
| `StepStarted` 执行中 | `query_result()` → 有结果则补发事件，无结果则重新提交 |
| `WaitingForSignal` | 保留等待状态，解释器重新绑定，继续等待 |
| `Sleeping` | 重新计算剩余时间，注册新定时器 |

#### 4.4 风险缓解

- **重复执行风险**：`query_result()` 是幂等性保障的关键。若 Runtime 不支持查询，默认行为是保守策略（标记为 Failed）
- **信号丢失**：恢复后 `WaitingForSignal` 状态的实例需等待重新发送信号，或依赖外部系统的信号持久化
- **定时器漂移**：恢复后重新计算 `wake_at - now`，而非依赖旧定时器
