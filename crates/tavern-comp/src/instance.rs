use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::CompError;
use crate::event::WorkflowEvent;
use crate::workflow::StepStatus;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct InstanceState {
    pub id: String,
    pub workflow_id: String,
    pub status: InstanceStatus,

    /// 当前上下文（逐步由 StepCompleted 事件构建）
    pub context: Value,

    /// 步骤结果
    pub step_results: HashMap<String, crate::workflow::StepResult>,

    /// 已完成的步骤 ID（用于 DAG 入度计算）
    pub completed_steps: HashSet<String>,

    /// 当前正在运行的步骤
    pub running_steps: HashSet<String>,

    /// 已完成但信号未到的步骤（阻塞后续步骤调度）
    pub signal_blocked_steps: HashSet<String>,

    /// 已调度但尚未开始的步骤（防止事件异步到达前的重复调度）
    pub scheduled_steps: HashSet<String>,

    /// 活跃定时器（timer_id → wake_at）
    pub pending_timers: HashMap<String, DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum InstanceStatus {
    #[default]
    Pending,
    Running,
    WaitingForSignal {
        signal: String,
    },
    Sleeping {
        wake_at: DateTime<Utc>,
    },
    Completed,
    Failed,
}

impl InstanceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceStatus::Pending => "pending",
            InstanceStatus::Running => "running",
            InstanceStatus::WaitingForSignal { .. } => "waiting_for_signal",
            InstanceStatus::Sleeping { .. } => "sleeping",
            InstanceStatus::Completed => "completed",
            InstanceStatus::Failed => "failed",
        }
    }
}

impl InstanceState {
    /// 应用单个事件到状态。无副作用，可安全重放。
    pub fn apply(&mut self, event: &WorkflowEvent) -> Result<(), CompError> {
        match event {
            WorkflowEvent::InstanceCreated {
                workflow_id,
                inputs,
            } => {
                self.workflow_id = workflow_id.clone();
                self.context = inputs.clone();
                self.status = InstanceStatus::Pending;
            }
            WorkflowEvent::InstanceStarted => {
                self.status = InstanceStatus::Running;
            }
            WorkflowEvent::StepScheduled { step_id, .. } => {
                self.scheduled_steps.insert(step_id.clone());
            }
            WorkflowEvent::StepStarted {
                step_id,
                started_at,
            } => {
                if self.running_steps.contains(step_id) {
                    tracing::warn!(step_id = %step_id, "StepStarted for already-running step");
                }
                self.scheduled_steps.remove(step_id);
                self.running_steps.insert(step_id.clone());
                self.step_results
                    .entry(step_id.clone())
                    .and_modify(|r| r.started_at = Some(*started_at))
                    .or_insert(crate::workflow::StepResult {
                        status: StepStatus::Running,
                        output: None,
                        error: None,
                        started_at: Some(*started_at),
                        completed_at: None,
                        attempt: 0,
                    });
            }
            WorkflowEvent::StepCompleted {
                step_id,
                output,
                output_key,
                attempt,
                completed_at,
            } => {
                self.running_steps.remove(step_id);
                self.completed_steps.insert(step_id.clone());
                if let Some(ref key) = output_key {
                    if let Some(obj) = self.context.as_object_mut() {
                        obj.insert(key.clone(), output.clone());
                    }
                }
                self.step_results.insert(
                    step_id.clone(),
                    crate::workflow::StepResult {
                        status: StepStatus::Completed,
                        output: Some(output.clone()),
                        error: None,
                        started_at: self.step_results.get(step_id).and_then(|r| r.started_at),
                        completed_at: Some(*completed_at),
                        attempt: *attempt,
                    },
                );
            }
            WorkflowEvent::SignalWaitStarted {
                step_id,
                signal_name,
            } => {
                self.signal_blocked_steps.insert(step_id.clone());
                self.status = InstanceStatus::WaitingForSignal {
                    signal: signal_name.clone(),
                };
            }
            WorkflowEvent::StepFailed {
                step_id,
                error,
                attempt,
                will_retry,
                ..
            } => {
                self.running_steps.remove(step_id);
                if !will_retry {
                    self.status = InstanceStatus::Failed;
                }
                self.step_results.insert(
                    step_id.clone(),
                    crate::workflow::StepResult {
                        status: StepStatus::Failed,
                        output: None,
                        error: Some(error.clone()),
                        started_at: self.step_results.get(step_id).and_then(|r| r.started_at),
                        completed_at: Some(Utc::now()),
                        attempt: *attempt,
                    },
                );
            }
            WorkflowEvent::SignalReceived {
                signal_name,
                payload,
                ..
            } => {
                let expected = matches!(
                    self.status,
                    InstanceStatus::WaitingForSignal { ref signal } if signal == signal_name
                );
                if !expected {
                    tracing::warn!(
                        current = ?self.status,
                        signal = %signal_name,
                        "SignalReceived in unexpected state, ignored"
                    );
                    return Ok(());
                }
                // V2.0 一次只等待一个信号，直接清空阻塞集合
                self.signal_blocked_steps.clear();
                self.status = InstanceStatus::Running;
                if let Some(obj) = self.context.as_object_mut() {
                    let signals = obj
                        .entry("signals".to_string())
                        .or_insert_with(|| Value::Object(serde_json::Map::new()))
                        .as_object_mut()
                        .unwrap();
                    signals.insert(signal_name.clone(), payload.clone());
                }
            }
            WorkflowEvent::StepRetryScheduled { step_id, .. } => {
                self.scheduled_steps.insert(step_id.clone());
            }
            WorkflowEvent::TimerFired { timer_id } => {
                self.pending_timers.remove(timer_id);
                if timer_id.starts_with("retry_") {
                    // timer_id format: retry_{step_id}_{attempt}
                    if let Some(rest) = timer_id.strip_prefix("retry_") {
                        if let Some((step_id, _)) = rest.rsplit_once('_') {
                            self.scheduled_steps.remove(step_id);
                        }
                    }
                }
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
        }
        Ok(())
    }
}
