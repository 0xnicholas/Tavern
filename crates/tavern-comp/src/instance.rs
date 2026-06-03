use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::CompError;
use crate::event::{SignalAction, WorkflowEvent};
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
                if let Some(key) = output_key
                    && let Some(obj) = self.context.as_object_mut()
                {
                    obj.insert(key.clone(), output.clone());
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
            WorkflowEvent::BreakpointHit { step_id, .. } => {
                self.scheduled_steps.remove(step_id);
                self.signal_blocked_steps.insert(step_id.clone());
                self.status = InstanceStatus::WaitingForSignal {
                    signal: format!("__breakpoint__{}", step_id),
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
                action,
                reviewer,
                ..
            } => {
                // V0.3.2: 审批驳回 → 终止工作流
                if matches!(action, Some(SignalAction::Reject)) {
                    let reason = reviewer
                        .as_deref()
                        .map(|r| {
                            format!(
                                "rejected by {}: {}",
                                r,
                                payload
                                    .get("reason")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("no reason")
                            )
                        })
                        .unwrap_or_else(|| {
                            format!(
                                "rejected: {}",
                                payload
                                    .get("reason")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("no reason")
                            )
                        });
                    let blocked_step_ids: Vec<String> =
                        self.signal_blocked_steps.iter().cloned().collect();
                    self.signal_blocked_steps.clear();
                    self.status = InstanceStatus::Failed;
                    for step_id in blocked_step_ids {
                        if let Some(result) = self.step_results.get_mut(&step_id) {
                            result.status = StepStatus::Failed;
                            result.error = Some(reason.clone());
                        }
                    }
                    return Ok(());
                }

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
                    if let Some(rest) = timer_id.strip_prefix("retry_")
                        && let Some((step_id, _)) = rest.rsplit_once('_')
                    {
                        self.scheduled_steps.remove(step_id);
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
            WorkflowEvent::LLMCallStarted { .. }
            | WorkflowEvent::LLMCallCompleted { .. }
            | WorkflowEvent::LLMCallFailed { .. }
            | WorkflowEvent::ToolCallStarted { .. }
            | WorkflowEvent::ToolCallCompleted { .. }
            | WorkflowEvent::ToolCallFailed { .. }
            | WorkflowEvent::External { .. } => {
                // Audit-only events: stored for observability but don't affect workflow state
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn blank_state() -> InstanceState {
        InstanceState {
            id: "i1".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_apply_instance_created() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::InstanceCreated {
                workflow_id: "wf1".to_string(),
                inputs: json!({"x": 1}),
            })
            .unwrap();

        assert_eq!(state.workflow_id, "wf1");
        assert_eq!(state.context, json!({"x": 1}));
        assert!(matches!(state.status, InstanceStatus::Pending));
    }

    #[test]
    fn test_apply_instance_started() {
        let mut state = blank_state();
        state.apply(&WorkflowEvent::InstanceStarted).unwrap();
        assert!(matches!(state.status, InstanceStatus::Running));
    }

    #[test]
    fn test_apply_step_completed_updates_context_and_results() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::InstanceCreated {
                workflow_id: "wf".to_string(),
                inputs: json!({}),
            })
            .unwrap();
        state
            .apply(&WorkflowEvent::StepStarted {
                step_id: "s1".to_string(),
                started_at: Utc::now(),
            })
            .unwrap();

        state
            .apply(&WorkflowEvent::StepCompleted {
                step_id: "s1".to_string(),
                output: json!("out"),
                attempt: 1,
                output_key: Some("res".to_string()),
                completed_at: Utc::now(),
            })
            .unwrap();

        assert!(state.completed_steps.contains("s1"));
        assert!(!state.running_steps.contains("s1"));
        assert_eq!(state.context["res"], "out");
        assert!(matches!(
            state.step_results["s1"].status,
            StepStatus::Completed
        ));
    }

    #[test]
    fn test_apply_step_failed_no_retry() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::StepStarted {
                step_id: "s1".to_string(),
                started_at: Utc::now(),
            })
            .unwrap();

        state
            .apply(&WorkflowEvent::StepFailed {
                step_id: "s1".to_string(),
                error: "err".to_string(),
                attempt: 1,
                will_retry: false,
            })
            .unwrap();

        assert!(matches!(state.status, InstanceStatus::Failed));
        assert!(matches!(
            state.step_results["s1"].status,
            StepStatus::Failed
        ));
    }

    #[test]
    fn test_apply_signal_wait_and_receive() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::InstanceCreated {
                workflow_id: "wf".to_string(),
                inputs: json!({}),
            })
            .unwrap();
        state
            .apply(&WorkflowEvent::SignalWaitStarted {
                step_id: "s1".to_string(),
                signal_name: "approve".to_string(),
            })
            .unwrap();

        assert!(state.signal_blocked_steps.contains("s1"));
        assert!(matches!(
            state.status,
            InstanceStatus::WaitingForSignal { ref signal } if signal == "approve"
        ));

        state
            .apply(&WorkflowEvent::SignalReceived {
                action: None,
                reviewer: None,
                signal_name: "approve".to_string(),
                payload: json!({"by": "admin"}),
                received_at: Utc::now(),
            })
            .unwrap();

        assert!(state.signal_blocked_steps.is_empty());
        assert!(matches!(state.status, InstanceStatus::Running));
        assert_eq!(state.context["signals"]["approve"]["by"], "admin");
    }

    #[test]
    fn test_apply_signal_received_unexpected_is_ignored() {
        let mut state = blank_state();
        state.status = InstanceStatus::Running;
        let result = state.apply(&WorkflowEvent::SignalReceived {
            action: None,
            reviewer: None,
            signal_name: "approve".to_string(),
            payload: json!({}),
            received_at: Utc::now(),
        });
        assert!(result.is_ok());
        assert!(matches!(state.status, InstanceStatus::Running));
    }

    // ── V0.3.2: 审批测试 ──

    #[test]
    fn test_apply_signal_received_reject_fails_workflow() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::InstanceCreated {
                workflow_id: "wf".to_string(),
                inputs: json!({}),
            })
            .unwrap();
        state
            .apply(&WorkflowEvent::SignalWaitStarted {
                step_id: "s1".to_string(),
                signal_name: "approve".to_string(),
            })
            .unwrap();
        state.step_results.insert(
            "s1".to_string(),
            crate::workflow::StepResult {
                status: StepStatus::Completed,
                output: Some(json!("draft")),
                error: None,
                started_at: Some(Utc::now()),
                completed_at: Some(Utc::now()),
                attempt: 1,
            },
        );

        let result = state.apply(&WorkflowEvent::SignalReceived {
            action: Some(SignalAction::Reject),
            reviewer: Some("alice".to_string()),
            signal_name: "approve".to_string(),
            payload: json!({"reason": "needs work"}),
            received_at: Utc::now(),
        });
        assert!(result.is_ok());
        assert!(matches!(state.status, InstanceStatus::Failed));
        let s1 = state.step_results.get("s1").unwrap();
        assert!(matches!(s1.status, StepStatus::Failed));
        assert!(s1.error.as_ref().unwrap().contains("alice"));
        assert!(s1.error.as_ref().unwrap().contains("needs work"));
    }

    #[test]
    fn test_apply_signal_received_approve_continues() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::InstanceCreated {
                workflow_id: "wf".to_string(),
                inputs: json!({}),
            })
            .unwrap();
        state
            .apply(&WorkflowEvent::SignalWaitStarted {
                step_id: "s1".to_string(),
                signal_name: "approve".to_string(),
            })
            .unwrap();

        let result = state.apply(&WorkflowEvent::SignalReceived {
            action: Some(SignalAction::Approve),
            reviewer: Some("alice".to_string()),
            signal_name: "approve".to_string(),
            payload: json!({}),
            received_at: Utc::now(),
        });
        assert!(result.is_ok());
        assert!(matches!(state.status, InstanceStatus::Running));
        assert!(state.signal_blocked_steps.is_empty());
        assert_eq!(state.context["signals"]["approve"], json!({}));
    }

    #[test]
    fn test_apply_timer_fired_retry() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::StepRetryScheduled {
                step_id: "s1".to_string(),
                attempt: 2,
                scheduled_at: Utc::now(),
            })
            .unwrap();
        assert!(state.scheduled_steps.contains("s1"));

        state
            .apply(&WorkflowEvent::TimerFired {
                timer_id: "retry_s1_2".to_string(),
            })
            .unwrap();

        assert!(!state.scheduled_steps.contains("s1"));
    }

    #[test]
    fn test_apply_timer_fired_signal_timeout() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::TimerFired {
                timer_id: "signal_timeout_s1".to_string(),
            })
            .unwrap();
        // 仅移除 pending_timers，不改变状态（解释器负责处理超时失败）
        assert!(state.pending_timers.is_empty());
    }

    #[test]
    fn test_apply_cancel_requested() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::CancelRequested {
                requested_at: Utc::now(),
            })
            .unwrap();
        assert!(matches!(state.status, InstanceStatus::Failed));
    }

    #[test]
    fn test_apply_workflow_completed() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::WorkflowCompleted {
                outputs: json!({"o": 1}),
                completed_at: Utc::now(),
            })
            .unwrap();
        assert!(matches!(state.status, InstanceStatus::Completed));
    }

    #[test]
    fn test_apply_workflow_failed() {
        let mut state = blank_state();
        state
            .apply(&WorkflowEvent::WorkflowFailed {
                reason: "boom".to_string(),
                failed_at: Utc::now(),
            })
            .unwrap();
        assert!(matches!(state.status, InstanceStatus::Failed));
    }
}
