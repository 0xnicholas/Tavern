use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// V0.3.2: 审批动作类型，用于 SignalReceived 事件。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalAction {
    Approve,
    Reject,
}

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
        attempt: u64,
        output_key: Option<String>,
        completed_at: DateTime<Utc>,
    },
    StepFailed {
        step_id: String,
        error: String,
        attempt: u64,
        will_retry: bool,
    },
    StepRetryScheduled {
        step_id: String,
        attempt: u64,
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
        /// V0.3.2: 审批动作（向后兼容：旧事件反序列化为 None）
        #[serde(default)]
        action: Option<SignalAction>,
        /// V0.3.2: 审批人标识
        #[serde(default)]
        reviewer: Option<String>,
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

    /// 外部事件（Flow 事件、自定义事件等）
    External {
        event_type: String,
        payload: Value,
    },
}
