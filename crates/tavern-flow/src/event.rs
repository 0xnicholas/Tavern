//! FlowEvent — events for Flow execution audit trail.
//! Serialized and stored via `WorkflowEvent::External` in the EventStore.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tavern_comp::WorkflowEvent;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FlowEvent {
    FlowStarted {
        flow_name: String,
        inputs: Value,
        started_at: DateTime<Utc>,
    },
    MethodCompleted {
        method_name: String,
        output: Value,
        completed_at: DateTime<Utc>,
    },
    MethodFailed {
        method_name: String,
        error: String,
    },
    RouterDecision {
        router_name: String,
        labels: Vec<String>,
    },
    FlowCompleted {
        outputs: Value,
        completed_at: DateTime<Utc>,
    },
    FlowFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
    /// V0.3.7: Flow 方法断点命中。
    BreakpointHit {
        method_name: String,
        paused_at: DateTime<Utc>,
    },
}

impl FlowEvent {
    pub fn to_workflow_event(&self) -> WorkflowEvent {
        let event_type = match self {
            FlowEvent::FlowStarted { .. } => "flow_started",
            FlowEvent::MethodCompleted { .. } => "method_completed",
            FlowEvent::MethodFailed { .. } => "method_failed",
            FlowEvent::RouterDecision { .. } => "router_decision",
            FlowEvent::FlowCompleted { .. } => "flow_completed",
            FlowEvent::FlowFailed { .. } => "flow_failed",
            FlowEvent::BreakpointHit { .. } => "breakpoint_hit",
        };
        WorkflowEvent::External {
            event_type: event_type.to_string(),
            payload: serde_json::to_value(self).unwrap_or_default(),
        }
    }
}
