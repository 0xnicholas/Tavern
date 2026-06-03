use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

use crate::error::CompError;
use crate::event::WorkflowEvent;
use crate::instance::InstanceState;
use crate::store::EventStore;

// ── Data Models ──

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionReplay {
    pub execution_id: String,
    pub workflow_id: String,
    /// 实例首次产生 InstanceStarted 事件的时间；若不存在（如空执行），回退为当前 UTC 时间
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status: String,
    pub total_events: usize,
    pub timeline: Vec<TimelineEntry>,
    pub summary: ReplaySummary,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TimelineEntry {
    pub sequence: usize,
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub step_id: Option<String>,
    pub state_diff: Option<StateDiff>,
    /// 仅 StepCompleted/StepFailed 有值：该步骤从 Started 到 Completed/Failed 的耗时
    pub duration_ms: Option<u64>,
    /// detail=high 时包含原始事件的完整 payload
    pub raw_payload: Option<Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StateDiff {
    pub context_changed: bool,
    pub context_keys_added: Vec<String>,
    pub context_keys_modified: Vec<String>,
    pub step_status_before: Option<String>,
    pub step_status_after: Option<String>,
    /// 截断到 500 字符的预览（避免大 payload）
    pub output_preview: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplaySummary {
    pub total_steps: usize,
    pub completed_steps: usize,
    pub failed_steps: usize,
    pub total_duration_ms: u64,
    pub retries_count: usize,
    pub signals_received: usize,
}

// ── Detail Level ──

#[derive(Clone, Debug, Default)]
pub enum DetailLevel {
    #[default]
    Medium,
    Low,
    High,
}

impl DetailLevel {
    pub fn parse(s: &str) -> Result<Self, CompError> {
        match s {
            "low" => Ok(DetailLevel::Low),
            "medium" => Ok(DetailLevel::Medium),
            "high" => Ok(DetailLevel::High),
            _ => Err(CompError::InvalidParameter {
                field: "detail".to_string(),
                reason: format!("expected 'low', 'medium', or 'high', got '{}'", s),
            }),
        }
    }

    /// Returns true if this event type should be included at this detail level.
    pub fn includes(&self, event: &WorkflowEvent) -> bool {
        match self {
            DetailLevel::Low => matches!(
                event,
                WorkflowEvent::InstanceStarted
                    | WorkflowEvent::StepStarted { .. }
                    | WorkflowEvent::StepCompleted { .. }
                    | WorkflowEvent::StepFailed { .. }
                    | WorkflowEvent::WorkflowCompleted { .. }
                    | WorkflowEvent::WorkflowFailed { .. }
            ),
            DetailLevel::Medium => matches!(
                event,
                WorkflowEvent::InstanceStarted
                    | WorkflowEvent::StepScheduled { .. }
                    | WorkflowEvent::StepStarted { .. }
                    | WorkflowEvent::StepCompleted { .. }
                    | WorkflowEvent::StepFailed { .. }
                    | WorkflowEvent::SignalReceived { .. }
                    | WorkflowEvent::SignalWaitStarted { .. }
                    | WorkflowEvent::BreakpointHit { .. }
                    | WorkflowEvent::CancelRequested { .. }
                    | WorkflowEvent::StepRetryScheduled { .. }
                    | WorkflowEvent::WorkflowCompleted { .. }
                    | WorkflowEvent::WorkflowFailed { .. }
            ),
            DetailLevel::High => true,
        }
    }
}

// ── Replay Options ──

#[derive(Clone, Debug, Default)]
pub struct ReplayOptions {
    pub detail: DetailLevel,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub step_id: Option<String>,
}

impl ReplayOptions {
    pub fn validate(&self) -> Result<(), CompError> {
        if let (Some(from), Some(to)) = (self.from, self.to)
            && from > to
        {
            return Err(CompError::InvalidReplayRange {
                reason: "'from' must be before or equal to 'to'".to_string(),
            });
        }
        Ok(())
    }
}

// ── Event Timestamp Extraction ──

/// Extract the timestamp from any WorkflowEvent variant.
///
/// Events with explicit timestamps use their own field.
/// Events without explicit timestamps fall back to `Utc::now()`.
pub fn event_timestamp(event: &WorkflowEvent) -> DateTime<Utc> {
    match event {
        WorkflowEvent::InstanceCreated { .. } => Utc::now(),
        WorkflowEvent::InstanceStarted => Utc::now(),
        WorkflowEvent::StepScheduled { .. } => Utc::now(),
        WorkflowEvent::StepStarted { started_at, .. } => *started_at,
        WorkflowEvent::StepCompleted { completed_at, .. } => *completed_at,
        WorkflowEvent::StepFailed { .. } => Utc::now(),
        WorkflowEvent::StepRetryScheduled { scheduled_at, .. } => *scheduled_at,
        WorkflowEvent::SignalWaitStarted { .. } => Utc::now(),
        WorkflowEvent::BreakpointHit { paused_at, .. } => *paused_at,
        WorkflowEvent::SignalReceived { received_at, .. } => *received_at,
        WorkflowEvent::TimerFired { .. } => Utc::now(),
        WorkflowEvent::CancelRequested { requested_at } => *requested_at,
        WorkflowEvent::LLMCallStarted { started_at, .. } => *started_at,
        WorkflowEvent::LLMCallCompleted { completed_at, .. } => *completed_at,
        WorkflowEvent::LLMCallFailed { failed_at, .. } => *failed_at,
        WorkflowEvent::ToolCallStarted { started_at, .. } => *started_at,
        WorkflowEvent::ToolCallCompleted { completed_at, .. } => *completed_at,
        WorkflowEvent::ToolCallFailed { failed_at, .. } => *failed_at,
        WorkflowEvent::WorkflowCompleted { completed_at, .. } => *completed_at,
        WorkflowEvent::WorkflowFailed { failed_at, .. } => *failed_at,
        WorkflowEvent::External { .. } => Utc::now(),
    }
}

// ── ExecutionReplayer ──

pub struct ExecutionReplayer;

impl ExecutionReplayer {
    pub async fn replay(
        store: &dyn EventStore,
        instance_id: &str,
        opts: ReplayOptions,
    ) -> Result<ExecutionReplay, CompError> {
        opts.validate()?;

        // 1. Read all events from EventStore
        let all_events = store.read_stream(instance_id).await?;

        if all_events.is_empty() {
            return Err(CompError::InstanceNotFound {
                id: instance_id.to_string(),
            });
        }

        // 2. Initialize state with InstanceCreated (needed for workflow_id)
        let mut state = InstanceState {
            id: instance_id.to_string(),
            ..Default::default()
        };
        for event in &all_events {
            if let WorkflowEvent::InstanceCreated { .. } = event {
                state.apply(event)?;
                break;
            }
        }

        // 3. Build filtered timeline events
        let mut events: Vec<_> = all_events
            .into_iter()
            .filter(|e| opts.detail.includes(e))
            .collect();

        // 4. Time window filter
        if let Some(from) = opts.from {
            events.retain(|e| event_timestamp(e) >= from);
        }
        if let Some(to) = opts.to {
            events.retain(|e| event_timestamp(e) <= to);
        }

        // 5. Step filter
        if let Some(ref step_id) = opts.step_id {
            events.retain(|e| event_step_id(e) == Some(step_id.as_str()));
        }

        // 6. Replay and build timeline
        let mut timeline = Vec::new();
        let mut summary = ReplaySummary::default();
        let mut started_at: Option<DateTime<Utc>> = None;
        let mut completed_at: Option<DateTime<Utc>> = None;
        let mut step_start_times: std::collections::HashMap<String, DateTime<Utc>> =
            std::collections::HashMap::new();
        let mut seen_steps: HashSet<String> = HashSet::new();

        for (seq, event) in events.iter().enumerate() {
            let before = state.clone();
            state.apply(event)?;

            let ts = event_timestamp(event);
            let mut entry = TimelineEntry {
                sequence: seq + 1,
                timestamp: ts,
                event_type: event_type_name(event),
                step_id: event_step_id(event).map(String::from),
                state_diff: None,
                duration_ms: None,
                raw_payload: None,
            };

            // Compute state diff
            if matches!(
                event,
                WorkflowEvent::StepStarted { .. }
                    | WorkflowEvent::StepCompleted { .. }
                    | WorkflowEvent::StepFailed { .. }
                    | WorkflowEvent::InstanceStarted
                    | WorkflowEvent::SignalReceived { .. }
            ) {
                entry.state_diff = Some(compute_state_diff(&before, &state, event));
            }

            // Compute duration for completed/failed steps
            match event {
                WorkflowEvent::StepStarted {
                    step_id,
                    started_at: st,
                } => {
                    step_start_times.insert(step_id.clone(), *st);
                }
                WorkflowEvent::StepCompleted {
                    step_id,
                    completed_at: ct,
                    ..
                } => {
                    if let Some(st) = step_start_times.get(step_id) {
                        entry.duration_ms = Some((*ct - *st).num_milliseconds().max(0) as u64);
                    }
                }
                WorkflowEvent::StepFailed { step_id, .. } => {
                    if let Some(st) = step_start_times.get(step_id) {
                        entry.duration_ms =
                            Some((Utc::now() - *st).num_milliseconds().max(0) as u64);
                    }
                }
                _ => {}
            }

            // Raw payload for high detail
            if matches!(opts.detail, DetailLevel::High) {
                entry.raw_payload = Some(serde_json::to_value(event).unwrap_or_default());
            }

            // Track summary
            match event {
                WorkflowEvent::InstanceStarted => {
                    started_at = Some(ts);
                }
                WorkflowEvent::StepStarted { step_id, .. } => {
                    if seen_steps.insert(step_id.clone()) {
                        summary.total_steps += 1;
                    }
                }
                WorkflowEvent::StepCompleted { .. } => {
                    summary.completed_steps += 1;
                }
                WorkflowEvent::StepFailed {
                    will_retry: false, ..
                } => {
                    summary.failed_steps += 1;
                }
                WorkflowEvent::StepRetryScheduled { .. } => {
                    summary.retries_count += 1;
                }
                WorkflowEvent::SignalReceived { .. } => {
                    summary.signals_received += 1;
                }
                WorkflowEvent::WorkflowCompleted {
                    completed_at: ct, ..
                } => {
                    completed_at = Some(*ct);
                }
                WorkflowEvent::WorkflowFailed { failed_at: ft, .. } => {
                    completed_at = Some(*ft);
                }
                _ => {}
            }

            timeline.push(entry);
        }

        // Compute total duration
        if let (Some(start), Some(end)) = (started_at, completed_at) {
            summary.total_duration_ms = (end - start).num_milliseconds().max(0) as u64;
        }

        Ok(ExecutionReplay {
            execution_id: instance_id.to_string(),
            workflow_id: state.workflow_id.clone(),
            started_at: started_at.unwrap_or_else(Utc::now),
            completed_at,
            status: state.status.as_str().to_string(),
            total_events: timeline.len(),
            timeline,
            summary,
        })
    }
}

// ── Helper Functions ──

fn event_type_name(event: &WorkflowEvent) -> String {
    match event {
        WorkflowEvent::InstanceCreated { .. } => "InstanceCreated",
        WorkflowEvent::InstanceStarted => "InstanceStarted",
        WorkflowEvent::StepScheduled { .. } => "StepScheduled",
        WorkflowEvent::StepStarted { .. } => "StepStarted",
        WorkflowEvent::StepCompleted { .. } => "StepCompleted",
        WorkflowEvent::StepFailed { .. } => "StepFailed",
        WorkflowEvent::StepRetryScheduled { .. } => "StepRetryScheduled",
        WorkflowEvent::SignalWaitStarted { .. } => "SignalWaitStarted",
        WorkflowEvent::BreakpointHit { .. } => "BreakpointHit",
        WorkflowEvent::SignalReceived { .. } => "SignalReceived",
        WorkflowEvent::TimerFired { .. } => "TimerFired",
        WorkflowEvent::CancelRequested { .. } => "CancelRequested",
        WorkflowEvent::LLMCallStarted { .. } => "LLMCallStarted",
        WorkflowEvent::LLMCallCompleted { .. } => "LLMCallCompleted",
        WorkflowEvent::LLMCallFailed { .. } => "LLMCallFailed",
        WorkflowEvent::ToolCallStarted { .. } => "ToolCallStarted",
        WorkflowEvent::ToolCallCompleted { .. } => "ToolCallCompleted",
        WorkflowEvent::ToolCallFailed { .. } => "ToolCallFailed",
        WorkflowEvent::WorkflowCompleted { .. } => "WorkflowCompleted",
        WorkflowEvent::WorkflowFailed { .. } => "WorkflowFailed",
        WorkflowEvent::External { .. } => "External",
    }
    .to_string()
}

fn event_step_id(event: &WorkflowEvent) -> Option<&str> {
    match event {
        WorkflowEvent::StepScheduled { step_id, .. }
        | WorkflowEvent::StepStarted { step_id, .. }
        | WorkflowEvent::StepCompleted { step_id, .. }
        | WorkflowEvent::StepFailed { step_id, .. }
        | WorkflowEvent::StepRetryScheduled { step_id, .. }
        | WorkflowEvent::SignalWaitStarted { step_id, .. }
        | WorkflowEvent::LLMCallStarted { step_id, .. }
        | WorkflowEvent::LLMCallCompleted { step_id, .. }
        | WorkflowEvent::LLMCallFailed { step_id, .. }
        | WorkflowEvent::ToolCallStarted { step_id, .. }
        | WorkflowEvent::ToolCallCompleted { step_id, .. }
        | WorkflowEvent::ToolCallFailed { step_id, .. } => Some(step_id),
        _ => None,
    }
}

fn step_status_str(status: &crate::workflow::StepStatus) -> String {
    match status {
        crate::workflow::StepStatus::Pending => "Pending",
        crate::workflow::StepStatus::Running => "Running",
        crate::workflow::StepStatus::Completed => "Completed",
        crate::workflow::StepStatus::Failed => "Failed",
    }
    .to_string()
}

fn compute_state_diff(
    before: &InstanceState,
    after: &InstanceState,
    event: &WorkflowEvent,
) -> StateDiff {
    let mut diff = StateDiff::default();

    // Context keys diff
    if let (Some(before_obj), Some(after_obj)) =
        (before.context.as_object(), after.context.as_object())
    {
        for key in after_obj.keys() {
            if !before_obj.contains_key(key) {
                diff.context_keys_added.push(key.clone());
                diff.context_changed = true;
            } else if before_obj.get(key) != after_obj.get(key) {
                diff.context_keys_modified.push(key.clone());
                diff.context_changed = true;
            }
        }
    }

    // Step status transitions — derive from actual state, not hardcoded from event type
    match event {
        WorkflowEvent::InstanceStarted => {
            diff.step_status_before = Some("Pending".to_string());
            diff.step_status_after = Some("Running".to_string());
        }
        WorkflowEvent::StepStarted { step_id, .. }
        | WorkflowEvent::StepCompleted { step_id, .. }
        | WorkflowEvent::StepFailed { step_id, .. } => {
            diff.step_status_before = before
                .step_results
                .get(step_id)
                .map(|r| step_status_str(&r.status));
            diff.step_status_after = after
                .step_results
                .get(step_id)
                .map(|r| step_status_str(&r.status));
            if let WorkflowEvent::StepCompleted { output, .. } = event {
                diff.output_preview = Some(truncate_preview(output));
            }
        }
        WorkflowEvent::SignalReceived { .. } => {
            diff.step_status_before = Some("WaitingForSignal".to_string());
            diff.step_status_after = Some("Running".to_string());
        }
        _ => {}
    }

    diff
}

fn truncate_preview(value: &Value) -> String {
    let s = value.to_string();
    let chars: Vec<char> = s.chars().collect();
    if chars.len() > 500 {
        chars.into_iter().take(497).collect::<String>() + "..."
    } else {
        s
    }
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryEventStore;
    use crate::workflow::{Step, Workflow};
    use chrono::Duration;

    fn make_test_workflow() -> Workflow {
        Workflow {
            id: "test_pipeline".to_string(),
            name: "Test Pipeline".to_string(),
            description: None,
            process: crate::workflow::Process::Sequential,
            planning: None,
            webhook: None,
            schedule: None,
            schedule_inputs: serde_json::Value::Null,
            steps: vec![
                Step {
                    id: "research".to_string(),
                    agent_id: "researcher".to_string(),
                    task: "Research {{topic}}".to_string(),
                    depends_on: vec![],
                    output_key: Some("research_notes".to_string()),
                    timeout: Some(300),
                    retries: Some(0),
                    retry_delay: Some(0),
                    wait_for_signal: None,
                    signal_timeout: None,
                    expected_output: None,
                    signal_timeout_action: None,
                    breakpoint: false,
                    model_override: None,
                    or_depends_on: vec![],
                    router: None,
                },
                Step {
                    id: "write".to_string(),
                    agent_id: "writer".to_string(),
                    task: "Write about {{research_notes}}".to_string(),
                    depends_on: vec!["research".to_string()],
                    output_key: Some("draft".to_string()),
                    timeout: Some(300),
                    retries: Some(0),
                    retry_delay: Some(0),
                    wait_for_signal: None,
                    signal_timeout: None,
                    expected_output: None,
                    signal_timeout_action: None,
                    breakpoint: false,
                    model_override: None,
                    or_depends_on: vec![],
                    router: None,
                },
            ],
            inputs: vec![],
            outputs: vec![],
        }
    }

    fn create_test_events() -> Vec<WorkflowEvent> {
        let wf = make_test_workflow();
        vec![
            WorkflowEvent::InstanceCreated {
                workflow_id: wf.id.clone(),
                inputs: serde_json::json!({"topic": "AI"}),
            },
            WorkflowEvent::InstanceStarted,
            WorkflowEvent::StepScheduled {
                step_id: "research".to_string(),
                attempt: 1,
            },
            WorkflowEvent::StepStarted {
                step_id: "research".to_string(),
                started_at: Utc::now(),
            },
            WorkflowEvent::StepCompleted {
                step_id: "research".to_string(),
                output: serde_json::json!("research output"),
                attempt: 1,
                output_key: Some("research_notes".to_string()),
                completed_at: Utc::now(),
            },
            WorkflowEvent::StepScheduled {
                step_id: "write".to_string(),
                attempt: 1,
            },
            WorkflowEvent::StepStarted {
                step_id: "write".to_string(),
                started_at: Utc::now(),
            },
            WorkflowEvent::StepCompleted {
                step_id: "write".to_string(),
                output: serde_json::json!("draft output"),
                attempt: 1,
                output_key: Some("draft".to_string()),
                completed_at: Utc::now(),
            },
            WorkflowEvent::WorkflowCompleted {
                outputs: serde_json::json!({}),
                completed_at: Utc::now(),
            },
        ]
    }

    #[tokio::test]
    async fn test_replay_basic_timeline() {
        let store = MemoryEventStore::new();
        let events = create_test_events();
        for event in &events {
            store.append("exec-1", event.clone()).await.unwrap();
        }

        let opts = ReplayOptions::default();
        let replay = ExecutionReplayer::replay(&store, "exec-1", opts)
            .await
            .unwrap();

        assert_eq!(replay.execution_id, "exec-1");
        assert_eq!(replay.workflow_id, "test_pipeline");
        assert_eq!(replay.status, "completed");
        assert!(!replay.timeline.is_empty());
        assert_eq!(replay.summary.completed_steps, 2);
        assert_eq!(replay.summary.total_steps, 2);
    }

    #[tokio::test]
    async fn test_replay_detail_low() {
        let store = MemoryEventStore::new();
        let events = create_test_events();
        for event in &events {
            store.append("exec-2", event.clone()).await.unwrap();
        }

        let opts = ReplayOptions {
            detail: DetailLevel::Low,
            ..Default::default()
        };
        let replay = ExecutionReplayer::replay(&store, "exec-2", opts)
            .await
            .unwrap();

        let has_scheduled = replay
            .timeline
            .iter()
            .any(|e| e.event_type == "StepScheduled");
        assert!(!has_scheduled);
    }

    #[tokio::test]
    async fn test_replay_detail_high() {
        let store = MemoryEventStore::new();
        let events = create_test_events();
        for event in &events {
            store.append("exec-3", event.clone()).await.unwrap();
        }

        let opts = ReplayOptions {
            detail: DetailLevel::High,
            ..Default::default()
        };
        let replay = ExecutionReplayer::replay(&store, "exec-3", opts)
            .await
            .unwrap();

        let completed = replay
            .timeline
            .iter()
            .find(|e| e.event_type == "StepCompleted")
            .expect("should have StepCompleted");
        assert!(completed.raw_payload.is_some());
    }

    #[tokio::test]
    async fn test_replay_time_window() {
        let store = MemoryEventStore::new();
        let events = create_test_events();
        for event in &events {
            store.append("exec-4", event.clone()).await.unwrap();
        }

        let opts = ReplayOptions {
            detail: DetailLevel::Medium,
            to: Some(Utc::now() + Duration::hours(1)),
            ..Default::default()
        };
        let replay = ExecutionReplayer::replay(&store, "exec-4", opts)
            .await
            .unwrap();
        assert!(!replay.timeline.is_empty());

        let opts2 = ReplayOptions {
            detail: DetailLevel::Medium,
            from: Some(Utc::now() + Duration::hours(1)),
            ..Default::default()
        };
        let replay2 = ExecutionReplayer::replay(&store, "exec-4", opts2)
            .await
            .unwrap();
        assert!(replay2.timeline.is_empty());
    }

    #[tokio::test]
    async fn test_replay_step_filter() {
        let store = MemoryEventStore::new();
        let events = create_test_events();
        for event in &events {
            store.append("exec-5", event.clone()).await.unwrap();
        }

        let opts = ReplayOptions {
            detail: DetailLevel::Medium,
            step_id: Some("research".to_string()),
            ..Default::default()
        };
        let replay = ExecutionReplayer::replay(&store, "exec-5", opts)
            .await
            .unwrap();

        assert!(replay.timeline.iter().all(|e| {
            e.step_id.as_ref() == Some(&"research".to_string()) || e.step_id.is_none()
        }));
    }

    #[tokio::test]
    async fn test_replay_nonexistent_instance() {
        let store = MemoryEventStore::new();
        let opts = ReplayOptions::default();
        let result = ExecutionReplayer::replay(&store, "nonexistent", opts).await;
        assert!(matches!(result, Err(CompError::InstanceNotFound { .. })));
    }

    #[tokio::test]
    async fn test_replay_invalid_range() {
        let store = MemoryEventStore::new();
        let events = create_test_events();
        for event in &events {
            store.append("exec-6", event.clone()).await.unwrap();
        }

        let opts = ReplayOptions {
            from: Some(Utc::now() + Duration::hours(1)),
            to: Some(Utc::now()),
            ..Default::default()
        };
        let result = ExecutionReplayer::replay(&store, "exec-6", opts).await;
        assert!(matches!(result, Err(CompError::InvalidReplayRange { .. })));
    }

    #[tokio::test]
    async fn test_replay_empty_execution() {
        let store = MemoryEventStore::new();
        store
            .append(
                "exec-empty",
                WorkflowEvent::InstanceCreated {
                    workflow_id: "test".to_string(),
                    inputs: serde_json::json!({}),
                },
            )
            .await
            .unwrap();

        let opts = ReplayOptions::default(); // detail=Medium, excludes InstanceCreated
        let replay = ExecutionReplayer::replay(&store, "exec-empty", opts)
            .await
            .unwrap();
        assert!(replay.timeline.is_empty());
        assert_eq!(replay.status, "pending");
    }

    #[tokio::test]
    async fn test_replay_state_diff() {
        let store = MemoryEventStore::new();
        let events = create_test_events();
        for event in &events {
            store.append("exec-7", event.clone()).await.unwrap();
        }

        let opts = ReplayOptions::default();
        let replay = ExecutionReplayer::replay(&store, "exec-7", opts)
            .await
            .unwrap();

        let completed = replay
            .timeline
            .iter()
            .find(|e| e.event_type == "StepCompleted" && e.step_id == Some("research".to_string()))
            .expect("research completed");
        let diff = completed.state_diff.as_ref().expect("has diff");
        assert!(diff.context_changed);
        assert!(
            diff.context_keys_added
                .contains(&"research_notes".to_string())
        );
        assert_eq!(diff.step_status_before, Some("Running".to_string()));
        assert_eq!(diff.step_status_after, Some("Completed".to_string()));
    }

    #[tokio::test]
    async fn test_replay_detail_medium() {
        let store = MemoryEventStore::new();
        let events = create_test_events();
        for event in &events {
            store.append("exec-8", event.clone()).await.unwrap();
        }

        let opts = ReplayOptions {
            detail: DetailLevel::Medium,
            ..Default::default()
        };
        let replay = ExecutionReplayer::replay(&store, "exec-8", opts)
            .await
            .unwrap();

        let has_scheduled = replay
            .timeline
            .iter()
            .any(|e| e.event_type == "StepScheduled");
        assert!(has_scheduled);

        let completed = replay
            .timeline
            .iter()
            .find(|e| e.event_type == "StepCompleted")
            .expect("has completed");
        assert!(completed.raw_payload.is_none());
        assert!(completed.state_diff.is_some());
    }
}
