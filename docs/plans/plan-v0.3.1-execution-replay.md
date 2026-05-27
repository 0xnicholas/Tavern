# Execution Replay API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `GET /executions/:id/replay` endpoint that renders EventStore events as a readable timeline with StateDiff.

**Architecture:** Pure read-only query: `ExecutionReplayer` reads events from `EventStore`, filters by detail/time/step, replays them through `InstanceState` to compute diffs, and returns `ExecutionReplay` JSON.

**Tech Stack:** Rust, axum, serde_json, chrono, thiserror

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/tavern-comp/src/replay.rs` | Create | `ExecutionReplayer`, `ReplayOptions`, `StateDiff`, `TimelineEntry`, `ReplaySummary`, `ExecutionReplay`, `event_timestamp`, `detail_filter`, `compute_state_diff` |
| `crates/tavern-comp/src/lib.rs` | Modify | Add `pub mod replay; pub use replay::*;` |
| `crates/tavern-comp/src/error.rs` | Modify | Add `InvalidReplayRange`, `InvalidParameter` |
| `crates/tavern-server/src/handlers.rs` | Modify | Add `ReplayQueryParams`, `replay_execution_handler`, `map_replay_error` |
| `crates/tavern-server/src/router.rs` | Modify | Add `GET /executions/:id/replay` route |

---

## Task 1: Add Error Variants

**Files:**
- Modify: `crates/tavern-comp/src/error.rs`

- [ ] **Step 1: Add two new error variants**

Add after `PlanningAgentNotRegistered`:

```rust
#[error("invalid replay range: {reason}")]
InvalidReplayRange { reason: String },

#[error("invalid parameter '{field}': {reason}")]
InvalidParameter { field: String, reason: String },
```

- [ ] **Step 2: Add Clone arm in CompError::clone**

Add at the end of the match in `impl Clone for CompError`:

```rust
CompError::InvalidReplayRange { reason } => CompError::InvalidReplayRange {
    reason: reason.clone(),
},
CompError::InvalidParameter { field, reason } => CompError::InvalidParameter {
    field: field.clone(),
    reason: reason.clone(),
},
```

- [ ] **Step 3: Verify compiles**

Run: `cargo check -p tavern-comp`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add crates/tavern-comp/src/error.rs
git commit -m "feat(replay): add InvalidReplayRange and InvalidParameter error variants"
```

---

## Task 2: Implement Core Replay Module

**Files:**
- Create: `crates/tavern-comp/src/replay.rs`

- [ ] **Step 1: Create `crates/tavern-comp/src/replay.rs` with data models**

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

use crate::error::CompError;
use crate::event::WorkflowEvent;
use crate::instance::{InstanceState, InstanceStatus};
use crate::store::EventStore;
use crate::workflow::StepStatus;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionReplay {
    pub execution_id: String,
    pub workflow_id: String,
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
    pub duration_ms: Option<u64>,
    pub raw_payload: Option<Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StateDiff {
    pub context_changed: bool,
    pub context_keys_added: Vec<String>,
    pub context_keys_modified: Vec<String>,
    pub step_status_before: Option<String>,
    pub step_status_after: Option<String>,
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
```

- [ ] **Step 2: Add `ReplayOptions` and `DetailLevel`**

Append to `replay.rs`:

```rust
#[derive(Clone, Debug, Default)]
pub enum DetailLevel {
    #[default]
    Medium,
    Low,
    High,
}

impl DetailLevel {
    pub fn from_str(s: &str) -> Result<Self, CompError> {
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
                    | WorkflowEvent::StepStarted { .. }
                    | WorkflowEvent::StepCompleted { .. }
                    | WorkflowEvent::StepFailed { .. }
                    | WorkflowEvent::SignalReceived { .. }
                    | WorkflowEvent::SignalWaitStarted { .. }
                    | WorkflowEvent::CancelRequested { .. }
                    | WorkflowEvent::StepRetryScheduled { .. }
                    | WorkflowEvent::WorkflowCompleted { .. }
                    | WorkflowEvent::WorkflowFailed { .. }
            ),
            DetailLevel::High => true,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReplayOptions {
    pub detail: DetailLevel,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub step_id: Option<String>,
}

impl ReplayOptions {
    pub fn validate(&self) -> Result<(), CompError> {
        if let (Some(from), Some(to)) = (self.from, self.to) {
            if from > to {
                return Err(CompError::InvalidReplayRange {
                    reason: "'from' must be before or equal to 'to'".to_string(),
                });
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 3: Add `event_timestamp` helper**

Append to `replay.rs`:

```rust
/// Extract the timestamp from any WorkflowEvent variant.
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
        WorkflowEvent::SignalReceived { received_at, .. } => *received_at,
        WorkflowEvent::TimerFired { .. } => Utc::now(),
        WorkflowEvent::CancelRequested { requested_at } => *requested_at,
        WorkflowEvent::WorkflowCompleted { completed_at, .. } => *completed_at,
        WorkflowEvent::WorkflowFailed { failed_at, .. } => *failed_at,
        WorkflowEvent::External { .. } => Utc::now(),
    }
}
```

> **Note:** For events without explicit timestamps (InstanceCreated, InstanceStarted, StepScheduled, StepFailed, SignalWaitStarted, TimerFired, External), we use `Utc::now()` as a fallback. In practice, these events are persisted close to their occurrence time. A future improvement would be to add `created_at` to all events, but that's out of scope for V0.3.1.

- [ ] **Step 4: Add `ExecutionReplayer`**

Append to `replay.rs`:

```rust
pub struct ExecutionReplayer;

impl ExecutionReplayer {
    pub async fn replay(
        store: &dyn EventStore,
        instance_id: &str,
        opts: ReplayOptions,
    ) -> Result<ExecutionReplay, CompError> {
        opts.validate()?;

        // 1. Read all events
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
            events.retain(|e| event_step_id(e).as_ref() == Some(step_id));
        }

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
                WorkflowEvent::StepStarted { step_id, started_at: st } => {
                    step_start_times.insert(step_id.clone(), *st);
                }
                WorkflowEvent::StepCompleted { step_id, completed_at: ct, .. } => {
                    if let Some(st) = step_start_times.get(step_id) {
                        entry.duration_ms = Some((*ct - *st).num_milliseconds().max(0) as u64);
                    }
                }
                WorkflowEvent::StepFailed { step_id, .. } => {
                    if let Some(st) = step_start_times.get(step_id) {
                        entry.duration_ms = Some((Utc::now() - *st).num_milliseconds().max(0) as u64);
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
                WorkflowEvent::StepFailed { will_retry: false, .. } => {
                    summary.failed_steps += 1;
                }
                WorkflowEvent::StepRetryScheduled { .. } => {
                    summary.retries_count += 1;
                }
                WorkflowEvent::SignalReceived { .. } => {
                    summary.signals_received += 1;
                }
                WorkflowEvent::WorkflowCompleted { completed_at: ct, .. } => {
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
            started_at: started_at.unwrap_or_else(|| Utc::now()),
            completed_at,
            status: state.status.as_str().to_string(),
            total_events: timeline.len(),
            timeline,
            summary,
        })
    }
}
```

- [ ] **Step 5: Add helper functions**

Append to `replay.rs`:

```rust
fn event_type_name(event: &WorkflowEvent) -> String {
    match event {
        WorkflowEvent::InstanceCreated { .. } => "InstanceCreated".to_string(),
        WorkflowEvent::InstanceStarted => "InstanceStarted".to_string(),
        WorkflowEvent::StepScheduled { .. } => "StepScheduled".to_string(),
        WorkflowEvent::StepStarted { .. } => "StepStarted".to_string(),
        WorkflowEvent::StepCompleted { .. } => "StepCompleted".to_string(),
        WorkflowEvent::StepFailed { .. } => "StepFailed".to_string(),
        WorkflowEvent::StepRetryScheduled { .. } => "StepRetryScheduled".to_string(),
        WorkflowEvent::SignalWaitStarted { .. } => "SignalWaitStarted".to_string(),
        WorkflowEvent::SignalReceived { .. } => "SignalReceived".to_string(),
        WorkflowEvent::TimerFired { .. } => "TimerFired".to_string(),
        WorkflowEvent::CancelRequested { .. } => "CancelRequested".to_string(),
        WorkflowEvent::WorkflowCompleted { .. } => "WorkflowCompleted".to_string(),
        WorkflowEvent::WorkflowFailed { .. } => "WorkflowFailed".to_string(),
        WorkflowEvent::External { .. } => "External".to_string(),
    }
}

fn event_step_id(event: &WorkflowEvent) -> Option<&str> {
    match event {
        WorkflowEvent::StepScheduled { step_id, .. }
        | WorkflowEvent::StepStarted { step_id, .. }
        | WorkflowEvent::StepCompleted { step_id, .. }
        | WorkflowEvent::StepFailed { step_id, .. }
        | WorkflowEvent::StepRetryScheduled { step_id, .. }
        | WorkflowEvent::SignalWaitStarted { step_id, .. } => Some(step_id),
        _ => None,
    }
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

    // Step status
    match event {
        WorkflowEvent::InstanceStarted => {
            diff.step_status_before = Some("Pending".to_string());
            diff.step_status_after = Some("Running".to_string());
        }
        WorkflowEvent::StepStarted { step_id, .. } => {
            diff.step_status_before = Some("Pending".to_string());
            diff.step_status_after = Some("Running".to_string());
        }
        WorkflowEvent::StepCompleted { step_id, output, .. } => {
            diff.step_status_before = Some("Running".to_string());
            diff.step_status_after = Some("Completed".to_string());
            diff.output_preview = Some(truncate_preview(output));
        }
        WorkflowEvent::StepFailed { .. } => {
            diff.step_status_before = Some("Running".to_string());
            diff.step_status_after = Some("Failed".to_string());
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

```

> **Note:** `seen_steps: HashSet<String>` 在 `ExecutionReplayer::replay` 的局部变量声明处已经初始化，见 Step 4 代码中的 `let mut seen_steps: HashSet<String> = HashSet::new();`。不需要额外的 helper 函数。

- [ ] **Step 6: Wire into lib.rs**

Modify `crates/tavern-comp/src/lib.rs`:

```rust
pub mod replay;
// ... existing mods ...

pub use replay::{
    ExecutionReplay, ExecutionReplayer, ReplayOptions, ReplaySummary,
    StateDiff, TimelineEntry,
};
```

- [ ] **Step 7: Verify compiles**

Run: `cargo check -p tavern-comp`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add crates/tavern-comp/src/replay.rs crates/tavern-comp/src/lib.rs
git commit -m "feat(replay): add ExecutionReplayer with StateDiff and detail levels

- ReplayOptions with detail=low/medium/high, time window, step_id filter
- StateDiff: context keys, step status transitions, output preview
- TimelineEntry with sequence, timestamp, duration_ms
- ReplaySummary: total/completed/failed steps, retries, signals, duration"
```

---

## Task 3: Add Server Handler

**Files:**
- Modify: `crates/tavern-server/src/handlers.rs`
- Modify: `crates/tavern-server/src/router.rs`

- [ ] **Step 1: Add query params struct and handler to handlers.rs**

Add after `default_empty_object`:

```rust
#[derive(Deserialize)]
pub struct ReplayQueryParams {
    #[serde(default = "default_detail")]
    pub detail: String,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub step_id: Option<String>,
}

fn default_detail() -> String {
    "medium".to_string()
}
```

Add after `cancel_execution_handler`:

```rust
pub async fn replay_execution_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<ReplayQueryParams>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let detail = match tavern_comp::replay::DetailLevel::from_str(&params.detail) {
        Ok(d) => d,
        Err(e) => return Err(map_comp_error(e)),
    };

    let opts = tavern_comp::replay::ReplayOptions {
        detail,
        from: params.from,
        to: params.to,
        step_id: params.step_id,
    };

    let replay = match tavern_comp::replay::ExecutionReplayer::replay(
        state.event_store.as_ref(),
        &id,
        opts,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return Err(map_comp_error(e)),
    };

    Ok(Json(replay))
}
```

- [ ] **Step 2: Update imports and error mapping in handlers.rs**

Verify `handlers.rs` imports:

```rust
use chrono::{DateTime, Utc};
```

Add two new arms to `map_comp_error` (before the catch-all `_` arm) to map replay errors to HTTP 400:

```rust
CompError::InvalidReplayRange { reason } => (
    StatusCode::BAD_REQUEST,
    ApiError::new(StatusCode::BAD_REQUEST, "InvalidReplayRange", reason.clone()),
),
CompError::InvalidParameter { field, reason } => (
    StatusCode::BAD_REQUEST,
    ApiError::new(
        StatusCode::BAD_REQUEST,
        "InvalidParameter",
        format!("{}: {}", field, reason),
    ),
),
```

> **Note:** `map_comp_error` has a catch-all `_` arm mapping to 500, so compilation won't fail without this. But adding explicit arms ensures the correct HTTP 400 status per spec.

- [ ] **Step 3: Add route to router.rs**

Add after the `/executions/:id/events/stream` route in `protected_routes`:

```rust
.route(
    "/executions/:id/replay",
    get(handlers::replay_execution_handler),
)
```

- [ ] **Step 4: Verify compiles**

Run: `cargo check --workspace`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/tavern-server/src/handlers.rs crates/tavern-server/src/router.rs
git commit -m "feat(server): add GET /executions/:id/replay handler"
```

---

## Task 4: Write Tests

**Files:**
- Modify: `crates/tavern-comp/src/replay.rs` (add `#[cfg(test)] mod tests`)

- [ ] **Step 1: Add test module to replay.rs**

Append to `crates/tavern-comp/src/replay.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use crate::store::MemoryEventStore;
    use crate::workflow::{Step, Workflow};

    fn make_test_workflow() -> Workflow {
        Workflow {
            id: "test_pipeline".to_string(),
            name: "Test Pipeline".to_string(),
            description: None,
            process: crate::workflow::Process::Sequential,
            planning: None,
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
        let replay = ExecutionReplayer::replay(&store, "exec-1", opts).await.unwrap();

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
        let replay = ExecutionReplayer::replay(&store, "exec-2", opts).await.unwrap();

        // Low detail should exclude StepScheduled
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
        let replay = ExecutionReplayer::replay(&store, "exec-3", opts).await.unwrap();

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

        // Filter to only events before now (all should pass)
        let opts = ReplayOptions {
            detail: DetailLevel::Medium,
            to: Some(Utc::now() + Duration::hours(1)),
            ..Default::default()
        };
        let replay = ExecutionReplayer::replay(&store, "exec-4", opts).await.unwrap();
        assert!(!replay.timeline.is_empty());

        // Filter to only events after now (should be empty)
        let opts2 = ReplayOptions {
            detail: DetailLevel::Medium,
            from: Some(Utc::now() + Duration::hours(1)),
            ..Default::default()
        };
        let replay2 = ExecutionReplayer::replay(&store, "exec-4", opts2).await.unwrap();
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
        let replay = ExecutionReplayer::replay(&store, "exec-5", opts).await.unwrap();

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
        // Only create event, no execution events
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
        let replay = ExecutionReplayer::replay(&store, "exec-empty", opts).await.unwrap();
        assert!(replay.timeline.is_empty()); // Medium excludes InstanceCreated
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
        let replay = ExecutionReplayer::replay(&store, "exec-7", opts).await.unwrap();

        let completed = replay
            .timeline
            .iter()
            .find(|e| e.event_type == "StepCompleted" && e.step_id == Some("research".to_string()))
            .expect("research completed");
        let diff = completed.state_diff.as_ref().expect("has diff");
        assert!(diff.context_changed);
        assert!(diff.context_keys_added.contains(&"research_notes".to_string()));
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
        let replay = ExecutionReplayer::replay(&store, "exec-8", opts).await.unwrap();

        // Medium should include StepScheduled but exclude raw_payload
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
```

- [ ] **Step 2: Fix compilation issues**

Run: `cargo test -p tavern-comp`
Expected: 9 new tests PASS + existing tests still PASS

- [ ] **Step 3: Commit**

```bash
git add crates/tavern-comp/src/replay.rs
git commit -m "test(replay): add 9 unit tests for ExecutionReplayer

- test_replay_basic_timeline: verifies timeline order and summary
- test_replay_detail_low: verifies StepScheduled excluded
- test_replay_detail_high: verifies raw_payload included
- test_replay_detail_medium: verifies Signal events and context diff
- test_replay_time_window: verifies from/to filtering
- test_replay_step_filter: verifies step_id filtering
- test_replay_nonexistent_instance: verifies 404
- test_replay_invalid_range: verifies 400
- test_replay_empty_execution: verifies empty timeline
- test_replay_state_diff: verifies context_keys_added and step_status"
```

---

## Task 5: Full Workspace Verification

- [ ] **Step 1: Run full test suite**

```bash
cargo test --workspace
```
Expected: 182 tests passed (173 existing + 9 new), 0 failed

- [ ] **Step 2: Run clippy**

```bash
cargo clippy --workspace
```
Expected: Only existing minor warnings in tavern-flow, no new warnings

- [ ] **Step 3: Run fmt check**

```bash
cargo fmt -- --check
```
Expected: Zero changes

- [ ] **Step 4: Commit verification lock**

```bash
git commit --allow-empty -m "chore: verify all checks pass (test/clippy/fmt)"
```

---

## Task 6: Final Review Checklist

- [ ] `ExecutionReplayer` is in `crates/tavern-comp/src/replay.rs`
- [ ] `pub mod replay;` added to `crates/tavern-comp/src/lib.rs`
- [ ] `InvalidReplayRange` and `InvalidParameter` added to `CompError`
- [ ] `GET /executions/:id/replay` route added to `router.rs`
- [ ] `replay_execution_handler` added to `handlers.rs`
- [ ] 9 unit tests in `replay.rs` all pass
- [ ] `cargo test --workspace` shows 182 tests passed
- [ ] `cargo clippy --workspace` has zero new warnings
- [ ] `cargo fmt -- --check` is clean
- [ ] All commits have descriptive messages