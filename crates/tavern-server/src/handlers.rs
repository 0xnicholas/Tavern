use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tavern_comp::{CompError, SignalAction};
use tavern_core::RuntimeError;
use tavern_hero::TavernError;

use chrono::{DateTime, Utc};

use crate::state::AppState;

#[derive(Serialize)]
pub struct ApiError {
    pub error: String,
    pub message: String,
    #[serde(skip)]
    pub status: StatusCode,
}

impl ApiError {
    pub fn new(status: StatusCode, error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            error: error.into(),
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(self)).into_response()
    }
}

#[derive(Deserialize)]
pub struct ExecuteRequest {
    pub task: String,
    #[serde(default = "default_empty_object")]
    pub context: Value,
}

fn default_empty_object() -> Value {
    serde_json::json!({})
}

pub fn map_tavern_error(err: &TavernError) -> (StatusCode, ApiError) {
    match err {
        TavernError::AgentNotFound { id } => (
            StatusCode::NOT_FOUND,
            ApiError::new(
                StatusCode::NOT_FOUND,
                "AgentNotFound",
                format!("Agent '{}' not found", id),
            ),
        ),
        TavernError::DuplicateAgent { .. } => (
            StatusCode::CONFLICT,
            ApiError::new(StatusCode::CONFLICT, "DuplicateAgent", err.to_string()),
        ),
        TavernError::ConfigParse { .. } | TavernError::Io(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                err.to_string(),
            ),
        ),
        TavernError::Runtime(runtime_err) => match runtime_err {
            RuntimeError::RequestFailed { .. }
            | RuntimeError::NetworkError(_)
            | RuntimeError::InvalidResponse(_) => (
                StatusCode::BAD_GATEWAY,
                ApiError::new(StatusCode::BAD_GATEWAY, "RuntimeError", err.to_string()),
            ),
        },
    }
}

pub async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // Check event store connectivity
    let store_check = match state.event_store.read_stream("__health_check__").await {
        Ok(_) => "connected",
        Err(_) => "disconnected",
    };

    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "checks": {
            "store": store_check,
        }
    }))
}

pub async fn list_agents_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let summaries = state.hero.list_agents_summary().await;
    Json(summaries)
}

pub async fn get_agent_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    match state.hero.get_agent(&id).await {
        Some(config) => Ok(Json(config.clone())),
        None => Err(map_tavern_error(&TavernError::AgentNotFound { id })),
    }
}

const MAX_TASK_LENGTH: usize = 100_000;

pub async fn execute_agent_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<ExecuteRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    if req.task.len() > MAX_TASK_LENGTH {
        return Err((
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "TaskTooLong",
                format!("task exceeds max length of {}", MAX_TASK_LENGTH),
            ),
        ));
    }
    match state.hero.execute(&id, &req.task, Some(req.context)).await {
        Ok(result) => Ok(Json(result)),
        Err(e) => Err(map_tavern_error(&e)),
    }
}

// ── V0.3.8: Agent 动态管理 ──

pub async fn create_agent_handler(
    State(state): State<Arc<AppState>>,
    Json(config): Json<tavern_core::AgentConfig>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    state
        .hero
        .register_agent(config)
        .await
        .map_err(|e| map_tavern_error(&e))?;
    Ok(StatusCode::CREATED)
}

pub async fn delete_agent_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    state
        .hero
        .unregister_agent(&id)
        .await
        .map_err(|e| map_tavern_error(&e))?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------- CompError mapping ----------

pub fn map_comp_error(err: CompError) -> (StatusCode, ApiError) {
    match &err {
        CompError::WorkflowNotFound { id } => (
            StatusCode::NOT_FOUND,
            ApiError::new(
                StatusCode::NOT_FOUND,
                "WorkflowNotFound",
                format!("Workflow '{}' not found", id),
            ),
        ),
        CompError::AgentNotFound { id } => (
            StatusCode::NOT_FOUND,
            ApiError::new(
                StatusCode::NOT_FOUND,
                "AgentNotFound",
                format!("Agent '{}' not found", id),
            ),
        ),
        CompError::MissingInput { name } => (
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "MissingInput",
                format!("Missing required input: {}", name),
            ),
        ),
        CompError::InvalidInputType { got } => (
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "InvalidInputType",
                format!("Invalid input type: expected JSON object, got {}", got),
            ),
        ),
        CompError::ConfigParse { reason, .. } => (
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "ConfigParse",
                format!("Configuration error: {}", reason),
            ),
        ),
        CompError::TemplateParse { reason } => (
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "TemplateParse",
                format!("Template parse error: {}", reason),
            ),
        ),
        CompError::StepFailed { step_id, reason } => (
            StatusCode::BAD_GATEWAY,
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "StepFailed",
                format!("Step '{}' failed: {}", step_id, reason),
            ),
        ),
        CompError::InstanceNotFound { id } => (
            StatusCode::NOT_FOUND,
            ApiError::new(
                StatusCode::NOT_FOUND,
                "InstanceNotFound",
                format!("Execution '{}' not found", id),
            ),
        ),
        CompError::InstanceClosed { id } => (
            StatusCode::CONFLICT,
            ApiError::new(
                StatusCode::CONFLICT,
                "InstanceClosed",
                format!("Execution '{}' is closed", id),
            ),
        ),
        CompError::SignalRejected { id, signal } => (
            StatusCode::CONFLICT,
            ApiError::new(
                StatusCode::CONFLICT,
                "SignalRejected",
                format!("Instance '{}' is not waiting for signal '{}'", id, signal),
            ),
        ),
        CompError::StoreError(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "StoreError",
                err.to_string(),
            ),
        ),
        CompError::ManagerError { reason } => (
            StatusCode::BAD_GATEWAY,
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "ManagerError",
                format!("Manager agent error: {}", reason),
            ),
        ),
        CompError::ManagerLoopExceeded { max_loops } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ManagerLoopExceeded",
                format!("Manager exceeded {} loops", max_loops),
            ),
        ),
        CompError::PlanningError { reason } => (
            StatusCode::BAD_GATEWAY,
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "PlanningError",
                format!("Planning error: {}", reason),
            ),
        ),
        CompError::PlanningAgentNotRegistered { id } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "PlanningAgentNotRegistered",
                format!("Planning agent '{}' not registered", id),
            ),
        ),
        CompError::InvalidReplayRange { reason } => (
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "InvalidReplayRange",
                reason.clone(),
            ),
        ),
        CompError::InvalidParameter { field, reason } => (
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!("{}: {}", field, reason),
            ),
        ),
        CompError::Hero(hero_err) => map_tavern_error(hero_err),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                err.to_string(),
            ),
        ),
    }
}

// ---------- Workflow handlers ----------

pub async fn list_workflows_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let registry = state.registry.read().await;
    let summaries = registry.list_all();
    Json(summaries)
}

pub async fn get_workflow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let registry = state.registry.read().await;
    match registry.get(&id) {
        Some(workflow) => Ok(Json(workflow.clone())),
        None => Err(map_comp_error(CompError::WorkflowNotFound { id })),
    }
}

#[derive(Serialize)]
pub struct WorkflowRunResponse {
    pub context: Value,
    pub outputs: Value,
    pub step_results: Value,
}

pub async fn run_workflow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(inputs): Json<Value>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let workflow = {
        let registry = state.registry.read().await;
        match registry.get(&id) {
            Some(w) => w.clone(),
            None => return Err(map_comp_error(CompError::WorkflowNotFound { id })),
        }
    };

    let start = std::time::Instant::now();
    let engine = tavern_comp::WorkflowEngine::new(state.hero.clone())
        .with_max_concurrency(state.max_concurrency)
        .with_store(state.event_store.clone());
    let result = match engine.run(&workflow, inputs).await {
        Ok(r) => {
            state.workflow_executions.fetch_add(1, Ordering::Relaxed);
            let ms = start.elapsed().as_millis() as u64;
            state
                .workflow_duration_ms_total
                .fetch_add(ms, Ordering::Relaxed);
            record_duration_bucket(&state.workflow_duration_buckets, ms);
            r
        }
        Err(e) => {
            state.workflow_executions.fetch_add(1, Ordering::Relaxed);
            state.workflow_failures.fetch_add(1, Ordering::Relaxed);
            state
                .workflow_duration_ms_total
                .fetch_add(start.elapsed().as_millis() as u64, Ordering::Relaxed);
            return Err(map_comp_error(e));
        }
    };

    Ok(Json(WorkflowRunResponse {
        context: result.context,
        outputs: result.outputs,
        step_results: serde_json::to_value(&result.step_results).unwrap_or_default(),
    }))
}

// ── V0.3.4: 批量执行 ──

const MAX_BATCH_SIZE: usize = 100;
const MAX_BATCH_CONCURRENCY: usize = 50;

#[derive(Deserialize)]
pub struct BatchRunRequest {
    pub inputs: Vec<Value>,
    #[serde(default = "default_batch_concurrency")]
    pub max_concurrency: u32,
}

fn default_batch_concurrency() -> u32 {
    10
}

#[derive(Serialize)]
pub struct BatchRunResponse {
    pub workflow_id: String,
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub results: Vec<BatchResultItem>,
}

#[derive(Serialize)]
pub struct BatchResultItem {
    pub index: usize,
    pub execution_id: String,
    pub status: String,
    pub inputs: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outputs: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub duration_ms: u64,
}

pub async fn run_workflow_batch_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<BatchRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    use std::time::Instant;
    use tokio::sync::Semaphore;
    use tokio::task::JoinSet;

    // 1. Validate
    if req.inputs.is_empty() || req.inputs.len() > MAX_BATCH_SIZE {
        return Err((
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "InvalidBatchSize",
                format!("batch size must be 1-{}", MAX_BATCH_SIZE),
            ),
        ));
    }
    if req.max_concurrency == 0 || req.max_concurrency as usize > MAX_BATCH_CONCURRENCY {
        return Err((
            StatusCode::BAD_REQUEST,
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "InvalidConcurrency",
                format!("max_concurrency must be 1-{}", MAX_BATCH_CONCURRENCY),
            ),
        ));
    }

    // 2. Lookup workflow
    let workflow = {
        let registry = state.registry.read().await;
        match registry.get(&id) {
            Some(w) => w.clone(),
            None => return Err(map_comp_error(CompError::WorkflowNotFound { id })),
        }
    };

    // 3. Parallel execution
    let engine = Arc::new(
        tavern_comp::WorkflowEngine::new(state.hero.clone())
            .with_max_concurrency(state.max_concurrency)
            .with_store(state.event_store.clone()),
    );

    let semaphore = Arc::new(Semaphore::new(req.max_concurrency as usize));
    let mut join_set = JoinSet::new();
    let total = req.inputs.len();

    for (i, inputs) in req.inputs.into_iter().enumerate() {
        let engine = engine.clone();
        let workflow = workflow.clone();
        let inputs_for_result = inputs.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        join_set.spawn(async move {
            let _permit = permit;
            let start = Instant::now();
            // Use start() + await_completion() to capture execution_id for both success/failure
            let exec_result = {
                let start_handle = engine.start(&workflow, inputs).await;
                match start_handle {
                    Ok(mut handle) => {
                        let id = handle.id().to_string();
                        let id_for_err = id.clone();
                        handle
                            .await_completion()
                            .await
                            .map(|r| (id, r))
                            .map_err(|e| (id_for_err, e))
                    }
                    Err(e) => Err((String::new(), e)),
                }
            };
            let duration_ms = start.elapsed().as_millis() as u64;
            (i, inputs_for_result, exec_result, duration_ms)
        });
    }

    // 4. Collect results
    let mut items: Vec<BatchResultItem> = Vec::new();
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    let mut total_duration_ms = 0u64;

    while let Some(Ok((i, inputs, exec_result, duration_ms))) = join_set.join_next().await {
        total_duration_ms += duration_ms;
        record_duration_bucket(&state.workflow_duration_buckets, duration_ms);
        match exec_result {
            Ok((exec_id, r)) => {
                succeeded += 1;
                items.push(BatchResultItem {
                    index: i,
                    execution_id: exec_id,
                    status: "completed".to_string(),
                    inputs,
                    outputs: Some(r.outputs),
                    error: None,
                    duration_ms,
                });
            }
            Err((exec_id, e)) => {
                failed += 1;
                items.push(BatchResultItem {
                    index: i,
                    execution_id: exec_id,
                    status: "failed".to_string(),
                    inputs,
                    outputs: None,
                    error: Some(e.to_string()),
                    duration_ms,
                });
            }
        }
    }
    items.sort_by_key(|r| r.index);

    // 5. Update metrics
    state
        .workflow_executions
        .fetch_add(total as u64, Ordering::Relaxed);
    state
        .workflow_failures
        .fetch_add(failed as u64, Ordering::Relaxed);
    state
        .workflow_duration_ms_total
        .fetch_add(total_duration_ms, Ordering::Relaxed);

    Ok(Json(BatchRunResponse {
        workflow_id: id,
        total,
        succeeded,
        failed,
        results: items,
    }))
}

#[derive(Serialize)]
pub struct StartWorkflowResponse {
    pub execution_id: String,
}

pub async fn start_workflow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(inputs): Json<Value>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let workflow = {
        let registry = state.registry.read().await;
        match registry.get(&id) {
            Some(w) => w.clone(),
            None => return Err(map_comp_error(CompError::WorkflowNotFound { id })),
        }
    };

    let engine = tavern_comp::WorkflowEngine::new(state.hero.clone())
        .with_max_concurrency(state.max_concurrency)
        .with_store(state.event_store.clone());
    let handle = match engine.start(&workflow, inputs).await {
        Ok(h) => h,
        Err(e) => return Err(map_comp_error(e)),
    };

    let execution_id = handle.id().to_string();
    let interpreter_handle = handle.interpreter_handle;
    let signal_tx = handle.signal_tx;
    {
        let mut handles = state.execution_handles.write().await;
        handles.insert(execution_id.clone(), signal_tx);
    }

    // Pre-create broadcast sender for SSE subscriptions
    {
        let mut broadcasts = state.event_broadcasts.write().await;
        broadcasts.entry(execution_id.clone()).or_insert_with(|| {
            tokio::sync::broadcast::channel::<tavern_comp::WorkflowEvent>(128).0
        });
    }

    let exec_id = execution_id.clone();
    let handles_arc = state.execution_handles.clone();
    let broadcasts_arc = state.event_broadcasts.clone();
    tokio::spawn(async move {
        let _ = interpreter_handle.await;
        let mut handles = handles_arc.write().await;
        handles.remove(&exec_id);
        let mut broadcasts = broadcasts_arc.write().await;
        broadcasts.remove(&exec_id);
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(StartWorkflowResponse { execution_id }),
    ))
}

#[derive(Serialize)]
pub struct ExecutionResponse {
    pub execution_id: String,
    pub workflow_id: String,
    pub status: String,
    pub context: Value,
    pub outputs: Value,
    pub step_results: Value,
    pub started_at: Option<String>,
}

pub async fn get_execution_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let events = match state.event_store.read_stream(&id).await {
        Ok(e) => e,
        Err(e) => return Err(map_comp_error(e)),
    };

    if events.is_empty() {
        return Err(map_comp_error(CompError::InstanceNotFound { id }));
    }

    let mut instance_state = tavern_comp::InstanceState {
        id: id.clone(),
        ..Default::default()
    };
    for event in &events {
        let _ = instance_state.apply(event);
    }

    let started_at = events.iter().find_map(|e| match e {
        tavern_comp::WorkflowEvent::StepStarted { started_at, .. } => Some(started_at.to_rfc3339()),
        _ => None,
    });

    Ok(Json(ExecutionResponse {
        execution_id: id,
        workflow_id: instance_state.workflow_id,
        status: instance_state.status.as_str().to_string(),
        context: instance_state.context,
        outputs: match &instance_state.status {
            tavern_comp::InstanceStatus::Completed => events
                .iter()
                .find_map(|e| match e {
                    tavern_comp::WorkflowEvent::WorkflowCompleted { outputs, .. } => {
                        Some(outputs.clone())
                    }
                    _ => None,
                })
                .unwrap_or_default(),
            _ => Value::Object(serde_json::Map::new()),
        },
        step_results: serde_json::to_value(&instance_state.step_results).unwrap_or_default(),
        started_at,
    }))
}

pub async fn get_execution_events_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let events = match state.event_store.read_stream(&id).await {
        Ok(e) => e,
        Err(e) => return Err(map_comp_error(e)),
    };

    if events.is_empty() {
        return Err(map_comp_error(CompError::InstanceNotFound { id }));
    }

    Ok(Json(events))
}

#[derive(Deserialize)]
pub struct SignalRequest {
    pub signal_name: String,
    pub payload: Value,
}

pub async fn signal_execution_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<SignalRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    {
        let handles = state.execution_handles.read().await;
        if let Some(signal_tx) = handles.get(&id) {
            let result = signal_tx
                .send(tavern_comp::WorkflowEvent::SignalReceived {
                    signal_name: req.signal_name,
                    payload: req.payload,
                    received_at: Utc::now(),
                    action: None,
                    reviewer: None,
                })
                .await
                .map_err(|_| CompError::InstanceClosed { id: id.clone() });
            match result {
                Ok(()) => return Ok(StatusCode::ACCEPTED),
                Err(e) => return Err(map_comp_error(e)),
            }
        }
    }

    let events = match state.event_store.read_stream(&id).await {
        Ok(e) => e,
        Err(e) => return Err(map_comp_error(e)),
    };

    if events.is_empty() {
        return Err(map_comp_error(CompError::InstanceNotFound { id }));
    }

    Err(map_comp_error(CompError::InstanceClosed { id }))
}

pub async fn cancel_execution_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    {
        let handles = state.execution_handles.read().await;
        if let Some(signal_tx) = handles.get(&id) {
            let result = signal_tx
                .send(tavern_comp::WorkflowEvent::CancelRequested {
                    requested_at: Utc::now(),
                })
                .await
                .map_err(|_| CompError::InstanceClosed { id: id.clone() });
            match result {
                Ok(()) => return Ok(StatusCode::ACCEPTED),
                Err(e) => return Err(map_comp_error(e)),
            }
        }
    }

    let events = match state.event_store.read_stream(&id).await {
        Ok(e) => e,
        Err(e) => return Err(map_comp_error(e)),
    };

    if events.is_empty() {
        return Err(map_comp_error(CompError::InstanceNotFound { id }));
    }

    Err(map_comp_error(CompError::InstanceClosed { id }))
}

// ── Replay handler ──

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

pub async fn replay_execution_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<ReplayQueryParams>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let detail = match tavern_comp::replay::DetailLevel::parse(&params.detail) {
        Ok(d) => d,
        Err(e) => return Err(map_comp_error(e)),
    };

    let opts = tavern_comp::replay::ReplayOptions {
        detail,
        from: params.from,
        to: params.to,
        step_id: params.step_id,
    };

    let replay =
        match tavern_comp::replay::ExecutionReplayer::replay(state.event_store.as_ref(), &id, opts)
            .await
        {
            Ok(r) => r,
            Err(e) => return Err(map_comp_error(e)),
        };

    Ok(Json(replay))
}

// ---------- Workflow management handlers ----------

pub async fn create_workflow_handler(
    State(state): State<Arc<AppState>>,
    Json(workflow): Json<tavern_comp::Workflow>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let mut registry = state.registry.write().await;
    match registry.register(workflow) {
        Ok(()) => Ok(StatusCode::CREATED),
        Err(e) => Err(map_comp_error(e)),
    }
}

pub async fn delete_workflow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let mut registry = state.registry.write().await;
    match registry.unregister(&id) {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err(map_comp_error(e)),
    }
}

pub async fn reload_workflows_handler(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let path = std::path::Path::new(&state.workflow_config_dir);

    // 先加载到临时 registry，成功后再原子替换，避免中间状态暴露空注册表
    let mut new_registry = tavern_comp::WorkflowRegistry::new();
    if path.exists() {
        if let Err(e) = new_registry.load_from_dir(path) {
            return Err(map_comp_error(e));
        }
    }
    let mut registry = state.registry.write().await;
    *registry = new_registry;
    Ok(StatusCode::NO_CONTENT)
}

// ── Helper ──

/// 将执行耗时计入对应的直方图桶。
fn record_duration_bucket(buckets: &[Arc<AtomicU64>; 7], ms: u64) {
    let idx = match ms {
        0..100 => 0,
        100..500 => 1,
        500..1000 => 2,
        1000..5000 => 3,
        5000..30000 => 4,
        30000..60000 => 5,
        _ => 6,
    };
    buckets[idx].fetch_add(1, Ordering::Relaxed);
}

// ---------- Metrics handler ----------

pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let executions = state.workflow_executions.load(Ordering::Relaxed);
    let failures = state.workflow_failures.load(Ordering::Relaxed);
    let duration_ms = state.workflow_duration_ms_total.load(Ordering::Relaxed);

    let b = &state.workflow_duration_buckets;
    let b0 = b[0].load(Ordering::Relaxed);
    let b1 = b[1].load(Ordering::Relaxed);
    let b2 = b[2].load(Ordering::Relaxed);
    let b3 = b[3].load(Ordering::Relaxed);
    let b4 = b[4].load(Ordering::Relaxed);
    let b5 = b[5].load(Ordering::Relaxed);
    let body = format!(
        "# HELP tavern_workflow_executions_total Total workflow executions\n\
         # TYPE tavern_workflow_executions_total counter\n\
         tavern_workflow_executions_total {}\n\n\
         # HELP tavern_workflow_execution_failures_total Total workflow execution failures\n\
         # TYPE tavern_workflow_execution_failures_total counter\n\
         tavern_workflow_execution_failures_total {}\n\n\
         # HELP tavern_workflow_execution_duration_ms_total Cumulative workflow execution duration in milliseconds\n\
         # TYPE tavern_workflow_execution_duration_ms_total counter\n\
         tavern_workflow_execution_duration_ms_total {}\n\n\
         # HELP tavern_workflow_execution_duration_seconds Histogram of workflow execution durations\n\
         # TYPE tavern_workflow_execution_duration_seconds histogram\n\
         tavern_workflow_execution_duration_seconds_bucket{{le=\"0.1\"}} {}\n\
         tavern_workflow_execution_duration_seconds_bucket{{le=\"0.5\"}} {}\n\
         tavern_workflow_execution_duration_seconds_bucket{{le=\"1\"}} {}\n\
         tavern_workflow_execution_duration_seconds_bucket{{le=\"5\"}} {}\n\
         tavern_workflow_execution_duration_seconds_bucket{{le=\"30\"}} {}\n\
         tavern_workflow_execution_duration_seconds_bucket{{le=\"60\"}} {}\n\
         tavern_workflow_execution_duration_seconds_bucket{{le=\"+Inf\"}} {}\n\
         tavern_workflow_execution_duration_seconds_count {}\n\
         tavern_workflow_execution_duration_seconds_sum {}\n",
        executions,
        failures,
        duration_ms,
        b0, b0 + b1, b0 + b1 + b2, b0 + b1 + b2 + b3,
        b0 + b1 + b2 + b3 + b4, b0 + b1 + b2 + b3 + b4 + b5,
        executions,
        executions,
        duration_ms as f64 / 1000.0,
    );

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

// ── Auth ──

#[derive(Serialize)]
pub struct TokenRefreshResponse {
    pub token: String,
    pub expires_in: u64,
}

/// 刷新 Bearer Token。
/// 仅在 auth.type = "bearer" 时可用；api_key 模式下返回 400。
pub async fn refresh_token_handler(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    if state.config.auth.auth_type != "bearer" {
        return Err((
            StatusCode::BAD_REQUEST,
            ApiError {
                error: "auth mode not supported".to_string(),
                message: "token refresh is only available with auth.type = 'bearer'".to_string(),
                status: StatusCode::BAD_REQUEST,
            },
        ));
    }

    let secret = &state.config.auth.jwt_secret;
    let secret = if secret.is_empty() {
        "tavern-default-secret"
    } else {
        secret
    };

    let expiration = chrono::Utc::now()
        .checked_add_signed(chrono::TimeDelta::hours(24))
        .unwrap()
        .timestamp() as usize;

    let claims = serde_json::json!({
        "sub": "tavern",
        "exp": expiration,
    });

    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError {
                error: "token generation failed".to_string(),
                message: e.to_string(),
                status: StatusCode::INTERNAL_SERVER_ERROR,
            },
        )
    })?;

    Ok(Json(TokenRefreshResponse {
        token,
        expires_in: 86400,
    }))
}

// ── V0.3.3: 断点 handlers ──

#[derive(Serialize)]
pub struct BreakpointItem {
    pub execution_id: String,
    pub step_id: String,
    pub reason: String,
    pub paused_at: Option<String>,
}

#[derive(Serialize)]
pub struct BreakpointListResponse {
    pub breakpoints: Vec<BreakpointItem>,
}

pub async fn list_breakpoints_handler(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let statuses = state
        .event_store
        .list_by_status(tavern_comp::InstanceStatus::WaitingForSignal {
            signal: String::new(),
        })
        .await
        .map_err(|e| map_comp_error(e))?;

    let mut breakpoints = Vec::new();
    for instance_id in statuses {
        let events = state
            .event_store
            .read_stream(&instance_id)
            .await
            .map_err(|e| map_comp_error(e))?;

        for event in &events {
            if let tavern_comp::WorkflowEvent::BreakpointHit {
                step_id,
                reason,
                paused_at,
            } = event
            {
                breakpoints.push(BreakpointItem {
                    execution_id: instance_id.clone(),
                    step_id: step_id.clone(),
                    reason: reason.clone(),
                    paused_at: Some(paused_at.to_rfc3339()),
                });
            }
        }
    }

    Ok(Json(BreakpointListResponse { breakpoints }))
}

// ── V0.3.2: 克隆 handler ──

#[derive(Serialize)]
pub struct CloneExecutionResponse {
    pub execution_id: String,
    pub cloned_from: String,
    pub workflow_id: String,
    pub inputs: Value,
}

pub async fn clone_execution_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let engine = tavern_comp::WorkflowEngine::new(state.hero.clone())
        .with_max_concurrency(state.max_concurrency)
        .with_store(state.event_store.clone());

    let info = engine
        .get_execution_info(&id)
        .await
        .map_err(|e| map_comp_error(e))?;

    // 仅允许克隆已完成或已失败的执行
    match &info.status {
        tavern_comp::InstanceStatus::Completed | tavern_comp::InstanceStatus::Failed => {}
        _ => {
            return Err((
                StatusCode::CONFLICT,
                ApiError::new(
                    StatusCode::CONFLICT,
                    "CannotClone",
                    "cannot clone a running or waiting execution".to_string(),
                ),
            ));
        }
    }

    let workflow = {
        let registry = state.registry.read().await;
        match registry.get(&info.workflow_id) {
            Some(w) => w.clone(),
            None => {
                return Err(map_comp_error(CompError::WorkflowNotFound {
                    id: info.workflow_id,
                }))
            }
        }
    };

    let mut handle = engine
        .start(&workflow, info.inputs.clone())
        .await
        .map_err(|e| map_comp_error(e))?;

    let new_id = handle.id().to_string();
    let signal_tx = handle.signal_tx.clone();

    // Register the new execution
    {
        let mut handles = state.execution_handles.write().await;
        handles.insert(new_id.clone(), signal_tx);
    }
    {
        let mut broadcasts = state.event_broadcasts.write().await;
        broadcasts.entry(new_id.clone()).or_insert_with(|| {
            tokio::sync::broadcast::channel::<tavern_comp::WorkflowEvent>(128).0
        });
    }

    // Spawn cleanup
    let cleanup_id = new_id.clone();
    let handles_arc = state.execution_handles.clone();
    let broadcasts_arc = state.event_broadcasts.clone();
    tokio::spawn(async move {
        let _ = handle.await_completion().await;
        handles_arc.write().await.remove(&cleanup_id);
        broadcasts_arc.write().await.remove(&cleanup_id);
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(CloneExecutionResponse {
            execution_id: new_id,
            cloned_from: id,
            workflow_id: info.workflow_id,
            inputs: info.inputs,
        }),
    ))
}

// ── V0.3.2: 审批 handlers ──

#[derive(Serialize)]
pub struct ApprovalItem {
    pub execution_id: String,
    pub workflow_id: String,
    pub step_id: String,
    pub signal_name: String,
    pub context: Value,
    pub step_output: Value,
    pub waited_since: Option<String>,
    pub timeout_at: Option<String>,
}

#[derive(Serialize)]
pub struct ApprovalListResponse {
    pub approvals: Vec<ApprovalItem>,
}

pub async fn list_approvals_handler(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let statuses = state
        .event_store
        .list_by_status(tavern_comp::InstanceStatus::WaitingForSignal {
            signal: String::new(),
        })
        .await
        .map_err(|e| map_comp_error(e))?;

    let mut approvals = Vec::new();
    for instance_id in statuses {
        let events = state
            .event_store
            .read_stream(&instance_id)
            .await
            .map_err(|e| map_comp_error(e))?;

        let mut workflow_id = String::new();
        let mut context = Value::Null;
        let mut step_id = String::new();
        let mut signal_name = String::new();
        let mut waited_since: Option<String> = None;
        let mut step_output = Value::Null;

        let mut instance_state = tavern_comp::InstanceState {
            id: instance_id.clone(),
            ..Default::default()
        };

        for event in &events {
            let _ = instance_state.apply(event);
            match event {
                tavern_comp::WorkflowEvent::InstanceCreated {
                    workflow_id: wid,
                    inputs: inp,
                } => {
                    workflow_id = wid.clone();
                    context = inp.clone();
                }
                tavern_comp::WorkflowEvent::SignalWaitStarted {
                    step_id: sid,
                    signal_name: sn,
                } => {
                    step_id = sid.clone();
                    signal_name = sn.clone();
                }
                tavern_comp::WorkflowEvent::StepCompleted {
                    step_id: sid,
                    output,
                    ..
                } if *sid == step_id => {
                    step_output = output.clone();
                }
                tavern_comp::WorkflowEvent::StepStarted {
                    step_id: sid,
                    started_at,
                } if *sid == step_id => {
                    waited_since = Some(started_at.to_rfc3339());
                }
                _ => {}
            }
        }

        // Merge step outputs into context
        if let Some(obj) = context.as_object_mut() {
            for (key, val) in instance_state.context.as_object().unwrap_or(&serde_json::Map::new())
            {
                if key != "signals" {
                    obj.insert(key.clone(), val.clone());
                }
            }
        }

        approvals.push(ApprovalItem {
            execution_id: instance_id,
            workflow_id,
            step_id,
            signal_name,
            context,
            step_output,
            waited_since,
            timeout_at: None,
        });
    }

    Ok(Json(ApprovalListResponse { approvals }))
}

#[derive(Deserialize)]
pub struct ApproveRequest {
    pub comment: Option<String>,
    pub reviewer: String,
}

#[derive(Deserialize)]
pub struct RejectRequest {
    pub reason: String,
    pub reviewer: String,
}

pub async fn approve_step_handler(
    State(state): State<Arc<AppState>>,
    Path((execution_id, step_id)): Path<(String, String)>,
    Json(body): Json<ApproveRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let payload = json!({"comment": body.comment});
    send_approval_signal(
        state,
        execution_id,
        step_id,
        SignalAction::Approve,
        body.reviewer,
        payload,
    )
    .await
}

pub async fn reject_step_handler(
    State(state): State<Arc<AppState>>,
    Path((execution_id, step_id)): Path<(String, String)>,
    Json(body): Json<RejectRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let payload = json!({"reason": body.reason});
    send_approval_signal(
        state,
        execution_id,
        step_id,
        SignalAction::Reject,
        body.reviewer,
        payload,
    )
    .await
}

async fn send_approval_signal(
    state: Arc<AppState>,
    execution_id: String,
    step_id: String,
    action: SignalAction,
    reviewer: String,
    payload: Value,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    // 从待审批步骤中查找 signal_name
    let events = state
        .event_store
        .read_stream(&execution_id)
        .await
        .map_err(|e| map_comp_error(e))?;

    if events.is_empty() {
        return Err(map_comp_error(CompError::InstanceNotFound {
            id: execution_id.clone(),
        }));
    }

    let signal_name = events
        .iter()
        .find_map(|e| match e {
            tavern_comp::WorkflowEvent::SignalWaitStarted {
                step_id: sid,
                signal_name: sn,
            } if *sid == step_id => Some(sn.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            (
                StatusCode::CONFLICT,
                ApiError::new(
                    StatusCode::CONFLICT,
                    "StepNotWaiting",
                    format!("step '{}' is not waiting for approval", step_id),
                ),
            )
        })?;

    let event = tavern_comp::WorkflowEvent::SignalReceived {
        signal_name,
        payload,
        received_at: Utc::now(),
        action: Some(action),
        reviewer: Some(reviewer),
    };

    // 尝试通过 execution_handles 发送（活跃实例）
    {
        let handles = state.execution_handles.read().await;
        if let Some(signal_tx) = handles.get(&execution_id) {
            signal_tx
                .send(event.clone())
                .await
                .map_err(|_| map_comp_error(CompError::InstanceClosed {
                    id: execution_id.clone(),
                }))?;
            return Ok(StatusCode::ACCEPTED);
        }
    }

    // Fallback：实例可能已从 checkpoint 恢复，直接 append 到 EventStore
    state
        .event_store
        .append(&execution_id, event)
        .await
        .map_err(|e| map_comp_error(e))?;

    Ok(StatusCode::ACCEPTED)
}

// ── V0.3.6: 定时调度 handler ──

pub async fn list_schedules_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let schedules = state.scheduler.list().await;
    Json(serde_json::json!({ "schedules": schedules }))
}

// ── Flow handlers ──

#[derive(Serialize)]
pub struct FlowListResponse {
    pub flows: Vec<tavern_flow::registry::FlowSummary>,
}

#[derive(Deserialize)]
pub struct StartFlowRequest {
    pub inputs: serde_json::Value,
}

#[derive(Serialize)]
pub struct StartFlowResponse {
    pub flow_id: String,
}

#[derive(Serialize)]
pub struct FlowStatusResponse {
    pub flow_id: String,
    pub flow_name: String,
    pub status: String,
    pub started_at: String,
}

pub async fn list_flows_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let flows = state.flow_registry.list().await;
    Json(FlowListResponse { flows })
}

const MAX_FLOW_INPUT_SIZE: usize = 1024 * 1024; // 1 MiB

pub async fn start_flow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<StartFlowRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    // Validate input size
    if let Ok(serialized) = serde_json::to_vec(&body.inputs) {
        if serialized.len() > MAX_FLOW_INPUT_SIZE {
            return Err((
                StatusCode::BAD_REQUEST,
                ApiError {
                    error: "InputTooLarge".to_string(),
                    message: format!(
                        "flow inputs exceed max size of {} bytes",
                        MAX_FLOW_INPUT_SIZE
                    ),
                    status: StatusCode::BAD_REQUEST,
                },
            ));
        }
    }

    let instance = state
        .flow_registry
        .create_instance(&id, body.inputs)
        .await
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                ApiError {
                    error: "flow not found".to_string(),
                    message: e.to_string(),
                    status: StatusCode::NOT_FOUND,
                },
            )
        })?;

    let (handle, ref_handle) = instance.start().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError {
                error: "flow start failed".to_string(),
                message: e.to_string(),
                status: StatusCode::INTERNAL_SERVER_ERROR,
            },
        )
    })?;

    let flow_id = ref_handle.flow_id.clone();
    let handles = state.flow_handles.clone();
    {
        let mut h = handles.write().await;
        h.insert(flow_id.clone(), ref_handle);
    }

    // Spawn cleanup
    let cleanup_id = flow_id.clone();
    tokio::spawn(async move {
        let mut handle = handle;
        let _ = handle.await_completion().await;
        handles.write().await.remove(&cleanup_id);
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(StartFlowResponse {
            flow_id: flow_id.clone(),
        }),
    ))
}

pub async fn get_flow_status_handler(
    State(state): State<Arc<AppState>>,
    Path(flow_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let handles = state.flow_handles.read().await;
    let ref_handle = handles.get(&flow_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            ApiError {
                error: "flow not found".to_string(),
                message: format!("flow '{}' not found", flow_id),
                status: StatusCode::NOT_FOUND,
            },
        )
    })?;

    Ok(Json(FlowStatusResponse {
        flow_id: ref_handle.flow_id.clone(),
        flow_name: ref_handle.flow_name.clone(),
        status: ref_handle.status_str().to_string(),
        started_at: ref_handle.started_at.to_rfc3339(),
    }))
}

pub async fn cancel_flow_handler(
    State(state): State<Arc<AppState>>,
    Path(flow_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    let handles = state.flow_handles.read().await;
    match handles.get(&flow_id) {
        Some(ref_handle) => {
            ref_handle.cancel();
            tracing::info!(flow_id = %flow_id, "flow cancellation requested");
            Ok(StatusCode::ACCEPTED)
        }
        None => Err((
            StatusCode::NOT_FOUND,
            ApiError {
                error: "flow not found".to_string(),
                message: format!("flow '{}' not found", flow_id),
                status: StatusCode::NOT_FOUND,
            },
        )),
    }
}
