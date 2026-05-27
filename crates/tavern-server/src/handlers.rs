use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tavern_comp::CompError;
use tavern_core::RuntimeError;
use tavern_hero::TavernError;

use chrono::Utc;

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
            state
                .workflow_duration_ms_total
                .fetch_add(start.elapsed().as_millis() as u64, Ordering::Relaxed);
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

// ---------- Metrics handler ----------

pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let executions = state.workflow_executions.load(Ordering::Relaxed);
    let failures = state.workflow_failures.load(Ordering::Relaxed);
    let duration_ms = state.workflow_duration_ms_total.load(Ordering::Relaxed);

    let body = format!(
        "# HELP tavern_workflow_executions_total Total workflow executions\n\
         # TYPE tavern_workflow_executions_total counter\n\
         tavern_workflow_executions_total {}\n\n\
         # HELP tavern_workflow_execution_failures_total Total workflow execution failures\n\
         # TYPE tavern_workflow_execution_failures_total counter\n\
         tavern_workflow_execution_failures_total {}\n\n\
         # HELP tavern_workflow_execution_duration_ms_total Total workflow execution duration in milliseconds\n\
         # TYPE tavern_workflow_execution_duration_ms_total counter\n\
         tavern_workflow_execution_duration_ms_total {}\n",
        executions, failures, duration_ms
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

// ── Flow handlers ──

use tavern_flow::FlowFactory;

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

pub async fn start_flow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<StartFlowRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
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
    let _handles = state.flow_handles.read().await;
    // Cancel requires the handle which is owned by the spawned cleanup task.
    // For now, just check existence and return 202.
    if !_handles.contains_key(&flow_id) {
        return Err((
            StatusCode::NOT_FOUND,
            ApiError {
                error: "flow not found".to_string(),
                message: format!("flow '{}' not found", flow_id),
                status: StatusCode::NOT_FOUND,
            },
        ));
    }
    Ok(StatusCode::ACCEPTED)
}
