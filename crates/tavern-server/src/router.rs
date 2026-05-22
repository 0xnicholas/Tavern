use axum::{
    extract::DefaultBodyLimit,
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

const MAX_BODY_SIZE: usize = 1024 * 1024; // 1 MiB

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(handlers::health_handler))
        .route("/agents", get(handlers::list_agents_handler))
        .route("/agents/:id", get(handlers::get_agent_handler))
        .route("/agents/:id/execute", post(handlers::execute_agent_handler))
        .route("/workflows", get(handlers::list_workflows_handler))
        .route("/workflows", post(handlers::create_workflow_handler))
        .route("/workflows/:id", get(handlers::get_workflow_handler))
        .route("/workflows/:id", delete(handlers::delete_workflow_handler))
        .route("/workflows/:id/run", post(handlers::run_workflow_handler))
        .route("/workflows/:id/start", post(handlers::start_workflow_handler))
        .route("/workflows/reload", post(handlers::reload_workflows_handler))
        .route("/executions/:id", get(handlers::get_execution_handler))
        .route("/executions/:id/events", get(handlers::get_execution_events_handler))
        .route("/executions/:id/signal", post(handlers::signal_execution_handler))
        .route("/executions/:id/cancel", post(handlers::cancel_execution_handler))
        .route("/metrics", get(handlers::metrics_handler))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
