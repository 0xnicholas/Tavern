use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, post},
    Extension, Router,
};
use std::sync::Arc;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

const MAX_BODY_SIZE: usize = 1024 * 1024; // 1 MiB

pub fn create_router(state: Arc<AppState>) -> Router {
    let auth_type = state.config.auth.auth_type.clone();
    let metrics_public = state.config.observability.metrics_public;
    let auth_config = Arc::new(state.config.auth.clone());

    let mut public_routes = Router::new().route("/health", get(handlers::health_handler));

    let mut protected_routes = Router::new()
        .route("/agents", get(handlers::list_agents_handler))
        .route("/agents/:id", get(handlers::get_agent_handler))
        .route("/agents/:id/execute", post(handlers::execute_agent_handler))
        .route("/workflows", get(handlers::list_workflows_handler))
        .route("/workflows", post(handlers::create_workflow_handler))
        .route("/workflows/:id", get(handlers::get_workflow_handler))
        .route("/workflows/:id", delete(handlers::delete_workflow_handler))
        .route("/workflows/:id/run", post(handlers::run_workflow_handler))
        .route(
            "/workflows/:id/start",
            post(handlers::start_workflow_handler),
        )
        .route(
            "/workflows/reload",
            post(handlers::reload_workflows_handler),
        )
        .route("/executions/:id", get(handlers::get_execution_handler))
        .route(
            "/executions/:id/events",
            get(handlers::get_execution_events_handler),
        )
        .route(
            "/executions/:id/signal",
            post(handlers::signal_execution_handler),
        )
        .route(
            "/executions/:id/cancel",
            post(handlers::cancel_execution_handler),
        )
        .route(
            "/executions/:id/events/stream",
            get(crate::sse::execution_events_stream_handler),
        );

    if metrics_public {
        public_routes = public_routes.route("/metrics", get(handlers::metrics_handler));
    } else {
        protected_routes = protected_routes.route("/metrics", get(handlers::metrics_handler));
    }

    // Auth refresh (public, only works in bearer mode)
    public_routes = public_routes.route("/auth/refresh", post(handlers::refresh_token_handler));

    // Flow endpoints
    let mut flow_routes = Router::new()
        .route("/flows", get(handlers::list_flows_handler))
        .route("/flows/:id/start", post(handlers::start_flow_handler))
        .route("/flows/:id/status", get(handlers::get_flow_status_handler))
        .route("/flows/:id/cancel", post(handlers::cancel_flow_handler));

    if auth_type != "none" {
        flow_routes = flow_routes.layer(middleware::from_fn_with_state(
            auth_config.clone(),
            crate::auth::auth_middleware,
        ));
    }

    // Merge flow routes into protected
    protected_routes = protected_routes.merge(flow_routes);

    if auth_type != "none" {
        protected_routes = protected_routes.layer(middleware::from_fn_with_state(
            auth_config.clone(),
            crate::auth::auth_middleware,
        ));
    }

    public_routes
        .merge(protected_routes)
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
        .layer(Extension(auth_config))
}
