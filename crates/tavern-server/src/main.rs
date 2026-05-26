use notify::Watcher;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use tavern_core::Runtime;
use tavern_hero::TavernHero;
use tracing::info;

mod auth;
mod handlers;
mod router;
mod shutdown;
mod sse;
mod state;

#[tokio::main]
async fn main() {
    let config = match tavern_config::TavernConfig::load() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    // Initialize tracing based on log_format
    let log_format = config.observability.log_format.clone();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    match log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .init();
        }
        _ => {
            tracing_subscriber::fmt().with_env_filter(env_filter).init();
        }
    }

    let runtime_url = if config.runtime.url.is_empty() {
        tracing::error!("runtime.url is required (set RUNTIME_URL or TAVERN_RUNTIME_URL)");
        std::process::exit(1);
    } else {
        config.runtime.url.clone()
    };

    let runtime: Arc<dyn Runtime> = Arc::new(
        tavern_adapters::PandariaRuntime::new(runtime_url).expect("failed to build HTTP client"),
    );

    let hero = TavernHero::new(runtime);
    let agent_config_path = Path::new(&config.server.agent_config_dir);
    if agent_config_path.exists() {
        if let Err(e) = hero.load_from_dir(agent_config_path).await {
            tracing::error!("failed to load agent configs: {}", e);
        }
    }

    let mut registry = tavern_comp::WorkflowRegistry::new();
    let workflow_config_path = Path::new(&config.server.workflow_config_dir);
    if workflow_config_path.exists() {
        if let Err(e) = registry.load_from_dir(workflow_config_path) {
            tracing::error!("failed to load workflow configs: {}", e);
        }
    }

    let hero = Arc::new(hero);
    let registry = Arc::new(tokio::sync::RwLock::new(registry));

    // 启动 Workflow 配置文件监听（自动热重载）
    if config.reload.enabled {
        let watch_path = workflow_config_path.to_path_buf();
        let watch_registry = registry.clone();
        let debounce_ms = config.reload.debounce_ms;
        tokio::spawn(async move {
            start_workflow_watcher(watch_path, watch_registry, debounce_ms).await;
        });
    }

    // 根据配置创建 EventStore
    let event_store: Arc<dyn tavern_comp::EventStore> = match config.store.store_type.as_str() {
        "sqlite" => {
            let store = tavern_comp::SqliteEventStore::new(&config.store.database_url)
                .await
                .expect("failed to initialize SQLite event store");
            Arc::new(store)
        }
        _ => Arc::new(tavern_comp::MemoryEventStore::new()),
    };

    let app = router::create_router(Arc::new(state::AppState {
        hero,
        registry,
        workflow_config_dir: config.server.workflow_config_dir.clone(),
        workflow_executions: Arc::new(AtomicU64::new(0)),
        workflow_failures: Arc::new(AtomicU64::new(0)),
        workflow_duration_ms_total: Arc::new(AtomicU64::new(0)),
        max_concurrency: config.server.max_workflow_concurrency,
        event_store,
        execution_handles: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        config: config.clone(),
    }));

    let addr: SocketAddr = match format!("{}:{}", config.server.host, config.server.port).parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("invalid server address: {}", e);
            std::process::exit(1);
        }
    };

    info!("Tavern server listening on {}", addr);

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("failed to bind to {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::graceful_shutdown_listener())
        .await
        .unwrap();
}

/// 监听 Workflow 配置目录，文件变更时自动重载。
async fn start_workflow_watcher(
    path: std::path::PathBuf,
    registry: Arc<tokio::sync::RwLock<tavern_comp::WorkflowRegistry>>,
    debounce_ms: u64,
) {
    if !path.exists() {
        return;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<notify::Event, notify::Error>>(100);

    let mut watcher = match notify::RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| {
            let _ = tx.blocking_send(res);
        },
        notify::Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("failed to create workflow watcher: {}", e);
            return;
        }
    };

    if let Err(e) = watcher.watch(&path, notify::RecursiveMode::NonRecursive) {
        tracing::error!("failed to watch workflow directory: {}", e);
        return;
    }

    loop {
        let res = match rx.recv().await {
            Some(r) => r,
            None => break,
        };

        if let Ok(event) = res {
            match event.kind {
                notify::EventKind::Create(_)
                | notify::EventKind::Modify(_)
                | notify::EventKind::Remove(_) => {
                    // Debounce：收到事件后等待 500ms，期间有新事件则继续等待
                    loop {
                        match tokio::time::timeout(
                            tokio::time::Duration::from_millis(debounce_ms),
                            rx.recv(),
                        )
                        .await
                        {
                            Ok(Some(_)) => continue,
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }

                    let mut new_registry = tavern_comp::WorkflowRegistry::new();
                    if path.exists() {
                        if let Err(e) = new_registry.load_from_dir(&path) {
                            tracing::error!("workflow auto-reload failed: {}", e);
                        } else {
                            let mut reg = registry.write().await;
                            *reg = new_registry;
                            tracing::info!("workflows auto-reloaded from {:?}", path);
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tower::ServiceExt;

    use tavern_adapters::MockRuntime;
    use tavern_core::Runtime;
    use tavern_hero::TavernHero;

    use crate::router;
    use crate::state::AppState;

    fn default_workflow() -> tavern_comp::Workflow {
        tavern_comp::Workflow {
            id: "content_pipeline".to_string(),
            name: "Content Pipeline".to_string(),
            description: None,
            steps: vec![
                tavern_comp::Step {
                    id: "research".to_string(),
                    agent_id: "researcher".to_string(),
                    task: "research {{topic}}".to_string(),
                    depends_on: vec![],
                    output_key: Some("notes".to_string()),
                    timeout: None,
                    retries: None,
                    retry_delay: None,
                    wait_for_signal: None,
                    signal_timeout: None,
                    expected_output: None,
                },
                tavern_comp::Step {
                    id: "write".to_string(),
                    agent_id: "researcher".to_string(),
                    task: "write based on {{notes}}".to_string(),
                    depends_on: vec!["research".to_string()],
                    output_key: Some("draft".to_string()),
                    timeout: None,
                    retries: None,
                    retry_delay: None,
                    wait_for_signal: None,
                    signal_timeout: None,
                    expected_output: None,
                },
                tavern_comp::Step {
                    id: "edit".to_string(),
                    agent_id: "researcher".to_string(),
                    task: "edit {{draft}}".to_string(),
                    depends_on: vec!["write".to_string()],
                    output_key: Some("final".to_string()),
                    timeout: None,
                    retries: None,
                    retry_delay: None,
                    wait_for_signal: None,
                    signal_timeout: None,
                    expected_output: None,
                },
            ],
            inputs: vec![tavern_comp::InputDef {
                name: "topic".to_string(),
                required: true,
                default: None,
            }],
            outputs: vec![tavern_comp::OutputDef {
                name: "article".to_string(),
                value: "{{final}}".to_string(),
            }],
            process: tavern_comp::Process::Sequential,
            planning: None,
        }
    }

    async fn create_test_app() -> axum::Router {
        create_test_app_with_workflow(
            |_agent_id, task, _context, _sp, _model| {
                if task.starts_with("research") {
                    Ok(json!("research notes"))
                } else if task.starts_with("write") {
                    Ok(json!("draft article"))
                } else {
                    Ok(json!("final article"))
                }
            },
            default_workflow(),
        )
        .await
    }

    #[tokio::test]
    async fn test_health() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_agents() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/agents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_agent_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/agents/researcher")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_agent_not_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/agents/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_execute_agent_success() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/agents/researcher/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"task": "research"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_execute_agent_not_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/agents/unknown/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"task": "research"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ---------- Workflow API tests ----------

    #[tokio::test]
    async fn test_list_workflows() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/workflows")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_workflow_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/workflows/content_pipeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_workflow_not_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/workflows/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_run_workflow_success() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/content_pipeline/run")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"topic": "AI"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["context"]["topic"], "AI");
        assert_eq!(json["context"]["notes"], "research notes");
        assert_eq!(json["outputs"]["article"], "final article");
        assert!(json["step_results"].get("research").is_some());
        assert!(json["step_results"].get("write").is_some());
        assert!(json["step_results"].get("edit").is_some());
    }

    #[tokio::test]
    async fn test_run_workflow_not_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/unknown/run")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"topic": "AI"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_run_workflow_missing_input() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/content_pipeline/run")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    async fn create_test_app_with_workflow<F>(handler: F, workflow: tavern_comp::Workflow) -> axum::Router
    where
        F: Fn(&str, &str, Option<Value>, &str, &str) -> Result<Value, tavern_core::RuntimeError>
            + Send
            + Sync
            + 'static,
    {
        let runtime: Arc<dyn Runtime> = Arc::new(MockRuntime::new(handler));
        let hero = TavernHero::new(runtime);

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("agent.yaml"),
            r#"
id: researcher
name: 研究员
model:
  provider: openai
  name: gpt-4o
instructions: 研究
"#,
        )
        .unwrap();
        hero.load_from_dir(dir.path()).await.unwrap();
        let hero = Arc::new(hero);

        let mut registry = tavern_comp::WorkflowRegistry::new();
        registry.register(workflow).unwrap();
        let registry = Arc::new(tokio::sync::RwLock::new(registry));

        router::create_router(Arc::new(AppState {
            hero,
            registry,
            workflow_config_dir: "./configs/workflows".to_string(),
            workflow_executions: Arc::new(AtomicU64::new(0)),
            workflow_failures: Arc::new(AtomicU64::new(0)),
            workflow_duration_ms_total: Arc::new(AtomicU64::new(0)),
            max_concurrency: usize::MAX,
            event_store: Arc::new(tavern_comp::MemoryEventStore::new()),
            execution_handles: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            config: tavern_config::TavernConfig::default(),
        }))
    }

    #[tokio::test]
    async fn test_run_workflow_agent_not_found() {
        let wf = tavern_comp::Workflow {
            id: "bad_agent".to_string(),
            name: "Bad Agent".to_string(),
            description: None,
            steps: vec![tavern_comp::Step {
                id: "s1".to_string(),
                agent_id: "nonexistent".to_string(),
                task: "do something".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            }],
            inputs: vec![],
            outputs: vec![],
            process: tavern_comp::Process::Sequential,
            planning: None,
        };
        let app = create_test_app_with_workflow(|_, _, _, _, _| Ok(json!("ok")), wf).await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/bad_agent/run")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_run_workflow_step_failed() {
        let wf = tavern_comp::Workflow {
            id: "fail_flow".to_string(),
            name: "Fail Flow".to_string(),
            description: None,
            steps: vec![tavern_comp::Step {
                id: "s1".to_string(),
                agent_id: "researcher".to_string(),
                task: "fail".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            }],
            inputs: vec![],
            outputs: vec![],
            process: tavern_comp::Process::Sequential,
            planning: None,
        };
        let app = create_test_app_with_workflow(
            |_, _, _, _, _| {
                Err(tavern_core::RuntimeError::RequestFailed {
                    status: 500,
                    body: "boom".to_string(),
                })
            },
            wf,
        ).await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/fail_flow/run")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("tavern_workflow_executions_total"));
        assert!(text.contains("tavern_workflow_execution_failures_total"));
        assert!(text.contains("tavern_workflow_execution_duration_ms_total"));
    }

    #[tokio::test]
    async fn test_create_and_delete_workflow() {
        let app = create_test_app().await;
        let workflow_json = serde_json::json!({
            "id": "new_flow",
            "name": "New Flow",
            "steps": [
                {
                    "id": "s1",
                    "agent_id": "researcher",
                    "task": "do something"
                }
            ]
        });

        // Create
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows")
                    .header("content-type", "application/json")
                    .body(Body::from(workflow_json.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Verify it exists
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/workflows/new_flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/workflows/new_flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Verify it no longer exists
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/workflows/new_flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // NOTE: 循环依赖 Workflow 的场景已由 `tavern-comp` 的 `WorkflowRegistry::register`
    // 在注册时拦截（`validate_static` → `validate_dag`），server 层不会收到非法 Workflow。
    // 对应单元测试：`crates/tavern-comp/src/validator.rs::tests::test_dag_cycle`

    // ── V2 API tests ──

    #[tokio::test]
    async fn test_start_workflow_returns_202_and_execution_id() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/content_pipeline/start")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"topic": "AI"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["execution_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn test_get_execution_not_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/executions/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_execution_events_not_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/executions/nonexistent/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_signal_execution_not_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/executions/nonexistent/signal")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"signal_name": "approve", "payload": {}}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_cancel_execution_not_found() {
        let app = create_test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/executions/nonexistent/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_start_and_get_execution_flow() {
        use std::time::Duration;

        let app = create_test_app().await;

        // Start workflow
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/content_pipeline/start")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"topic": "AI"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let execution_id = json["execution_id"].as_str().unwrap().to_string();

        // Poll execution until completed (with timeout)
        let mut status = String::new();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/executions/{}", execution_id))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);

            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            status = json["status"].as_str().unwrap().to_string();
            if status == "completed" || status == "failed" {
                break;
            }
        }
        assert_eq!(status, "completed");

        // Get events
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/executions/{}/events", execution_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let events: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(events.as_array().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn test_signal_workflow_execution() {
        use std::time::Duration;

        let wf = tavern_comp::Workflow {
            id: "signal_flow".to_string(),
            name: "Signal Flow".to_string(),
            description: None,
            steps: vec![tavern_comp::Step {
                id: "s1".to_string(),
                agent_id: "researcher".to_string(),
                task: "do something".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: Some("approve".to_string()),
                signal_timeout: None,
                expected_output: None,
            }],
            inputs: vec![],
            outputs: vec![],
            process: tavern_comp::Process::Sequential,
            planning: None,
        };
        let app = create_test_app_with_workflow(|_, _, _, _, _| Ok(json!("done")), wf).await;

        // Start workflow
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/signal_flow/start")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let execution_id = json["execution_id"].as_str().unwrap().to_string();

        // Wait for step to enter signal wait
        let mut status = String::new();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/executions/{}", execution_id))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            status = json["status"].as_str().unwrap().to_string();
            if status == "waiting_for_signal" {
                break;
            }
        }
        assert_eq!(status, "waiting_for_signal");

        // Send signal
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/executions/{}/signal", execution_id))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"signal_name": "approve", "payload": {"by": "admin"}}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Poll until completed
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/executions/{}", execution_id))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            status = json["status"].as_str().unwrap().to_string();
            if status == "completed" || status == "failed" {
                break;
            }
        }
        assert_eq!(status, "completed");
    }

    #[tokio::test]
    async fn test_cancel_workflow_execution() {
        use std::time::Duration;
        use tavern_core::Runtime;

        struct SlowRuntime;
        #[async_trait::async_trait]
        impl Runtime for SlowRuntime {
            async fn execute(
                &self,
                _agent_id: &str,
                _task: &str,
                _context: Option<Value>,
                _system_prompt: &str,
                _model: &str,
            ) -> Result<Value, tavern_core::RuntimeError> {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(json!("done"))
            }
        }

        let runtime: Arc<dyn Runtime> = Arc::new(SlowRuntime);
        let hero = TavernHero::new(runtime);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("agent.yaml"),
            r#"
id: researcher
name: 研究员
model:
  provider: openai
  name: gpt-4o
instructions: 研究
"#,
        )
        .unwrap();
        hero.load_from_dir(dir.path()).await.unwrap();
        let hero = Arc::new(hero);

        let mut registry = tavern_comp::WorkflowRegistry::new();
        registry.register(default_workflow()).unwrap();
        let registry = Arc::new(tokio::sync::RwLock::new(registry));

        let app = router::create_router(Arc::new(AppState {
            hero,
            registry,
            workflow_config_dir: "./configs/workflows".to_string(),
            workflow_executions: Arc::new(AtomicU64::new(0)),
            workflow_failures: Arc::new(AtomicU64::new(0)),
            workflow_duration_ms_total: Arc::new(AtomicU64::new(0)),
            max_concurrency: usize::MAX,
            event_store: Arc::new(tavern_comp::MemoryEventStore::new()),
            execution_handles: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            config: tavern_config::TavernConfig::default(),
        }));

        // Start workflow
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/content_pipeline/start")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"topic": "AI"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let execution_id = json["execution_id"].as_str().unwrap().to_string();

        // Wait a bit for step to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/executions/{}/cancel", execution_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Poll until failed
        let mut status = String::new();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/executions/{}", execution_id))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            status = json["status"].as_str().unwrap().to_string();
            if status == "failed" {
                break;
            }
        }
        assert_eq!(status, "failed");
    }
}
