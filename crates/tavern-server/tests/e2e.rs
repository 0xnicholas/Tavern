use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{json, Value};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

use tavern_adapters::MockRuntime;
use tavern_core::Runtime;
use tavern_hero::TavernHero;
use tavern_server::{router, state::AppState};

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
                agent_id: "writer".to_string(),
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
                agent_id: "editor".to_string(),
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

async fn create_test_app_with_workflow<F>(
    handler: F,
    workflow: tavern_comp::Workflow,
) -> axum::Router
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
    std::fs::write(
        dir.path().join("writer.yaml"),
        r#"
id: writer
name: 作家
model:
  provider: openai
  name: gpt-4o
instructions: 写作
"#,
    )
    .unwrap();
    std::fs::write(
        dir.path().join("editor.yaml"),
        r#"
id: editor
name: 编辑
model:
  provider: openai
  name: gpt-4o
instructions: 编辑
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
                workflow_duration_buckets: [Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0))],
        max_concurrency: usize::MAX,
        event_store: Arc::new(tavern_comp::MemoryEventStore::new()),
        execution_handles: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        event_broadcasts: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        flow_registry: Arc::new(tavern_flow::FlowRegistry::new()),
        flow_handles: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        config: tavern_config::TavernConfig::default(),
    }))
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

async fn response_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// 端到端完整用户旅程测试。
#[tokio::test]
async fn test_end_to_end_full_journey() {
    let app = create_test_app().await;

    // 1. Health check
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["status"], "ok");
    assert_eq!(json["version"], "0.1.0");

    // 2. List agents
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/agents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let agents: Vec<Value> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(agents.len(), 3);
    let ids: Vec<String> = agents
        .iter()
        .map(|a| a["id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"researcher".to_string()));
    assert!(ids.contains(&"writer".to_string()));
    assert!(ids.contains(&"editor".to_string()));

    // 3. Get specific agent
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/agents/researcher")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let agent = response_json(response).await;
    assert_eq!(agent["id"], "researcher");

    // 4. Execute agent directly
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/agents/researcher/execute")
                .header("content-type", "application/json")
                .body(Body::from(json!({"task": "research AI"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result = response_json(response).await;
    assert_eq!(result, "research notes");

    // 5. List workflows
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/workflows")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 6. Get workflow
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/workflows/content_pipeline")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let wf = response_json(response).await;
    assert_eq!(wf["id"], "content_pipeline");

    // 7. Run workflow synchronously
    let response = app
        .clone()
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
    let run_result = response_json(response).await;
    assert_eq!(run_result["context"]["topic"], "AI");
    assert_eq!(run_result["context"]["notes"], "research notes");
    assert_eq!(run_result["outputs"]["article"], "final article");

    // 8. Start workflow asynchronously
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/content_pipeline/start")
                .header("content-type", "application/json")
                .body(Body::from(json!({"topic": "Rust"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let start_result = response_json(response).await;
    let execution_id = start_result["execution_id"].as_str().unwrap().to_string();
    assert!(!execution_id.is_empty());

    // 9. Poll execution until completed
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
        let exec = response_json(response).await;
        status = exec["status"].as_str().unwrap().to_string();
        if status == "completed" || status == "failed" {
            break;
        }
    }
    assert_eq!(status, "completed");

    // 10. Get execution events
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/executions/{}/events", execution_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let events: Vec<Value> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        events.len() >= 2,
        "expected at least 2 events, got {}",
        events.len()
    );

    // 11. Metrics endpoint
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let metrics_body = String::from_utf8(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(metrics_body.contains("tavern_workflow_executions_total"));
    assert!(metrics_body.contains("tavern_workflow_execution_duration_ms_total"));
}

/// 测试带信号的工作流完整生命周期。
#[tokio::test]
async fn test_end_to_end_signal_workflow() {
    let signal_wf = tavern_comp::Workflow {
        id: "approval_flow".to_string(),
        name: "Approval Flow".to_string(),
        description: None,
        steps: vec![tavern_comp::Step {
            id: "s1".to_string(),
            agent_id: "researcher".to_string(),
            task: "prepare report".to_string(),
            depends_on: vec![],
            output_key: Some("report".to_string()),
            timeout: None,
            retries: None,
            retry_delay: None,
            wait_for_signal: Some("approve".to_string()),
            signal_timeout: None,
            expected_output: None,
        }],
        inputs: vec![],
        outputs: vec![tavern_comp::OutputDef {
            name: "report".to_string(),
            value: "{{report}}".to_string(),
        }],
        process: tavern_comp::Process::Sequential,
        planning: None,
    };

    let app = create_test_app_with_workflow(
        |_agent_id, _task, _context, _sp, _model| Ok(json!("report content")),
        signal_wf,
    )
    .await;

    // Start workflow
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/approval_flow/start")
                .header("content-type", "application/json")
                .body(Body::from(json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let result = response_json(response).await;
    let execution_id = result["execution_id"].as_str().unwrap().to_string();

    // Poll until waiting_for_signal
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
        let exec = response_json(response).await;
        status = exec["status"].as_str().unwrap().to_string();
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
        let exec = response_json(response).await;
        status = exec["status"].as_str().unwrap().to_string();
        if status == "completed" || status == "failed" {
            break;
        }
    }
    assert_eq!(status, "completed");
}

/// 测试工作流 CRUD 生命周期。
#[tokio::test]
async fn test_end_to_end_workflow_crud() {
    let app = create_test_app().await;

    // Create workflow
    let new_wf = json!({
        "id": "crud_test",
        "name": "CRUD Test",
        "steps": [
            {
                "id": "s1",
                "agent_id": "researcher",
                "task": "do something"
            }
        ],
        "inputs": [],
        "outputs": []
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(new_wf.to_string()))
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
                .uri("/workflows/crud_test")
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
                .uri("/workflows/crud_test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify it no longer exists
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/workflows/crud_test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ── SSE & Auth tests ──

async fn create_test_app_with_auth(auth_type: &str, keys: Vec<&str>) -> axum::Router {
    let runtime: Arc<dyn Runtime> = Arc::new(MockRuntime::new(
        |_agent_id, task, _context, _sp, _model| {
            Ok(match task {
                t if t.starts_with("research") => json!("research notes"),
                t if t.starts_with("write") => json!("draft article"),
                _ => json!("final article"),
            })
        },
    ));
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
    std::fs::write(
        dir.path().join("writer.yaml"),
        r#"
id: writer
name: 作家
model:
  provider: openai
  name: gpt-4o
instructions: 写作
"#,
    )
    .unwrap();
    std::fs::write(
        dir.path().join("editor.yaml"),
        r#"
id: editor
name: 编辑
model:
  provider: openai
  name: gpt-4o
instructions: 编辑
"#,
    )
    .unwrap();
    hero.load_from_dir(dir.path()).await.unwrap();
    let hero = Arc::new(hero);

    let mut registry = tavern_comp::WorkflowRegistry::new();
    registry.register(default_workflow()).unwrap();
    let registry = Arc::new(tokio::sync::RwLock::new(registry));

    let mut config = tavern_config::TavernConfig::default();
    config.auth.auth_type = auth_type.to_string();
    config.auth.keys = keys.iter().map(|k| k.to_string()).collect();

    router::create_router(Arc::new(AppState {
        hero,
        registry,
        workflow_config_dir: "./configs/workflows".to_string(),
        workflow_executions: Arc::new(AtomicU64::new(0)),
        workflow_failures: Arc::new(AtomicU64::new(0)),
        workflow_duration_ms_total: Arc::new(AtomicU64::new(0)),
                workflow_duration_buckets: [Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0))],
        max_concurrency: usize::MAX,
        event_store: Arc::new(tavern_comp::MemoryEventStore::new()),
        execution_handles: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        event_broadcasts: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        flow_registry: Arc::new(tavern_flow::FlowRegistry::new()),
        flow_handles: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        config,
    }))
}

/// SSE endpoint returns 200 and correct content-type.
#[tokio::test]
async fn test_sse_events_stream_endpoint_accessible() {
    let app = create_test_app().await;

    // First start a workflow to get an execution_id
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/content_pipeline/start")
                .header("content-type", "application/json")
                .body(Body::from(json!({"topic": "test"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let result = response_json(response).await;
    let execution_id = result["execution_id"].as_str().unwrap().to_string();

    // Connect to SSE endpoint
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/executions/{}/events/stream", execution_id))
                .header("accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/event-stream"),
        "expected text/event-stream, got {}",
        content_type
    );
}

/// SSE endpoint returns 401 when auth is enabled and no api_key provided.
#[tokio::test]
async fn test_sse_events_stream_requires_auth() {
    let app = create_test_app_with_auth("api_key", vec!["sk-test-123"]).await;

    // Start workflow
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/content_pipeline/start")
                .header("content-type", "application/json")
                .header("x-api-key", "sk-test-123")
                .body(Body::from(json!({"topic": "test"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body_text = String::from_utf8(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "expected 202, got {}: {}",
        status,
        body_text
    );
    let result: Value = serde_json::from_str(&body_text).unwrap();
    let execution_id = result["execution_id"].as_str().unwrap().to_string();

    // Without api_key → 401
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/executions/{}/events/stream", execution_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // With valid api_key → 200
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/executions/{}/events/stream?api_key=sk-test-123",
                    execution_id
                ))
                .header("accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Protected endpoints require auth when api_key mode is enabled.
#[tokio::test]
async fn test_auth_middleware_blocks_unauthenticated() {
    let app = create_test_app_with_auth("api_key", vec!["sk-test-456"]).await;

    // Health is public
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Agents requires auth
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/agents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // With valid key → 200
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/agents")
                .header("x-api-key", "sk-test-456")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // With invalid key → 401
    let response = app
        .oneshot(
            Request::builder()
                .uri("/agents")
                .header("x-api-key", "wrong-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Workflow reload endpoint works.
#[tokio::test]
async fn test_reload_workflows_endpoint() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/reload")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// POST /workflows/:id/run returns 404 for missing workflow.
#[tokio::test]
async fn test_run_workflow_not_found() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/nonexistent_workflow/run")
                .header("content-type", "application/json")
                .body(Body::from(json!({"topic": "test"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
