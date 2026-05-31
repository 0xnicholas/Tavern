# V0.3.4 Implementation Plan

> **For agentic workers:** 单 track，全 server 层。

**Goal:** `POST /workflows/:id/run_batch` — 并行批量执行 Workflow。

**Architecture:** handler 中 engine.start() + JoinSet + Semaphore，聚合结果返回。

**Spec:** `docs/specs/spec-v0.3.4-batch-execution.md`（FROZEN）

**Tech Stack:** 无新增依赖

---

## File Structure

| File | Action | Content |
|------|--------|---------|
| `crates/tavern-server/src/handlers.rs` | Modify | `BatchRunRequest`, `BatchResponse`, `run_workflow_batch_handler` |
| `crates/tavern-server/src/router.rs` | Modify | `POST /workflows/:id/run_batch` |
| `crates/tavern-server/src/main.rs` | Modify | 集成测试 |

---

## Task 1: Handler

**Files:** `crates/tavern-server/src/handlers.rs`

- [ ] **Step 1: 定义常量**

```rust
const MAX_BATCH_SIZE: usize = 100;
const MAX_BATCH_CONCURRENCY: usize = 50;
```

- [ ] **Step 2: 定义请求/响应类型**

```rust
#[derive(Deserialize)]
struct BatchRunRequest {
    inputs: Vec<Value>,
    #[serde(default = "default_batch_concurrency")]
    max_concurrency: u32,
}
fn default_batch_concurrency() -> u32 { 10 }

#[derive(Serialize)]
struct BatchRunResponse {
    workflow_id: String,
    total: usize,
    succeeded: usize,
    failed: usize,
    results: Vec<BatchResultItem>,
}

#[derive(Serialize)]
struct BatchResultItem {
    index: usize,
    execution_id: String,
    status: String,
    inputs: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    outputs: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    duration_ms: u64,
}
```

- [ ] **Step 3: 实现 handler**

插入到 `run_workflow_handler` 附近：

```rust
pub async fn run_workflow_batch_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<BatchRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    // 1. Validate
    if req.inputs.is_empty() || req.inputs.len() > MAX_BATCH_SIZE {
        return Err((StatusCode::BAD_REQUEST, ApiError::new(..., "batch size must be 1-100")));
    }
    if req.max_concurrency == 0 || req.max_concurrency as usize > MAX_BATCH_CONCURRENCY {
        return Err((StatusCode::BAD_REQUEST, ApiError::new(..., "max_concurrency must be 1-50")));
    }

    // 2. Lookup workflow
    let workflow = {
        let registry = state.registry.read().await;
        registry.get(&id).cloned()
            .ok_or_else(|| map_comp_error(CompError::WorkflowNotFound { id: id.clone() }))?
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
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        join_set.spawn(async move {
            let _permit = permit;
            let start = Instant::now();
            let result = engine.run(&workflow, inputs).await;
            let duration_ms = start.elapsed().as_millis() as u64;
            (i, result, duration_ms)
        });
    }

    // 4. Collect results
    let mut items: Vec<BatchResultItem> = Vec::new();
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    let mut total_duration_ms = 0u64;

    while let Some(Ok((i, result, duration_ms))) = join_set.join_next().await {
        total_duration_ms += duration_ms;
        match result {
            Ok(r) => {
                succeeded += 1;
                items.push(BatchResultItem {
                    index: i,
                    execution_id: r.step_results.keys().next().cloned().unwrap_or_default(),
                    status: "completed".into(),
                    inputs: Value::Null, // inputs captured in item
                    outputs: Some(r.outputs),
                    error: None,
                    duration_ms,
                });
            }
            Err(e) => {
                failed += 1;
                items.push(BatchResultItem {
                    index: i,
                    execution_id: String::new(),
                    status: "failed".into(),
                    inputs: Value::Null,
                    outputs: None,
                    error: Some(e.to_string()),
                    duration_ms,
                });
            }
        }
    }
    items.sort_by_key(|r| r.index);

    // 5. Update metrics
    state.workflow_executions.fetch_add(total as u64, Ordering::Relaxed);
    state.workflow_failures.fetch_add(failed as u64, Ordering::Relaxed);
    state.workflow_duration_ms_total.fetch_add(total_duration_ms, Ordering::Relaxed);

    Ok(Json(BatchRunResponse {
        workflow_id: id,
        total,
        succeeded,
        failed,
        results: items,
    }))
}
```

> 注：需要额外保存 inputs 到 BatchResultItem。可以用 `Arc<Vec<(usize, Value)>>` 或简单地在 spawn 闭包中 capture inputs.clone()。

- [ ] **Step 4: 添加 imports**

```rust
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use std::time::Instant;
use futures::future::join_all;  // 备选方案
```

---

## Task 2: Route

**Files:** `crates/tavern-server/src/router.rs`

- [ ] **Step 1: 注册路由**

在 workflow 路由组中添加：

```rust
.route("/workflows/:id/run_batch", post(handlers::run_workflow_batch_handler))
```

---

## Task 3: Tests

**Files:** `crates/tavern-server/src/main.rs`（`#[cfg(test)] mod tests`）

- [ ] **Step 1: 全部成功测试**

```rust
#[tokio::test]
async fn test_run_workflow_batch_all_success() {
    let app = create_test_app().await;
    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/workflows/content_pipeline/run_batch")
            .header("content-type", "application/json")
            .body(Body::from(json!({
                "inputs": [
                    {"topic": "AI"},
                    {"topic": "Rust"},
                    {"topic": "DB"}
                ]
            }).to_string()))
            .unwrap(),
    ).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_body(response).await;
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["total"], 3);
    assert_eq!(json["succeeded"], 3);
    assert_eq!(json["failed"], 0);
}
```

- [ ] **Step 2: 部分失败测试**

```rust
#[tokio::test]
async fn test_run_workflow_batch_partial_failure() {
    // Use a workflow with required input, omit it for one entry
    let app = create_test_app().await;
    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/workflows/content_pipeline/run_batch")
            .header("content-type", "application/json")
            .body(Body::from(json!({
                "inputs": [
                    {"topic": "AI"},
                    {}  // missing required "topic"
                ]
            }).to_string()))
            .unwrap(),
    ).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_body(response).await;
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["total"], 2);
    assert_eq!(json["succeeded"], 1);
    assert_eq!(json["failed"], 1);
}
```

- [ ] **Step 3: 校验测试**

```rust
#[tokio::test]
async fn test_run_workflow_batch_empty_inputs_400() { /* inputs: [] → 400 */ }
#[tokio::test]
async fn test_run_workflow_batch_too_large_400() { /* 101 inputs → 400 */ }
#[tokio::test]
async fn test_run_workflow_batch_invalid_concurrency_400() { /* max_concurrency: 0 → 400 */ }
#[tokio::test]
async fn test_run_workflow_batch_not_found_404() { /* /unknown/run_batch → 404 */ }
```

---

## Task 4: 修复 BatchResultItem 的 inputs 字段

目前 handler 伪代码中 `inputs: Value::Null`，但在 spawn 闭包内可以 capture `inputs.clone()`。修正：

```rust
for (i, inputs) in req.inputs.into_iter().enumerate() {
    let inputs_for_result = inputs.clone();  // 保存到结果中
    // ... spawn ...
    items.push(BatchResultItem {
        inputs: inputs_for_result,
        // ...
    });
}
```

---

## 最终验证

- [ ] `cargo check --workspace` — 0 errors
- [ ] `cargo test --workspace` — 全部通过（194 + 新增 ~6）
- [ ] `cargo clippy --workspace` — 无新增 warning
- [ ] 手动 e2e：3 组 inputs 全部成功
- [ ] 手动 e2e：1 组失败其余成功
