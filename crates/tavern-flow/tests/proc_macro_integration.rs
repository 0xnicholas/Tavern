//! Smoke test: proc-macro generates FlowStepExecutor + __workflow_definition correctly.

use tavern_flow::{Flow, FlowError, flow_impl};
use tavern_comp::FlowStepExecutor;
use serde_json::Value;

// ── Test 1: Simple linear pipeline ──

#[derive(Flow)]
struct LinearPipeline {
    value: String,
}

#[flow_impl(crate = "tavern_flow")]
impl LinearPipeline {
    #[start]
    async fn step_a(&mut self) -> Result<String, FlowError> {
        self.value = "from_a".to_string();
        Ok("result_a".to_string())
    }

    #[listen("step_a")]
    async fn step_b(&mut self, data: String) -> Result<String, FlowError> {
        Ok(format!("got: {}", data))
    }
}

#[test]
fn test_linear_workflow_definition() {
    let wf = LinearPipeline::__workflow_definition();
    assert_eq!(wf.id, "LinearPipeline");
    assert_eq!(wf.steps.len(), 2);
    assert_eq!(wf.steps[0].id, "step_a");
    assert!(wf.steps[0].depends_on.is_empty());
    assert!(wf.steps[0].or_depends_on.is_empty());
    assert_eq!(wf.steps[0].output_key.as_deref(), Some("step_a"));
    assert_eq!(wf.steps[0].agent_id, tavern_comp::FLOW_AGENT_ID);

    assert_eq!(wf.steps[1].id, "step_b");
    assert!(wf.steps[1].depends_on.is_empty());
    assert_eq!(wf.steps[1].or_depends_on, vec!["step_a"]);
    assert_eq!(wf.steps[1].output_key.as_deref(), Some("step_b"));
}

#[tokio::test]
async fn test_linear_dispatch() {
    let mut pipeline = LinearPipeline { value: String::new() };
    let result = pipeline
        .execute_step("step_a", Value::Null)
        .await
        .expect("step_a should succeed");
    assert_eq!(result, Value::String("result_a".to_string()));

    let result = pipeline
        .execute_step("step_b", result)
        .await
        .expect("step_b should succeed");
    assert_eq!(result, Value::String("got: result_a".to_string()));
}

#[tokio::test]
async fn test_linear_run() {
    let pipeline = LinearPipeline { value: String::new() };
    let result = pipeline
        .run(serde_json::json!({}))
        .await
        .expect("run should succeed");
    // When no explicit outputs defined, result is the outputs object (may be empty)
    // The step completed — verified by no error
    assert!(result.is_object());
}

// ── Test 2: Router pipeline ──

#[derive(Flow)]
struct RouterPipeline {
    approved: bool,
}

#[flow_impl(crate = "tavern_flow")]
impl RouterPipeline {
    #[start]
    async fn process(&mut self) -> Result<String, FlowError> {
        Ok("draft_content".to_string())
    }

    #[router("process")]
    async fn gate(&mut self, content: String) -> String {
        if content.len() > 5 {
            self.approved = true;
            "approved".to_string()
        } else {
            "rejected".to_string()
        }
    }

    #[listen("approved")]
    async fn on_approved(&mut self, data: String) -> Result<String, FlowError> {
        Ok(format!("published: {}", data))
    }
}

#[test]
fn test_router_workflow_definition() {
    let wf = RouterPipeline::__workflow_definition();
    assert_eq!(wf.steps.len(), 3);

    // Router step
    let router_step = &wf.steps[1];
    assert_eq!(router_step.id, "__router__gate");
    assert_eq!(router_step.depends_on, vec!["process"]);
    assert!(router_step.router.is_some());
    assert_eq!(router_step.router.as_ref().unwrap().upstream, "process");
    assert!(router_step.output_key.is_none());

    // Label listener
    let listener_step = &wf.steps[2];
    assert_eq!(listener_step.id, "on_approved");
    assert_eq!(listener_step.or_depends_on, vec!["__label__approved"]);
    assert!(listener_step.depends_on.is_empty());
}

#[tokio::test]
async fn test_router_dispatch() {
    let mut pipeline = RouterPipeline { approved: false };
    let result = pipeline
        .execute_step("process", Value::Null)
        .await
        .expect("process should succeed");
    assert_eq!(result, Value::String("draft_content".to_string()));

    let result = pipeline
        .execute_step("__router__gate", result)
        .await
        .expect("gate should succeed");
    assert_eq!(result, Value::String("approved".to_string()));
    assert!(pipeline.approved);
}

#[tokio::test]
async fn test_router_run() {
    let pipeline = RouterPipeline { approved: false };
    let result = pipeline.run(serde_json::json!({})).await.expect("run should succeed");
    // Router flow completed successfully — verified by no error
    assert!(result.is_object());
    // Note: pipeline moved into run(), cannot check state after
}

// ── Test 3: OR combinator ──

#[derive(Flow)]
struct OrPipeline {
    executed: Vec<String>,
}

#[flow_impl(crate = "tavern_flow")]
impl OrPipeline {
    #[start]
    async fn source_a(&mut self) -> Result<String, FlowError> {
        self.executed.push("a".into());
        Ok("result_a".to_string())
    }

    #[start]
    async fn source_b(&mut self) -> Result<String, FlowError> {
        self.executed.push("b".into());
        Ok("result_b".to_string())
    }

    #[listen(or("source_a", "source_b"))]
    async fn consumer(&mut self, data: String) -> Result<String, FlowError> {
        self.executed.push(format!("got:{}", data));
        Ok(format!("final:{}", data))
    }
}

#[test]
fn test_or_workflow_definition() {
    let wf = OrPipeline::__workflow_definition();
    let consumer = &wf.steps[2];
    assert_eq!(consumer.id, "consumer");
    assert_eq!(consumer.or_depends_on, vec!["__label__source_a", "__label__source_b"]);
    assert!(consumer.depends_on.is_empty());
}

// ── Test 4: AND combinator ──

#[derive(Flow)]
struct AndPipeline {
    ready: bool,
}

#[flow_impl(crate = "tavern_flow")]
impl AndPipeline {
    #[start]
    async fn first(&mut self) -> Result<String, FlowError> {
        Ok("first".to_string())
    }

    #[start]
    async fn second(&mut self) -> Result<String, FlowError> {
        Ok("second".to_string())
    }

    #[listen(and("first", "second"))]
    async fn after_both(&mut self) -> Result<String, FlowError> {
        self.ready = true;
        Ok("done".to_string())
    }
}

#[test]
fn test_and_workflow_definition() {
    let wf = AndPipeline::__workflow_definition();
    let after = &wf.steps[2];
    assert_eq!(after.id, "after_both");
    assert_eq!(after.depends_on, vec!["first", "second"]);
    assert!(after.or_depends_on.is_empty());
}

// ── Test 5: FlowStepExecutor for unknown method ──

#[tokio::test]
async fn test_unknown_method_returns_error() {
    let mut pipeline = LinearPipeline { value: String::new() };
    let result = pipeline.execute_step("nonexistent", Value::Null).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("method not found"));
}
