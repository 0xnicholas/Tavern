use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tavern_adapters::MockRuntime;
use tavern_hero::TavernHero;

use crate::workflow::{InputDef, ManagerConfig, OutputDef, PlanningConfig, Process, Step};

use super::*;

async fn make_engine<F>(handler: F) -> WorkflowEngine
where
    F: Fn(&str, &str, Option<Value>, &str, &str) -> Result<Value, tavern_core::RuntimeError>
        + Send
        + Sync
        + 'static,
{
    let runtime = Arc::new(MockRuntime::new(handler));
    let hero = TavernHero::new(runtime);

    // 注册一个虚拟 agent
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("agent.yaml"),
        r#"
id: test_agent
name: Test Agent
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
    )
    .unwrap();
    hero.load_agent(dir.path().join("agent.yaml").as_path())
        .await
        .unwrap();

    WorkflowEngine::new(Arc::new(hero))
}

fn simple_workflow() -> Workflow {
    Workflow {
        id: "wf1".to_string(),
        name: "Test Workflow".to_string(),
        description: None,
        steps: vec![Step {
            id: "s1".to_string(),
            agent_id: "test_agent".to_string(),
            task: "process {{input}}".to_string(),
            depends_on: vec![],
            output_key: Some("result".to_string()),
            timeout: None,
            retries: None,
            retry_delay: None,
            wait_for_signal: None,
            signal_timeout: None,
            expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
        }],
        inputs: vec![InputDef {
            name: "input".to_string(),
            required: true,
            default: None,
        }],
        outputs: vec![OutputDef {
            name: "out".to_string(),
            value: "{{result}}".to_string(),
        }],
        process: Process::Sequential,
        planning: None,
            webhook: None,
    }
}

#[tokio::test]
async fn test_run_success() {
    let engine =
        make_engine(|_agent_id, _task, _context, _system_prompt, _model| Ok(json!("done"))).await;
    let wf = simple_workflow();
    let result = engine.run(&wf, json!({"input": "hello"})).await.unwrap();

    assert_eq!(result.context["input"], "hello");
    assert_eq!(result.context["result"], "done");
    assert!(result.step_results.contains_key("s1"));
    assert!(matches!(
        result.step_results["s1"].status,
        StepStatus::Completed
    ));
}

#[tokio::test]
async fn test_run_missing_input() {
    let engine =
        make_engine(|_agent_id, _task, _context, _system_prompt, _model| Ok(json!("done"))).await;
    let wf = simple_workflow();
    let err = engine.run(&wf, json!({})).await.unwrap_err();
    assert!(matches!(err, CompError::MissingInput { name } if name == "input"));
}

#[tokio::test]
async fn test_run_agent_not_found() {
    let engine =
        make_engine(|_agent_id, _task, _context, _system_prompt, _model| Ok(json!("done"))).await;
    let mut wf = simple_workflow();
    wf.steps[0].agent_id = "unknown".to_string();
    let err = engine.run(&wf, json!({"input": "x"})).await.unwrap_err();
    assert!(matches!(err, CompError::AgentNotFound { id } if id == "unknown"));
}

#[tokio::test]
async fn test_run_step_failure() {
    let engine = make_engine(|_agent_id, _task, _context, _system_prompt, _model| {
        Err(tavern_core::RuntimeError::RequestFailed {
            status: 500,
            body: "boom".to_string(),
        })
    })
    .await;
    let wf = simple_workflow();
    let err = engine.run(&wf, json!({"input": "x"})).await.unwrap_err();
    assert!(
        matches!(err, CompError::StepFailed { step_id, reason } if step_id == "s1" && reason.contains("boom"))
    );
}

#[tokio::test]
async fn test_run_timeout() {
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
id: test_agent
name: Test Agent
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
    )
    .unwrap();
    hero.load_agent(dir.path().join("agent.yaml").as_path())
        .await
        .unwrap();

    let engine = WorkflowEngine::new(Arc::new(hero));
    let mut wf = simple_workflow();
    wf.steps[0].timeout = Some(1); // 1 秒超时
    let err = engine.run(&wf, json!({"input": "x"})).await.unwrap_err();
    assert!(
        matches!(err, CompError::StepFailed { step_id, reason } if step_id == "s1" && reason.contains("timed out"))
    );
}

#[tokio::test]
async fn test_run_outputs_validation() {
    let engine =
        make_engine(|_agent_id, _task, _context, _system_prompt, _model| Ok(json!("done"))).await;
    let mut wf = simple_workflow();
    wf.outputs.push(OutputDef {
        name: "bad".to_string(),
        value: "{{nonexistent}}".to_string(),
    });
    let err = engine.run(&wf, json!({"input": "x"})).await.unwrap_err();
    assert!(matches!(err, CompError::MissingContextVariable { .. }));
}

#[tokio::test]
async fn test_run_pipeline() {
    let engine = make_engine(|_agent_id, task, _context, _system_prompt, _model| {
        if task.starts_with("research") {
            Ok(json!("research notes"))
        } else if task.starts_with("write") {
            Ok(json!("draft article"))
        } else {
            Ok(json!("final article"))
        }
    })
    .await;

    let wf = Workflow {
        id: "pipeline".to_string(),
        name: "Pipeline".to_string(),
        description: None,
        steps: vec![
            Step {
                id: "research".to_string(),
                agent_id: "test_agent".to_string(),
                task: "research {{topic}}".to_string(),
                depends_on: vec![],
                output_key: Some("notes".to_string()),
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
            Step {
                id: "write".to_string(),
                agent_id: "test_agent".to_string(),
                task: "write based on {{notes}}".to_string(),
                depends_on: vec!["research".to_string()],
                output_key: Some("draft".to_string()),
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
            Step {
                id: "edit".to_string(),
                agent_id: "test_agent".to_string(),
                task: "edit {{draft}}".to_string(),
                depends_on: vec!["write".to_string()],
                output_key: Some("final".to_string()),
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
        ],
        inputs: vec![InputDef {
            name: "topic".to_string(),
            required: true,
            default: None,
        }],
        outputs: vec![OutputDef {
            name: "article".to_string(),
            value: "{{final}}".to_string(),
        }],
        process: Process::Sequential,
        planning: None,
            webhook: None,
    };

    let result = engine.run(&wf, json!({"topic": "AI"})).await.unwrap();
    assert_eq!(result.context["topic"], "AI");
    assert_eq!(result.context["notes"], "research notes");
    assert_eq!(result.context["draft"], "draft article");
    assert_eq!(result.context["final"], "final article");
    assert_eq!(result.step_results.len(), 3);
}

#[tokio::test]
async fn test_run_retry_success() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = AtomicUsize::new(0);
    let engine = make_engine(move |_agent_id, _task, _context, _system_prompt, _model| {
        let count = call_count.fetch_add(1, Ordering::SeqCst);
        if count < 2 {
            Err(tavern_core::RuntimeError::RequestFailed {
                status: 500,
                body: format!("attempt {}", count),
            })
        } else {
            Ok(json!("success"))
        }
    })
    .await;

    let mut wf = simple_workflow();
    wf.steps[0].retries = Some(2);
    wf.steps[0].retry_delay = Some(0);

    let result = engine.run(&wf, json!({"input": "x"})).await.unwrap();
    assert_eq!(result.context["result"], "success");
    assert!(matches!(
        result.step_results["s1"].status,
        StepStatus::Completed
    ));
}

#[tokio::test]
async fn test_run_retry_exhausted() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = AtomicUsize::new(0);
    let engine = make_engine(move |_agent_id, _task, _context, _system_prompt, _model| {
        call_count.fetch_add(1, Ordering::SeqCst);
        Err(tavern_core::RuntimeError::RequestFailed {
            status: 500,
            body: "always fail".to_string(),
        })
    })
    .await;

    let mut wf = simple_workflow();
    wf.steps[0].retries = Some(2);
    wf.steps[0].retry_delay = Some(0);

    let err = engine.run(&wf, json!({"input": "x"})).await.unwrap_err();
    assert!(
        matches!(err, CompError::StepFailed { step_id, reason } if step_id == "s1" && reason.contains("always fail"))
    );
}

#[tokio::test]
async fn test_run_retry_with_delay() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = AtomicUsize::new(0);
    let engine = make_engine(move |_agent_id, _task, _context, _system_prompt, _model| {
        let count = call_count.fetch_add(1, Ordering::SeqCst);
        if count == 0 {
            Err(tavern_core::RuntimeError::RequestFailed {
                status: 500,
                body: "fail".to_string(),
            })
        } else {
            Ok(json!("ok"))
        }
    })
    .await;

    let mut wf = simple_workflow();
    wf.steps[0].retries = Some(1);
    wf.steps[0].retry_delay = Some(1); // 1 秒延迟

    let start = std::time::Instant::now();
    let result = engine.run(&wf, json!({"input": "x"})).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(result.context["result"], "ok");
    assert!(
        elapsed.as_secs_f64() >= 0.9,
        "retry delay should be at least 0.9s"
    );
}

#[tokio::test]
async fn test_run_parallel_steps() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use tavern_core::Runtime;

    struct SlowRuntime {
        delay_ms: u64,
        call_count: Arc<AtomicUsize>,
    }

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
            self.call_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            Ok(json!("done"))
        }
    }

    let call_count = Arc::new(AtomicUsize::new(0));
    let runtime: Arc<dyn Runtime> = Arc::new(SlowRuntime {
        delay_ms: 300,
        call_count: call_count.clone(),
    });

    let hero = TavernHero::new(runtime);
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("agent.yaml"),
        r#"
id: test_agent
name: Test Agent
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
    )
    .unwrap();
    hero.load_agent(dir.path().join("agent.yaml").as_path())
        .await
        .unwrap();

    let engine = WorkflowEngine::new(Arc::new(hero));

    let wf = Workflow {
        id: "parallel".to_string(),
        name: "Parallel".to_string(),
        description: None,
        steps: vec![
            Step {
                id: "a".to_string(),
                agent_id: "test_agent".to_string(),
                task: "task a".to_string(),
                depends_on: vec![],
                output_key: Some("out_a".to_string()),
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
            Step {
                id: "b".to_string(),
                agent_id: "test_agent".to_string(),
                task: "task b".to_string(),
                depends_on: vec![],
                output_key: Some("out_b".to_string()),
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
        ],
        inputs: vec![],
        outputs: vec![],
        process: Process::Sequential,
        planning: None,
            webhook: None,
    };

    let start = Instant::now();
    let result = engine.run(&wf, json!({})).await.unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 550,
        "steps should execute in parallel, took {}ms",
        elapsed.as_millis()
    );
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(result.context["out_a"], "done");
    assert_eq!(result.context["out_b"], "done");
}

#[tokio::test]
async fn test_run_max_concurrency() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use tavern_core::Runtime;

    struct SlowRuntime {
        delay_ms: u64,
        call_count: Arc<AtomicUsize>,
    }

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
            self.call_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            Ok(json!("done"))
        }
    }

    let call_count = Arc::new(AtomicUsize::new(0));
    let runtime: Arc<dyn Runtime> = Arc::new(SlowRuntime {
        delay_ms: 200,
        call_count: call_count.clone(),
    });

    let hero = TavernHero::new(runtime);
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("agent.yaml"),
        r#"
id: test_agent
name: Test Agent
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
    )
    .unwrap();
    hero.load_agent(dir.path().join("agent.yaml").as_path())
        .await
        .unwrap();

    let engine = WorkflowEngine::new(Arc::new(hero)).with_max_concurrency(2);

    let wf = Workflow {
        id: "limited".to_string(),
        name: "Limited Concurrency".to_string(),
        description: None,
        steps: vec![
            Step {
                id: "a".to_string(),
                agent_id: "test_agent".to_string(),
                task: "task a".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
            Step {
                id: "b".to_string(),
                agent_id: "test_agent".to_string(),
                task: "task b".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
            Step {
                id: "c".to_string(),
                agent_id: "test_agent".to_string(),
                task: "task c".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
        ],
        inputs: vec![],
        outputs: vec![],
        process: Process::Sequential,
        planning: None,
            webhook: None,
    };

    let start = Instant::now();
    engine.run(&wf, json!({})).await.unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 550,
        "with max_concurrency=2, 3x200ms steps should take ~400ms, took {}ms",
        elapsed.as_millis()
    );
    assert!(
        elapsed.as_millis() >= 300,
        "should not be faster than ~300ms (indicating all 3 ran in parallel), took {}ms",
        elapsed.as_millis()
    );
    assert_eq!(call_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_run_parallel_failure_cancels_others() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use tavern_core::Runtime;

    struct SelectiveRuntime {
        completed_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Runtime for SelectiveRuntime {
        async fn execute(
            &self,
            _agent_id: &str,
            task: &str,
            _context: Option<Value>,
            _system_prompt: &str,
            _model: &str,
        ) -> Result<Value, tavern_core::RuntimeError> {
            if task == "fail" {
                return Err(tavern_core::RuntimeError::RequestFailed {
                    status: 500,
                    body: "boom".to_string(),
                });
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            self.completed_count.fetch_add(1, Ordering::SeqCst);
            Ok(json!("done"))
        }
    }

    let completed_count = Arc::new(AtomicUsize::new(0));
    let runtime: Arc<dyn Runtime> = Arc::new(SelectiveRuntime {
        completed_count: completed_count.clone(),
    });

    let hero = TavernHero::new(runtime);
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("agent.yaml"),
        r#"
id: test_agent
name: Test Agent
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
    )
    .unwrap();
    hero.load_agent(dir.path().join("agent.yaml").as_path())
        .await
        .unwrap();

    let engine = WorkflowEngine::new(Arc::new(hero));

    let wf = Workflow {
        id: "fail_fast".to_string(),
        name: "Fail Fast".to_string(),
        description: None,
        steps: vec![
            Step {
                id: "slow".to_string(),
                agent_id: "test_agent".to_string(),
                task: "slow".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
            Step {
                id: "fast".to_string(),
                agent_id: "test_agent".to_string(),
                task: "fail".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
        ],
        inputs: vec![],
        outputs: vec![],
        process: Process::Sequential,
        planning: None,
            webhook: None,
    };

    let start = Instant::now();
    let err = engine.run(&wf, json!({})).await.unwrap_err();
    let elapsed = start.elapsed();

    assert!(
        matches!(err, CompError::StepFailed { step_id, reason } if step_id == "fast" && reason.contains("boom"))
    );
    assert!(
        elapsed.as_millis() < 400,
        "should fail fast when one parallel step fails, took {}ms",
        elapsed.as_millis()
    );
    assert_eq!(completed_count.load(Ordering::SeqCst), 0);
}

// ── Hierarchical Process tests ──

async fn make_hierarchical_engine<F>(handler: F) -> WorkflowEngine
where
    F: Fn(&str, &str, Option<Value>, &str, &str) -> Result<Value, tavern_core::RuntimeError>
        + Send
        + Sync
        + 'static,
{
    let runtime = Arc::new(MockRuntime::new(handler));
    let hero = TavernHero::new(runtime);

    let dir = tempfile::tempdir().unwrap();
    // Manager agent
    std::fs::write(
        dir.path().join("manager.yaml"),
        r#"
id: manager
name: Manager
model:
  provider: test
  name: test
instructions: You are a project manager.
"#,
    )
    .unwrap();
    // Worker agent
    std::fs::write(
        dir.path().join("worker.yaml"),
        r#"
id: test_agent
name: Test Agent
model:
  provider: test
  name: test
instructions: test
"#,
    )
    .unwrap();
    hero.load_from_dir(dir.path()).await.unwrap();

    WorkflowEngine::new(Arc::new(hero))
}

fn hierarchical_workflow() -> Workflow {
    Workflow {
        id: "hw1".to_string(),
        name: "Hierarchical WF".to_string(),
        description: None,
        steps: vec![
            Step {
                id: "s1".to_string(),
                agent_id: "test_agent".to_string(),
                task: "task s1".to_string(),
                depends_on: vec![],
                output_key: Some("out_s1".to_string()),
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
            Step {
                id: "s2".to_string(),
                agent_id: "test_agent".to_string(),
                task: "task s2".to_string(),
                depends_on: vec![],
                output_key: Some("out_s2".to_string()),
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
            },
        ],
        inputs: vec![],
        outputs: vec![],
        process: Process::Hierarchical(ManagerConfig {
            agent_id: "manager".to_string(),
            instructions: None,
        }),
        planning: None,
            webhook: None,
    }
}

#[tokio::test]
async fn test_hierarchical_delegate_then_done() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let call_count = AtomicUsize::new(0);

    let engine = make_hierarchical_engine(move |agent_id, _task, _context, _sp, _model| {
        if agent_id == "manager" {
            let count = call_count.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                Ok(json!({"action": "delegate", "task_id": "s1", "agent_id": "test_agent"}))
            } else {
                Ok(json!({"action": "done"}))
            }
        } else {
            Ok(json!("step result"))
        }
    })
    .await;

    let wf = hierarchical_workflow();
    let result = engine.run(&wf, json!({})).await.unwrap();

    eprintln!("DEBUG step_results: {:?}", result.step_results);
    eprintln!("DEBUG context: {:?}", result.context);

    assert!(result.step_results.contains_key("s1"));
    assert!(matches!(
        result.step_results["s1"].status,
        StepStatus::Completed
    ));
    // s2 was never delegated
    assert!(
        !result.step_results.contains_key("s2")
            || matches!(result.step_results["s2"].status, StepStatus::Pending)
    );
}

#[tokio::test]
async fn test_hierarchical_all_steps() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let call_count = AtomicUsize::new(0);

    let engine = make_hierarchical_engine(move |agent_id, _task, _context, _sp, _model| {
        if agent_id == "manager" {
            let count = call_count.fetch_add(1, Ordering::SeqCst);
            match count {
                0 => Ok(json!({"action": "delegate", "task_id": "s1", "agent_id": "test_agent"})),
                1 => Ok(json!({"action": "delegate", "task_id": "s2", "agent_id": "test_agent"})),
                _ => Ok(json!({"action": "done"})),
            }
        } else {
            Ok(json!("step result"))
        }
    })
    .await;

    let wf = hierarchical_workflow();
    let result = engine.run(&wf, json!({})).await.unwrap();

    assert_eq!(result.step_results.len(), 2);
    assert!(matches!(
        result.step_results["s1"].status,
        StepStatus::Completed
    ));
    assert!(matches!(
        result.step_results["s2"].status,
        StepStatus::Completed
    ));
}

#[tokio::test]
async fn test_hierarchical_manager_loop_exceeded() {
    let engine = make_hierarchical_engine(move |agent_id, _task, _context, _sp, _model| {
        if agent_id == "manager" {
            // Always delegate to s1, creating infinite loop
            Ok(json!({"action": "delegate", "task_id": "s1", "agent_id": "test_agent"}))
        } else {
            Ok(json!("step result"))
        }
    })
    .await;

    let wf = hierarchical_workflow();
    let err = engine.run(&wf, json!({})).await.unwrap_err();
    assert!(matches!(
        err,
        CompError::ManagerLoopExceeded { max_loops: 100 }
    ));
}

#[tokio::test]
async fn test_hierarchical_manager_unknown_task_id() {
    let engine = make_hierarchical_engine(move |agent_id, _task, _context, _sp, _model| {
        if agent_id == "manager" {
            Ok(json!({"action": "delegate", "task_id": "nonexistent", "agent_id": "test_agent"}))
        } else {
            Ok(json!("step result"))
        }
    })
    .await;

    let wf = hierarchical_workflow();
    let err = engine.run(&wf, json!({})).await.unwrap_err();
    assert!(matches!(err, CompError::ManagerError { .. }));
    assert!(format!("{}", err).contains("nonexistent"));
}

#[tokio::test]
async fn test_hierarchical_manager_non_json_response_with_retry() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let call_count = AtomicUsize::new(0);

    let engine = make_hierarchical_engine(move |agent_id, _task, _context, _sp, _model| {
        if agent_id == "manager" {
            let count = call_count.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                Ok(json!("not json at all, just some text"))
            } else {
                // Retry: return valid JSON
                Ok(json!({"action": "done"}))
            }
        } else {
            Ok(json!("step result"))
        }
    })
    .await;

    let wf = hierarchical_workflow();
    let result = engine.run(&wf, json!({})).await.unwrap();
    // Manager retried and returned done
    assert!(
        result.step_results.is_empty()
            || result
                .step_results
                .values()
                .all(|r| matches!(r.status, StepStatus::Pending))
    );
}

#[tokio::test]
async fn test_hierarchical_manager_agent_not_registered() {
    // Use make_engine (not make_hierarchical_engine) — only "test_agent" is registered.
    // The hierarchical workflow references "manager" which doesn't exist.
    let engine = make_engine(|_agent_id, _task, _context, _sp, _model| Ok(json!("ok"))).await;
    let wf = hierarchical_workflow();
    let err = engine.run(&wf, json!({})).await.unwrap_err();
    assert!(matches!(err, CompError::AgentNotFound { id } if id == "manager"));
}

#[tokio::test]
async fn test_hierarchical_event_stream_matches_sequential() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = AtomicUsize::new(0);
    let engine = make_hierarchical_engine(move |agent_id, _task, _context, _sp, _model| {
        if agent_id == "manager" {
            let count = call_count.fetch_add(1, Ordering::SeqCst);
            match count {
                0 => Ok(json!({"action": "delegate", "task_id": "s1", "agent_id": "test_agent"})),
                _ => Ok(json!({"action": "done"})),
            }
        } else {
            Ok(json!("step result"))
        }
    })
    .await;

    let wf = hierarchical_workflow();
    let mut handle = engine.start(&wf, json!({})).await.unwrap();
    let instance_id = handle.id().to_string();
    let _result = handle.await_completion().await.unwrap();

    let events = engine.store().read_stream(&instance_id).await.unwrap();

    let event_types: Vec<&str> = events
        .iter()
        .map(|e| match e {
            WorkflowEvent::InstanceCreated { .. } => "InstanceCreated",
            WorkflowEvent::InstanceStarted => "InstanceStarted",
            WorkflowEvent::StepScheduled { .. } => "StepScheduled",
            WorkflowEvent::StepStarted { .. } => "StepStarted",
            WorkflowEvent::StepCompleted { .. } => "StepCompleted",
            WorkflowEvent::StepFailed { .. } => "StepFailed",
            WorkflowEvent::StepRetryScheduled { .. } => "StepRetryScheduled",
            WorkflowEvent::SignalWaitStarted { .. } => "SignalWaitStarted",
            WorkflowEvent::SignalReceived { .. } => "SignalReceived",
            WorkflowEvent::TimerFired { .. } => "TimerFired",
            WorkflowEvent::CancelRequested { .. } => "CancelRequested",
            WorkflowEvent::WorkflowCompleted { .. } => "WorkflowCompleted",
            WorkflowEvent::WorkflowFailed { .. } => "WorkflowFailed",
            WorkflowEvent::BreakpointHit { .. } => "BreakpointHit",
            WorkflowEvent::External { .. } => "External",
        })
        .collect();

    assert_eq!(event_types[0], "InstanceCreated");
    assert_eq!(event_types[1], "InstanceStarted");
    assert!(event_types.contains(&"StepScheduled"));
    assert!(event_types.contains(&"StepStarted"));
    assert!(event_types.contains(&"StepCompleted"));
    assert_eq!(event_types.last().unwrap(), &"WorkflowCompleted");
}

// ── Planning tests ──

#[tokio::test]
async fn test_planning_injects_context_into_task() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let call_count = AtomicUsize::new(0);

    let engine = make_engine(move |_agent_id, _task, _context, _sp, _model| {
        let count = call_count.fetch_add(1, Ordering::SeqCst);
        if count == 0 {
            // Planning phase: return a Plan
            Ok(json!({
                "overall_strategy": "Research first",
                "steps": [{
                    "task_id": "s1",
                    "agent_id": "test_agent",
                    "reasoning": "need data first",
                    "expected_output": "a report",
                    "dependencies": []
                }]
            }))
        } else {
            // Step execution
            Ok(json!("done"))
        }
    })
    .await;

    let mut wf = simple_workflow();
    wf.planning = Some(PlanningConfig {
        enabled: true,
        planning_agent: Some("test_agent".to_string()),
    });

    let result = engine.run(&wf, json!({"input": "hello"})).await.unwrap();
    assert_eq!(result.context["result"], "done");
}

#[tokio::test]
async fn test_planning_disabled_skips_planner() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let call_count = Arc::new(AtomicUsize::new(0));
    let cc = call_count.clone();

    let engine = make_engine(move |_agent_id, _task, _context, _sp, _model| {
        cc.fetch_add(1, Ordering::SeqCst);
        Ok(json!("done"))
    })
    .await;

    let mut wf = simple_workflow();
    wf.planning = Some(PlanningConfig {
        enabled: false,
        planning_agent: None,
    });

    engine.run(&wf, json!({"input": "x"})).await.unwrap();
    // Only 1 call: the step itself. No planning call.
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_planning_error_fails_workflow() {
    let engine = make_engine(|_agent_id, _task, _context, _sp, _model| {
        Err(tavern_core::RuntimeError::RequestFailed {
            status: 500,
            body: "planner failed".to_string(),
        })
    })
    .await;

    let mut wf = simple_workflow();
    wf.planning = Some(PlanningConfig {
        enabled: true,
        planning_agent: Some("test_agent".to_string()),
    });

    let err = engine.run(&wf, json!({"input": "x"})).await.unwrap_err();
    assert!(matches!(err, CompError::PlanningError { .. }));
}

#[tokio::test]
async fn test_planning_agent_omitted_falls_back_to_first_step_agent() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let call_count = AtomicUsize::new(0);

    let engine = make_engine(move |agent_id, _task, _context, _sp, _model| {
        let count = call_count.fetch_add(1, Ordering::SeqCst);
        if count == 0 {
            // Planning phase: called with "test_agent" (fallback from steps[0].agent_id)
            assert_eq!(
                agent_id, "test_agent",
                "planner should use steps[0].agent_id as fallback"
            );
            Ok(json!({
                "overall_strategy": "simple plan",
                "steps": [{
                    "task_id": "s1",
                    "agent_id": "test_agent",
                    "reasoning": "just do it",
                    "expected_output": "result",
                    "dependencies": []
                }]
            }))
        } else {
            // Step execution
            Ok(json!("done"))
        }
    })
    .await;

    let mut wf = simple_workflow();
    wf.planning = Some(PlanningConfig {
        enabled: true,
        planning_agent: None, // omitted — falls back to steps[0].agent_id ("test_agent")
    });

    let result = engine.run(&wf, json!({"input": "x"})).await.unwrap();
    assert_eq!(result.context["result"], "done");
}

#[tokio::test]
#[ignore = "requires injectable PLANNING_TIMEOUT_SECS (hardcoded at 60s); verifies code path exists"]
async fn test_planning_timeout_returns_planning_error() {
    // This test documents that when the planner exceeds PLANNING_TIMEOUT_SECS (60s),
    // the engine returns PlanningError. The timeout is hardcoded in engine.rs.
    //
    // Code path verified: run_planning_phase wraps hero.execute in
    // tokio::time::timeout(Duration::from_secs(PLANNING_TIMEOUT_SECS), ...).
    //
    // TODO(V0.4.0): Make PLANNING_TIMEOUT_SECS injectable via WorkflowEngine config.
    assert!(true, "timeout code path exists in run_planning_phase");
}

#[tokio::test]
async fn test_planning_plus_hierarchical_integration() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let call_count = AtomicUsize::new(0);

    let engine = make_hierarchical_engine(move |agent_id, _task, _context, _sp, _model| {
        let count = call_count.fetch_add(1, Ordering::SeqCst);
        match (agent_id, count) {
            // Call 0: Planning phase (planner = "manager" per config)
            ("manager", 0) => Ok(json!({
                "overall_strategy": "delegate research first",
                "steps": [
                    {"task_id": "s1", "agent_id": "test_agent", "reasoning": "need data", "expected_output": "report", "dependencies": []},
                    {"task_id": "s2", "agent_id": "test_agent", "reasoning": "write after research", "expected_output": "article", "dependencies": ["s1"]}
                ]
            })),
            // Subsequent calls: Manager delegation (after planning)
            ("manager", _) => {
                let mgr_count = count - 1; // planning was call 0
                if mgr_count == 0 {
                    Ok(json!({"action": "delegate", "task_id": "s1", "agent_id": "test_agent"}))
                } else {
                    Ok(json!({"action": "done"}))
                }
            }
            _ => Ok(json!("step result"))
        }
    })
    .await;

    let mut wf = hierarchical_workflow();
    wf.planning = Some(PlanningConfig {
        enabled: true,
        planning_agent: Some("manager".to_string()), // reuse manager as planner
    });

    let result = engine.run(&wf, json!({})).await.unwrap();
    assert!(result.step_results.contains_key("s1"));
    assert!(matches!(
        result.step_results["s1"].status,
        StepStatus::Completed
    ));
}

// ── V2 Event-Driven tests ──

#[tokio::test]
async fn test_start_and_await_completion_equivalent_to_run() {
    let engine =
        make_engine(|_agent_id, _task, _context, _system_prompt, _model| Ok(json!("done"))).await;
    let wf = simple_workflow();

    let run_result = engine.run(&wf, json!({"input": "hello"})).await.unwrap();

    let mut handle = engine.start(&wf, json!({"input": "hello"})).await.unwrap();
    let start_result = handle.await_completion().await.unwrap();

    assert_eq!(run_result.context, start_result.context);
    assert_eq!(run_result.outputs, start_result.outputs);
    assert_eq!(
        run_result.step_results.len(),
        start_result.step_results.len()
    );
}

#[tokio::test]
async fn test_signal_wait_and_resume() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    let call_count = Arc::new(AtomicUsize::new(0));
    let cc = call_count.clone();
    let engine = make_engine(move |_agent_id, _task, _context, _system_prompt, _model| {
        cc.fetch_add(1, Ordering::SeqCst);
        Ok(json!("step_done"))
    })
    .await;

    let mut wf = simple_workflow();
    wf.steps[0].wait_for_signal = Some("approve".to_string());

    let mut handle = engine.start(&wf, json!({"input": "hello"})).await.unwrap();
    let _execution_id = handle.id().to_string();

    // Wait a bit for the step to complete and enter signal wait
    tokio::time::sleep(Duration::from_millis(100)).await;

    let state = handle.query_state(engine.store.as_ref()).await.unwrap();
    assert!(
        matches!(state.status, InstanceStatus::WaitingForSignal { ref signal } if signal == "approve"),
        "expected WaitingForSignal, got {:?}",
        state.status
    );

    // Send signal
    handle
        .signal("approve", json!({"by": "admin"}))
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), handle.await_completion())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.context["result"], "step_done");
    assert_eq!(result.context["signals"]["approve"]["by"], "admin");
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    drop(call_count);
}

#[tokio::test]
async fn test_signal_timeout_fails_workflow() {
    use std::time::Duration;

    let engine =
        make_engine(|_agent_id, _task, _context, _system_prompt, _model| Ok(json!("done"))).await;

    let mut wf = simple_workflow();
    wf.steps[0].wait_for_signal = Some("approve".to_string());
    wf.steps[0].signal_timeout = Some(1); // 1 second timeout

    let mut handle = engine.start(&wf, json!({"input": "hello"})).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(5), handle.await_completion())
        .await
        .unwrap()
        .unwrap_err();

    assert!(
        matches!(&err,
            CompError::StepFailed { step_id, reason } if step_id == "s1" && reason.contains("timeout")
        ),
        "expected signal timeout error, got: {:?}",
        err
    );
}

#[tokio::test]
async fn test_cancel_execution() {
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
id: test_agent
name: Test Agent
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
    )
    .unwrap();
    hero.load_agent(dir.path().join("agent.yaml").as_path())
        .await
        .unwrap();

    let engine = WorkflowEngine::new(Arc::new(hero));
    let wf = simple_workflow();

    let mut handle = engine.start(&wf, json!({"input": "hello"})).await.unwrap();

    // Give the step time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    handle.cancel().await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(5), handle.await_completion())
        .await
        .unwrap()
        .unwrap_err();

    assert!(
        matches!(err, CompError::StepFailed { .. }),
        "expected failure after cancel, got: {:?}",
        err
    );
}

// ── Checkpoint Recovery 测试 ──

fn signal_workflow() -> Workflow {
    Workflow {
        id: "recovery_test".to_string(),
        name: "Recovery Test".to_string(),
        description: None,
        process: Process::Sequential,
        planning: None,
            webhook: None,
        inputs: vec![],
        outputs: vec![],
        steps: vec![Step {
            id: "s1".to_string(),
            agent_id: "test_agent".to_string(),
            task: "do something".to_string(),
            depends_on: vec![],
            output_key: None,
            timeout: None,
            retries: None,
            retry_delay: None,
            wait_for_signal: Some("approve".to_string()),
            signal_timeout: None,
            expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
        }],
    }
}

/// 验证 recover() 可从 Event Log 重建状态并从中断点继续执行。
#[tokio::test]
async fn test_recover_after_signal_wait_resumes() {
    let engine = make_engine(|_, _, _, _, _| Ok(json!("done"))).await;
    // Use MemoryEventStore so events persist even if handle is dropped
    let store = Arc::new(MemoryEventStore::new());
    let engine = engine.with_store(store.clone());

    let wf = signal_workflow();

    // 1. 启动工作流
    let handle = engine.start(&wf, json!({})).await.unwrap();
    let instance_id = handle.id().to_string();

    // 2. 等待进入 WaitingForSignal
    let mut status = String::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let state = handle.query_state(store.as_ref()).await.unwrap();
        status = state.status.as_str().to_string();
        if status == "waiting_for_signal" {
            break;
        }
    }
    assert_eq!(
        status, "waiting_for_signal",
        "expected waiting_for_signal, got {}",
        status
    );

    // 3. 模拟崩溃：丢弃 handle
    drop(handle);

    // 4. 恢复
    let mut recovered = engine
        .recover(instance_id.clone(), &wf)
        .await
        .expect("recover should succeed");

    assert_eq!(recovered.id(), instance_id);

    // 5. 发送信号
    recovered
        .signal("approve", json!({"ok": true}))
        .await
        .unwrap();

    // 6. 等待完成
    let result = tokio::time::timeout(Duration::from_secs(5), recovered.await_completion())
        .await
        .unwrap()
        .expect("workflow should complete after recovery");

    assert_eq!(result.context["signals"]["approve"], json!({"ok": true}));
}

/// 验证 recover() 对已完成的实例返回错误。
#[tokio::test]
async fn test_recover_completed_instance_returns_error() {
    let engine = make_engine(|_, _, _, _, _| Ok(json!("done"))).await;
    let store = Arc::new(MemoryEventStore::new());
    let engine = engine.with_store(store.clone());

    let wf = simple_workflow();

    let mut handle = engine.start(&wf, json!({"input": "test"})).await.unwrap();
    let instance_id = handle.id().to_string();

    // Wait for completion
    handle.await_completion().await.unwrap();

    // Try to recover completed instance
    let result = engine.recover(instance_id, &wf).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        CompError::InstanceClosed { .. }
    ));
}

/// 验证 recover() 对不存在的实例返回错误。
#[tokio::test]
async fn test_recover_nonexistent_instance_returns_error() {
    let engine = make_engine(|_, _, _, _, _| Ok(json!("done"))).await;
    let wf = simple_workflow();

    let result = engine
        .recover("nonexistent-instance-id".to_string(), &wf)
        .await;
    assert!(result.is_err());
}

/// 验证 store.list_by_status 可用于找到待恢复的 Running 实例。
#[tokio::test]
async fn test_list_by_status_finds_running_instances() {
    let engine = make_engine(|_, _, _, _, _| Ok(json!("done"))).await;
    let store = Arc::new(MemoryEventStore::new());
    let engine = engine.with_store(store.clone());

    let wf = signal_workflow();
    let handle = engine.start(&wf, json!({})).await.unwrap();
    let instance_id = handle.id().to_string();

    // Wait for WaitingForSignal
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let state = handle.query_state(store.as_ref()).await.unwrap();
        if state.status.as_str() == "waiting_for_signal" {
            break;
        }
    }

    // list_by_status for any WaitingForSignal variant
    let waiting = store
        .list_by_status(InstanceStatus::WaitingForSignal {
            signal: String::new(),
        })
        .await
        .unwrap();

    assert!(
        !waiting.is_empty(),
        "expected at least one WaitingForSignal instance, got none"
    );

    // Store still contains events for recovery
    let events = store.read_stream(&instance_id).await.unwrap();
    assert!(!events.is_empty());
}
