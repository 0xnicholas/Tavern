use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::context::render_template;
use crate::event::WorkflowEvent;
use crate::flow_executor::FlowStepExecutor;
use crate::workflow::{FLOW_AGENT_ID, Step};

pub struct StepExecutor {
    hero: Option<Arc<tavern_hero::TavernHero>>,
    flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
    tx: mpsc::Sender<WorkflowEvent>,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl StepExecutor {
    pub fn new(
        hero: Option<Arc<tavern_hero::TavernHero>>,
        flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
        tx: mpsc::Sender<WorkflowEvent>,
        max_concurrency: usize,
    ) -> Self {
        Self {
            hero,
            flow_executor,
            tx,
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrency.min(65536))),
        }
    }

    pub async fn submit(&self, step: Arc<Step>, context: Value, attempt: u64, will_retry: bool) {
        let hero = self.hero.clone();
        let flow_executor = self.flow_executor.clone();
        let tx = self.tx.clone();
        let output_key = step.output_key.clone();
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();

        tokio::spawn(async move {
            let _permit = permit;

            let started = WorkflowEvent::StepStarted {
                step_id: step.id.clone(),
                started_at: Utc::now(),
            };
            if let Err(e) = tx.send(started).await {
                tracing::error!(error = %e, "interpreter closed, step start event dropped");
                return;
            }

            let model = step
                .model_override
                .as_ref()
                .map(|m| format!("{}/{}", m.provider, m.name));
            let result =
                Self::execute_once(&step, &context, hero, flow_executor, model.as_deref(), &tx)
                    .await;

            let event = match result {
                Ok(output) => WorkflowEvent::StepCompleted {
                    step_id: step.id.clone(),
                    output,
                    attempt,
                    output_key,
                    completed_at: Utc::now(),
                },
                Err(error) => WorkflowEvent::StepFailed {
                    step_id: step.id.clone(),
                    error,
                    attempt,
                    will_retry,
                },
            };
            if let Err(e) = tx.send(event).await {
                tracing::error!(error = %e, "interpreter closed, step result dropped");
            }
        });
    }

    async fn execute_once(
        step: &Step,
        context: &Value,
        hero: Option<Arc<tavern_hero::TavernHero>>,
        flow_executor: Option<Arc<tokio::sync::Mutex<dyn FlowStepExecutor>>>,
        model_override: Option<&str>,
        tx: &mpsc::Sender<WorkflowEvent>,
    ) -> Result<Value, String> {
        // V0.4: Flow 哨兵路由
        if step.agent_id == FLOW_AGENT_ID {
            let executor = flow_executor
                .as_ref()
                .ok_or_else(|| "flow executor not configured".to_string())?;
            let mut guard = executor.lock().await;
            let input = resolve_flow_input(step, context);
            return guard.execute_step(&step.task, input).await;
        }

        let task = match render_template(&step.task, context) {
            Ok(t) => t,
            Err(e) => return Err(format!("template render failed: {}", e)),
        };

        let hero = hero.ok_or_else(|| "hero not configured".to_string())?;
        let timeout = step.timeout.unwrap_or(300);
        let model = model_override.unwrap_or(&step.agent_id).to_string();

        // Emit LLMCallStarted
        let llm_started = WorkflowEvent::LLMCallStarted {
            step_id: step.id.clone(),
            model: model.clone(),
            prompt_tokens: None,
            started_at: Utc::now(),
        };
        if let Err(e) = tx.send(llm_started).await {
            tracing::warn!(error = %e, "LLMCallStarted event dropped");
        }

        let fut: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Value, tavern_hero::TavernError>> + Send>,
        > = if let Some(model) = model_override {
            Box::pin(hero.execute_with_model(&step.agent_id, &task, Some(context.clone()), model))
        } else {
            Box::pin(hero.execute(&step.agent_id, &task, Some(context.clone())))
        };
        match tokio::time::timeout(Duration::from_secs(timeout), fut).await {
            Ok(Ok(output)) => {
                // Emit LLMCallCompleted
                let llm_completed = WorkflowEvent::LLMCallCompleted {
                    step_id: step.id.clone(),
                    output: output.clone(),
                    usage: None,
                    completed_at: Utc::now(),
                };
                if let Err(e) = tx.send(llm_completed).await {
                    tracing::warn!(error = %e, "LLMCallCompleted event dropped");
                }
                Ok(output)
            }
            Ok(Err(e)) => {
                // Emit LLMCallFailed
                let llm_failed = WorkflowEvent::LLMCallFailed {
                    step_id: step.id.clone(),
                    error: e.to_string(),
                    failed_at: Utc::now(),
                };
                if let Err(e) = tx.send(llm_failed).await {
                    tracing::warn!(error = %e, "LLMCallFailed event dropped");
                }
                Err(e.to_string())
            }
            Err(_) => {
                let err = format!("step timed out after {}s", timeout);
                // Emit LLMCallFailed (timeout)
                let llm_failed = WorkflowEvent::LLMCallFailed {
                    step_id: step.id.clone(),
                    error: err.clone(),
                    failed_at: Utc::now(),
                };
                if let Err(e) = tx.send(llm_failed).await {
                    tracing::warn!(error = %e, "LLMCallFailed(timeout) event dropped");
                }
                Err(err)
            }
        }
    }
}

/// 为 Flow step 解析输入：取第一个依赖 step 的输出。
fn resolve_flow_input(step: &Step, context: &Value) -> Value {
    if let Some(ref router) = step.router {
        return context
            .get(&router.upstream)
            .cloned()
            .unwrap_or(Value::Null);
    }
    let upstreams: Vec<&str> = if !step.depends_on.is_empty() {
        step.depends_on.iter().map(|s| s.as_str()).collect()
    } else {
        step.or_depends_on.iter().map(|s| s.as_str()).collect()
    };
    upstreams
        .first()
        .and_then(|id| context.get(id))
        .cloned()
        .unwrap_or(Value::Null)
}
