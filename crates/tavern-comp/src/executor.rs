use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::context::render_template;
use crate::event::WorkflowEvent;
use crate::workflow::Step;

pub struct StepExecutor {
    hero: Arc<tavern_hero::TavernHero>,
    tx: mpsc::Sender<WorkflowEvent>,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl StepExecutor {
    pub fn new(
        hero: Arc<tavern_hero::TavernHero>,
        tx: mpsc::Sender<WorkflowEvent>,
        max_concurrency: usize,
    ) -> Self {
        Self {
            hero,
            tx,
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrency.min(65536))),
        }
    }

    pub async fn submit(&self, step: Arc<Step>, context: Value, attempt: u64, will_retry: bool) {
        let hero = self.hero.clone();
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
        let result = Self::execute_once(&step, &context, &hero, model.as_deref()).await;

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
        hero: &tavern_hero::TavernHero,
        model_override: Option<&str>,
    ) -> Result<Value, String> {
        let task = match render_template(&step.task, context) {
            Ok(t) => t,
            Err(e) => return Err(format!("template render failed: {}", e)),
        };

        let timeout = step.timeout.unwrap_or(300);

        let fut: std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, tavern_hero::TavernError>> + Send>> =
            if let Some(model) = model_override {
                Box::pin(hero.execute_with_model(&step.agent_id, &task, Some(context.clone()), model))
            } else {
                Box::pin(hero.execute(&step.agent_id, &task, Some(context.clone())))
            };
        match tokio::time::timeout(Duration::from_secs(timeout), fut).await {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!("step timed out after {}s", timeout)),
        }
    }
}
