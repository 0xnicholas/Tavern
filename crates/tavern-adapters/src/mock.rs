use serde_json::Value;
use tavern_core::{Runtime, RuntimeError};

type MockHandler =
    Box<dyn Fn(&str, &str, Option<Value>, &str, &str, &[tavern_core::ToolDef]) -> Result<Value, RuntimeError> + Send + Sync>;

pub struct MockRuntime {
    handler: MockHandler,
}

impl MockRuntime {
    pub fn new<F>(handler: F) -> Self
    where
        F: Fn(&str, &str, Option<Value>, &str, &str, &[tavern_core::ToolDef]) -> Result<Value, RuntimeError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            handler: Box::new(handler),
        }
    }
}

#[async_trait::async_trait]
impl Runtime for MockRuntime {
    async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<Value>,
        system_prompt: &str,
        model: &str,
        tools: &[tavern_core::ToolDef],
    ) -> Result<Value, RuntimeError> {
        (self.handler)(agent_id, task, context, system_prompt, model, tools)
    }
}
