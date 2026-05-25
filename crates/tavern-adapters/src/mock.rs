use serde_json::Value;
use tavern_core::{Runtime, RuntimeError};

type MockHandler =
    Box<dyn Fn(&str, &str, Option<Value>, &str, &str) -> Result<Value, RuntimeError> + Send + Sync>;

pub struct MockRuntime {
    /// 同步闭包，应在立即返回的轻量逻辑中使用。
    /// 如需异步模拟，需自定义 Runtime 实现。
    handler: MockHandler,
}

impl MockRuntime {
    pub fn new<F>(handler: F) -> Self
    where
        F: Fn(&str, &str, Option<Value>, &str, &str) -> Result<Value, RuntimeError>
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
    ) -> Result<Value, RuntimeError> {
        (self.handler)(agent_id, task, context, system_prompt, model)
    }
}
