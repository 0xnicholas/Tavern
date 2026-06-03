use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

/// Flow 方法步骤执行器。由 #[flow_impl] proc-macro 自动实现。
/// 引擎遇到 agent_id == FLOW_AGENT_ID 时，通过此 trait 执行步骤，而非通过 TavernHero。
pub trait FlowStepExecutor: Send + 'static {
    fn execute_step(
        &mut self,
        step_id: &str,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExecutor;

    impl FlowStepExecutor for MockExecutor {
        fn execute_step(
            &mut self,
            step_id: &str,
            _input: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
            let id = step_id.to_string();
            Box::pin(async move { Ok(Value::String(format!("result_{}", id))) })
        }
    }

    #[tokio::test]
    async fn test_mock_flow_executor() {
        let mut exec = MockExecutor;
        let output = exec.execute_step("research", Value::Null).await.unwrap();
        assert_eq!(output, Value::String("result_research".into()));
    }
}
