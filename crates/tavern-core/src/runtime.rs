use serde_json::Value;

/// 底层 Runtime 抽象，负责将任务提交给实际的 LLM/Agent 执行后端。
///
/// 实现者需要保证 `execute` 是线程安全的（`Send + Sync`）。
#[async_trait::async_trait]
pub trait Runtime: Send + Sync {
    /// 向 Runtime 提交任务执行。
    ///
    /// # Arguments
    /// * `agent_id` - 目标 Agent 的唯一标识
    /// * `task` - 任务描述
    /// * `context` - 可选的上下文 JSON 对象
    /// * `system_prompt` - Agent 的系统提示词
    /// * `model` - 模型标识（如 "openai/gpt-4o"）
    ///
    /// # Errors
    /// 当网络请求失败、返回非 2xx 状态码或响应无法解析时返回 [`RuntimeError`]。
    async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<Value>,
        system_prompt: &str,
        model: &str,
    ) -> Result<Value, RuntimeError>;
}

/// Runtime 执行过程中可能发生的错误。
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// 底层服务返回非 2xx 状态码。
    #[error("runtime request failed: status={status}, body={body}")]
    RequestFailed { status: u16, body: String },

    /// 网络层错误（连接超时、DNS 失败等）。
    #[error("runtime network error: {0}")]
    NetworkError(String),

    /// 响应体无法解析为合法 JSON。
    #[error("runtime response invalid: {0}")]
    InvalidResponse(String),
}
