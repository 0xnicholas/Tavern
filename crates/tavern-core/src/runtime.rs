use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 工具定义，在 Tavern → Runtime 之间传递。
/// 携带完整的 Pandaria ToolConfig 序列化所需字段。
/// MockRuntime 等简单实现忽略除 id 外的字段。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDef {
    /// 工具标识（用于 handler 路由和 system prompt 注入）
    pub id: String,

    /// LLM function name（默认 = ""，序列化时 fallback 到 id）
    #[serde(default)]
    pub name: String,

    /// 工具描述
    #[serde(default)]
    pub description: String,

    /// JSON Schema，描述工具参数（默认 {}）
    #[serde(default)]
    pub parameters: Value,

    /// Pandaria 回调的目标端点（Hero 根据 TAVERN_PUBLIC_URL 拼接）
    #[serde(default)]
    pub endpoint: String,

    /// 回调超时（毫秒）
    #[serde(default = "default_tool_timeout")]
    pub timeout_ms: u64,

    /// 工具私有配置（LLM 不可见，仅 handler 读取）
    #[serde(default)]
    pub config: Option<Value>,
}

fn default_tool_timeout() -> u64 {
    30000
}

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
    /// * `system_prompt` - Agent 的系统提示词（已注入 skills/constraints 文档）
    /// * `model` - 模型标识（如 "openai/gpt-4o"）
    /// * `tools` - 技能定义（供 Runtime 注册为实际的 tool calling 函数）
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
        tools: &[ToolDef],
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
