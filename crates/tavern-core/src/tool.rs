use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 工具执行错误。
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("invalid params: {0}")]
    InvalidParams(String),

    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    #[error("tool not found: {0}")]
    NotFound(String),
}

/// 工具执行结果，序列化为 Pandaria 回调响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub content: Vec<ContentPart>,
    pub is_error: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// 返回给 LLM 的内容片段。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentPart {
    #[serde(rename = "type")]
    pub content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// 工具执行 handler trait。
/// 每个工具（web_search, code_exec 等）实现此 trait。
#[async_trait::async_trait]
pub trait ToolHandler: Send + Sync {
    /// 执行工具调用。
    ///
    /// # Arguments
    /// * `params` — LLM 传入的 JSON 参数
    /// * `tenant_id` — 租户标识（用于租户级配置和审计）
    /// * `session_id` — Pandaria session ID（用于关联上下文）
    /// * `tool_call_id` — Pandaria tool call ID（用于去重/审计）
    async fn execute(
        &self,
        params: Value,
        tenant_id: &str,
        session_id: &str,
        tool_call_id: &str,
    ) -> Result<ToolResult, ToolError>;
}

/// 工具注册表，线程安全。
///
/// 启动时注册内置 handler，运行时通过 name 查找。
/// `RwLock` 内层：启动后只读（主线程注册 → Arc 共享 → 多线程并发读）。
#[derive(Default)]
pub struct ToolRegistry {
    handlers: RwLock<HashMap<String, Arc<dyn ToolHandler>>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            handlers: RwLock::new(HashMap::new()),
        }
    }

    /// 注册工具 handler。
    /// 同名覆盖（最后注册的生效）。
    pub fn register(&self, name: String, handler: Arc<dyn ToolHandler>) {
        self.handlers.write().unwrap().insert(name, handler);
    }

    /// 按名称查找工具 handler。
    pub fn get(&self, name: &str) -> Option<Arc<dyn ToolHandler>> {
        self.handlers.read().unwrap().get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockTool;

    #[async_trait::async_trait]
    impl ToolHandler for MockTool {
        async fn execute(
            &self,
            _params: Value,
            _tenant_id: &str,
            _session_id: &str,
            _tool_call_id: &str,
        ) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                content: vec![],
                is_error: false,
                details: None,
            })
        }
    }

    #[test]
    fn test_registry_register_and_get() {
        let registry = ToolRegistry::new();
        let handler = Arc::new(MockTool);

        registry.register("mock_tool".to_string(), handler.clone());
        let found = registry.get("mock_tool");
        assert!(found.is_some());

        // 同一个 Arc（指针相等）
        let found = found.unwrap();
        assert_eq!(Arc::as_ptr(&found), Arc::as_ptr(&handler));
    }

    #[test]
    fn test_registry_get_nonexistent() {
        let registry = ToolRegistry::new();
        let found = registry.get("nonexistent");
        assert!(found.is_none());
    }

    #[test]
    fn test_registry_register_overwrite() {
        let registry = ToolRegistry::new();
        let handler1 = Arc::new(MockTool);
        let handler2 = Arc::new(MockTool);

        registry.register("tool".to_string(), handler1);
        registry.register("tool".to_string(), handler2.clone());

        let found = registry.get("tool").unwrap();
        assert_eq!(Arc::as_ptr(&found), Arc::as_ptr(&handler2));
    }
}
