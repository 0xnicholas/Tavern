use serde_json::Value;
use tavern_core::{ContentPart, ToolError, ToolHandler, ToolResult};

const MAX_RESPONSE_BYTES: usize = 1 * 1024 * 1024; // 1 MB

/// HTTP 边车工具执行器。通过 HTTP POST 调用外部工具服务。
pub struct SidecarHandler {
    url: String,
    timeout_ms: u64,
    client: reqwest::Client,
}

impl SidecarHandler {
    pub fn new(url: &str, timeout_ms: u64) -> Self {
        Self {
            url: url.to_string(),
            timeout_ms,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl ToolHandler for SidecarHandler {
    async fn execute(
        &self,
        params: Value,
        tenant_id: &str,
        session_id: &str,
        tool_call_id: &str,
    ) -> Result<ToolResult, ToolError> {
        let request = serde_json::json!({
            "params": params,
            "tool_call_id": tool_call_id,
            "session_id": session_id,
            "tenant_id": tenant_id,
        });

        let resp = self
            .client
            .post(&self.url)
            .json(&request)
            .timeout(std::time::Duration::from_millis(self.timeout_ms))
            .send()
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("sidecar unreachable: {}", e)))?;

        let status = resp.status().as_u16();
        let body_bytes = resp
            .bytes()
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("failed to read sidecar response: {}", e)))?;

        if body_bytes.len() > MAX_RESPONSE_BYTES {
            return Ok(ToolResult {
                content: vec![ContentPart {
                    content_type: "text".into(),
                    text: Some(format!(
                        "sidecar response exceeded {}MB limit",
                        MAX_RESPONSE_BYTES / (1024 * 1024)
                    )),
                }],
                is_error: true,
                details: None,
            });
        }

        let body: serde_json::Value = serde_json::from_slice(&body_bytes).map_err(|e| {
            ToolError::ExecutionFailed(format!("invalid JSON from sidecar: {}", e))
        })?;

        if status >= 400 {
            return Ok(ToolResult {
                content: vec![ContentPart {
                    content_type: "text".into(),
                    text: Some(format!("sidecar returned {}: {}", status, body)),
                }],
                is_error: true,
                details: None,
            });
        }

        serde_json::from_value(body).map_err(|e| ToolError::ExecutionFailed(e.to_string()))
    }
}
