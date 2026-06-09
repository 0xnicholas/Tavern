use async_trait::async_trait;
use serde_json::Value;
use tavern_core::{ContentPart, ToolError, ToolHandler, ToolResult};

pub struct WebSearchHandler {
    client: reqwest::Client,
}

impl WebSearchHandler {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    async fn query_ddg(&self, query: &str) -> Result<Value, ToolError> {
        let url = format!(
            "https://api.duckduckgo.com/?q={}&format=json&no_html=1",
            urlencoding::encode(query)
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
        let body = resp
            .text()
            .await
            .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
        serde_json::from_str(&body)
            .map_err(|e| ToolError::ExecutionFailed(format!("JSON parse: {}", e)))
    }
}

#[async_trait]
impl ToolHandler for WebSearchHandler {
    async fn execute(
        &self,
        params: Value,
        _tenant_id: &str,
        _session_id: &str,
        _tool_call_id: &str,
    ) -> Result<ToolResult, ToolError> {
        let query = params["query"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidParams("missing 'query'".into()))?;

        let data = self.query_ddg(query).await?;
        let text = format_ddg_response(&data);

        Ok(ToolResult {
            content: vec![ContentPart {
                content_type: "text".into(),
                text: Some(text),
            }],
            is_error: false,
            details: Some(data),
        })
    }
}

fn format_ddg_response(data: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(abs) = data["AbstractText"].as_str() {
        if !abs.is_empty() {
            parts.push(format!("Summary: {}", abs));
            if let Some(url) = data["AbstractURL"].as_str() {
                parts.push(format!("Source: {}", url));
            }
        }
    }
    if let Some(topics) = data["RelatedTopics"].as_array() {
        for (i, topic) in topics.iter().enumerate() {
            if let Some(text) = topic["Text"].as_str() {
                parts.push(format!("{}. {}", i + 1, text));
            }
        }
    }
    if parts.is_empty() {
        "No results found.".to_string()
    } else {
        parts.join("\n\n")
    }
}
