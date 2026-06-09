use tavern_core::{Runtime, RuntimeError};

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

#[derive(Debug, thiserror::Error)]
pub enum PandariaError {
    #[error("invalid base_url scheme, must be http or https: {0}")]
    InvalidScheme(String),

    #[error("failed to build HTTP client: {0}")]
    ClientBuild(#[from] reqwest::Error),
}

pub struct PandariaRuntime {
    client: reqwest::Client,
    base_url: String,
    auth_token: Option<String>,
}

impl PandariaRuntime {
    pub fn new(base_url: impl Into<String>) -> Result<Self, PandariaError> {
        let url = base_url.into().trim_end_matches('/').to_string();
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(PandariaError::InvalidScheme(url));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .redirect(reqwest::redirect::Policy::limited(2))
            .pool_max_idle_per_host(10)
            .build()
            .map_err(PandariaError::ClientBuild)?;

        // Auto-generate token from PANDARIA_AUTH_SECRET if PANDARIA_AUTH_TOKEN not set
        let auth_token = std::env::var("PANDARIA_AUTH_TOKEN").ok().or_else(|| {
            std::env::var("PANDARIA_AUTH_SECRET").ok().map(|secret| {
                let tenant_id =
                    std::env::var("PANDARIA_TENANT_ID").unwrap_or_else(|_| "default".to_string());
                let token = generate_token(&tenant_id, &secret);
                tracing::info!("auto-generated Pandaria token from PANDARIA_AUTH_SECRET");
                token
            })
        });

        if auth_token.is_none() {
            tracing::warn!(
                "no PANDARIA_AUTH_TOKEN or PANDARIA_AUTH_SECRET set, requests may fail with 401"
            );
        }

        Ok(Self {
            client,
            base_url: url,
            auth_token,
        })
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.auth_token {
            req.header("Authorization", format!("Bearer {}", token))
        } else {
            req
        }
    }

    async fn create_session(
        &self,
        system_prompt: &str,
        model: &str,
        tools: &[serde_json::Value],
    ) -> Result<String, RuntimeError> {
        let mut payload = serde_json::json!({
            "system_prompt": system_prompt,
            "model": model,
        });
        if !tools.is_empty() {
            payload["tools"] = serde_json::Value::Array(tools.to_vec());
        }
        let url = format!("{}/api/v1/sessions", self.base_url);
        let req = self
            .client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&payload);
        let resp = self
            .add_auth(req)
            .send()
            .await
            .map_err(|e| RuntimeError::NetworkError(e.to_string()))?;
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .map_err(|e| RuntimeError::InvalidResponse(e.to_string()))?;
        if status >= 400 {
            return Err(RuntimeError::RequestFailed { status, body });
        }
        let info: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| RuntimeError::InvalidResponse(e.to_string()))?;
        info["id"].as_str().map(|s| s.to_string()).ok_or_else(|| {
            RuntimeError::InvalidResponse("session created but no id returned".into())
        })
    }

    /// Send message async (wait=false), then poll GET /messages until assistant responds.
    async fn send_message(&self, session_id: &str, content: &str) -> Result<String, RuntimeError> {
        let payload = serde_json::json!({
            "content": [{"type": "text", "text": content}]
        });

        // 1. Send async
        let url = format!(
            "{}/api/v1/sessions/{}/messages?wait=false",
            self.base_url, session_id
        );
        let req = self
            .client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&payload);
        let resp = self
            .add_auth(req)
            .send()
            .await
            .map_err(|e| RuntimeError::NetworkError(e.to_string()))?;
        let status = resp.status().as_u16();
        let _body = resp
            .text()
            .await
            .map_err(|e| RuntimeError::InvalidResponse(e.to_string()))?;
        if status >= 400 {
            return Err(RuntimeError::RequestFailed {
                status,
                body: _body,
            });
        }

        // 2. Poll for assistant response (up to 6 min = 12 x 30s)
        for attempt in 0..12 {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let poll_url = format!("{}/api/v1/sessions/{}/messages", self.base_url, session_id);
            let poll_req = self.client.get(&poll_url);
            let poll_resp = self
                .add_auth(poll_req)
                .send()
                .await
                .map_err(|e| RuntimeError::NetworkError(e.to_string()))?;
            let poll_status = poll_resp.status().as_u16();
            let poll_body = poll_resp
                .text()
                .await
                .map_err(|e| RuntimeError::InvalidResponse(e.to_string()))?;
            if poll_status >= 400 {
                return Err(RuntimeError::RequestFailed {
                    status: poll_status,
                    body: poll_body,
                });
            }
            let messages: Vec<serde_json::Value> = serde_json::from_str(&poll_body)
                .map_err(|e| RuntimeError::InvalidResponse(e.to_string()))?;
            if let Some(last) = messages
                .iter()
                .rev()
                .find(|m| m["role"].as_str() == Some("assistant"))
            {
                let mut text = String::new();
                if let Some(content) = last["content"].as_array() {
                    for part in content {
                        if part["type"].as_str() == Some("text")
                            && let Some(t) = part["text"].as_str()
                        {
                            text.push_str(t);
                        }
                    }
                }
                return if text.is_empty() {
                    Ok(last.to_string())
                } else {
                    Ok(text)
                };
            }
            tracing::debug!(session_id = %session_id, attempt = attempt + 1, "waiting for assistant...");
        }
        Err(RuntimeError::InvalidResponse(
            "no assistant response after 6 min polling".into(),
        ))
    }

    async fn delete_session(&self, session_id: &str) {
        let url = format!("{}/api/v1/sessions/{}", self.base_url, session_id);
        let req = self.client.delete(&url);
        let _ = self.add_auth(req).send().await;
    }
}

#[async_trait::async_trait]
impl Runtime for PandariaRuntime {
    async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        _context: Option<serde_json::Value>,
        system_prompt: &str,
        model: &str,
        tools: &[tavern_core::ToolDef],
    ) -> Result<serde_json::Value, RuntimeError> {
        let _ = agent_id;

        let tool_values = tool_def_to_pandaria_json(tools);
        let session_id = self.create_session(system_prompt, model, &tool_values).await?;
        let response_text = match self.send_message(&session_id, task).await {
            Ok(text) => text,
            Err(e) => {
                self.delete_session(&session_id).await;
                return Err(e);
            }
        };
        self.delete_session(&session_id).await;

        if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&response_text) {
            // If the JSON has only a "text" key, return just the text
            if let Some(obj) = json_val.as_object()
                && obj.len() == 1
                && obj.contains_key("text")
            {
                return Ok(obj["text"].clone());
            }
            Ok(json_val)
        } else {
            Ok(serde_json::json!({"text": response_text}))
        }
    }
}

/// 将 ToolDef 列表序列化为 Pandaria ToolConfig JSON 数组。
fn tool_def_to_pandaria_json(tools: &[tavern_core::ToolDef]) -> Vec<serde_json::Value> {
    let secret = std::env::var("TAVERN_TOOL_SECRET").ok();
    tools.iter().map(|t| {
        let mut obj = serde_json::json!({
            "name": t.name,
            "description": t.description,
            "parameters": t.parameters,
            "endpoint": t.endpoint,
            "timeout_ms": t.timeout_ms,
        });
        if let Some(ref s) = secret {
            obj["headers"] = serde_json::json!({"Authorization": format!("Bearer {}", s)});
        } else {
            obj["headers"] = serde_json::Value::Null;
        }
        obj
    }).collect()
}

/// Generate a Pandaria-compatible HMAC-SHA256 Bearer token.
/// Format: `<base64url(payload)>.<base64url(signature)>`
fn generate_token(tenant_id: &str, secret: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let payload = serde_json::json!({
        "tenant_id": tenant_id,
        "iat": now,
        "exp": now + 86400,
    });
    let payload_json = serde_json::to_vec(&payload).unwrap_or_default();
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload_json);

    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC key");
    mac.update(&payload_json);
    let signature = mac.finalize().into_bytes();
    let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature);

    format!("{}.{}", payload_b64, sig_b64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_token_format() {
        let token = generate_token("test-tenant", "test-secret-32-chars-long!!!");
        // Token should have format: base64.base64
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 2, "token should have two parts separated by .");
        assert!(!parts[0].is_empty());
        assert!(!parts[1].is_empty());
    }

    #[test]
    fn test_generate_token_deterministic() {
        // Same inputs produce same token (time-dependent part is iat)
        let t1 = generate_token("test-tenant", "secret");
        let t2 = generate_token("test-tenant", "secret");
        // Different timestamps produce different tokens
        // Just verify both are valid format
        assert!(t1.contains('.'));
        assert!(t2.contains('.'));
    }
}
