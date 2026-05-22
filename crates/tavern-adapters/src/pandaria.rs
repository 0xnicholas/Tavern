use tavern_core::{Runtime, RuntimeError};

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
}

impl PandariaRuntime {
    pub fn new(base_url: impl Into<String>) -> Result<Self, PandariaError> {
        let url = base_url.into().trim_end_matches('/').to_string();
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(PandariaError::InvalidScheme(url));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::limited(2))
            .pool_max_idle_per_host(10)
            .build()
            .map_err(PandariaError::ClientBuild)?;
        Ok(Self {
            client,
            base_url: url,
        })
    }
}

#[async_trait::async_trait]
impl Runtime for PandariaRuntime {
    async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, RuntimeError> {
        let payload = serde_json::json!({
            "agent_id": agent_id,
            "task": task,
            "context": context.unwrap_or_else(|| serde_json::json!({})),
        });

        const MAX_PAYLOAD_BYTES: usize = 10 * 1024 * 1024; // 10 MiB
        let payload_str = payload.to_string();
        if payload_str.len() > MAX_PAYLOAD_BYTES {
            return Err(RuntimeError::InvalidResponse(format!(
                "payload exceeds max size of {} bytes",
                MAX_PAYLOAD_BYTES
            )));
        }

        let url = format!("{}/v1/agents/execute", self.base_url);
        let resp = self
            .client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload_str)
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

        serde_json::from_str(&body).map_err(|e| RuntimeError::InvalidResponse(e.to_string()))
    }
}
