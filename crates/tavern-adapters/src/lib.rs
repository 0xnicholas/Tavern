pub mod mock;
pub mod pandaria;

pub use mock::MockRuntime;
pub use pandaria::PandariaRuntime;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tavern_core::{Runtime, RuntimeError};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_pandaria_success() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/execute"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": "ok"})))
            .mount(&mock_server)
            .await;

        let runtime = PandariaRuntime::new(mock_server.uri()).unwrap();
        let result = runtime.execute("agent1", "task1", None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!({"result": "ok"}));
    }

    #[tokio::test]
    async fn test_pandaria_4xx_error() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/execute"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&mock_server)
            .await;

        let runtime = PandariaRuntime::new(mock_server.uri()).unwrap();
        let result = runtime.execute("agent1", "task1", None).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::RequestFailed { status, body } => {
                assert_eq!(status, 404);
                assert_eq!(body, "not found");
            }
            other => panic!("expected RequestFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_pandaria_5xx_error() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/execute"))
            .respond_with(ResponseTemplate::new(502).set_body_string("bad gateway"))
            .mount(&mock_server)
            .await;

        let runtime = PandariaRuntime::new(mock_server.uri()).unwrap();
        let result = runtime.execute("agent1", "task1", None).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::RequestFailed { status, body } => {
                assert_eq!(status, 502);
                assert_eq!(body, "bad gateway");
            }
            other => panic!("expected RequestFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_pandaria_invalid_json_response() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/execute"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&mock_server)
            .await;

        let runtime = PandariaRuntime::new(mock_server.uri()).unwrap();
        let result = runtime.execute("agent1", "task1", None).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::InvalidResponse(_) => {}
            other => panic!("expected InvalidResponse, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_mock_runtime_success() {
        let runtime = MockRuntime::new(|_agent_id, _task, _context| Ok(json!({"mock_result": 42})));
        let result = runtime.execute("agent1", "task1", None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!({"mock_result": 42}));
    }

    #[tokio::test]
    async fn test_mock_runtime_error() {
        let runtime = MockRuntime::new(|_agent_id, _task, _context| {
            Err(RuntimeError::RequestFailed {
                status: 500,
                body: "mock error".to_string(),
            })
        });
        let result = runtime.execute("agent1", "task1", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pandaria_with_context() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/execute"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"echo": true})))
            .mount(&mock_server)
            .await;

        let runtime = PandariaRuntime::new(mock_server.uri()).unwrap();
        let result = runtime
            .execute("agent1", "task1", Some(json!({"key": "value"})))
            .await;
        assert!(result.is_ok());
    }
}
