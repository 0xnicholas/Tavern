pub mod mock;
pub mod pandaria;

pub use mock::MockRuntime;
pub use pandaria::PandariaRuntime;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tavern_core::{Runtime, RuntimeError};
    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_pandaria_success() {
        let mock_server = MockServer::start().await;

        // Mock session creation
        Mock::given(method("POST"))
            .and(path("/api/v1/sessions"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "id": "session-123",
                "model": "openai/gpt-4o"
            })))
            .mount(&mock_server)
            .await;

        // Mock message send (async POST)
        Mock::given(method("POST"))
            .and(path_regex(r"/api/v1/sessions/session-123/messages.*"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "turn_index": 1
            })))
            .mount(&mock_server)
            .await;

        // Mock poll for assistant response (GET)
        Mock::given(method("GET"))
            .and(path("/api/v1/sessions/session-123/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"role": "user", "content": [{"type": "text", "text": "task1"}]},
                {"role": "assistant", "content": [{"type": "text", "text": "hello from assistant"}]}
            ])))
            .mount(&mock_server)
            .await;

        // Mock session deletion
        Mock::given(method("DELETE"))
            .and(path("/api/v1/sessions/session-123"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let runtime = PandariaRuntime::new(mock_server.uri()).unwrap();
        let result = runtime
            .execute("agent1", "task1", None, "You are helpful.", "openai/gpt-4o", &[])
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!({"text": "hello from assistant"}));
    }

    #[tokio::test]
    async fn test_pandaria_session_creation_error() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/sessions"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&mock_server)
            .await;

        let runtime = PandariaRuntime::new(mock_server.uri()).unwrap();
        let result = runtime
            .execute("agent1", "task1", None, "prompt", "openai/gpt-4o", &[])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pandaria_message_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v1/sessions"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "id": "session-err",
                "model": "openai/gpt-4o"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("POST"))
            .and(path_regex(r"/api/v1/sessions/session-err/messages.*"))
            .respond_with(ResponseTemplate::new(404).set_body_string("session gone"))
            .mount(&mock_server)
            .await;

        // Session delete (cleanup after error)
        Mock::given(method("DELETE"))
            .and(path("/api/v1/sessions/session-err"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let runtime = PandariaRuntime::new(mock_server.uri()).unwrap();
        let result = runtime
            .execute("agent1", "task1", None, "prompt", "openai/gpt-4o", &[])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_runtime_success() {
        let runtime = MockRuntime::new(|_agent_id, _task, _context, _sp, _model, _tools| {
            Ok(json!({"mock_result": 42}))
        });
        let result = runtime
            .execute("agent1", "task1", None, "prompt", "gpt-4o", &[])
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!({"mock_result": 42}));
    }

    #[tokio::test]
    async fn test_mock_runtime_error() {
        let runtime = MockRuntime::new(|_a, _t, _c, _s, _m, _tools| {
            Err(RuntimeError::RequestFailed {
                status: 500,
                body: "mock error".to_string(),
            })
        });
        let result = runtime
            .execute("agent1", "task1", None, "prompt", "gpt-4o", &[])
            .await;
        assert!(result.is_err());
    }
}
