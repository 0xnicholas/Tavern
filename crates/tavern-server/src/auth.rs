use axum::{
    extract::{Query, Request},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SseQuery {
    api_key: Option<String>,
}

pub async fn auth_middleware(
    Query(query): Query<SseQuery>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let state = req
        .extensions()
        .get::<Arc<tavern_config::AuthConfig>>()
        .cloned();
    let config = match state {
        Some(c) => c,
        None => return Ok(next.run(req).await),
    };

    let authorized = match config.auth_type.as_str() {
        "none" => true,
        "api_key" => check_api_key(&headers, &query, &config.keys),
        "bearer" => check_bearer(&headers, &config.jwt_secret),
        _ => true,
    };

    if authorized {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn check_api_key(headers: &HeaderMap, query: &SseQuery, valid_keys: &[String]) -> bool {
    let key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| query.api_key.clone());

    match key {
        Some(k) => valid_keys.contains(&k),
        None => false,
    }
}

fn check_bearer(headers: &HeaderMap, jwt_secret: &str) -> bool {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));

    match token {
        Some(t) => {
            let validation = Validation::default();
            decode::<Claims>(
                t,
                &DecodingKey::from_secret(jwt_secret.as_bytes()),
                &validation,
            )
            .is_ok()
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_api_key_valid() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "sk-abc".parse().unwrap());
        assert!(check_api_key(
            &headers,
            &SseQuery { api_key: None },
            &["sk-abc".to_string()]
        ));
    }

    #[test]
    fn test_check_api_key_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "bad".parse().unwrap());
        assert!(!check_api_key(
            &headers,
            &SseQuery { api_key: None },
            &["sk-abc".to_string()]
        ));
    }

    #[test]
    fn test_check_api_key_from_query() {
        let headers = HeaderMap::new();
        assert!(check_api_key(
            &headers,
            &SseQuery {
                api_key: Some("sk-abc".to_string())
            },
            &["sk-abc".to_string()]
        ));
    }
}
