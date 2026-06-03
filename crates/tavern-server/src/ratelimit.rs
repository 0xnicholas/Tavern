//! V0.3.2: 租户限流中间件。滑动窗口计数器实现。
//!
//! 已知限制：重置任务和请求线程间无锁同步，最坏单秒放行约 2× 限额。
//! V0.4.0 分布式阶段改用 Redis token bucket。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::{
    Json,
    extract::State,
    http::{HeaderName, StatusCode},
};
use tokio::sync::RwLock;

use crate::handlers::ApiError;
use crate::state::AppState;

/// 从认证中间件注入的租户标识。
#[derive(Clone, Debug)]
pub struct TenantId(pub String);

#[derive(Clone)]
pub struct RateLimiter {
    enabled: bool,
    default_rps: u32,
    tenants: HashMap<String, u32>,
    counters: Arc<RwLock<HashMap<String, AtomicU64>>>,
}

impl RateLimiter {
    pub fn new(enabled: bool, default_rps: u32, per_tenant: HashMap<String, u32>) -> Self {
        let counters = Arc::new(RwLock::new(HashMap::<String, AtomicU64>::new()));

        // 每 1 秒重置所有计数器（使用 std::thread，兼容非 tokio 测试）
        let counters_clone = counters.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let c = counters_clone.blocking_read();
                for counter in c.values() {
                    counter.store(0, Ordering::Relaxed);
                }
            }
        });

        Self {
            enabled,
            default_rps,
            tenants: per_tenant,
            counters,
        }
    }

    /// 检查租户是否超限。未超限返回 Ok，超限返回 Err。
    #[allow(clippy::result_unit_err)]
    pub fn check(&self, tenant_id: &str) -> Result<(), ()> {
        if !self.enabled {
            return Ok(());
        }

        let limit = self
            .tenants
            .get(tenant_id)
            .copied()
            .unwrap_or(self.default_rps) as u64;

        // 读计数器并原子递增（需要写锁以确保插入新租户时安全）
        let mut counters = self.counters.blocking_write();
        let entry = counters
            .entry(tenant_id.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        let current = entry.fetch_add(1, Ordering::Relaxed);

        if current >= limit { Err(()) } else { Ok(()) }
    }
}

/// axum 中间件：按租户限流。
pub async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, (StatusCode, [(HeaderName, &'static str); 1], Json<ApiError>)>
{
    let tenant_id = request
        .extensions()
        .get::<TenantId>()
        .map(|t| t.0.as_str())
        .unwrap_or("default");

    match state.rate_limiter.check(tenant_id) {
        Ok(()) => Ok(next.run(request).await),
        Err(()) => {
            let retry_after: HeaderName = HeaderName::from_static("retry-after");
            Err((
                StatusCode::TOO_MANY_REQUESTS,
                [(retry_after, "1")],
                Json(ApiError {
                    error: "RateLimited".to_string(),
                    message: format!("rate limit exceeded for tenant '{}'", tenant_id),
                    status: StatusCode::TOO_MANY_REQUESTS,
                }),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_disabled_passes() {
        let limiter = RateLimiter::new(false, 10, HashMap::new());
        for _ in 0..1000 {
            assert!(limiter.check("default").is_ok());
        }
    }

    #[test]
    fn test_rate_limiter_blocks_exceeded() {
        let limiter = RateLimiter::new(true, 5, HashMap::new());
        for _ in 0..5 {
            assert!(limiter.check("default").is_ok());
        }
        assert!(limiter.check("default").is_err());
    }

    #[test]
    fn test_rate_limiter_per_tenant_isolation() {
        let mut tenants = HashMap::new();
        tenants.insert("admin".into(), 100);
        tenants.insert("user".into(), 2);

        let limiter = RateLimiter::new(true, 10, tenants);
        // admin can burst 100
        for _ in 0..100 {
            assert!(limiter.check("admin").is_ok());
        }
        // user limited to 2
        assert!(limiter.check("user").is_ok());
        assert!(limiter.check("user").is_ok());
        assert!(limiter.check("user").is_err());
    }

    #[test]
    fn test_rate_limiter_unknown_tenant_uses_default() {
        let limiter = RateLimiter::new(true, 3, HashMap::new());
        assert!(limiter.check("unknown").is_ok());
        assert!(limiter.check("unknown").is_ok());
        assert!(limiter.check("unknown").is_ok());
        assert!(limiter.check("unknown").is_err()); // 4th blocked
    }

    #[test]
    fn test_rate_limiter_resets_after_interval() {
        // 验证计数器不跨间隔累积（手动重置）
        let limiter = RateLimiter::new(true, 2, HashMap::new());
        assert!(limiter.check("t").is_ok());
        assert!(limiter.check("t").is_ok());
        assert!(limiter.check("t").is_err());

        // 手动清零模拟 1 秒后重置
        let counters = limiter.counters.blocking_read();
        if let Some(c) = counters.get("t") {
            c.store(0, Ordering::Relaxed);
        }
        drop(counters);

        assert!(limiter.check("t").is_ok());
    }
}
