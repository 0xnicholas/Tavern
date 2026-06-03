use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tavern_comp::{WorkflowEvent, WorkflowRegistry};
use tavern_hero::TavernHero;
use tokio::sync::{RwLock, broadcast, mpsc};

use crate::ratelimit::RateLimiter;
use crate::scheduler::Scheduler;

pub type EventBroadcasts = Arc<RwLock<HashMap<String, broadcast::Sender<WorkflowEvent>>>>;

#[derive(Clone)]
pub struct AppState {
    pub hero: Arc<TavernHero>,
    pub registry: Arc<RwLock<WorkflowRegistry>>,
    /// 工作流配置目录路径
    pub workflow_config_dir: String,
    /// 工作流执行总次数
    pub workflow_executions: Arc<AtomicU64>,
    /// 工作流执行失败次数
    pub workflow_failures: Arc<AtomicU64>,
    /// 工作流执行总耗时（毫秒）
    pub workflow_duration_ms_total: Arc<AtomicU64>,
    /// 工作流执行耗时直方图桶 (ms): <100, <500, <1k, <5k, <30k, <60k, >=60k
    pub workflow_duration_buckets: [Arc<AtomicU64>; 7],
    /// 工作流最大并发数
    pub max_concurrency: usize,
    /// 事件存储
    pub event_store: Arc<dyn tavern_comp::EventStore>,
    /// 活跃执行实例的信号通道，用于 signal/cancel 操作
    pub execution_handles: Arc<RwLock<HashMap<String, mpsc::Sender<WorkflowEvent>>>>,
    /// SSE 广播注册表（用于 /executions/:id/events/stream）
    pub event_broadcasts: EventBroadcasts,
    /// V0.3.6: Cron 定时调度器
    pub scheduler: Arc<Scheduler>,
    /// V0.3.2: 租户限流器
    pub rate_limiter: RateLimiter,
    /// 运行时配置（用于认证中间件等）
    pub config: tavern_config::TavernConfig,
}
