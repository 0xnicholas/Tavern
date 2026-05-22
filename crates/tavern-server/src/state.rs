use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tavern_comp::{WorkflowEvent, WorkflowRegistry};
use tavern_hero::TavernHero;
use tokio::sync::{mpsc, RwLock};

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
    /// 工作流最大并发数
    pub max_concurrency: usize,
    /// 事件存储
    pub event_store: Arc<tavern_comp::MemoryEventStore>,
    /// 活跃执行实例的信号通道，用于 signal/cancel 操作
    pub execution_handles: Arc<RwLock<HashMap<String, mpsc::Sender<WorkflowEvent>>>>,
}
