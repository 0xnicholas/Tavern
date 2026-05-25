use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::error::CompError;
use crate::event::WorkflowEvent;
use crate::instance::{InstanceState, InstanceStatus};

#[async_trait]
pub trait EventStore: Send + Sync {
    /// 追加事件到指定实例的事件流。
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError>;

    /// 读取实例的完整事件流，按发生顺序返回。
    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError>;

    /// 列出指定状态的实例 ID（用于 ExecutionSupervisor 恢复）
    async fn list_by_status(&self, _status: InstanceStatus) -> Result<Vec<String>, CompError> {
        Ok(vec![])
    }

    /// 保存状态快照（可选优化）
    async fn save_snapshot(
        &self,
        instance_id: &str,
        state: &InstanceState,
    ) -> Result<(), CompError>;
    async fn load_snapshot(&self, instance_id: &str) -> Result<Option<InstanceState>, CompError>;
}

pub struct MemoryEventStore {
    streams: RwLock<HashMap<String, Vec<WorkflowEvent>>>,
    snapshots: RwLock<HashMap<String, InstanceState>>,
}

impl MemoryEventStore {
    pub fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventStore for MemoryEventStore {
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError> {
        let mut streams = self.streams.write().await;
        streams
            .entry(instance_id.to_string())
            .or_default()
            .push(event);
        Ok(())
    }

    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError> {
        let streams = self.streams.read().await;
        Ok(streams.get(instance_id).cloned().unwrap_or_default())
    }

    async fn list_by_status(&self, status: InstanceStatus) -> Result<Vec<String>, CompError> {
        let streams = self.streams.read().await;
        let mut result = Vec::new();
        for (id, events) in streams.iter() {
            let mut state = InstanceState {
                id: id.clone(),
                ..Default::default()
            };
            for event in events {
                let _ = state.apply(event);
            }
            if std::mem::discriminant(&state.status) == std::mem::discriminant(&status) {
                result.push(id.clone());
            }
        }
        Ok(result)
    }

    async fn save_snapshot(
        &self,
        instance_id: &str,
        state: &InstanceState,
    ) -> Result<(), CompError> {
        let mut snapshots = self.snapshots.write().await;
        snapshots.insert(instance_id.to_string(), state.clone());
        Ok(())
    }

    async fn load_snapshot(&self, instance_id: &str) -> Result<Option<InstanceState>, CompError> {
        let snapshots = self.snapshots.read().await;
        Ok(snapshots.get(instance_id).cloned())
    }
}
