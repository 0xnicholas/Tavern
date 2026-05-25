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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[tokio::test]
    async fn test_memory_store_append_and_read() {
        let store = MemoryEventStore::new();
        let instance_id = "test-instance";

        store
            .append(
                instance_id,
                WorkflowEvent::InstanceCreated {
                    workflow_id: "wf1".to_string(),
                    inputs: json!({"key": "val"}),
                },
            )
            .await
            .unwrap();

        store
            .append(instance_id, WorkflowEvent::InstanceStarted)
            .await
            .unwrap();

        let events = store.read_stream(instance_id).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], WorkflowEvent::InstanceCreated { .. }));
        assert!(matches!(events[1], WorkflowEvent::InstanceStarted));
    }

    #[tokio::test]
    async fn test_memory_store_read_empty_stream() {
        let store = MemoryEventStore::new();
        let events = store.read_stream("nonexistent").await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_memory_store_list_by_status() {
        let store = MemoryEventStore::new();

        // Instance 1: Completed
        store
            .append(
                "i1",
                WorkflowEvent::InstanceCreated {
                    workflow_id: "wf".to_string(),
                    inputs: json!({}),
                },
            )
            .await
            .unwrap();
        store
            .append("i1", WorkflowEvent::InstanceStarted)
            .await
            .unwrap();
        store
            .append(
                "i1",
                WorkflowEvent::WorkflowCompleted {
                    outputs: json!({}),
                    completed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        // Instance 2: Failed
        store
            .append(
                "i2",
                WorkflowEvent::InstanceCreated {
                    workflow_id: "wf".to_string(),
                    inputs: json!({}),
                },
            )
            .await
            .unwrap();
        store
            .append("i2", WorkflowEvent::InstanceStarted)
            .await
            .unwrap();
        store
            .append(
                "i2",
                WorkflowEvent::WorkflowFailed {
                    reason: "boom".to_string(),
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let completed = store
            .list_by_status(InstanceStatus::Completed)
            .await
            .unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0], "i1");

        let failed = store.list_by_status(InstanceStatus::Failed).await.unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0], "i2");

        let running = store.list_by_status(InstanceStatus::Running).await.unwrap();
        assert!(running.is_empty());
    }

    #[tokio::test]
    async fn test_memory_store_snapshot_roundtrip() {
        let store = MemoryEventStore::new();
        let instance_id = "snap-instance";

        let mut state = InstanceState {
            id: instance_id.to_string(),
            workflow_id: "wf1".to_string(),
            status: InstanceStatus::Running,
            ..Default::default()
        };
        state.context = json!({"foo": "bar"});

        store.save_snapshot(instance_id, &state).await.unwrap();

        let loaded = store.load_snapshot(instance_id).await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, instance_id);
        assert_eq!(loaded.workflow_id, "wf1");
        assert!(matches!(loaded.status, InstanceStatus::Running));
        assert_eq!(loaded.context, json!({"foo": "bar"}));
    }

    #[tokio::test]
    async fn test_memory_store_load_snapshot_missing() {
        let store = MemoryEventStore::new();
        let loaded = store.load_snapshot("missing").await.unwrap();
        assert!(loaded.is_none());
    }
}
