use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    response::{Sse, sse::Event},
};
use serde::Deserialize;
use tokio::sync::{RwLock, broadcast};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};

use tavern_comp::{CompError, EventStore, WorkflowEvent};

use crate::state::{AppState, EventBroadcasts};

#[derive(Debug, Deserialize)]
pub struct SseAuthQuery {
    pub api_key: Option<String>,
}

/// BroadcastingEventStore: 包装任意 EventStore，在 append 成功后广播事件。
pub struct BroadcastingEventStore {
    inner: Arc<dyn EventStore>,
    broadcasts: EventBroadcasts,
}

impl BroadcastingEventStore {
    pub fn new(inner: Arc<dyn EventStore>) -> Self {
        Self {
            inner,
            broadcasts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_broadcasts(inner: Arc<dyn EventStore>, broadcasts: EventBroadcasts) -> Self {
        Self { inner, broadcasts }
    }

    pub async fn pre_create_sender(&self, instance_id: &str) {
        let mut broadcasts = self.broadcasts.write().await;
        broadcasts
            .entry(instance_id.to_string())
            .or_insert_with(|| broadcast::channel::<WorkflowEvent>(128).0);
    }

    pub async fn cleanup(&self, instance_id: &str) {
        let mut broadcasts = self.broadcasts.write().await;
        broadcasts.remove(instance_id);
    }
}

#[async_trait]
impl EventStore for BroadcastingEventStore {
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError> {
        self.inner.append(instance_id, event.clone()).await?;

        let broadcasts = self.broadcasts.read().await;
        if let Some(sender) = broadcasts.get(instance_id) {
            let _ = sender.send(event);
        }
        Ok(())
    }

    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError> {
        self.inner.read_stream(instance_id).await
    }

    async fn list_by_status(
        &self,
        status: tavern_comp::InstanceStatus,
    ) -> Result<Vec<String>, CompError> {
        self.inner.list_by_status(status).await
    }

    async fn save_snapshot(
        &self,
        instance_id: &str,
        state: &tavern_comp::InstanceState,
    ) -> Result<(), CompError> {
        self.inner.save_snapshot(instance_id, state).await
    }

    async fn load_snapshot(
        &self,
        instance_id: &str,
    ) -> Result<Option<tavern_comp::InstanceState>, CompError> {
        self.inner.load_snapshot(instance_id).await
    }
}

/// SSE handler: 实时推送工作流实例的事件流。
pub async fn execution_events_stream_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<SseAuthQuery>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, axum::Error>>>, StatusCode> {
    // Auth check (same logic as auth middleware)
    if state.config.auth.auth_type != "none" {
        let authorized = match state.config.auth.auth_type.as_str() {
            "api_key" => {
                let valid_keys = &state.config.auth.keys;
                match query.api_key {
                    Some(ref k) => valid_keys.contains(k),
                    None => false,
                }
            }
            _ => false,
        };
        if !authorized {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    // Subscribe to the broadcast channel for this instance.
    // If no sender exists yet, create one (the workflow may not have started yet).
    let rx = {
        let mut broadcasts = state.event_broadcasts.write().await;
        let sender = broadcasts
            .entry(id.clone())
            .or_insert_with(|| broadcast::channel::<WorkflowEvent>(128).0);
        sender.subscribe()
    };

    let stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(event) => {
            let json = serde_json::to_string(&event).ok()?;
            Some(Ok(Event::default().data(json)))
        }
        Err(_) => None,
    });

    Ok(Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default()))
}

use axum::http::StatusCode;

#[cfg(test)]
mod tests {
    use super::*;
    use tavern_comp::MemoryEventStore;

    #[tokio::test]
    async fn test_broadcasting_store_subscribe_and_receive() {
        let inner: Arc<dyn EventStore> = Arc::new(MemoryEventStore::new());
        let broadcasts = Arc::new(RwLock::new(HashMap::new()));
        let store = BroadcastingEventStore::with_broadcasts(inner, broadcasts.clone());

        // Subscribe before append
        let mut rx = {
            let (tx, rx) = broadcast::channel::<WorkflowEvent>(16);
            broadcasts.write().await.insert("inst-1".to_string(), tx);
            rx
        };

        // Append event
        let event = WorkflowEvent::InstanceStarted;
        store.append("inst-1", event.clone()).await.unwrap();

        // Should receive the event
        let received = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(received, WorkflowEvent::InstanceStarted));
    }

    #[tokio::test]
    async fn test_broadcasting_store_cleanup_removes_sender() {
        let inner: Arc<dyn EventStore> = Arc::new(MemoryEventStore::new());
        let broadcasts = Arc::new(RwLock::new(HashMap::new()));
        let store = BroadcastingEventStore::with_broadcasts(inner, broadcasts.clone());

        // Insert a sender
        let (tx, _rx) = broadcast::channel::<WorkflowEvent>(16);
        broadcasts.write().await.insert("inst-2".to_string(), tx);

        // Verify it exists
        assert!(broadcasts.read().await.contains_key("inst-2"));

        // Cleanup
        store.cleanup("inst-2").await;

        // Should be gone
        assert!(!broadcasts.read().await.contains_key("inst-2"));
    }
}
