#![allow(dead_code, unused_imports)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    response::{sse::Event, Sse},
};
use serde::Deserialize;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;

use tavern_comp::{CompError, EventStore, WorkflowEvent};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct SseAuthQuery {
    pub api_key: Option<String>,
}

/// BroadcastingEventStore: 包装任意 EventStore，在 append 成功后广播事件。
pub struct BroadcastingEventStore {
    inner: Arc<dyn EventStore>,
    broadcasts: Arc<RwLock<HashMap<String, broadcast::Sender<WorkflowEvent>>>>,
}

impl BroadcastingEventStore {
    pub fn new(inner: Arc<dyn EventStore>) -> Self {
        Self {
            inner,
            broadcasts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_or_create_sender(&self, instance_id: &str) -> broadcast::Receiver<WorkflowEvent> {
        let _broadcasts = self.broadcasts.clone();
        let _id = instance_id.to_string();
        unreachable!("use subscribe method instead")
    }

    pub async fn subscribe(&self, instance_id: &str) -> broadcast::Receiver<WorkflowEvent> {
        let mut broadcasts = self.broadcasts.write().await;
        let sender = broadcasts
            .entry(instance_id.to_string())
            .or_insert_with(|| broadcast::channel::<WorkflowEvent>(128).0);
        sender.subscribe()
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
    Path(_id): Path<String>,
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

    // For SSE with BroadcastingEventStore, we'd need the store to support subscription.
    // Since AppState.event_store is Arc<dyn EventStore>, we cannot directly access BroadcastingEventStore.
    // This handler serves as the endpoint; actual SSE streaming requires the store to be a BroadcastingEventStore.
    // For now, return an empty SSE stream as placeholder (full integration in Phase 4 follow-up).
    let stream = tokio_stream::iter(vec![]);
    Ok(Sse::new(stream))
}

use axum::http::StatusCode;
