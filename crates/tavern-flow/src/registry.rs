//! FlowRegistry — type-erased flow registration and instance creation.

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::{FlowDispatch, FlowEngine, FlowError, FlowHandle, FlowHandleRef, FlowMetadata};

/// Factory trait: creates type-erased flow instances from JSON inputs.
pub trait FlowFactory: Send + Sync {
    fn metadata(&self) -> FlowMetadata;
    fn create(&self, inputs: Value) -> Result<Box<dyn StartableFlow>, FlowError>;
}

/// Type-erased flow that can be started asynchronously.
#[async_trait]
pub trait StartableFlow: Send {
    /// Start async execution. Returns (handle, ref_handle).
    async fn start(self: Box<Self>) -> Result<(FlowHandle, FlowHandleRef), FlowError>;
}

// Blanket impl for any FlowEngine
#[async_trait]
impl<F: crate::Flow + FlowDispatch + Send + 'static> StartableFlow for FlowEngine<F> {
    async fn start(self: Box<Self>) -> Result<(FlowHandle, FlowHandleRef), FlowError> {
        Ok((*self).start_async())
    }
}

/// Lightweight, cloneable reference to a running flow instance.
#[derive(Clone, Debug, Serialize)]
pub struct FlowSummary {
    pub id: String,
    pub name: String,
    pub start_methods: Vec<String>,
}

struct RegistryEntry {
    name: String,
    factory: Box<dyn FlowFactory>,
}

/// Thread-safe registry of named flows.
pub struct FlowRegistry {
    entries: RwLock<HashMap<String, RegistryEntry>>,
}

impl FlowRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for FlowRegistry {
    fn default() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }
}

impl FlowRegistry {
    pub async fn register(&self, id: &str, name: &str, factory: Box<dyn FlowFactory>) {
        self.entries.write().await.insert(
            id.to_string(),
            RegistryEntry {
                name: name.to_string(),
                factory,
            },
        );
    }

    pub async fn list(&self) -> Vec<FlowSummary> {
        self.entries
            .read()
            .await
            .iter()
            .map(|(id, entry)| {
                let meta = entry.factory.metadata();
                FlowSummary {
                    id: id.clone(),
                    name: entry.name.clone(),
                    start_methods: meta
                        .methods
                        .iter()
                        .filter(|m| m.is_start)
                        .map(|m| m.name.clone())
                        .collect(),
                }
            })
            .collect()
    }

    pub async fn get(&self, id: &str) -> Option<FlowSummary> {
        self.entries.read().await.get(id).map(|entry| {
            let meta = entry.factory.metadata();
            FlowSummary {
                id: id.to_string(),
                name: entry.name.clone(),
                start_methods: meta
                    .methods
                    .iter()
                    .filter(|m| m.is_start)
                    .map(|m| m.name.clone())
                    .collect(),
            }
        })
    }

    pub async fn create_instance(
        &self,
        id: &str,
        inputs: Value,
    ) -> Result<Box<dyn StartableFlow>, FlowError> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(id)
            .ok_or_else(|| FlowError::Other(format!("flow '{}' not found", id)))?;
        entry.factory.create(inputs)
    }
}
