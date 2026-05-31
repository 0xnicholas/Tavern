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

// ── SqliteEventStore ──

#[cfg(feature = "sqlite")]
pub struct SqliteEventStore {
    pool: sqlx::SqlitePool,
}

#[cfg(feature = "sqlite")]
impl SqliteEventStore {
    pub async fn new(database_url: &str) -> Result<Self, CompError> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(database_url)
            .await
            .map_err(|e| CompError::StoreError(e.to_string()))?;

        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .map_err(|e| CompError::StoreError(e.to_string()))?;

        // WAL mode must be set outside of transactions
        if database_url != ":memory:" {
            sqlx::query("PRAGMA journal_mode = WAL")
                .execute(&pool)
                .await
                .ok();
            sqlx::query("PRAGMA synchronous = NORMAL")
                .execute(&pool)
                .await
                .ok();
        }

        Ok(Self { pool })
    }

    async fn upsert_instance_meta(
        &self,
        instance_id: &str,
        workflow_id: &str,
        status: &str,
    ) -> Result<(), CompError> {
        sqlx::query(
            r#"
            INSERT INTO workflow_instances (instance_id, workflow_id, status, updated_at)
            VALUES (?1, ?2, ?3, strftime('%s', 'now') * 1000)
            ON CONFLICT(instance_id) DO UPDATE SET
                status = excluded.status,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(instance_id)
        .bind(workflow_id)
        .bind(status)
        .execute(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl EventStore for SqliteEventStore {
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError> {
        let payload =
            serde_json::to_string(&event).map_err(|e| CompError::StoreError(e.to_string()))?;

        sqlx::query("INSERT INTO workflow_events (instance_id, payload) VALUES (?1, ?2)")
            .bind(instance_id)
            .bind(&payload)
            .execute(&self.pool)
            .await
            .map_err(|e| CompError::StoreError(e.to_string()))?;

        // Update instance meta for list_by_status
        let (workflow_id, status) = match &event {
            WorkflowEvent::InstanceCreated { workflow_id, .. } => {
                (Some(workflow_id.as_str()), "pending")
            }
            WorkflowEvent::InstanceStarted => (None, "running"),
            WorkflowEvent::WorkflowCompleted { .. } => (None, "completed"),
            WorkflowEvent::WorkflowFailed { .. } => (None, "failed"),
            _ => (None, ""),
        };

        if !status.is_empty() {
            let wf_id = workflow_id.unwrap_or("");
            self.upsert_instance_meta(instance_id, wf_id, status)
                .await?;
        }

        Ok(())
    }

    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT payload FROM workflow_events WHERE instance_id = ?1 ORDER BY id ASC",
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        let mut events = Vec::new();
        for (payload,) in rows {
            let event: WorkflowEvent =
                serde_json::from_str(&payload).map_err(|e| CompError::StoreError(e.to_string()))?;
            events.push(event);
        }
        Ok(events)
    }

    async fn list_by_status(&self, status: InstanceStatus) -> Result<Vec<String>, CompError> {
        let status_str = status.as_str();
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT instance_id FROM workflow_instances WHERE status = ?1",
        )
        .bind(status_str)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    async fn save_snapshot(
        &self,
        instance_id: &str,
        state: &InstanceState,
    ) -> Result<(), CompError> {
        let state_json =
            serde_json::to_string(state).map_err(|e| CompError::StoreError(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO workflow_snapshots (instance_id, state, version)
            VALUES (?1, ?2, 1)
            ON CONFLICT(instance_id) DO UPDATE SET
                state = excluded.state,
                version = version + 1,
                updated_at = strftime('%s', 'now') * 1000
            "#,
        )
        .bind(instance_id)
        .bind(&state_json)
        .execute(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;
        Ok(())
    }

    async fn load_snapshot(&self, instance_id: &str) -> Result<Option<InstanceState>, CompError> {
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT state FROM workflow_snapshots WHERE instance_id = ?1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        match row {
            Some((state_json,)) => {
                let state: InstanceState = serde_json::from_str(&state_json)
                    .map_err(|e| CompError::StoreError(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }
}

// ── PostgreSQLEventStore ──

#[cfg(feature = "postgres")]
pub struct PostgreSQLEventStore {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgreSQLEventStore {
    pub async fn new(database_url: &str) -> Result<Self, CompError> {
        let pool = sqlx::PgPool::connect(database_url)
            .await
            .map_err(|e| CompError::StoreError(e.to_string()))?;

        sqlx::migrate!("./migrations/postgres")
            .run(&pool)
            .await
            .map_err(|e| CompError::StoreError(e.to_string()))?;

        Ok(Self { pool })
    }

    async fn upsert_instance_meta(
        &self,
        instance_id: &str,
        workflow_id: &str,
        status: &str,
    ) -> Result<(), CompError> {
        sqlx::query(
            r#"
            INSERT INTO workflow_instances (instance_id, workflow_id, status, updated_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT(instance_id) DO UPDATE SET
                status = excluded.status,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(instance_id)
        .bind(workflow_id)
        .bind(status)
        .execute(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl EventStore for PostgreSQLEventStore {
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError> {
        let payload =
            serde_json::to_string(&event).map_err(|e| CompError::StoreError(e.to_string()))?;

        sqlx::query("INSERT INTO workflow_events (instance_id, payload) VALUES ($1, $2::jsonb)")
            .bind(instance_id)
            .bind(&payload)
            .execute(&self.pool)
            .await
            .map_err(|e| CompError::StoreError(e.to_string()))?;

        // Update instance meta for list_by_status
        let (workflow_id, status) = match &event {
            WorkflowEvent::InstanceCreated { workflow_id, .. } => {
                (Some(workflow_id.as_str()), "pending")
            }
            WorkflowEvent::InstanceStarted => (None, "running"),
            WorkflowEvent::WorkflowCompleted { .. } => (None, "completed"),
            WorkflowEvent::WorkflowFailed { .. } => (None, "failed"),
            _ => (None, ""),
        };

        if !status.is_empty() {
            let wf_id = workflow_id.unwrap_or("");
            self.upsert_instance_meta(instance_id, wf_id, status)
                .await?;
        }

        Ok(())
    }

    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError> {
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT payload FROM workflow_events WHERE instance_id = $1 ORDER BY id ASC",
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        let mut events = Vec::new();
        for (payload,) in rows {
            let event: WorkflowEvent = serde_json::from_value(payload)
                .map_err(|e| CompError::StoreError(e.to_string()))?;
            events.push(event);
        }
        Ok(events)
    }

    async fn list_by_status(&self, status: InstanceStatus) -> Result<Vec<String>, CompError> {
        let status_str = status.as_str();
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT instance_id FROM workflow_instances WHERE status = $1",
        )
        .bind(status_str)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    async fn save_snapshot(
        &self,
        instance_id: &str,
        state: &InstanceState,
    ) -> Result<(), CompError> {
        let state_json =
            serde_json::to_value(state).map_err(|e| CompError::StoreError(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO workflow_snapshots (instance_id, state, version)
            VALUES ($1, $2::jsonb, 1)
            ON CONFLICT(instance_id) DO UPDATE SET
                state = excluded.state,
                version = workflow_snapshots.version + 1,
                updated_at = now()
            "#,
        )
        .bind(instance_id)
        .bind(&state_json)
        .execute(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;
        Ok(())
    }

    async fn load_snapshot(&self, instance_id: &str) -> Result<Option<InstanceState>, CompError> {
        let row = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT state FROM workflow_snapshots WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        match row {
            Some((state_json,)) => {
                let state: InstanceState = serde_json::from_value(state_json)
                    .map_err(|e| CompError::StoreError(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
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

    // ── SqliteEventStore tests ──

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn test_sqlite_store_append_and_read() {
        let store = SqliteEventStore::new(":memory:").await.unwrap();
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
    #[cfg(feature = "sqlite")]
    async fn test_sqlite_store_read_empty_stream() {
        let store = SqliteEventStore::new(":memory:").await.unwrap();
        let events = store.read_stream("nonexistent").await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn test_sqlite_store_list_by_status() {
        let store = SqliteEventStore::new(":memory:").await.unwrap();

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
    #[cfg(feature = "sqlite")]
    async fn test_sqlite_store_snapshot_roundtrip() {
        let store = SqliteEventStore::new(":memory:").await.unwrap();
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
    #[cfg(feature = "sqlite")]
    async fn test_sqlite_store_load_snapshot_missing() {
        let store = SqliteEventStore::new(":memory:").await.unwrap();
        let loaded = store.load_snapshot("missing").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn test_sqlite_store_upsert_instance_meta() {
        let store = SqliteEventStore::new(":memory:").await.unwrap();

        store
            .upsert_instance_meta("i1", "wf1", "pending")
            .await
            .unwrap();
        store
            .upsert_instance_meta("i1", "wf1", "running")
            .await
            .unwrap();

        let running = store.list_by_status(InstanceStatus::Running).await.unwrap();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0], "i1");
    }

    // ── PostgreSQLEventStore tests ──
    // 需要 TEST_POSTGRES_URL 环境变量（如: postgres://tavern:tavern@localhost:5432/tavern）

    #[allow(dead_code)]
    fn pg_test_url() -> Option<String> {
        std::env::var("TEST_POSTGRES_URL").ok()
    }

    #[tokio::test]
    #[cfg(feature = "postgres")]
    async fn test_pg_store_append_and_read() {
        let url = match pg_test_url() {
            Some(u) => u,
            None => {
                eprintln!("SKIP: TEST_POSTGRES_URL not set");
                return;
            }
        };
        let store = PostgreSQLEventStore::new(&url).await.unwrap();
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
    #[cfg(feature = "postgres")]
    async fn test_pg_store_read_empty_stream() {
        let url = match pg_test_url() {
            Some(u) => u,
            None => return,
        };
        let store = PostgreSQLEventStore::new(&url).await.unwrap();
        let events = store.read_stream("nonexistent").await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    #[cfg(feature = "postgres")]
    async fn test_pg_store_list_by_status() {
        let url = match pg_test_url() {
            Some(u) => u,
            None => return,
        };
        let store = PostgreSQLEventStore::new(&url).await.unwrap();

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
    }

    #[tokio::test]
    #[cfg(feature = "postgres")]
    async fn test_pg_store_snapshot_roundtrip() {
        let url = match pg_test_url() {
            Some(u) => u,
            None => return,
        };
        let store = PostgreSQLEventStore::new(&url).await.unwrap();
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
    #[cfg(feature = "postgres")]
    async fn test_pg_store_load_snapshot_missing() {
        let url = match pg_test_url() {
            Some(u) => u,
            None => return,
        };
        let store = PostgreSQLEventStore::new(&url).await.unwrap();
        let loaded = store.load_snapshot("missing").await.unwrap();
        assert!(loaded.is_none());
    }
}
