# Tavern V0.2.0: 生产就绪

## 1. 概述

V0.2.0 将 Tavern 从 MVP 验证阶段推进到**生产可用**阶段。核心目标是解决 V0.1.0 中最关键的运行时缺口：状态丢失、配置僵化、安全裸奔和运维困难。

### 1.1 设计原则

- **状态持久化优先**：EventStore 从纯内存演进为可插拔持久化，默认 SQLite，可选 PostgreSQL
- **配置即代码**：统一配置管理（文件 + 环境变量），支持热重载，无需重启修改配置
- **安全最小化**：API Key / Bearer Token 认证，拒绝裸奔
- **标准输出**：可观测性数据（metrics / logs）输出标准格式，由外部平台消费，不自建
- **向后兼容**：V0.1.0 的 YAML 配置、HTTP API、Rust trait 全部保留
- **零外部依赖（可选）**：SQLite 为默认持久化后端，不强制 PostgreSQL/Redis

### 1.2 与 V0.1.0 的核心差异

| 维度 | V0.1.0 | V0.2.0 |
|------|--------|--------|
| 状态存储 | 内存 `MemoryEventStore`，重启丢失 | SQLite / PostgreSQL，持久化保留 |
| Agent 重载 | 需重启服务 | `notify` 热重载，与 Workflow 一致 |
| API 安全 | 无认证，全公开 | API Key / Bearer Token |
| 实时事件 | 30s 轮询 `GET /messages` | SSE `/executions/:id/events/stream` |
| 配置管理 | 零散环境变量 | `figment` 统一（TOML + 环境变量） |
| 可观测性 | 3 个 Prometheus counter | 标准输出供外部平台消费 |
| 部署 | `cargo run` | Docker 镜像 + 健康检查 |

---

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| EventStore | 事件持久化抽象，V0.2.0 新增 SQLite/PostgreSQL 实现 |
| ConfigManager | 统一配置管理器，合并 TOML 文件和环境变量 |
| AuthMiddleware | API 认证中间件，支持 API Key 和 Bearer Token |
| SSE | Server-Sent Events，`text/event-stream` 实时推送 |
| Hot Reload | 文件变更时自动重新加载配置，不重启进程 |
| Snapshot | 实例状态快照，优化 EventStore 重放性能 |
| Graceful Shutdown | 优雅停机，等待活跃请求完成后再退出 |

---

## 3. 架构

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Tavern Server V0.2.0                        │
│                                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────────┐ │
│  │ Auth        │  │ Config      │  │  Hot Reload Watcher         │ │
│  │ Middleware  │  │ Manager     │  │  (agents + workflows)       │ │
│  └──────┬──────┘  └──────┬──────┘  └─────────────────────────────┘ │
│         │                │                                         │
│  ┌──────▼────────────────▼─────────────────────────────────────┐   │
│  │                         axum Router                          │   │
│  │  /health  /agents  /workflows  /executions  /metrics  /docs  │   │
│  └──────┬───────────────────────────────────────────────────────┘   │
│         │                                                           │
│  ┌──────▼───────────────────────────────────────────────────────┐   │
│  │                    tavern-comp / tavern-hero                  │   │
│  │  WorkflowEngine ──▶ EventStore ──┬──▶ SQLiteEventStore       │   │
│  │                                  └──▶ PostgresEventStore     │   │
│  │  TavernHero ──▶ AgentRegistry ──▶ notify 热重载             │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  External       │
                    │  Observability  │
                    │  (Prometheus /  │
                    │   Grafana /     │
                    │   Datadog)      │
                    └─────────────────┘
```

### 3.2 Crate 职责变化

| Crate | V0.2.0 变化 |
|-------|-------------|
| `tavern-comp` | 新增 `SqliteEventStore`、`PostgresEventStore`；`MemoryEventStore` 保留为测试默认 |
| `tavern-hero` | `TavernHero` 支持 `load_from_dir` 热重载监听器；AgentRegistry 支持原子替换 |
| `tavern-server` | 新增 `AuthMiddleware`、`ConfigManager`、`GracefulShutdown`；集成 SSE |
| *(new)* | `tavern-config`（可选）：统一配置管理 crate，解耦 server 和具体配置源 |

---

## 4. 数据模型

### 4.1 EventStore Schema

SQLite 和 PostgreSQL 使用独立的迁移目录，由 `sqlx migrate` 管理。`sqlx query!` 宏使用离线查询数据（`sqlx prepare` 生成 `.sqlx/`），避免开发环境必须连接数据库。

#### SQLite Schema（`migrations/sqlite/`）

```sql
-- SQLite 不支持 JSONB、BIGSERIAL、TIMESTAMPTZ；JSON 存为 TEXT，时间戳存为 INTEGER (unix epoch 毫秒)
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE workflow_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    payload     TEXT NOT NULL,  -- 完整 JSON，包含 "type" 字段
    created_at  INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

CREATE INDEX idx_events_instance_seq ON workflow_events(instance_id, id);

CREATE TABLE workflow_snapshots (
    instance_id TEXT PRIMARY KEY,
    state       TEXT NOT NULL,   -- JSON
    version     INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

CREATE TABLE workflow_instances (
    instance_id   TEXT PRIMARY KEY,
    workflow_id   TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    INTEGER DEFAULT (strftime('%s', 'now') * 1000),
    updated_at    INTEGER DEFAULT (strftime('%s', 'now') * 1000),
    completed_at  INTEGER
);

CREATE INDEX idx_instances_status ON workflow_instances(status);
CREATE INDEX idx_instances_workflow ON workflow_instances(workflow_id);
```

#### PostgreSQL Schema（`migrations/postgres/`）

```sql
CREATE TABLE workflow_events (
    id          BIGSERIAL PRIMARY KEY,
    instance_id UUID NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_events_instance_seq ON workflow_events(instance_id, id);

CREATE TABLE workflow_snapshots (
    instance_id UUID PRIMARY KEY,
    state       JSONB NOT NULL,
    version     INTEGER NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE workflow_instances (
    instance_id   UUID PRIMARY KEY,
    workflow_id   TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX idx_instances_status ON workflow_instances(status);
CREATE INDEX idx_instances_workflow ON workflow_instances(workflow_id);
```

### 4.2 Config Schema（TOML）

```toml
# config.toml
[server]
host = "0.0.0.0"
port = 3000
max_body_size = "1MiB"

[auth]
type = "api_key"          # "none" | "api_key" | "bearer"
keys = ["sk-tavern-xxx"]  # API Key 白名单
# token_secret = ""       # Bearer JWT secret（type="bearer" 时生效）

[store]
type = "sqlite"           # "memory" | "sqlite" | "postgres"
path = "./tavern.db"      # SQLite 路径
# url = ""                # PostgreSQL 连接字符串
# max_connections = 10    # 连接池大小

[observability]
log_format = "json"       # "pretty" | "json"
metrics_enabled = true

[runtime]
url = "http://localhost:8080"
timeout_seconds = 300

[reload]
agents = true
workflows = true
debounce_ms = 500
```

### 4.3 环境变量映射

| 环境变量 | TOML 路径 | 默认值 |
|----------|-----------|--------|
| `TAVERN_SERVER_HOST` | `server.host` | `0.0.0.0` |
| `TAVERN_SERVER_PORT` | `server.port` | `3000` |
| `TAVERN_AUTH_TYPE` | `auth.type` | `none` |
| `TAVERN_AUTH_KEYS` | `auth.keys` | `[]` |
| `TAVERN_STORE_TYPE` | `store.type` | `memory` |
| `TAVERN_STORE_PATH` | `store.path` | `./tavern.db` |
| `TAVERN_STORE_URL` | `store.url` | — |
| `TAVERN_LOG_FORMAT` | `observability.log_format` | `pretty` |
| `TAVERN_RUNTIME_URL` | `runtime.url` | — |
| `TAVERN_RELOAD_AGENTS` | `reload.agents` | `true` |
| `TAVERN_RELOAD_WORKFLOWS` | `reload.workflows` | `true` |

> 环境变量优先级高于 TOML 文件，符合 12-factor app 原则。

---

## 5. 接口定义

### 5.1 EventStore trait（扩展）

V0.1.0 的 `EventStore` trait 保持不变，新增两个实现。

```rust
// crates/tavern-comp/src/store.rs

/// 新增：SQLite 持久化实现
pub struct SqliteEventStore {
    pool: sqlx::SqlitePool,
}

impl SqliteEventStore {
    pub async fn new(path: &str) -> Result<Self, CompError> {
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);
        let pool = sqlx::SqlitePool::connect_with(options).await
            .map_err(|e| CompError::StoreError(e.to_string()))?;
        sqlx::migrate!("migrations/sqlite").run(&pool).await
            .map_err(|e| CompError::StoreError(e.to_string()))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl EventStore for SqliteEventStore {
    /// 整存整取：将 WorkflowEvent 完整序列化为 JSON（包含 serde tag 的 "type" 字段）
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError> {
        let payload = serde_json::to_string(&event)
            .map_err(|e| CompError::StoreError(e.to_string()))?;
        sqlx::query(
            "INSERT INTO workflow_events (instance_id, payload) VALUES (?1, ?2)"
        )
        .bind(instance_id)
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        // 同步更新实例元数据（辅助表加速 list_by_status）
        self.upsert_instance_meta(instance_id, &event).await?;
        Ok(())
    }

    /// 读取完整 JSON 后直接反序列化（serde tag 自动识别变体）
    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT payload FROM workflow_events WHERE instance_id = ?1 ORDER BY id"
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        rows.into_iter()
            .map(|(payload,)| serde_json::from_str(&payload)
                .map_err(|e| CompError::StoreError(e.to_string())))
            .collect()
    }

    /// 利用辅助表实现 O(1) 状态筛选（避免全表重建）
    async fn list_by_status(&self, status: InstanceStatus) -> Result<Vec<String>, CompError> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT instance_id FROM workflow_instances WHERE status = ?1"
        )
        .bind(status.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    async fn save_snapshot(&self, instance_id: &str, state: &InstanceState) -> Result<(), CompError> {
        let state_json = serde_json::to_string(state)
            .map_err(|e| CompError::StoreError(e.to_string()))?;
        sqlx::query(
            "INSERT INTO workflow_snapshots (instance_id, state, version)
             VALUES (?1, ?2, 0)
             ON CONFLICT(instance_id) DO UPDATE SET
             state = excluded.state, version = version + 1, updated_at = (strftime('%s', 'now') * 1000)"
        )
        .bind(instance_id)
        .bind(state_json)
        .execute(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;
        Ok(())
    }

    async fn load_snapshot(&self, instance_id: &str) -> Result<Option<InstanceState>, CompError> {
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT state FROM workflow_snapshots WHERE instance_id = ?1"
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CompError::StoreError(e.to_string()))?;

        match row {
            Some((json,)) => serde_json::from_str(&json)
                .map_err(|e| CompError::StoreError(e.to_string())),
            None => Ok(None),
        }
    }
}
```

**PostgreSQLEventStore**：结构与 `SqliteEventStore` 基本一致，差异：
- 使用 `sqlx::PgPool` 和 `$1` 占位符
- 时间戳函数用 `now()` 而非 `strftime`
- 迁移目录：`migrations/postgres/`
- 条件编译：`#[cfg(feature = "postgres")]`

### 5.2 ConfigManager（新增）

```rust
// crates/tavern-server/src/config.rs（或新增 tavern-config crate）

use figment::{Figment, providers::{Toml, Env, Format}};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct TavernConfig {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub store: StoreConfig,
    pub observability: ObservabilityConfig,
    pub runtime: RuntimeConfig,
    pub reload: ReloadConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    #[serde(default = "default_auth_type")]
    pub auth_type: String,  // "none" | "api_key" | "bearer"
    /// API Key 白名单。TOML 中写数组，环境变量中用逗号分隔：
    /// `TAVERN_AUTH_KEYS="sk-a,sk-b,sk-c"`
    #[serde(default, deserialize_with = "deserialize_comma_separated")]
    pub keys: Vec<String>,
    /// Bearer JWT 签名密钥（HS256），type="bearer" 时生效
    pub token_secret: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreConfig {
    #[serde(default = "default_store_type")]
    pub store_type: String,  // "memory" | "sqlite" | "postgres"
    pub path: Option<String>,
    pub url: Option<String>,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_log_format")]
    pub log_format: String,  // "pretty" | "json"
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,
    /// 是否公开 `/metrics`（无需认证）。生产环境建议设为 false
    #[serde(default = "default_true")]
    pub metrics_public: bool,
}

/// 逗号分隔字符串 → Vec<String>
fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

impl TavernConfig {
    pub fn load() -> Result<Self, figment::Error> {
        let mut figment = Figment::new()
            .merge(Toml::file("config.toml"))
            .merge(Env::prefixed("TAVERN_"));

        // ── V0.1.0 向后兼容：旧环境变量映射到新路径 ──
        // 旧变量在 V0.3.0 中废弃，V0.2.0 兼容期内仍有效
        if let Ok(url) = std::env::var("RUNTIME_URL") {
            figment = figment.merge(("runtime", "url", url));
        }
        if let Ok(dir) = std::env::var("AGENT_CONFIG_DIR") {
            figment = figment.merge(("server", "agent_config_dir", dir));
        }
        if let Ok(dir) = std::env::var("WORKFLOW_CONFIG_DIR") {
            figment = figment.merge(("server", "workflow_config_dir", dir));
        }
        if let Ok(host) = std::env::var("SERVER_HOST") {
            figment = figment.merge(("server", "host", host));
        }
        if let Ok(port) = std::env::var("SERVER_PORT") {
            figment = figment.merge(("server", "port", port));
        }

        figment.extract()
    }
}
```

### 5.3 AuthMiddleware（新增）

```rust
// crates/tavern-server/src/auth.rs

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
    http::{StatusCode, header},
};

/// Bearer Token claim 格式（HS256 JWT）
#[derive(Debug, serde::Deserialize)]
struct Claims {
    sub: String,    // tenant_id / user_id
    exp: usize,     // Unix timestamp
}

pub async fn auth_middleware(
    State(config): State<AuthConfig>,
    request: Request,
    next: Next,
) -> Response {
    match config.auth_type.as_str() {
        "none" => next.run(request).await,
        "api_key" => {
            let key = request.headers()
                .get("x-api-key")
                .and_then(|v| v.to_str().ok());
            match key {
                Some(k) if config.keys.contains(&k.to_string()) => next.run(request).await,
                _ => (StatusCode::UNAUTHORIZED, "Invalid API Key").into_response(),
            }
        }
        "bearer" => {
            let token = request.headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            match validate_bearer(token, config.token_secret.as_deref()) {
                Ok(_) => next.run(request).await,
                Err(_) => (StatusCode::UNAUTHORIZED, "Invalid Bearer Token").into_response(),
            }
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "Unknown auth type").into_response(),
    }
}

fn validate_bearer(
    token: Option<&str>,
    secret: Option<&str>,
) -> Result<(), jsonwebtoken::errors::Error> {
    let token = token.ok_or_else(|| jsonwebtoken::errors::Error::from(
        jsonwebtoken::errors::ErrorKind::InvalidToken
    ))?;
    let secret = secret.ok_or_else(|| jsonwebtoken::errors::Error::from(
        jsonwebtoken::errors::ErrorKind::InvalidIssuer
    ))?;
    let _ = jsonwebtoken::decode::<Claims>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
        &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256),
    )?;
    Ok(())
}
```

**新增依赖**（`tavern-server/Cargo.toml`）：
```toml
jsonwebtoken = "9"
```

### 5.4 SSE Handler（新增）

### 5.4 SSE 与 EventStore 桥接（新增）

SSE 广播与 EventStore 通过 `BroadcastingEventStore` 包装器桥接。每次 `append` 事件后自动广播到订阅者，避免内存泄漏（实例完成后广播发送器自动失效，无需清理注册表）。

```rust
// crates/tavern-server/src/sse.rs

use axum::{
    extract::{Path, Query, State},
    response::sse::{Event, Sse},
};
use futures::stream::Stream;
use std::collections::HashMap;
use std::convert::Infallible;

/// AppState 中的广播注册表（仅保留活跃实例）
pub type EventBroadcasts = Arc<RwLock<HashMap<String, broadcast::Sender<WorkflowEvent>>>>;

/// BroadcastingEventStore：在 append 后自动广播事件
pub struct BroadcastingEventStore {
    inner: Arc<dyn EventStore>,
    broadcasts: EventBroadcasts,
}

#[async_trait]
impl EventStore for BroadcastingEventStore {
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError> {
        self.inner.append(instance_id, event.clone()).await?;
        // 广播到订阅者（忽略无订阅者的错误）
        if let Some(tx) = self.broadcasts.read().await.get(instance_id) {
            let _ = tx.send(event);
        }
        Ok(())
    }

    // 其余方法直接委托给 inner
    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError> {
        self.inner.read_stream(instance_id).await
    }
    async fn list_by_status(&self, status: InstanceStatus) -> Result<Vec<String>, CompError> {
        self.inner.list_by_status(status).await
    }
    async fn save_snapshot(&self, instance_id: &str, state: &InstanceState) -> Result<(), CompError> {
        self.inner.save_snapshot(instance_id, state).await
    }
    async fn load_snapshot(&self, instance_id: &str) -> Result<Option<InstanceState>, CompError> {
        self.inner.load_snapshot(instance_id).await
    }
}

/// SSE 认证参数
#[derive(Debug, serde::Deserialize)]
struct SseQuery {
    api_key: Option<String>,
}

pub async fn execution_events_stream_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<SseQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    // SSE 认证：通过 query param（header 在 EventSource 中不可自定义）
    if state.config.auth.auth_type != "none" {
        let valid = match state.config.auth.auth_type.as_str() {
            "api_key" => query.api_key.as_ref()
                .map(|k| state.config.auth.keys.contains(k))
                .unwrap_or(false),
            _ => false, // bearer 在 SSE 中暂不支持
        };
        if !valid {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    // 若该实例尚无广播发送器，新建一个（首次订阅时创建）
    let rx = {
        let broadcasts = state.event_broadcasts.read().await;
        match broadcasts.get(&id) {
            Some(tx) => tx.subscribe(),
            None => {
                drop(broadcasts);
                let (tx, rx) = tokio::sync::broadcast::channel::<WorkflowEvent>(128);
                state.event_broadcasts.write().await.insert(id.clone(), tx);
                rx
            }
        }
    };

    let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
        .filter_map(|result| async move {
            match result {
                Ok(event) => {
                    let json = serde_json::to_string(&event).ok()?;
                    Some(Ok(Event::default().data(json)))
                }
                Err(_) => None,
            }
        });

    Ok(Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default()))
}
```

**广播清理**：实例进入终态（Completed/Failed）后，解释器循环结束，`ExecutionHandle` 的 `Drop` 实现从 `event_broadcasts` 中移除该实例的 key，避免内存泄漏。

### 5.5 Agent Hot Reload（扩展）

Agent 热重载逻辑放在 `tavern-server` 层，与 Workflow 热重载行为一致。`TavernHero` 暴露 `reload_from_dir` 方法供 server 调用。

**`tavern-hero` 层**：

```rust
// crates/tavern-hero/src/registry.rs

impl AgentRegistry {
    /// 清空注册表（用于热重载原子替换）
    pub fn clear(&mut self) {
        self.agents.clear();
    }
}

// crates/tavern-hero/src/hero.rs

impl TavernHero {
    /// 原子替换：清空后重新加载目录下所有 Agent
    pub fn reload_from_dir(&self, dir: &Path) -> Result<(), TavernError> {
        let configs = crate::loader::load_from_dir(dir)?;
        let mut registry = self.registry.write().unwrap();
        registry.clear();
        for (config, path) in configs {
            if let Err(e) = registry.register(config) {
                tracing::warn!("failed to register agent from {:?}: {}", path, e);
            }
        }
        drop(registry);
        tracing::info!(count = self.registry.read().unwrap().len(), "agents hot reloaded");
        Ok(())
    }
}
```

**`tavern-server` 层**（复用现有 Workflow watcher 模式）：

```rust
// crates/tavern-server/src/main.rs

async fn start_agent_watcher(
    path: PathBuf,
    hero: Arc<TavernHero>,
    debounce: Duration,
) {
    if !path.exists() { return; }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<notify::Event, notify::Error>>(100);

    let mut watcher = match notify::RecommendedWatcher::new(
        move |res| { let _ = tx.blocking_send(res); },
        notify::Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => { tracing::error!("failed to create agent watcher: {}", e); return; }
    };

    if let Err(e) = watcher.watch(&path, notify::RecursiveMode::NonRecursive) {
        tracing::error!("failed to watch agent directory: {}", e); return;
    }

    loop {
        let res = match rx.recv().await {
            Some(r) => r,
            None => break,
        };
        if let Ok(event) = res {
            match event.kind {
                notify::EventKind::Create(_) |
                notify::EventKind::Modify(_) |
                notify::EventKind::Remove(_) => {
                    // debounce：等待 debounce 时长内无新事件
                    loop {
                        match tokio::time::timeout(debounce, rx.recv()).await {
                            Ok(Some(_)) => continue,
                            Ok(None) | Err(_) => break,
                        }
                    }
                    if let Err(e) = hero.reload_from_dir(&path) {
                        tracing::error!("agent hot reload failed: {}", e);
                    } else {
                        tracing::info!("agents hot reloaded from {:?}", path);
                    }
                }
                _ => {}
            }
        }
    }
}
```

### 5.6 Graceful Shutdown（新增）

```rust
// crates/tavern-server/src/shutdown.rs

use tokio::signal;

pub async fn graceful_shutdown_listener() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info("received Ctrl+C, starting graceful shutdown"),
        _ = terminate => tracing::info("received SIGTERM, starting graceful shutdown"),
    }
}
```

在 `main.rs` 中使用：

```rust
let app = router::create_router(state);
let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

axum::serve(listener, app)
    .with_graceful_shutdown(graceful_shutdown_listener())
    .await
    .unwrap();
```

---

## 6. REST API 变更

### 6.1 新增端点

| 方法 | 路径 | 说明 | 认证 |
|------|------|------|------|
| `GET` | `/executions/:id/events/stream` | SSE 实时事件流 | 需认证 |
| `POST` | `/auth/refresh` | 刷新 Bearer Token | 需旧 Token |

### 6.2 认证要求

V0.2.0 所有非健康检查端点默认需要认证：

```
GET  /health                    → 无需认证
GET  /metrics                   → 默认无需认证（`metrics_public = true`），可配置为需认证
GET  /agents                    → 需认证
POST /agents/:id/execute        → 需认证
GET  /workflows                 → 需认证
POST /workflows/:id/start       → 需认证
GET  /executions/:id            → 需认证
POST /executions/:id/signal     → 需认证
GET  /executions/:id/events     → 需认证
GET  /executions/:id/events/stream → 需认证（SSE 通过 query param 传递 key）
```

**SSE 认证方案**：

SSE 不支持自定义 HTTP header，因此使用 query parameter：

```
GET /executions/:id/events/stream?api_key=sk-tavern-xxx
```

### 6.3 健康检查增强

```
GET /health
```

**响应 200：**
```json
{
  "status": "ok",
  "version": "0.2.0",
  "checks": {
    "store": "connected",
    "runtime": "connected"
  }
}
```

**响应 503（store 断开）：**
```json
{
  "status": "degraded",
  "version": "0.2.0",
  "checks": {
    "store": "disconnected",
    "runtime": "connected"
  }
}
```

---

## 7. 可观测性策略

Tavern **不自建**可观测性平台，输出标准格式供外部平台消费。

### 7.1 输出矩阵

| 维度 | Tavern 输出格式 | 消费方式 | 外部平台 |
|------|----------------|----------|----------|
| **Metrics** | Prometheus exposition format | `GET /metrics` | Prometheus / Grafana / Datadog |
| **Logs** | 结构化 JSON（单行） | stdout | ELK / Loki / Datadog Logs / CloudWatch |
| **Tracing** | OpenTelemetry Protocol (OTLP) | OTLP exporter | Jaeger / Tempo / Datadog APM |
| **Events** | SSE `text/event-stream` | WebSocket/SSE | 自定义前端 / Webhook |

### 7.2 日志格式

**开发模式（`TAVERN_LOG_FORMAT=pretty`）：**
```
2026-05-25T10:30:00.123 INFO tavern_server::handlers: workflow started workflow_id=content_pipeline execution_id=550e8400-e29b-41d4-a716-446655440000
```

**生产模式（`TAVERN_LOG_FORMAT=json`）：**
```json
{
  "timestamp": "2026-05-25T10:30:00.123Z",
  "level": "INFO",
  "target": "tavern_server::handlers",
  "message": "workflow started",
  "fields": {
    "workflow_id": "content_pipeline",
    "execution_id": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

### 7.3 Metrics 扩展

在 V0.1.0 的 3 个 counter 基础上新增 histogram：

```
# HELP tavern_workflow_execution_duration_seconds Workflow execution duration
# TYPE tavern_workflow_execution_duration_seconds histogram
tavern_workflow_execution_duration_seconds_bucket{le="0.1"} 12
tavern_workflow_execution_duration_seconds_bucket{le="1.0"} 45
tavern_workflow_execution_duration_seconds_bucket{le="10.0"} 78
tavern_workflow_execution_duration_seconds_bucket{le="+Inf"} 80
tavern_workflow_execution_duration_seconds_sum 523.4
tavern_workflow_execution_duration_seconds_count 80

# HELP tavern_step_execution_duration_seconds Step execution duration
# TYPE tavern_step_execution_duration_seconds histogram
tavern_step_execution_duration_seconds_bucket{step_id="research",le="1.0"} 50
...
```

---

## 8. 错误定义

### 8.1 新增 CompError 变体

```rust
#[derive(Debug, thiserror::Error)]
pub enum CompError {
    // ── V0.1.0 保留（全部保留）──
    // ...

    // ── V0.2.0 新增 ──
    #[error("event store error: {0}")]
    StoreError(String),

    #[error("config error: {0}")]
    ConfigError(String),
}
```

### 8.2 HTTP 状态码映射（新增）

| 错误 | HTTP Status |
|------|-------------|
| Unauthorized（认证失败） | 401 |
| Forbidden（无权访问） | 403 |
| StoreError | 503 Service Unavailable |
| ConfigError | 500 Internal Server Error |

---

## 9. Docker 部署

### 9.1 Dockerfile

```dockerfile
# Stage 1: Chef
FROM lukemathwalker/cargo-chef:latest-rust-1.78 AS chef
WORKDIR /app

# Stage 2: Planner
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release -p tavern-server

# Stage 4: Runtime
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/tavern-server /usr/local/bin/
COPY configs /app/configs

ENV TAVERN_CONFIG_PATH=/app/config.toml
ENV RUST_LOG=info

EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

ENTRYPOINT ["tavern-server"]
```

### 9.2 docker-compose.yml

```yaml
version: "3.8"

services:
  tavern:
    build: .
    ports:
      - "3000:3000"
    environment:
      - TAVERN_STORE_TYPE=postgres
      - TAVERN_STORE_URL=postgres://tavern:tavern@db:5432/tavern
      - TAVERN_RUNTIME_URL=http://pandaria:8080
      - TAVERN_AUTH_TYPE=api_key
      - TAVERN_AUTH_KEYS=sk-tavern-prod-xxx
      - TAVERN_LOG_FORMAT=json
    volumes:
      - ./configs:/app/configs:ro
      - ./config.toml:/app/config.toml:ro
    depends_on:
      db:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "wget", "--spider", "http://localhost:3000/health"]
      interval: 30s
      timeout: 3s
      retries: 3

  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: tavern
      POSTGRES_PASSWORD: tavern
      POSTGRES_DB: tavern
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U tavern"]
      interval: 5s
      timeout: 3s
      retries: 5

  pandaria:
    image: pandaria:latest
    ports:
      - "8080:8080"

volumes:
  pgdata:
```

---

## 10. 环境变量（完整）

| 变量 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `TAVERN_CONFIG_PATH` | 否 | `./config.toml` | TOML 配置文件路径 |
| `TAVERN_SERVER_HOST` | 否 | `0.0.0.0` | 服务绑定地址 |
| `TAVERN_SERVER_PORT` | 否 | `3000` | 服务端口 |
| `TAVERN_SERVER_MAX_BODY_SIZE` | 否 | `1048576` | 最大请求体（字节） |
| `TAVERN_AUTH_TYPE` | 否 | `none` | 认证类型 |
| `TAVERN_AUTH_KEYS` | 条件 | — | API Key 列表（逗号分隔） |
| `TAVERN_AUTH_TOKEN_SECRET` | 条件 | — | Bearer JWT secret |
| `TAVERN_STORE_TYPE` | 否 | `memory` | 存储后端 |
| `TAVERN_STORE_PATH` | 条件 | `./tavern.db` | SQLite 路径 |
| `TAVERN_STORE_URL` | 条件 | — | PostgreSQL URL |
| `TAVERN_STORE_MAX_CONNECTIONS` | 否 | `10` | 连接池大小 |
| `TAVERN_LOG_FORMAT` | 否 | `pretty` | 日志格式 |
| `TAVERN_METRICS_ENABLED` | 否 | `true` | 是否启用 /metrics |
| `TAVERN_METRICS_PUBLIC` | 否 | `true` | `/metrics` 是否公开（false 时需认证） |
| `TAVERN_RUNTIME_URL` | 否 | — | Runtime 地址 |
| `TAVERN_RUNTIME_TIMEOUT_SECONDS` | 否 | `300` | Runtime 超时 |
| `TAVERN_RELOAD_AGENTS` | 否 | `true` | Agent 热重载 |
| `TAVERN_RELOAD_WORKFLOWS` | 否 | `true` | Workflow 热重载 |
| `TAVERN_RELOAD_DEBOUNCE_MS` | 否 | `500` | 热重载防抖（毫秒） |
| `RUST_LOG` | 否 | `info` | 日志级别 |

---

## 11. 迁移指南

### 从 V0.1.0 到 V0.2.0

**1. 配置文件**

V0.1.0 的环境变量仍然兼容，但推荐迁移到 TOML：

```bash
# V0.1.0（旧环境变量，V0.2.0 兼容期内仍有效，V0.3.0 废弃）
RUNTIME_URL=http://localhost:8080 AGENT_CONFIG_DIR=./configs/agents cargo run -p tavern-server

# V0.2.0（推荐：TOML + TAVERN_ 前缀环境变量）
cargo run -p tavern-server  # 自动读取 config.toml 和环境变量
```

> **兼容性说明**：V0.2.0 的 `TavernConfig::load()` 会同时读取旧环境变量（`RUNTIME_URL`、`AGENT_CONFIG_DIR`、`SERVER_HOST`、`SERVER_PORT`、`WORKFLOW_CONFIG_DIR`）作为回退。这些旧变量将在 **V0.3.0 中移除**，请尽早迁移到 `TAVERN_` 前缀变量。

**2. 认证启用**

V0.2.0 默认 `auth.type = "none"`，行为与 V0.1.0 一致。启用认证：

```toml
[auth]
type = "api_key"
keys = ["sk-tavern-xxx"]
```

**3. 持久化切换**

```toml
[store]
type = "sqlite"
path = "./tavern.db"
```

首次启动时自动执行 `sqlx migrate run`。内存存储保留为测试默认。

**4. API 变更**

- `POST /workflows/:id/run` → 保留（V1 兼容）
- `POST /workflows/:id/start` → 保留
- `GET /executions/:id/events/stream` → **新增**（SSE 替代轮询）
- 所有端点默认无需认证（`auth.type = "none"`）

---

## 12. 验收标准

### V0.2.0 Definition of Done

- [ ] `cargo check --workspace` 通过，无警告
- [ ] `cargo test --workspace` 通过（V0.1.0 测试 + V0.2.0 新增测试）
- [ ] `cargo clippy --workspace` 通过
- [ ] SQLite EventStore：append / read_stream / snapshot 全通过
- [ ] PostgreSQL EventStore：同上（CI 中可用 `testcontainers`）
- [ ] Agent 热重载：修改 YAML 后 500ms 内生效
- [ ] API Key 认证：无效 key 返回 401，有效 key 正常访问
- [ ] SSE：客户端可实时接收 WorkflowEvent
- [ ] 优雅停机：Ctrl+C 后等待活跃请求完成
- [ ] Docker 镜像：`docker build` 成功，`docker-compose up` 可运行
- [ ] 健康检查：`/health` 返回 store 和 runtime 连通状态
- [ ] 结构化日志：`TAVERN_LOG_FORMAT=json` 输出单行 JSON
- [ ] SQLite WAL 模式：`PRAGMA journal_mode` 返回 `wal`
- [ ] `/metrics` 认证：`metrics_public = false` 时未认证访问返回 401

---

## 13. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| `sqlx` 编译时需要数据库连接（`query!` 宏） | 开发环境复杂 | 使用 `sqlx-cli` 管理离线查询数据；CI 中预生成 `.sqlx` 目录 |
| 持久化引入后性能下降 | 高并发场景延迟增加 | Snapshot 机制减少重放；Benchmark 基线对比 |
| 认证导致 SSE 连接问题 | SSE 无法使用标准 header | Query parameter 传递 api_key；文档明确说明 |
| 热重载文件竞争 | 同时修改多个文件时状态不一致 | Debounce + 原子替换注册表；错误 Agent 跳过不影响其他 |
| Docker 镜像体积过大 | 部署慢 | `cargo-chef` 分层缓存；`debian:bookworm-slim` 精简基础镜像 |

---

## 14. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 0.1.0 | 2026-05-25 | 初始 MVP：事件溯源引擎、HTTP API |
| 0.2.0 | — | 生产就绪：持久化、认证、热重载、SSE、配置管理、Docker |
