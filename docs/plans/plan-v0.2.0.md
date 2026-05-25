# Plan: Tavern V0.2.0 生产就绪

> Source spec: `docs/specs/spec-v0.2.0-production-ready.md` (frozen 2026-05-25)  
> Target crates: `tavern-comp`, `tavern-hero`, `tavern-server`

---

## 0. 前置检查

冻结 spec 后编码前必须确认：

- [ ] `cargo test --workspace` 当前全绿（106/106）
- [ ] `cargo clippy --workspace` 零 warning
- [ ] `cargo fmt -- --check` 零变更
- [ ] 已创建功能分支：`git checkout -b feat/v0.2.0`
- [ ] Workspace root `Cargo.toml` 已添加 `"crates/tavern-config"`（若独立 crate）

---

## 1. 总体阶段

| 阶段 | 范围 | 核心交付物 | 预估工时 | 前置依赖 |
|------|------|-----------|----------|----------|
| **Phase 1** | EventStore 持久化 | SQLiteEventStore、PostgresEventStore、snapshot | 6h | — |
| **Phase 2** | 统一配置管理 | `TavernConfig` figment 集成、TOML + env、向后兼容 | 4h | — |
| **Phase 3** | API 认证 | AuthMiddleware（API Key + Bearer）、JWT claim | 4h | Phase 2（需要配置） |
| **Phase 4** | SSE 实时事件流 | `BroadcastingEventStore`、SSE handler、广播清理 | 4h | Phase 1 + Phase 3 |
| **Phase 5** | Agent 热重载 | `AgentRegistry::clear()`、`reload_from_dir`、server watcher | 3h | — |
| **Phase 6** | 优雅停机 + 健康检查 | SIGTERM handler、`/health` 增强 | 2h | — |
| **Phase 7** | Docker + CI | Dockerfile、docker-compose、`.sqlx` 离线数据 | 4h | Phase 1 |
| **Phase 8** | 集成测试 + 回归 | 端到端测试、向后兼容验证 | 4h | Phase 1-7 |

**总预估**：约 31 小时（建议预留 **38 小时**，含 20% 缓冲。sqlx/Docker 环节易超时）

---

## 2. Phase 1: EventStore 持久化

### 2.0 依赖新增

**`crates/tavern-comp/Cargo.toml`**：
```toml
[dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "sqlite", "postgres", "migrate", "json"] }
# postgres feature 条件编译

[features]
default = ["sqlite"]
postgres = ["sqlx/postgres"]
```

**`crates/tavern-server/Cargo.toml`**：
```toml
[dependencies]
jsonwebtoken = "9"
```

### 2.1 创建迁移目录

```bash
mkdir -p crates/tavern-comp/migrations/sqlite
mkdir -p crates/tavern-comp/migrations/postgres
```

**`migrations/sqlite/20260525000001_init.sql`**（spec §4.1）：
```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE workflow_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    payload     TEXT NOT NULL,
    created_at  INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);
CREATE INDEX idx_events_instance_seq ON workflow_events(instance_id, id);

CREATE TABLE workflow_snapshots (
    instance_id TEXT PRIMARY KEY,
    state       TEXT NOT NULL,
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

**`migrations/postgres/20260525000001_init.sql`**（spec §4.1）：
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

### 2.2 实现 SqliteEventStore

**文件**：`crates/tavern-comp/src/store.rs`

**修改点**：
1. 保留现有 `MemoryEventStore`（向后兼容，测试默认）
2. 在 `#[cfg(feature = "sqlite")]` 下新增 `SqliteEventStore`
3. `EventStore::list_by_status` 保留 trait 默认实现（向后兼容）；为 `MemoryEventStore` 提供覆盖实现（O(n) 遍历，测试用足够）
4. `append()` 使用整存整取（`serde_json::to_string`），反序列化用 `from_str`
5. `list_by_status()` 利用 `workflow_instances` 辅助表
6. `save_snapshot` / `load_snapshot` 使用 SQLite upsert 语法

**关键代码模板**（spec §5.1 已提供，直接按 spec 实现）。

### 2.3 实现 PostgreSQLEventStore

**文件**：`crates/tavern-comp/src/store.rs`

在 `#[cfg(feature = "postgres")]` 下实现，结构与 `SqliteEventStore` 基本一致，差异：
- `sqlx::PgPool` + `$1` 占位符
- 迁移目录：`migrations/postgres/`
- 时间戳：`now()`

### 2.4 测试

**新增测试**（`crates/tavern-comp/src/store.rs` `#[cfg(test)]`）：
- [ ] `MemoryEventStore` 保留现有测试，`list_by_status` 覆盖实现正确工作
- [ ] `SqliteEventStore::new(":memory:")` — 内存 SQLite 测试（无需文件）
- [ ] `append` + `read_stream` 往返测试
- [ ] `list_by_status` 筛选测试
- [ ] `save_snapshot` + `load_snapshot` 测试
- [ ] 辅助表 `upsert_instance_meta` 验证（instance_id 存在时更新，不存在时插入）

### 2.5 验收标准

- [ ] `cargo test -p tavern-comp` 全绿
- [ ] `cargo test -p tavern-comp --features postgres`（CI 中可选）
- [ ] SQLite `:memory:` 模式无需外部数据库文件

---

## 3. Phase 2: 统一配置管理

### 3.1 新增 `tavern-config` crate（可选但推荐）

**决策**：为保持 crate 边界清晰，新建 `crates/tavern-config/`。如果工期紧张，可内嵌到 `tavern-server/src/config.rs`。

**`crates/tavern-config/Cargo.toml`**：
```toml
[package]
name = "tavern-config"
version.workspace = true

[dependencies]
figment = { version = "0.10", features = ["toml", "env"] }
serde = { version = "1.0", features = ["derive"] }
```

### 3.2 实现 `TavernConfig`

**文件**：`crates/tavern-config/src/lib.rs`

按 spec §5.2 实现：
- `TavernConfig` 顶层结构
- `ServerConfig`、`AuthConfig`、`StoreConfig`、`ObservabilityConfig`、`RuntimeConfig`、`ReloadConfig`
- `AuthConfig.keys` 的逗号分隔自定义反序列化
- `TavernConfig::load()` 的 figment 构建 + V0.1.0 兼容回退
- `default_*` 辅助函数

### 3.3 在 server 中集成

**文件**：`crates/tavern-server/src/main.rs`

修改 `main()`：
1. 调用 `TavernConfig::load()`
2. 根据 `config.store.store_type` 创建对应 `EventStore`
3. 根据 `config.auth.auth_type` 决定是否加载认证中间件
4. 保留旧环境变量直接读取作为最后的 fallback（兼容层）

### 3.4 向后兼容测试

**新增测试**（`tavern-server` 集成测试）：
- [ ] 旧环境变量 `RUNTIME_URL` 仍能生效
- [ ] `TAVERN_RUNTIME_URL` 优先级高于 `RUNTIME_URL`
- [ ] 缺少 `config.toml` 时仅用环境变量也能启动

### 3.5 验收标准

- [ ] `cargo test -p tavern-server` 全绿
- [ ] 删除 `config.toml` 后通过环境变量仍能运行服务
- [ ] 设置 `TAVERN_AUTH_KEYS="a,b,c"` 后 `AuthConfig.keys == vec!["a","b","c"]`

---

## 4. Phase 3: API 认证

### 4.1 实现 AuthMiddleware

**文件**：`crates/tavern-server/src/auth.rs`（新增）

按 spec §5.3 实现：
- `auth_middleware()` 函数
- `validate_bearer()` 使用 `jsonwebtoken` + `Claims { sub, exp }`
- 支持 `none` / `api_key` / `bearer` 三种模式

### 4.2 Router 集成认证

**文件**：`crates/tavern-server/src/router.rs`

```rust
let auth_layer = axum::middleware::from_fn_with_state(
    state.config.auth.clone(),
    auth_middleware,
);

let public_routes = Router::new()
    .route("/health", get(handlers::health_handler));

let mut protected_routes = Router::new()
    .route("/agents", get(handlers::list_agents_handler))
    // ... 所有其他端点
    .layer(auth_layer);

// metrics_public = false 时 /metrics 归入 protected
if !config.metrics_public {
    protected_routes = protected_routes.route("/metrics", get(handlers::metrics_handler));
}

let app = public_routes.merge(protected_routes);
```

**注意**：`create_router` 签名从 `fn(state: Arc<AppState>)` 扩展为接受 `config: &ObservabilityConfig`，以支持运行时决定 `/metrics` 路由分组。`/metrics` 的公开性由 `config.metrics_public` 控制。

### 4.3 SSE 认证

**文件**：`crates/tavern-server/src/sse.rs`

SSE 通过 query param 认证：
```
GET /executions/:id/events/stream?api_key=sk-xxx
```

实现见 spec §5.4。

### 4.4 测试

- [ ] `auth_type = "none"` 时所有请求通过
- [ ] 无效 API Key 返回 401
- [ ] 有效 API Key 正常访问
- [ ] 无效 Bearer Token 返回 401
- [ ] 过期 Bearer Token 返回 401
- [ ] SSE 无 `api_key` query param 返回 401

### 4.5 验收标准

- [ ] `cargo test -p tavern-server` 全绿
- [ ] `curl -H "x-api-key: bad" http://localhost:3000/agents` → 401
- [ ] `curl -H "x-api-key: sk-tavern-xxx" http://localhost:3000/agents` → 200

---

## 5. Phase 4: SSE 实时事件流

### 5.1 实现 BroadcastingEventStore

**文件**：`crates/tavern-server/src/sse.rs`

按 spec §5.4：
1. `BroadcastingEventStore` 包装器
2. `append()` 中 `store.append()` 成功后广播
3. `read_stream/list_by_status/save_snapshot/load_snapshot` 委托给 `inner`

### 5.2 SSE Handler

**文件**：`crates/tavern-server/src/sse.rs`

1. `execution_events_stream_handler()`
2. Query param 认证
3. 懒创建 `broadcast::channel`（首次订阅时）
4. `BroadcastStream` → `Event` 转换

### 5.3 广播清理

**文件**：`crates/tavern-server/src/handlers.rs` 或 `state.rs`

实例完成时从 `event_broadcasts` HashMap 中移除 key。有两种方案：

**方案 A（推荐）**：在 `ExecutionHandle` 的 `Drop` 实现中发送清理信号。

**方案 B**：在 `get_execution_handler` 中检测到终态时清理（被动）。

选择方案 A，更可靠。

### 5.4 测试

- [ ] SSE 客户端连接后能收到 `StepCompleted` 事件
- [ ] 实例完成后广播发送器从 HashMap 中移除
- [ ] 多个 SSE 客户端同时订阅同一实例，都能收到事件

### 5.5 验收标准

- [ ] `curl -N "http://localhost:3000/executions/:id/events/stream?api_key=xxx"` 实时输出 JSON 事件
- [ ] 实例完成后再次订阅返回空流（或 404）

---

## 6. Phase 5: Agent 热重载

### 6.1 `AgentRegistry::clear()`

**文件**：`crates/tavern-hero/src/registry.rs`

```rust
pub fn clear(&mut self) {
    self.agents.clear();
}
```

### 6.2 `TavernHero::reload_from_dir()`

**文件**：`crates/tavern-hero/src/hero.rs`

```rust
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
```

### 6.3 Server 层 Watcher

**文件**：`crates/tavern-server/src/main.rs`

复用 Workflow watcher 的 `notify` 模式（spec §5.5 已提供完整代码）。与 Workflow watcher 的区别：
- 监听目录：`configs/agents/`（而非 `configs/workflows/`）
- 调用 `hero.reload_from_dir()` 而非 `registry.load_from_dir()`

### 6.4 测试

- [ ] 修改 `configs/agents/researcher.yaml` 后 500ms 内生效
- [ ] 新增 Agent YAML 后可通过 API 查询到
- [ ] 删除 Agent YAML 后 API 返回 404
- [ ] 错误 YAML 被跳过，不影响其他 Agent

### 6.5 验收标准

- [ ] `echo 'new content' > configs/agents/test.yaml`，500ms 后 `GET /agents/test` → 200

---

## 7. Phase 6: 优雅停机 + 健康检查

### 7.1 Graceful Shutdown

**文件**：`crates/tavern-server/src/shutdown.rs`（新增）

按 spec §5.6 实现 `graceful_shutdown_listener()`。

在 `main.rs` 中：
```rust
axum::serve(listener, app)
    .with_graceful_shutdown(graceful_shutdown_listener())
    .await
    .unwrap();
```

### 7.2 健康检查增强

**文件**：`crates/tavern-server/src/handlers.rs`

修改 `health_handler`：
1. 检测 `EventStore` 连通性（`read_stream("__health_check__")` 或更轻量的 ping）
2. 检测 `Runtime` 连通性（可选，spec 中提及但非阻塞）
3. 返回 JSON（spec §6.3）

### 7.3 测试

- [ ] `Ctrl+C` 后进程在活跃请求完成后退出
- [ ] `/health` 返回 `checks.store = "connected"`

### 7.4 验收标准

- [ ] `kill -TERM <pid>` → 日志输出 "starting graceful shutdown"
- [ ] `/health` 返回 JSON 包含 `version` 和 `checks`

---

## 8. Phase 7: Docker + CI

### 8.1 Dockerfile

**文件**：`Dockerfile`

按 spec §9.1 实现，注意：
1. `cargo-chef` 分层缓存
2. Runtime stage 安装 `ca-certificates wget`
3. `HEALTHCHECK` 使用 `wget --spider`

### 8.2 docker-compose.yml

**文件**：`docker-compose.yml`

按 spec §9.2，包含 `tavern`、`db`、`pandaria` 三个服务。

### 8.3 `.sqlx` 离线数据

**CI 步骤**：
```bash
# 在 GitHub Actions 中
cargo install sqlx-cli
cargo sqlx prepare --workspace
# 提交 .sqlx/ 目录到 git
```

**开发环境**：
```bash
cargo sqlx prepare --workspace  # 生成 .sqlx/query-*.json
```

### 8.4 CI 增强

**`.github/workflows/ci.yml`**（新增或修改）：
```yaml
- name: SQLx Prepare Check
  run: |
    cargo sqlx prepare --workspace --check
```

### 8.5 验收标准

- [ ] `docker build -t tavern .` 成功
- [ ] `docker-compose up` 后 `curl http://localhost:3000/health` → 200
- [ ] `cargo sqlx prepare --check` 通过（.sqlx/ 已提交）

---

## 9. Phase 8: 集成测试 + 回归

### 9.1 端到端测试

**文件**：`crates/tavern-server/tests/integration_v020.rs`（新增独立集成测试目录）

使用 `#[tokio::test]` + `tower::ServiceExt::oneshot` 进行端到端测试：
- [ ] 完整 Workflow 执行：POST `/workflows/:id/start` → SSE 接收事件 → GET `/executions/:id` 验证状态
- [ ] 认证中间件：未认证访问 `/agents` → 401
- [ ] 热重载 + 新 Agent 执行：修改 YAML → reload → POST `/agents/:id/execute`

### 9.2 向后兼容测试

- [ ] `POST /workflows/:id/run` 仍返回同步结果（V0.1.0 行为）
- [ ] `cargo test --workspace` 全绿（V0.1.0 的 106 个测试必须全部通过）
- [ ] YAML 配置格式零变更（`configs/agents/*.yaml`、`configs/workflows/*.yaml`）

### 9.3 性能基准

- [ ] 内存 EventStore vs SQLite EventStore：100 次 Workflow 执行耗时对比
- [ ] Snapshot 开启前后：1000 事件实例的重放耗时对比

### 9.4 验收标准（V0.2.0 DoD）

- [ ] `cargo check --workspace` 通过，无 warning
- [ ] `cargo clippy --workspace` 通过
- [ ] `cargo fmt -- --check` 通过
- [ ] `cargo test --workspace` 全绿（V0.1.0 106 + V0.2.0 新增）
- [ ] SQLite EventStore：append / read_stream / snapshot / list_by_status
- [ ] Agent 热重载：500ms debounce 内生效
- [ ] API Key 认证：无效 → 401，有效 → 200
- [ ] SSE：客户端实时接收事件流
- [ ] 优雅停机：`SIGTERM` 后等待活跃请求
- [ ] Docker：`docker build` + `docker-compose up` 可运行
- [ ] 健康检查：返回 `version` + `checks`
- [ ] 结构化日志：`TAVERN_LOG_FORMAT=json` 输出单行 JSON
- [ ] SQLite WAL：`PRAGMA journal_mode` 返回 `wal`
- [ ] `/metrics` 认证：`metrics_public = false` 时未认证返回 401
- [ ] 旧环境变量兼容：`RUNTIME_URL` 在 V0.2.0 仍有效

---

## 10. 文件变更清单

### 新增文件

```
crates/tavern-comp/migrations/sqlite/20260525000001_init.sql
crates/tavern-comp/migrations/postgres/20260525000001_init.sql
crates/tavern-comp/src/store_sqlite.rs    # 或内嵌到 store.rs
crates/tavern-comp/src/store_postgres.rs  # #[cfg(feature = "postgres")]
crates/tavern-config/                     # 若独立 crate
crates/tavern-config/Cargo.toml
crates/tavern-config/src/lib.rs
crates/tavern-server/src/auth.rs
crates/tavern-server/src/config.rs        # 若未独立 crate
crates/tavern-server/src/sse.rs
crates/tavern-server/src/shutdown.rs
crates/tavern-server/tests/integration_v020.rs  # 端到端集成测试
Dockerfile
docker-compose.yml
.sqlx/                                    # git tracked
.gitignore                                # 排除 *.db
```

### 修改文件

```
crates/tavern-comp/Cargo.toml             # +sqlx, +features
Cargo.toml                                # +"crates/tavern-config" workspace member
crates/tavern-comp/Cargo.toml             # +sqlx, +features
crates/tavern-comp/src/store.rs           # +SqliteEventStore +PostgresEventStore +MemoryEventStore::list_by_status
crates/tavern-comp/src/lib.rs             # 条件编译导出
crates/tavern-comp/src/handle.rs          # +Drop 清理广播发送器
crates/tavern-hero/src/registry.rs        # +clear()
crates/tavern-hero/src/hero.rs            # +reload_from_dir()
crates/tavern-server/Cargo.toml           # +tavern-config, +jsonwebtoken, +figment
crates/tavern-server/src/router.rs        # +auth layer, +SSE route, +metrics 路由分组
crates/tavern-server/src/handlers.rs      # +auth handler, +SSE handler, health增强
crates/tavern-server/src/state.rs         # +config: TavernConfig, +broadcasts
crates/tavern-server/src/main.rs          # +config load, +graceful shutdown, +watchers
.github/workflows/ci.yml                  # +sqlx prepare check
```

### 删除文件

无。

---

## 11. 风险与回退

| 风险 | 回退方案 |
|------|---------|
| `sqlx` 编译时要求数据库连接，阻塞开发 | 使用 `:memory:` SQLite 模式本地开发；CI 中用 `cargo sqlx prepare` |
| Phase 1 持久化引入后 V0.1.0 测试失败 | 保留 `MemoryEventStore` 为默认；仅当 `store.type != "memory"` 时使用持久化 |
| 认证误拦截健康检查 | `public_routes` 和 `protected_routes` 分离；`/health` 始终在 public |
| SSE 广播内存泄漏 | 若方案 A（Drop 清理）失败，降级为方案 B（被动清理）+ 定期 GC |
| Docker 镜像构建超时 | 本地预构建 `cargo-chef` 缓存层；CI 中使用 GitHub Actions cache |

---

## 12. 每日 Check-in 模板

每天工作结束时回答：

```
日期：____
已完成阶段：____
遇到阻塞：____（无 / 有，需协助）
明日计划：____
测试状态：____/____ 通过
check/clippy/fmt：全绿 / 有 warning
```

---

## 13. 备注

- **向后兼容红线**：任何修改不得破坏 V0.1.0 的 `cargo test --workspace`。若必须破坏，需在计划阶段标注并升级版本号到 V0.3.0。
- **Feature flag 策略**：`postgres` 为可选 feature，默认关闭。CI 中增加 `--features postgres` 测试矩阵。
- **Config crate 决策**：若 4 小时内无法完成 `tavern-config` crate 的独立构建，降级为 `tavern-server/src/config.rs` 内嵌实现。
- **Snapshot 策略**：V0.2.0 初始实现中，snapshot 在 `WorkflowCompleted` / `WorkflowFailed` 时保存即可。每 N 事件保存的增量优化延后到 V0.3.0。
