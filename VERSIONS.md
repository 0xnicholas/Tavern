# Tavern 版本说明

本文档记录 Tavern 各版本的变更历史、功能范围与已知限制。

---

## 版本号规则

Tavern 采用语义化版本控制（SemVer）：

```
主版本号.次版本号.修订号
```

- **主版本号**：重大架构变更或不兼容 API 修改
- **次版本号**：新增功能，向下兼容
- **修订号**：Bug 修复或性能优化

---

## V0.1.0 — 初始版本（Current）

> 状态：开发中（开发分支）  
> 目标：最小可用产品（MVP），验证核心架构

### 新增功能

#### Agent 系统（`tavern-hero`）
- YAML 声明式 Agent 配置（`id`, `name`, `model`, `instructions`, `skills`, `constraints`, `memory`）
- Agent 注册表：内存 HashMap，支持防重复注册
- 批量目录加载：自动识别 `.yaml` / `.yml` 文件
- 配置校验：ID 格式、`temperature` 范围、`max_context_turns` 上限
- 直接向 Agent 提交任务执行

#### Workflow 引擎（`tavern-comp`）
- YAML 声明式 Workflow 配置（`steps`, `inputs`, `outputs`, `process`, `planning`）
- **Sequential 模式**：DAG 拓扑排序执行，支持并行步骤
- **Hierarchical 模式**：Manager Agent 动态委派任务
- **Planning 模式**：AgentPlanner 生成执行计划并注入上下文
- 事件溯源架构：`WorkflowEvent` + `EventStore` + `InstanceState` 重放
- 步骤级特性：超时控制、失败重试（`retries` + `retry_delay`）、信号等待（`wait_for_signal`）
- 模板渲染：`{{var}}` 插值，支持嵌套对象和 Jinja filter
- 并发控制：基于 `tokio::sync::Semaphore` 的 `max_concurrency`

#### Runtime 适配（`tavern-adapters`）
- `Runtime` trait 抽象：与具体 LLM 后端解耦
- `PandariaRuntime`：HTTP 适配，Session 生命周期管理，HMAC-SHA256 Token 自动生成
- `MockRuntime`：同步闭包，用于测试和本地开发

#### HTTP 服务（`tavern-server`）
- **Agent API**：列表、查询、直接执行
- **Workflow API**：列表、查询、创建、删除、同步执行（`run`）、异步执行（`start`）
- **Execution API**：查询状态、查询事件流、发送信号、取消执行
- **管理 API**：Workflow 热重载（`notify` 监听 + debounce）
- **Metrics API**：Prometheus 格式 counter（executions / failures / duration）
- 统一错误响应：`ApiError` + 语义化 HTTP 状态码

#### 示例配置
- `configs/agents/`：researcher、writer、editor
- `configs/workflows/`：content_pipeline（研究 → 写作 → 编辑）

### 架构亮点

```
tavern-server  →  tavern-comp  →  tavern-hero  →  tavern-adapters  →  tavern-core
     HTTP           Workflow        Agent           Runtime            Types
```

- 单向依赖，无循环引用
- 所有共享类型定义在 `tavern-core`
- 所有 trait 抽象定义在 `tavern-core`，实现分布在各 crate

### 测试覆盖

| Crate | 测试数 | 说明 |
|-------|--------|------|
| `tavern-core` | 12 | 配置反序列化、默认值、新类型 |
| `tavern-adapters` | 7 | Runtime 集成、Token 生成 |
| `tavern-hero` | 11 | 注册表、加载器、并发安全 |
| `tavern-comp` | 59 | 引擎集成：DAG、Hierarchical、Planning、重试、超时、并行 |
| `tavern-server` | 17 | 全链路 HTTP API 测试 |
| **合计** | **106** | |

### 已知限制

以下限制在 V0.1.0 中属于**预期内缺口**，将在后续版本迭代：

| 限制 | 说明 | 计划版本 |
|------|------|----------|
| 无持久化存储 | `EventStore` 仅内存实现，重启后执行历史丢失 | V0.2 |
| Agent 无热重载 | 新增 Agent 需重启服务加载 | V0.2 |
| 无认证/授权 | 所有 API 公开可访问 | V0.2 |
| Pandaria 轮询延迟 | `send_message` 采用 30s 间隔轮询，非 SSE | V0.2 |
| 无 OpenAPI 文档 | 仅有外部 Pandaria 的 openapi.yaml | V0.2 |
| 单进程架构 | 无分布式/高可用支持 | V0.4+ |
| 无 Web UI | 纯后端 HTTP 服务 | 待定 |

### 发布前检查清单

- [x] 修复 `test_pandaria_success` 测试失败（`tavern-adapters`）— 补充 GET `/messages` 轮询 mock
- [x] 清理 compiler warning（unused imports / dead code / needless borrow）
- [x] 清理 `derivable_impls` warning（`Process`, `InstanceStatus`）
- [x] `cargo test --workspace` 全绿（106/106 passed）
- [x] `cargo fmt` 无变更
- [x] 确认版本号统一为 `0.1.0`

### 运行方式

```bash
# 1. 检查
cargo check --workspace
cargo test --workspace
cargo clippy --workspace

# 2. 启动服务（连接 Pandaria）
RUNTIME_URL=http://localhost:8080 RUST_LOG=info cargo run -p tavern-server

# 3. 或使用 Mock Runtime 本地测试
cargo test --workspace
```

---

---

## 开发规划总览

| 版本 | 目标 | 预估工期 | 核心主题 |
|------|------|----------|----------|
| **V0.2.0** | 生产就绪 | 2–3 周 | 持久化、认证、实时流、JSON 日志 |
| **V0.3.0** | 调试与管控 | 2 周 | 执行回放、断点、审批、限流 |
| **V0.4.0** | 分布式 | 3–4 周 | 集群化、队列、状态同步 |
| **V1.0.0** | 稳定版 | 待定 | GA 发布，API 冻结 |

---

## V0.2.0 — 生产就绪（Planned）

> 目标：支持持久化、热重载、基础安全，达到可用状态

### 里程碑

| 序号 | 任务 | 优先级 | 涉及 Crate | 技术选型 |
|------|------|--------|------------|----------|
| 1 | **PostgreSQL EventStore** | P0 | `tavern-comp` | `sqlx` + `tokio-postgres` |
| 2 | **SQLite EventStore** | P1 | `tavern-comp` | `sqlx` + `libsqlite3-sys` |
| 3 | **Agent 热重载** | P0 | `tavern-hero` | 复用 `notify` crate |
| 4 | **API Key / Bearer 认证** | P0 | `tavern-server` | `tower-http` auth layer |
| 5 | **SSE 实时事件流** | P1 | `tavern-server` | `tokio::sync::broadcast` |
| 6 | **OpenAPI / Swagger UI** | P2 | `tavern-server` | `utoipa` + `utoipa-swagger-ui` |
| 7 | **统一配置管理** | P2 | `tavern-server` | `figment` (TOML + env) |
| 8 | **结构化 JSON 日志** | P1 | `tavern-server` | `tracing-subscriber` JSON layer，供外部平台消费 |
| 9 | **优雅停机** | P1 | `tavern-server` | `tokio::signal` |
| 10 | **发布 Docker 镜像** | P2 | CI/CD | `Dockerfile` + `cargo-chef` |

### 架构预留

`tavern-comp/src/store.rs` 中已定义 `EventStore` trait：

```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(&self, instance_id: &str, event: WorkflowEvent) -> Result<(), CompError>;
    async fn read_stream(&self, instance_id: &str) -> Result<Vec<WorkflowEvent>, CompError>;
    async fn save_snapshot(&self, instance_id: &str, state: &InstanceState) -> Result<(), CompError>;
    async fn load_snapshot(&self, instance_id: &str) -> Result<Option<InstanceState>, CompError>;
}
```

新增持久化实现只需实现上述 trait，无需改动引擎逻辑。

### 可观测性策略

Tavern **不自建**可观测性平台，而是输出标准格式供外部平台消费：

| 维度 | Tavern 输出 | 外部平台 |
|------|------------|----------|
| **Metrics** | Prometheus `/metrics`（counter + histogram） | Datadog / Grafana / Prometheus |
| **Logs** | 结构化 JSON（`RUST_LOG=info` + JSON layer） | ELK / Loki / Datadog Logs |
| **Tracing** | 标准 `tracing` span（不绑定具体后端） | Jaeger / Tempo / Datadog APM |

原则：只负责**产生数据**，不负责**存储、查询、告警**。避免重复建设，降低维护成本。

### 数据模型（初稿）

```sql
CREATE TABLE workflow_events (
    id          BIGSERIAL PRIMARY KEY,
    instance_id UUID NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_events_instance ON workflow_events(instance_id, id);

CREATE TABLE workflow_snapshots (
    instance_id UUID PRIMARY KEY,
    state       JSONB NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);
```

---

## V0.3.0 — 执行调试与高级管控（Planned）

> 目标：深度调试能力和精细化运行时管控

### 里程碑

| 序号 | 任务 | 优先级 | 涉及 Crate | 说明 |
|------|------|--------|------------|------|
| 1 | **执行回放 API** | P0 | `tavern-comp` | 从 EventStore 重放任意实例的事件流 |
| 2 | **断点调试** | P1 | `tavern-comp` | 在特定事件处暂停执行，允许人工介入 |
| 3 | **步骤级审批** | P1 | `tavern-server` | `wait_for_signal` 强化，支持人工审批节点 |
| 4 | **执行分支（A/B）** | P2 | `tavern-comp` | 同一 Workflow 并行执行两组参数，对比输出 |
| 5 | **执行克隆** | P2 | `tavern-comp` | 复制历史执行的输入，重新触发 |
| 6 | **租户级限流** | P1 | `tavern-server` | `tower::limit::RateLimit`，按 API Key 隔离 |
| 7 | **租户配额管理** | P2 | `tavern-server` | 每日/每小时 Workflow 执行次数上限 |

### 执行回放设计

```bash
# 回放指定实例的所有事件（只读）
GET /executions/:id/replay

# 断点：在 StepFailed 处暂停，等待人工决策
POST /executions/:id/replay?breakpoint=StepFailed
```

### 审批节点 Workflow 示例

```yaml
steps:
  - id: generate_report
    agent_id: writer
    task: "撰写财报摘要"
    output_key: report

  - id: manager_approve
    agent_id: system
    task: "等待主管审批"
    wait_for_signal: manager_approval   # 人工信号
    signal_timeout: 86400               # 24 小时
    output_key: approval_result

  - id: publish
    agent_id: system
    task: "发布已审批的报告"
    depends_on: [manager_approve]
```

---

## V0.4.0 — 分布式（Planned）

> 目标：水平扩展、高可用

### 前提条件

V0.4.0 要求 V0.2.0 的 PostgreSQL EventStore 已完成，因为分布式场景必须共享持久化状态。

### 里程碑

| 序号 | 任务 | 优先级 | 涉及 Crate | 技术选型 |
|------|------|--------|------------|----------|
| 1 | **Redis 任务队列** | P0 | `tavern-comp` | `fred` / `redis` crate |
| 2 | **执行器工作节点** | P0 | 新增 crate | 从 Server 解耦执行器 |
| 3 | **分布式锁** | P1 | `tavern-comp` | Redis Redlock |
| 4 | **状态同步** | P1 | `tavern-comp` | 基于 EventStore + 乐观锁 |
| 5 | **Workflow 版本管理** | P2 | `tavern-comp` | `version` 字段 + 历史表 |
| 6 | ** leader 选举** | P2 | 新增 crate | 用于定时任务（如超时检测） |
| 7 | **gRPC 内部通信** | P2 | 新增 crate | `tonic`（Server ↔ Worker） |
| 8 | **Kubernetes 部署** | P2 | K8s | Helm Chart + HPA |

### 架构演进

```
V0.1 (单进程)              V0.4 (分布式)
┌─────────┐                ┌──────────┐      ┌──────────┐
│ Server  │──run──▶        │  Server  │──gRPC─▶│ Worker 1 │
│ (all)   │                │  (API)   │       └──────────┘
└─────────┘                │          │──gRPC─▶┌──────────┐
                           │          │        │ Worker 2 │
                           │          │        └──────────┘
                           └────┬─────┘
                                │
                           ┌────┴─────┐
                           │  Redis   │
                           │  + PG    │
                           └──────────┘
```

### Workflow 版本管理

```yaml
# configs/workflows/content_pipeline.yaml
id: content_pipeline
version: 3  # 新增字段
name: 内容生产流水线
# ...
```

- Workflow 注册表支持多版本共存
- 执行时锁定启动时的版本号
- 历史版本不可修改，仅可查看和回滚

---

## 技术债务与持续优化

以下任务不绑定特定版本，应在每次迭代中持续跟进：

| 任务 | 说明 | 建议频率 |
|------|------|----------|
| 依赖升级 | `cargo update` + 检查安全漏洞 (`cargo audit`) | 每两周 |
| 性能基准 | Workflow 执行延迟 P50/P95/P99 | 每个版本发布前 |
| 文档同步 | README、API 文档、配置示例与代码保持一致 | 每次 PR |
| 测试补强 | 集成测试覆盖率 > 80%，增加混沌测试 | 持续 |
| 内存分析 | `valgrind` / `heaptrack` 检查泄漏 | 每季度 |

---

## 建议开发节奏

```
Week 1-2:  V0.2.0 功能开发（持久化 + 认证）
Week 3:    V0.2.0 测试 + 文档 + 发布候选
Week 4:    V0.2.0 发布 + V0.3.0 设计评审
Week 5-6:  V0.3.0 功能开发（调试与管控）
Week 7:    V0.3.0 测试 + 发布
Week 8-10: V0.4.0 功能开发（分布式）
Week 11:   V0.4.0 测试 + 发布候选
Week 12:   V0.4.0 发布
```

**关键原则**：
1. **向后兼容**：V0.x 系列保持 API 兼容，不破坏现有 YAML 配置格式
2. **trait 先行**：新功能先定义 trait，再写实现，便于后续替换（如 EventStore、Runtime）
3. **测试驱动**：每个功能必须有对应的集成测试，HTTP API 测试使用 `tower::ServiceExt::oneshot`
4. **文档即代码**：YAML 示例、API 文档、VERSIONS.md 与代码同步提交

---

## 历史归档

| 版本 | 发布日期 | 说明 |
|------|----------|------|
| V0.1.0 | — | 初始 MVP，核心架构验证 |
