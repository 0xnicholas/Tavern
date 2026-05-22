# Tavern Hero 开发计划（修订版）

## 项目背景

Tavern 是一个快速灵活的多智能体自动化框架。Tavern Hero 是其 Agent 管理子系统，负责从 YAML 配置加载 Agent 定义、维护内存注册表、通过抽象 Runtime 接口分发任务，并对外暴露 HTTP REST API。

当前项目处于起步阶段，仅有技术规格文档（`docs/specs/spec-hero.md`、`docs/specs/spec-comp.md`），无实际代码。本计划依据 `spec-hero.md` 制定，目标为交付 V0.1.0 版本。

---

## 目标与范围

**版本**：V0.1.0  
**核心交付物**：

| Crate | 说明 |
|-------|------|
| `tavern-core` | 共享类型、`Runtime` trait、基础错误 |
| `tavern-adapters` | Pandaria HTTP 适配器 + Mock 适配器 |
| `tavern-hero` | Agent 配置加载、内存注册表、任务分发 |
| `tavern-server` | 进程组装层：注入适配器、暴露 HTTP API |

**非目标**（后续版本）：
- 运行时动态增删 Agent（热重载）
- 技能注册表与 schema 验证
- 工作流编排（由 Tavern Comp 负责，不在本计划内）

---

## 开发策略

按依赖顺序**自底向上**实现：

```
tavern-core → tavern-adapters → tavern-hero → tavern-server
```

每个 crate 完成后立即编写单元测试，确保通过后再进入下一层。  
`tavern-server` 作为组装层最后实现，集成所有下游 crate 并通过端到端测试验证。

---

## Phase 1: 项目骨架（0.5 天）

### 任务 1.1 初始化 Cargo Workspace
- 创建根目录 `Cargo.toml`，定义 workspace 成员
- 创建 `crates/` 下四个目录及各自 `Cargo.toml`
- 按 spec §13 依赖矩阵添加基础依赖，注意 feature 配置：
  - `tokio = { version = "1", features = ["full"] }`
  - `serde = { version = "1", features = ["derive"] }`
  - `axum = { version = "0.7", features = ["macros"] }`
  - `reqwest = { version = "0.12", features = ["json"] }`
  - `tower-http = { version = "0.5", features = ["trace"] }`
- **明确 `tavern-server` 的 `Cargo.toml` 需依赖 `tavern-adapters`**（用于构造 `PandariaRuntime`）
- 创建 `.gitignore`

**验收标准**：
- `cargo check` 在 workspace 根目录执行成功，无编译错误
- 四个 crate 均能被 cargo 识别

### 任务 1.2 创建 configs 目录结构
- `configs/agents/`：存放示例 Agent YAML
- `configs/workflows/`：预留 Comp 配置目录（空目录即可）
- 创建 `researcher.yaml`、`writer.yaml` 示例配置（参考 spec §9.1）

**验收标准**：
- 示例 YAML 能通过后续实现的 loader 解析

---

## Phase 2: tavern-core（1 天）

### 任务 2.1 Runtime Trait
- 实现 `crates/tavern-core/src/runtime.rs`
- 定义 `Runtime` trait（`async_trait`）
- 定义 `RuntimeError` 枚举（`RequestFailed`、`InvalidResponse`）

**参考**：spec §5.1、§8.1

**验收标准**：
- trait 定义与 spec 一致
- 编译通过

### 任务 2.2 共享配置类型
- 实现 `crates/tavern-core/src/config.rs`
- 定义数据模型：`AgentConfig`、`ModelConfig`、`SkillConfig`、`MemoryConfig`
- **定义 `AgentSummary`，派生 `Serialize`**（用于 HTTP API 响应）
- 实现必要的 `Default`
- **注意**：`AgentSummary` 需要 `Serialize`；`AgentConfig` 等需要 `Deserialize`

**参考**：spec §4、§6.2

**验收标准**：
- 所有结构体字段、类型、默认值与 spec 一致
- `serde::Deserialize` / `Serialize` 派生正确
- 示例 YAML 可反序列化为 `AgentConfig`

### 任务 2.3 错误基础
- 实现 `crates/tavern-core/src/error.rs`
- 定义共享错误类型（若 spec 中 core 无独立错误，则确认由上层扩展）

**验收标准**：
- `thiserror` 宏使用正确
- 错误类型实现了 `std::error::Error`

### 任务 2.4 lib.rs 组装与测试
- 组装 `lib.rs`，控制模块可见性
- 编写单元测试：验证结构体反序列化、`AgentSummary` 序列化、错误构造

**验收标准**：
- `cargo test -p tavern-core` 全部通过

---

## Phase 3: tavern-adapters（1 天）

### 任务 3.1 Pandaria HTTP 适配器
- 实现 `crates/tavern-adapters/src/pandaria.rs`
- `PandariaRuntime` 结构体
- `Runtime` trait 实现：构造 HTTP 请求、发送、处理响应
- 错误映射：HTTP 错误 → `RuntimeError::RequestFailed`，JSON 解析错误 → `RuntimeError::InvalidResponse`
- **超时**：`reqwest` 客户端设置默认超时 30s（V0.1.0 不暴露给 Hero，内部硬编码）

**参考**：spec §5.2、§10.2

**验收标准**：
- 使用 `reqwest` 发送 POST 请求到 `/v1/agents/execute`
- payload 结构符合 spec
- 超时、重试等细节按 V0.1.0 决策处理（内部默认值，不暴露给 Hero）

### 任务 3.2 Mock 适配器
- 实现 `crates/tavern-adapters/src/mock.rs`
- 支持注入闭包，用于单元测试
- **注意**：`Runtime::execute` 是 async 的，Mock 内部用 `async { (self.handler)(...) }` 包装同步闭包

**参考**：spec §5.3

**验收标准**：
- 可构造返回固定结果的 MockRuntime
- 可构造返回错误的 MockRuntime

### 任务 3.3 测试
- **使用 `wiremock`** 对 Pandaria 适配器做集成测试（模拟 HTTP 服务端）
- 编写 Mock 适配器基础测试
- 覆盖场景：HTTP 成功 / 4xx / 5xx / 网络错误 / 无效 JSON 响应

**验收标准**：
- `cargo test -p tavern-adapters` 全部通过
- HTTP 成功/失败/无效响应三种场景覆盖

---

## Phase 4: tavern-hero（1.5 天）

### 任务 4.1 AgentRegistry
- 实现 `crates/tavern-hero/src/registry.rs`
- `HashMap<String, AgentConfig>` 封装
- 方法：`new`、`register`、`get`、`list_all`、`list_summary`
- **配置验证**：在 `register` 中执行以下校验：
  - `id` 匹配 `^[a-zA-Z0-9_-]+$`，长度 1-64
  - `model.temperature` 在 0.0 - 2.0 范围内（若需运行时验证）
  - `memory.max_context_turns` 为 `None` 或 ≥ 1
- 错误：`DuplicateAgent`、以及校验失败返回 `ConfigParse`

**参考**：spec §6.1、§6.2、§9.2

**验收标准**：
- ID 重复时返回 `TavernError::DuplicateAgent`
- 非法 ID 格式返回 `TavernError::ConfigParse`
- `list_summary` 只暴露 id/name/description

### 任务 4.2 YAML Loader
- 实现 `crates/tavern-hero/src/loader.rs`
- 单文件加载：解析 YAML → `AgentConfig`
- 目录批量加载：遍历 `.yaml`/`.yml`
- 错误处理：`ConfigParse`、`Io`
- **故障语义**：遇到首个错误即终止，此前已加载的 Agent 保留在注册表中（不回滚）

**参考**：spec §6.3（`load_agent`、`load_from_dir`）、§9

**验收标准**：
- 支持完整示例配置反序列化
- 字段缺失使用 serde 默认值
- 非法 YAML / 非法 ID 格式正确返回错误

### 任务 4.3 TavernHero 主结构
- 实现 `crates/tavern-hero/src/hero.rs`
- `TavernHero` 结构体持有 `AgentRegistry` + `Arc<dyn Runtime>`
- 方法：`new`、`load_from_dir`、`load_agent`、`get_agent`、`list_agents`、`list_agents_summary`、`execute`
- `execute` 前置检查：agent_id 必须在注册表中存在

**参考**：spec §6.3

**验收标准**：
- `TavernHero` 自动实现 `Send + Sync`
- `execute` 找不到 Agent 时返回 `TavernError::AgentNotFound`

### 任务 4.4 错误定义
- 实现 `crates/tavern-hero/src/error.rs`
- `TavernError` 枚举完整定义

**参考**：spec §8.1

**验收标准**：
- 所有变体与 spec 一致
- `#[from]` 转换正确

### 任务 4.5 测试
- Registry 单元测试：注册、查询、重复、列表
- **Registry 验证测试**：非法 ID、非法 temperature、非法 max_context_turns
- Loader 单元测试：正确 YAML、缺失字段、无效 YAML、目录加载
- Hero 集成测试：使用 MockRuntime 验证 execute 调用链

**验收标准**：
- `cargo test -p tavern-hero` 全部通过
- 核心场景全部覆盖（非法配置、正常路径、错误路径）

---

## Phase 5: tavern-server（2 天）

### 任务 5.1 状态管理
- 实现 `crates/tavern-server/src/state.rs`
- 使用 `Arc<TavernHero>` 作为共享状态
- **重要**：`TavernHero` 必须先完成可变初始化（`load_from_dir`），再被 `Arc::new(hero)` 包装

**参考**：spec §3.1、§12

### 任务 5.2 HTTP Router
- 实现 `crates/tavern-server/src/router.rs`
- 使用 `axum` 定义路由：
  - `GET /health`
  - `GET /agents`
  - `GET /agents/:id`
  - `POST /agents/:id/execute`
- **集成 `tower-http TraceLayer`**，记录请求方法和路径

**参考**：spec §7

### 任务 5.3 Handlers 与 API 类型
- 实现 `crates/tavern-server/src/handlers.rs`
- 定义 `ApiError` 结构体：
  ```rust
  #[derive(Serialize)]
  struct ApiError {
      error: String,
      message: String,
  }
  ```
- 为 `ApiError` 实现 `IntoResponse`，按 spec §8.2 映射 HTTP 状态码
- 定义 `ExecuteRequest` DTO：
  ```rust
  #[derive(Deserialize)]
  struct ExecuteRequest {
      task: String,
      #[serde(default)]
      context: serde_json::Value,
  }
  ```
- 每个路由对应的处理函数
- **错误映射**：
  - `AgentNotFound` → 404
  - `ConfigParse` / `Io` → 500
  - `RuntimeError::RequestFailed` / `RuntimeError::InvalidResponse` → 502

**参考**：spec §7、§8.2

### 任务 5.4 main.rs 组装
- 使用 `std::env` 读取环境变量（V0.1.0 不引入 `dotenvy`）
  - `RUNTIME_URL`（必需）
  - `AGENT_CONFIG_DIR`（默认 `./configs/agents`）
  - `SERVER_HOST`（默认 `0.0.0.0`）
  - `SERVER_PORT`（默认 `3000`）
- 初始化 `tracing_subscriber::fmt::init()`（读取 `RUST_LOG`，默认 `info`）
- 创建 `PandariaRuntime` 实例（使用 `RUNTIME_URL`）
- 创建 `TavernHero`、执行 `load_from_dir` 加载配置、再 `Arc::new(hero)`
- 启动 axum 服务

**参考**：spec §12

**验收标准**：
- 服务可正常启动并监听指定端口
- `tracing` 正确输出请求日志
- 环境变量缺失时给出清晰的 panic 信息（`RUNTIME_URL`）

### 任务 5.5 端到端测试
- 使用 `reqwest` + `wiremock`（模拟 Pandaria Runtime）启动测试服务器
- 验证：
  - `GET /health` → 200 `{"status":"ok"}`
  - `GET /agents` → AgentSummary 列表
  - `GET /agents/:id` → 完整 AgentConfig / 404 + 标准错误体
  - `POST /agents/:id/execute` → Runtime 结果透传 / 404 / 502
  - 错误响应体格式符合 spec（含 `error` 和 `message` 字段）

**验收标准**：
- `cargo test -p tavern-server` 全部通过
- 覆盖所有 spec 中定义的 HTTP 场景和错误码

---

## Phase 6: 集成与验收（0.5 天）

### 任务 6.1 Workspace 级构建与测试
- `cargo build --workspace` 通过
- `cargo test --workspace` 全部通过
- `cargo clippy --workspace` 无警告（或明确允许的部分除外）
- `cargo fmt` 格式化

### 任务 6.2 文档与元数据更新
- 更新 `README.md`：快速开始、环境变量说明、运行方式
- **更新 `AGENTS.md`**：记录构建、测试和运行指令
- 确保 crate 级文档（`lib.rs` 中的 `//!`）完整

### 任务 6.3 手动验证
- 启动 server + mock Pandaria 服务（可用 `wiremock` 独立进程或简单 HTTP 服务）
- 用 curl 验证完整流程：加载 Agent → 查询 → 执行任务

---

## 里程碑与时间线

| 阶段 | 内容 | 预估工时 |
|------|------|---------|
| Phase 1 | 项目骨架 | 0.5 天 |
| Phase 2 | tavern-core | 1 天 |
| Phase 3 | tavern-adapters | 1 天 |
| Phase 4 | tavern-hero | 1.5 天 |
| Phase 5 | tavern-server | **2 天** |
| Phase 6 | 集成验收 | 0.5 天 |
| **合计** | | **~6.5 天** |

> 注：以上时间为纯开发估算，不含评审、返工和等待依赖时间。Phase 5 增加 0.5 天用于 E2E 测试和错误响应体统一实现。

---

## 依赖与风险

| 风险 | 影响 | 应对 |
|------|------|------|
| spec 与 comp 规格存在交集（如 core 类型） | 中期需调整 core | Phase 2 预留扩展接口，保持 core 轻量 |
| Pandaria Runtime 实际协议与 spec 假设不一致 | adapters 需返工 | 适配器层完全隔离，变更不影响 Hero/Server |
| axum/tokio 版本兼容问题 | 编译失败 | 锁定版本在 Cargo.toml，使用稳定版 |
| YAML 配置复杂度超预期 | loader 解析出错 | 先实现标准路径，边缘 case 用测试捕获 |
| **axum 学习曲线 / Tower 中间件** | Phase 5 延期 | 文档先行，先跑通 `hello-world` 再接入业务逻辑 |

---

## 验收清单（V0.1.0 发布标准）

- [ ] `cargo build --workspace` 零错误
- [ ] `cargo test --workspace` 全部通过
- [ ] Server 可启动并响应 `/health`
- [ ] 可从目录加载 Agent YAML 配置
- [ ] 可通过 REST API 查询 Agent 列表和详情
- [ ] 可通过 REST API 提交任务，正确转发到 Runtime
- [ ] **错误响应体格式统一**（`{"error": "...", "message": "..."}`）
- [ ] **错误码映射符合 spec §8.2**（404 / 500 / 502）
- [ ] **配置验证生效**（非法 ID、超范围 temperature 被拒绝）
- [ ] README 包含快速开始指南
- [ ] `AGENTS.md` 包含构建和运行指令
