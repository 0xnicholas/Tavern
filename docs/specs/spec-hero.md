# Tavern Hero 技术规格说明书

## 1. 概述

Tavern Hero 是 Tavern 框架的 Agent 管理子系统，负责：
- 从 YAML 配置文件加载 Agent 定义
- 在内存中维护 Agent 注册表
- 通过抽象的 Runtime 接口提交任务执行请求
- 通过 HTTP REST API 对外暴露管理能力

Tavern Hero **不直接依赖**任何具体的 Runtime 实现（如 Pandaria）。Runtime 适配器由独立的 `tavern-adapters` crate 提供，通过依赖注入传入。

### 1.1 设计原则
- **配置驱动**：Agent 完全由 YAML 定义，零代码即可创建
- **无状态**：本身不维持运行时状态，重启后从配置重建
- **初始化后只读（V0.1.0）**：Agent 注册表在初始化阶段构建，运行时为只读。不支持运行时动态增删 Agent（热重载在后续版本考虑）
- **轻量**：只负责管理和路由，不执行具体任务
- **运行时解耦**：通过 `Runtime` trait 与具体 Runtime 实现解耦

---

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| Agent | 具备特定能力、可执行任务的智能体定义 |
| Skill | Agent 可调用的工具/能力标识 |
| Runtime | 任务执行环境的抽象接口，由适配器实现 |
| Adapter | 具体 Runtime（如 Pandaria）的适配实现 |
| Hero | Tavern Hero 模块本身 |
| Server | `tavern-server` 独立进程 |

---

## 3. 架构

```
┌─────────────────┐     HTTP/REST    ┌──────────────────┐
│   Application   │ ◄──────────────► │   Tavern Server  │
│   Layer         │                  │   (tavern-server)│
└─────────────────┘                  └────────┬─────────┘
                                              │
                   ┌──────────────────────────┼──────────────────┐
                   │                          │                  │
            ┌──────▼──────┐           ┌──────▼──────┐           │
            │ Tavern Hero │           │ Tavern Comp │           │
            │             │           │             │           │
            │  Registry   │           │   Workflow  │           │
            │  Loader     │           │   Engine    │           │
            └──────┬──────┘           └──────┬──────┘           │
                   │                          │                  │
                   └────────────┬─────────────┘                  │
                                │                                │
                         ┌──────▼──────┐                         │
                         │  tavern-    │                         │
                         │   core      │                         │
                         │  (Runtime   │                         │
                         │   trait)    │                         │
                         └──────┬──────┘                         │
                                │                                │
                         ┌──────▼──────┐                         │
                         │   tavern-   │                         │
                         │  adapters   │                         │
                         │  Pandaria   │                         │
                         │  Adapter    │                         │
                         └─────────────┘                         │
                                                                  │
                    [注入点：server 创建适配器实例并注入] ◄──────────┘
```

### 3.1 Crate 职责

| Crate | 职责 |
|-------|------|
| `tavern-core` | 共享类型、`Runtime` trait 定义、错误基础 |
| `tavern-adapters` | 具体 Runtime 适配实现（Pandaria HTTP 客户端等） |
| `tavern-hero` | Agent 配置加载、内存注册表、任务分发（通过 Runtime trait） |
| `tavern-comp` | 多 Agent 协作编排（通过 Runtime trait 分发任务） |
| `tavern-server` | 进程组装层：创建适配器实例、注入、暴露 HTTP API |

---

## 4. 数据模型

### 4.1 AgentConfig

Agent 的完整配置定义。

```rust
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    /// 全局唯一标识符
    /// 约束：^[a-zA-Z0-9_-]+$，长度 1-64
    pub id: String,
    
    /// 可读名称
    pub name: String,
    
    /// 描述（可选）
    /// YAML 中可省略，默认 null
    #[serde(default)]
    pub description: Option<String>,
    
    /// LLM 模型配置
    pub model: ModelConfig,
    
    /// 系统提示词 / 角色设定
    pub instructions: String,
    
    /// Agent 可调用的技能列表
    /// YAML 中可省略，默认 []
    #[serde(default)]
    pub skills: Vec<SkillConfig>,
    
    /// 行为约束列表
    /// YAML 中可省略，默认 []
    #[serde(default)]
    pub constraints: Vec<String>,
    
    /// 记忆配置
    /// YAML 中可省略，默认 disabled
    #[serde(default)]
    pub memory: MemoryConfig,
}
```

### 4.2 ModelConfig

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct ModelConfig {
    /// 模型提供商，如 "openai", "anthropic"
    pub provider: String,
    
    /// 模型名称，如 "gpt-4o"
    pub name: String,
    
    /// 采样温度
    /// 范围：0.0 - 2.0
    /// YAML 中可省略，默认 0.7
    #[serde(default = "default_temperature")]
    pub temperature: f32,
}

fn default_temperature() -> f32 {
    0.7
}
```

### 4.3 SkillConfig

> **V0.1.0 说明**：Hero 不维护技能注册表。`skills[].id` 由 Agent 配置自由声明，skill 的有效性和 schema 验证由 Runtime（Pandaria）负责。

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct SkillConfig {
    /// 技能唯一标识
    pub id: String,
    
    /// 技能特定配置，格式由技能本身定义
    /// YAML 中可省略，默认 {}
    #[serde(default = "default_empty_object")]
    pub config: serde_json::Value,
}

fn default_empty_object() -> serde_json::Value {
    serde_json::json!({})
}
```

### 4.4 MemoryConfig

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    /// 是否启用上下文记忆
    /// YAML 中可省略，默认 false
    #[serde(default)]
    pub enabled: bool,
    
    /// 最大保留对话轮数
    /// None 表示无限制
    /// YAML 中可省略，默认 None
    #[serde(default)]
    pub max_context_turns: Option<u32>,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_context_turns: None,
        }
    }
}
```

---

## 5. 接口定义

### 5.1 Runtime Trait（tavern-core）

`tavern-core` 定义抽象接口，**不涉及任何具体 Runtime 细节**。

```rust
// crates/tavern-core/src/runtime.rs
use serde_json::Value;

#[async_trait::async_trait]
pub trait Runtime: Send + Sync {
    /// 向 Runtime 提交任务执行
    async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<Value>,
    ) -> Result<Value, RuntimeError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("runtime request failed: status={status}, body={body}")]
    RequestFailed { status: u16, body: String },
    
    #[error("runtime response invalid: {0}")]
    InvalidResponse(String),
}
```

### 5.2 Pandaria Adapter（tavern-adapters）

Pandaria HTTP 适配实现，隔离所有 Pandaria 特定逻辑。

```rust
// crates/tavern-adapters/src/pandaria.rs
use reqwest;
use tavern_core::{Runtime, RuntimeError};

pub struct PandariaRuntime {
    client: reqwest::Client,
    base_url: String,
}

impl PandariaRuntime {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Runtime for PandariaRuntime {
    async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, RuntimeError> {
        let payload = serde_json::json!({
            "agent_id": agent_id,
            "task": task,
            "context": context.unwrap_or_else(|| serde_json::json!({})),
        });

        let url = format!("{}/v1/agents/execute", self.base_url);
        let resp = self.client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| RuntimeError::RequestFailed { 
                status: 0, 
                body: e.to_string() 
            })?;

        let status = resp.status().as_u16();
        let body = resp.text().await.map_err(|e| 
            RuntimeError::InvalidResponse(e.to_string())
        )?;

        if status >= 400 {
            return Err(RuntimeError::RequestFailed { status, body });
        }

        serde_json::from_str(&body)
            .map_err(|e| RuntimeError::InvalidResponse(e.to_string()))
    }
}
```

### 5.3 Mock Adapter（tavern-adapters）

用于单元测试，无需真实 HTTP 调用。

```rust
// crates/tavern-adapters/src/mock.rs
use tavern_core::{Runtime, RuntimeError};

pub struct MockRuntime {
    /// 同步闭包，应在立即返回的轻量逻辑中使用。
    /// 如需异步模拟，需自定义 Runtime 实现。
    handler: Box<dyn Fn(&str, &str, Option<Value>) -> Result<Value, RuntimeError> + Send + Sync>,
}

#[async_trait::async_trait]
impl Runtime for MockRuntime {
    async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<Value>,
    ) -> Result<Value, RuntimeError> {
        (self.handler)(agent_id, task, context)
    }
}
```

---

## 6. Library API（tavern-hero）

`tavern-hero` 只依赖 `tavern-core`，不依赖 `tavern-adapters`。

### 6.1 AgentRegistry

```rust
impl AgentRegistry {
    /// 创建空注册表
    pub fn new() -> Self;
    
    /// 注册 Agent
    /// 错误：id 已存在时返回 TavernError::DuplicateAgent
    pub fn register(&mut self, config: AgentConfig) -> Result<(), TavernError>;
    
    /// 查询 Agent
    pub fn get(&self, id: &str) -> Option<&AgentConfig>;
    
    /// 列出全部 Agent（返回完整配置的引用）
    pub fn list_all(&self) -> Vec<&AgentConfig>;
    
    /// 列出全部 Agent 摘要（适用于无需完整配置的调用方）
    pub fn list_summary(&self) -> Vec<AgentSummary>;
}
```

### 6.2 AgentSummary

HTTP API 及外部调用方使用的 Agent 摘要结构，避免直接暴露完整配置。

```rust
#[derive(Debug, Clone, Serialize)]
pub struct AgentSummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}
```

### 6.3 TavernHero

```rust
use std::sync::Arc;
use tavern_core::Runtime;

pub struct TavernHero {
    registry: AgentRegistry,       // HashMap 天然 Send + Sync
    runtime: Arc<dyn Runtime>,     // Runtime: Send + Sync
}
// TavernHero 自动实现 Send + Sync

impl TavernHero {
    /// 初始化，注入 Runtime 实现
    pub fn new(runtime: Arc<dyn Runtime>) -> Self;
    
    /// 从目录批量加载 YAML 配置
    /// 遍历目录下所有 .yaml / .yml 文件
    /// 
    /// 故障语义：遇到首个错误即终止，此前已加载的 Agent 保留在注册表中（不回滚）。
    /// 调用方如需原子性，应先单独验证所有文件再调用加载。
    pub fn load_from_dir(&mut self, dir: &Path) -> Result<(), TavernError>;
    
    /// 加载单个 Agent 配置
    /// 返回注册的 agent_id
    pub fn load_agent(&mut self, path: &Path) -> Result<String, TavernError>;
    
    /// 查询已注册 Agent
    pub fn get_agent(&self, id: &str) -> Option<&AgentConfig>;
    
    /// 列出所有已注册 Agent（返回完整配置的引用）
    pub fn list_agents(&self) -> Vec<&AgentConfig>;
    
    /// 列出所有已注册 Agent 的摘要
    pub fn list_agents_summary(&self) -> Vec<AgentSummary>;
    
    /// 提交任务执行
    /// 前置检查：agent_id 必须在注册表中存在
    pub async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, TavernError>;
}
```

---

## 7. REST API（tavern-server）

Base URL: `http://{host}:{port}`

Content-Type: `application/json`

### 7.1 健康检查

```
GET /health
```

> **V0.1.0 说明**：此为浅健康检查，仅验证 server 进程存活，不探测下游 Runtime 连通性。Runtime 可用性通过实际任务执行的 502 响应暴露。

**响应 200:**
```json
{
  "status": "ok"
}
```

### 7.2 列出所有 Agent

```
GET /agents
```

**响应 200:**
```json
[
  {
    "id": "researcher",
    "name": "研究员",
    "description": "擅长信息检索"
  }
]
```

返回字段为 AgentConfig 的摘要子集（id, name, description）。

### 7.3 获取单个 Agent

```
GET /agents/:id
```

**响应 200:** 完整 AgentConfig JSON

**响应 404:**
```json
{
  "error": "AgentNotFound",
  "message": "Agent 'xxx' not found"
}
```

### 7.4 执行 Agent 任务

```
POST /agents/:id/execute
```

**请求体:**
```json
{
  "task": "调查 2024 年 AI Agent 框架发展现状",
  "context": {}
}
```

- `task`（必填）：任务描述字符串
- `context`（可选）：附加上下文对象，默认 `{}`

**响应 200:** Runtime 返回的 JSON 透传

**响应 404:** Agent 不存在

**响应 502:** Runtime 不可用或返回错误

---

## 8. 错误定义

### 8.1 TavernError（tavern-hero）

```rust
#[derive(Debug, thiserror::Error)]
pub enum TavernError {
    #[error("agent '{id}' already registered")]
    DuplicateAgent { id: String },
    
    #[error("agent '{id}' not found")]
    AgentNotFound { id: String },
    
    #[error("config parse failed at {path}: {reason}")]
    ConfigParse { path: String, reason: String },
    
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("runtime error: {0}")]
    Runtime(#[from] RuntimeError),
}
```

### 8.2 HTTP 状态码映射

| 错误 | HTTP Status |
|------|-------------|
| AgentNotFound | 404 Not Found |
| ConfigParse | 500 Internal Server Error |
| Io | 500 Internal Server Error |
| Runtime::RequestFailed(4xx/5xx) | 502 Bad Gateway |
| Runtime::InvalidResponse | 502 Bad Gateway |

---

## 9. YAML 配置格式规范

### 9.1 完整示例

```yaml
id: researcher
name: 研究员
description: 擅长信息检索和资料整理

model:
  provider: openai
  name: gpt-4o
  temperature: 0.3

instructions: |
  你是一个专业的研究助理。你的任务是：
  1. 针对用户提出的问题，检索相关信息
  2. 整理成结构化的报告
  3. 引用信息来源

skills:
  - id: web_search
    config:
      max_results: 5
      region: zh-CN

  - id: summarize
    config:
      max_length: 500

constraints:
  - 回答必须使用中文
  - 必须引用信息来源

memory:
  enabled: true
  max_context_turns: 10
```

### 9.2 字段约束

| 字段 | 类型 | 必需 | 默认 | 约束 |
|------|------|------|------|------|
| id | string | 是 | - | `^[a-zA-Z0-9_-]+$`, 1-64 字符 |
| name | string | 是 | - | 非空 |
| description | string | 否 | null | - |
| model | object | 是 | - | - |
| model.provider | string | 是 | - | 非空 |
| model.name | string | 是 | - | 非空 |
| model.temperature | float | 否 | 0.7 | 0.0 - 2.0 |
| instructions | string | 是 | - | 非空 |
| skills | array | 否 | [] | 元素为 object |
| skills[].id | string | 是 | - | 非空 |
| skills[].config | object | 否 | {} | 任意 JSON object |
| constraints | array | 否 | [] | 元素为 string |
| memory | object | 否 | {} | - |
| memory.enabled | bool | 否 | false | - |
| memory.max_context_turns | integer | 否 | null | ≥ 1 |

---

## 10. 与 Runtime 的交互边界

Hero 通过 `tavern-core` 定义的 `Runtime` trait 与具体 Runtime 交互，**不感知**任何底层协议细节。

### 10.1 任务执行

```
TavernHero::execute()
    → Runtime::execute(agent_id, task, context)
        → 由 tavern-adapters 中的具体适配器实现
```

### 10.2 V0.1.0 设计决策

| 议题 | 决策 | 说明 |
|------|------|------|
| 超时配置 | 不暴露给 Hero | 由 `tavern-adapters` 内部硬编码默认值（如 30s） |
| 重试策略 | 不暴露给 Hero | 由 `tavern-adapters` 内部管理（V0.1.0 建议不重试） |
| 连接池 | 不暴露给 Hero | 由 `tavern-adapters` 内部维护 |
| 超时错误分类 | 统一为 `RuntimeError::RequestFailed` | Hero 不区分网络超时与 HTTP 错误，统一透传 502 |

> Pandaria 具体的 HTTP 通信协议（请求路径、头信息、响应格式）由 adapters 规格文档定义，详见未来发布的 `docs/specs/spec-adapters.md`。

---

## 11. 目录结构

```
tavern/
├── Cargo.toml
├── README.md
├── configs/
│   ├── agents/
│   │   ├── researcher.yaml
│   │   └── writer.yaml
│   └── workflows/
│       └── content_pipeline.yaml
├── crates/
│   ├── tavern-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── config.rs
│   │       ├── runtime.rs      # Runtime trait
│   │       └── error.rs
│   ├── tavern-adapters/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── pandaria.rs     # Pandaria HTTP 适配器
│   │       └── mock.rs         # 测试适配器
│   ├── tavern-hero/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── registry.rs
│   │       ├── hero.rs
│   │       └── loader.rs        # 私有模块，YAML 加载逻辑
│   └── tavern-server/
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs
│           ├── router.rs
│           ├── handlers.rs
│           └── state.rs
└── docs/
    └── specs/
        ├── spec-hero.md
        └── spec-comp.md
```

---

## 12. 环境变量

> **说明**：以下环境变量由 `tavern-server` 进程在启动时读取，通过构造函数参数注入到 Hero 和 Adapters 中。`tavern-hero` 作为 library crate 不直接读取环境变量。

| 变量 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `RUNTIME_URL` | 是 | - | Runtime 地址（适配器使用） |
| `AGENT_CONFIG_DIR` | 否 | `./configs/agents` | Agent YAML 配置目录 |
| `SERVER_HOST` | 否 | `0.0.0.0` | 服务绑定地址 |
| `SERVER_PORT` | 否 | `3000` | 服务端口 |
| `RUST_LOG` | 否 | `info` | 日志级别 |

---

## 13. 依赖矩阵

| Crate | core | adapters | hero | comp | server |
|-------|:----:|:--------:|:----:|:----:|:------:|
| `serde` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `serde_yaml` | - | - | ✓ | ✓ | - |
| `serde_json` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `thiserror` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `reqwest` | - | ✓ | - | - | - |
| `axum` | - | - | - | - | ✓ |
| `tokio` | - | - | ✓ | ✓ | ✓ |
| `tracing` | - | - | - | - | ✓ |
| `async-trait` | ✓ | ✓ | ✓ | - | ✓ |
| `tavern-core` | - | ✓ | ✓ | ✓ | ✓ |
| `tavern-adapters` | - | - | - | - | ✓ |
| `tavern-hero` | - | - | - | ✓ | ✓ |
| `tavern-comp` | - | - | - | - | ✓ |

---

## 14. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 0.1.0 | 2026-05-20 | 初始版本：静态配置、Runtime trait 解耦、适配器架构 |
