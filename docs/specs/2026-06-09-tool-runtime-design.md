# Tool Runtime — Tavern 作为工具注册中心与执行引擎

**日期**: 2026-06-09
**状态**: 已评审（迭代 1）

---

## 背景

Pandaria 是 thin proxy：它不内置任何工具实现。LLM 产生 ToolCall 后，Pandaria 通过 `HttpProxyTool` 将请求转为 HTTP POST 打到外部 endpoint。当前 Tavern 只把 skills 以文本形式注入 system prompt，不等同于真正执行。

**目标**：Tavern 承担工具注册与执行职责。Pandaria 管 Agent 生命周期，Tavern 管工具生命周期。边界清晰：

```
LLM (in Pandaria)
  → ToolCall { name: "web_search", params: {...} }
    → HttpProxyTool::execute()
      → POST /api/tools/web_search (打到 Tavern)
        → Tavern ToolHandler::execute()
          → 实际执行（调搜索引擎 API、执行沙箱代码等）
          → 返回 { content, is_error }
```

## 设计决策

### 为什么不是代理模式

Pandaria 的 `ToolConfig.endpoint` 可以直接填外部服务 URL。若 Tavern 只做透明转发（LLM → Pandaria → Tavern → 外部服务），多一跳无意义。Tavern 的价值在于租户级工具配置、用量计量、安全执行——这些 Pandaria 不做。

### Tavern 的三项核心职责

| 职责 | 说明 |
|---|---|
| **租户级配置管理** | 同一工具名、不同租户可用不同 API key、配额、沙箱参数 |
| **用量计量与计费** | 按 tenant_id 记录工具调用次数、外部 API 费用 |
| **安全执行环境** | URL 过滤、结果缓存、沙箱隔离、结果格式标准化 |

## SkillConfig 扩展

### 当前（仅文本注入）

```yaml
skills:
  - id: web_search
    config:
      max_results: 5
```

### 目标（完整 ToolConfig）

```yaml
skills:
  - id: web_search
    name: web_search                     # LLM function name（默认 = id）
    description: Search the web          # 工具描述
    parameters:                          # JSON Schema（默认 = {}）
      type: object
      properties:
        query:
          type: string
          description: The search query
      required: [query]
    timeout_ms: 30000                    # 默认 30000
    config:                              # 工具私有配置（tenant 级覆盖用）
      max_results: 5
```

所有新增字段有默认值，向后兼容——不写照样作为纯文本注入。

> **设计说明**：`headers` 是 Pandaria ToolConfig 的可选字段，但不在 SkillConfig YAML 中暴露。PandariaRuntime 序列化时根据环境变量决定：`TAVERN_TOOL_SECRET` 未设置时 `headers` 设为 `null`；已设置时填充 `{"Authorization": "Bearer <secret>"}`。`endpoint` 也不在 YAML 暴露，统一由 Tavern 从 `TAVERN_PUBLIC_URL + "/api/tools/" + id` 自动构造，避免配置漂移。

### Rust 类型变更

```rust
// tavern-core/src/config.rs
pub struct SkillConfig {
    pub id: String,

    // 新增字段
    #[serde(default)]               // 默认 = id
    pub name: Option<String>,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]               // JSON Schema，默认 {}。空对象表示工具无结构化参数。
    pub parameters: Value,

    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,            // 默认 30000

    #[serde(default)]
    pub config: Value,              // 私有配置，默认 {}。LLM 不可见，仅 handler 读取。
}
```

## 传输层变更

### Pandaria 的 ToolConfig 格式（来源：api-gateway/src/types.rs）

```json
{
  "name": "web_search",
  "description": "Search the web for information",
  "parameters": { "type": "object", "properties": {...}, "required": [...] },
  "endpoint": "https://tavern.example.com/api/tools/web_search",
  "timeout_ms": 30000,
  "headers": null
}
```

### create_session 适配

**关键设计决策**：不引入独立的 `PandariaToolConfig` 类型，而是**扩展 `ToolDef`** 使其携带 Pandaria 序列化所需的全部字段。`Runtime::execute()` 已经接收 `&[ToolDef]`，`PandariaRuntime` 从同一个 `ToolDef` 中提取字段并序列化为 Pandaria wire format。MockRuntime 忽略新增字段。单一数据流，无需 trait 改动或 hero→adapter 的旁路通道。

```rust
// tavern-core/src/runtime.rs（扩展 ToolDef，不是新类型）
pub struct ToolDef {
    /// 工具标识（用于 handler 路由和 system prompt 注入）
    pub id: String,

    /// LLM function name（默认 = id）
    #[serde(default)]
    pub name: String,

    /// 工具描述
    #[serde(default)]
    pub description: String,

    /// JSON Schema（默认 {}）
    #[serde(default)]
    pub parameters: Value,

    /// Pandaria 回调的目标端点（Hero 根据 TAVERN_PUBLIC_URL 拼接）
    #[serde(default)]
    pub endpoint: String,

    /// 回调超时（毫秒）
    #[serde(default = "default_tool_timeout")]
    pub timeout_ms: u64,

    /// 工具私有配置（LLM 不可见，仅 handler 读取）
    #[serde(default)]
    pub config: Option<Value>,
}

fn default_tool_timeout() -> u64 { 30000 }
```

`PandariaRuntime::create_session()` 从 `&[ToolDef]` 直接构建 Pandaria 的 `ToolConfig` JSON：

```rust
fn tool_def_to_pandaria_json(tools: &[ToolDef], public_url: &str) -> Vec<Value> {
    tools.iter().map(|t| {
        let endpoint = if t.endpoint.is_empty() {
            format!("{}/api/tools/{}", public_url, t.name)
        } else {
            t.endpoint.clone()
        };
        let mut obj = serde_json::json!({
            "name": t.name,
            "description": t.description,
            "parameters": t.parameters,
            "endpoint": endpoint,
            "timeout_ms": t.timeout_ms,
        });
        // headers 仅在 TAVERN_TOOL_SECRET 设置时填充
        if let Ok(secret) = std::env::var("TAVERN_TOOL_SECRET") {
            obj["headers"] = json!({"Authorization": format!("Bearer {}", secret)});
        } else {
            obj["headers"] = Value::Null;
        }
        obj
    }).collect()
}
```

### 数据流

```
SkillConfig (YAML)
  → hero::skills_to_tool_defs(skills, public_url) → Vec<ToolDef>
    → Runtime::execute(..., &tool_defs)
      → PandariaRuntime::execute()
        → tool_def_to_pandaria_json(tool_defs) → Vec<Value>
          → create_session(..., &tool_values)
            → POST /api/v1/sessions { tools: [...] }
```

无旁路通道，无 trait 改动，ToolDef 是全局唯一工具描述类型。

### endpoint 构建

完整 URL 通过环境变量 `TAVERN_PUBLIC_URL` 拼接：

```
TAVERN_PUBLIC_URL=https://tavern.example.com
→ endpoint = https://tavern.example.com/api/tools/web_search
```

若 `TAVERN_PUBLIC_URL` **或** `TAVERN_TOOL_SECRET` 未设置，`skills_to_tool_defs()` 返回空数组，回退到纯文本注入模式（工具端点路由不注册，无可用工具）。两者必须同时配置才能启用 tool calling。

> **生产环境要求**：`TAVERN_PUBLIC_URL` 必须是 Pandaria 可达的地址。若 Tavern 和 Pandaria 在同一 VPC/集群内，使用内部 DNS（如 `http://tavern:3000`）。

## ToolHandler 抽象

### Trait 定义

```rust
// tavern-core/src/tool.rs（新文件）
use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;

/// 工具执行过程中可能发生的错误。
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("invalid params: {0}")]
    InvalidParams(String),

    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    #[error("tool not found: {0}")]
    NotFound(String),
}

#[async_trait::async_trait]
pub trait ToolHandler: Send + Sync {
    async fn execute(
        &self,
        params: Value,
        tenant_id: &str,
        session_id: &str,
        tool_call_id: &str,
    ) -> Result<ToolResult, ToolError>;
}

pub struct ToolResult {
    /// 返回给 LLM 的内容
    pub content: Vec<ContentPart>,
    /// true 表示工具执行出错
    pub is_error: bool,
    /// 可选的结构化附加数据（Pandaria ToolResultMessage.details）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

pub struct ContentPart {
    #[serde(rename = "type")]
    pub content_type: String,  // "text" | "image" | ...
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    // 未来扩展: image_url, file 等
}
```

### Registry

```rust
pub struct ToolRegistry {
    handlers: HashMap<String, Arc<dyn ToolHandler>>,
}

impl ToolRegistry {
    pub fn new() -> Self;
    pub fn register(&mut self, name: String, handler: Arc<dyn ToolHandler>);
    pub fn get(&self, name: &str) -> Option<&Arc<dyn ToolHandler>>;
}
```

注册发生在 `tavern-server/src/main.rs` 启动时。

## HTTP 端点

### POST /api/tools/:name

**请求体**（格式由 Pandaria HttpProxyTool 定义，参考 Pandaria 源码 `tool-executor/src/proxy.rs`）：

```json
{
  "tool_call_id": "call_abc123",
  "params": { "query": "Rust async patterns", "max_results": 5 },
  "session_id": "uuid-here",
  "tenant_id": "test-tenant"
}
```

**响应体**：

```json
{
  "content": [
    { "type": "text", "text": "1. 搜索结果 A\n2. 搜索结果 B" }
  ],
  "details": { "total_results": 42, "search_engine": "duckduckgo" },
  "is_error": false
}
```

`details` 为可选字段，对应 Pandaria `ToolResultMessage.details`（OpenAPI schema line 878: `type: object`）。

### 鉴权策略

工具端点由 Pandaria 内部调用，不走用户 Auth 中间件。两种可选方案：

**方案 A（MVP 采用）：共享 Secret**
- 新增 `TAVERN_TOOL_SECRET` 环境变量
- Pandaria 在 ToolConfig.headers 中传 `Authorization: Bearer <TAVERN_TOOL_SECRET>`
- Tavern 端点验证此 header，匹配则放行
- 优点：零配置协商（Tavern 同时是 ToolConfig 的生成方和验证方）

**方案 B（后续考虑）：内部网络绑定**
- Tavern 监听 `127.0.0.1:3001`（仅内部，不对外暴露）
- Pandaria 通过内网直连，无鉴权
- 缺点：多端口运维复杂，不适合单容器部署

### 处理流程

```
POST /api/tools/web_search
  → 验证 Authorization header（方案 A）
    → 从 path 提取 name = "web_search"
      → ToolRegistry::get("web_search")
        → handler.execute(params, tenant_id, session_id, tool_call_id)
          → 返回 ToolResult
            → 序列化为 JSON 响应
```

### 错误处理

| 场景 | HTTP 状态码 | 响应体 |
|---|---|---|
| tool name 未注册 | 404 | 标准 `ApiError { error, message }` |
| handler 执行异常 | 200 | `{ content: [{type: "text", text: "error: ..."}], is_error: true }` |
| handler 执行成功 | 200 | `{ content: [...], is_error: false }` |
| params 无效 | 400 | 标准 `ApiError` |
| 鉴权失败 | 401 | 标准 `ApiError` |

> Pandaria `HttpProxyTool` 只认 2xx。非 2xx 视为 tool execution error。handler 内部异常统一返回 200 + `is_error: true`，避免 Pandaria 误判为基础设施故障。

## 内置 Handler（MVP）

第一个版本只实现一个 `web_search` handler。

> **⚠️ Demo 占位说明**：MVP 使用 [DuckDuckGo Instant Answer API](https://api.duckduckgo.com/)，**该 API 仅返回结构化摘要（定义/计算/Wikipedia 摘要），不返回传统搜索结果列表**。MVP 目的仅为验证 tool calling 管道通断，不追求搜索质量。后续必须替换为 Brave Search API、SerpAPI 或 Bing API 并配套 API key 管理。

```rust
pub struct WebSearchHandler {
    client: reqwest::Client,
}

impl WebSearchHandler {
    pub fn new() -> Self {
        Self { client: reqwest::Client::new() }
    }

    /// 调 DuckDuckGo Instant Answer API。
    /// 返回格式: { "AbstractText": "...", "AbstractURL": "...", "RelatedTopics": [...] }
    async fn query_ddg(&self, query: &str) -> Result<Value, ToolError> {
        let url = format!("https://api.duckduckgo.com/?q={}&format=json&no_html=1",
            urlencoding::encode(query));
        let resp = self.client.get(&url).send().await
            .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
        serde_json::from_str(&resp.text().await.unwrap_or_default())
            .map_err(|e| ToolError::ExecutionFailed(e.to_string()))
    }
}

#[async_trait::async_trait]
impl ToolHandler for WebSearchHandler {
    async fn execute(
        &self,
        params: Value,
        _tenant_id: &str,
        _session_id: &str,
        _tool_call_id: &str,
    ) -> Result<ToolResult, ToolError> {
        let query = params["query"].as_str()
            .ok_or_else(|| ToolError::InvalidParams("missing 'query'".into()))?;

        let data = self.query_ddg(query).await?;
        let text = format_ddg_response(&data);

        Ok(ToolResult {
            content: vec![ContentPart { content_type: "text".into(), text: Some(text) }],
            is_error: false,
            details: Some(data),
        })
    }
}

/// LLM 传入的 `max_results` 参数在本 MVP 中忽略——handler 使用 tool YAML 中的 `config.max_results` 决定格式。
/// 这是有意为之：`config` 是管理员配置，不受 LLM 控制，避免提示词注入篡改配额/行为。
fn format_ddg_response(data: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(abs) = data["AbstractText"].as_str() {
        if !abs.is_empty() {
            parts.push(format!("Summary: {}", abs));
            if let Some(url) = data["AbstractURL"].as_str() {
                parts.push(format!("Source: {}", url));
            }
        }
    }
    if let Some(topics) = data["RelatedTopics"].as_array() {
        for (i, topic) in topics.iter().enumerate() {
            if let Some(text) = topic["Text"].as_str() {
                parts.push(format!("{}. {}", i + 1, text));
            }
        }
    }
    if parts.is_empty() {
        "No results found.".to_string()
    } else {
        parts.join("\n\n")
    }
}
```

## 风险与已知限制

| 风险 | 影响 | 缓解措施 |
|---|---|---|
| Polling 超时（6 分钟）vs 连续 tool call | 多轮 tool call 可能超时 | MVP 不处理；后续可增大 polling 超时或改用 SSE push |
| Session 删除 vs inflight tool callback | 极端情况 callback 返回时 session 已消失 | Pandaria 在 LLM 最终完成后才删 session，tool call 在最终完成前，无竞态。仅 polling 超时触发删除时可能冲突——边缘情况，MVP 忽略 |
| DDG API 数据有限 | 搜索质量差 | MVP 仅验证管道；后续切换到正式搜索 API |
| Tool call 幂等性 | Pandaria 重试导致重复执行 | MVP 不处理；`tool_call_id` 保留供后续 dedup 使用 |

## 实现步骤

| 步骤 | 文件 | 内容 | 测试 |
|---|---|---|---|
| 1 | `tavern-core/src/config.rs` | SkillConfig 加字段（name, description, parameters, timeout_ms），向后兼容 | YAML 反序列化测试 |
| 2 | `tavern-core/src/runtime.rs` | **扩展** `ToolDef`：加 `name`、`description`、`parameters`、`endpoint`、`timeout_ms` 字段（全部 `#[serde(default)]`），删除旧的 `from_skill()` 构造器 | 序列化测试，验证默认值 |
| 3 | `tavern-core/src/tool.rs` | **新建**：`ToolHandler` trait, `ToolResult`, `ToolError`, `ToolRegistry`, `ContentPart` | Registry 注册/查找单元测试 |
| 4 | `tavern-hero/src/hero.rs` | 新增 `skills_to_tool_defs()`：从 `SkillConfig` + `TAVERN_PUBLIC_URL` 构造完整 `Vec<ToolDef>`；`execute()` 方法调用它替代旧的 `ToolDef::from_skill()`。若 `TAVERN_PUBLIC_URL` 或 `TAVERN_TOOL_SECRET` 未设置，返回空数组（回退纯文本模式） | 转换函数单元测试（含 fallback 场景） |
| 5 | `tavern-adapters/src/pandaria.rs` | `create_session` 新增 `tools: &[Value]` 参数（由 `tool_def_to_pandaria_json()` 预序列化）；`execute()` 中调用该函数从 `&[ToolDef]` + `TAVERN_TOOL_SECRET` 生成 JSON；session payload 中包含完整 tools 数组 | mock wiremock 测试验证 session payload 含 tools |
| 6 | `tavern-server/src/router.rs` | 注册 `POST /api/tools/:name` 路由到 public_routes（工具端点独立鉴权，不走用户 Auth） | — |
| 7 | `tavern-server/src/handlers.rs` | 新增 `tool_call_handler`：验证 `Authorization` header（若 `TAVERN_TOOL_SECRET` 已设置）→ 查 Registry → 执行 handler → 根据 `ToolError` 分支返回 400/200+is_error | handler 单元测试 + 集成测试 |
| 8 | `tavern-server/src/main.rs` | 启动时创建 `ToolRegistry`、注册 `WebSearchHandler`、注入 `AppState`；读取 `TAVERN_TOOL_SECRET` 环境变量 | — |
| 9 | `tavern-core/`（或 `tavern-adapters/`） | 实现 `WebSearchHandler`（DDG 占位） | 单元测试（mock HTTP） |
| 10 | 集成测试 | e2e：Tavern → Pandaria 创建 session（含 tools）→ Pandaria 回调 tool endpoint → 验证响应格式 | e2e test |
| 11 | README + Cargo.toml | 更新 skills 文档、tool calling 架构说明；新增 `urlencoding` 依赖 | — |

## 不在此 spec 范围内

- 租户级配置覆盖（per-tenant API key / 配额）——后续 spec
- 用量计量与计费——后续 spec
- 动态注册/热加载 tool handler——后续 spec
- `code_exec` handler（沙箱）——独立 spec
- Flow DSL 的 tool 集成——后续 spec
- Tool call 幂等性 / 去重——后续 spec
- Tool call 可观测性（metrics / tracing）——后续 spec
- Polling 超时优化——后续 spec
