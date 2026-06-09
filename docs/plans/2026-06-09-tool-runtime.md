# Tool Runtime 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 ToolHandler 抽象、WebSearchHandler、HTTP 回调端点、ToolDef 扩展，完成 Tavern → Pandaria 的 tool calling 管道

**Architecture:** 扩展 ToolDef 携带完整 Pandaria ToolConfig 字段，新增 ToolHandler trait + ToolRegistry 做工具执行，hero 侧 skills_to_tool_defs() 构造完整 ToolDef，PandariaRuntime 从 ToolDef 序列化 Pandaria wire format

**Tech Stack:** Rust, axum, reqwest, serde, tokio

---

## 文件结构

| 文件 | 操作 | 职责 |
|---|---|---|
| `crates/tavern-core/src/config.rs` | 修改 | SkillConfig 加字段 |
| `crates/tavern-core/src/runtime.rs` | 修改 | 扩展 ToolDef，删除 from_skill() |
| `crates/tavern-core/src/tool.rs` | **新建** | ToolHandler trait, ToolResult, ToolError, ToolRegistry, ContentPart |
| `crates/tavern-core/src/lib.rs` | 修改 | 导出 tool 模块 |
| `crates/tavern-hero/src/hero.rs` | 修改 | skills_to_tool_defs(), 更新 execute 方法 |
| `crates/tavern-adapters/src/pandaria.rs` | 修改 | tool_def_to_pandaria_json(), create_session 传 tools |
| `crates/tavern-adapters/src/mock.rs` | 修改 | MockRuntime 适配新 ToolDef |
| `crates/tavern-server/src/tools/web_search.rs` | **新建** | WebSearchHandler 实现 |
| `crates/tavern-server/src/tools/mod.rs` | **新建** | tools 模块声明 |
| `crates/tavern-server/src/router.rs` | 修改 | 注册 /api/tools/:name |
| `crates/tavern-server/src/handlers.rs` | 修改 | tool_call_handler |
| `crates/tavern-server/src/state.rs` | 修改 | AppState 加 ToolRegistry |
| `crates/tavern-server/src/main.rs` | 修改 | 启动注册 handler，读取 env vars |
| `crates/tavern-server/Cargo.toml` | 修改 | 加 reqwest + urlencoding dep |
| `crates/tavern-comp/src/engine/tests.rs` | 修改 | 适配新 ToolDef |
| `crates/tavern-hero/src/lib.rs` | 修改 | 测试适配 |
| `crates/tavern-adapters/src/lib.rs` | 修改 | 测试适配 |

---

### Task 1: SkillConfig 扩展

**Files:**
- Modify: `crates/tavern-core/src/config.rs`

- [ ] **Step 1: 加字段**

```rust
// SkillConfig 新增字段（全部有 serde default，向后兼容）
pub struct SkillConfig {
    pub id: String,

    #[serde(default)]  // None 时 fallback 到 id
    pub name: Option<String>,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]  // JSON Schema，默认 {}
    pub parameters: serde_json::Value,

    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,

    #[serde(default)]
    pub config: serde_json::Value,  // 私有配置
}

fn default_timeout() -> u64 { 30000 }
```

- [ ] **Step 2: 写 YAML 反序列化测试** — 验证旧格式和完整新格式都能正确解析

- [ ] **Step 3: 运行测试** `cargo test -p tavern-core --lib`

- [ ] **Step 4: Commit** `git commit -m "feat(core): extend SkillConfig with name, description, parameters, timeout_ms"`

---

### Task 2: ToolDef 扩展

**Files:**
- Modify: `crates/tavern-core/src/runtime.rs`
- Modify: `crates/tavern-core/src/lib.rs`

- [ ] **Step 1: 扩展 ToolDef 并删除 from_skill()**

```rust
pub struct ToolDef {
    pub id: String,

    #[serde(default)]
    pub name: String,

    #[serde(default)]
    pub description: String,

    #[serde(default)]
    pub parameters: Value,

    #[serde(default)]
    pub endpoint: String,

    #[serde(default = "default_tool_timeout")]
    pub timeout_ms: u64,

    #[serde(default)]
    pub config: Option<Value>,
}

fn default_tool_timeout() -> u64 { 30000 }

// 删除: impl ToolDef { pub fn from_skill(...) }
```

- [ ] **Step 2: 更新 lib.rs 导出** — `pub use runtime::{Runtime, RuntimeError, ToolDef};` 保持不变

- [ ] **Step 3: 写 ToolDef 序列化测试** — 验证默认值和完整反序列化

- [ ] **Step 4: 运行测试** `cargo test -p tavern-core --lib`

- [ ] **Step 5: Commit** `git commit -m "feat(core): extend ToolDef with pandaria wire fields"`

---

### Task 3: ToolHandler trait + ToolRegistry

**Files:**
- Create: `crates/tavern-core/src/tool.rs`
- Modify: `crates/tavern-core/src/lib.rs`

- [ ] **Step 1: 新建 tool.rs**

```rust
// 完整内容见 spec 第 8-10 节
pub enum ToolError { InvalidParams(String), ExecutionFailed(String), NotFound(String) }

#[async_trait::async_trait]
pub trait ToolHandler: Send + Sync { ... }

pub struct ToolResult { pub content: Vec<ContentPart>, pub is_error: bool, pub details: Option<Value> }

pub struct ContentPart { #[serde(rename = "type")] pub content_type: String, pub text: Option<String> }

pub struct ToolRegistry { handlers: HashMap<String, Arc<dyn ToolHandler>> }

impl ToolRegistry { pub fn new(), pub fn register(), pub fn get() }
```

- [ ] **Step 2: 导出** — `pub mod tool;` 加到 lib.rs；`pub use tool::*;`

- [ ] **Step 3: 写 Registry 单元测试** — 注册、查找、查找不存在的 key

- [ ] **Step 4: 运行测试** `cargo test -p tavern-core --lib`

- [ ] **Step 5: Commit** `git commit -m "feat(core): add ToolHandler trait, ToolRegistry, ToolResult"`

---

### Task 4: hero 端 skills_to_tool_defs()

**Files:**
- Modify: `crates/tavern-hero/src/hero.rs`

- [ ] **Step 1: 实现 skills_to_tool_defs()**

```rust
fn skills_to_tool_defs(skills: &[tavern_core::SkillConfig]) -> Vec<tavern_core::ToolDef> {
    let public_url = match std::env::var("TAVERN_PUBLIC_URL") {
        Ok(url) => url.trim_end_matches('/').to_string(),
        Err(_) => return vec![],  // 未配置 → 空数组
    };
    let has_secret = std::env::var("TAVERN_TOOL_SECRET").is_ok();
    if !has_secret {
        return vec![];
    }

    skills.iter().map(|s| {
        tavern_core::ToolDef {
            id: s.id.clone(),
            name: s.name.clone().unwrap_or_else(|| s.id.clone()),
            description: s.description.clone().unwrap_or_default(),
            parameters: s.parameters.clone(),
            endpoint: format!("{}/api/tools/{}", public_url, s.name.as_deref().unwrap_or(&s.id)),
            timeout_ms: s.timeout_ms,
            config: if s.config.is_null() { None } else { Some(s.config.clone()) },
        }
    }).collect()
}
```

- [ ] **Step 2: 更新 execute() 和 execute_with_model()**

把 `ToolDef::from_skill()` 替换为 `skills_to_tool_defs(&agent.skills)`

- [ ] **Step 3: 写转换函数测试** — 正常场景、TAVERN_PUBLIC_URL 未设→空数组、TAVERN_TOOL_SECRET 未设→空数组

- [ ] **Step 4: 运行测试** `cargo test -p tavern-hero --lib`

- [ ] **Step 5: Commit** `git commit -m "feat(hero): add skills_to_tool_defs with env-var gating"`

---

### Task 5: PandariaRuntime tool 序列化

**Files:**
- Modify: `crates/tavern-adapters/src/pandaria.rs`

- [ ] **Step 1: 实现 tool_def_to_pandaria_json()**

```rust
fn tool_def_to_pandaria_json(tools: &[tavern_core::ToolDef]) -> Vec<serde_json::Value> {
    let secret = std::env::var("TAVERN_TOOL_SECRET").ok();
    tools.iter().map(|t| {
        let mut obj = serde_json::json!({
            "name": t.name,
            "description": t.description,
            "parameters": t.parameters,
            "endpoint": t.endpoint,
            "timeout_ms": t.timeout_ms,
        });
        if let Some(ref s) = secret {
            obj["headers"] = json!({"Authorization": format!("Bearer {}", s)});
        } else {
            obj["headers"] = Value::Null;
        }
        obj
    }).collect()
}
```

- [ ] **Step 2: 更新 create_session** — 新增 `tools: &[Value]` 参数，`payload["tools"]` 条件追加

- [ ] **Step 3: 更新 execute** — 调用 `tool_def_to_pandaria_json(tools)` 并传入 `create_session`

- [ ] **Step 4: 运行测试** `cargo test -p tavern-adapters --lib`（现有 wiremock 测试应通过，tools 为空时 payload 不变）

- [ ] **Step 5: Commit** `git commit -m "feat(adapters): serialize ToolDef to Pandaria ToolConfig wire format"`

---

### Task 6: MockRuntime 适配

**Files:**
- Modify: `crates/tavern-adapters/src/mock.rs`

- [ ] **Step 1: 确认 MockRuntime 签名** — handler 已接收 `&[ToolDef]`，新 ToolDef 字段被忽略即可

- [ ] **Step 2: 运行测试** `cargo test -p tavern-adapters --lib`

- [ ] **Step 3: Commit**（若无变化则合并到 Task 5 的 commit）

---

### Task 7: 测试适配（hero + adapter + comp）

**Files:**
- Modify: `crates/tavern-hero/src/lib.rs`
- Modify: `crates/tavern-adapters/src/lib.rs`
- Modify: `crates/tavern-comp/src/engine/tests.rs`

旧代码中 `ToolDef::from_skill()` 已被删除，调用处需适配新的构造函数。测试中 MockRuntime 闭包签名可能需要微调（因为 `ToolDef` 新增了字段但测试用 `&[]` 传空数组即可）。

- [ ] **Step 1: 全局搜索 `from_skill`** 确认无残留引用

- [ ] **Step 2: 更新所有调用处** 为直接构造 ToolDef 或传 `&[]`

- [ ] **Step 3: 运行完整测试** `cargo test -p tavern-hero -p tavern-adapters -p tavern-comp --lib`

- [ ] **Step 4: Commit** `git commit -m "test: adapt to extended ToolDef"`

---

### Task 8: WebSearchHandler 实现

**Files:**
- Create: `crates/tavern-server/src/tools/mod.rs`
- Create: `crates/tavern-server/src/tools/web_search.rs`
- Modify: `crates/tavern-server/src/lib.rs`（声明 `pub mod tools;`）
- Modify: `crates/tavern-server/Cargo.toml`（加 `reqwest` 和 `urlencoding` 为正式依赖）

- [ ] **Step 1: 创建 mod.rs** — `pub mod web_search;`

- [ ] **Step 2: 实现 WebSearchHandler**

```rust
// 完整代码见 spec 内置 Handler 节
// 包含 WebSearchHandler::new(), query_ddg(), ToolHandler impl, format_ddg_response()
```

- [ ] **Step 3: 添加依赖**

```toml
# Cargo.toml
reqwest = { version = "0.12", features = ["json"] }  # 从 dev-dep 提升
urlencoding = "2"
```

- [ ] **Step 4: 写 WebSearchHandler 单元测试** — mock reqwest 响应，验证格式化输出

- [ ] **Step 5: 在 lib.rs 声明** `pub mod tools;`

- [ ] **Step 6: 运行测试** `cargo test -p tavern-server --lib`

- [ ] **Step 7: Commit** `git commit -m "feat(server): add WebSearchHandler (DDG demo placeholder)"`

---

### Task 9: AppState + ToolRegistry 注入

**Files:**
- Modify: `crates/tavern-server/src/state.rs`

- [ ] **Step 1: AppState 加字段**

```rust
pub struct AppState {
    // ... 现有字段 ...
    pub tool_registry: Arc<tavern_core::ToolRegistry>,
}
```

- [ ] **Step 2: 运行编译检查** `cargo check -p tavern-server`

- [ ] **Step 3: Commit** `git commit -m "feat(server): add ToolRegistry to AppState"`

---

### Task 10: /api/tools/:name 路由 + handler

**Files:**
- Modify: `crates/tavern-server/src/router.rs`
- Modify: `crates/tavern-server/src/handlers.rs`

- [ ] **Step 1: router.rs 注册路由**

```rust
// 加到 public_routes（独立鉴权）
.route("/api/tools/:name", post(handlers::tool_call_handler))
```

- [ ] **Step 2: handlers.rs 实现 tool_call_handler**

```rust
#[derive(Deserialize)]
struct ToolCallRequest {
    tool_call_id: String,
    params: Value,
    session_id: String,
    tenant_id: String,
}

pub async fn tool_call_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ToolCallRequest>,
) -> Result<impl IntoResponse, (StatusCode, ApiError)> {
    // 1. Auth check (if TAVERN_TOOL_SECRET set)
    if let Ok(secret) = std::env::var("TAVERN_TOOL_SECRET") {
        let auth = headers.get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if auth != format!("Bearer {}", secret) {
            return Err((StatusCode::UNAUTHORIZED, ApiError::new(401, "Unauthorized", "")));
        }
    }

    // 2. Lookup handler
    let handler = state.tool_registry.get(&name)
        .ok_or_else(|| (StatusCode::NOT_FOUND, ApiError::new(404, "ToolNotFound", format!("tool '{}' not registered", name))))?
        .clone();

    // 3. Execute
    match handler.execute(body.params, &body.tenant_id, &body.session_id, &body.tool_call_id).await {
        Ok(result) => Ok(Json(result)),
        Err(ToolError::InvalidParams(msg)) => Err((StatusCode::BAD_REQUEST, ApiError::new(400, "InvalidParams", msg))),
        Err(ToolError::ExecutionFailed(msg)) => Ok(Json(ToolResult {
            content: vec![ContentPart { content_type: "text".into(), text: Some(msg) }],
            is_error: true,
            details: None,
        })),
        Err(ToolError::NotFound(msg)) => Err((StatusCode::NOT_FOUND, ApiError::new(404, "ToolNotFound", msg))),
    }
}
```

- [ ] **Step 3: 运行编译检查** `cargo check -p tavern-server`

- [ ] **Step 4: 写 handler 测试** — tool 未找到 → 404，handler 返回 error → 200 + is_error，auth fail → 401

- [ ] **Step 5: Commit** `git commit -m "feat(server): add POST /api/tools/:name endpoint with auth"`

---

### Task 11: main.rs 启动注册

**Files:**
- Modify: `crates/tavern-server/src/main.rs`

- [ ] **Step 1: 创建并注入 ToolRegistry**

```rust
let tool_registry = tavern_core::ToolRegistry::new();
tool_registry.register("web_search".to_string(), Arc::new(tavern_server::tools::web_search::WebSearchHandler::new()));
let tool_registry = Arc::new(tool_registry);
```

- [ ] **Step 2: 传入 AppState** — 在 app_state 构造中加 `tool_registry: tool_registry.clone()`

- [ ] **Step 3: 运行编译检查** `cargo check -p tavern-server`

- [ ] **Step 4: Commit** `git commit -m "feat(server): register WebSearchHandler at startup"`

---

### Task 12: 集成测试 + e2e

**Files:**
- Modify: `crates/tavern-server/src/main.rs`（已有 e2e 测试）

- [ ] **Step 1: 写 tool call e2e 测试** — 启动 test app，POST /api/tools/web_search，验证返回格式

- [ ] **Step 2: 写 Pandaria session 集成测试** — 验证 create_session payload 包含 tools 字段

- [ ] **Step 3: 运行完整测试** `cargo test --workspace --lib && cargo test -p tavern-server`

- [ ] **Step 4: Commit** `git commit -m "test: add e2e tests for tool call pipeline"`

---

### Task 13: README 更新

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 更新 skills 文档** — 标注 skills 现在支持 tool calling 和所需环境变量

- [ ] **Step 2: 新增环境变量说明** — `TAVERN_PUBLIC_URL`, `TAVERN_TOOL_SECRET`

- [ ] **Step 3: Commit** `git commit -m "docs: update README for tool calling and new env vars"`
