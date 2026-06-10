# Multi-Language Tool Runner 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现 SubprocessHandler + SidecarHandler，支持 Python/JS/Go 等语言编写的工具接入 tool calling 管道

**Architecture:** ToolRunner 枚举 + SkillConfig 扩展；SubprocessHandler（stdin/stdout JSON）+ SidecarHandler（HTTP POST）；启动时自动注册

**Tech Stack:** Rust, tokio::process, reqwest, serde, axum

---

### Task 1: SkillConfig + ToolRunner 类型

**Files:**
- Modify: `crates/tavern-core/src/config.rs`

- [ ] **Step 1: 加 ToolRunner 枚举 + SkillConfig 字段**

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunner {
    #[default]
    Rust,
    Subprocess,
    Sidecar,
}

// SkillConfig 新增：
#[serde(default)] pub runner: ToolRunner,
#[serde(default)] pub command: Option<String>,
#[serde(default)] pub cwd: Option<String>,
#[serde(default)] pub env: Option<std::collections::HashMap<String, String>>,
#[serde(default)] pub url: Option<String>,
```

- [ ] **Step 2: YAML 反序列化测试** — 旧格式（无 runner）→ 默认 Rust，新格式 runner: subprocess + command → 正确解析

- [ ] **Step 3: Commit** `feat(core): add ToolRunner enum and SkillConfig fields for external tools`

---

### Task 2: ToolCallRequest 序列化

**Files:**
- Modify: `crates/tavern-server/src/handlers.rs`

- [ ] **Step 1: 加 Serialize + pub(crate)**

```rust
#[derive(serde::Deserialize, serde::Serialize)]  // 加 Serialize
pub(crate) struct ToolCallRequest { ... }
```

- [ ] **Step 2: Commit** `feat(server): make ToolCallRequest serializable for handler reuse`

---

### Task 3: SubprocessHandler

**Files:**
- Modify: `crates/tavern-server/src/tools/mod.rs`
- Create: `crates/tavern-server/src/tools/subprocess.rs`

- [ ] **Step 1: 实现**

```rust
pub struct SubprocessHandler {
    command: String,
    timeout_ms: u64,
    cwd: Option<String>,
    env: Option<HashMap<String, String>>,
}

impl SubprocessHandler {
    pub fn new(command: &str, timeout_ms: u64, cwd: Option<&str>, env: Option<&HashMap<String, String>>) -> Self { ... }
}

impl ToolHandler for SubprocessHandler {
    async fn execute(&self, params, tenant_id, session_id, tool_call_id) -> Result<ToolResult, ToolError> {
        // 1. 构建 request JSON
        // 2. 拆分 command: 首 token = prog，其余 = args
        // 3. spawn: stdin/stdout/stderr piped, kill_on_drop(true)
        // 4. cwd/env 配置
        // 5. 写 stdin + shutdown
        // 6. tokio::time::timeout + wait_with_output
        // 7. stdout 最多 10MB（truncate + kill）
        // 8. exit 0 → 解析 stdout JSON → ToolResult
        // 9. exit != 0 → is_error: true + stderr
        // 10. 超时 → kill + is_error: true
    }
}
```

- [ ] **Step 2: 单元测试** — echo mock（`echo '{"content":[{"type":"text","text":"ok"}],"is_error":false}'`）

- [ ] **Step 3: Commit**

---

### Task 4: SidecarHandler

**Files:**
- Create: `crates/tavern-server/src/tools/sidecar.rs`

- [ ] **Step 1: 实现**

```rust
pub struct SidecarHandler {
    url: String,
    timeout_ms: u64,
}

impl ToolHandler for SidecarHandler {
    async fn execute(...) -> Result<ToolResult, ToolError> {
        // POST url, body = ToolCallRequest JSON, timeout
        // 响应最多 1MB
        // 非 2xx → is_error: true
        // 正常 → 解析 JSON → ToolResult
    }
}
```

- [ ] **Step 2: wiremock 测试**

- [ ] **Step 3: Commit**

---

### Task 5: 启动自动注册

**Files:**
- Modify: `crates/tavern-server/src/main.rs`

- [ ] **Step 1: 实现 `register_external_tools()`** — 从 `hero.list_agents()` 遍历，skill.id 为 key，subprocess/sidecar 自动注册，重复 warn!

- [ ] **Step 2: 调用** — 在 `hero.load_from_dir()` 之后，`app_state` 构造之前

- [ ] **Step 3: Commit**

---

### Task 6: 测试 + README

- [ ] **Step 1: e2e 测试** — POST /api/tools/test_subprocess → 200

- [ ] **Step 2: README 更新** — runner 文档 + Python 示例 + 热重载限制

- [ ] **Step 3: Commit**
