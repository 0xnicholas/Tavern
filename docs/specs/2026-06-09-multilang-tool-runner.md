# 多语言 Tool Runner — Subprocess + Sidecar 模式

**日期**: 2026-06-09
**状态**: 已评审（迭代 1）

---

## 背景

当前工具全部用 Rust 编写（impl `ToolHandler` trait），缺乏非 Rust 语言支持。目标：Python/JS/Go 等语言写的工具也能接入 Tavern 的 tool calling 管道，且输入输出协议统一。

## 设计决策

### 架构：三种 Runner 统一入口

`ToolHandler` trait 不变。新增两个通用 `ToolHandler` 实现，与现有 Rust handler 并列：

```
             ToolRegistry.get("web_search")
                      │
         ┌────────────┼──────────────┐
         ▼            ▼              ▼
  RustHandler   SubprocessHandler  SidecarHandler
  impl TH       impl TH            impl TH
```

Registry 里存的永远是 `Arc<dyn ToolHandler>`，调用方（`tool_call_handler`）不关心哪个实现。

### 协议统一

三种 runner 的输入输出格式完全一致——复用 Pandaria 回调 Tavern 的格式：

```
输入:  { "params": {...}, "tool_call_id": "...", "session_id": "...", "tenant_id": "..." }
输出:  { "content": [{"type":"text","text":"..."}], "is_error": false, "details": {...} }
```

Python 开发者只需要写一个接收 JSON stdin、输出 JSON stdout 的程序。格式和 Pandaria 回调 Tavern 的一致，减少认知负担。

## SkillConfig 扩展

新增 `runner` 和相关字段：

### YAML 配置

`runner` 是标量标签，`command`/`url`/`cwd`/`env` 是 `SkillConfig` 上的可选字段（与 `runner` 同级，不作为嵌套结构）：

```yaml
skills:
  # Rust 原生（默认）
  - id: web_search
    runner: rust

  # 子进程模式
  - id: code_exec
    runner: subprocess
    command: python3 tools/code_exec.py
    timeout_ms: 60000

  # HTTP 边车模式
  - id: data_analysis
    runner: sidecar
    url: http://localhost:8001/tools/analysis
    timeout_ms: 30000
```

### Rust 类型

`ToolRunner` 是简单 unit-variant 枚举。`command`/`url`/`cwd`/`env` 作为 `Option` 字段直接放在 `SkillConfig` 上——匹配 YAML 的扁平结构：

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunner {
    #[default]
    Rust,
    Subprocess,
    Sidecar,
}

// SkillConfig 新增字段：
pub struct SkillConfig {
    // ... 现有字段 ...

    #[serde(default)]
    pub runner: ToolRunner,       // 默认 Rust

    #[serde(default)]
    pub command: Option<String>,  // subprocess 模式：启动命令

    #[serde(default)]
    pub cwd: Option<String>,      // subprocess 模式：工作目录

    #[serde(default)]
    pub env: Option<std::collections::HashMap<String, String>>,  // subprocess 模式：环境变量

    #[serde(default)]
    pub url: Option<String>,      // sidecar 模式：边车 URL
}
```

`command` 字符串按空白字符拆分为程序名和参数：首 token 为可执行文件，其余 token 为参数（类似 shell 行为但不展开通配符/变量）。

`runner` 字段默认 `Rust`（向后兼容——不写 `runner` 的 YAML 行为不变）。

## SubprocessHandler

### 执行流程

```
SubprocessHandler::execute(params, ...)
  → 构建 command（支持 cwd 和 env 覆盖）
  → tokio::process::Command::new(command)
      .stdin(Stdio::piped())
      .stdout(Stdio::piped())
      .stderr(Stdio::piped())          // 捕获 stderr 用于错误报告
      .kill_on_drop(true)                // server shutdown 时自动杀子进程
      .spawn()
  → child.stdin.write_all(request_json)
  → child.stdin.shutdown()              // 通知子进程输入结束
  → timeout(timeout_ms, child.wait_with_output())
  → 检查 exit code、解析 stdout JSON → ToolResult
```

**输出大小限制**：stdout 最多读取 10MB。超过则 kill 子进程，返回 `is_error: true`。

**协议约束**：子进程在读完 stdin 之前不能写 stdout（避免死锁）。对于请求-响应模式的工具这是自然行为，但需要在 Python 协议文档中明确说明。

**环境变量**：默认子进程继承 server 的环境（包括 `TAVERN_TOOL_SECRET` 等敏感变量）。YAML 可配置 `env: {}` 清除所有环境变量，或指定白名单。

### 错误处理

| 场景 | 返回 |
|---|---|
| 子进程启动失败 | `is_error: true, content: "failed to spawn: {err}"` |
| 超时 | kill 进程 + `is_error: true, content: "timeout after {ms}ms"` |
| 子进程返回非零 exit code | `is_error: true, content: "exit code {n}: {stderr}"` |
| stdout 不是合法 JSON | `is_error: true, content: "invalid JSON from tool"` |
| 正常执行 | 子进程的 JSON 响应原样返回 |

### 进程生命周期

- **每次调用 spawn**：简单，无状态管理
- **优化（后续）**：进程池复用，减少冷启动延迟
- MVP 不实现进程池

## SidecarHandler

### 执行流程

```
SidecarHandler::execute(params, ...)
  → POST {url}
      Body: { "params": {...}, "tool_call_id": "...", "session_id": "...", "tenant_id": "..." }
      Timeout: {timeout_ms}
  → 响应体 JSON 原样作为 ToolResult 返回
```

### 错误处理

| 场景 | 返回 |
|---|---|
| 连接失败/超时 | `is_error: true, content: "sidecar unreachable: {err}"` |
| 响应非 2xx | `is_error: true, content: "sidecar returned {status}: {body}"` |
| 响应不是合法 JSON | `is_error: true, content: "invalid JSON from sidecar"` |
| 正常执行 | sidecar 的 JSON 响应原样返回 |

## Python 工具协议

无论 subprocess 还是 sidecar，Python 工具代码完全相同——只是入口不同：

```python
#!/usr/bin/env python3
import sys, json

def handle(request: dict) -> dict:
    params = request["params"]
    query = params.get("query", "")

    # 实际逻辑
    results = do_search(query)

    return {
        "content": [{"type": "text", "text": format_results(results)}],
        "is_error": False,
    }

# === 子进程入口 ===
if __name__ == "__main__":
    request = json.loads(sys.stdin.read())
    response = handle(request)
    print(json.dumps(response))

# === HTTP 入口（FastAPI）===
# from fastapi import FastAPI
# app = FastAPI()
# @app.post("/tools/search")
# async def search(request: ToolCallRequest):
#     return handle(request.dict())
```

## Pandaria ToolConfig 影响

三种 runner 在 Pandaria 看来完全相同——都是 `HttpProxyTool`，都通过 `endpoint` 回调。区别只在 Tavern 内部（`tool_call_handler` → `ToolRegistry.get` → handler 实现不同）。`tool_def_to_pandaria_json()` 不需要改动。

## 启动注册与热重载

### 冷启动

`main.rs` 在 `hero.load_from_dir()` 完成后调用 `register_external_tools(&hero, &tool_registry)`：

```rust
async fn register_external_tools(hero: &TavernHero, registry: &ToolRegistry) {
    for agent in hero.list_agents().await {  // 从 Hero 拿，不重新解析 YAML
        for skill in &agent.skills {
            match skill.runner {
                ToolRunner::Subprocess => {
                    if let Some(ref command) = skill.command {
                        let handler = SubprocessHandler::new(
                            command, skill.timeout_ms,
                            skill.cwd.as_deref(), skill.env.as_ref(),
                        );
                        if registry.get(&skill.id).is_some() {
                            tracing::warn!(
                                tool_id = %skill.id,
                                agent_id = %agent.id,
                                "tool re-registered; previous handler replaced"
                            );
                        }
                        registry.register(skill.id.clone(), Arc::new(handler));
                    }
                }
                ToolRunner::Sidecar => {
                    if let Some(ref url) = skill.url {
                        let handler = SidecarHandler::new(url, skill.timeout_ms);
                        registry.register(skill.id.clone(), Arc::new(handler));
                    }
                }
                ToolRunner::Rust => { /* 已在步骤 1 手动注册 */ }
            }
        }
    }
}
```

注册 key 统一使用 `skill.id`（稳定标识符），不使用 `skill.name`（LLM 可见名）。Pandaria 回调 `/api/tools/{id}`，handler 查找也用 `id`，三者一致。

### 热重载

**当前限制**：Agent 热重载（`start_agent_watcher`）不会更新 `ToolRegistry`。
- 如果用户在热重载时新增了 `runner: subprocess` skill，工具不会自动注册
- 如果移除了一个 skill，对应 handler 残留在 Registry 中

**处理方式**（MVP）：文档化此限制。热重载后需重启 server 才能让新工具生效。后续 spec 可扩展为 watcher 也传入 `Arc<ToolRegistry>` 并做全量刷新。

## 请求序列化

Subprocess/Sidecar handler 需要将 `ToolHandler::execute()` 的四个参数序列化为 JSON。复用 handler 中已有的 `ToolCallRequest` struct，将其可见性改为 `pub(crate)`：

```rust
// tavern-server/src/handlers.rs
#[derive(Serialize)]  // 加 Serialize derive
pub(crate) struct ToolCallRequest { ... }
```

两个 handler 可直接 `serde_json::to_string(&ToolCallRequest { ... })`。

## 实现步骤

| 步骤 | 文件 | 内容 |
|---|---|---|
| 1 | `tavern-core/src/config.rs` | SkillConfig 新增 `runner: ToolRunner`（简单 unit 枚举），`command`/`cwd`/`env`/`url` 为 Option 字段；`ToolRunner` 需 `derive(Default)` |
| 2 | `tavern-server/src/tools/mod.rs` | 添加 `pub mod subprocess; pub mod sidecar;` |
| 3 | `tavern-server/src/tools/subprocess.rs` | 实现 `SubprocessHandler`：spawn 子进程，stdin/stdout/stderr piped，`kill_on_drop`，10MB 输出上限，`command` 空白字符拆分 |
| 4 | `tavern-server/src/tools/sidecar.rs` | 实现 `SidecarHandler`：HTTP POST 到 sidecar URL，超时处理，1MB 响应上限 |
| 5 | `tavern-server/src/handlers.rs` | `ToolCallRequest` 加 `#[derive(Serialize)]`，改为 `pub(crate)` |
| 6 | `tavern-server/src/main.rs` | 新增 `async fn register_external_tools()`：从 `hero.list_agents()` 遍历，skill.id 为注册 key，同名 `warn!` |
| 7 | 测试 | SubprocessHandler UT（shell echo mock）；SidecarHandler UT（wiremock）；子进程 stderr 成功时 `debug!` 日志 |
| 8 | README | 新增 runner 文档 + Python 示例 + 热重载限制 + `validate_agent_config` 不校验 runner 字段的说明 |

## 不在此 spec 范围内

- 进程池（SubprocessHandler 复用进程）
- 子进程健康检查 / 自动重启
- Sidecar 服务发现（目前是固定 URL）
- `runner: wasm` 模式
