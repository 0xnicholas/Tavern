# Tavern V0.3.5: Webhook 回调

> **状态：FROZEN** — 2026-05-30，review 1 轮，6 个问题全部修复  
> **依赖**：`reqwest`、`hmac`、`sha2`（新增到 tavern-comp）  
> **目标**：Workflow 执行完成时回调外部 URL

## 1. 概述

### 1.1 问题

当前 Workflow 执行结果只能通过 API 轮询或 SSE 获取。在事件驱动架构中，调用方需要 Tavern 主动推送结果到指定 URL，而非被动等待查询。

### 1.2 设计原则

- **零破坏**：现有 API 不变，198 测试继续通过
- **Fire-and-forget**：回调不影响 Workflow 执行结果
- **覆盖同步和异步执行**：`run()` 和 `start()` 完成时都触发回调
- **仅配置层**：不新增 API 端点，Workflow YAML + engine 层

---

## 2. 配置

### Workflow YAML

```yaml
id: content_pipeline
name: 内容生产流水线
webhook:
  url: "https://example.com/callback"
  secret: "my-secret"         # HMAC-SHA256 签名密钥（可选）
  timeout_secs: 30            # 回调请求超时秒数，默认 30
  retries: 3                  # 回调失败重试次数，0-10，默认 0
  retry_delay: 5              # 重试间隔秒数，默认 5
steps:
  - id: research
    ...
```

所有字段可选。`webhook` 缺失或 `url` 为空则跳过回调。`retries` 上限 10。

---

## 3. 回调 Payload

### 请求

```
POST {url}
Content-Type: application/json
X-Tavern-Signature: sha256=<hex>  (仅当 secret 配置时)
```

### Body

```json
{
  "event": "workflow.completed",
  "workflow_id": "content_pipeline",
  "execution_id": "550e8400-...",
  "status": "completed",
  "context": { "topic": "AI", "research_notes": "...", "draft": "..." },
  "outputs": { "article": "..." },
  "step_results": {
    "research": { "status": "Completed", "output": "..." },
    "write": { "status": "Completed", "output": "..." },
    "edit": { "status": "Completed", "output": "..." }
  },
  "timestamp": "2026-05-30T12:00:00Z"
}
```

| 字段 | completed | failed |
|------|-----------|--------|
| `event` | `"workflow.completed"` | `"workflow.failed"` |
| `status` | `"completed"` | `"failed"` |
| `step_results` | 正常 | 包含错误信息 |

### 签名

```
HMAC-SHA256(body_bytes, secret) → hex
Header: X-Tavern-Signature: sha256=<hex>
```

---

## 4. 实现

### 4.1 触发点

在 `run_interpreter_loop` 的完成路径（`Action::Complete` 和 `Action::Fail` 分支），确保 `run()` 和异步 `start()` 都触发：

```rust
// engine.rs: run_interpreter_loop 末尾
if let Some(ref webhook) = workflow.webhook {
    if !webhook.url.is_empty() {
        let payload = build_webhook_payload(
            &workflow.id,
            &instance_id,
            &result,  // &Result<WorkflowResult, CompError>
        );
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(webhook.timeout_secs.unwrap_or(30)))
            .build()
            .unwrap_or_default();
        let url = webhook.url.clone();
        let secret = webhook.secret.clone();
        let retries = webhook.retries.unwrap_or(0).min(10);
        let retry_delay = webhook.retry_delay.unwrap_or(5);
        tokio::spawn(async move {
            send_webhook(client, &url, &payload, secret.as_deref(), retries, retry_delay).await;
        });
    }
}
```

### 4.2 重试逻辑（指数退避）

```rust
async fn send_webhook(
    client: reqwest::Client,
    url: &str,
    payload: &Value,
    secret: Option<&str>,
    retries: u64,
    retry_delay: u64,
) {
    let body = payload.to_string();
    let mut req = client
        .post(url)
        .header("Content-Type", "application/json")
        .body(body.clone());

    if let Some(secret) = secret {
        let sig = compute_hmac_sha256(body.as_bytes(), secret);
        req = req.header("X-Tavern-Signature", format!("sha256={}", sig));
    }

    for attempt in 0..=retries {
        match req.try_clone() {
            Some(r) => {
                let resp = r.send().await;
                match resp {
                    Ok(r) if r.status().is_success() => return,
                    Ok(r) => {
                        tracing::warn!(
                            url = %url, status = %r.status(), attempt = attempt,
                            "webhook delivery failed"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            url = %url, error = %e, attempt = attempt,
                            "webhook delivery error"
                        );
                    }
                }
            }
            None => break, // request body consumed, can't retry
        }
        if attempt < retries {
            let delay = retry_delay * 2u64.pow(attempt as u32);
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
    }
    tracing::error!(url = %url, "webhook failed after {} retries", retries);
}
```

### 4.3 配置类型

```rust
// workflow.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub retries: Option<u64>,
    #[serde(default)]
    pub retry_delay: Option<u64>,
}

// Workflow 新增字段
#[serde(default)]
pub webhook: Option<WebhookConfig>,
```

---

## 5. 边界条件

| 场景 | 行为 |
|------|------|
| Workflow 未配置 webhook | 不回调 |
| `webhook.url` 为空 | 不回调 |
| 回调 HTTP 超时 | 重试 |
| `retries > 10` | 截断为 10 |
| 重试耗尽 | 记录 error 日志，不影响 Workflow 结果 |
| 回调期间进程崩溃 | 回调丢失（fire-and-forget，不保证送达） |
| 同步 `POST /run` | 回调在后台 spawned task 中执行 |
| 异步 `POST /start` | 同上，解释器完成时触发 |

---

## 6. 测试计划

| # | 测试 | 层级 |
|---|------|:--:|
| 1 | 配置 webhook 的 Workflow 执行成功，回调被调用，payload 正确 | unit |
| 2 | 配置 webhook 的 Workflow 执行失败，event="workflow.failed" | unit |
| 3 | 未配置 webhook 的 Workflow，不发起 HTTP 请求 | unit |
| 4 | 回调 URL 不可达，重试耗尽后仅日志不抛错 | unit |
| 5 | HMAC-SHA256 签名正确生成 | unit |
| 6 | `retries: 100` 被截断为 10 | unit |

使用 `wiremock`（需新增 `tavern-comp` dev-dependency）模拟回调服务器。

---

## 7. 实现文件

| 文件 | 动作 | 内容 |
|------|------|------|
| `crates/tavern-comp/Cargo.toml` | 修改 | 新增 `reqwest`、`hmac`、`sha2` 依赖 + `wiremock` dev-dependency |
| `crates/tavern-comp/src/workflow.rs` | 修改 | `WebhookConfig` 类型、Workflow 新增 `webhook` 字段 |
| `crates/tavern-comp/src/engine.rs` | 修改 | `run_interpreter_loop` 完成路径触发回调 + `send_webhook` + `build_webhook_payload` + `compute_hmac_sha256` |

---

## 附录 A: Review 修订记录

| # | 级别 | 原问题 | 修订 |
|---|:--:|------|------|
| 2 | HIGH | 回调仅在 `run()` 中触发，异步 `start()` 遗漏 | 移到 `run_interpreter_loop` 的 `Action::Complete`/`Action::Fail` 分支 |
| 3 | HIGH | `event` 字段始终为 `"workflow.completed"` | 改为动态值：成功 `workflow.completed`，失败 `workflow.failed` |
| 1 | MEDIUM | 依赖位置讨论 | 确认在 `tavern-comp` 新增 `reqwest`（务实选择） |
| 4 | MEDIUM | 缺少超时配置 | 新增 `timeout_secs` 字段，默认 30 |
| 5 | MEDIUM | `retries` 无上限 | 增加 `.min(10)` 截断 |
| 6 | LOW | wiremock 跨 crate 使用 | 明确 `tavern-comp` 需新增 wiremock dev-dependency |
