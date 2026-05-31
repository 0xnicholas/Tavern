# Tavern V0.3.4: 批量执行

> **状态：FROZEN** — 2026-05-30，review 1 轮，6 个问题全部修复  
> **依赖**：无外部依赖  
> **目标**：对同一 Workflow 并行提交多组输入，聚合结果

## 1. 概述

### 1.1 问题

当前每次 `POST /workflows/:id/run` 只能传入一组 `inputs`。用户需要处理多条数据时（如批量审核 100 篇文章、批量研究 50 个话题），必须串行调用 API 或手动写脚本循环，效率低且无法利用服务端并行能力。

### 1.2 设计原则

- **零破坏**：现有 API 不变，194 测试继续通过
- **零新依赖**：纯 server 层功能，复用现有 engine + tokio
- **尽力而为**：部分成功，不因单条失败而中止整批
- **上限可控**：限制每批最大条数，防止 DoS
- **真正并行**：使用 `engine.start()` + `ExecutionHandle` + `JoinSet`，非串行阻塞

### 1.3 已知限制

- 客户端 HTTP 断开后，已 spawned 的 task **继续执行**（tokio::spawn 是 detach 的）。不在此版本解决。
- 100 条 × LLM 输出可能产生 MB 级响应体，调用方通过 `max_concurrency` 和 batch size 自行控制。

---

## 2. API

### `POST /workflows/:id/run_batch`

**Request:**
```json
{
  "inputs": [
    { "topic": "AI Agent 框架" },
    { "topic": "Rust 异步编程" },
    { "topic": "分布式系统设计" }
  ],
  "max_concurrency": 5
}
```

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|:--:|--------|------|
| `inputs` | `[Value]` | ✅ | — | 每组输入参数的数组，长度 1 ~ 100 |
| `max_concurrency` | `u32` | ❌ | `10` | 最大并行执行数，1 ~ 50 |

**Response (200):**
```json
{
  "workflow_id": "content_pipeline",
  "total": 3,
  "succeeded": 2,
  "failed": 1,
  "results": [
    {
      "index": 0,
      "execution_id": "uuid-1",
      "status": "completed",
      "inputs": { "topic": "AI Agent 框架" },
      "outputs": { "article": "..." },
      "duration_ms": 15234
    },
    {
      "index": 1,
      "execution_id": "uuid-2",
      "status": "completed",
      "inputs": { "topic": "Rust 异步编程" },
      "outputs": { "article": "..." },
      "duration_ms": 12300
    },
    {
      "index": 2,
      "execution_id": "uuid-3",
      "status": "failed",
      "inputs": { "topic": "分布式系统设计" },
      "error": "Step 'research' failed: timeout",
      "duration_ms": 30050
    }
  ]
}
```

> 响应体可能较大（100 条 × 输出），建议调用方按需控制 batch size。

**错误响应**：

| 场景 | 状态码 | 说明 |
|------|:------:|------|
| Workflow 不存在 | 404 | 同现有 `/run` |
| `inputs` 为空或 `len > 100` | 400 | `"batch size must be 1-100"` |
| `max_concurrency` 为 0 或 `> 50` | 400 | `"max_concurrency must be 1-50"` |
| Agent 不存在 | 502 | 单条错误，其余继续 |
| 全部失败 | 200 | `succeeded: 0, failed: N`（正常响应体） |

---

## 3. 实现

### 3.1 常量

```rust
const MAX_BATCH_SIZE: usize = 100;
const MAX_BATCH_CONCURRENCY: usize = 50;
```

### 3.2 执行流程

```
1. 校验 workflow 存在 + inputs.len() in [1, 100] + max_concurrency in [1, 50]
2. 对每组 inputs 调用 engine.start(workflow, inputs) → Vec<ExecutionHandle>
3. 用 JoinSet + Semaphore(max_concurrency) 并行 await_completion
4. 收集所有结果，聚合为 BatchResult
5. 更新 metrics：workflow_executions += total, failures += failed, duration_ms_total += sum
```

### 3.3 伪代码

```rust
async fn run_workflow_batch_handler(...) -> Result<Json<BatchResponse>, ...> {
    let workflow = registry.get(&id)?;
    validate_inputs(&req)?; // len, max_concurrency

    let engine = WorkflowEngine::new(hero)
        .with_max_concurrency(max_concurrency)
        .with_store(event_store);

    let semaphore = Arc::new(Semaphore::new(req.max_concurrency as usize));
    let mut join_set = JoinSet::new();

    for (i, inputs) in req.inputs.into_iter().enumerate() {
        let engine = engine.clone();
        let workflow = workflow.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        join_set.spawn(async move {
            let _permit = permit;
            let start = Instant::now();
            let result = engine.start(&workflow, inputs).await;
            // ... handle result, track metrics ...
            (i, result, start.elapsed().as_millis() as u64)
        });
    }

    let mut results = Vec::new();
    while let Some(Ok((i, result, duration_ms))) = join_set.join_next().await {
        results.push(/* build BatchResultItem */);
    }
    results.sort_by_key(|r| r.index);

    // Update metrics
    state.workflow_executions.fetch_add(total, Relaxed);
    state.workflow_failures.fetch_add(failed, Relaxed);
    state.workflow_duration_ms_total.fetch_add(total_duration_ms, Relaxed);

    Ok(Json(BatchResponse { ... }))
}
```

### 3.4 超时与取消

- 单条执行超时：依赖 Workflow step 自身的 `timeout` 配置
- 整批超时：不设服务端超时。调用方管理 HTTP 超时
- 取消：客户端断开 → spawned task 继续执行（已知限制）

---

## 4. 边界条件

| 场景 | 行为 |
|------|------|
| 空 inputs | `400 BadRequest` |
| inputs > 100 | `400 BadRequest` |
| max_concurrency 为 0 或 > 50 | `400 BadRequest` |
| Workflow 不存在 | `404 NotFound` |
| 某条 inputs 不合法（missing required） | 该条失败，其余继续 |
| 全部失败 | `200`，succeeded=0 |
| 客户端 HTTP 断开 | spawned task 继续执行，最终写入 EventStore |
| 同一 inputs 重复提交 | 各自独立执行，不同 execution_id |

---

## 5. 测试计划

| # | 测试 | 层级 |
|---|------|:--:|
| 1 | 3 组 inputs 全部成功，返回 3 completed，metrics 更新 | e2e |
| 2 | 1 组 missing input 失败，其余成功，succeeded=2 failed=1 | e2e |
| 3 | 空 inputs 返回 400 | unit |
| 4 | 101 组 inputs 返回 400 | unit |
| 5 | max_concurrency=0 返回 400 | unit |
| 6 | max_concurrency=1 串行执行成功 | unit |
| 7 | Workflow 不存在返回 404 | unit |

---

## 6. 实现文件

| 文件 | 动作 | 内容 |
|------|------|------|
| `crates/tavern-server/src/handlers.rs` | 修改 | `BatchRunRequest`、`BatchResponse`、`run_workflow_batch_handler` |
| `crates/tavern-server/src/router.rs` | 修改 | `POST /workflows/:id/run_batch` |
| `crates/tavern-server/src/main.rs` | 修改 | 新增 2 个集成测试 |

> 纯 server 层，不涉及 comp 引擎。总新增代码约 80 行。

---

## 附录 A: Review 修订记录

| # | 级别 | 原问题 | 修订 |
|---|:--:|------|------|
| 1 | HIGH | `engine.run()` 是串行阻塞的 | 改用 `engine.start()` + `ExecutionHandle` + `JoinSet` + `Semaphore` |
| 2 | HIGH | spawned task 不会随 HTTP 断开取消 | 明确标注为已知限制 |
| 3 | HIGH | 缺少 metrics 更新 | 补充 `fetch_add` 逻辑 |
| 4 | MEDIUM | 响应体可能过大 | 文档加注说明 |
| 5 | MEDIUM | failed 条目无 duration_ms | 补充 failed 也包含 duration_ms |
| 6 | LOW | Semaphore + engine max_concurrency 叠加 | 确认无问题，不需修改 |
