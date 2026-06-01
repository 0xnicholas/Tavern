# Tavern V0.3.1: Execution Replay API

> **状态**: COMPLETE — 2026-05-28  
> 目标版本: V0.3.1  
> 前置依赖: V0.3.0 (Flow 生产化已完成)

---

## 1. 概述

执行回放 API 允许用户查询已完成的 Workflow 或 Flow 实例的完整执行历史，以**时间线视图**形式呈现步骤状态演进、上下文变化和事件序列。区别于 `GET /executions/:id/events`（原始事件流），回放 API 提供**渲染后的可读视图**，包含 `StateDiff`、执行摘要和多种粒度控制。

### 1.1 设计原则

- **只读**：回放是纯查询操作，不产生任何副作用，不修改 EventStore
- **增量重建**：逐条重放 `WorkflowEvent` 并对比 `before/after` `InstanceState`，生成精确的 `StateDiff`
- **时间窗口过滤**：在 EventStore 层提前过滤，避免加载无关事件
- **向后兼容**：不修改现有 `WorkflowEvent`、`InstanceState`、`EventStore` trait

---

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| Replay | 从 EventStore 读取事件流并渲染为时间线视图的过程 |
| TimelineEntry | 时间线上的单个条目，对应一个 `WorkflowEvent` + `StateDiff` |
| StateDiff | 事件应用前后 `InstanceState` 的差异（context keys、step status） |
| Detail Level | `low` / `medium` / `high`，控制返回的事件类型和 diff 粒度 |

---

## 3. 数据模型

### 3.1 ExecutionReplay

```rust
pub struct ExecutionReplay {
    pub execution_id: String,
    pub workflow_id: String,
    /// 实例首次产生 InstanceStarted 事件的时间；若不存在（如空执行），回退为当前 UTC 时间
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status: String,
    pub total_events: usize,
    pub timeline: Vec<TimelineEntry>,
    pub summary: ReplaySummary,
}
```

### 3.2 TimelineEntry

```rust
pub struct TimelineEntry {
    pub sequence: usize,
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub step_id: Option<String>,
    pub state_diff: Option<StateDiff>,
    /// 仅 StepCompleted/StepFailed 有值：该步骤从 Started 到 Completed/Failed 的耗时
    pub duration_ms: Option<u64>,
    /// detail=high 时包含原始事件的完整 payload
    pub raw_payload: Option<Value>,
}
```

### 3.3 StateDiff

```rust
pub struct StateDiff {
    pub context_changed: bool,
    pub context_keys_added: Vec<String>,
    pub context_keys_modified: Vec<String>,
    pub step_status_before: Option<String>,
    pub step_status_after: Option<String>,
    /// 截断到 500 字符的预览（避免大 payload）
    pub output_preview: Option<String>,
}
```

### 3.4 ReplaySummary

```rust
pub struct ReplaySummary {
    pub total_steps: usize,
    pub completed_steps: usize,
    pub failed_steps: usize,
    pub total_duration_ms: u64,
    pub retries_count: usize,
    pub signals_received: usize,
}
```

---

## 4. REST API

### 4.1 端点

```
GET /executions/:id/replay
```

### 4.2 Query 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `detail` | `low` \| `medium` \| `high` | `medium` | 事件粒度级别 |
| `from` | ISO8601 | — | 起始时间（可选，含边界） |
| `to` | ISO8601 | — | 结束时间（可选，含边界） |
| `step_id` | string | — | 只返回指定步骤的事件（可选）。保留匹配步骤的事件，同时保留无 step_id 的全局生命周期事件（如 InstanceStarted、WorkflowCompleted） |

### 4.3 Detail 级别定义

| 级别 | 包含的事件类型 | StateDiff 内容 | raw_payload |
|------|---------------|---------------|-------------|
| `low` | InstanceStarted, StepStarted, StepCompleted, StepFailed, WorkflowCompleted, WorkflowFailed | `step_status_before/after` only; `context_changed: false`, `context_keys_added: []`, `context_keys_modified: []` | ❌ |
| `medium` | low + SignalReceived, SignalWaitStarted, CancelRequested, StepRetryScheduled | + context_keys_added/modified + output_preview | ❌ |
| `high` | 全部事件类型 | 完整 StateDiff + output_preview | ✅ |

### 4.4 响应示例（detail=medium）

**200 OK:**
```json
{
  "execution_id": "550e8400-e29b-41d4-a716-446655440000",
  "workflow_id": "content_pipeline",
  "started_at": "2026-05-27T10:00:00Z",
  "completed_at": "2026-05-27T10:12:40Z",
  "status": "Completed",
  "total_events": 47,
  "timeline": [
    {
      "sequence": 1,
      "timestamp": "2026-05-27T10:00:00Z",
      "event_type": "InstanceStarted",
      "step_id": null,
      "state_diff": {
        "context_changed": false,
        "context_keys_added": [],
        "context_keys_modified": [],
        "step_status_before": null,
        "step_status_after": "Running"
      },
      "duration_ms": null,
      "raw_payload": null
    },
    {
      "sequence": 4,
      "timestamp": "2026-05-27T10:00:02Z",
      "event_type": "StepStarted",
      "step_id": "research",
      "state_diff": {
        "context_changed": false,
        "context_keys_added": [],
        "context_keys_modified": [],
        "step_status_before": "Pending",
        "step_status_after": "Running"
      },
      "duration_ms": null,
      "raw_payload": null
    },
    {
      "sequence": 7,
      "timestamp": "2026-05-27T10:02:34Z",
      "event_type": "StepCompleted",
      "step_id": "research",
      "state_diff": {
        "context_changed": true,
        "context_keys_added": ["research_notes"],
        "context_keys_modified": [],
        "step_status_before": "Running",
        "step_status_after": "Completed",
        "output_preview": "关于 AI Agent 的研究报告..."
      },
      "duration_ms": 152000,
      "raw_payload": null
    }
  ],
  "summary": {
    "total_steps": 3,
    "completed_steps": 3,
    "failed_steps": 0,
    "total_duration_ms": 760000,
    "retries_count": 0,
    "signals_received": 0
  }
}
```

---

## 5. 架构

```
┌─────────────────────────────────────────────────────────────┐
│                    tavern-server                              │
│  GET /executions/:id/replay                                   │
│       ↓                                                       │
│  handlers::replay_execution_handler                           │
│       ↓                                                       │
│  ExecutionReplayer::replay(store, instance_id, opts)          │
│       ↓                                                       │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              tavern-comp                                  │ │
│  │                                                           │ │
│  │  1. EventStore::read_stream(instance_id)                  │ │
│  │     → Vec<WorkflowEvent>                                  │ │
│  │  2. 按时间窗口过滤 (from/to)                               │ │
│  │  3. 按 step_id 过滤（如指定）                              │ │
│  │  4. 按 detail 级别过滤事件类型                             │ │
│  │  5. 逐条重放重建 InstanceState，生成 StateDiff             │ │
│  │  6. 组装 TimelineEntry + ReplaySummary                    │ │
│  │                                                           │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 5.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `ExecutionReplayer` | `crates/tavern-comp/src/replay.rs` | 读取 EventStore、过滤、重放、生成回放视图 |
| `ReplayOptions` | `crates/tavern-comp/src/replay.rs` | 查询参数封装（detail、时间窗口、step_id） |
| `replay_execution_handler` | `crates/tavern-server/src/handlers.rs` | HTTP 层：参数解析、错误映射、响应组装 |

### 5.2 StateDiff 计算

StateDiff 在逐条重放事件时**增量计算**，而非比较两个完整 JSON 对象：

1. 在 `apply(event)` 前 `clone` 当前 `InstanceState` 为 `before`
2. `apply(event)` 后得到 `after`
3. 比较 `before.context` vs `after.context` 的 keys 差异
4. 根据事件类型推导 `step_status_before/after`
5. 对 `StepCompleted` 截取 `output_preview`（截断到 500 字符）

### 5.3 性能优化

- **时间窗口过滤优先**：`from/to` 参数在读取事件流后立刻过滤，减少后续处理量
- **Lazy raw_payload**：`detail=high` 时才序列化原始 payload，避免不必要的 JSON 转换

---

## 6. 错误处理

### 6.1 新增 CompError 变体

```rust
#[derive(Debug, thiserror::Error)]
pub enum CompError {
    // ... existing variants ...

    #[error("invalid replay range: {reason}")]
    InvalidReplayRange { reason: String },

    #[error("invalid parameter '{field}': {reason}")]
    InvalidParameter { field: String, reason: String },
}
```

### 6.2 HTTP 状态码映射

| 场景 | CompError | HTTP Status |
|------|-----------|-------------|
| execution_id 不存在 | `InstanceNotFound` | 404 |
| `from` > `to` | `InvalidReplayRange` | 400 |
| `detail` 值非法 | `InvalidParameter` | 400 |
| EventStore 读取失败 | `StoreError` | 503 |
| `step_id` 在 workflow 中不存在 | —（返回空时间线） | 200 |

---

## 7. 测试策略

| 测试 | 说明 |
|------|------|
| `test_replay_basic_timeline` | 标准 workflow 执行后回放，验证时间线顺序、summary 正确 |
| `test_replay_detail_low` | `detail=low` 只返回用户级事件，无 raw_payload |
| `test_replay_detail_medium` | `detail=medium` 包含 Signal 事件和 context diff |
| `test_replay_detail_high` | `detail=high` 包含全部事件和 raw_payload |
| `test_replay_time_window` | `from/to` 过滤，只返回窗口内事件 |
| `test_replay_step_filter` | `?step_id=research` 只返回该步骤事件 |
| `test_replay_empty_execution` | 刚 start 尚未产生事件，返回空时间线 |
| `test_replay_nonexistent_instance` | 返回 404 |
| `test_replay_invalid_range` | `from` > `to` 返回 400 |

**目标**：新增 9 个测试，测试数 173 → **182**。

---

## 8. 文件变更清单

### 新增

```
crates/tavern-comp/src/replay.rs          # ExecutionReplayer, ReplayOptions, StateDiff, TimelineEntry, ReplaySummary
```

### 修改

```
crates/tavern-comp/src/lib.rs             # pub mod replay; pub use replay::*;
crates/tavern-comp/src/error.rs           # + InvalidReplayRange, InvalidParameter
crates/tavern-server/src/handlers.rs      # + replay_execution_handler
crates/tavern-server/src/router.rs        # + GET /executions/:id/replay
```

---

## 9. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 大事件流（>10000 条）导致内存爆炸 | 时间窗口过滤减少加载量 |
| 逐条重建 state 太慢 | 仅 diff context keys（不 deep-diff 值） |
| context diff 计算耗时 | 只比较 keys，不 deep-diff 值 |
| 重复请求相同 replay | V0.3.1 不加缓存，V0.4.x 考虑 Redis LRU |
| 时间窗口过滤对无显式时间戳事件不可靠 | InstanceCreated/InstanceStarted/StepScheduled/StepFailed/SignalWaitStarted/TimerFired/External 的事件时间戳回退为 `Utc::now()`，可能导致 `from/to` 过滤不准确 |

---

## 10. 向后兼容

- 不修改任何现有 trait、struct、enum
- 不修改现有端点行为
- 新增端点 `GET /executions/:id/replay` 为纯增量

---

## 11. 验收标准

- [x] `cargo test --workspace` 183 全绿
- [x] `cargo clippy --workspace` 零新增 warning（仅 tavern-flow 已有 warning）
- [x] `cargo fmt -- --check` 零变更
- [x] `GET /executions/:id/replay` 返回正确时间线（detail=medium）
- [x] `detail=low` 只返回用户级事件
- [x] `detail=high` 包含 raw_payload
- [x] `from/to` 时间窗口过滤正确
- [x] `step_id` 过滤正确
- [x] 不存在的 execution_id → 404
- [x] `from` > `to` → 400
- [x] 空执行返回空时间线 + summary
