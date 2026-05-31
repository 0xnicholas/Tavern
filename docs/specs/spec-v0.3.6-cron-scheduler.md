# Tavern V0.3.6: Cron 定时调度

> **状态：FROZEN** — 2026-05-30，review 1 轮，6 个问题全部修复  
> **依赖**：零新依赖（自实现 cron 解析器）  
> **目标**：按 Cron 表达式定时触发 Workflow 执行

## 1. 概述

### 1.1 问题

当前 Workflow 只能通过 HTTP API 手动触发。周期性任务（如每小时分析数据、每天生成报告）需要外部 cron 服务或手动脚本定时调用 API。

### 1.2 设计原则

- **零新依赖**：自实现 5 字段 cron 解析器
- **最小化**：仅支持 `matches(now)` 检查，不实现 `next_after`（避免日期计算复杂度）
- **后台任务**：server 启动后后台 tokio task 每秒检查
- **防重复**：同一分钟只触发一次
- **参数化**：定时任务可配置默认 inputs
- **自动发现**：Scheduler 每 60 秒扫描 registry，无需单独 reload API
- **UTC**：所有 cron 表达式按 UTC 解析

---

## 2. 配置

### Workflow YAML

```yaml
id: daily_report
name: 日报生成
schedule: "0 8 * * *"         # 每天早上 8:00 UTC
schedule_inputs:               # 定时触发时传入的 inputs（可选）
  topic: "每日技术动态"
steps:
  - id: generate
    agent_id: writer
    task: "研究以下主题: {{topic}}"
    ...
```

| 字段 | 语法 | 示例 | 默认值 |
|------|------|------|--------|
| `schedule` | 标准 5 字段 cron | `"*/15 * * * *"` | `None`（不定时） |
| `schedule_inputs` | JSON object | `{"topic": "日报"}` | `{}` |

支持的 cron 语法：`*`、数字（`5`）、步进（`*/15`、`3/15`）、列表（`0,6,12,18`）。  
不支持：`?`、`L`、`W`、`#`、`@yearly` 别名、秒字段、字符串月份/星期。

---

## 3. API

### `GET /schedules`

列出所有已注册的定时任务。

```json
{
  "schedules": [
    {
      "workflow_id": "daily_report",
      "schedule": "0 8 * * *",
      "inputs": { "topic": "每日技术动态" },
      "last_run_at": "2026-05-30T08:00:05Z",
      "last_execution_id": "uuid"
    }
  ]
}
```

`POST /schedules/reload` **不提供**。Scheduler 每 60 秒自动从 registry 扫描。

---

## 4. Cron 解析器

```rust
struct CronExpr {
    minutes: CronField,
    hours: CronField,
    days_of_month: CronField,
    months: CronField,
    days_of_week: CronField,
}

enum CronField {
    Any,                                // *
    Single(u8),                         // 5
    List(Vec<u8>),                      // 0,6,12,18
    Step { interval: u8, start: u8 },   // */15 = 每15从0开始；3/15 = 每15从3开始
}

impl CronExpr {
    fn parse(expr: &str) -> Result<Self, String>  // 5 字段空格分隔
    fn matches(&self, dt: &DateTime<Utc>) -> bool // 当前时间是否匹配
}
```

不实现 `next_after`。调度器用 `matches(now)` + 分钟级去重替代。

---

## 5. 调度器

```rust
pub struct Scheduler {
    hero: Arc<TavernHero>,
    store: Arc<dyn EventStore>,
    registry: Arc<RwLock<WorkflowRegistry>>,
    state: Arc<RwLock<SchedulerState>>,
}

struct SchedulerState {
    entries: Vec<ScheduleEntry>,
    last_scan: DateTime<Utc>,
}

struct ScheduleEntry {
    workflow_id: String,
    inputs: Value,
    cron: CronExpr,
    last_run_minute: Option<i64>,   // 防止同分钟重复触发
    last_execution_id: Option<String>,
}
```

### 后台循环

```rust
async fn run(self: Arc<Self>) {
    let mut tick = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        tick += 1;
        let now = Utc::now();

        // 每 60 秒重新扫描 registry
        if tick % 60 == 0 {
            self.refresh_entries().await;
        }

        let entries = {
            let state = self.state.read().await;
            state.entries.clone() // 释放锁后再触发
        };

        for entry in &entries {
            let current_minute = now.timestamp() / 60;
            if entry.last_run_minute == Some(current_minute) {
                continue; // 同分钟已触发
            }
            if !entry.cron.matches(&now) {
                continue;
            }

            // 触发 workflow
            let workflow = {
                let registry = self.registry.read().await;
                registry.get(&entry.workflow_id).cloned()
            };

            if let Some(wf) = workflow {
                let engine = WorkflowEngine::new(self.hero.clone())
                    .with_store(self.store.clone());
                match engine.start(&wf, entry.inputs.clone()).await {
                    Ok(handle) => {
                        let exec_id = handle.id().to_string();
                        tokio::spawn(async move { let _ = handle.await_completion().await; });
                        let mut state = self.state.write().await;
                        if let Some(e) = state.entries.iter_mut().find(|e| e.workflow_id == entry.workflow_id) {
                            e.last_run_minute = Some(current_minute);
                            e.last_execution_id = Some(exec_id);
                        }
                    }
                    Err(e) => {
                        tracing::error!(workflow_id = %entry.workflow_id, error = %e, "scheduled workflow failed to start");
                    }
                }
            }
        }
    }
}
```

关键：先 clone entries 释放读锁，再执行 `engine.start()`。

---

## 6. 边界条件

| 场景 | 行为 |
|------|------|
| Workflow 无 `schedule` | 不调度 |
| `schedule` 语法错误 | 启动/scan 时解析失败，记录 error，跳过该 workflow |
| 同分钟多次匹配 | `last_run_minute` 去重，只触发一次 |
| 上次执行仍在运行 | 不检查执行状态，到分钟即触发（可能并发多实例） |
| Server 重启 | `last_run_minute` 丢失，重新计算 |
| 多个 workflow 同时触发 | 顺序 `engine.start()`，各自独立 |
| 调度精度 | 秒级（每 tick 1s），cron 最小粒度分钟 |

---

## 7. 测试计划

| # | 测试 | 层级 |
|---|------|:--:|
| 1 | `CronExpr::parse("0 8 * * *")` → matches 8:00，不匹配 8:01 | unit |
| 2 | `CronExpr::parse("*/15 * * * *")` → matches 0,15,30,45 分 | unit |
| 3 | `CronExpr::parse("0 0,12 * * *")` → matches 0:00 和 12:00 | unit |
| 4 | 无效表达式解析失败 | unit |
| 5 | 同分钟多次 tick 仅触发一次 | unit |
| 6 | Workflow 无 schedule 不被调度 | unit |
| 7 | `schedule_inputs` 正确传入 engine.start() | unit |

---

## 8. 实现文件

| 文件 | 动作 | 内容 |
|------|------|------|
| `crates/tavern-comp/src/workflow.rs` | 修改 | Workflow 新增 `schedule`、`schedule_inputs` 字段 |
| `crates/tavern-server/src/scheduler.rs` | 新建 | `CronExpr` + `CronField` + `Scheduler` + `ScheduleInfo` |
| `crates/tavern-server/src/handlers.rs` | 修改 | `list_schedules_handler` |
| `crates/tavern-server/src/router.rs` | 修改 | `GET /schedules` |
| `crates/tavern-server/src/main.rs` | 修改 | 启动 Scheduler，注入 AppState |
| `crates/tavern-server/src/state.rs` | 修改 | AppState 新增 `scheduler` |

---

## 附录 A: Review 修订记录

| # | 级别 | 原问题 | 修订 |
|---|:--:|------|------|
| 1 | HIGH | 定时任务无法传入参数 | 新增 `schedule_inputs` 字段 |
| 2 | HIGH | `next_after` 实现复杂 | 去掉，用 `matches(now)` + 分钟去重 |
| 3 | HIGH | 读锁内调用 engine.start() | clone entries 后释放锁再触发 |
| 4 | MEDIUM | `/schedules/reload` 冗余 | 去掉，Scheduler 每 60s 自动扫描 registry |
| 5 | LOW | `Step(u8, u8)` 语义不明 | 改为 `Step { interval, start }` |
| 6 | LOW | 缺少 DST 说明 | 明确 cron 按 UTC 解析 |
