# Tavern

Fast and Flexible Multi-Agent Automation Framework — event-sourced orchestration with pluggable agent runtimes.

**One engine, two ways to define workflows:** YAML for production operations, Rust proc-macros for code-first pipelines.

- **Tavern Comp**: Event-sourced workflow engine — DAG scheduling (AND/OR dependencies), Router conditional branching, parallel execution, retries, signals, approval gates, breakpoints, cron scheduling, batch execution, execution replay
- **Tavern Hero**: Agent lifecycle management — YAML-defined agents, runtime CRUD, validation, hot-reload
- **Tavern Flow**: Rust-native DSL — `#[start]` `#[listen]` `#[router]` proc-macros that compile to Comp workflows
- **Tavern Server**: REST API (27 endpoints), SSE real-time event streams, Prometheus metrics, API Key / Bearer Token auth, tenant rate limiting

Consumes [Pandaria](https://github.com/0xnicholas/pandaria) as the agent execution runtime.

## Quick Start

### Build

```bash
cargo build --workspace
```

### Test

```bash
cargo test --workspace
```

### Run Server

```bash
# Default: SQLite (system libsqlite3)
RUNTIME_URL=http://localhost:8080 cargo run -p tavern-server

# With bundled SQLite (v3.45+, no system dependency)
RUNTIME_URL=http://localhost:8080 cargo run -p tavern-server --features bundled-sqlite

# With PostgreSQL
RUNTIME_URL=http://localhost:8080 TAVERN_STORE__STORE_TYPE=postgres \
  TAVERN_STORE__DATABASE_URL=postgres://tavern:tavern@localhost:5432/tavern \
  cargo run -p tavern-server --features postgres
```

Listens on `0.0.0.0:3000`. Agent configs load from `./configs/agents`, workflow configs from `./configs/workflows`.

### Docker

```bash
docker compose up -d          # SQLite mode
docker compose --profile postgres up -d  # PostgreSQL mode
```

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `TAVERN_RUNTIME__URL` | Yes | — | Pandaria Runtime HTTP endpoint |
| `TAVERN_AGENT__CONFIG_DIR` | No | `./configs/agents` | Agent YAML config directory |
| `TAVERN_WORKFLOW__CONFIG_DIR` | No | `./configs/workflows` | Workflow YAML config directory |
| `TAVERN_SERVER__HOST` | No | `0.0.0.0` | Server bind address |
| `TAVERN_SERVER__PORT` | No | `3000` | Server port |
| `TAVERN_STORE__STORE_TYPE` | No | `sqlite` | `sqlite` or `postgres` |
| `TAVERN_STORE__DATABASE_URL` | No | `sqlite://data/tavern.db` | Database connection URL |
| `TAVERN_AUTH__AUTH_TYPE` | No | `none` | `none`, `api_key`, or `bearer` |
| `TAVERN_AUTH__API_KEY` | No | — | API Key (when `auth_type=api_key`) |
| `TAVERN_OBSERVABILITY__LOG_FORMAT` | No | `text` | `text` or `json` |
| `TAVERN_OBSERVABILITY__METRICS_PUBLIC` | No | `false` | Expose `/metrics` without auth |
| `TAVERN_RATE_LIMIT__ENABLED` | No | `false` | Enable per-tenant rate limiting |
| `RUST_LOG` | No | `info` | Log level (`debug`, `info`, `warn`, `error`) |

## Defining Agents

Each agent is a YAML file in `configs/agents/`:

```yaml
# configs/agents/researcher.yml
id: researcher
name: 研究员
model:
  provider: openai
  name: gpt-4o
instructions: |
  你是信息收集专家。对给定主题进行全面调研，输出结构化笔记。
skills:
  - id: web_search
```

Agents can use different models, different providers, and independent system prompts. Supported providers: OpenAI, Anthropic, and any OpenAI-compatible API.

## Defining Workflows — YAML

```yaml
# configs/workflows/content_pipeline.yml
id: content_pipeline
name: 内容生产流水线
description: 研究 → 写作 → 编辑 → 审批 → 发布

steps:
  - id: research
    agent_id: researcher
    task: "研究以下主题并整理关键信息: {{topic}}"
    output_key: research_notes
    retries: 1
    retry_delay: 2

  - id: write
    agent_id: writer
    task: "根据以下研究资料撰写文章: {{research_notes}}"
    depends_on: [research]
    output_key: draft
    retries: 1

  - id: edit
    agent_id: editor
    task: "编辑以下文章，改进语言和结构: {{draft}}"
    depends_on: [write]
    output_key: final_article
    wait_for_signal: editorial_approval    # 人工审批节点
    signal_timeout: 86400                  # 24 小时超时

  - id: publish
    agent_id: publisher
    task: "发布已审批文章: {{final_article}}"
    depends_on: [edit]

inputs:
  - name: topic
    required: true

outputs:
  - name: final_article
    value: "{{final_article}}"
```

### OR Dependencies (V0.4)

Steps can trigger when **any one** upstream completes, not just all:

```yaml
  - id: notify
    agent_id: notifier
    task: "任一数据源就绪后发送通知"
    or_depends_on: [fetch_api, fetch_db]   # 谁先完成用谁
```

### Hierarchical Process (Manager Agent)

For tasks where execution order is unpredictable — a Manager Agent dynamically delegates:

```yaml
id: adaptive_team
name: 自适应研究团队
process: hierarchical

manager:
  agent_id: manager
  instructions: |
    你是项目经理。根据研究需求，动态委派任务给最合适的专家。

steps:
  - id: research_task
    agent_id: researcher
    task: "研究: {{topic}}"
    expected_output: "详细研究报告"

  - id: analysis_task
    agent_id: analyst
    task: "深度分析研究结果"
    expected_output: "分析报告与建议"
```

### Step Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `depends_on` | `[]string` | `[]` | AND: all upstreams must complete |
| `or_depends_on` | `[]string` | `[]` | OR: any upstream triggers (V0.4) |
| `output_key` | `string` | — | Context key for step output |
| `timeout` | `int` | `300` | Step timeout in seconds |
| `retries` | `int` | `0` | Max retry attempts |
| `retry_delay` | `int` | `0` | Seconds between retries |
| `wait_for_signal` | `string` | — | External signal name (approval gate) |
| `signal_timeout` | `int` | — | Signal wait timeout in seconds |
| `signal_timeout_action` | `string` | `fail` | `fail` or `reject` on timeout |
| `breakpoint` | `bool` | `false` | Pause before execution (debug) |
| `model_override` | `object` | — | Override agent model for this step |
| `expected_output` | `string` | — | Hint for Manager/Planning agent |

## Defining Workflows — Rust DSL

For code-first pipelines with conditional logic:

```rust
use tavern_flow::{Flow, flow_impl, start, listen, router, FlowError};
use std::sync::Arc;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct TeamState {
    draft: Option<String>,
    approved: bool,
}

#[derive(Flow)]
struct ContentTeam {
    state: TeamState,
    hero: Arc<TavernHero>,
}

#[flow_impl(crate = "tavern_flow")]
impl ContentTeam {
    #[start]
    async fn research(&mut self) -> Result<String, FlowError> {
        let result = self.hero.execute("researcher", "调研AI Agent框架", None).await
            .map_err(|e| FlowError::Other(e.to_string()))?;
        Ok(result.to_string())
    }

    #[listen("research")]
    async fn write(&mut self, notes: String) -> Result<String, FlowError> {
        let article = format!("根据以下笔记撰写: {}", notes);
        self.state.draft = Some(article.clone());
        Ok(article)
    }

    #[router("write")]
    async fn quality_gate(&mut self, draft: String) -> String {
        if draft.len() > 500 {
            self.state.approved = true;
            "approved".to_string()
        } else {
            "rewrite".to_string()
        }
    }

    #[listen("approved")]
    async fn publish(&mut self, draft: String) -> Result<String, FlowError> {
        Ok(format!("发布: {}", draft))
    }

    #[listen("rewrite")]
    async fn revise(&mut self, draft: String) -> Result<String, FlowError> {
        let better = self.hero.execute("writer", &format!("重写: {}", draft), None).await
            .map_err(|e| FlowError::Other(e.to_string()))?;
        Ok(better.to_string())
    }
}

// Execute
#[tokio::main]
async fn main() {
    let pipeline = ContentTeam { state: TeamState { draft: None, approved: false }, hero };
    let result = pipeline.run(serde_json::json!({})).await.unwrap();
}
```

| Proc-macro | Expands to |
|-----------|-----------|
| `#[start]` | Step with no dependencies |
| `#[listen("method")]` | Step with OR dependency on method |
| `#[listen(or("a","b"))]` | Step with OR dependency: any of a,b |
| `#[listen(and("a","b"))]` | Step with AND dependency: both a,b |
| `#[router("upstream")]` | Step that outputs label(s) to route downstream |
| `#[flow_impl]` | Generates FlowStepExecutor + Workflow definition + `run()` |

The proc-macros expand to `tavern_comp::Workflow` + `tavern_comp::FlowStepExecutor` and execute on the same `WorkflowEngine`. No separate runtime.

## API Endpoints

### Agent

| Method | Path | Description |
|---|---|---|
| `GET` | `/agents` | List all agents |
| `GET` | `/agents/:id` | Get agent config |
| `POST` | `/agents` | Create agent (runtime, no restart) |
| `DELETE` | `/agents/:id` | Delete agent |
| `POST` | `/agents/:id/execute` | Execute agent directly |

### Workflow

| Method | Path | Description |
|---|---|---|
| `GET` | `/workflows` | List all workflows |
| `POST` | `/workflows` | Register a workflow (JSON) |
| `GET` | `/workflows/:id` | Get workflow definition |
| `DELETE` | `/workflows/:id` | Remove a workflow |
| `POST` | `/workflows/:id/run` | Run workflow (sync, returns result) |
| `POST` | `/workflows/:id/start` | Start workflow (async, returns execution_id) |
| `POST` | `/workflows/:id/run_batch` | Batch parallel execution |
| `POST` | `/workflows/reload` | Hot-reload configs from YAML dir |

### Execution

| Method | Path | Description |
|---|---|---|
| `GET` | `/executions/:id` | Query execution state + step results |
| `GET` | `/executions/:id/events` | Get event log (audit trail) |
| `GET` | `/executions/:id/events/stream` | SSE real-time event stream |
| `POST` | `/executions/:id/signal` | Send signal (approve/reject/webhook) |
| `POST` | `/executions/:id/cancel` | Cancel running execution |
| `GET` | `/executions/:id/replay` | Execution replay (StateDiff + timeline) |
| `POST` | `/executions/:id/clone` | Clone historical execution, re-trigger |

### Approval

| Method | Path | Description |
|---|---|---|
| `GET` | `/approvals` | List pending approval steps |
| `POST` | `/executions/:id/steps/:step_id/approve` | Approve step |
| `POST` | `/executions/:id/steps/:step_id/reject` | Reject step |

### Observability

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/breakpoints` | List breakpoint steps |
| `GET` | `/schedules` | List cron schedules |
| `POST` | `/auth/refresh` | Token refresh (Bearer mode) |

## Examples

### Execute Agent

```bash
curl -X POST http://localhost:3000/agents/researcher/execute \
  -H "Content-Type: application/json" \
  -d '{"task": "调查 AI Agent 框架发展现状"}'
```

### Run Workflow (Sync)

```bash
curl -X POST http://localhost:3000/workflows/content_pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"topic": "Rust 生态 2026 年发展趋势"}'
```

Response:

```json
{
  "context": {
    "topic": "Rust 生态 2026 年发展趋势",
    "research_notes": "...",
    "draft": "...",
    "final_article": "..."
  },
  "outputs": {
    "final_article": "..."
  },
  "step_results": {
    "research": { "status": "Completed", "output": "...", "attempt": 1 },
    "write":    { "status": "Completed", "output": "...", "attempt": 1 },
    "edit":     { "status": "Completed", "output": "...", "attempt": 1 }
  }
}
```

### Start Workflow (Async) + SSE Stream

```bash
# Start
curl -X POST http://localhost:3000/workflows/content_pipeline/start \
  -H "Content-Type: application/json" \
  -d '{"topic": "异步执行示例"}'
# -> { "execution_id": "550e8400-e29b-41d4-a716-446655440000" }

# Real-time event stream
curl -N http://localhost:3000/executions/550e8400.../events/stream

# Query state
curl http://localhost:3000/executions/550e8400-e29b-41d4-a716-446655440000
```

### Approval Workflow

```bash
# List pending approvals
curl http://localhost:3000/approvals

# Approve a step
curl -X POST http://localhost:3000/executions/<id>/steps/edit/approve \
  -H "Content-Type: application/json" \
  -d '{"reviewer": "alice", "comments": "Looks good"}'

# Reject a step
curl -X POST http://localhost:3000/executions/<id>/steps/edit/reject \
  -H "Content-Type: application/json" \
  -d '{"reviewer": "alice", "reason": "Needs more research"}'
```

### Dynamic Workflow Registration

```bash
curl -X POST http://localhost:3000/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "id": "quick_analysis",
    "name": "快速分析",
    "steps": [
      {"id": "s1", "agent_id": "researcher", "task": "分析: {{question}}"}
    ],
    "inputs": [{"name": "question", "required": true}]
  }'
```

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                     Tavern Server                           │
│  axum HTTP / SSE / Prometheus / Auth / Rate Limiter / Cron │
│                                                             │
│  YAML Path                      Rust DSL Path               │
│  ┌──────────────────┐          ┌────────────────────────┐  │
│  │ configs/workflows │          │ #[derive(Flow)]         │  │
│  │   .yml            │          │ #[flow_impl]            │  │
│  └────────┬─────────┘          │   #[start] fn research  │  │
│           │                    │   #[listen("research")] │  │
│           ▼                    │   #[router("write")]    │  │
│  ┌─────────────────────────────┴────────────────────────┐  │
│  │              tavern-comp (唯一引擎)                    │  │
│  │  WorkflowEngine — 事件溯源状态机                       │  │
│  │  DAG (AND/OR) · Router · 信号 · 审批 · 重试 · 回放    │  │
│  │  EventStore: SQLite / PostgreSQL / Memory              │  │
│  └──────────────────────────┬───────────────────────────┘  │
│                             │                               │
│  ┌──────────────┐  ┌────────┴────────┐                     │
│  │ tavern-hero   │  │ tavern-adapters │                     │
│  │ Agent 注册表  │  │ Pandaria / Mock │                     │
│  └──────────────┘  └─────────────────┘                     │
└────────────────────────────────────────────────────────────┘
```

## Project Structure

```
tavern/
├── configs/
│   ├── agents/              # Agent YAML definitions
│   └── workflows/           # Workflow YAML definitions
├── crates/
│   ├── tavern-core/         # Shared types, Runtime trait
│   ├── tavern-adapters/     # Pandaria HTTP adapter + Mock
│   ├── tavern-hero/         # Agent registry, YAML loader, CRUD
│   ├── tavern-comp/         # Event-sourced workflow engine (唯一引擎)
│   ├── tavern-flow-macros/  # Proc-macro DSL (#[start], #[listen], #[router])
│   ├── tavern-flow/         # DSL re-exports + FlowError type
│   ├── tavern-config/       # Unified config (figment: TOML + env)
│   └── tavern-server/       # axum HTTP server
├── docker-compose.yml       # SQLite (default) or PostgreSQL profile
├── Dockerfile
└── docs/
    ├── specs/               # Technical specifications
    └── plans/               # Development plans
```

## Feature Flags

| Feature | Crate | Description |
|---------|-------|-------------|
| `sqlite` (default) | `tavern-comp` | SQLite EventStore (system libsqlite3) |
| `postgres` | `tavern-comp` | PostgreSQL EventStore |
| `bundled-sqlite` | `tavern-comp` | Use bundled SQLite v3.45+ (no system dep) |

```bash
# PostgreSQL mode
cargo build --features postgres

# Bundled SQLite mode (self-contained binary)
cargo build --features bundled-sqlite
```
