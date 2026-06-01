
# Tavern

Fast and Flexible Multi-Agent Automation Framework — event-sourced orchestration with pluggable agent runtimes.

- **Tavern Hero**: Agent lifecycle management — YAML-defined agents, runtime CRUD, validation
- **Tavern Comp**: Event-sourced workflow engine — DAG scheduling, parallel execution, retries, signals, approval gates, breakpoints
- **Tavern Flow**: Method-level event-driven orchestration — `#[start]` `#[listen]` `#[router]` macros, parallel execution
- **Tavern Server**: REST API, SSE event streams, Prometheus metrics, auth, rate limiting

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
RUNTIME_URL=http://localhost:8080 cargo run -p tavern-server
```

Listens on `0.0.0.0:3000`. Agent configs load from `./configs/agents`, workflow configs from `./configs/workflows`.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `RUNTIME_URL` | Yes | — | Pandaria Runtime HTTP endpoint |
| `AGENT_CONFIG_DIR` | No | `./configs/agents` | Agent YAML config directory |
| `WORKFLOW_CONFIG_DIR` | No | `./configs/workflows` | Workflow YAML config directory |
| `SERVER_HOST` | No | `0.0.0.0` | Server bind address |
| `SERVER_PORT` | No | `3000` | Server port |
| `MAX_WORKFLOW_CONCURRENCY` | No | unlimited | Max parallel steps per workflow |
| `RUST_LOG` | No | `info` | Log level (`debug`, `info`, `warn`, `error`) |

## API Endpoints

### Agent

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/agents` | List all agents (summary) |
| `GET` | `/agents/:id` | Get agent config |
| `POST` | `/agents/:id/execute` | Execute agent task |

### Workflow

| Method | Path | Description |
|---|---|---|
| `GET` | `/workflows` | List all workflows |
| `POST` | `/workflows` | Register a workflow |
| `GET` | `/workflows/:id` | Get workflow definition |
| `DELETE` | `/workflows/:id` | Remove a workflow |
| `POST` | `/workflows/:id/run` | Run workflow (sync, returns result) |
| `POST` | `/workflows/:id/start` | Start workflow (async, returns execution ID) |
| `POST` | `/workflows/reload` | Hot-reload workflows from config dir |

### Execution

| Method | Path | Description |
|---|---|---|
| `GET` | `/executions/:id` | Query execution state |
| `GET` | `/executions/:id/events` | Get execution event log |
| `POST` | `/executions/:id/signal` | Send signal to waiting execution |
| `POST` | `/executions/:id/cancel` | Cancel a running execution |

### Metrics

| Method | Path | Description |
|---|---|---|
| `GET` | `/metrics` | Prometheus-format metrics |

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
  -d '{"topic": "2025 年 Rust 生态发展趋势"}'
```

Response:

```json
{
  "context": {
    "topic": "2025 年 Rust 生态发展趋势",
    "research_notes": "...",
    "draft": "...",
    "final_article": "..."
  },
  "outputs": {
    "final_article": "..."
  },
  "step_results": {
    "research": { "status": "Completed", ... },
    "write": { "status": "Completed", ... },
    "edit": { "status": "Completed", ... }
  }
}
```

### Start Workflow (Async) + Query Execution

```bash
# Start (returns execution ID immediately)
curl -X POST http://localhost:3000/workflows/content_pipeline/start \
  -H "Content-Type: application/json" \
  -d '{"topic": "异步执行示例"}'
# -> { "execution_id": "550e8400-e29b-41d4-a716-446655440000" }

# Query execution state
curl http://localhost:3000/executions/550e8400-e29b-41d4-a716-446655440000
# -> { "status": "running", "step_results": { ... }, "started_at": "..." }

# Cancel if needed
curl -X POST http://localhost:3000/executions/550e8400-e29b-41d4-a716-446655440000/cancel
```

### Workflow Configuration Example

```yaml
id: content_pipeline
name: 内容生产流水线
description: 研究 -> 写作 -> 编辑的协作流程

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

inputs:
  - name: topic
    required: true

outputs:
  - name: final_article
    value: "{{final_article}}"
```

Templates use `{{key}}` syntax (minijinja). Context variables flow from inputs → step outputs → outputs, with DAG-driven scheduling. Steps can specify `retries`, `retry_delay`, `wait_for_signal`, and `signal_timeout`.

## Project Structure

```
tavern/
├── configs/
│   ├── agents/          # Agent YAML definitions
│   └── workflows/       # Workflow YAML definitions
├── crates/
│   ├── tavern-core/     # Shared types, Runtime trait
│   ├── tavern-adapters/ # Pandaria HTTP adapter + mock
│   ├── tavern-hero/     # Agent registry, YAML loader
│   ├── tavern-comp/     # Event-sourced workflow engine
│   └── tavern-server/   # axum HTTP server
└── docs/
    ├── specs/           # Technical specifications
    └── plans/           # Development plans
```
