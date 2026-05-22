
# Tavern
Fast and Flexible Multi-Agent Automation Framework

- Tavern Hero: 创建和管理 Agent
- Tavern Comp: Multi-Agent 协作编排

Agent Runtime 消费使用 [Pandaria](https://github.com/0xnicholas/pandaria)

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
# 需要设置 RUNTIME_URL（Pandaria Runtime 地址）
RUNTIME_URL=http://localhost:8080 cargo run -p tavern-server
```

默认监听 `0.0.0.0:3000`，Agent 配置从 `./configs/agents` 加载。

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUNTIME_URL` | Yes | - | Pandaria Runtime HTTP endpoint |
| `AGENT_CONFIG_DIR` | No | `./configs/agents` | Agent YAML config directory |
| `SERVER_HOST` | No | `0.0.0.0` | Server bind address |
| `SERVER_PORT` | No | `3000` | Server port |
| `RUST_LOG` | No | `info` | Log level (e.g. `debug`, `info`, `warn`) |

### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/agents` | List all agents (summary) |
| GET | `/agents/:id` | Get agent config |
| POST | `/agents/:id/execute` | Execute agent task |

### Example: Execute Agent

```bash
curl -X POST http://localhost:3000/agents/researcher/execute \
  -H "Content-Type: application/json" \
  -d '{"task": "调查 2024 年 AI Agent 框架发展现状"}'
```

## References

项目与 [CrewAI](https://github.com/crewAIInc/crewAI) 概念接近，但比 CrewAI 更轻量级，因为 Agent Runtime 是独立的，不依赖于 Tavern 的特定实现。
