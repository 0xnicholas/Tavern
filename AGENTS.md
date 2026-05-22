# Agent Instructions

## Project Structure

```
tavern/
├── Cargo.toml              # Workspace root
├── configs/
│   ├── agents/             # Agent YAML configs
│   └── workflows/          # Workflow YAML configs (reserved for Comp)
├── crates/
│   ├── tavern-core/        # Shared types, Runtime trait
│   ├── tavern-adapters/    # Pandaria HTTP adapter + Mock adapter
│   ├── tavern-hero/        # Agent registry, YAML loader, task dispatch
│   └── tavern-server/      # HTTP server (axum), process assembly
└── docs/
    ├── plans/              # Development plans
    └── specs/              # Technical specifications
```

## Build & Test

```bash
# Check all crates
cargo check --workspace

# Run all tests
cargo test --workspace

# Run clippy
cargo clippy --workspace

# Format code
cargo fmt
```

## Run Server

```bash
# Development (with mock runtime or local Pandaria)
RUNTIME_URL=http://localhost:8080 RUST_LOG=debug cargo run -p tavern-server

# Production
RUNTIME_URL=https://pandaria.example.com cargo run --release -p tavern-server
```

## Adding a New Agent

Create a YAML file in `configs/agents/`:

```yaml
id: my_agent
name: My Agent
model:
  provider: openai
  name: gpt-4o
instructions: |
  Your system prompt here.
```

Restart server to load new configs (V0.1.0 does not support hot-reload).
