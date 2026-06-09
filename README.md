# Tavern

快速灵活的多智能体自动化框架 —— 事件溯源工作流编排，可插拔的智能体运行时。

**一套引擎，两种工作流定义方式：** 生产运维用 YAML，代码优先管线用 Rust 过程宏。

- **Tavern Comp**：事件溯源工作流引擎 —— DAG 调度（AND/OR 依赖）、Router 条件分支、并行执行、重试、信号量、审批关卡、断点调试、定时调度、批量执行、执行回放、Webhook 回调
- **Tavern Hero**：智能体生命周期管理 —— YAML 定义智能体、运行时 CRUD、校验、热重载、按步骤覆盖模型
- **Tavern Flow**：Rust 原生 DSL —— `#[start]` `#[listen]` `#[router]` 过程宏，编译为 Comp 工作流，无需 YAML
- **Tavern Server**：REST API（30+ 端点）、SSE 实时事件流、Prometheus 指标、API Key / Bearer Token 鉴权、租户限流、启动时自动恢复中断实例

底层消费 [Pandaria](https://github.com/0xnicholas/pandaria) 作为智能体执行运行时（任意实现 `tavern_core::Runtime` trait 的后端均可接入）。

## 快速开始

### 构建

```bash
cargo build --workspace
```

### 测试

```bash
cargo test --workspace
cargo clippy --workspace
cargo fmt
```

### 启动服务

```bash
# 开发模式（默认：内存存储、无鉴权、热重载开启）
RUNTIME_URL=http://localhost:8080 cargo run -p tavern-server

# SQLite 持久化
RUNTIME_URL=http://localhost:8080 TAVERN_STORE__STORE_TYPE=sqlite \
  TAVERN_STORE__DATABASE_URL=sqlite://data/tavern.db \
  cargo run -p tavern-server

# PostgreSQL 持久化
RUNTIME_URL=http://localhost:8080 TAVERN_STORE__STORE_TYPE=postgres \
  TAVERN_STORE__DATABASE_URL=postgres://tavern:tavern@localhost:5432/tavern \
  cargo run -p tavern-server --features postgres

# 生产模式（JSON 日志、API Key 鉴权、限流）
RUNTIME_URL=https://pandaria.example.com \
  TAVERN_STORE__STORE_TYPE=sqlite \
  TAVERN_OBSERVABILITY__LOG_FORMAT=json \
  TAVERN_AUTH__AUTH_TYPE=api_key \
  TAVERN_AUTH__KEYS=sk-prod-abc123 \
  TAVERN_RATE_LIMIT__ENABLED=true \
  cargo run --release -p tavern-server
```

服务监听 `0.0.0.0:3000`。智能体配置从 `./configs/agents` 加载，工作流配置从 `./configs/workflows` 加载。两个目录均支持文件监听自动热重载（500ms 防抖）。

### Docker

```bash
docker compose up -d                       # SQLite 模式（默认）
docker compose --profile postgres up -d    # PostgreSQL 模式
```

## 环境变量

配置使用 [figment](https://crates.io/crates/figment)，`TAVERN_` 前缀 + `__` 分隔符映射到嵌套结构。同时兼容旧版扁平环境变量（`RUNTIME_URL` 等）。

| 变量 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `TAVERN_RUNTIME__URL` 或 `RUNTIME_URL` | 是 | — | Pandaria Runtime HTTP 端点 |
| `TAVERN_SERVER__HOST` | 否 | `0.0.0.0` | 服务绑定地址 |
| `TAVERN_SERVER__PORT` | 否 | `3000` | 服务端口 |
| `TAVERN_SERVER__AGENT_CONFIG_DIR` | 否 | `./configs/agents` | 智能体 YAML 配置目录 |
| `TAVERN_SERVER__WORKFLOW_CONFIG_DIR` | 否 | `./configs/workflows` | 工作流 YAML 配置目录 |
| `TAVERN_SERVER__MAX_WORKFLOW_CONCURRENCY` | 否 | 不限制 | 工作流步骤最大并发数 |
| `TAVERN_STORE__STORE_TYPE` | 否 | `memory` | 存储类型：`memory`、`sqlite`、`postgres` |
| `TAVERN_STORE__DATABASE_URL` | 否 | `sqlite://tavern.db` | 数据库连接字符串 |
| `TAVERN_AUTH__AUTH_TYPE` | 否 | `none` | 鉴权类型：`none`、`api_key`、`bearer` |
| `TAVERN_AUTH__KEYS` | 否 | — | 逗号分隔的 API Key 列表（`api_key` 模式） |
| `TAVERN_AUTH__JWT_SECRET` | 否 | `change-me` | JWT 密钥（`bearer` 模式） |
| `TAVERN_OBSERVABILITY__LOG_FORMAT` | 否 | `pretty` | 日志格式：`pretty` 或 `json` |
| `TAVERN_OBSERVABILITY__METRICS_PUBLIC` | 否 | `true` | 无需鉴权暴露 `/metrics` |
| `TAVERN_RATE_LIMIT__ENABLED` | 否 | `false` | 启用租户限流 |
| `TAVERN_RATE_LIMIT__DEFAULT_RPS` | 否 | `10` | 每租户默认每秒请求数 |
| `TAVERN_CONFIG_PATH` | 否 | `config.toml` | TOML 配置文件路径 |
| `TAVERN_PUBLIC_URL` | 否* | — | Tavern 对外可达地址（tool calling 必需） |
| `TAVERN_TOOL_SECRET` | 否* | — | 工具端点鉴权密钥（tool calling 必需） |
| `RUST_LOG` | 否 | `info` | 日志级别（`debug`、`info`、`warn`、`error`） |

## 定义智能体

每个智能体是 `configs/agents/` 下的 YAML 文件。也可以通过 REST API 在运行时注册。

```yaml
# configs/agents/researcher.yaml
id: researcher
name: 研究员
description: 信息收集与综合分析专家
model:
  provider: openai
  name: gpt-4o
  temperature: 0.3
instructions: |
  你是研究助理，负责全面收集并综合整理信息。
skills:
  - id: web_search
    name: web_search
    description: Search the web for information
    parameters:
      type: object
      properties:
        query:
          type: string
          description: The search query
      required: [query]
    timeout_ms: 30000
    config:
      max_results: 5
constraints:
  - 回答必须使用用户指定的语言
memory:
  enabled: true
  max_context_turns: 10
```

支持的模型提供商：任何兼容 OpenAI API 的服务（通过 Pandaria 透传）。

### Tool Calling（技能执行）

Tavern 支持真正的 tool calling：配置 `skills` 后，Pandaria 会将 LLM 的 ToolCall 请求转发回调到 Tavern，由 Tavern 执行实际工具逻辑。

启用条件（两者缺一不可）：
```bash
export TAVERN_PUBLIC_URL=https://tavern.example.com   # Pandaria 可达的 Tavern 地址
export TAVERN_TOOL_SECRET=your-shared-secret          # 端点鉴权
```

流程：`LLM → Pandaria HttpProxyTool → POST /api/tools/:name → Tavern ToolHandler.execute() → 返回结果`

Skill 完整定义参考上例。`name`/`description`/`parameters` 构成 LLM 可见的 function schema，`config` 为 Tavern handler 私有配置（LLM 不可见）。所有新增字段有默认值，旧版 YAML 不写照样作为纯文本注入工作。

## 定义工作流 —— YAML

工作流支持三种执行模式：**Sequential**（带 AND/OR 依赖的 DAG）、**Hierarchical**（Manager 智能体动态委派任务）、**Planning**（LLM 预先规划步骤执行顺序）。

### Sequential DAG（默认）

```yaml
# configs/workflows/content_pipeline.yaml
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
    task: "根据研究资料撰写文章: {{research_notes}}"
    depends_on: [research]
    output_key: draft
    retries: 2
    retry_delay: 5

  - id: edit
    agent_id: editor
    task: "编辑文章，改进语言和结构: {{draft}}"
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

webhook:
  url: https://hooks.example.com/tavern
  secret: whsec_abc123
  timeout_secs: 30
  retries: 3
```

### OR 依赖

当**任意一个**上游步骤完成时即触发：

```yaml
  - id: notify
    agent_id: notifier
    task: "至少一个数据源已就绪，发送通知"
    or_depends_on: [fetch_api, fetch_db]   # 谁先完成就触发谁
```

### Router（条件分支）

Router 步骤在运行时评估输出，产生标签供下游监听者匹配：

```yaml
  - id: review_router
    agent_id: reviewer
    task: "审核草稿 {{draft}} 并决定下一步"
    depends_on: [write]
    router:
      upstream: write
    # 输出: "approved" 或 "rejected"（或标签列表）

  - id: publish
    agent_id: publisher
    task: "发布: {{draft}}"
    or_depends_on: [__label__approved]

  - id: rewrite
    agent_id: writer
    task: "重写: {{draft}}"
    or_depends_on: [__label__rejected]
```

### Hierarchical 模式（Manager 智能体）

适用于执行顺序不可预测的任务 —— Manager 智能体动态委派：

```yaml
id: adaptive_team
name: 自适应研究团队
process: hierarchical

manager:
  agent_id: manager
  instructions: |
    你是项目经理。根据研究需求，将任务动态委派给最合适的专家。

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

### Planning（LLM 预规划）

启用 `planning` 后，LLM 会在执行前分析工作流并生成最优步骤依赖图：

```yaml
id: complex_research
name: 复杂研究流程
steps: [...]
planning:
  enabled: true
  planning_agent: "planner"  # 可选，默认使用第一个步骤的智能体
```

### 步骤参数全表

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `id` | `string` | 必填 | 步骤唯一标识（工作流内唯一） |
| `agent_id` | `string` | 必填 | 执行此步骤的智能体 ID |
| `task` | `string` | 必填 | 任务提示词，支持 `{{var}}` 插值 |
| `depends_on` | `[]string` | `[]` | AND 依赖：所有上游必须完成 |
| `or_depends_on` | `[]string` | `[]` | OR 依赖：任一上游完成即触发 |
| `output_key` | `string` | — | 输出存入上下文的键名（下游可用 `{{key}}` 引用） |
| `timeout` | `int` | `300` | 步骤超时（秒） |
| `retries` | `int` | `0` | 最大重试次数 |
| `retry_delay` | `int` | `0` | 重试间隔（秒） |
| `wait_for_signal` | `string` | — | 外部信号名称（审批关卡） |
| `signal_timeout` | `int` | — | 信号等待超时（秒） |
| `signal_timeout_action` | `string` | `fail` | 超时动作：`fail` 或 `reject` |
| `breakpoint` | `bool` | `false` | 执行前暂停（调试断点） |
| `model_override` | `object` | — | 覆盖智能体模型 `{provider, name}` |
| `expected_output` | `string` | — | 预期输出描述（Manager/Planning 提示用） |
| `router` | `object` | — | `{upstream: "step_id"}` —— 根据输出标签路由 |

### 工作流参数

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `process` | `string` | `sequential` | 执行模式：`sequential` 或 `hierarchical` |
| `planning` | `object` | — | `{enabled: bool, planning_agent?: string}` |
| `webhook` | `object` | — | `{url, secret?, timeout_secs?, retries?, retry_delay?}` |
| `schedule` | `string` | — | Cron 表达式，定时自动执行 |
| `schedule_inputs` | `object` | `null` | 定时触发时的默认输入 |

## 定义工作流 —— Rust DSL（Flow）

代码优先管线，使用 `tavern_flow` 过程宏 DSL。带注解的方法编译为 `tavern_comp::Workflow`，在同一引擎上执行 —— 无需 YAML。

```rust
use tavern_flow::{Flow, flow_impl, start, listen, router, FlowError};

#[derive(Flow)]
struct ContentTeam {
    state: TeamState,
}

#[flow_impl(crate = "tavern_flow")]
impl ContentTeam {
    #[start]
    async fn research(&mut self) -> Result<String, FlowError> {
        Ok("关于 AI Agent 的调研笔记".to_string())
    }

    #[listen("research")]
    async fn write(&mut self, notes: String) -> Result<String, FlowError> {
        let article = format!("基于笔记撰写: {}", notes);
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
        Ok(format!("已发布: {}", draft))
    }

    #[listen("rewrite")]
    async fn revise(&mut self, draft: String) -> Result<String, FlowError> {
        Ok(format!("已重写: {}", draft))
    }
}

// 一行执行（引擎自动处理 DAG 调度）
#[tokio::main]
async fn main() {
    let pipeline = ContentTeam { state: TeamState::default() };
    let result = pipeline.run(serde_json::json!({})).await.unwrap();
}
```

| 过程宏 | 展开为 |
|---|---|
| `#[start]` | 无依赖步骤（入口点） |
| `#[listen("method")]` | OR 依赖：method 输出或 router 标签 |
| `#[listen(or("a","b"))]` | OR 依赖：a 或 b 任一完成 |
| `#[listen(and("a","b"))]` | AND 依赖：a 和 b 都必须完成 |
| `#[router("upstream")]` | 路由步骤：输出标签驱动下游监听者 |
| `#[flow_impl]` | 生成 `FlowStepExecutor` + `Workflow` 定义 + `run()` 方法 |

过程宏编译时展开为 Comp 类型，在同一 `WorkflowEngine` 上执行。无独立运行时、无 YAML 文件、无运行时 YAML 解析 —— 零成本抽象。

## API 端点

### 智能体

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/agents` | 列出所有已注册智能体（摘要） |
| `GET` | `/agents/:id` | 获取完整智能体配置 |
| `POST` | `/agents` | 运行时注册智能体（JSON body） |
| `DELETE` | `/agents/:id` | 移除智能体 |
| `POST` | `/agents/:id/execute` | 直接执行智能体 |

### 工作流

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/workflows` | 列出所有工作流（摘要） |
| `POST` | `/workflows` | 注册工作流（JSON body） |
| `GET` | `/workflows/:id` | 获取完整工作流定义 |
| `DELETE` | `/workflows/:id` | 移除工作流 |
| `POST` | `/workflows/:id/run` | 同步执行工作流（返回结果） |
| `POST` | `/workflows/:id/start` | 异步启动工作流（返回 `execution_id`） |
| `POST` | `/workflows/:id/run_batch` | 批量并行执行（输入数组） |
| `POST` | `/workflows/reload` | 从 YAML 目录热重载配置 |

### 执行

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/executions/:id` | 查询执行状态与步骤结果 |
| `GET` | `/executions/:id/events` | 获取事件日志（完整审计轨迹） |
| `GET` | `/executions/:id/events/stream` | SSE 实时事件流 |
| `GET` | `/executions/:id/replay` | 执行回放（StateDiff + 时间线） |
| `POST` | `/executions/:id/clone` | 克隆历史执行，重新触发 |
| `POST` | `/executions/:id/signal` | 发送信号（审批/驳回/webhook） |
| `POST` | `/executions/:id/cancel` | 取消正在运行的工作流 |

### 审批

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/approvals` | 列出待审批步骤 |
| `POST` | `/executions/:id/steps/:step_id/approve` | 审批通过 |
| `POST` | `/executions/:id/steps/:step_id/reject` | 驳回步骤 |

### Flow（Rust DSL 编排）

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/flows` | 列出已注册的 Flow |
| `POST` | `/flows/:id/start` | 启动 Flow 执行 |
| `GET` | `/flows/:id/status` | 查询 Flow 执行状态 |
| `POST` | `/flows/:id/cancel` | 取消 Flow 执行 |

### 可观测性与系统

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康检查（含存储连通性） |
| `GET` | `/metrics` | Prometheus 指标 |
| `GET` | `/breakpoints` | 列出暂停的断点步骤 |
| `GET` | `/schedules` | 列出定时调度 |
| `POST` | `/auth/refresh` | 刷新 Token（Bearer 鉴权模式） |

## 示例

### 运行时注册并执行智能体

```bash
# 运行时注册
curl -X POST http://localhost:3000/agents \
  -H "Content-Type: application/json" \
  -d '{
    "id": "translator",
    "name": "翻译助手",
    "model": {"provider": "openai", "name": "gpt-4o"},
    "instructions": "准确翻译文本"
  }'

# 直接执行
curl -X POST http://localhost:3000/agents/translator/execute \
  -H "Content-Type: application/json" \
  -d '{"task": "将以下内容翻译为日语: 你好，最近过得怎么样？"}'

# 移除
curl -X DELETE http://localhost:3000/agents/translator
```

### 同步执行工作流

```bash
curl -X POST http://localhost:3000/workflows/content_pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"topic": "Rust 生态 2026 年发展趋势"}'
```

响应：

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

### 异步启动 + SSE 事件流

```bash
# 异步启动
curl -X POST http://localhost:3000/workflows/content_pipeline/start \
  -H "Content-Type: application/json" \
  -d '{"topic": "异步执行示例"}'
# -> { "execution_id": "550e8400-e29b-41d4-a716-446655440000" }

# 实时事件流
curl -N http://localhost:3000/executions/550e8400.../events/stream

# 轮询状态
curl http://localhost:3000/executions/550e8400-e29b-41d4-a716-446655440000

# 获取事件日志
curl http://localhost:3000/executions/550e8400.../events
```

### 审批工作流

```bash
# 列出待审批
curl http://localhost:3000/approvals

# 审批通过
curl -X POST http://localhost:3000/executions/<id>/steps/edit/approve \
  -H "Content-Type: application/json" \
  -d '{"reviewer": "alice", "comments": "文章质量不错"}'

# 驳回
curl -X POST http://localhost:3000/executions/<id>/steps/edit/reject \
  -H "Content-Type: application/json" \
  -d '{"reviewer": "alice", "reason": "需要更多数据支持"}'
```

### 批量执行

```bash
curl -X POST http://localhost:3000/workflows/content_pipeline/run_batch \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {"topic": "AI 智能体"},
      {"topic": "Rust Web 框架"},
      {"topic": "数据库发展趋势"}
    ]
  }'
```

### 执行回放与克隆

```bash
curl http://localhost:3000/executions/<id>/replay
# 返回状态差异 + 时间线 —— 精确定位每一步的变化

curl -X POST http://localhost:3000/executions/<id>/clone
# 使用相同输入重新执行
```

### Cron 定时工作流

```yaml
# configs/workflows/daily_report.yaml
id: daily_report
name: 每日摘要
schedule: "0 9 * * *"   # 每天 09:00 执行
schedule_inputs:
  date_range: "last_24h"
steps:
  - id: summarize
    agent_id: analyst
    task: "总结 {{date_range}} 期间的活动"
```

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│                     Tavern Server                             │
│  axum HTTP / SSE / Prometheus / Auth / Rate Limiter / Cron   │
│  热重载 (notify) · 断点恢复 · Webhook 回调                    │
│                                                               │
│  ┌── YAML 工作流 ──────────────────┐  ┌── Rust DSL ──────┐  │
│  │ configs/agents/*.yaml           │  │ #[derive(Flow)]   │  │
│  │ configs/workflows/*.yaml        │  │ #[flow_impl]      │  │
│  │ tavern-hero (智能体注册表)       │  │ tavern-flow-macros│  │
│  └──────────┬──────────────────────┘  │   #[start] fn ... │  │
│             │                         │   #[listen("x")]  │  │
│             ▼                         │   #[router("y")]  │  │
│  ┌────────────────────────────────────┴────────────────────┐  │
│  │              tavern-comp（唯一引擎）                      │  │
│  │  WorkflowEngine — 事件溯源状态机                          │  │
│  │  DAG (AND/OR) · Router · 信号 · 审批 · 重试              │  │
│  │  回放 · 克隆 · Planning · Hierarchical (Manager)         │  │
│  │  EventStore: Memory / SQLite / PostgreSQL                 │  │
│  └──────────────────────────┬──────────────────────────────┘  │
│                             │                                  │
│  ┌────────────┐  ┌──────────┴──────────┐                      │
│  │ tavern-hero │  │ tavern-adapters      │                     │
│  │ 智能体 CRUD │  │ PandariaHTTP / Mock  │                     │
│  └────────────┘  └─────────────────────┘                      │
└──────────────────────────────────────────────────────────────┘
```

## 项目结构

```
tavern/
├── Cargo.toml                  # 工作区根（8 个 crate）
├── configs/
│   ├── agents/                 # 智能体 YAML 定义
│   └── workflows/              # 工作流 YAML 定义
├── crates/
│   ├── tavern-core/            # 共享类型、Runtime trait、AgentConfig
│   ├── tavern-adapters/        # Pandaria HTTP 适配器 + Mock 适配器
│   ├── tavern-hero/            # 智能体注册、YAML 加载、任务分发
│   ├── tavern-comp/            # 事件溯源工作流引擎（核心）
│   ├── tavern-config/          # 统一配置（figment: TOML + env）
│   ├── tavern-flow-macros/     # 过程宏 DSL（#[start], #[listen], #[router]）
│   ├── tavern-flow/            # DSL 重导出 + FlowError 类型
│   └── tavern-server/          # axum HTTP 服务器
├── docker-compose.yml
├── Dockerfile
└── docs/
    ├── specs/                  # 技术规格
    └── plans/                  # 开发计划
```

## Feature Flags

| Feature | Crate | 说明 |
|---|---|---|
| `sqlite`（默认） | `tavern-comp` | SQLite EventStore（系统 libsqlite3） |
| `postgres` | `tavern-comp` | PostgreSQL EventStore |
| `bundled-sqlite` | `tavern-comp` | 内置 SQLite v3.45+（无需系统依赖） |

```bash
# PostgreSQL 模式
cargo build --workspace --features postgres

# 内置 SQLite（完全自包含的二进制文件）
cargo build --workspace --features bundled-sqlite
```

### 运行时选择存储

`TAVERN_STORE__STORE_TYPE` 环境变量在启动时选择存储后端：

| 值 | 后端 | 需要 Feature | 适用场景 |
|---|---|---|---|
| `memory`（默认） | 内存 | 无 | 开发、测试 |
| `sqlite` | SQLite 文件 | `sqlite`（默认） | 单节点生产 |
| `postgres` | PostgreSQL | `postgres` | 多节点、高可用部署 |

## 关键行为

- **事件溯源**：每次状态变更都记录为 `WorkflowEvent`。事件日志是唯一事实来源 —— 恢复时从事件重建内存状态。
- **断点恢复**：进程崩溃重启后，自动恢复 `Running` 和 `WaitingForSignal` 状态的实例。
- **热重载**：智能体和工作流的 YAML 目录均通过 `notify` 监听。文件变更自动触发重载（500ms 防抖）。可通过 `TAVERN_RELOAD__ENABLED=false` 关闭。
- **Webhook 回调**：工作流完成或失败时触发。支持 HMAC-SHA256 签名，可配置超时和重试次数。
- **SSE 流**：每个执行实例的实时事件推送（Server-Sent Events）。支持通过查询参数传递 API Key 鉴权。
