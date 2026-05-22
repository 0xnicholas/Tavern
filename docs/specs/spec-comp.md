# Tavern Comp 技术规格说明书

## 1. 概述

Tavern Comp 是 Tavern 框架的多 Agent 协作编排子系统，负责：
- 从 YAML 配置文件加载 Workflow（工作流）定义
- 解析步骤间的依赖关系，构建执行 DAG
- 按拓扑顺序调度 Agent 执行任务
- 在步骤间传递上下文数据（通过模板注入）
- 通过 Tavern Hero 查询 Agent 定义并提交任务

Tavern Comp **不直接依赖** Runtime 适配器。任务执行通过注入的 `TavernHero` 完成，Hero 再向 Runtime 转发。

### 1.1 设计原则
- **声明式编排**：Workflow 完全由 YAML 定义，描述"什么 Agent 按什么顺序做什么"
- **上下文流转**：前序步骤的输出自动注入后序步骤的任务模板
- **依赖驱动**：通过 `depends_on` 声明步骤依赖，引擎自动拓扑排序
- **Hero 复用**：Comp 只负责编排逻辑，Agent 管理和 Runtime 调用复用 Hero

---

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| Workflow | 多 Agent 协作流程的完整定义 |
| Step | 工作流中的一个执行步骤，对应一个 Agent 的一次任务调用 |
| DAG | 有向无环图，由步骤和依赖关系构成 |
| Context | 工作流执行期间的共享数据池，存储步骤输出和外部输入 |
| Input | 工作流的外部输入参数 |
| Output | 工作流的最终输出，通常是最末步骤的结果 |
| Template | 任务描述中的变量插值语法，如 `{{var}}` |

---

## 3. 架构

```
┌─────────────────┐     HTTP/REST    ┌──────────────────┐
│   Application   │ ◄──────────────► │   Tavern Server  │
└─────────────────┘                  └────────┬─────────┘
                                              │
                                       ┌──────▼──────┐
                                       │ Tavern Comp │
                                       │             │
                                       │  Workflow   │
                                       │  Engine     │
                                       └──────┬──────┘
                                              │ Arc<TavernHero>
                                       ┌──────▼──────┐
                                       │ Tavern Hero │
                                       │  Registry   │
                                       │  Loader     │
                                       └──────┬──────┘
                                              │ Arc<dyn Runtime>
                                       ┌──────▼──────┐
                                       │   tavern-   │
                                       │  adapters   │
                                       └─────────────┘
```

### 3.1 Crate 职责

| Crate | 与 Comp 的关系 |
|-------|---------------|
| `tavern-core` | Comp 依赖 core 的 `RuntimeError` 等共享类型 |
| `tavern-hero` | Comp **直接依赖** Hero，通过 Hero 查询 Agent 并执行 |
| `tavern-comp` | Workflow 定义、DAG 构建、拓扑执行、模板渲染 |
| `tavern-server` | 组装 Hero + Comp，暴露 Workflow HTTP API |

---

## 4. 数据模型

### 4.1 Workflow

工作流的完整配置定义。

```rust
pub struct Workflow {
    /// 全局唯一标识符
    /// 约束：^[a-zA-Z0-9_-]+$，长度 1-64
    pub id: String,
    
    /// 可读名称
    pub name: String,
    
    /// 描述（可选）
    pub description: Option<String>,
    
    /// 执行步骤列表
    pub steps: Vec<Step>,
    
    /// 外部输入参数定义
    /// 默认：空列表
    pub inputs: Vec<InputDef>,
    
    /// 工作流最终输出定义
    /// 默认：空列表（REST 响应中 outputs 字段为空对象 {}）
    pub outputs: Vec<OutputDef>,
}
```

### 4.2 Step

```rust
pub struct Step {
    /// 步骤唯一标识（workflow 内唯一）
    pub id: String,
    
    /// 使用的 Agent ID（引用 Hero 注册表中的 Agent）
    pub agent_id: String,
    
    /// 任务描述模板，支持 {{var}} 插值
    pub task: String,
    
    /// 依赖的步骤 ID 列表
    /// 默认：空列表（表示可立即执行）
    pub depends_on: Vec<String>,
    
    /// 输出存储到 Context 的键名
    /// 默认：步骤不保存输出到 Context
    pub output_key: Option<String>,
    
    /// 步骤执行超时时间（秒）
    /// 反序列化缺失时为 None，执行引擎 fallback 到 300
    pub timeout: Option<u64>,
}
```

### 4.3 InputDef

```rust
pub struct InputDef {
    /// 参数名称
    pub name: String,
    
    /// 是否必填
    /// 默认：true
    pub required: bool,
    
    /// 默认值（支持任意 JSON 类型）
    pub default: Option<serde_json::Value>,
}
```

### 4.4 WorkflowResult

```rust
pub struct WorkflowResult {
    /// 最终上下文（包含所有输入和步骤输出）
    pub context: serde_json::Value,
    
    /// 每个步骤的详细执行结果
    pub step_results: HashMap<String, StepResult>,
}

pub struct StepResult {
    pub status: StepStatus,
    /// 步骤成功执行时的返回值
    /// Failed 状态下为 None
    pub output: Option<serde_json::Value>,
    /// 步骤失败时的错误信息
    /// Completed 状态下为 None
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

pub enum StepStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

### 4.5 OutputDef

工作流最终输出的定义。

```rust
pub struct OutputDef {
    /// 输出字段名称
    pub name: String,
    
    /// 输出值模板，支持 {{var}} 插值
    /// 例如："{{final_article}}"
    pub value: String,
}
```
```

---

## 5. 接口定义

### 5.1 WorkflowEngine（tavern-comp）

```rust
use std::sync::Arc;
use tavern_hero::TavernHero;

pub struct WorkflowEngine {
    hero: Arc<TavernHero>,
}

impl WorkflowEngine {
    /// 初始化，注入 TavernHero
    pub fn new(hero: Arc<TavernHero>) -> Self;
    
    /// 验证 Workflow 的合法性（含动态检查）
    /// 
    /// 内部调用 `Workflow::validate_static()` 做静态校验：
    /// - 检查步骤 ID 唯一性
    /// - 检查依赖的步骤是否存在
    /// - 检查依赖图是否有环
    /// - 检查 output_key 唯一性（若不为 null）
    /// 
    /// 再额外检查：引用的 Agent 是否在 Hero 注册表中
    pub fn validate(&self, workflow: &Workflow) -> Result<(), CompError>;
    
    /// 执行 Workflow
    /// 
    /// 执行流程：
    /// 1. 构建 DAG，拓扑排序
    /// 2. 按拓扑顺序执行步骤
    /// 3. 每个步骤：渲染模板 → 调用 Hero.execute → 保存输出到 Context
    /// 4. 全部步骤成功后，渲染 Workflow `outputs` 模板（若定义了 outputs）
    /// 5. 任一步骤失败，整个 Workflow 失败
    /// 
    /// outputs 渲染失败（如引用不存在的变量）时，返回 `MissingContextVariable` 错误
    pub async fn run(
        &self,
        workflow: &Workflow,
        inputs: serde_json::Value,
    ) -> Result<WorkflowResult, CompError>;
}
```

---

## 6. Library API（tavern-comp）

### 6.1 Workflow 加载

```rust
impl Workflow {
    /// 从 YAML 文件加载
    pub fn from_yaml(path: &Path) -> Result<Self, CompError>;
    
    /// 从 YAML 字符串加载
    pub fn from_yaml_str(content: &str) -> Result<Self, CompError>;
    
    /// 静态校验（不依赖 Hero）
    /// 检查：ID 唯一性、依赖存在性、DAG 无环、output_key 唯一性
    pub fn validate_static(&self) -> Result<(), CompError>;
}
```

### 6.2 WorkflowRegistry

内存中的 Workflow 注册表，负责加载、存储和查询 Workflow 定义。

```rust
pub struct WorkflowRegistry {
    // HashMap<String, Workflow>
}

impl WorkflowRegistry {
    /// 创建空注册表
    pub fn new() -> Self;
    
    /// 注册 Workflow
    /// 错误：id 已存在时返回 CompError::DuplicateWorkflow
    pub fn register(&mut self, workflow: Workflow) -> Result<(), CompError>;
    
    /// 查询 Workflow
    pub fn get(&self, id: &str) -> Option<&Workflow>;
    
    /// 列出全部 Workflow（摘要信息）
    pub fn list_all(&self) -> Vec<WorkflowSummary>;
}

/// Workflow 摘要信息，用于列表接口
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowSummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}

impl WorkflowRegistry {
    /// 从目录批量加载 YAML 配置
    /// 遍历目录下所有 .yaml / .yml 文件
    /// 
    /// 故障语义：遇到首个错误即终止，此前已加载的 Workflow 保留在注册表中（不回滚）。
    /// 调用方如需原子性，应先单独验证所有文件再调用加载。
    pub fn load_from_dir(&mut self, dir: &Path) -> Result<(), CompError>;
}
```

### 6.3 WorkflowEngine

见 5.1 节。

---

## 7. REST API（tavern-server）

Base URL: `http://{host}:{port}`

Content-Type: `application/json`

### 7.1 列出所有 Workflow

```
GET /workflows
```

**响应 200:**
```json
[
  {
    "id": "content_pipeline",
    "name": "内容生产流水线",
    "description": "研究 -> 写作 -> 编辑"
  }
]
```

### 7.2 获取单个 Workflow

```
GET /workflows/:id
```

**响应 200:** 完整 Workflow JSON

**响应 404:**
```json
{
  "error": "WorkflowNotFound",
  "message": "Workflow 'xxx' not found"
}
```

### 7.3 执行 Workflow

```
POST /workflows/:id/run
```

**请求体:**
```json
{
  "topic": "2024 年 AI Agent 框架发展现状"
}
```

请求体中的字段对应 Workflow 的 `inputs` 定义。

**响应 200:**
```json
{
  "context": {
    "topic": "2024 年 AI Agent 框架发展现状",
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

**响应 404:** Workflow 不存在

**响应 400:** 输入参数缺失或格式错误

**响应 404:** Agent 未找到（引用的 Agent ID 在 Hero 注册表中不存在）

**响应 500:** 工作流定义错误（如循环依赖、重复 output_key、模板变量缺失、outputs 渲染失败）

**响应 502:** 某个步骤的 Agent 执行失败（Runtime 错误）

---

## 8. 错误定义

### 8.1 CompError（tavern-comp）

```rust
#[derive(Debug, thiserror::Error)]
pub enum CompError {
    #[error("workflow '{id}' not found")]
    WorkflowNotFound { id: String },
    
    #[error("workflow '{id}' already registered")]
    DuplicateWorkflow { id: String },
    
    #[error("step '{id}' not found in workflow")]
    StepNotFound { id: String },
    
    #[error("duplicate step id '{id}' in workflow")]
    DuplicateStep { id: String },
    
    #[error("cyclic dependency detected in workflow")]
    CyclicDependency,
    
    #[error("agent '{id}' not found in registry")]
    AgentNotFound { id: String },
    
    #[error("duplicate output key '{key}' in workflow")]
    DuplicateOutputKey { key: String },
    
    #[error("missing context variable: {name}")]
    MissingContextVariable { name: String },
    
    #[error("step '{step_id}' failed: {reason}")]
    StepFailed { step_id: String, reason: String },
    
    #[error("missing required input: {name}")]
    MissingInput { name: String },
    
    #[error("config parse failed at {path}: {reason}")]
    ConfigParse { path: String, reason: String },
    
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("hero error: {0}")]
    Hero(#[from] tavern_hero::TavernError),
}
```

### 8.2 HTTP 状态码映射

| CompError | HTTP Status |
|-----------|-------------|
| WorkflowNotFound | 404 Not Found |
| DuplicateWorkflow | 409 Conflict |
| StepNotFound | 500 Internal Server Error |
| DuplicateStep | 500 Internal Server Error |
| CyclicDependency | 500 Internal Server Error |
| DuplicateOutputKey | 500 Internal Server Error |
| AgentNotFound | 404 Not Found |
| StepFailed | 502 Bad Gateway |
| MissingInput | 400 Bad Request |
| MissingContextVariable | 500 Internal Server Error |
| ConfigParse | 500 Internal Server Error |
| Io | 500 Internal Server Error |
| Hero(TavernError) | 按 TavernError 的映射规则 |

---

## 9. YAML 配置格式规范

### 9.1 完整示例

```yaml
id: content_pipeline
name: 内容生产流水线
description: 研究 -> 写作 -> 编辑的协作流程

steps:
  - id: research
    agent_id: researcher
    task: "研究以下主题并整理关键信息: {{topic}}"
    output_key: research_notes

  - id: write
    agent_id: writer
    task: "根据以下研究资料撰写文章: {{research_notes}}"
    depends_on: [research]
    output_key: draft

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

### 9.2 字段约束

| 字段 | 类型 | 必需 | 默认 | 约束 |
|------|------|------|------|------|
| id | string | 是 | - | `^[a-zA-Z0-9_-]+$`, 1-64 字符 |
| name | string | 是 | - | 非空 |
| description | string | 否 | null | - |
| steps | array | 是 | - | 至少 1 个元素 |
| steps[].id | string | 是 | - | workflow 内唯一 |
| steps[].agent_id | string | 是 | - | 引用 Hero 注册表中的 Agent |
| steps[].task | string | 是 | - | 支持 `{{var}}` 模板插值 |
| steps[].depends_on | array | 否 | [] | 元素为已定义的 step id |
| steps[].output_key | string | 否 | null | 非空，workflow 内唯一 |
| steps[].timeout | integer | 否 | 300 | ≥ 1，单位：秒 |
| inputs | array | 否 | [] | - |
| inputs[].name | string | 是 | - | 非空 |
| inputs[].required | bool | 否 | true | - |
| inputs[].default | any | 否 | null | 任意 JSON 值 |
| outputs | array | 否 | [] | - |
| outputs[].name | string | 是 | - | 非空 |
| outputs[].value | string | 是 | - | 支持 `{{var}}` 模板插值 |

---

## 10. 模板渲染规则

### 10.1 语法

采用简单的 `{{key}}` 插值语法。

```rust
"研究主题: {{topic}}"
// 渲染后: "研究主题: AI Agent 框架"
```

### 10.2 上下文查找顺序

渲染 `task` 模板时，按以下顺序查找变量：

1. **外部输入**（`inputs`）：调用方传入的参数
2. **前序步骤输出**（`output_key`）：已执行步骤保存到 Context 的值
3. **找不到时**：返回 `CompError::MissingContextVariable` 错误

> 若 input 与 output_key 同名，外部输入优先。但建议避免命名冲突。

### 10.3 实现

使用简单的字符串替换，不引入完整模板引擎：

```rust
fn render_template(template: &str, context: &Value) -> Result<String, CompError> {
    // 查找 {{key}} 模式，从 context 中替换
}
```

> **版本演进**：V0.1.0 仅支持简单的 `{{key}}` 字符串替换，不支持嵌套对象访问（如 `{{research.notes}}`）或默认值语法。未来版本将评估引入 minijinja 等轻量模板引擎。

### 10.4 Context 保存策略

步骤执行结果的流向规则：

- **声明了 `output_key` 的步骤**：`hero.execute()` 的返回值写入 Context，键名为 `output_key` 的值。后续步骤的模板可通过 `{{output_key}}` 引用。
- **未声明 `output_key` 的步骤**：返回值**不进入**全局 Context，仅保存在 `StepResult.output` 中供结果追溯。后续步骤无法通过模板引用该输出。
- **结论**：若后续步骤需要引用某步骤的输出，该步骤必须显式声明 `output_key`。

---

## 11. 执行引擎行为

### 11.1 拓扑排序

1. 解析所有 `depends_on` 关系，构建邻接表
2. 使用 Kahn 算法进行拓扑排序
3. 发现环时返回 `CyclicDependency` 错误

### 11.2 执行策略

**初始版本（V0.1.0）：串行执行**

按拓扑排序后的顺序，依次执行每个步骤：
```
research → write → edit
```

**未来扩展：并行执行**

同一层级（入度同时为 0）的步骤可并行执行：
```
research → [write_a, write_b] → edit
```

### 11.3 失败处理

- 任一步骤失败，整个 Workflow 立即失败
- 已完成的步骤结果保留在 `WorkflowResult.step_results` 中
- 返回 `StepFailed` 错误，包含失败的步骤 ID 和原因

### 11.4 执行控制

**超时**

- 每个 Step 有独立的执行超时时间
- 默认：300 秒
- 可通过 `step.timeout` 字段覆盖（单位：秒，最小值 1）
- 超时后步骤状态为 `Failed`，错误信息包含超时提示

**重试**

- V0.1.0 **不实现**重试机制，默认 0 次重试
- 未来版本将通过 `step.retries` 字段支持有限次重试

**并发限制**

- V0.1.0 为串行执行，不涉及并发
- 并行执行扩展时，`WorkflowEngine` 将通过构造参数限制最大并发数，防止下游 Runtime 过载

> **超时层叠说明**：Comp 的 `step.timeout`（默认 300s）是编排层等待 Hero 返回的超时。Hero 通过 adapters 调用 Runtime 时，adapters 内部有独立的 HTTP 传输超时（通常更短，如 30s）。若任务实际执行时间超过 adapters 的超时，Comp 会收到 `RuntimeError::RequestFailed`，此时 Comp 的超时不会生效。两个超时值需在部署时协调。

---

## 12. 与 Hero 的集成

### 12.1 依赖关系

```toml
# crates/tavern-comp/Cargo.toml
[dependencies]
tavern-core = { path = "../tavern-core" }
tavern-hero = { path = "../tavern-hero" }
serde = { version = "1.0", features = ["derive"] }
serde_yaml = "0.9"
serde_json = "1.0"
thiserror = "1.0"
tokio = { version = "1.0", features = ["full"] }
```

### 12.2 调用链

```
WorkflowEngine::run()
    → 拓扑排序
    → 对每个 Step:
        → render_template()          # Comp 内部
        → hero.get_agent(step.agent_id)  # 验证 Agent 存在
        → hero.execute(agent_id, task, context)  # 提交任务
        → 保存结果到 Context
```

---

## 13. 目录结构

```
tavern/
├── configs/
│   ├── agents/              # Hero 配置
│   └── workflows/           # Comp 配置
│       └── content_pipeline.yaml
├── crates/
│   ├── tavern-core/
│   ├── tavern-adapters/
│   ├── tavern-hero/
│   ├── tavern-comp/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── workflow.rs      # Workflow 数据模型 + YAML 解析
│   │       ├── registry.rs      # WorkflowRegistry 注册表
│   │       ├── engine.rs        # WorkflowEngine 执行逻辑
│   │       ├── context.rs       # 上下文 + 模板渲染
│   │       ├── validator.rs     # DAG 验证（拓扑排序、环检测）
│   │       └── error.rs         # CompError 定义
│   └── tavern-server/
└── docs/
    └── specs/
        ├── spec-hero.md
        └── spec-comp.md
```

---

## 14. 环境变量

| 变量 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `WORKFLOW_CONFIG_DIR` | 否 | `./configs/workflows` | Workflow YAML 配置目录 |

其他环境变量与 Hero 共用（`RUNTIME_URL`、`AGENT_CONFIG_DIR` 等）。

---

## 15. 依赖矩阵（含 Comp）

| Crate | core | adapters | hero | comp | server |
|-------|:----:|:--------:|:----:|:----:|:------:|
| `serde` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `serde_yaml` | - | - | ✓ | ✓ | - |
| `serde_json` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `thiserror` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `reqwest` | - | ✓ | - | - | - |
| `axum` | - | - | - | - | ✓ |
| `tokio` | - | - | ✓ | ✓ | ✓ |
| `tracing` | - | - | - | - | ✓ |
| `async-trait` | ✓ | ✓ | ✓ | - | - |
| `tavern-core` | - | ✓ | ✓ | ✓ | ✓ |
| `tavern-adapters` | - | - | - | - | ✓ |
| `tavern-hero` | - | - | - | ✓ | ✓ |
| `tavern-comp` | - | - | - | - | ✓ |

---

## 16. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 0.1.0 | 2026-05-20 | 初始版本：串行执行、DAG 验证、模板渲染、Hero 集成 |
