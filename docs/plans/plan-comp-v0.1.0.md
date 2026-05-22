# Tavern Comp (tavern-comp) 开发计划（调整后）

> 基于 `docs/specs/spec-comp.md` 编写
> 目标：实现 Workflow 多 Agent 协作编排子系统 (V0.1.0)
> 调整日期：2026-05-21
> **前提**：`tavern-core`、`tavern-hero`、`tavern-adapters`、`tavern-server`（Hero API 部分）已实现

---

## 1. 项目现状

- **已完成**：
  - `tavern-core`：`Runtime` trait、`RuntimeError`、`AgentConfig`/`AgentSummary`/`ModelConfig`/`SkillConfig`/`MemoryConfig`
  - `tavern-hero`：`AgentRegistry`、`TavernHero`（含 `get_agent`/`execute`/`load_from_dir`/`load_agent`）、`TavernError`、YAML loader（私有）
  - `tavern-adapters`：`MockRuntime`、`PandariaRuntime`（含 30s HTTP 超时）
  - `tavern-server`（Hero API）：`GET /health`、`GET /agents`、`GET /agents/:id`、`POST /agents/:id/execute`、错误映射、启动逻辑
- **缺失**：`tavern-comp` crate 未创建；Server 无 Workflow 端点

---

## 2. Hero 接口兼容性确认

`tavern-hero` 当前已实现接口与 `spec-comp` 要求**完全对齐**：

```rust
// 来自 crates/tavern-hero/src/hero.rs
pub fn get_agent(&self, id: &str) -> Option<&AgentConfig>;
pub async fn execute(&self, agent_id: &str, task: &str, context: Option<Value>) -> Result<Value, TavernError>;
```

`TavernHero` 通过 `Arc<TavernHero>` 满足 Server State 的 `Clone + Send + Sync` 要求。

---

## 3. 阶段划分

### Phase A: tavern-comp 数据模型与错误（0.5 天）

**目标**：创建 `tavern-comp` crate，完成全部数据结构定义。

| 任务 | 文件 | 说明 |
|------|------|------|
| 创建 crate 骨架 | `Cargo.toml` | 依赖：`tavern-core`、`tavern-hero`、`serde`、`serde_yaml`、`serde_json`、`thiserror`、`tokio`、`chrono` |
| `CompError` | `src/error.rs` | 覆盖 spec 8.1 全部 12 个变体 |
| Workflow 数据模型 | `src/workflow.rs` | `Workflow`、`Step`、`InputDef`、`OutputDef` |
| 执行结果模型 | `src/workflow.rs` | `WorkflowResult`、`StepResult`、`StepStatus` |
| `lib.rs` 导出 | `src/lib.rs` | pub mod error, workflow |

**serde 默认值配置**：
按 spec 9.2，为含默认值的字段配置 `#[serde(default)]`：
- `Workflow.description`, `Workflow.inputs`, `Workflow.outputs`
- `Step.depends_on`, `Step.output_key`, `Step.timeout`
- `InputDef.required`
- `OutputDef` 各字段

**约束验证**：
- `Workflow.id`：`^[a-zA-Z0-9_-]+$`, 长度 1-64
- `Step.id`：workflow 内唯一
- `Step.timeout`：Option<u64>，反序列化缺失时为 None

**验收标准**：
- `cargo test` 通过结构体序列化/反序列化测试

---

### Phase B: YAML 解析与静态验证（1 天）

**目标**：加载 Workflow 配置并完成不依赖 Hero 的校验。

| 任务 | 文件 | 接口 |
|------|------|------|
| YAML 反序列化 | `src/workflow.rs` | `Workflow::from_yaml`, `Workflow::from_yaml_str` |
| 静态验证 | `src/workflow.rs` | `Workflow::validate_static` |
| DAG 验证模块 | `src/validator.rs` | Kahn 算法拓扑排序 + 环检测 |

**validate_static 检查项**：
1. `Workflow.id` 格式合法：`^[a-zA-Z0-9_-]+$`，长度 1-64
2. `Step.id` 唯一性 → `DuplicateStep`
3. `depends_on` 引用的步骤存在 → `StepNotFound`
4. 依赖图无环 → `CyclicDependency`
5. `output_key` 为 `Some` 时，值必须非空字符串，且 workflow 内唯一 → `DuplicateOutputKey`

**验收标准**：
- 能正确解析 `configs/workflows/content_pipeline.yaml`
- 对重复 step id、循环依赖、缺失依赖、重复 output_key 返回对应错误
- `cargo test` 覆盖全部校验分支

---

### Phase C: WorkflowRegistry（0.5 天）

**目标**：实现 Workflow 注册表。

| 任务 | 文件 | 接口 |
|------|------|------|
| 注册表实现 | `src/registry.rs` | `WorkflowRegistry::new/register/get/list_all/load_from_dir` |
| `WorkflowSummary` | `src/registry.rs` | `id, name, description`（`#[derive(Debug, Clone, Serialize)]`） |

**故障语义**：`load_from_dir` 遇到首个错误即终止，已加载的不回滚。

**验收标准**：
- 重复注册同一 workflow id 返回 `DuplicateWorkflow`
- `list_all` 返回摘要列表
- `cargo test` 通过

---

### Phase D: 模板渲染与上下文（0.5 天）

**目标**：实现 `{{var}}` 插值与 Context 管理。

| 任务 | 文件 | 说明 |
|------|------|------|
| 模板渲染 | `src/context.rs` | `render_template(template, context) -> Result<String, CompError>` |
| 上下文构建 | `src/context.rs` | 合并 inputs + 前序步骤 output_key |

**渲染规则**：
- 语法：`{{key}}` 简单字符串替换（正则或字符串扫描实现）
- 查找顺序：外部 inputs → 前序 output_key → `MissingContextVariable`
- 若 input 与 output_key 同名，外部输入优先
- 不支持嵌套对象访问（如 `{{research.notes}}`）

**验收标准**：
- `"研究主题: {{topic}}"` 正确渲染
- 缺失变量返回 `CompError::MissingContextVariable { name }`
- `cargo test` 覆盖正常渲染与错误分支

---

### Phase E: 执行引擎（1.5 天）

**目标**：实现 `WorkflowEngine` 核心编排逻辑。

| 任务 | 文件 | 接口 |
|------|------|------|
| Engine 结构体 | `src/engine.rs` | `WorkflowEngine { hero: Arc<TavernHero> }` |
| 动态验证 | `src/engine.rs` | `validate(&self, workflow)` 检查 Agent 存在性 |
| 执行逻辑 | `src/engine.rs` | `async fn run(&self, workflow, inputs)` |

**run 执行流程**：
1. `validate_static()` → `validate()`（检查 agent_id 在 Hero 注册表中存在）
2. **校验外部 `inputs`**：检查必填项、应用默认值，缺失时返回 `MissingInput`
3. 构建 DAG，Kahn 拓扑排序
4. 按拓扑顺序**串行执行**每个步骤（V0.1.0 不并行）
5. 对每个 Step：
   - 渲染 `task` 模板（按 inputs → output_key 顺序查找变量）
   - `hero.execute(agent_id, rendered_task, context)` 提交任务
   - **若声明了 `output_key`**：返回值写入全局 Context，键名为 `output_key`
   - **若未声明 `output_key`**：返回值**不进入**全局 Context，仅保留在 `StepResult.output`
   - 设置 `started_at` / `completed_at`
6. 全部成功后渲染 Workflow `outputs` 模板
7. 任一步骤失败 → 立即返回 `StepFailed`，保留已完成结果

**超时处理**：
- Step 级别超时，默认 300s
- 使用 `tokio::time::timeout(Duration::from_secs(step.timeout.unwrap_or(300)), hero.execute(...))`
- 超时后步骤状态为 `Failed`

**验收标准**：
- content_pipeline 完整执行成功
- 缺少必填 input 返回 `MissingInput`
- 引用的 Agent 不存在返回 `AgentNotFound`
- 步骤执行失败返回 `StepFailed`
- outputs 渲染失败返回 `MissingContextVariable`
- `cargo test` 通过（使用 `tavern_adapters::MockRuntime`）

---

### Phase F: tavern-server Workflow API 集成（1 天）

**目标**：在现有 Server 基础上补充 Workflow HTTP API。

**现有状态**：Server 已有 Hero API（`/health`、`/agents/*`），`AppState { hero: Arc<TavernHero> }`。

| 任务 | 文件 | 说明 |
|------|------|------|
| 扩展 AppState | `src/state.rs` | 增加 `registry: Arc<RwLock<WorkflowRegistry>>` 或初始化后只读方案 |
| 扩展路由 | `src/router.rs` | 新增：`GET /workflows`、`GET /workflows/:id`、`POST /workflows/:id/run` |
| Workflow Handler | `src/handlers.rs` | 实现上述端点；`POST /workflows/:id/run` 需在 `WorkflowResult` 基础上额外渲染并组装 `outputs` 字段 |
| 错误映射扩展 | `src/handlers.rs` | `CompError` → HTTP Status（spec-comp 8.2），与现有 `map_tavern_error` 并列 |
| 启动逻辑扩展 | `src/main.rs` | 读取 `WORKFLOW_CONFIG_DIR`（默认 `./configs/workflows`），加载 Workflow YAML |

**关键实现决策**：
- **State 并发策略**：`WorkflowRegistry` 在 V0.1.0 为初始化后只读（无运行时注册/删除），可用 `Arc<WorkflowRegistry>` 直接放入 `AppState`。若需 `&mut self` 的 `register`，初始化阶段完成后不再修改。
- **响应格式**：`POST /workflows/:id/run` 返回体为 `{ context, outputs, step_results }`，其中 `outputs` 由 server handler 根据 Workflow 的 `outputs` 定义从 `WorkflowResult.context` 中渲染组装。

**API 端点**：
- `GET /workflows` → 200 + `[WorkflowSummary]`
- `GET /workflows/:id` → 200 完整 Workflow JSON / 404 `WorkflowNotFound`
- `POST /workflows/:id/run` → 200 `{ context, outputs, step_results }` / 400 / 404 / 500 / 502

**错误映射补充**（与现有 `map_tavern_error` 并列）：

| CompError | HTTP Status |
|-----------|-------------|
| `WorkflowNotFound` | 404 |
| `MissingInput` | 400 |
| `AgentNotFound` | 404 |
| `StepFailed` | 502 |
| `MissingContextVariable` | 500 |
| `CyclicDependency` / `DuplicateStep` / `DuplicateOutputKey` / `StepNotFound` | 500 |

**验收标准**：
- `GET /workflows` 返回已加载的 Workflow 摘要列表
- `POST /workflows/content_pipeline/run` 带正确 inputs 返回 200 与完整结果（含 `outputs`）
- 错误场景返回正确的 HTTP 状态码和 JSON 错误体（与现有 Hero API 错误格式一致）

---

### Phase G: 集成测试与验收（1 天）

**目标**：验证全链路正确性。

| 任务 | 说明 |
|------|------|
| 单元测试 | `tavern-comp` 内部模块的单元测试 |
| 集成测试 | `tavern-comp` + Mock Hero 的编排测试 |
| Server 集成测试 | 在 `tavern-server/src/main.rs` 的 `#[cfg(test)]` 中补充 Workflow 端点测试 |
| 端到端测试 | 启动 server，通过 HTTP 调用完整 Workflow |
| 工作区级测试 | `cargo test --workspace` 全部通过 |

**端到端测试用例**：
1. content_pipeline 完整流程（research → write → edit），验证 `context` 和 `outputs`
2. 循环依赖 Workflow 返回 500
3. 缺失 input 返回 400
4. 不存在的 Agent 返回 404
5. 不存在的 Workflow 返回 404
6. 步骤执行失败（MockRuntime 返回 Err）返回 502

**验收标准**：
- `cargo test --workspace` 全部通过
- 无编译警告（`cargo check --workspace` clean）

---

## 4. 时间表（调整后）

| 阶段 | 内容 | 预估工时 |
|------|------|----------|
| Phase A | comp 数据模型与错误 | 0.5 天 |
| Phase B | YAML 解析与静态验证 | 1 天 |
| Phase C | WorkflowRegistry | 0.5 天 |
| Phase D | 模板与上下文 | 0.5 天 |
| Phase E | WorkflowEngine | 1.5 天 |
| Phase F | Server Workflow API 集成 | 1 天 |
| Phase G | 集成测试与验收 | 1 天 |
| **总计** | | **~6 天** |

---

## 5. 依赖关系

```
tavern-comp
├── tavern-core
├── tavern-hero
│   └── tavern-core
tavern-server
├── tavern-core
├── tavern-hero
├── tavern-adapters
│   └── tavern-core
└── tavern-comp（新增依赖）
    ├── tavern-core
    └── tavern-hero
```

---

## 6. 关键设计决策（保留/调整）

1. **串行执行（V0.1.0）**：WorkflowEngine 按拓扑顺序串行执行步骤。并行扩展在 V0.2.0 评估。
2. **无重试（V0.1.0）**：步骤失败立即中断 Workflow，不重试。
3. **不回滚加载**：`load_from_dir` 遇到错误保留已加载内容。
4. **模板引擎不引入外部依赖**：V0.1.0 使用正则/字符串替换实现 `{{key}}`。
5. **State 只读假设**：`WorkflowRegistry` 初始化后只读，server 运行时无锁访问。如需热重载，后续版本引入 `RwLock`。
