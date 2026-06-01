# Tavern vs CrewAI 深度对比

> 基于 CrewAI 源码架构（`crew.py` / `agent.py` / `task.py` / `process.py` / `flow.py` / `memory/`）与 Tavern V0.3.9 源码对比。

## 1. 架构哲学

### CrewAI

```
Crew (orchestrator)
  ├─ Process (Sequential | Hierarchical)
  ├─ Agents[] (role, goal, backstory, llm, tools, memory, allow_delegation)
  ├─ Tasks[] (description, expected_output, agent, context, async_execution)
  ├─ ManagerAgent (自动生成，Hierarchical 模式专用)
  ├─ Memory (short_term / long_term / entity)
  └─ Tools[] (LangChain tools)
```

- **核心抽象是 Agent**：Agent 有角色、目标、背景故事——模拟人类团队成员
- **Process 决定调度策略**：Sequential = 按定义顺序；Hierarchical = Manager 每轮输出 JSON 委派
- **执行模型**：`Crew.kickoff()` → 同步阻塞，返回 `CrewOutput`。无中间状态暴露
- **状态**：全内存。`CrewOutput` 包含 `raw`（最终字符串）、`json_dict`、`token_usage`

### Tavern

```
Workflow (DAG definition)
  ├─ Process (Sequential | Hierarchical | Flow)
  ├─ Steps[] (id, agent_id, task, depends_on, timeout, retries, wait_for_signal, breakpoint, model_override, output_key)
  ├─ EventStore (SQLite/PostgreSQL, 每个状态变更记录为事件)
  ├─ InstanceState (运行中实例的完整状态机)
  └─ ExecutionHandle (外部可查询/取消/发信号)
```

- **核心抽象是 Workflow**：Step 引用 Agent。Agent 是被调用的资源，不是主角
- **Process 决定调度策略**：Sequential = DAG 拓扑排序并行；Hierarchical = Manager 委派（与 CrewAI 同）；Flow = 方法级事件驱动图
- **执行模型**：`engine.run()`（同步）/ `engine.start()`（异步返回 ExecutionHandle，可查询进度）
- **状态**：全持久化。崩溃后从 EventStore 恢复，可重放审计

---

## 2. CrewAI 关键源码对应 Tavern

| CrewAI 组件 | 文件 | 功能 | Tavern 对应 |
|------------|------|------|------------|
| `Crew` | `crew.py` | 顶层编排容器 | `Workflow` + `WorkflowEngine` |
| `Agent` | `agent.py` | 角色/目标/工具/LLM | `AgentConfig`（YAML） + `tavern-hero` 注册表 |
| `Task` | `task.py` | 描述/期望输出/上下文 | `Step`（功能超集：depends_on, timeout, retries, breakpoint, model_override） |
| `Process` | `process.py` | Sequential / Hierarchical | `Process` 枚举（Sequential / Hierarchical）+ `Flow` |
| `Flow` | `flow.py` | 事件驱动编排（start/listen/or/router） | `tavern-flow` crate（独立引擎） |
| `CrewOutput` | `crew.py` | 执行结果 | `WorkflowResult`（context + outputs + step_results） |
| `TaskOutput` | `task.py` | 单任务结果 | `StepResult`（status + output + error + timestamps + attempt） |
| Memory | `memory/` | 短期/长期/实体记忆 | `MemoryConfig`（`max_context_turns`） |
| Tools | `tools/` | LangChain 工具集成 | 无内建工具系统（委托 Pandaria 运行时） |
| ManagerAgent | `crew.py`（内联） | 自动生成 Manager 系统提示 | `ManagerConfig`（显式配置） |

---

## 3. 执行流对比

### CrewAI Sequential

```python
# crew.py 简化逻辑
for task in tasks:
    agent = task.agent or agents[0]
    output = agent.execute_task(task, context, tools)
    task.output = output
return CrewOutput(tasks_output=tasks)
```

- 严格串行。Task 定义顺序即执行顺序。
- 无依赖图。无法表达「Task C 依赖 A 和 B 同时完成」。

### Tavern Sequential (DAG)

```rust
// engine.rs 简化逻辑
let dag = build_dag_maps(workflow);  // 入度表 + 邻接表
let ready = steps with in_degree == 0;
// 并行提交所有就绪步骤
for step in ready {
    executor.submit(step);  // tokio::spawn
}
// 拓扑排序自动推进
```

- Task 通过 `depends_on` 显式声明依赖，无依赖的步骤自动并行。
- 等价于 CrewAI 的 Task 新增 `depends_on: [other_task_id]`。

### CrewAI Hierarchical

```python
# crew.py 简化逻辑
manager_agent = Agent(role="Manager", ...)
while pending_tasks:
    prompt = build_manager_prompt(agents, pending_tasks, completed_tasks)
    decision = manager_agent.call(prompt)  # JSON { action, task_id, agent_id }
    if decision.action == "delegate":
        agent.execute_task(task)
    elif decision.action == "done":
        break
```

### Tavern Hierarchical

```rust
// engine.rs 简化逻辑
loop {
    manager_loops += 1;
    if manager_loops > MAX_MANAGER_LOOPS { break Err(...); }
    let prompt = build_manager_prompt(agents, pending, completed);
    let decision = parse_manager_json(hero.execute(manager, prompt));
    match decision {
        Delegate { task_id, agent_id } => {
            executor.submit(step);  // tokio::spawn 异步执行
            // 等待步骤结果
        }
        Done => break,
    }
}
```

**行为几乎一致**。Tavern 额外提供：
- `MAX_MANAGER_LOOPS` 防止无限循环
- Manager JSON 解析失败自动重试一次
- 步骤异步执行（CrewAI 的 Hierarchical 也是同步等待每个 task）

---

## 4. CrewAI Flow vs Tavern Flow

CrewAI 在近期版本引入了 `Flow` 概念，与 Tavern 的 `tavern-flow` 高度相似。

### CrewAI Flow

```python
from crewai.flow.flow import Flow, start, listen, router

class MyFlow(Flow):
    @start()
    def step_a(self): ...

    @listen(step_a)
    def step_b(self, result): ...

    @router(step_b)
    def decision(self): return "path_a"  # or "path_b"

    @listen("path_a")
    def handle_a(self): ...

    @listen("path_b")
    def handle_b(self): ...
```

### Tavern Flow

```rust
#[derive(Flow)]
struct MyPipeline { state: MyState }

#[flow_impl(crate = "tavern_flow")]
impl MyPipeline {
    #[start]
    async fn step_a(&mut self) -> Result<String, FlowError> { ... }

    #[listen("step_a")]
    async fn step_b(&mut self, data: String) -> Result<String, FlowError> { ... }

    #[router("step_b")]
    async fn gate(&mut self, content: String) -> String { ... }
}
```

| 维度 | CrewAI Flow | Tavern Flow |
|------|------------|------------|
| 语言 | Python 装饰器 | Rust proc-macro |
| 组合器 | `@start`, `@listen`, `@router` | `#[start]`, `#[listen]`, `#[router]`，额外支持 `or("a","b")` / `and("a","b")` |
| 并行 | Python asyncio | tokio 多线程 + JoinSet + Semaphore |
| 持久化 | 无 | EventStore 复用 |
| 取消 | 无 | `Arc<AtomicBool>` + cancel API |
| Webhook | 无 | `with_webhook()` 构建器 |
| 断点 | 无 | 方法级 breakpoint |

---

## 5. 内存与状态

### CrewAI Memory

```python
# 三层记忆架构
class Memory:
    short_term: ShortTermMemory    # 对话缓冲 (ConversationBufferWindow)
    long_term: LongTermMemory      # 向量嵌入 (ChromaDB/Weaviate/Qdrant)
    entity: EntityMemory           # 知识图谱 (SQLite 存储实体关系)
```

- 记忆绑定在 Agent 上，跨 Task 累积
- 长期记忆通过向量相似度检索
- 实体记忆抽取人物/地点/概念关系

### Tavern Memory

```rust
pub struct MemoryConfig {
    pub enabled: bool,
    pub max_context_turns: Option<u32>,  // 仅此一项
}
```

- 仅控制上下文窗口大小
- 无向量嵌入、无知识图谱
- 这是 Tavern 目前**最大的功能差距**

---

## 6. 功能矩阵（完整版）

| 功能 | CrewAI | Tavern | 备注 |
|------|:--:|:--:|------|
| **核心执行** ||||
| YAML 配置 | ✅ | ✅ | |
| Python/Rust 代码配置 | ✅ | — | Tavern 纯 YAML |
| Sequential 执行 | ✅ | ✅ | |
| Hierarchical 执行 | ✅ ManagerAgent | ✅ ManagerAgent | 行为一致 |
| DAG 并行 | ❌（Flow 替代） | ✅ 拓扑排序 | CrewAI 用 Flow |
| Flow 事件驱动 | ✅ `@start/@listen` | ✅ `#[start]/#[listen]` | 高度相似 |
| OR/AND 组合器 | ❌ | ✅ `or("a","b")` / `and("a","b")` | |
| **管控** ||||
| 超时 | 手动 try/except | ✅ Step.timeout | |
| 重试 | 手动循环 | ✅ Step.retries + 指数退避 | |
| 人工审批 | ✅ Task.human_input | ✅ SignalAction + 超时 | CrewAI 仅 bool 开关 |
| 断点调试 | ❌ | ✅ Step.breakpoint | |
| Webhook | ❌ | ✅ HMAC 签名回调 | |
| Cron 定时 | ❌ | ✅ 5 字段 cron | |
| 批量执行 | ❌ | ✅ 并行聚合 | |
| 执行克隆 | ❌ | ✅ 历史输入复用 | |
| 租户限流 | ❌ | ✅ 滑动窗口 | |
| **持久化** ||||
| 执行事件流 | ❌ 内存 | ✅ EventStore (SQLite/PG) | |
| 崩溃恢复 | ❌ | ✅ Checkpoint Recovery | |
| 执行重放 | ❌ | ✅ Replay API | |
| **运行时** ||||
| LLM 集成 | ✅ 内置 OpenAI/Anthropic/Gemini | ✅ Pandaria HTTP 适配器 | CrewAI 更多内置 |
| Tool 系统 | ✅ LangChain tools | ❌ 依赖 Pandaria | |
| 模型覆盖 | ❌ | ✅ Step.model_override | |
| Agent 动态管理 | ❌ 需重启 | ✅ 运行时 CRUD | |
| **运维** ||||
| REST API | ❌ | ✅ 完整 CRUD | |
| SSE 实时流 | ❌ | ✅ | |
| 认证 | ❌ | ✅ API Key / JWT | |
| 热重载 | ❌ | ✅ notify watcher | |
| Metrics | ❌（LangSmith） | ✅ Prometheus histogram | |
| Docker | ❌ | ✅ docker-compose | |
| **记忆** ||||
| 短期记忆 | ✅ 窗口缓冲 | ⚠️ 仅 max_context_turns | |
| 长期记忆 | ✅ 向量嵌入 | ❌ | |
| 实体记忆 | ✅ 知识图谱 | ❌ | |
| **生态** ||||
| 社区 | 40k+ stars | — | |
| 文档 | 大量 | 基础 | |
| Python 集成 | ✅ 原生 | — | |

---

## 7. 总结

| | CrewAI | Tavern |
|------|--------|--------|
| **优势** | 记忆系统、Tool 生态、社区、Python 集成 | 事件溯源、管控（审批/断点/定时/限流）、性能、持久化 |
| **劣势** | 无持久化、无恢复、无管控 | 无记忆系统、无 Tool 生态、无社区 |
| **适合** | 探索/研究/原型，Python 生态集成 | 生产流水线，需要可审计/可恢复/可管控 |
| **独特** | Memory 三层架构 | EventStore + Checkpoint Recovery |
