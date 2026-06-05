# AI Agent Builder 产品分析报告

> 2026-06-03 — 对 Make.com、Zapier、Activepieces 的综合对比分析

## 1. 概述

AI Agent Builder 是低代码/无代码自动化平台向 AI 原生编排演进的新品类。核心能力是让用户通过可视化界面或自然语言定义 AI Agent 的工作流，Agent 自主调用工具、做出决策、与外部系统交互。三家代表性产品代表了三种不同的技术路线和市场定位。

| | Make | Zapier | Activepieces |
|---|---|---|---|
| **定位** | 可视化复杂工作流 | 规模化 AI 编排 | 开源 AI 优先自动化 |
| **成立** | 2012 (Integromat) | 2011 | 2022 |
| **总部** | 捷克布拉格 | 美国旧金山 | 开源社区 |
| **商业模式** | SaaS (用量计费) | SaaS (任务计费) | 开源 + SaaS |
| **集成数** | 3,000+ | 9,000+ | 300+ |
| **AI Agent** | 2025.04 Beta | 2025.05 重构 | 2024 内建 MCP |

## 2. 产品架构与 AI Agent 能力

### 2.1 Make.com — 可视化优先，Agent 嵌入工作流画布

**核心理念**：AI Agent 不是独立产品，而是可视化 Scenario Builder 的一类新模块。Agent 和确定性自动化在同一个画布上共存，用户能看到 Agent 的每一步决策。

**架构特点**：
- **统一画布**：Agent 作为 Scenario 的一个模块，与 API 调用、数据转换、条件分支并列
- **透明决策**：每次 Agent 行动都有可审计的日志，展示「Agent 看到了什么 → 想了什么 → 做了什么」
- **MCP Toolbox**（2026.03）：Agent 可通过 MCP 协议调用外部工具，用户自行配置 MCP Server
- **执行模型**：Scenario 引擎驱动，支持复杂分支、循环、错误处理

**技术栈推断**：自研可视化引擎，后端可能是 Node.js/TypeScript 或 Go，使用自研 Scenario 运行时。

**定价**（2026）：
| 计划 | 月费 | AI Agent 用量 |
|------|------|--------------|
| Free | $0 | 1,000 credits/月 |
| Core | $11/月 | 2,100+ credits |
| Pro | $19/月 | 8,300+ credits |
| Teams | $36/月 | 21,000+ credits |
| Enterprise | 定制 | 定制 |

每个 AI Agent 调用消耗 5 operations + 工具调用 operations。

### 2.2 Zapier — 规模化编排，多 Agent 协作

**核心理念**：从「连接 App 的自动化工具」转型为「AI 编排平台」。核心概念是 Pods（Agent 分组管理）和 Dashboards（集中监控），让用户像管理微服务一样管理 AI Agent。

**架构特点**：
- **Pods 概念**：将相关的 Agent 分组到 Pod 中，每个 Pod 有独立的 schedule、trigger、监控
- **Agent-to-Agent Calling**（2025.08）：Agent 之间可以相互调用，形成专业化的 AI 团队（一个 Agent 负责分类，另一个负责执行）
- **Multi-Agent System (MAS)**：通过 MCP 共享工具和上下文，Agent 之间通过定义好的角色边界和有意设计的 handoff 协调
- **可变人工监督**：每个 Agent 可独立设置人类审批级别（全自动 / 关键节点审批 / 全部审批）
- **9,000+ 集成**：数量最大，覆盖长尾 SaaS

**技术栈推断**：Python/Django 后端，可能正在迁移部分服务到更现代的架构。Agent 运行时可能是自研的编排引擎。

**定价**（2026）：
| 计划 | 月费 | AI Agent 用量 |
|------|------|--------------|
| Free | $0 | 400 activities/月 |
| Pro | ~$33/月 | 按量计费 |
| Enterprise | 定制 | 定制 |

Zapier 的计费模型以「task」为单位，AI Agent 按 activity 计费。

### 2.3 Activepieces — 开源，MCP 原生，开发者友好

**核心理念**：开源、AI 优先的自动化平台。从设计之初就将 MCP（Model Context Protocol）作为核心架构组件，而非后期添加。适用于需要数据主权、定制化、避免 vendor lock-in 的场景。

**架构特点**：
- **开源（MIT License）**：22K+ GitHub Stars，TypeScript 全栈
- **MCP 原生**：内建 MCP Server，AI 助手可通过自然语言创建 Flow、管理 Table、测试自动化
- **Pieces Framework**：类型安全的扩展框架，社区可贡献新的集成（Pieces）
- **AI Agent Library**：预构建 Agent（AI 客服、智能邮件处理等），开箱即用
- **自托管 + RBAC**：支持 Docker 自部署，最近修复了 MCP 工具的 RBAC 权限绕过漏洞（PR #12462），说明安全在快速迭代中仍是挑战
- **Piece Builder Skill**：AI 编码 Agent 帮助用户构建新的 Pieces 集成

**技术栈**：TypeScript (NestJS 后端 + Angular 前端)，PostgreSQL，Redis，ClickHouse（分析）

**定价**（2026）：
| 计划 | 月费 | 说明 |
|------|------|------|
| Self-Hosted | $0 | 无限 Tasks，无限 AI Agents |
| Cloud Free | $0 | 1,000 Tasks/月 |
| Cloud Plus | $25/月 | 无限 Tasks + AI Agents |
| Enterprise | 定制 | SSO, RBAC, 审计日志 |

自托管版本功能与 Cloud Plus 等价，无功能阉割。

## 3. 多 Agent 协作对比

| 维度 | Make | Zapier | Activepieces |
|------|------|--------|-------------|
| **Agent 间通信** | 通过 Scenario 变量传递 | Agent-to-Agent Calling + MCP | MCP + Flow 变量 |
| **协作模式** | 顺序/并行（画布连线） | 角色分工 + Handoff | 顺序/分支（Flow DAG） |
| **人工审批** | Scenario 中插入审批模块 | Agent 级审批设置 | Flow 中插入审批步骤 |
| **条件分支** | 强（可视化条件/路由） | 弱（依赖 Zap 逻辑） | 强（Flow 分支/循环） |
| **动态委派** | 不支持 | Manager Agent 委派 | 不支持 |
| **并行执行** | 支持（画布并行分支） | 支持（Pod 级） | 支持（Flow 并行分支） |
| **状态持久化** | 有限（Scenario 执行上下文） | Pod Dashboard 持久化 | Table（内建数据库） |

## 4. 全功能矩阵对比

| 功能维度 | Make | Zapier | Activepieces | **Tavern** |
|----------|------|--------|-------------|------------|
| **开源** | ❌ 闭源 | ❌ 闭源 | ✅ MIT | ✅ MIT |
| **私有部署** | 企业版（$$$） | 企业版（$$$） | ✅ Docker | ✅ Docker |
| **可视化画布** | ✅ 强（Scenario Builder） | ✅ 中等（Zap Editor） | ✅ Flow Builder | ❌ 无（YAML + Rust） |
| **自然语言建 Agent** | ✅ | ✅ | ✅（MCP） | ❌ |
| **SDK / 代码定义** | API only | API only | API + Pieces SDK | ✅ Rust proc-macro DSL |
| **Agent 注册** | 画布内定义 | Pod 内定义 | Agent Library | YAML + REST API CRUD |
| **AND 依赖** | ✅（画布线） | ✅（Zap 触发） | ✅（Flow） | ✅ `depends_on` |
| **OR 依赖（任一触发）** | ❌ | ❌ | ❌ | ✅ `or_depends_on` |
| **条件分支（Router）** | ✅ 可视化条件 | 弱（Filter step） | ✅ Flow 分支 | ✅ `#[router]` + label routing |
| **动态委派（Manager）** | ❌ | ✅ Agent-to-Agent Calling | ❌ | ✅ Hierarchical Process |
| **并行执行** | ✅（画布分支） | ✅（Pod 级） | ✅（Flow 级） | ✅ JoinSet + Semaphore |
| **人工审批** | ✅ 审批模块 | ✅ Agent 级设置 | ✅ 审批 step | ✅ `wait_for_signal` + REST API |
| **断点调试** | ❌ | ❌ | ❌ | ✅ `breakpoint` + 信号恢复 |
| **重试 / 超时** | ✅ 内建 | ✅ 内建 | ✅ 内建 | ✅ `retries` + `retry_delay` + `timeout` |
| **Cron 定时调度** | ✅ Scenario schedule | ✅ Pod schedule | ✅ Flow trigger | ✅ `schedule` (5-field cron) |
| **批量执行** | ❌ | ❌ | ❌ | ✅ `run_batch` |
| **SSE 实时事件流** | ❌ | ❌ | ❌（WebSocket） | ✅ `GET /events/stream` |
| **事件溯源（EventStore）** | ❌ | ❌ | ❌ | ✅ SQLite / PostgreSQL EventStore |
| **执行回放（Replay）** | ❌ | ❌ | ❌ | ✅ StateDiff + Timeline |
| **执行克隆** | ❌ | ❌ | ❌ | ✅ `POST /clone` |
| **Webhook 回调** | ✅ | ✅ | ✅ | ✅ HMAC 签名 + 重试 |
| **认证** | OAuth / API Key | OAuth / API Key | OAuth / API Key | API Key / Bearer Token + Refresh |
| **多租户限流** | 企业版 | 企业版 | 企业版 | ✅ `RateLimitConfig` |
| **LLM 调用** | ✅ 内建 | ✅ 内建 | ✅ 内建（MCP） | ⚠️ 外部 Pandaria（可替换） |
| **MCP 支持** | ✅ Toolbox | ✅ | ✅ 原生内建 | ❌（待添加） |
| **集成数量** | 3,000+ | 9,000+ | 300+（可扩展） | 任意（适配器模式） |
| **语言** | 推断 Node.js/Go | Python/Django | TypeScript (NestJS) | Rust |
| **定价** | $11-36/月 + 用量 | $33/月 + 用量 | $0-25/月 | **$0（自托管，无限调用）** |

> ✅ = 优秀  |  ⚠️ = 部分/需改进  |  ❌ = 不支持

## 5. 与 Tavern 的定位差异

| 维度 | Make / Zapier / Activepieces | Tavern |
|------|------------------------------|--------|
| **目标用户** | 非开发者（低代码/无代码） | 开发者（YAML + Rust DSL） |
| **定义方式** | 拖拽画布 / 自然语言 | YAML 配置文件 / Rust proc-macro |
| **执行引擎** | 各自闭源自研 | `WorkflowEngine`（事件溯源） |
| **Agent 运行时** | 平台内建 LLM 调用 | 外部 Pandaria Runtime（可替换） |
| **持久化** | 平台托管 | SQLite / PostgreSQL（EventStore） |
| **开源** | 闭源 SaaS | MIT 全开源 |
| **私有部署** | 企业版定制 | Docker 一键部署 |
| **多 Agent 编排** | 顺序/并行 DAG | DAG (AND/OR) + Hierarchical + Router |
| **审批/信号** | 内建 | `wait_for_signal` + REST API |
| **SDK/可编程** | API + 有限 SDK | Rust proc-macro DSL + 完整 REST API |

**核心差异**：Make/Zapier/Activepieces 是**低代码平台**，降低 AI 工作流门槛。Tavern 是**开发者框架**，提供最大灵活性和可控性——你用代码定义 Agent 和编排逻辑，部署在自己的基础设施上。

## 8. 趋势与洞察

### 8.1 MCP 成为 Agent-工具交互的标准协议

三家平台全部集成了 MCP（Model Context Protocol）：
- **Activepieces**：MCP 原生，内建 MCP Server
- **Zapier**：通过 MCP 实现 Agent 间工具共享
- **Make**：2026.03 引入 MCP Toolbox

MCP 正在成为 AI Agent 与外部工具通信的事实标准，类似于 REST API 之于微服务。

### 8.2 「透明性」成为差异化卖点

Make 的核心营销信息是「Trust Through Transparency」——用户可以看到 Agent 的每一步推理和决策。Zapier 的 Dashboards 也提供类似的审计能力。这表明用户（尤其是企业用户）对 AI Agent 的「黑箱」行为高度警惕。

### 8.3 从「单个 Agent」到「Agent Team」

三家都在从单 Agent 自动化转向多 Agent 协作：
- Zapier 的 Agent-to-Agent Calling + Pod 分组
- Make 的多个 Agent 模块在同一个 Scenario 中协作
- Activepieces 的 Agent Library 可组合

这与 Tavern 的 Workflow（多 Agent 按 DAG 协作）+ Hierarchical Process（Manager 动态委派）的设计方向一致。

### 8.4 定价模式分化

| 模式 | 代表 | 特点 |
|------|------|------|
| 用量计费 (usage-based) | Make, Zapier | 每次 Agent 调用计费，规模越大越贵 |
| 固定月费 (flat-rate) | Activepieces Cloud | $25/月无限用量 |
| 免费自托管 (self-host) | Activepieces, Tavern | 零边际成本，适合高频调用场景 |

对于高频 AI Agent 使用场景，开源自托管方案（Activepieces/Tavern）的成本优势随调用量线性增长。

## 6. Tavern 差距分析

基于全功能矩阵，Tavern 在 29 个维度中的表现：

```
✅ 领先 (16): 开源、私有部署、SDK、OR 依赖、Router、Hierarchical、
              断点、批量、SSE、EventStore、Replay、Clone、
              多租户限流、Cron、重试/超时、审批
⚠️ 持平/需改进 (2): LLM 调用（依赖外部 Runtime）、MCP 支持
❌ 缺失 (11): 可视化画布、自然语言建 Agent、Agent-to-Agent Calling、
              内建 LLM 调用、MCP 原生集成、Pods/Dashboards、
              Agent Library、集成市场、Pieces 扩展机制、
              ClickHouse 分析、全局搜索
```

### 6.1 结构性强项

这些是 Tavern 独有且竞品短期内难以复制的：

| 强项 | 竞品状态 | 为什么难复制 |
|------|---------|-------------|
| **EventStore（事件溯源）** | 三家都无 | 需要从零重构执行引擎。Make/Zapier 的遗留引擎极难迁移 |
| **ExecutionReplay（执行回放）** | 三家都无 | 依赖 EventStore 基础。竞品的执行日志不完整，无法重建状态 |
| **OR 依赖 + Router 条件分支** | 三家都无 | 竞品的 DAG 模型都是纯 AND。Make 的画布线天然 AND |
| **Hierarchical Process（Manager 委派）** | 仅 Zapier 有（Agent Calling） | Tavern 的 Manager 有 Planning 阶段 + JSON 决策协议，更结构化 |
| **断点调试** | 三家都无 | 需要 Step 级暂停 + 外部信号恢复，竞品缺乏事件驱动的状态机 |
| **Rust DSL（proc-macro）** | 三家都无 | 语言差异——竞品的 JS/Python 生态无法提供编译时类型安全 |

### 6.2 结构性强项

这些差距按优先级排序：

#### 🔴 P0 — 核心可用性缺口

| 差距 | 影响 | 方案 | 预估工作量 |
|------|------|------|-----------|
| **内建 LLM 调用** | Agent 无法真正执行。当前依赖不存在的 Pandaria Runtime，导致 Tavern 实际上「不可用」 | `tavern-adapters` 新增 `OpenAiAdapter` / `AnthropicAdapter`，直连 LLM API | 2-3 天 |
| **全局搜索** | 无 | 无 | 无 |

#### 🟡 P1 — 生态集成缺口

| 差距 | 影响 | 方案 | 预估工作量 |
|------|------|------|-----------|
| **MCP 支持** | 竞品已全部支持，Tavern Agent 无法调用 MCP 生态的工具 | `tavern-adapters` 新增 `McpAdapter`，Agent 通过 MCP Client 调用外部工具 | 3-5 天 |
| **集成市场 / Agent Library** | 没有预构建的 Agent 模板，每个用户从零开始 | 在 `configs/agents/` 下提供示例 Agent（researcher, writer, reviewer, coder），作为模板库 | 1-2 天 |

#### 🟢 P2 — 用户体验缺口

| 差距 | 影响 | 方案 | 预估工作量 |
|------|------|------|-----------|
| **可视化画布** | 无法覆盖非开发者用户 | 提供 REST API + SSE，让第三方前端工具（如 n8n、Flowise）对接 | 外部依赖 |
| **自然语言建 Agent** | 无低代码入口 | 可写一个 CLI 工具，接受自然语言描述 → 生成 YAML 配置 | 2-3 天 |
| **Pods/Dashboards 监控** | 无集中监控面板 | `/metrics` (Prometheus) + `/executions/:id/replay` 已有基础，加 Grafana dashboard JSON | 1 天 |

## 7. 建议路线图

```
V0.4 (当前)        V0.5 (下一步)         V0.6               V1.0
─────────────────────────────────────────────────────────────────
EventStore ✅       LLM Adapter 🔴       MCP Adapter 🟡     可视化前端 🟢
OR/Router ✅        Agent Templates 🟡   Grafana Dashboard 🟢  自然语言 CLI 🟢
Flow DSL ✅         直连 OpenAI           MCP Tool 调用       第三方前端对接
27 REST API ✅      Anthropic 支持        MCP Server          Agent 市场
Replay ✅
Cron ✅
```

### 关键决策点

1. **LLM Adapter 是第一优先级**：没有它，Tavern 无法跑通完整的「定义 Agent → 执行 → 返回结果」闭环。这是 V0.5 的唯一阻塞项
2. **保持开发者定位**：不必追逐可视化画布（Make 的核心壁垒）。把 REST API 和 Rust DSL 做到极致，让第三方前端工具来对接
3. **MCP 是生态入口**：三个竞品已经验证了 MCP 的标准地位。Tavern 支持 MCP 后，Agent 可直接调用 400+ MCP Server，瞬间补齐「集成数量」短板
4. **事件溯源是长期壁垒**：ExecutionReplay 和 StateDiff 是竞品无法短期复制的差异化能力。在 V1.0 营销中应作为核心卖点

## 8. 竞品趋势与洞察
