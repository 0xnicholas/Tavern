use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentConfig {
    /// 全局唯一标识符
    /// 约束：^[a-zA-Z0-9_-]+$，长度 1-64
    pub id: String,

    /// 可读名称
    pub name: String,

    /// 描述（可选）
    /// YAML 中可省略，默认 null
    #[serde(default)]
    pub description: Option<String>,

    /// LLM 模型配置
    pub model: ModelConfig,

    /// 系统提示词 / 角色设定
    pub instructions: String,

    /// Agent 可调用的技能列表
    /// YAML 中可省略，默认 []
    #[serde(default)]
    pub skills: Vec<SkillConfig>,

    /// 行为约束列表
    /// YAML 中可省略，默认 []
    #[serde(default)]
    pub constraints: Vec<String>,

    /// 记忆配置
    /// YAML 中可省略，默认 disabled
    #[serde(default)]
    pub memory: MemoryConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelConfig {
    /// 模型提供商，如 "openai", "anthropic"
    pub provider: String,

    /// 模型名称，如 "gpt-4o"
    pub name: String,

    /// 采样温度
    /// 范围：0.0 - 2.0
    /// YAML 中可省略，默认 0.7
    #[serde(default = "default_temperature")]
    pub temperature: f32,
}

fn default_temperature() -> f32 {
    0.7
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SkillConfig {
    /// 技能唯一标识
    pub id: String,

    /// LLM function name（默认 = id）
    #[serde(default)]
    pub name: Option<String>,

    /// 工具描述
    #[serde(default)]
    pub description: Option<String>,

    /// JSON Schema，描述工具参数（默认 {}）
    #[serde(default = "default_empty_object")]
    pub parameters: serde_json::Value,

    /// 工具回调超时（毫秒），默认 30000
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,

    /// 工具执行方式，默认 Rust（向后兼容）
    #[serde(default)]
    pub runner: ToolRunner,

    /// subprocess 模式：启动命令（按空白字符拆分为 prog + args）
    #[serde(default)]
    pub command: Option<String>,

    /// subprocess 模式：子进程工作目录。None = 继承 server CWD
    #[serde(default)]
    pub cwd: Option<String>,

    /// subprocess 模式：环境变量。None = 继承 server 环境，Some({}) = 清除
    #[serde(default)]
    pub env: Option<std::collections::HashMap<String, String>>,

    /// sidecar 模式：边车 HTTP URL
    #[serde(default)]
    pub url: Option<String>,

    /// 技能特定配置，格式由技能本身定义
    /// YAML 中可省略，默认 {}
    #[serde(default = "default_empty_object")]
    pub config: serde_json::Value,
}

fn default_timeout() -> u64 {
    30000
}

fn default_empty_object() -> serde_json::Value {
    serde_json::json!({})
}

/// 工具执行方式。
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunner {
    /// Rust 原生 ToolHandler（main.rs 手动注册）
    #[default]
    Rust,
    /// 子进程模式（stdin/stdout JSON 协议）
    Subprocess,
    /// HTTP 边车模式
    Sidecar,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MemoryConfig {
    /// 是否启用上下文记忆
    /// YAML 中可省略，默认 false
    #[serde(default)]
    pub enabled: bool,

    /// 最大保留对话轮数
    /// None 表示无限制
    /// YAML 中可省略，默认 None
    #[serde(default)]
    pub max_context_turns: Option<u32>,
}

/// Agent 摘要信息，用于列表接口
#[derive(Debug, Clone, Serialize)]
pub struct AgentSummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}

/// 校验 ID 是否符合 ^[a-zA-Z0-9_-]+$ 格式，且长度在 1-64 之间。
pub fn is_valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

// ── Phase 1: CrewAI Alignment 类型 ──

/// 执行策略，存储在 Workflow 上。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Process {
    /// 默认：DAG 拓扑排序 + 事件溯源
    #[default]
    Sequential,
    /// Manager Agent 动态委派
    Hierarchical(ManagerConfig),
}

/// Hierarchical 模式的 Manager Agent 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagerConfig {
    /// Manager Agent 的 ID（必须在 registry 中注册）
    pub agent_id: String,
    /// 可选：覆盖 Manager Agent 的 instructions
    #[serde(default)]
    pub instructions: Option<String>,
}

/// Planning 模式的配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningConfig {
    /// 是否启用 Planning
    pub enabled: bool,
    /// AgentPlanner 的 agent_id。None 时回退到 workflow.steps[0].agent_id
    #[serde(default)]
    pub planning_agent: Option<String>,
}

/// AgentPlanner 生成的执行计划。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub steps: Vec<PlanStep>,
    pub overall_strategy: String,
}

/// Plan 中的单个步骤。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub task_id: String,
    pub agent_id: String,
    pub reasoning: String,
    pub expected_output: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
}
