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

    /// 技能特定配置，格式由技能本身定义
    /// YAML 中可省略，默认 {}
    #[serde(default = "default_empty_object")]
    pub config: serde_json::Value,
}

fn default_empty_object() -> serde_json::Value {
    serde_json::json!({})
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
