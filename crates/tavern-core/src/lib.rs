pub mod config;
pub mod runtime;

pub use config::{is_valid_id, AgentConfig, AgentSummary, MemoryConfig, ModelConfig, SkillConfig};
pub use runtime::{Runtime, RuntimeError};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_agent_config_deserialize() {
        let yaml = r#"
id: researcher
name: 研究员
description: 擅长信息检索

model:
  provider: openai
  name: gpt-4o
  temperature: 0.3

instructions: |
  你是一个研究助理。

skills:
  - id: web_search
    config:
      max_results: 5

constraints:
  - 回答必须使用中文

memory:
  enabled: true
  max_context_turns: 10
"#;
        let config: AgentConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.id, "researcher");
        assert_eq!(config.name, "研究员");
        assert_eq!(config.description, Some("擅长信息检索".to_string()));
        assert_eq!(config.model.provider, "openai");
        assert_eq!(config.model.name, "gpt-4o");
        assert!((config.model.temperature - 0.3).abs() < f32::EPSILON);
        assert_eq!(config.instructions.trim(), "你是一个研究助理。");
        assert_eq!(config.skills.len(), 1);
        assert_eq!(config.skills[0].id, "web_search");
        assert_eq!(config.skills[0].config, json!({"max_results": 5}));
        assert_eq!(config.constraints, vec!["回答必须使用中文"]);
        assert!(config.memory.enabled);
        assert_eq!(config.memory.max_context_turns, Some(10));
    }

    #[test]
    fn test_agent_config_defaults() {
        let yaml = r#"
id: writer
name: 写作助手
model:
  provider: openai
  name: gpt-4o
instructions: 写作
"#;
        let config: AgentConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.id, "writer");
        assert_eq!(config.description, None);
        assert!((config.model.temperature - 0.7).abs() < f32::EPSILON);
        assert!(config.skills.is_empty());
        assert!(config.constraints.is_empty());
        assert!(!config.memory.enabled);
        assert_eq!(config.memory.max_context_turns, None);
    }

    #[test]
    fn test_agent_summary_serialize() {
        let summary = AgentSummary {
            id: "researcher".to_string(),
            name: "研究员".to_string(),
            description: Some("擅长信息检索".to_string()),
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"id\":\"researcher\""));
        assert!(json.contains("\"name\":\"研究员\""));
    }

    #[test]
    fn test_runtime_error_display() {
        let err = RuntimeError::RequestFailed {
            status: 500,
            body: "internal error".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("status=500"));
        assert!(msg.contains("internal error"));

        let err2 = RuntimeError::InvalidResponse("bad json".to_string());
        assert!(format!("{}", err2).contains("bad json"));
    }
}
