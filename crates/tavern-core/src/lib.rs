pub mod config;
pub mod runtime;
pub mod tool;

pub use config::{
    AgentConfig, AgentSummary, ManagerConfig, MemoryConfig, ModelConfig, Plan, PlanStep,
    PlanningConfig, Process, SkillConfig, ToolRunner, is_valid_id,
};
pub use runtime::{Runtime, RuntimeError, ToolDef};
pub use tool::{ContentPart, ToolError, ToolHandler, ToolRegistry, ToolResult};

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

    // ── Phase 1: CrewAI Alignment 新类型测试 ──

    #[test]
    fn test_process_default_is_sequential() {
        let process = Process::default();
        assert!(matches!(process, Process::Sequential));
    }

    #[test]
    fn test_process_sequential_deserialize_from_yaml() {
        let yaml = "sequential";
        let process: Process = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(process, Process::Sequential));
    }

    #[test]
    fn test_manager_config_deserialize() {
        let yaml = r#"
agent_id: manager
instructions: "You are a project manager."
"#;
        let config: ManagerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.agent_id, "manager");
        assert_eq!(
            config.instructions.as_deref(),
            Some("You are a project manager.")
        );
    }

    #[test]
    fn test_manager_config_defaults() {
        let yaml = r#"agent_id: manager"#;
        let config: ManagerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.agent_id, "manager");
        assert_eq!(config.instructions, None);
    }

    #[test]
    fn test_planning_config_deserialize() {
        let yaml = r#"
enabled: true
planning_agent: "planner"
"#;
        let config: PlanningConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.planning_agent.as_deref(), Some("planner"));
    }

    #[test]
    fn test_planning_config_defaults() {
        let yaml = "enabled: false";
        let config: PlanningConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.planning_agent, None);
    }

    #[test]
    fn test_plan_deserialize() {
        let json = r#"{
  "overall_strategy": "Research first, then write.",
  "steps": [
    {
      "task_id": "research",
      "agent_id": "researcher",
      "reasoning": "Need information first",
      "expected_output": "A research report",
      "dependencies": []
    }
  ]
}"#;
        let plan: Plan = serde_json::from_str(json).unwrap();
        assert_eq!(plan.overall_strategy, "Research first, then write.");
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].task_id, "research");
        assert_eq!(plan.steps[0].agent_id, "researcher");
        assert_eq!(plan.steps[0].dependencies.len(), 0);
    }

    // ── Tool Runtime: SkillConfig 扩展测试 ──

    #[test]
    fn test_skill_config_backward_compatible() {
        // 旧格式（无新字段）应正常反序列化，新字段取默认值
        let yaml = r#"
id: web_search
config:
  max_results: 5
"#;
        let skill: SkillConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(skill.id, "web_search");
        assert_eq!(skill.name, None);
        assert_eq!(skill.description, None);
        assert_eq!(skill.parameters, serde_json::json!({}));
        assert_eq!(skill.timeout_ms, 30000);
        assert_eq!(skill.config, serde_json::json!({"max_results": 5}));
    }

    #[test]
    fn test_skill_config_full_new_format() {
        let yaml = r#"
id: web_search
name: search_web
description: Search the web for information
parameters:
  type: object
  properties:
    query:
      type: string
      description: The search query
  required: [query]
timeout_ms: 15000
config:
  max_results: 10
"#;
        let skill: SkillConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(skill.id, "web_search");
        assert_eq!(skill.name, Some("search_web".to_string()));
        assert_eq!(skill.description, Some("Search the web for information".to_string()));
        assert_eq!(skill.parameters["type"], "object");
        assert_eq!(skill.timeout_ms, 15000);
        assert_eq!(skill.config, serde_json::json!({"max_results": 10}));
    }

    // ── Tool Runtime: ToolDef 序列化测试 ──

    #[test]
    fn test_tool_def_default_values() {
        let tool = ToolDef {
            id: "web_search".into(),
            name: String::new(),
            description: String::new(),
            parameters: serde_json::Value::Null,
            endpoint: String::new(),
            timeout_ms: 0,
            config: None,
        };
        let json = serde_json::to_string(&tool).unwrap();
        let parsed: ToolDef = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "web_search");
        assert!(json.contains("\"name\""));
    }

    #[test]
    fn test_tool_def_full_serialization_roundtrip() {
        let tool = ToolDef {
            id: "web_search".into(),
            name: "search_web".into(),
            description: "Search the web".into(),
            parameters: json!({"type": "object", "properties": {"query": {"type": "string"}}}),
            endpoint: "https://tavern.local/api/tools/search_web".into(),
            timeout_ms: 15000,
            config: Some(json!({"max_results": 5})),
        };
        let json = serde_json::to_string(&tool).unwrap();
        let parsed: ToolDef = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "web_search");
        assert_eq!(parsed.name, "search_web");
        assert_eq!(parsed.description, "Search the web");
        assert_eq!(parsed.endpoint, "https://tavern.local/api/tools/search_web");
        assert_eq!(parsed.timeout_ms, 15000);
        assert_eq!(parsed.config, Some(json!({"max_results": 5})));
    }

    #[test]
    fn test_tool_def_deserialize_default_timeout() {
        let json = r#"{"id":"test","name":"","description":"","parameters":{},"endpoint":""}"#;
        let tool: ToolDef = serde_json::from_str(json).unwrap();
        assert_eq!(tool.timeout_ms, 30000);
        assert_eq!(tool.config, None);
    }

    // ── ToolRunner 测试 ──

    #[test]
    fn test_skill_config_default_runner_is_rust() {
        let yaml = "id: ws\nconfig: {}";
        let skill: SkillConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(skill.runner, ToolRunner::Rust));
        assert_eq!(skill.command, None);
        assert_eq!(skill.url, None);
    }

    #[test]
    fn test_skill_config_subprocess_runner() {
        let yaml = r#"
id: code_exec
name: run_code
runner: subprocess
command: python3 tools/code_exec.py
cwd: /tmp/sandbox
env:
  PATH: /usr/bin
"#;
        let skill: SkillConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(skill.runner, ToolRunner::Subprocess));
        assert_eq!(skill.command.as_deref(), Some("python3 tools/code_exec.py"));
        assert_eq!(skill.cwd.as_deref(), Some("/tmp/sandbox"));
        assert!(skill.env.is_some());
    }

    #[test]
    fn test_skill_config_sidecar_runner() {
        let yaml = r#"
id: data_analysis
runner: sidecar
url: http://localhost:8001/tools/analysis
"#;
        let skill: SkillConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(skill.runner, ToolRunner::Sidecar));
        assert_eq!(skill.url.as_deref(), Some("http://localhost:8001/tools/analysis"));
    }
}
