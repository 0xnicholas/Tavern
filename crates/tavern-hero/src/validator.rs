use tavern_core::{AgentConfig, is_valid_id};

/// 校验 Agent 配置的合法性。
///
/// 返回 `Ok(())` 表示通过，返回 `Err(String)` 表示失败原因。
pub fn validate_agent_config(config: &AgentConfig) -> Result<(), String> {
    // ID 格式校验
    if !is_valid_id(&config.id) {
        return Err(format!("invalid agent id '{}'", config.id));
    }

    // 非空字段校验
    if config.name.trim().is_empty() {
        return Err("agent name must not be empty".to_string());
    }
    if config.model.provider.trim().is_empty() {
        return Err("model.provider must not be empty".to_string());
    }
    if config.model.name.trim().is_empty() {
        return Err("model.name must not be empty".to_string());
    }
    if config.instructions.trim().is_empty() {
        return Err("instructions must not be empty".to_string());
    }

    // temperature 范围校验
    if config.model.temperature < 0.0 || config.model.temperature > 2.0 {
        return Err(format!(
            "temperature must be in [0.0, 2.0], got {}",
            config.model.temperature
        ));
    }

    // max_context_turns 校验
    const MAX_CONTEXT_TURNS: u32 = 10_000;
    if let Some(turns) = config.memory.max_context_turns {
        if turns < 1 {
            return Err(format!("max_context_turns must be >= 1, got {}", turns));
        }
        if turns > MAX_CONTEXT_TURNS {
            return Err(format!(
                "max_context_turns must be <= {}, got {}",
                MAX_CONTEXT_TURNS, turns
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::default_agent;

    #[test]
    fn test_valid_config() {
        let config = default_agent();
        assert!(validate_agent_config(&config).is_ok());
    }

    #[test]
    fn test_invalid_id() {
        let mut config = default_agent();
        config.id = "a b".to_string();
        assert_eq!(
            validate_agent_config(&config),
            Err("invalid agent id 'a b'".to_string())
        );
    }

    #[test]
    fn test_empty_name() {
        let mut config = default_agent();
        config.name = "   ".to_string();
        assert_eq!(
            validate_agent_config(&config),
            Err("agent name must not be empty".to_string())
        );
    }

    #[test]
    fn test_empty_provider() {
        let mut config = default_agent();
        config.model.provider = "".to_string();
        assert_eq!(
            validate_agent_config(&config),
            Err("model.provider must not be empty".to_string())
        );
    }

    #[test]
    fn test_empty_model_name() {
        let mut config = default_agent();
        config.model.name = "  ".to_string();
        assert_eq!(
            validate_agent_config(&config),
            Err("model.name must not be empty".to_string())
        );
    }

    #[test]
    fn test_empty_instructions() {
        let mut config = default_agent();
        config.instructions = "\n   \n".to_string();
        assert_eq!(
            validate_agent_config(&config),
            Err("instructions must not be empty".to_string())
        );
    }

    #[test]
    fn test_invalid_temperature() {
        let mut config = default_agent();
        config.model.temperature = 3.0;
        assert_eq!(
            validate_agent_config(&config),
            Err("temperature must be in [0.0, 2.0], got 3".to_string())
        );
    }

    #[test]
    fn test_invalid_max_context_turns() {
        let mut config = default_agent();
        config.memory.max_context_turns = Some(0);
        assert_eq!(
            validate_agent_config(&config),
            Err("max_context_turns must be >= 1, got 0".to_string())
        );
    }

    #[test]
    fn test_unicode_id_rejected() {
        let mut config = default_agent();
        config.id = "研究员".to_string();
        assert_eq!(
            validate_agent_config(&config),
            Err("invalid agent id '研究员'".to_string())
        );
    }
}
