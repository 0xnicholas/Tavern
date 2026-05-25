pub mod error;
pub mod hero;
pub mod loader;
pub mod registry;
pub mod validator;

pub use error::TavernError;
pub use hero::TavernHero;
pub use registry::AgentRegistry;

#[cfg(test)]
pub mod fixtures {
    use tavern_core::{AgentConfig, MemoryConfig, ModelConfig};

    pub fn default_agent() -> AgentConfig {
        AgentConfig {
            id: "test-agent".to_string(),
            name: "Test".to_string(),
            description: None,
            model: ModelConfig {
                provider: "openai".to_string(),
                name: "gpt-4o".to_string(),
                temperature: 0.7,
            },
            instructions: "test instructions".to_string(),
            skills: vec![],
            constraints: vec![],
            memory: MemoryConfig::default(),
        }
    }

    pub fn agent_with_id(id: &str) -> AgentConfig {
        let mut a = default_agent();
        a.id = id.to_string();
        a
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tavern_adapters::MockRuntime;

    use super::*;
    use crate::fixtures::agent_with_id;

    // ---------- Registry tests ----------

    #[test]
    fn test_registry_register_and_get() {
        let mut reg = AgentRegistry::new();
        let config = agent_with_id("a1");
        assert!(reg.register(config).is_ok());
        assert!(reg.get("a1").is_some());
        assert!(reg.get("a2").is_none());
    }

    #[test]
    fn test_registry_duplicate() {
        let mut reg = AgentRegistry::new();
        let config = agent_with_id("a1");
        reg.register(config.clone()).unwrap();
        let err = reg.register(config).unwrap_err();
        assert!(matches!(err, TavernError::DuplicateAgent { id } if id == "a1"));
    }

    #[test]
    fn test_registry_list_summary() {
        let mut reg = AgentRegistry::new();
        let mut config = agent_with_id("a1");
        config.description = Some("desc".to_string());
        reg.register(config).unwrap();
        let summaries = reg.list_summary();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, "a1");
        assert_eq!(summaries[0].name, "Test");
        assert_eq!(summaries[0].description, Some("desc".to_string()));
    }

    // ---------- Loader tests ----------

    #[test]
    fn test_loader_valid_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.yaml");
        std::fs::write(
            &path,
            r#"
id: writer
name: 写作助手
model:
  provider: openai
  name: gpt-4o
instructions: 写作
"#,
        )
        .unwrap();

        let config = loader::load_agent(&path).unwrap();
        assert_eq!(config.id, "writer");
        assert_eq!(config.name, "写作助手");
    }

    #[test]
    fn test_loader_invalid_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.yaml");
        std::fs::write(&path, "not: yaml: [").unwrap();

        let err = loader::load_agent(&path).unwrap_err();
        assert!(matches!(err, TavernError::ConfigParse { path, .. } if path.ends_with("bad.yaml")));
    }

    #[test]
    fn test_loader_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.yaml"),
            r#"
id: a
name: A
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("b.yml"),
            r#"
id: b
name: B
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
        )
        .unwrap();
        // 非 yaml 文件应被忽略
        std::fs::write(dir.path().join("ignore.txt"), "ignore").unwrap();

        let configs = loader::load_from_dir(dir.path()).unwrap();
        assert_eq!(configs.len(), 2);
    }

    // ---------- Hero integration tests ----------

    #[tokio::test]
    async fn test_hero_execute_success() {
        let runtime = Arc::new(MockRuntime::new(
            |_agent_id, _task, _context, _sp, _model| Ok(json!({"result": "ok"})),
        ));
        let hero = TavernHero::new(runtime);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.yaml");
        std::fs::write(
            &path,
            r#"
id: researcher
name: 研究员
model:
  provider: openai
  name: gpt-4o
instructions: 研究
"#,
        )
        .unwrap();
        hero.load_agent(&path).unwrap();

        let result = hero.execute("researcher", "task", None).await.unwrap();
        assert_eq!(result, json!({"result": "ok"}));
    }

    #[tokio::test]
    async fn test_hero_execute_agent_not_found() {
        let runtime = Arc::new(MockRuntime::new(
            |_agent_id, _task, _context, _sp, _model| Ok(json!({"result": "ok"})),
        ));
        let hero = TavernHero::new(runtime);

        let err = hero.execute("unknown", "task", None).await.unwrap_err();
        assert!(matches!(err, TavernError::AgentNotFound { id } if id == "unknown"));
    }

    #[tokio::test]
    async fn test_hero_load_from_dir() {
        let runtime = Arc::new(MockRuntime::new(
            |_agent_id, _task, _context, _sp, _model| Ok(json!({})),
        ));
        let hero = TavernHero::new(runtime);

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.yaml"),
            r#"
id: a
name: A
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
        )
        .unwrap();

        hero.load_from_dir(dir.path()).unwrap();
        assert_eq!(hero.list_agents().len(), 1);
        assert_eq!(hero.list_agents_summary()[0].id, "a");
    }

    // ---------- Concurrency safety tests ----------

    #[tokio::test]
    async fn test_hero_concurrent_load_and_execute() {
        let runtime = Arc::new(MockRuntime::new(
            |_agent_id, _task, _context, _sp, _model| Ok(json!({"result": "ok"})),
        ));
        let hero = Arc::new(TavernHero::new(runtime));

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("agent.yaml"),
            r#"
id: concurrent_agent
name: Concurrent Agent
model:
  provider: openai
  name: gpt-4o
instructions: test
"#,
        )
        .unwrap();

        let hero_load = hero.clone();
        let dir_path = dir.path().to_path_buf();
        let load_handle = std::thread::spawn(move || {
            hero_load.load_from_dir(&dir_path).unwrap();
        });

        // 在加载的同时反复查询注册表
        for _ in 0..100 {
            let _ = hero.list_agents_summary();
            let _ = hero.get_agent("concurrent_agent");
        }

        load_handle.join().unwrap();

        // 加载完成后应能正常执行
        let result = hero
            .execute("concurrent_agent", "task", None)
            .await
            .unwrap();
        assert_eq!(result, json!({"result": "ok"}));
    }
}
