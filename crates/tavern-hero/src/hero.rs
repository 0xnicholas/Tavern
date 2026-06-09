use std::path::Path;
use std::sync::Arc;

use serde_json::Value;
use tavern_core::{AgentConfig, AgentSummary, Runtime};
use tokio::sync::RwLock;
use tracing::{info, instrument};

use crate::error::TavernError;
use crate::registry::AgentRegistry;

/// Agent 管理核心，负责加载、注册和向 Runtime 提交任务。
pub struct TavernHero {
    registry: RwLock<AgentRegistry>,
    runtime: Arc<dyn Runtime>,
}

impl TavernHero {
    /// 初始化，注入 Runtime 实现。
    pub fn new(runtime: Arc<dyn Runtime>) -> Self {
        Self {
            registry: RwLock::new(AgentRegistry::new()),
            runtime,
        }
    }

    /// 从目录批量加载 YAML 配置。
    #[instrument(skip(self))]
    pub async fn load_from_dir(&self, dir: &Path) -> Result<(), TavernError> {
        let configs = crate::loader::load_from_dir(dir)?;
        let mut registry = self.registry.write().await;
        for (config, path) in configs {
            registry.register(config).map_err(|e| e.with_path(&path))?;
        }
        let count = registry.len();
        drop(registry);
        info!(count = count, "loaded agents from directory");
        Ok(())
    }

    /// V0.3.8: 运行时注册 Agent（不依赖 YAML 文件）。
    pub async fn register_agent(&self, config: AgentConfig) -> Result<(), TavernError> {
        let id = config.id.clone();
        let mut registry = self.registry.write().await;
        registry.register(config)?;
        drop(registry);
        info!(agent_id = %id, "agent registered at runtime");
        Ok(())
    }

    /// V0.3.8: 运行时移除 Agent。
    /// 若 id 不存在返回 AgentNotFound。
    pub async fn unregister_agent(&self, id: &str) -> Result<(), TavernError> {
        let mut registry = self.registry.write().await;
        registry
            .unregister(id)
            .ok_or_else(|| TavernError::AgentNotFound { id: id.to_string() })?;
        drop(registry);
        info!(agent_id = %id, "agent unregistered at runtime");
        Ok(())
    }

    /// 热重载：清空后从目录重新加载所有 Agent。
    #[instrument(skip(self))]
    pub async fn reload_from_dir(&self, dir: &Path) -> Result<(), TavernError> {
        let configs = crate::loader::load_from_dir(dir)?;
        let mut registry = self.registry.write().await;
        registry.clear();
        for (config, path) in configs {
            if let Err(e) = registry.register(config) {
                tracing::warn!("failed to register agent from {:?}: {}", path, e);
            }
        }
        let count = registry.len();
        drop(registry);
        info!(count = count, "agents hot reloaded");
        Ok(())
    }

    /// 加载单个 Agent 配置，返回注册的 agent_id。
    #[instrument(skip(self))]
    pub async fn load_agent(&self, path: &Path) -> Result<String, TavernError> {
        let config = crate::loader::load_agent(path)?;
        let id = config.id.clone();
        let mut registry = self.registry.write().await;
        registry.register(config).map_err(|e| e.with_path(path))?;
        drop(registry);
        info!(agent_id = %id, "loaded agent from file");
        Ok(id)
    }

    /// 查询已注册 Agent。
    pub async fn get_agent(&self, id: &str) -> Option<AgentConfig> {
        self.registry.read().await.get(id).cloned()
    }

    /// 列出所有已注册 Agent（返回完整配置的克隆）。
    pub async fn list_agents(&self) -> Vec<AgentConfig> {
        self.registry
            .read()
            .await
            .list_all()
            .into_iter()
            .cloned()
            .collect()
    }

    /// 列出所有已注册 Agent 的摘要。
    pub async fn list_agents_summary(&self) -> Vec<AgentSummary> {
        self.registry.read().await.list_summary()
    }

    /// 提交任务执行。
    /// 前置检查：agent_id 必须在注册表中存在。
    /// Skills 和 constraints 会被注入到 system prompt 中。
    #[instrument(skip(self, context), fields(agent_id = %agent_id))]
    pub async fn execute(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<Value>,
    ) -> Result<Value, TavernError> {
        let agent = self
            .registry
            .read()
            .await
            .get(agent_id)
            .cloned()
            .ok_or_else(|| TavernError::AgentNotFound {
                id: agent_id.to_string(),
            })?;
        info!(task_len = task.len(), "submitting task to runtime");
        let model = format!("{}/{}", agent.model.provider, agent.model.name);
        let system_prompt = build_system_prompt(&agent);
        let tools = skills_to_tool_defs(&agent.skills);
        let result = self
            .runtime
            .execute(agent_id, task, context, &system_prompt, &model, &tools)
            .await?;
        Ok(result)
    }

    /// V0.3.9: 提交任务执行，使用指定的模型覆盖 Agent 默认模型。
    #[instrument(skip(self, context), fields(agent_id = %agent_id, model = %model_override))]
    pub async fn execute_with_model(
        &self,
        agent_id: &str,
        task: &str,
        context: Option<Value>,
        model_override: &str,
    ) -> Result<Value, TavernError> {
        let agent = self
            .registry
            .read()
            .await
            .get(agent_id)
            .cloned()
            .ok_or_else(|| TavernError::AgentNotFound {
                id: agent_id.to_string(),
            })?;
        info!(task_len = task.len(), model = %model_override, "submitting task to runtime with model override");
        let system_prompt = build_system_prompt(&agent);
        let tools = skills_to_tool_defs(&agent.skills);
        let result = self
            .runtime
            .execute(agent_id, task, context, &system_prompt, model_override, &tools)
            .await?;
        Ok(result)
    }
}

/// 将 Agent 的 instructions、skills 和 constraints 组装成完整 system prompt。
fn build_system_prompt(agent: &tavern_core::AgentConfig) -> String {
    let mut prompt = agent.instructions.clone();

    // 注入 skills
    if !agent.skills.is_empty() {
        prompt.push_str("\n\n## Available Skills\n\n");
        for skill in &agent.skills {
            prompt.push_str(&format!("- **{}**", skill.id));
            if let Some(config_desc) = describe_skill_config(&skill.id, &skill.config) {
                prompt.push_str(&format!(": {}", config_desc));
            }
            prompt.push('\n');
        }
        prompt.push_str(
            "\nYou may use these skills when needed. Describe which skill you are using and its inputs.\n",
        );
    }

    // 注入 constraints
    if !agent.constraints.is_empty() {
        prompt.push_str("\n## Constraints\n\n");
        for constraint in &agent.constraints {
            prompt.push_str(&format!("- {}\n", constraint));
        }
    }

    prompt
}

/// 将 skill 的 config 转为简短描述。
fn describe_skill_config(skill_id: &str, config: &serde_json::Value) -> Option<String> {
    match skill_id {
        "web_search" => {
            let max = config.get("max_results")
                .and_then(|v| v.as_u64())
                .unwrap_or(5);
            Some(format!("Search the web (max {} results per query)", max))
        }
        "code_execution" | "code_interpreter" => {
            Some("Execute code in a sandboxed environment".to_string())
        }
        "file_read" | "file_reader" => {
            Some("Read and extract content from files".to_string())
        }
        _ => {
            if let Some(obj) = config.as_object() {
                if !obj.is_empty() {
                    let fields: Vec<String> = obj.iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    return Some(fields.join(", "));
                }
            }
            None
        }
    }
}

/// 将 SkillConfig 列表转换为 ToolDef 列表。
/// 需要 TAVERN_PUBLIC_URL 和 TAVERN_TOOL_SECRET 两个环境变量同时设置，
/// 否则返回空数组（回退到纯文本 skills 模式）。
fn skills_to_tool_defs(skills: &[tavern_core::SkillConfig]) -> Vec<tavern_core::ToolDef> {
    let public_url = match std::env::var("TAVERN_PUBLIC_URL") {
        Ok(url) => url.trim_end_matches('/').to_string(),
        Err(_) => return vec![],
    };
    if std::env::var("TAVERN_TOOL_SECRET").is_err() {
        return vec![];
    }

    skills
        .iter()
        .map(|s| {
            let name = s.name.clone().unwrap_or_else(|| s.id.clone());
            tavern_core::ToolDef {
                id: s.id.clone(),
                name: name.clone(),
                description: s.description.clone().unwrap_or_default(),
                parameters: s.parameters.clone(),
                endpoint: format!("{}/api/tools/{}", public_url, name),
                timeout_ms: s.timeout_ms,
                config: if s.config.is_null() { None } else { Some(s.config.clone()) },
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_skills_to_tool_defs_without_env_returns_empty() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            unsafe { std::env::remove_var("TAVERN_PUBLIC_URL") };
            unsafe { std::env::remove_var("TAVERN_TOOL_SECRET") };
        }

        let skill: tavern_core::SkillConfig = serde_yaml::from_str(
            "id: web_search\nname: search_web\ndescription: Search\nparameters:\n  type: object\ntimeout_ms: 10000\nconfig:\n  max: 5\n",
        )
        .unwrap();
        let tools = skills_to_tool_defs(&[skill]);
        assert!(tools.is_empty());
    }

    #[test]
    fn test_skills_to_tool_defs_constructs_correct_tool() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            unsafe { std::env::set_var("TAVERN_PUBLIC_URL", "http://localhost:3000") };
            unsafe { std::env::set_var("TAVERN_TOOL_SECRET", "test-secret") };
        }

        let skill: tavern_core::SkillConfig = serde_yaml::from_str(
            "id: web_search\nname: search_web\ndescription: Search web\nparameters:\n  type: object\ntimeout_ms: 10000\nconfig:\n  max: 5\n",
        )
        .unwrap();

        let tools = skills_to_tool_defs(&[skill]);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].id, "web_search");
        assert_eq!(tools[0].name, "search_web");
        assert_eq!(tools[0].description, "Search web");
        assert_eq!(
            tools[0].parameters,
            serde_json::json!({"type": "object"})
        );
        assert_eq!(
            tools[0].endpoint,
            "http://localhost:3000/api/tools/search_web"
        );
        assert_eq!(tools[0].timeout_ms, 10000);
        assert_eq!(
            tools[0].config,
            Some(serde_json::json!({"max": 5}))
        );

        unsafe {
            unsafe { std::env::remove_var("TAVERN_PUBLIC_URL") };
            unsafe { std::env::remove_var("TAVERN_TOOL_SECRET") };
        }
    }

    #[test]
    fn test_skills_to_tool_defs_name_falls_back_to_id() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            unsafe { std::env::set_var("TAVERN_PUBLIC_URL", "http://localhost:3000") };
            unsafe { std::env::set_var("TAVERN_TOOL_SECRET", "test-secret") };
        }

        // name 未设置时，应 fallback 到 id
        let skill: tavern_core::SkillConfig = serde_yaml::from_str(
            "id: my_tool\nconfig: {}\n",
        )
        .unwrap();

        let tools = skills_to_tool_defs(&[skill]);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "my_tool");
        assert_eq!(
            tools[0].endpoint,
            "http://localhost:3000/api/tools/my_tool"
        );

        unsafe {
            unsafe { std::env::remove_var("TAVERN_PUBLIC_URL") };
            unsafe { std::env::remove_var("TAVERN_TOOL_SECRET") };
        }
    }
}

