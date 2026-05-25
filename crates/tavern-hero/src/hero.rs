use std::path::Path;
use std::sync::Arc;

use serde_json::Value;
use tavern_core::{AgentConfig, AgentSummary, Runtime};
use tracing::{info, instrument};

use crate::error::TavernError;
use crate::registry::AgentRegistry;

/// Agent 管理核心，负责加载、注册和向 Runtime 提交任务。
pub struct TavernHero {
    registry: std::sync::RwLock<AgentRegistry>,
    runtime: Arc<dyn Runtime>,
}

impl TavernHero {
    /// 初始化，注入 Runtime 实现。
    pub fn new(runtime: Arc<dyn Runtime>) -> Self {
        Self {
            registry: std::sync::RwLock::new(AgentRegistry::new()),
            runtime,
        }
    }

    /// 从目录批量加载 YAML 配置。
    #[instrument(skip(self))]
    pub fn load_from_dir(&self, dir: &Path) -> Result<(), TavernError> {
        let configs = crate::loader::load_from_dir(dir)?;
        let mut registry = self.registry.write().unwrap();
        for (config, path) in configs {
            registry.register(config).map_err(|e| e.with_path(&path))?;
        }
        drop(registry);
        info!(
            count = self.registry.read().unwrap().len(),
            "loaded agents from directory"
        );
        Ok(())
    }

    /// 热重载：清空后从目录重新加载所有 Agent。
    #[instrument(skip(self))]
    pub fn reload_from_dir(&self, dir: &Path) -> Result<(), TavernError> {
        let configs = crate::loader::load_from_dir(dir)?;
        let mut registry = self.registry.write().unwrap();
        registry.clear();
        for (config, path) in configs {
            if let Err(e) = registry.register(config) {
                tracing::warn!("failed to register agent from {:?}: {}", path, e);
            }
        }
        drop(registry);
        info!(count = self.registry.read().unwrap().len(), "agents hot reloaded");
        Ok(())
    }

    /// 加载单个 Agent 配置，返回注册的 agent_id。
    #[instrument(skip(self))]
    pub fn load_agent(&self, path: &Path) -> Result<String, TavernError> {
        let config = crate::loader::load_agent(path)?;
        let id = config.id.clone();
        let mut registry = self.registry.write().unwrap();
        registry.register(config).map_err(|e| e.with_path(path))?;
        drop(registry);
        info!(agent_id = %id, "loaded agent from file");
        Ok(id)
    }

    /// 查询已注册 Agent。
    pub fn get_agent(&self, id: &str) -> Option<AgentConfig> {
        self.registry.read().unwrap().get(id).cloned()
    }

    /// 列出所有已注册 Agent（返回完整配置的克隆）。
    pub fn list_agents(&self) -> Vec<AgentConfig> {
        self.registry
            .read()
            .unwrap()
            .list_all()
            .into_iter()
            .cloned()
            .collect()
    }

    /// 列出所有已注册 Agent 的摘要。
    pub fn list_agents_summary(&self) -> Vec<AgentSummary> {
        self.registry.read().unwrap().list_summary()
    }

    /// 迭代所有已注册 Agent。
    pub fn iter_agents(&self) -> impl Iterator<Item = AgentConfig> {
        self.list_agents().into_iter()
    }

    /// 迭代所有已注册 Agent 的摘要。
    pub fn iter_agents_summary(&self) -> impl Iterator<Item = AgentSummary> {
        self.list_agents_summary().into_iter()
    }

    /// 提交任务执行。
    /// 前置检查：agent_id 必须在注册表中存在。
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
            .unwrap()
            .get(agent_id)
            .cloned()
            .ok_or_else(|| TavernError::AgentNotFound {
                id: agent_id.to_string(),
            })?;
        info!(task_len = task.len(), "submitting task to runtime");
        let model = format!("{}/{}", agent.model.provider, agent.model.name);
        let result = self
            .runtime
            .execute(agent_id, task, context, &agent.instructions, &model)
            .await?;
        Ok(result)
    }
}
