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
        registry.unregister(id).ok_or_else(|| TavernError::AgentNotFound {
            id: id.to_string(),
        })?;
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
        let result = self
            .runtime
            .execute(agent_id, task, context, &agent.instructions, &model)
            .await?;
        Ok(result)
    }
}
