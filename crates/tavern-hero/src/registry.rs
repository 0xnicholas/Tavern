use std::collections::HashMap;
use tavern_core::{AgentConfig, AgentSummary};
use tracing::instrument;

use crate::error::TavernError;

pub struct AgentRegistry {
    agents: HashMap<String, AgentConfig>,
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self {
            agents: HashMap::new(),
        }
    }

    /// 返回已注册 Agent 数量。
    pub fn len(&self) -> usize {
        self.agents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.agents.is_empty()
    }

    /// 注册 Agent，同时进行配置校验。
    #[instrument(skip(self, config), fields(agent_id = %config.id))]
    pub fn register(&mut self, config: AgentConfig) -> Result<(), TavernError> {
        if let Err(reason) = crate::validator::validate_agent_config(&config) {
            return Err(TavernError::ConfigParse {
                path: "<inline>".to_string(),
                reason,
            });
        }

        if self.agents.contains_key(&config.id) {
            return Err(TavernError::DuplicateAgent {
                id: config.id.clone(),
            });
        }

        self.agents.insert(config.id.clone(), config);
        Ok(())
    }

    pub fn get(&self, id: &str) -> Option<&AgentConfig> {
        self.agents.get(id)
    }

    pub fn list_all(&self) -> Vec<&AgentConfig> {
        self.agents.values().collect()
    }

    pub fn list_summary(&self) -> Vec<AgentSummary> {
        self.agents
            .values()
            .map(|c| AgentSummary {
                id: c.id.clone(),
                name: c.name.clone(),
                description: c.description.clone(),
            })
            .collect()
    }

    /// 迭代所有已注册 Agent（零分配）。
    pub fn iter(&self) -> impl Iterator<Item = &AgentConfig> {
        self.agents.values()
    }

    /// 迭代所有已注册 Agent 的摘要。
    pub fn iter_summary(&self) -> impl Iterator<Item = AgentSummary> + '_ {
        self.agents.values().map(|c| AgentSummary {
            id: c.id.clone(),
            name: c.name.clone(),
            description: c.description.clone(),
        })
    }
}
