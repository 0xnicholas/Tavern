use tavern_hero::TavernError;

#[derive(Debug, thiserror::Error)]
pub enum CompError {
    // -- V1 变体 --
    #[error("workflow '{id}' not found")]
    WorkflowNotFound { id: String },

    #[error("workflow '{id}' already registered")]
    DuplicateWorkflow { id: String },

    #[error("step '{id}' not found in workflow")]
    StepNotFound { id: String },

    #[error("duplicate step id '{id}' in workflow")]
    DuplicateStep { id: String },

    #[error("cyclic dependency detected in workflow")]
    CyclicDependency,

    #[error("agent '{id}' not found in registry")]
    AgentNotFound { id: String },

    #[error("duplicate output key '{key}' in workflow")]
    DuplicateOutputKey { key: String },

    #[error("missing context variable: {name}")]
    MissingContextVariable { name: String },

    #[error("template parse error: {reason}")]
    TemplateParse { reason: String },

    #[error("step '{step_id}' failed: {reason}")]
    StepFailed { step_id: String, reason: String },

    #[error("missing required input: {name}")]
    MissingInput { name: String },

    #[error("invalid input type: expected JSON object, got {got}")]
    InvalidInputType { got: String },

    #[error("config parse failed at {path}: {reason}")]
    ConfigParse { path: String, reason: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("hero error: {0}")]
    Hero(#[from] TavernError),

    #[error("execution instance '{id}' not found")]
    InstanceNotFound { id: String },

    #[error("execution instance '{id}' is closed")]
    InstanceClosed { id: String },

    #[error("instance '{id}' is not waiting for signal '{signal}'")]
    SignalRejected { id: String, signal: String },

    #[error("event store error: {0}")]
    StoreError(String),

    #[error("internal error: {0}")]
    Internal(String),

    // ── Phase 1: CrewAI Alignment ──
    #[error("manager agent error: {reason}")]
    ManagerError { reason: String },

    #[error("manager loop exceeded max loops ({max_loops})")]
    ManagerLoopExceeded { max_loops: usize },

    #[error("planning error: {reason}")]
    PlanningError { reason: String },

    #[error("planning agent '{id}' not registered")]
    PlanningAgentNotRegistered { id: String },
}

impl Clone for CompError {
    fn clone(&self) -> Self {
        match self {
            CompError::WorkflowNotFound { id } => CompError::WorkflowNotFound { id: id.clone() },
            CompError::DuplicateWorkflow { id } => CompError::DuplicateWorkflow { id: id.clone() },
            CompError::StepNotFound { id } => CompError::StepNotFound { id: id.clone() },
            CompError::DuplicateStep { id } => CompError::DuplicateStep { id: id.clone() },
            CompError::CyclicDependency => CompError::CyclicDependency,
            CompError::AgentNotFound { id } => CompError::AgentNotFound { id: id.clone() },
            CompError::DuplicateOutputKey { key } => {
                CompError::DuplicateOutputKey { key: key.clone() }
            }
            CompError::MissingContextVariable { name } => {
                CompError::MissingContextVariable { name: name.clone() }
            }
            CompError::TemplateParse { reason } => CompError::TemplateParse {
                reason: reason.clone(),
            },
            CompError::StepFailed { step_id, reason } => CompError::StepFailed {
                step_id: step_id.clone(),
                reason: reason.clone(),
            },
            CompError::MissingInput { name } => CompError::MissingInput { name: name.clone() },
            CompError::InvalidInputType { got } => CompError::InvalidInputType { got: got.clone() },
            CompError::ConfigParse { path, reason } => CompError::ConfigParse {
                path: path.clone(),
                reason: reason.clone(),
            },
            CompError::Io(e) => CompError::Io(std::io::Error::new(e.kind(), e.to_string())),
            CompError::Hero(e) => CompError::Internal(e.to_string()),
            CompError::InstanceNotFound { id } => CompError::InstanceNotFound { id: id.clone() },
            CompError::InstanceClosed { id } => CompError::InstanceClosed { id: id.clone() },
            CompError::SignalRejected { id, signal } => CompError::SignalRejected {
                id: id.clone(),
                signal: signal.clone(),
            },
            CompError::StoreError(s) => CompError::StoreError(s.clone()),
            CompError::Internal(s) => CompError::Internal(s.clone()),
            CompError::ManagerError { reason } => CompError::ManagerError {
                reason: reason.clone(),
            },
            CompError::ManagerLoopExceeded { max_loops } => CompError::ManagerLoopExceeded {
                max_loops: *max_loops,
            },
            CompError::PlanningError { reason } => CompError::PlanningError {
                reason: reason.clone(),
            },
            CompError::PlanningAgentNotRegistered { id } => {
                CompError::PlanningAgentNotRegistered { id: id.clone() }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_error_display() {
        let err = CompError::ManagerError {
            reason: "bad json".to_string(),
        };
        assert!(format!("{}", err).contains("bad json"));
    }

    #[test]
    fn test_manager_loop_exceeded_display() {
        let err = CompError::ManagerLoopExceeded { max_loops: 100 };
        assert!(format!("{}", err).contains("100"));
    }

    #[test]
    fn test_planning_error_display() {
        let err = CompError::PlanningError {
            reason: "timeout".to_string(),
        };
        assert!(format!("{}", err).contains("timeout"));
    }

    #[test]
    fn test_planning_agent_not_registered_display() {
        let err = CompError::PlanningAgentNotRegistered {
            id: "planner".to_string(),
        };
        assert!(format!("{}", err).contains("planner"));
    }
}
