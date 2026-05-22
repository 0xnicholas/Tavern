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
            CompError::DuplicateOutputKey { key } => CompError::DuplicateOutputKey { key: key.clone() },
            CompError::MissingContextVariable { name } => CompError::MissingContextVariable { name: name.clone() },
            CompError::TemplateParse { reason } => CompError::TemplateParse { reason: reason.clone() },
            CompError::StepFailed { step_id, reason } => CompError::StepFailed { step_id: step_id.clone(), reason: reason.clone() },
            CompError::MissingInput { name } => CompError::MissingInput { name: name.clone() },
            CompError::InvalidInputType { got } => CompError::InvalidInputType { got: got.clone() },
            CompError::ConfigParse { path, reason } => CompError::ConfigParse { path: path.clone(), reason: reason.clone() },
            CompError::Io(e) => CompError::Io(std::io::Error::new(e.kind(), e.to_string())),
            CompError::Hero(e) => CompError::Internal(e.to_string()),
            CompError::InstanceNotFound { id } => CompError::InstanceNotFound { id: id.clone() },
            CompError::InstanceClosed { id } => CompError::InstanceClosed { id: id.clone() },
            CompError::SignalRejected { id, signal } => CompError::SignalRejected { id: id.clone(), signal: signal.clone() },
            CompError::StoreError(s) => CompError::StoreError(s.clone()),
            CompError::Internal(s) => CompError::Internal(s.clone()),
        }
    }
}
