use tavern_core::RuntimeError;

#[derive(Debug, thiserror::Error)]
pub enum TavernError {
    #[error("agent '{id}' already registered")]
    DuplicateAgent { id: String },

    #[error("agent '{id}' not found")]
    AgentNotFound { id: String },

    #[error("config parse failed at {path}: {reason}")]
    ConfigParse { path: String, reason: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("runtime error: {0}")]
    Runtime(#[from] RuntimeError),
}

impl TavernError {
    /// 将 ConfigParse 错误的路径替换为实际路径，其他错误原样传递。
    pub fn with_path<E>(self, path: E) -> Self
    where
        E: Into<std::path::PathBuf>,
    {
        match self {
            TavernError::ConfigParse { reason, .. } => TavernError::ConfigParse {
                path: path.into().display().to_string(),
                reason,
            },
            other => other,
        }
    }
}
