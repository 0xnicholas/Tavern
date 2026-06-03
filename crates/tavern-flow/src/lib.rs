//! tavern-flow — Rust proc-macro DSL for method-level workflow orchestration.
//! Expands `#[start]`/`#[listen]`/`#[router]` into `tavern_comp::Workflow` +
//! `tavern_comp::FlowStepExecutor`, executed by `tavern_comp::WorkflowEngine`.

pub use tavern_flow_macros::{Flow, flow_impl, listen, router, start};

// Re-export from tavern-comp for user convenience
pub use tavern_comp::FlowStepExecutor;

/// Flow 方法错误类型。
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("flow error: {0}")]
    Other(String),
}
