//! tavern-flow — Rust 原生方法级事件驱动编排，类似 CrewAI Flow。
//!
//! ## 使用示例
//!
//! ```ignore
//! use tavern_flow_macros::{Flow, flow_impl, start, listen};
//!
//! #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! struct MyState { value: String }
//!
//! #[derive(Flow)]
//! #[flow(state = MyState)]
//! struct MyPipeline { hero: Arc<TavernHero> }
//!
//! #[flow_impl]
//! impl MyPipeline {
//!     #[start]
//!     async fn step_a(&mut self) -> Result<String, FlowError> { ... }
//!
//!     #[listen("step_a")]
//!     async fn step_b(&mut self, data: String) -> Result<String, FlowError> { ... }
//! }
//! ```

use std::collections::HashMap;

pub use tavern_flow_macros::{flow_impl, listen, start, Flow};

/// Flow 元数据：入口方法列表。
#[derive(Debug, Clone)]
pub struct FlowMetadata {
    pub start_methods: Vec<String>,
}

impl Default for FlowMetadata {
    fn default() -> Self {
        Self {
            start_methods: vec![],
        }
    }
}

/// Flow trait — 由 `#[derive(Flow)]` 自动实现。
pub trait Flow {
    fn metadata() -> FlowMetadata;
}

/// FlowDispatch trait — 由 `#[flow_impl]` 自动实现。
/// 将方法名路由到对应的异步方法调用。
pub trait FlowDispatch {
    fn dispatch(
        &mut self,
        method: &str,
        input: serde_json::Value,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, FlowError>> + Send + '_>,
    >;
}

/// Flow 错误类型。
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    #[error("method '{name}' not found")]
    MethodNotFound { name: String },

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("flow error: {0}")]
    Other(String),
}

/// FlowGraph 中的方法节点。
#[derive(Debug, Clone)]
struct MethodNode {
    name: String,
    is_start: bool,
    listeners: Vec<Listener>,
}

#[derive(Debug, Clone)]
enum Listener {
    Single(String),
}

/// FlowGraph — 方法依赖图。
pub(crate) struct FlowGraph {
    nodes: HashMap<String, MethodNode>,
}

impl FlowGraph {
    pub fn from_metadata(meta: &FlowMetadata) -> Self {
        // In the prototype, we only have start methods
        let mut nodes = HashMap::new();
        for name in &meta.start_methods {
            nodes.insert(
                name.clone(),
                MethodNode {
                    name: name.clone(),
                    is_start: true,
                    listeners: vec![],
                },
            );
        }
        Self { nodes }
    }

    pub fn start_nodes(&self) -> Vec<String> {
        self.nodes
            .values()
            .filter(|n| n.is_start)
            .map(|n| n.name.clone())
            .collect()
    }
}

/// FlowEngine — 方法图事件循环执行引擎。
pub struct FlowEngine<F: Flow + FlowDispatch> {
    flow: F,
    graph: FlowGraph,
}

impl<F: Flow + FlowDispatch> FlowEngine<F> {
    pub fn new(flow: F) -> Self {
        let meta = F::metadata();
        let graph = FlowGraph::from_metadata(&meta);
        Self { flow, graph }
    }

    /// 同步执行 flow（原型阶段简化：只执行 #[start] 方法）。
    pub async fn execute(
        &mut self,
        _inputs: serde_json::Value,
    ) -> Result<serde_json::Value, FlowError> {
        let starts = self.graph.start_nodes();
        if starts.is_empty() {
            return Err(FlowError::Other("no start methods found".to_string()));
        }

        // For prototype: execute only the first start method
        let method = &starts[0];
        let result = self
            .flow
            .dispatch(method.as_str(), serde_json::Value::Null)
            .await?;
        Ok(result)
    }

    /// 异步启动执行（后续实现）。
    #[allow(dead_code)]
    pub async fn start(self, _inputs: serde_json::Value) -> Result<FlowHandle, FlowError> {
        todo!()
    }
}

/// FlowHandle — 异步执行句柄（后续实现）。
#[allow(dead_code)]
pub struct FlowHandle;

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试 Flow trait 默认实现。
    #[test]
    fn test_flow_metadata_default() {
        let meta = FlowMetadata::default();
        assert!(meta.start_methods.is_empty());
    }

    /// 测试 FlowGraph 从 metadata 构建。
    #[test]
    fn test_flow_graph_from_metadata() {
        let meta = FlowMetadata {
            start_methods: vec!["step_a".to_string()],
        };
        let graph = FlowGraph::from_metadata(&meta);
        assert_eq!(graph.start_nodes(), vec!["step_a"]);
    }

    // ── Proc-macro 原型验证: 手动展开版 ──
    // 这是 `#[flow_impl(crate = "crate")]` 期望生成的内容

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct ManualState {
        value: String,
        transformed: Option<String>,
    }

    struct ManualPipeline {
        state: ManualState,
    }

    impl ManualPipeline {
        async fn step_a(&mut self) -> Result<String, FlowError> {
            self.state.value = "from_a".to_string();
            Ok("result_a".to_string())
        }

        async fn step_b(&mut self, data: String) -> Result<String, FlowError> {
            self.state.transformed = Some(format!("transformed: {}", data));
            Ok(format!("got: {}", data))
        }
    }

    impl FlowDispatch for ManualPipeline {
        fn dispatch(
            &mut self,
            method: &str,
            input: serde_json::Value,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<serde_json::Value, FlowError>> + Send + '_>,
        > {
            match method {
                "step_a" => Box::pin(self.step_a_wrapper()),
                "step_b" => {
                    let data: String = match serde_json::from_value(input) {
                        Ok(d) => d,
                        Err(e) => {
                            return Box::pin(std::future::ready(Err(FlowError::Serialization(
                                e.to_string(),
                            ))))
                        }
                    };
                    Box::pin(self.step_b_wrapper(data))
                }
                _ => Box::pin(std::future::ready(Err(FlowError::MethodNotFound {
                    name: method.to_string(),
                }))),
            }
        }
    }

    impl ManualPipeline {
        async fn step_a_wrapper(&mut self) -> Result<serde_json::Value, FlowError> {
            let result = self.step_a().await?;
            Ok(
                serde_json::to_value(result)
                    .map_err(|e| FlowError::Serialization(e.to_string()))?,
            )
        }

        async fn step_b_wrapper(&mut self, data: String) -> Result<serde_json::Value, FlowError> {
            let result = self.step_b(data).await?;
            Ok(
                serde_json::to_value(result)
                    .map_err(|e| FlowError::Serialization(e.to_string()))?,
            )
        }
    }

    impl Flow for ManualPipeline {
        fn metadata() -> FlowMetadata {
            FlowMetadata {
                start_methods: vec!["step_a".to_string()],
            }
        }
    }

    #[tokio::test]
    async fn test_manual_dispatch_start_method() {
        let mut pipeline = ManualPipeline {
            state: ManualState {
                value: String::new(),
                transformed: None,
            },
        };

        let result = pipeline
            .dispatch("step_a", serde_json::Value::Null)
            .await
            .expect("dispatch should succeed");
        assert_eq!(result, serde_json::json!("result_a"));
        assert_eq!(pipeline.state.value, "from_a");
    }

    #[tokio::test]
    async fn test_manual_dispatch_listen_method() {
        let mut pipeline = ManualPipeline {
            state: ManualState {
                value: String::new(),
                transformed: None,
            },
        };

        let result = pipeline
            .dispatch("step_b", serde_json::json!("hello"))
            .await
            .expect("dispatch should succeed");
        assert_eq!(result, serde_json::json!("got: hello"));
        assert_eq!(
            pipeline.state.transformed.as_deref(),
            Some("transformed: hello")
        );
    }

    // ── Proc-macro 验证: #[flow_impl] 生成正确的包装 ──

    use tavern_flow_macros::{flow_impl, listen, start, Flow};

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct MacroState {
        value: String,
    }

    #[derive(Flow)]
    struct MacroPipeline {
        state: MacroState,
    }

    #[flow_impl(crate = "crate")]
    impl MacroPipeline {
        #[start]
        async fn step_one(&mut self) -> Result<String, FlowError> {
            self.state.value = "done".to_string();
            Ok("result_one".to_string())
        }

        #[listen("step_one")]
        async fn step_two(&mut self, data: String) -> Result<String, FlowError> {
            Ok(format!("echo: {}", data))
        }
    }

    #[test]
    fn test_macro_flow_metadata() {
        let meta = MacroPipeline::metadata();
        assert_eq!(meta.start_methods, vec!["step_one"]);
    }

    #[tokio::test]
    async fn test_macro_dispatch_start() {
        let mut pipeline = MacroPipeline {
            state: MacroState {
                value: String::new(),
            },
        };
        let result = pipeline
            .dispatch("step_one", serde_json::Value::Null)
            .await
            .expect("dispatch should succeed");
        assert_eq!(result, serde_json::json!("result_one"));
        assert_eq!(pipeline.state.value, "done");
    }

    #[tokio::test]
    async fn test_macro_dispatch_listen() {
        let mut pipeline = MacroPipeline {
            state: MacroState {
                value: String::new(),
            },
        };
        let result = pipeline
            .dispatch("step_two", serde_json::json!("hello"))
            .await
            .expect("dispatch should succeed");
        assert_eq!(result, serde_json::json!("echo: hello"));
    }
}
