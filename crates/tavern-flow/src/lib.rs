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

/// Flow 元数据。
#[derive(Debug, Clone)]
pub struct FlowMetadata {
    pub methods: Vec<MethodInfo>,
}

impl Default for FlowMetadata {
    fn default() -> Self {
        Self { methods: vec![] }
    }
}

/// 单个方法的元数据。
#[derive(Debug, Clone)]
pub struct MethodInfo {
    pub name: String,
    pub is_start: bool,
    /// 监听的方法名列表
    pub listens_to: Vec<String>,
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

/// FlowGraph — 方法依赖图。
pub(crate) struct FlowGraph {
    nodes: HashMap<String, MethodInfo>,
    /// 下游依赖: method_name -> [下游 methods]
    downstream: HashMap<String, Vec<String>>,
    /// 上游依赖计数: method_name -> 未完成的上游数
    in_degree: HashMap<String, usize>,
}

impl FlowGraph {
    pub fn from_metadata(meta: &FlowMetadata) -> Self {
        let nodes: HashMap<String, MethodInfo> = meta
            .methods
            .iter()
            .map(|m| (m.name.clone(), m.clone()))
            .collect();

        // Build downstream map and in-degree
        let mut downstream: HashMap<String, Vec<String>> = HashMap::new();
        let mut in_degree: HashMap<String, usize> = HashMap::new();

        for method in &meta.methods {
            downstream.entry(method.name.clone()).or_default();
            in_degree.entry(method.name.clone()).or_insert(0);

            for upstream in &method.listens_to {
                downstream
                    .entry(upstream.clone())
                    .or_default()
                    .push(method.name.clone());
                *in_degree.entry(method.name.clone()).or_insert(0) += 1;
            }
        }

        Self {
            nodes,
            downstream,
            in_degree,
        }
    }

    pub fn start_nodes(&self) -> Vec<String> {
        self.nodes
            .values()
            .filter(|n| n.is_start)
            .map(|n| n.name.clone())
            .collect()
    }

    /// 获取当 method_name 完成后的下游就绪方法。
    pub fn on_complete(&mut self, method_name: &str) -> Vec<String> {
        let mut ready = Vec::new();
        if let Some(deps) = self.downstream.get(method_name).cloned() {
            for dep in deps {
                if let Some(count) = self.in_degree.get_mut(&dep) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        ready.push(dep.clone());
                    }
                }
            }
        }
        ready
    }

    /// 获取方法的上游输入源（第一个 listens_to）。
    pub fn upstream_for(&self, method_name: &str) -> Option<String> {
        self.nodes
            .get(method_name)
            .and_then(|m| m.listens_to.first().cloned())
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

    /// 执行完整 flow（事件循环，顺序执行）。
    pub async fn execute(
        &mut self,
        _inputs: serde_json::Value,
    ) -> Result<serde_json::Value, FlowError> {
        let starts = self.graph.start_nodes();
        if starts.is_empty() {
            return Err(FlowError::Other("no start methods found".to_string()));
        }

        let mut pending: std::collections::VecDeque<String> = starts.into();
        let mut outputs: HashMap<String, serde_json::Value> = HashMap::new();
        let mut last_output: Option<serde_json::Value> = None;

        while let Some(method_name) = pending.pop_front() {
            // Determine input: for start methods, use Null; for listeners, use upstream output
            let input = self
                .graph
                .upstream_for(&method_name)
                .and_then(|up| outputs.get(&up).cloned())
                .unwrap_or(serde_json::Value::Null);

            match self.flow.dispatch(&method_name, input).await {
                Ok(output) => {
                    last_output = Some(output.clone());
                    outputs.insert(method_name.clone(), output);

                    let next = self.graph.on_complete(&method_name);
                    for n in next {
                        pending.push_back(n);
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(last_output.unwrap_or(serde_json::Value::Null))
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
        assert!(meta.methods.is_empty());
    }

    /// 测试 FlowGraph 从 metadata 构建。
    #[test]
    fn test_flow_graph_from_metadata() {
        let meta = FlowMetadata {
            methods: vec![
                MethodInfo {
                    name: "step_a".to_string(),
                    is_start: true,
                    listens_to: vec![],
                },
                MethodInfo {
                    name: "step_b".to_string(),
                    is_start: false,
                    listens_to: vec!["step_a".to_string()],
                },
            ],
        };
        let mut graph = FlowGraph::from_metadata(&meta);
        assert_eq!(graph.start_nodes(), vec!["step_a"]);
        // step_b should become ready after step_a completes
        let next = graph.on_complete("step_a");
        assert_eq!(next, vec!["step_b"]);
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
                methods: vec![
                    MethodInfo {
                        name: "step_a".to_string(),
                        is_start: true,
                        listens_to: vec![],
                    },
                    MethodInfo {
                        name: "step_b".to_string(),
                        is_start: false,
                        listens_to: vec!["step_a".to_string()],
                    },
                ],
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
        assert_eq!(meta.methods.len(), 2);
        let step_one = &meta.methods[0];
        assert_eq!(step_one.name, "step_one");
        assert!(step_one.is_start);
        let step_two = &meta.methods[1];
        assert_eq!(step_two.name, "step_two");
        assert_eq!(step_two.listens_to, vec!["step_one"]);
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

    /// FlowEngine 完整事件循环：start → listen chain。
    #[tokio::test]
    async fn test_flow_engine_event_loop() {
        let mut pipeline = MacroPipeline {
            state: MacroState {
                value: String::new(),
            },
        };

        let mut engine = FlowEngine::new(pipeline);
        let result = engine
            .execute(serde_json::Value::Null)
            .await
            .expect("event loop should complete");
        assert_eq!(result, serde_json::json!("echo: result_one"));
    }
}
