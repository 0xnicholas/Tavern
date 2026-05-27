//! tavern-flow — Rust 原生方法级事件驱动编排，类似 CrewAI Flow。
//!
//! ## 使用示例
//!
//! ```ignore
//! use tavern_flow::{Flow, flow_impl, start, listen, FlowEngine};
//!
//! #[derive(Flow)]
//! struct MyPipeline { state: MyState }
//!
//! #[flow_impl(crate = "tavern_flow")]
//! impl MyPipeline {
//!     #[start]
//!     async fn step_a(&mut self) -> Result<String, FlowError> { ... }
//!
//!     #[listen("step_a")]
//!     async fn step_b(&mut self, data: String) -> Result<String, FlowError> { ... }
//! }
//!
//! let mut engine = FlowEngine::new(pipeline);
//! let result = engine.execute(json!({})).await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

pub use tavern_flow_macros::{flow_impl, listen, router, start, Flow};

pub mod event;
pub mod registry;
pub use registry::{FlowFactory, FlowRegistry, StartableFlow};

/// Flow 元数据。
#[derive(Debug, Clone, Default)]
pub struct FlowMetadata {
    pub methods: Vec<MethodInfo>,
}

/// 单个方法的元数据。
#[derive(Debug, Clone)]
pub struct MethodInfo {
    pub name: String,
    pub is_start: bool,
    pub is_router: bool,
    pub router_for: Option<String>,
    /// 监听类型
    pub listen_type: ListenType,
}

#[derive(Debug, Clone)]
pub enum ListenType {
    /// 监听单个方法名或 label
    Single(String),
    /// 任一完成即触发
    Or(Vec<String>),
    /// 全部完成才触发
    And(Vec<String>),
}

impl ListenType {
    /// 获取所有上游名称。
    pub fn upstreams(&self) -> Vec<String> {
        match self {
            ListenType::Single(s) => vec![s.clone()],
            ListenType::Or(v) | ListenType::And(v) => v.clone(),
        }
    }

    /// 是否为 OR 类型。
    pub fn is_or(&self) -> bool {
        matches!(self, ListenType::Or(_))
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

/// FlowGraph — 方法依赖图。
pub(crate) struct FlowGraph {
    nodes: HashMap<String, MethodInfo>,
    /// 下游依赖: method_name -> [下游 methods]
    downstream: HashMap<String, Vec<String>>,
    /// 上游依赖计数: method_name -> 未完成的上游数
    in_degree: HashMap<String, usize>,
    /// 路由器映射: upstream_method_name -> router_method_name
    routers: HashMap<String, String>,
    /// 标签监听: label_name -> [监听该 label 的方法名]
    label_listeners: HashMap<String, Vec<String>>,
}

impl FlowGraph {
    pub fn from_metadata(meta: &FlowMetadata) -> Self {
        let nodes: HashMap<String, MethodInfo> = meta
            .methods
            .iter()
            .map(|m| (m.name.clone(), m.clone()))
            .collect();

        let mut downstream: HashMap<String, Vec<String>> = HashMap::new();
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut routers: HashMap<String, String> = HashMap::new();
        let mut label_listeners: HashMap<String, Vec<String>> = HashMap::new();

        for method in &meta.methods {
            downstream.entry(method.name.clone()).or_default();
            in_degree.entry(method.name.clone()).or_insert(0);

            if method.is_router {
                if let Some(ref upstream) = method.router_for {
                    routers.insert(upstream.clone(), method.name.clone());
                    // Router depends on its upstream
                    *in_degree.entry(method.name.clone()).or_insert(0) += 1;
                    downstream
                        .entry(upstream.clone())
                        .or_default()
                        .push(method.name.clone());
                }
            } else {
                for upstream_name in method.listen_type.upstreams() {
                    if nodes.contains_key(&upstream_name) {
                        if method.listen_type.is_or() {
                            in_degree.entry(method.name.clone()).or_insert(1);
                        } else {
                            *in_degree.entry(method.name.clone()).or_insert(0) += 1;
                        }
                        downstream
                            .entry(upstream_name.clone())
                            .or_default()
                            .push(method.name.clone());
                    } else {
                        label_listeners
                            .entry(upstream_name.clone())
                            .or_default()
                            .push(method.name.clone());
                    }
                }
            }
        }

        Self {
            nodes,
            downstream,
            in_degree,
            routers,
            label_listeners,
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
        if let Some(deps) = self.downstream.get(method_name) {
            let deps: Vec<String> = deps.clone();
            for dep in deps {
                if let Some(count) = self.in_degree.get_mut(&dep) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        ready.push(dep.clone());
                        // Remove from in_degree to prevent re-triggering (important for OR)
                        self.in_degree.remove(&dep);
                    }
                }
            }
        }
        ready
    }

    /// 获取方法的上游输入源（第一个 listens_to 或 router_for）。
    pub fn upstream_for(&self, method_name: &str) -> Option<String> {
        self.nodes.get(method_name).and_then(|m| {
            m.listen_type
                .upstreams()
                .first()
                .cloned()
                .or_else(|| m.router_for.clone())
        })
    }

    /// 获取上游方法的 router（如果有）。
    pub fn router_for(&self, upstream: &str) -> Option<&str> {
        self.routers.get(upstream).map(|s| s.as_str())
    }

    /// 获取监听指定 label 的方法列表。
    pub fn listeners_for_label(&self, label: &str) -> Vec<String> {
        self.label_listeners.get(label).cloned().unwrap_or_default()
    }
}

/// FlowEngine — 方法图事件循环执行引擎。
pub struct FlowEngine<F> {
    flow: F,
    graph: FlowGraph,
    store: Option<Arc<dyn tavern_comp::EventStore>>,
    max_concurrency: usize,
}

impl<F: Flow + FlowDispatch + Send + 'static> FlowEngine<F> {
    pub fn new(flow: F) -> Self {
        let meta = F::metadata();
        let graph = FlowGraph::from_metadata(&meta);
        Self {
            flow,
            graph,
            store: None,
            max_concurrency: 1,
        }
    }

    pub fn with_store(mut self, store: Arc<dyn tavern_comp::EventStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    /// 异步启动 flow（非阻塞），返回 FlowHandle 用于等待结果。
    pub fn start_async(self) -> (FlowHandle, FlowHandleRef) {
        let flow_id = uuid::Uuid::new_v4().to_string();
        let status = Arc::new(std::sync::atomic::AtomicU8::new(1)); // running
        let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let (completion_tx, completion_rx) =
            tokio::sync::oneshot::channel::<Result<serde_json::Value, FlowError>>();

        let ref_handle = FlowHandleRef {
            flow_id: flow_id.clone(),
            flow_name: "unnamed".to_string(),
            status: status.clone(),
            started_at: chrono::Utc::now(),
        };

        let max_concurrency = self.max_concurrency;
        let status_clone = status.clone();
        tokio::spawn(async move {
            let result = if max_concurrency > 1 {
                Self::execute_inner_parallel(self.flow, self.graph, self.store, max_concurrency)
                    .await
            } else {
                let mut engine = Self {
                    flow: self.flow,
                    graph: self.graph,
                    store: self.store,
                    max_concurrency: 1,
                };
                engine.execute_inner().await
            };
            status_clone.store(
                match &result {
                    Ok(_) => 2,
                    Err(_) => 3,
                },
                std::sync::atomic::Ordering::Relaxed,
            );
            let _ = completion_tx.send(result);
        });

        let handle = FlowHandle {
            flow_id,
            completion_rx: Some(completion_rx),
            cancel_tx: Some(cancel_tx),
            status,
        };
        (handle, ref_handle)
    }

    /// 同步执行（内部调用 start + await）。
    pub async fn execute(
        &mut self,
        _inputs: serde_json::Value,
    ) -> Result<serde_json::Value, FlowError> {
        self.execute_inner().await
    }

    /// 内部事件循环实现。
    async fn execute_inner(&mut self) -> Result<serde_json::Value, FlowError> {
        let starts = self.graph.start_nodes();
        if starts.is_empty() {
            return Err(FlowError::Other("no start methods found".to_string()));
        }

        // Emit FlowStarted event
        self.persist_event(
            "flow",
            &crate::event::FlowEvent::FlowStarted {
                flow_name: "unnamed".to_string(),
                inputs: serde_json::Value::Null,
                started_at: chrono::Utc::now(),
            },
        );

        let mut pending: std::collections::VecDeque<String> = starts.into();
        let mut outputs: HashMap<String, serde_json::Value> = HashMap::new();
        let mut last_output: Option<serde_json::Value> = None;

        while let Some(method_name) = pending.pop_front() {
            let input = self
                .graph
                .upstream_for(&method_name)
                .and_then(|up| outputs.get(&up).cloned())
                .unwrap_or(serde_json::Value::Null);

            match self.flow.dispatch(&method_name, input).await {
                Ok(output) => {
                    last_output = Some(output.clone());
                    outputs.insert(method_name.clone(), output.clone());
                    self.persist_event(
                        "flow",
                        &crate::event::FlowEvent::MethodCompleted {
                            method_name: method_name.clone(),
                            output: output.clone(),
                            completed_at: chrono::Utc::now(),
                        },
                    );

                    // Check if this method has a router
                    if let Some(router_name) =
                        self.graph.router_for(&method_name).map(|s| s.to_string())
                    {
                        // Execute router to determine the label
                        let router_input = output.clone();
                        match self.flow.dispatch(&router_name, router_input.clone()).await {
                            Ok(label_val) => {
                                outputs.insert(router_name.clone(), label_val.clone());

                                // Handle single label (String) or multi-label (Array)
                                let labels: Vec<String> = match &label_val {
                                    serde_json::Value::String(s) => vec![s.clone()],
                                    serde_json::Value::Array(arr) => arr
                                        .iter()
                                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                        .collect(),
                                    _ => vec![],
                                };

                                for label in &labels {
                                    // Label listeners receive the router's input
                                    outputs.insert(label.clone(), router_input.clone());
                                    // Trigger label listeners
                                    let listeners = self.graph.listeners_for_label(label);
                                    for n in listeners {
                                        pending.push_back(n);
                                    }
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        // Normal flow: trigger downstream methods
                        let next = self.graph.on_complete(&method_name);
                        for n in next {
                            pending.push_back(n);
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(last_output.unwrap_or(serde_json::Value::Null))
    }

    fn persist_event(&self, flow_id: &str, event: &crate::event::FlowEvent) {
        if let Some(ref store) = self.store {
            let store = store.clone();
            let event = event.clone();
            let flow_id = flow_id.to_string();
            tokio::spawn(async move {
                let _ = store.append(&flow_id, event.to_workflow_event()).await;
            });
        }
    }

    /// 并行执行：tokio::sync::Mutex 串行化 dispatch，JoinSet + Semaphore 控制并发。
    async fn execute_inner_parallel(
        flow: F,
        mut graph: FlowGraph,
        _store: Option<Arc<dyn tavern_comp::EventStore>>,
        max_concurrency: usize,
    ) -> Result<serde_json::Value, FlowError> {
        use tokio::sync::Semaphore;
        use tokio::task::JoinSet;

        let inner = Arc::new(tokio::sync::Mutex::new(flow));
        let semaphore = Arc::new(Semaphore::new(max_concurrency));
        let outputs: Arc<tokio::sync::Mutex<HashMap<String, serde_json::Value>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let last_output: Arc<tokio::sync::Mutex<Option<serde_json::Value>>> =
            Arc::new(tokio::sync::Mutex::new(None));

        let mut pending: std::collections::VecDeque<String> = graph.start_nodes().into();
        if pending.is_empty() {
            return Err(FlowError::Other("no start methods found".into()));
        }

        while !pending.is_empty() {
            let mut join_set = JoinSet::new();

            // Spawn current batch
            for name in pending.drain(..) {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let inner = inner.clone();
                let outputs = outputs.clone();
                let last_output = last_output.clone();
                let name = name.clone();
                join_set.spawn(async move {
                    let _permit = permit;
                    let input = {
                        let out = outputs.lock().await;
                        // Find upstream output
                        out.values()
                            .last()
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    };
                    let result = {
                        let mut guard = inner.lock().await;
                        guard.dispatch(&name, input).await
                    };
                    match result {
                        Ok(val) => {
                            let mut lo = last_output.lock().await;
                            *lo = Some(val.clone());
                            let mut out = outputs.lock().await;
                            out.insert(name.clone(), val);
                            Ok(name)
                        }
                        Err(e) => Err((name, e)),
                    }
                });
            }

            // Collect results, compute next batch
            let mut next_batch = Vec::new();
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(name)) => {
                        let ready = graph.on_complete(&name);
                        next_batch.extend(ready);
                    }
                    Ok(Err((_name, e))) => return Err(e),
                    Err(e) => return Err(FlowError::Other(format!("task panicked: {}", e))),
                }
            }

            pending = next_batch.into();
        }

        let lo = last_output.lock().await;
        Ok(lo.clone().unwrap_or(serde_json::Value::Null))
    }
}

/// FlowHandle — 异步执行句柄（唯一，不可 Clone）。
pub struct FlowHandle {
    pub flow_id: String,
    completion_rx: Option<tokio::sync::oneshot::Receiver<Result<serde_json::Value, FlowError>>>,
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    status: Arc<std::sync::atomic::AtomicU8>,
}

impl FlowHandle {
    /// 阻塞等待 flow 完成。
    pub async fn await_completion(&mut self) -> Result<serde_json::Value, FlowError> {
        let rx = self
            .completion_rx
            .take()
            .ok_or_else(|| FlowError::Other("already awaited".to_string()))?;
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(FlowError::Other("flow task panicked".to_string())),
        }
    }

    /// 取消 flow 执行。
    pub fn cancel(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// FlowHandleRef — 轻量引用（Clone + Send），用于状态查询。
#[derive(Clone, Debug)]
pub struct FlowHandleRef {
    pub flow_id: String,
    pub flow_name: String,
    pub status: Arc<std::sync::atomic::AtomicU8>,
    pub started_at: chrono::DateTime<chrono::Utc>,
}

impl FlowHandleRef {
    pub fn status_str(&self) -> &'static str {
        match self.status.load(std::sync::atomic::Ordering::Relaxed) {
            0 => "pending",
            1 => "running",
            2 => "completed",
            3 => "failed",
            _ => "unknown",
        }
    }
}

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
                    is_router: false,
                    router_for: None,
                    listen_type: ListenType::Single(String::new()),
                },
                MethodInfo {
                    name: "step_b".to_string(),
                    is_start: false,
                    is_router: false,
                    router_for: None,
                    listen_type: ListenType::Single("step_a".to_string()),
                },
            ],
        };
        let mut graph = FlowGraph::from_metadata(&meta);
        assert_eq!(graph.start_nodes(), vec!["step_a"]);
        let next = graph.on_complete("step_a");
        assert_eq!(next, vec!["step_b"]);
        // step_b should not trigger again
        let next2 = graph.on_complete("step_a");
        assert!(next2.is_empty());
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
                        is_router: false,
                        router_for: None,
                        listen_type: ListenType::Single(String::new()),
                    },
                    MethodInfo {
                        name: "step_b".to_string(),
                        is_start: false,
                        is_router: false,
                        router_for: None,
                        listen_type: ListenType::Single("step_a".to_string()),
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
        assert_eq!(step_two.listen_type.upstreams(), vec!["step_one"]);
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

    /// FlowEngine 事件循环 + router 条件分支。
    #[tokio::test]
    async fn test_flow_engine_with_router() {
        use tavern_flow_macros::router;

        #[derive(Flow)]
        struct RouterPipeline {
            state: RouterState,
        }

        #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
        struct RouterState {
            approved: bool,
            result: Option<String>,
        }

        #[flow_impl(crate = "crate")]
        impl RouterPipeline {
            #[start]
            async fn process(&mut self) -> Result<String, FlowError> {
                Ok("draft_content".to_string())
            }

            #[router("process")]
            async fn gate(&mut self, content: String) -> String {
                if content.len() > 5 {
                    self.state.approved = true;
                    "approved".to_string()
                } else {
                    "rejected".to_string()
                }
            }

            #[listen("approved")]
            async fn on_approved(&mut self, data: String) -> Result<String, FlowError> {
                self.state.result = Some(format!("published: {}", data));
                Ok(format!("OK: {}", data))
            }

            #[listen("rejected")]
            async fn on_rejected(&mut self, data: String) -> Result<String, FlowError> {
                self.state.result = Some(format!("returned: {}", data));
                Ok(format!("NO: {}", data))
            }
        }

        let pipeline = RouterPipeline {
            state: RouterState::default(),
        };

        let mut engine = FlowEngine::new(pipeline);
        let result = engine
            .execute(serde_json::Value::Null)
            .await
            .expect("router flow should complete");

        assert!(engine.flow.state.approved);
        assert_eq!(
            engine.flow.state.result.as_deref(),
            Some("published: draft_content")
        );
        assert_eq!(result, serde_json::json!("OK: draft_content"));
    }

    /// FlowEngine: OR combinator — 任一上游完成即触发。
    #[tokio::test]
    async fn test_flow_or_combinator() {
        #[derive(Flow)]
        struct OrPipeline {
            state: OrState,
        }

        #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
        struct OrState {
            executed: Vec<String>,
        }

        #[flow_impl(crate = "crate")]
        impl OrPipeline {
            #[start]
            async fn source_a(&mut self) -> Result<String, FlowError> {
                self.state.executed.push("a".into());
                Ok("result_a".to_string())
            }

            #[start]
            async fn source_b(&mut self) -> Result<String, FlowError> {
                self.state.executed.push("b".into());
                Ok("result_b".to_string())
            }

            #[listen(or("source_a", "source_b"))]
            async fn consumer(&mut self, data: String) -> Result<String, FlowError> {
                self.state.executed.push(format!("got:{}", data));
                Ok(format!("final:{}", data))
            }
        }

        let pipeline = OrPipeline {
            state: OrState::default(),
        };

        let mut engine = FlowEngine::new(pipeline);
        engine
            .execute(serde_json::Value::Null)
            .await
            .expect("or flow should complete");

        // consumer should execute exactly once after first source completes
        let got_count = engine
            .flow
            .state
            .executed
            .iter()
            .filter(|s| s.starts_with("got:"))
            .count();
        assert_eq!(got_count, 1, "consumer should execute exactly once");
    }

    /// FlowEngine: AND combinator — 全部上游完成才触发。
    #[tokio::test]
    async fn test_flow_and_combinator() {
        #[derive(Flow)]
        struct AndPipeline {
            state: AndState,
        }

        #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
        struct AndState {
            ready: bool,
        }

        #[flow_impl(crate = "crate")]
        impl AndPipeline {
            #[start]
            async fn first(&mut self) -> Result<String, FlowError> {
                Ok("first".to_string())
            }

            #[start]
            async fn second(&mut self) -> Result<String, FlowError> {
                Ok("second".to_string())
            }

            #[listen(and("first", "second"))]
            async fn after_both(&mut self) -> Result<String, FlowError> {
                self.state.ready = true;
                Ok("done".to_string())
            }
        }

        let pipeline = AndPipeline {
            state: AndState::default(),
        };

        let mut engine = FlowEngine::new(pipeline);
        engine
            .execute(serde_json::Value::Null)
            .await
            .expect("and flow should complete");

        assert!(engine.flow.state.ready, "after_both should have executed");
    }

    /// FlowHandle: 异步启动 + await_completion。
    #[tokio::test]
    async fn test_flow_handle_start_and_await() {
        let pipeline = MacroPipeline {
            state: MacroState {
                value: String::new(),
            },
        };

        let engine = FlowEngine::new(pipeline);
        let (mut handle, _ref) = engine.start_async();

        let result = handle
            .await_completion()
            .await
            .expect("async flow should complete");
        assert_eq!(result, serde_json::json!("echo: result_one"));
    }

    /// 验证并行路径被启用（max_concurrency > 1 走 execute_inner_parallel）。
    #[tokio::test]
    async fn test_parallel_path_enabled() {
        #[derive(Flow)]
        struct SimplePipeline;

        #[flow_impl(crate = "crate")]
        impl SimplePipeline {
            #[start]
            async fn step(&mut self) -> Result<String, FlowError> {
                Ok("done".into())
            }
        }

        let engine = FlowEngine::new(SimplePipeline).with_max_concurrency(2);
        let (mut handle, _ref) = engine.start_async();
        let result = handle.await_completion().await;
        assert!(result.is_ok(), "parallel path should complete successfully");
        assert_eq!(result.unwrap(), serde_json::json!("done"));
    }
}
