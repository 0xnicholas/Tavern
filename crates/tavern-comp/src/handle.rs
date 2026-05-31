use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::error::CompError;
use crate::event::WorkflowEvent;
use crate::instance::InstanceState;
use crate::store::EventStore;
use crate::workflow::WorkflowResult;

#[derive(Debug)]
pub struct ExecutionHandle {
    pub id: String,
    pub signal_tx: mpsc::Sender<WorkflowEvent>,
    pub interpreter_handle: tokio::task::JoinHandle<Result<(), CompError>>,
    pub completion_rx: Option<tokio::sync::oneshot::Receiver<Result<WorkflowResult, CompError>>>,
}

impl ExecutionHandle {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 向运行中的实例发送外部信号
    pub async fn signal(&self, name: &str, payload: Value) -> Result<(), CompError> {
        self.signal_tx
            .send(WorkflowEvent::SignalReceived {
                signal_name: name.to_string(),
                payload,
                received_at: Utc::now(),
                action: None,
                reviewer: None,
            })
            .await
            .map_err(|_| CompError::InstanceClosed {
                id: self.id.clone(),
            })
    }

    /// 阻塞等待实例完成（V1 兼容层使用）
    /// 只能调用一次
    pub async fn await_completion(&mut self) -> Result<WorkflowResult, CompError> {
        if let Some(rx) = self.completion_rx.take() {
            rx.await
                .map_err(|_| CompError::Internal("completion channel closed".into()))?
        } else {
            Err(CompError::Internal(
                "await_completion already called".into(),
            ))
        }
    }

    /// 查询实例当前状态（通过 EventStore 重放重建）
    pub async fn query_state(&self, store: &dyn EventStore) -> Result<InstanceState, CompError> {
        let events = store.read_stream(&self.id).await?;
        let mut state = InstanceState {
            id: self.id.clone(),
            ..Default::default()
        };
        for event in events {
            state.apply(&event)?;
        }
        Ok(state)
    }

    /// 优雅关闭解释器（发送 Cancel 信号）
    pub async fn cancel(&self) -> Result<(), CompError> {
        self.signal_tx
            .send(WorkflowEvent::CancelRequested {
                requested_at: Utc::now(),
            })
            .await
            .map_err(|_| CompError::InstanceClosed {
                id: self.id.clone(),
            })
    }
}
