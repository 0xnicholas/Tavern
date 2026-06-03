use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::event::WorkflowEvent;

pub struct TimerRegistry {
    tx: mpsc::Sender<WorkflowEvent>,
}

impl TimerRegistry {
    pub fn new(tx: mpsc::Sender<WorkflowEvent>) -> Self {
        Self { tx }
    }

    /// 注册一个定时器，到期后发送 TimerFired 事件
    pub async fn register(&self, timer_id: String, wake_at: DateTime<Utc>) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let now = Utc::now();
            if wake_at > now
                && let Ok(duration) = (wake_at - now).to_std()
            {
                tokio::time::sleep(duration).await;
            }
            let event = WorkflowEvent::TimerFired { timer_id };
            if let Err(e) = tx.send(event).await {
                tracing::error!(error = %e, "interpreter closed, timer event dropped");
            }
        });
    }
}
