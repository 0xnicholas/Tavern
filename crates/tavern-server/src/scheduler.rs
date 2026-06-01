//! V0.3.6: Cron 定时调度器。
//!
//! 自实现最小化 5 字段 cron 解析器，支持 *、数字、*/N、逗号列表。
//! 后台每秒 tick，matches(now) + 分钟去重。

use std::sync::Arc;

use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::RwLock;

use tavern_comp::{WorkflowEngine, WorkflowRegistry};
use tavern_hero::TavernHero;

// ── Cron 解析器 ──

#[derive(Debug, Clone)]
struct CronExpr {
    minutes: CronField,
    hours: CronField,
    days_of_month: CronField,
    months: CronField,
    days_of_week: CronField,
}

#[derive(Debug, Clone)]
enum CronField {
    Any,
    Single(u8),
    List(Vec<u8>),
    Step { interval: u8, start: u8 },
}

impl CronField {
    fn matches(&self, value: u8) -> bool {
        match self {
            CronField::Any => true,
            CronField::Single(v) => *v == value,
            CronField::List(vs) => vs.contains(&value),
            CronField::Step { interval, start } => {
                if value < *start {
                    return false;
                }
                (value - start) % interval == 0
            }
        }
    }
}

impl CronExpr {
    fn parse(expr: &str) -> Result<Self, String> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!(
                "cron expression must have 5 fields, got {}",
                fields.len()
            ));
        }
        Ok(CronExpr {
            minutes: parse_field(fields[0], 0, 59)?,
            hours: parse_field(fields[1], 0, 23)?,
            days_of_month: parse_field(fields[2], 1, 31)?,
            months: parse_field(fields[3], 1, 12)?,
            days_of_week: parse_field(fields[4], 0, 6)?,
        })
    }

    fn matches(&self, dt: &DateTime<Utc>) -> bool {
        self.minutes.matches(dt.minute() as u8)
            && self.hours.matches(dt.hour() as u8)
            && self.days_of_month.matches(dt.day() as u8)
            && self.months.matches(dt.month() as u8)
            && self
                .days_of_week
                .matches(dt.weekday().num_days_from_sunday() as u8)
    }
}

fn parse_field(s: &str, min: u8, max: u8) -> Result<CronField, String> {
    if s == "*" {
        return Ok(CronField::Any);
    }
    // */N
    if let Some(rest) = s.strip_prefix("*/") {
        let interval: u8 = rest.parse().map_err(|_| format!("invalid step: {}", s))?;
        if interval == 0 {
            return Err("step interval must be > 0".into());
        }
        return Ok(CronField::Step { interval, start: 0 });
    }
    // N/M
    if let Some((start_str, interval_str)) = s.split_once('/') {
        let start: u8 = start_str
            .parse()
            .map_err(|_| format!("invalid start: {}", s))?;
        let interval: u8 = interval_str
            .parse()
            .map_err(|_| format!("invalid interval: {}", s))?;
        if interval == 0 {
            return Err("step interval must be > 0".into());
        }
        return Ok(CronField::Step { interval, start });
    }
    // Comma-separated list
    if s.contains(',') {
        let mut values = Vec::new();
        for part in s.split(',') {
            let v: u8 = part.parse().map_err(|_| format!("invalid value: {}", s))?;
            if v < min || v > max {
                return Err(format!("value {} out of range [{}, {}]", v, min, max));
            }
            values.push(v);
        }
        return Ok(CronField::List(values));
    }
    // Single number
    let v: u8 = s.parse().map_err(|_| format!("invalid value: {}", s))?;
    if v < min || v > max {
        return Err(format!("value {} out of range [{}, {}]", v, min, max));
    }
    Ok(CronField::Single(v))
}

// ── 调度器 ──

#[derive(Clone)]
struct ScheduleEntry {
    workflow_id: String,
    inputs: Value,
    cron: CronExpr,
    last_run_minute: Option<i64>,
    last_execution_id: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct ScheduleInfo {
    pub workflow_id: String,
    pub schedule: String,
    pub inputs: Value,
    pub last_run_at: Option<String>,
    pub last_execution_id: Option<String>,
}

pub struct Scheduler {
    hero: Arc<TavernHero>,
    store: Arc<dyn tavern_comp::EventStore>,
    registry: Arc<RwLock<WorkflowRegistry>>,
    state: Arc<RwLock<SchedulerState>>,
}

struct SchedulerState {
    entries: Vec<ScheduleEntry>,
}

impl Scheduler {
    pub fn new(
        hero: Arc<TavernHero>,
        store: Arc<dyn tavern_comp::EventStore>,
        registry: Arc<RwLock<WorkflowRegistry>>,
    ) -> Self {
        Self {
            hero,
            store,
            registry,
            state: Arc::new(RwLock::new(SchedulerState {
                entries: Vec::new(),
            })),
        }
    }

    /// 启动调度循环（应在 tokio::spawn 中调用）。
    pub async fn run(self: Arc<Self>) {
        let mut tick = 0u64;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            tick += 1;

            // 每 60 秒重新扫描 registry
            if tick % 60 == 0 {
                self.refresh_entries().await;
            }

            let now = Utc::now();
            let entries = {
                let state = self.state.read().await;
                state.entries.clone()
            };

            for entry in &entries {
                let current_minute = now.timestamp() / 60;
                if entry.last_run_minute == Some(current_minute) {
                    continue;
                }
                if !entry.cron.matches(&now) {
                    continue;
                }

                let workflow = {
                    let registry = self.registry.read().await;
                    registry.get(&entry.workflow_id).cloned()
                };

                if let Some(wf) = workflow {
                    let engine =
                        WorkflowEngine::new(self.hero.clone()).with_store(self.store.clone());
                    match engine.start(&wf, entry.inputs.clone()).await {
                        Ok(mut handle) => {
                            let exec_id = handle.id().to_string();
                            tokio::spawn(async move {
                                let _ = handle.await_completion().await;
                            });
                            let mut state = self.state.write().await;
                            if let Some(e) = state
                                .entries
                                .iter_mut()
                                .find(|e| e.workflow_id == entry.workflow_id)
                            {
                                e.last_run_minute = Some(current_minute);
                                e.last_execution_id = Some(exec_id);
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                workflow_id = %entry.workflow_id,
                                error = %e,
                                "scheduled workflow failed to start"
                            );
                        }
                    }
                }
            }
        }
    }

    async fn refresh_entries(&self) {
        let registry = self.registry.read().await;
        let workflows: Vec<_> = registry
            .list_all()
            .into_iter()
            .filter(|s| {
                registry
                    .get(&s.id)
                    .map(|w| w.schedule.is_some())
                    .unwrap_or(false)
            })
            .collect();

        let mut new_entries = Vec::new();
        for summary in workflows {
            if let Some(wf) = registry.get(&summary.id) {
                let expr = match CronExpr::parse(wf.schedule.as_ref().unwrap()) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::error!(
                            workflow_id = %wf.id,
                            error = %e,
                            "invalid cron expression, skipping"
                        );
                        continue;
                    }
                };
                new_entries.push(ScheduleEntry {
                    workflow_id: wf.id.clone(),
                    inputs: wf.schedule_inputs.clone(),
                    cron: expr,
                    last_run_minute: None,
                    last_execution_id: None,
                });
            }
        }
        drop(registry);

        let mut state = self.state.write().await;
        state.entries = new_entries;
        tracing::info!(count = state.entries.len(), "scheduler refreshed");
    }

    pub async fn list(&self) -> Vec<ScheduleInfo> {
        let state = self.state.read().await;
        let registry = self.registry.read().await;
        state
            .entries
            .iter()
            .map(|e| {
                let schedule = registry
                    .get(&e.workflow_id)
                    .and_then(|w| w.schedule.clone())
                    .unwrap_or_default();
                ScheduleInfo {
                    workflow_id: e.workflow_id.clone(),
                    schedule,
                    inputs: e.inputs.clone(),
                    last_run_at: e.last_run_minute.map(|m| {
                        let dt = DateTime::from_timestamp(m * 60, 0).unwrap_or_default();
                        dt.to_rfc3339()
                    }),
                    last_execution_id: e.last_execution_id.clone(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_cron_parse_any() {
        let expr = CronExpr::parse("* * * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2026, 5, 30, 12, 0, 0).unwrap();
        assert!(expr.matches(&dt));
    }

    #[test]
    fn test_cron_parse_specific_time() {
        let expr = CronExpr::parse("0 8 * * *").unwrap();
        let dt_match = Utc.with_ymd_and_hms(2026, 5, 30, 8, 0, 0).unwrap();
        let dt_nomatch = Utc.with_ymd_and_hms(2026, 5, 30, 8, 1, 0).unwrap();
        assert!(expr.matches(&dt_match));
        assert!(!expr.matches(&dt_nomatch));
    }

    #[test]
    fn test_cron_parse_step() {
        let expr = CronExpr::parse("*/15 * * * *").unwrap();
        assert!(expr.matches(&Utc.with_ymd_and_hms(2026, 5, 30, 12, 0, 0).unwrap()));
        assert!(expr.matches(&Utc.with_ymd_and_hms(2026, 5, 30, 12, 15, 0).unwrap()));
        assert!(expr.matches(&Utc.with_ymd_and_hms(2026, 5, 30, 12, 30, 0).unwrap()));
        assert!(expr.matches(&Utc.with_ymd_and_hms(2026, 5, 30, 12, 45, 0).unwrap()));
        assert!(!expr.matches(&Utc.with_ymd_and_hms(2026, 5, 30, 12, 7, 0).unwrap()));
    }

    #[test]
    fn test_cron_parse_list() {
        let expr = CronExpr::parse("0 0,12 * * *").unwrap();
        assert!(expr.matches(&Utc.with_ymd_and_hms(2026, 5, 30, 0, 0, 0).unwrap()));
        assert!(expr.matches(&Utc.with_ymd_and_hms(2026, 5, 30, 12, 0, 0).unwrap()));
        assert!(!expr.matches(&Utc.with_ymd_and_hms(2026, 5, 30, 6, 0, 0).unwrap()));
    }

    #[test]
    fn test_cron_parse_invalid() {
        assert!(CronExpr::parse("invalid").is_err());
        assert!(CronExpr::parse("0 0 0 0").is_err());
        assert!(CronExpr::parse("60 * * * *").is_err());
    }

    #[test]
    fn test_cron_field_step_with_start() {
        let field = parse_field("5/10", 0, 59).unwrap();
        assert!(field.matches(5));
        assert!(field.matches(15));
        assert!(field.matches(25));
        assert!(!field.matches(0));
        assert!(!field.matches(10));
    }
}
