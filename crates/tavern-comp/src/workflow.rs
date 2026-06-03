use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::error::CompError;

pub use tavern_core::{ManagerConfig, Plan, PlanningConfig, Process, is_valid_id};

/// V0.3.2: 审批超时默认动作。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SignalTimeoutAction {
    Fail,
    Reject,
}

/// V0.3.5: Webhook 回调配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub retries: Option<u64>,
    #[serde(default)]
    pub retry_delay: Option<u64>,
}

/// 工作流的完整配置定义。
///
/// 自定义反序列化以支持 `process: hierarchical` + `manager:` YAML 双 key 语法。
#[derive(Debug, Clone)]
pub struct Workflow {
    /// 全局唯一标识符
    /// 约束：^[a-zA-Z0-9_-]+$，长度 1-64
    pub id: String,

    /// 可读名称
    pub name: String,

    /// 描述（可选）
    pub description: Option<String>,

    /// 执行步骤列表
    pub steps: Vec<Step>,

    /// 外部输入参数定义
    /// 默认：空列表
    pub inputs: Vec<InputDef>,

    /// 工作流最终输出定义
    /// 默认：空列表（REST 响应中 outputs 字段为空对象 {}）
    pub outputs: Vec<OutputDef>,

    /// 执行策略
    /// YAML 缺失时默认 Sequential
    pub process: Process,

    /// Planning 配置
    /// YAML 缺失时默认 None（不启用 Planning）
    pub planning: Option<PlanningConfig>,

    /// V0.3.5: Webhook 回调配置
    pub webhook: Option<WebhookConfig>,

    /// V0.3.6: Cron 定时调度表达式
    pub schedule: Option<String>,

    /// V0.3.6: 定时触发时传入的默认 inputs
    pub schedule_inputs: Value,
}

impl Serialize for Workflow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Workflow", 8)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("description", &self.description)?;
        state.serialize_field("steps", &self.steps)?;
        state.serialize_field("inputs", &self.inputs)?;
        state.serialize_field("outputs", &self.outputs)?;
        match &self.process {
            Process::Sequential => {
                state.serialize_field("process", "sequential")?;
            }
            Process::Hierarchical(cfg) => {
                state.serialize_field("process", "hierarchical")?;
                state.serialize_field("manager", cfg)?;
            }
        }
        state.serialize_field("planning", &self.planning)?;
        state.serialize_field("webhook", &self.webhook)?;
        state.serialize_field("schedule", &self.schedule)?;
        state.serialize_field("schedule_inputs", &self.schedule_inputs)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Workflow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            id: String,
            name: String,
            #[serde(default)]
            description: Option<String>,
            steps: Vec<Step>,
            #[serde(default)]
            inputs: Vec<InputDef>,
            #[serde(default)]
            outputs: Vec<OutputDef>,
            #[serde(default)]
            process: Option<String>,
            #[serde(default)]
            manager: Option<ManagerConfig>,
            #[serde(default)]
            planning: Option<PlanningConfig>,
            #[serde(default)]
            webhook: Option<WebhookConfig>,
            #[serde(default)]
            schedule: Option<String>,
            #[serde(default)]
            schedule_inputs: Value,
        }

        let h = Helper::deserialize(deserializer)?;
        let process = match h.process.as_deref() {
            None | Some("sequential") => Process::Sequential,
            Some("hierarchical") => {
                let cfg = h.manager.ok_or_else(|| {
                    serde::de::Error::custom(
                        "process is 'hierarchical' but 'manager' section is missing",
                    )
                })?;
                Process::Hierarchical(cfg)
            }
            Some(other) => {
                return Err(serde::de::Error::custom(format!(
                    "unknown process type: '{}', expected 'sequential' or 'hierarchical'",
                    other
                )));
            }
        };

        Ok(Workflow {
            id: h.id,
            name: h.name,
            description: h.description,
            steps: h.steps,
            inputs: h.inputs,
            outputs: h.outputs,
            process,
            planning: h.planning,
            webhook: h.webhook,
            schedule: h.schedule,
            schedule_inputs: h.schedule_inputs,
        })
    }
}

impl Workflow {
    /// 从 YAML 文件加载。
    pub fn from_yaml(path: &Path) -> Result<Self, CompError> {
        let content = std::fs::read_to_string(path)?;
        Self::from_yaml_str(&content).map_err(|e| CompError::ConfigParse {
            path: path.display().to_string(),
            reason: e.to_string(),
        })
    }

    /// 从 YAML 字符串加载。
    pub fn from_yaml_str(content: &str) -> Result<Self, CompError> {
        serde_yaml::from_str(content).map_err(|e| CompError::ConfigParse {
            path: "<string>".to_string(),
            reason: e.to_string(),
        })
    }

    /// 静态校验（不依赖 Hero）。
    /// 检查：Workflow.id 格式、Step.id 唯一性、依赖存在性、DAG 无环、output_key 唯一性、资源上限。
    /// Hierarchical 模式跳过 DAG 校验，使用独立的步骤数限制。
    pub fn validate_static(&self) -> Result<(), CompError> {
        const MAX_STEPS: usize = 100;
        const MAX_HIERARCHICAL_TASKS: usize = 50;
        const MAX_INPUTS: usize = 50;
        const MAX_OUTPUTS: usize = 50;
        const MAX_STEP_TASK_LEN: usize = 10_000;
        const MAX_STEP_ID_LEN: usize = 64;
        const MAX_AGENT_ID_LEN: usize = 64;
        const MAX_OUTPUT_KEY_LEN: usize = 64;

        // 1. Workflow.id 格式校验
        if !is_valid_id(&self.id) {
            return Err(CompError::ConfigParse {
                path: "<workflow>".to_string(),
                reason: format!("invalid workflow id '{}'", self.id),
            });
        }

        // 2. steps 数量限制
        if self.steps.is_empty() {
            return Err(CompError::ConfigParse {
                path: "<workflow>".to_string(),
                reason: "workflow must have at least one step".to_string(),
            });
        }

        // Process-aware step count limit
        let max_allowed = match &self.process {
            Process::Sequential => MAX_STEPS,
            Process::Hierarchical(_) => MAX_HIERARCHICAL_TASKS,
        };
        if self.steps.len() > max_allowed {
            return Err(CompError::ConfigParse {
                path: "<workflow>".to_string(),
                reason: format!("workflow steps exceed limit of {}", max_allowed),
            });
        }

        // 3. inputs / outputs 数量限制 + name 校验
        if self.inputs.len() > MAX_INPUTS {
            return Err(CompError::ConfigParse {
                path: "<workflow>".to_string(),
                reason: format!("workflow inputs exceed limit of {}", MAX_INPUTS),
            });
        }
        for input in &self.inputs {
            if input.name.trim().is_empty() {
                return Err(CompError::ConfigParse {
                    path: "<workflow>".to_string(),
                    reason: "input name must not be empty".to_string(),
                });
            }
        }
        if self.outputs.len() > MAX_OUTPUTS {
            return Err(CompError::ConfigParse {
                path: "<workflow>".to_string(),
                reason: format!("workflow outputs exceed limit of {}", MAX_OUTPUTS),
            });
        }
        for output in &self.outputs {
            if output.name.trim().is_empty() {
                return Err(CompError::ConfigParse {
                    path: "<workflow>".to_string(),
                    reason: "output name must not be empty".to_string(),
                });
            }
        }

        // 4. Step.id 唯一性 + 字段长度限制
        let mut step_ids = std::collections::HashSet::new();
        for step in &self.steps {
            if step.id.len() > MAX_STEP_ID_LEN {
                return Err(CompError::ConfigParse {
                    path: "<workflow>".to_string(),
                    reason: format!(
                        "step id '{}' exceeds max length of {}",
                        step.id, MAX_STEP_ID_LEN
                    ),
                });
            }
            if step.agent_id.len() > MAX_AGENT_ID_LEN {
                return Err(CompError::ConfigParse {
                    path: "<workflow>".to_string(),
                    reason: format!(
                        "agent_id for step '{}' exceeds max length of {}",
                        step.id, MAX_AGENT_ID_LEN
                    ),
                });
            }
            if step.task.len() > MAX_STEP_TASK_LEN {
                return Err(CompError::ConfigParse {
                    path: "<workflow>".to_string(),
                    reason: format!(
                        "task for step '{}' exceeds max length of {}",
                        step.id, MAX_STEP_TASK_LEN
                    ),
                });
            }
            if !step_ids.insert(&step.id) {
                return Err(CompError::DuplicateStep {
                    id: step.id.clone(),
                });
            }
        }

        // 5. depends_on 存在性 + DAG 无环（Sequential 模式）或跳过（Hierarchical 模式）
        match &self.process {
            Process::Sequential => {
                crate::validator::validate_dag(self)?;
            }
            Process::Hierarchical(cfg) => {
                // 跳过 DAG 校验，仅校验 manager agent_id 格式
                if !is_valid_id(&cfg.agent_id) {
                    return Err(CompError::ConfigParse {
                        path: "<workflow>".to_string(),
                        reason: format!("invalid manager agent_id '{}'", cfg.agent_id),
                    });
                }
                // depends_on 存在性仍需校验（YAML 可能拼错 step id）
                let step_ids: std::collections::HashSet<&str> =
                    self.steps.iter().map(|s| s.id.as_str()).collect();
                for step in &self.steps {
                    for dep in &step.depends_on {
                        if !step_ids.contains(dep.as_str()) {
                            return Err(CompError::StepNotFound { id: dep.clone() });
                        }
                    }
                }
            }
        }

        // 6. output_key 唯一性（非空字符串）+ 长度限制
        let mut output_keys = std::collections::HashSet::new();
        for step in &self.steps {
            if let Some(ref key) = step.output_key {
                if key.is_empty() {
                    return Err(CompError::ConfigParse {
                        path: "<workflow>".to_string(),
                        reason: format!("output_key for step '{}' must not be empty", step.id),
                    });
                }
                if key.len() > MAX_OUTPUT_KEY_LEN {
                    return Err(CompError::ConfigParse {
                        path: "<workflow>".to_string(),
                        reason: format!(
                            "output_key for step '{}' exceeds max length of {}",
                            step.id, MAX_OUTPUT_KEY_LEN
                        ),
                    });
                }
                if !output_keys.insert(key) {
                    return Err(CompError::DuplicateOutputKey { key: key.clone() });
                }
            }
        }

        Ok(())
    }
}

/// V0.4: Router 路由配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    /// 路由输入来源 step_id
    pub upstream: String,
}

/// 哨兵值：标记此 step 由 Flow 方法执行，非 Agent 调用。
pub const FLOW_AGENT_ID: &str = "__flow__";

/// 工作流中的一个执行步骤。
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Step {
    /// 步骤唯一标识（workflow 内唯一）
    pub id: String,

    /// 使用的 Agent ID（引用 Hero 注册表中的 Agent）
    pub agent_id: String,

    /// 任务描述模板，支持 {{var}} 插值
    pub task: String,

    /// 依赖的步骤 ID 列表
    /// 默认：空列表（表示可立即执行）
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// 输出存储到 Context 的键名
    /// 默认：步骤不保存输出到 Context
    #[serde(default)]
    pub output_key: Option<String>,

    /// 步骤执行超时时间（秒）
    /// 反序列化缺失时为 None，执行引擎 fallback 到 300
    #[serde(default)]
    pub timeout: Option<u64>,

    /// 步骤失败时的重试次数
    /// 默认：0（不重试）
    #[serde(default)]
    pub retries: Option<u64>,

    /// 每次重试的间隔时间（秒）
    /// 默认：0（立即重试）
    #[serde(default)]
    pub retry_delay: Option<u64>,

    /// 等待的外部信号名称
    /// 若不为 null，步骤执行完成后引擎进入 WaitingForSignal 状态
    #[serde(default)]
    pub wait_for_signal: Option<String>,

    /// 信号等待超时（秒）
    /// 默认：null（无超时，永久等待）
    #[serde(default)]
    pub signal_timeout: Option<u64>,

    /// V0.3.2: 审批超时后默认动作。fail（默认）或 reject。
    #[serde(default)]
    pub signal_timeout_action: Option<SignalTimeoutAction>,

    /// V0.3.3: 是否在步骤执行前暂停（断点调试）。
    #[serde(default)]
    pub breakpoint: bool,

    /// V0.3.9: 覆盖 Agent 默认模型（provider + name）。
    #[serde(default)]
    pub model_override: Option<tavern_core::ModelConfig>,

    /// 可选的预期输出描述，帮助 LLM 理解任务目标。
    /// 在 Manager prompt 和 Planning Context 注入时使用。
    #[serde(default)]
    pub expected_output: Option<String>,

    /// V0.4: OR 依赖——任一上游完成即触发。与 depends_on 互斥。
    #[serde(default)]
    pub or_depends_on: Vec<String>,

    /// V0.4: Router 配置——非 None 时此 step 执行后产生 label(s) 触发下游。
    #[serde(default)]
    pub router: Option<RouterConfig>,
}

/// 外部输入参数定义。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InputDef {
    /// 参数名称
    pub name: String,

    /// 是否必填
    /// 默认：true
    #[serde(default = "default_true")]
    pub required: bool,

    /// 默认值（支持任意 JSON 类型）
    #[serde(default)]
    pub default: Option<Value>,
}

fn default_true() -> bool {
    true
}

fn default_attempt() -> u64 {
    1
}

/// 工作流最终输出的定义。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutputDef {
    /// 输出字段名称
    pub name: String,

    /// 输出值模板，支持 {{var}} 插值
    pub value: String,
}

/// 工作流执行结果。
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowResult {
    /// 最终上下文（包含所有输入和步骤输出）
    pub context: Value,

    /// 工作流最终输出（由 `OutputDef` 模板渲染）
    pub outputs: Value,

    /// 每个步骤的详细执行结果
    pub step_results: HashMap<String, StepResult>,
}

/// 单个步骤的执行结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult {
    pub status: StepStatus,
    /// 步骤成功执行时的返回值；Failed 状态下为 None
    pub output: Option<Value>,
    /// 步骤失败时的错误信息；Completed 状态下为 None
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    /// 当前尝试次数（从 1 开始）
    #[serde(default = "default_attempt")]
    pub attempt: u64,
}

/// 步骤执行状态。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StepStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_workflow_full_yaml_deserialize() {
        let yaml = r#"
id: content_pipeline
name: 内容生产流水线
description: 研究 -> 写作 -> 编辑的协作流程

steps:
  - id: research
    agent_id: researcher
    task: "研究以下主题并整理关键信息: {{topic}}"
    output_key: research_notes

  - id: write
    agent_id: writer
    task: "根据以下研究资料撰写文章: {{research_notes}}"
    depends_on: [research]
    output_key: draft

  - id: edit
    agent_id: editor
    task: "编辑以下文章，改进语言和结构: {{draft}}"
    depends_on: [write]
    output_key: final_article

inputs:
  - name: topic
    required: true

outputs:
  - name: final_article
    value: "{{final_article}}"
"#;
        let workflow: Workflow = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(workflow.id, "content_pipeline");
        assert_eq!(workflow.name, "内容生产流水线");
        assert_eq!(
            workflow.description,
            Some("研究 -> 写作 -> 编辑的协作流程".to_string())
        );
        assert_eq!(workflow.steps.len(), 3);
        assert_eq!(workflow.steps[0].id, "research");
        assert_eq!(workflow.steps[0].depends_on, Vec::<String>::new());
        assert_eq!(workflow.steps[1].depends_on, vec!["research"]);
        assert_eq!(workflow.inputs.len(), 1);
        assert_eq!(workflow.inputs[0].name, "topic");
        assert!(workflow.inputs[0].required);
        assert_eq!(workflow.outputs.len(), 1);
        assert_eq!(workflow.outputs[0].value, "{{final_article}}");
    }

    #[test]
    fn test_workflow_defaults() {
        let yaml = r#"
id: minimal
name: 最小工作流
steps:
  - id: s1
    agent_id: a1
    task: do something
"#;
        let workflow: Workflow = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(workflow.description, None);
        assert!(workflow.inputs.is_empty());
        assert!(workflow.outputs.is_empty());
        assert_eq!(workflow.steps[0].depends_on, Vec::<String>::new());
        assert_eq!(workflow.steps[0].output_key, None);
        assert_eq!(workflow.steps[0].timeout, None);
    }

    #[test]
    fn test_input_def_defaults() {
        let yaml = r#"
name: x
required: true
"#;
        let def: InputDef = serde_yaml::from_str(yaml).unwrap();
        assert!(def.required);
        assert_eq!(def.default, None);

        let yaml2 = r#"
name: y
"#;
        let def2: InputDef = serde_yaml::from_str(yaml2).unwrap();
        assert!(def2.required);
    }

    #[test]
    fn test_step_result_serialize() {
        let result = StepResult {
            status: StepStatus::Completed,
            output: Some(json!("hello")),
            error: None,
            started_at: None,
            completed_at: None,
            attempt: 1,
        };
        let json_str = serde_json::to_string(&result).unwrap();
        assert!(json_str.contains("Completed"));
    }

    // ── Phase 1: CrewAI Alignment 新字段测试 ──

    #[test]
    fn test_workflow_default_process_is_sequential() {
        let yaml = r#"
id: minimal
name: 最小工作流
steps:
  - id: s1
    agent_id: a1
    task: do something
"#;
        let workflow: Workflow = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(workflow.process, Process::Sequential));
        assert!(workflow.planning.is_none());
    }

    #[test]
    fn test_step_expected_output_default() {
        let step: Step = serde_yaml::from_str("id: s1\nagent_id: a1\ntask: do something").unwrap();
        assert_eq!(step.expected_output, None);
    }

    #[test]
    fn test_step_expected_output_some() {
        let yaml = r#"
id: s1
agent_id: a1
task: do something
expected_output: "A research report"
"#;
        let step: Step = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(step.expected_output.as_deref(), Some("A research report"));
    }

    #[test]
    fn test_workflow_with_planning() {
        let yaml = r#"
id: complex
name: Complex
steps:
  - id: s1
    agent_id: a1
    task: do something
planning:
  enabled: true
  planning_agent: "planner"
"#;
        let workflow: Workflow = serde_yaml::from_str(yaml).unwrap();
        let planning = workflow.planning.unwrap();
        assert!(planning.enabled);
        assert_eq!(planning.planning_agent.as_deref(), Some("planner"));
    }

    // ── V0.4: OR dependency + Router fields ──

    #[test]
    fn test_router_config_serialize() {
        let cfg = RouterConfig {
            upstream: "step_a".into(),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("upstream"));
        assert!(json.contains("step_a"));
        let back: RouterConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.upstream, "step_a");
    }

    #[test]
    fn test_step_with_or_depends_on_deserialize() {
        let yaml = r#"
id: s1
agent_id: a1
task: do something
or_depends_on:
  - upstream_a
  - upstream_b
"#;
        let step: Step = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(step.or_depends_on, vec!["upstream_a", "upstream_b"]);
        assert!(step.depends_on.is_empty());
    }

    #[test]
    fn test_step_with_router_deserialize() {
        let yaml = r#"
id: s1
agent_id: a1
task: route
depends_on:
  - source
router:
  upstream: source
"#;
        let step: Step = serde_yaml::from_str(yaml).unwrap();
        assert!(step.router.is_some());
        assert_eq!(step.router.unwrap().upstream, "source");
    }
}
