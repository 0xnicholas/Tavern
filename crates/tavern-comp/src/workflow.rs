use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::CompError;

pub use tavern_core::is_valid_id;

/// 工作流的完整配置定义。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Workflow {
    /// 全局唯一标识符
    /// 约束：^[a-zA-Z0-9_-]+$，长度 1-64
    pub id: String,

    /// 可读名称
    pub name: String,

    /// 描述（可选）
    #[serde(default)]
    pub description: Option<String>,

    /// 执行步骤列表
    pub steps: Vec<Step>,

    /// 外部输入参数定义
    /// 默认：空列表
    #[serde(default)]
    pub inputs: Vec<InputDef>,

    /// 工作流最终输出定义
    /// 默认：空列表（REST 响应中 outputs 字段为空对象 {}）
    #[serde(default)]
    pub outputs: Vec<OutputDef>,
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
    pub fn validate_static(&self) -> Result<(), CompError> {
        const MAX_STEPS: usize = 100;
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
        if self.steps.len() > MAX_STEPS {
            return Err(CompError::ConfigParse {
                path: "<workflow>".to_string(),
                reason: format!("workflow steps exceed limit of {}", MAX_STEPS),
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

        // 5. depends_on 存在性 + DAG 无环（由 validator 统一处理）
        crate::validator::validate_dag(self)?;

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

/// 工作流中的一个执行步骤。
#[derive(Debug, Clone, Deserialize, Serialize)]
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

fn default_empty_object() -> Value {
    Value::Object(serde_json::Map::new())
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
    #[serde(default = "default_empty_object")]
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
}
