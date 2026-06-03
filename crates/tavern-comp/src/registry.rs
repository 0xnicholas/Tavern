use std::collections::HashMap;
use std::path::Path;

use serde::Serialize;

use crate::error::CompError;
use crate::workflow::Workflow;

/// Workflow 摘要信息，用于列表接口。
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowSummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Default)]
pub struct WorkflowRegistry {
    workflows: HashMap<String, Workflow>,
}

impl WorkflowRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册 Workflow。
    /// 错误：id 已存在时返回 `CompError::DuplicateWorkflow`。
    pub fn register(&mut self, workflow: Workflow) -> Result<(), CompError> {
        workflow.validate_static()?;
        if self.workflows.contains_key(&workflow.id) {
            return Err(CompError::DuplicateWorkflow {
                id: workflow.id.clone(),
            });
        }
        self.workflows.insert(workflow.id.clone(), workflow);
        Ok(())
    }

    /// 查询 Workflow。
    pub fn get(&self, id: &str) -> Option<&Workflow> {
        self.workflows.get(id)
    }

    /// 列出全部 Workflow（摘要信息）。
    pub fn list_all(&self) -> Vec<WorkflowSummary> {
        self.workflows
            .values()
            .map(|w| WorkflowSummary {
                id: w.id.clone(),
                name: w.name.clone(),
                description: w.description.clone(),
            })
            .collect()
    }

    /// 卸载 Workflow。
    /// 错误：id 不存在时返回 `CompError::WorkflowNotFound`。
    pub fn unregister(&mut self, id: &str) -> Result<(), CompError> {
        if self.workflows.remove(id).is_none() {
            return Err(CompError::WorkflowNotFound { id: id.to_string() });
        }
        Ok(())
    }

    /// 从目录批量加载 YAML 配置。
    /// 遍历目录下所有 `.yaml` / `.yml` 文件。
    /// 遇到首个错误即终止，此前已加载的 Workflow 保留在注册表中（不回滚）。
    pub fn load_from_dir(&mut self, dir: &Path) -> Result<(), CompError> {
        let canonical_dir = std::fs::canonicalize(dir).map_err(CompError::Io)?;
        for entry in std::fs::read_dir(&canonical_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().map(is_yaml_ext).unwrap_or(false) {
                let workflow = Workflow::from_yaml(&path)?;
                self.register(workflow)?;
            }
        }
        Ok(())
    }

    /// 清空注册表。
    pub fn clear(&mut self) {
        self.workflows.clear();
    }
}

fn is_yaml_ext(ext: &std::ffi::OsStr) -> bool {
    ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{Process, Step};

    fn make_workflow(id: &str) -> Workflow {
        Workflow {
            id: id.to_string(),
            name: format!("Workflow {}", id),
            description: None,
            steps: vec![Step {
                id: "s1".to_string(),
                agent_id: "a1".to_string(),
                task: "task".to_string(),
                depends_on: vec![],
                output_key: None,
                timeout: None,
                retries: None,
                retry_delay: None,
                wait_for_signal: None,
                signal_timeout: None,
                expected_output: None,
                signal_timeout_action: None,
                breakpoint: false,
                model_override: None,
                or_depends_on: vec![],
                router: None,
            }],
            inputs: vec![],
            outputs: vec![],
            process: Process::Sequential,
            planning: None,
            webhook: None,
            schedule: None,
            schedule_inputs: serde_json::Value::Null,
        }
    }

    #[test]
    fn test_registry_register_and_get() {
        let mut reg = WorkflowRegistry::new();
        let wf = make_workflow("w1");
        reg.register(wf).unwrap();
        assert!(reg.get("w1").is_some());
        assert!(reg.get("w2").is_none());
    }

    #[test]
    fn test_registry_duplicate() {
        let mut reg = WorkflowRegistry::new();
        let wf = make_workflow("w1");
        reg.register(wf.clone()).unwrap();
        let err = reg.register(wf).unwrap_err();
        assert!(matches!(err, CompError::DuplicateWorkflow { id } if id == "w1"));
    }

    #[test]
    fn test_registry_list_all() {
        let mut reg = WorkflowRegistry::new();
        reg.register(make_workflow("w1")).unwrap();
        reg.register(make_workflow("w2")).unwrap();
        let list = reg.list_all();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_registry_load_from_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.yaml"),
            r#"
id: flow_a
name: Flow A
steps:
  - id: s1
    agent_id: a1
    task: task
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("b.yml"),
            r#"
id: flow_b
name: Flow B
steps:
  - id: s1
    agent_id: a1
    task: task
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("ignore.txt"), "ignore").unwrap();

        let mut reg = WorkflowRegistry::new();
        reg.load_from_dir(dir.path()).unwrap();
        assert_eq!(reg.list_all().len(), 2);
    }
}
