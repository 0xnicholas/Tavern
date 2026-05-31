use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::CompError;
use crate::workflow::Workflow;

/// DAG 结构分析结果。
pub struct DagMaps {
    pub in_degree: HashMap<String, usize>,
    pub adj: HashMap<String, Vec<String>>,
    pub step_ids: HashSet<String>,
}

/// 构建 DAG 的入度表和邻接表。
pub fn build_dag_maps(workflow: &Workflow) -> DagMaps {
    let step_ids: HashSet<String> = workflow.steps.iter().map(|s| s.id.clone()).collect();
    let mut in_degree: HashMap<String, usize> =
        workflow.steps.iter().map(|s| (s.id.clone(), 0)).collect();
    let mut adj: HashMap<String, Vec<String>> = workflow
        .steps
        .iter()
        .map(|s| (s.id.clone(), Vec::new()))
        .collect();

    for step in &workflow.steps {
        for dep in &step.depends_on {
            adj.entry(dep.clone()).or_default().push(step.id.clone());
            *in_degree.get_mut(&step.id).unwrap() += 1;
        }
    }
    DagMaps {
        in_degree,
        adj,
        step_ids,
    }
}

/// 对 Workflow 进行 DAG 验证：检查环并返回拓扑排序后的步骤 ID 列表。
///
/// 若发现环，返回 `CompError::CyclicDependency`。
pub fn validate_dag(workflow: &Workflow) -> Result<Vec<String>, CompError> {
    let n = workflow.steps.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    let DagMaps {
        mut in_degree,
        adj,
        step_ids,
    } = build_dag_maps(workflow);

    // 校验依赖存在性
    for step in &workflow.steps {
        for dep in &step.depends_on {
            if !step_ids.contains(dep) {
                return Err(CompError::StepNotFound { id: dep.clone() });
            }
        }
    }

    // Kahn 算法
    let mut queue: VecDeque<String> = VecDeque::new();
    for (id, degree) in &in_degree {
        if *degree == 0 {
            queue.push_back(id.clone());
        }
    }

    let mut topo_order: Vec<String> = Vec::with_capacity(n);

    while let Some(id) = queue.pop_front() {
        topo_order.push(id.clone());
        if let Some(neighbors) = adj.get(&id) {
            for neighbor in neighbors {
                let d = in_degree.get_mut(neighbor).unwrap();
                *d -= 1;
                if *d == 0 {
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }

    if topo_order.len() != n {
        return Err(CompError::CyclicDependency);
    }

    Ok(topo_order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{ManagerConfig, Process};

    fn make_step(id: &str, deps: Vec<&str>) -> crate::workflow::Step {
        crate::workflow::Step {
            id: id.to_string(),
            agent_id: "a1".to_string(),
            task: "task".to_string(),
            depends_on: deps.into_iter().map(|s| s.to_string()).collect(),
            output_key: None,
            timeout: None,
            retries: None,
            retry_delay: None,
            wait_for_signal: None,
            signal_timeout: None,
            expected_output: None,
            signal_timeout_action: None,
            breakpoint: false,
        }
    }

    #[test]
    fn test_dag_linear() {
        let workflow = Workflow {
            id: "w1".to_string(),
            name: "test".to_string(),
            description: None,
            steps: vec![
                make_step("a", vec![]),
                make_step("b", vec!["a"]),
                make_step("c", vec!["b"]),
            ],
            inputs: vec![],
            outputs: vec![],
            process: Process::Sequential,
            planning: None,
        };
        let order = validate_dag(&workflow).unwrap();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_dag_branch() {
        let workflow = Workflow {
            id: "w1".to_string(),
            name: "test".to_string(),
            description: None,
            steps: vec![
                make_step("a", vec![]),
                make_step("b", vec!["a"]),
                make_step("c", vec!["a"]),
                make_step("d", vec!["b", "c"]),
            ],
            inputs: vec![],
            outputs: vec![],
            process: Process::Sequential,
            planning: None,
        };
        let order = validate_dag(&workflow).unwrap();
        assert_eq!(order[0], "a");
        assert_eq!(order[3], "d");
    }

    #[test]
    fn test_dag_cycle() {
        let workflow = Workflow {
            id: "w1".to_string(),
            name: "test".to_string(),
            description: None,
            steps: vec![
                make_step("a", vec!["c"]),
                make_step("b", vec!["a"]),
                make_step("c", vec!["b"]),
            ],
            inputs: vec![],
            outputs: vec![],
            process: Process::Sequential,
            planning: None,
        };
        let err = validate_dag(&workflow).unwrap_err();
        assert!(matches!(err, CompError::CyclicDependency));
    }

    #[test]
    fn test_dag_missing_dependency() {
        let workflow = Workflow {
            id: "w1".to_string(),
            name: "test".to_string(),
            description: None,
            steps: vec![make_step("a", vec![]), make_step("b", vec!["x"])],
            inputs: vec![],
            outputs: vec![],
            process: Process::Sequential,
            planning: None,
        };
        let err = validate_dag(&workflow).unwrap_err();
        assert!(matches!(err, CompError::StepNotFound { id } if id == "x"));
    }

    // ── Phase 1: Hierarchical 校验测试 ──

    #[test]
    fn test_hierarchical_skips_dag_validation() {
        let workflow = Workflow {
            id: "w1".to_string(),
            name: "test".to_string(),
            description: None,
            steps: vec![
                make_step("a", vec!["c"]),
                make_step("b", vec!["a"]),
                make_step("c", vec!["b"]),
            ],
            inputs: vec![],
            outputs: vec![],
            process: Process::Hierarchical(ManagerConfig {
                agent_id: "manager".to_string(),
                instructions: None,
            }),
            planning: None,
        };
        // This workflow has a cycle (a -> c -> b -> a), but hierarchical mode skips DAG
        assert!(workflow.validate_static().is_ok());
    }

    #[test]
    fn test_hierarchical_manager_id_must_be_valid() {
        let workflow = Workflow {
            id: "w1".to_string(),
            name: "test".to_string(),
            description: None,
            steps: vec![make_step("a", vec![])],
            inputs: vec![],
            outputs: vec![],
            process: Process::Hierarchical(ManagerConfig {
                agent_id: "invalid id!".to_string(),
                instructions: None,
            }),
            planning: None,
        };
        let err = workflow.validate_static().unwrap_err();
        assert!(matches!(err, CompError::ConfigParse { .. }));
    }
}
