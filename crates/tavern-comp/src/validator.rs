use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::CompError;
use crate::workflow::Workflow;

/// DAG 结构分析结果。
pub struct DagMaps {
    pub in_degree: HashMap<String, usize>,
    pub adj: HashMap<String, Vec<String>>,
    pub step_ids: HashSet<String>,
    /// V0.4: OR step 集合
    pub or_steps: HashSet<String>,
}

/// 构建 DAG 的入度表和邻接表（增强版：处理 depends_on + or_depends_on）。
pub fn build_dag_maps(workflow: &Workflow) -> DagMaps {
    let step_ids: HashSet<String> = workflow.steps.iter().map(|s| s.id.clone()).collect();
    let mut in_degree: HashMap<String, usize> =
        workflow.steps.iter().map(|s| (s.id.clone(), 0)).collect();
    let mut adj: HashMap<String, Vec<String>> = workflow
        .steps
        .iter()
        .map(|s| (s.id.clone(), Vec::new()))
        .collect();
    let mut or_steps: HashSet<String> = HashSet::new();

    for step in &workflow.steps {
        if !step.or_depends_on.is_empty() {
            let all_labels = step
                .or_depends_on
                .iter()
                .all(|u| u.starts_with("__label__"));
            if all_labels {
                // 纯 label 依赖：入度为 0（无静态前置，运行时 Router 触发）
                // decide_next_action 中额外检查 label 是否已出现在 completed_steps
                in_degree.insert(step.id.clone(), 0);
            } else {
                // 混合或纯方法依赖：入度 = 1（任一上游完成即触发）
                in_degree.insert(step.id.clone(), 1);
            }
            or_steps.insert(step.id.clone());
            for upstream in &step.or_depends_on {
                // 非 label 边加入邻接表（用于环检测）
                if !upstream.starts_with("__label__") {
                    adj.entry(upstream.clone())
                        .or_default()
                        .push(step.id.clone());
                }
            }
        } else {
            // AND: in_degree = depends_on.len()
            for dep in &step.depends_on {
                adj.entry(dep.clone()).or_default().push(step.id.clone());
                *in_degree.get_mut(&step.id).unwrap() += 1;
            }
        }
    }
    DagMaps {
        in_degree,
        adj,
        step_ids,
        or_steps,
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
        or_steps: _,
    } = build_dag_maps(workflow);

    // 校验依赖存在性
    for step in &workflow.steps {
        // V0.4: 互斥检查
        if !step.depends_on.is_empty() && !step.or_depends_on.is_empty() {
            return Err(CompError::ConfigParse {
                path: "<workflow>".to_string(),
                reason: format!(
                    "step '{}' has both depends_on and or_depends_on — must be mutually exclusive",
                    step.id
                ),
            });
        }
        // V0.4: router upstream 必须在 depends_on 中
        if let Some(ref router) = step.router
            && !step.depends_on.contains(&router.upstream)
        {
            return Err(CompError::ConfigParse {
                path: "<workflow>".to_string(),
                reason: format!(
                    "step '{}' has router.upstream '{}' which is not in depends_on",
                    step.id, router.upstream
                ),
            });
        }
        for dep in &step.depends_on {
            if !step_ids.contains(dep) {
                return Err(CompError::StepNotFound { id: dep.clone() });
            }
        }
        // V0.4: or_depends_on 存在性检查（跳过 __label__ 条目）
        for dep in &step.or_depends_on {
            if !dep.starts_with("__label__") && !step_ids.contains(dep) {
                return Err(CompError::StepNotFound { id: dep.clone() });
            }
        }
    }

    // Kahn 算法
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut enqueued: HashSet<String> = HashSet::new();
    for (id, degree) in &in_degree {
        if *degree == 0 {
            queue.push_back(id.clone());
            enqueued.insert(id.clone());
        }
    }

    let mut topo_order: Vec<String> = Vec::with_capacity(n);

    while let Some(id) = queue.pop_front() {
        topo_order.push(id.clone());
        if let Some(neighbors) = adj.get(&id) {
            for neighbor in neighbors {
                let d = in_degree.get_mut(neighbor).unwrap();
                // saturating_sub: OR steps start at in_degree=1 but may have multiple non-label
                // upstreams that all point to the same step in adj; decrementing past 0 is a no-op.
                *d = d.saturating_sub(1);
                // V0.4: prevent double-enqueue of OR steps that hit 0 multiple times
                if *d == 0 && enqueued.insert(neighbor.clone()) {
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
            model_override: None,
            or_depends_on: vec![],
            router: None,
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
            webhook: None,
            schedule: None,
            schedule_inputs: serde_json::Value::Null,
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
            webhook: None,
            schedule: None,
            schedule_inputs: serde_json::Value::Null,
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
            webhook: None,
            schedule: None,
            schedule_inputs: serde_json::Value::Null,
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
            webhook: None,
            schedule: None,
            schedule_inputs: serde_json::Value::Null,
        };
        let err = validate_dag(&workflow).unwrap_err();
        assert!(matches!(err, CompError::StepNotFound { id } if id == "x"));
    }

    // ── V0.4: OR dependency tests ──

    fn make_step_or(id: &str, or_deps: Vec<&str>) -> crate::workflow::Step {
        crate::workflow::Step {
            or_depends_on: or_deps.into_iter().map(|s| s.to_string()).collect(),
            router: None,
            ..make_step(id, vec![])
        }
    }

    fn base_workflow() -> Workflow {
        Workflow {
            id: "w1".into(),
            name: "test".into(),
            description: None,
            steps: vec![],
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
    fn test_or_dep_build_dag_maps() {
        let workflow = Workflow {
            steps: vec![
                make_step("a", vec![]),
                make_step("b", vec![]),
                make_step_or("c", vec!["a", "b"]),
            ],
            ..base_workflow()
        };
        let dag = build_dag_maps(&workflow);
        // OR step: in_degree should be 1 (not 2)
        assert_eq!(dag.in_degree.get("c").copied(), Some(1));
        assert!(dag.or_steps.contains("c"));
    }

    #[test]
    fn test_or_dep_missing_dependency() {
        let workflow = Workflow {
            steps: vec![
                make_step("a", vec![]),
                make_step_or("b", vec!["nonexistent"]),
            ],
            ..base_workflow()
        };
        let err = validate_dag(&workflow).unwrap_err();
        assert!(matches!(err, CompError::StepNotFound { id } if id == "nonexistent"));
    }

    #[test]
    fn test_or_dep_mutual_exclusion_rejected() {
        let mut step = make_step("a", vec!["x"]);
        step.or_depends_on = vec!["y".into()];
        let workflow = Workflow {
            steps: vec![step],
            ..base_workflow()
        };
        let err = validate_dag(&workflow).unwrap_err();
        assert!(matches!(err, CompError::ConfigParse { .. }));
    }

    #[test]
    fn test_or_dep_cycle_detected() {
        let workflow = Workflow {
            steps: vec![make_step_or("a", vec!["b"]), make_step("b", vec!["a"])],
            ..base_workflow()
        };
        let err = validate_dag(&workflow).unwrap_err();
        assert!(matches!(err, CompError::CyclicDependency));
    }

    #[test]
    fn test_label_prefixed_or_dep_skipped_in_validation() {
        let workflow = Workflow {
            steps: vec![
                make_step("a", vec![]),
                make_step_or("b", vec!["__label__approved"]),
            ],
            ..base_workflow()
        };
        // __label__ entries should be skipped in existence check
        assert!(validate_dag(&workflow).is_ok());
    }

    #[test]
    fn test_router_upstream_not_in_depends_on_rejected() {
        let step = crate::workflow::Step {
            id: "r1".into(),
            agent_id: "a1".into(),
            task: "route".into(),
            depends_on: vec!["x".into()],
            router: Some(crate::workflow::RouterConfig {
                upstream: "y".into(),
            }),
            ..Default::default()
        };
        let workflow = Workflow {
            steps: vec![step],
            ..base_workflow()
        };
        let err = validate_dag(&workflow).unwrap_err();
        assert!(matches!(err, CompError::ConfigParse { .. }));
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
            webhook: None,
            schedule: None,
            schedule_inputs: serde_json::Value::Null,
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
            webhook: None,
            schedule: None,
            schedule_inputs: serde_json::Value::Null,
        };
        let err = workflow.validate_static().unwrap_err();
        assert!(matches!(err, CompError::ConfigParse { .. }));
    }
}
