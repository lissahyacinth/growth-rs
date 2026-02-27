use good_lp::solvers::highs::highs;
use good_lp::{Expression, Solution, SolverModel, constraint, variable, variables};
use thiserror::Error;
use tracing::{debug, info, instrument, trace, warn};

use crate::offering::{Offering, PodId, PodResources};

#[derive(Debug, PartialEq, Error)]
pub enum SolveError {
    #[error("solver resolution failed: {0}")]
    Resolution(#[from] good_lp::ResolutionError),
}

/// An offering paired with the maximum number of instances the pool allows.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundedOffering {
    pub offering: Offering,
    /// Maximum instances of this type the pool permits (from `ServerTypeConfig.max`).
    pub max_instances: u32,
}

/// Options controlling the solve behaviour.
pub struct SolveOptions {
    /// Penalty added to the objective for each unmet demand.
    /// Must be larger than the most expensive offering's hourly cost,
    /// otherwise the solver will prefer leaving pods unscheduled.
    pub unmet_demand_penalty: f64,
    /// Maximum wall-clock seconds the solver may run before returning
    /// the best feasible solution found so far.
    pub time_limit_seconds: f64,
}

impl Default for SolveOptions {
    fn default() -> Self {
        Self {
            unmet_demand_penalty: 1_000_000.0,
            time_limit_seconds: 30.0,
        }
    }
}

/// Expand bounded offerings into (type_index, instance_index) pairs.
///
/// For example, type 0 with max 3 produces `[(0,0), (0,1), (0,2)]`.
/// Each pair becomes a candidate slot the solver can activate independently.
fn build_candidate_offerings(bounded: &[BoundedOffering]) -> Vec<(usize, u32)> {
    bounded
        .iter()
        .enumerate()
        .flat_map(|(t, bo)| (0..bo.max_instances).map(move |i| (t, i)))
        .collect()
}

/// A node the solver decided to provision.
#[derive(Debug, Clone, PartialEq)]
pub struct PotentialNode {
    /// The offering this node is based on.
    pub offering: Offering,
    /// Pods assigned to this node by the solver.
    pub pods: Vec<PodId>,
}

#[derive(Debug, PartialEq)]
pub enum PlacementSolution {
    /// Every demand was placed — provision these nodes.
    AllPlaced(Vec<PotentialNode>),
    /// No demands existed — nothing to do.
    NoDemands,
    /// Some or all demands could not be placed.
    /// `nodes` may be empty (nothing schedulable) or non-empty (partial).
    IncompletePlacement {
        nodes: Vec<PotentialNode>,
        unmet: Vec<PodResources>,
    },
}

struct DecisionVariables {
    placements: Vec<Vec<good_lp::Variable>>,
    offering_active: Vec<good_lp::Variable>,
    unmet_demands: Vec<good_lp::Variable>,
}

fn log_inputs(demands: &[PodResources], offerings: &[Offering]) {
    let total_cpu: u32 = demands.iter().map(|d| d.resources.cpu).sum();
    let total_mem: u32 = demands.iter().map(|d| d.resources.memory_mib).sum();
    debug!(
        demand_count = demands.len(),
        total_cpu,
        total_memory_mib = total_mem,
        offering_types = offerings.len(),
        "solver inputs"
    );
    for (i, d) in demands.iter().enumerate() {
        trace!(i, namespace = %d.id.namespace, name = %d.id.name, cpu = d.resources.cpu, memory_mib = d.resources.memory_mib, gpu = d.resources.gpu, "demand");
    }
    for (i, o) in offerings.iter().enumerate() {
        trace!(i, instance_type = %o.instance_type, cpu = o.resources.cpu, memory_mib = o.resources.memory_mib, cost_per_hour = o.cost_per_hour, "offering");
    }
}

fn create_decision_variables(
    vars: &mut good_lp::ProblemVariables,
    num_demands: usize,
    candidate_offerings: &[(usize, u32)],
) -> DecisionVariables {
    let placements: Vec<Vec<_>> = (0..num_demands)
        .map(|demand| {
            candidate_offerings
                .iter()
                .enumerate()
                .map(|(candidate, _)| {
                    vars.add(
                        variable()
                            .binary()
                            .name(format!("placements_{demand}_{candidate}")),
                    )
                })
                .collect()
        })
        .collect();

    let offering_active: Vec<_> = candidate_offerings
        .iter()
        .enumerate()
        .map(|(candidate, _)| vars.add(variable().binary().name(format!("active_{candidate}"))))
        .collect();

    let unmet_demands: Vec<_> = (0..num_demands)
        .map(|demand| vars.add(variable().binary().name(format!("unmet_{demand}"))))
        .collect();

    DecisionVariables {
        placements,
        offering_active,
        unmet_demands,
    }
}

fn build_objective(
    candidate_offerings: &[(usize, u32)],
    offerings: &[Offering],
    dv: &DecisionVariables,
    unmet_demand_penalty: f64,
) -> Expression {
    let offering_cost: Expression = candidate_offerings
        .iter()
        .enumerate()
        .map(|(candidate_idx, (type_idx, _))| {
            dv.offering_active[candidate_idx] * offerings[*type_idx].cost_per_hour
        })
        .sum();

    let penalty: Expression = dv
        .unmet_demands
        .iter()
        .map(|&u| u * unmet_demand_penalty)
        .sum();

    offering_cost + penalty
}

fn add_constraints<P: SolverModel>(
    mut problem: P,
    demands: &[PodResources],
    offerings: &[Offering],
    candidate_offerings: &[(usize, u32)],
    bounded: &[BoundedOffering],
    dv: &DecisionVariables,
) -> P {
    // Each pod can only be assigned to one node, or it's unscheduled.
    for demand in 0..demands.len() {
        let total: Expression = dv.placements[demand].iter().copied().sum();
        problem = problem.with(constraint!(total + dv.unmet_demands[demand] == 1));
    }

    // A pod can only be placed on an active (provisioned) candidate.
    for demand in 0..demands.len() {
        for candidate in 0..candidate_offerings.len() {
            problem = problem.with(constraint!(
                dv.placements[demand][candidate] <= dv.offering_active[candidate]
            ));
        }
    }

    // Per-type NodePool max: the number of active candidates of each offering type
    // must not exceed the pool's max_instances for that type.
    for (type_idx, bo) in bounded.iter().enumerate() {
        let active_of_type: Expression = candidate_offerings
            .iter()
            .enumerate()
            .filter(|(_, (t, _))| *t == type_idx)
            .map(|(candidate_idx, _)| dv.offering_active[candidate_idx])
            .sum();
        problem = problem.with(constraint!(active_of_type <= bo.max_instances as f64));
    }

    // GPU model incompatibility: if a pod needs a specific GPU model and the
    // offering doesn't provide it, force placement to zero.
    for (demand_idx, pod) in demands.iter().enumerate() {
        let Some(needed) = &pod.resources.gpu_model else {
            continue;
        };
        for (candidate_idx, (type_idx, _)) in candidate_offerings.iter().enumerate() {
            if offerings[*type_idx].resources.gpu_model.as_ref() != Some(needed) {
                problem = problem.with(constraint!(
                    dv.placements[demand_idx][candidate_idx] == 0
                ));
            }
        }
    }

    // Capacity constraints per candidate.
    // TODO: Add ephemeral storage capacity constraints
    for (candidate_idx, (type_idx, _)) in candidate_offerings.iter().enumerate() {
        let cpu_used: Expression = demands
            .iter()
            .enumerate()
            .map(|(demand, pod)| dv.placements[demand][candidate_idx] * pod.resources.cpu as f64)
            .sum();
        let mem_used: Expression = demands
            .iter()
            .enumerate()
            .map(|(demand, pod)| {
                dv.placements[demand][candidate_idx] * pod.resources.memory_mib as f64
            })
            .sum();
        let gpu_requests: Expression = demands
            .iter()
            .enumerate()
            .map(|(demand, pod)| {
                dv.placements[demand][candidate_idx] * pod.resources.gpu as f64
            })
            .sum();
        problem = problem.with(constraint!(
            cpu_used <= offerings[*type_idx].resources.cpu as f64
        ));
        problem = problem.with(constraint!(
            mem_used <= offerings[*type_idx].resources.memory_mib as f64
        ));
        problem = problem.with(constraint!(
            gpu_requests <= offerings[*type_idx].resources.gpu as f64
        ));
    }

    // TODO: Add AntiAffinity
    problem
}

fn extract_solution(
    solution: &impl Solution,
    demands: &[PodResources],
    offerings: &[Offering],
    candidate_offerings: &[(usize, u32)],
    dv: &DecisionVariables,
) -> PlacementSolution {
    // Collect unmet demands.
    let mut unmet: Vec<PodResources> = Vec::new();
    for (demand_idx, demand) in demands.iter().enumerate() {
        if solution.value(dv.unmet_demands[demand_idx]) > 0.5 {
            debug!(pod = %demand.id, "pod unschedulable after solve");
            unmet.push(demand.clone());
        }
    }

    // Build a PotentialNode for each active candidate offering.
    let mut nodes: Vec<PotentialNode> = Vec::new();
    for (candidate_idx, &(type_idx, _)) in candidate_offerings.iter().enumerate() {
        if solution.value(dv.offering_active[candidate_idx]) <= 0.5 {
            continue;
        }
        let pods: Vec<PodId> = demands
            .iter()
            .enumerate()
            .filter(|(d, _)| solution.value(dv.placements[*d][candidate_idx]) > 0.5)
            .map(|(_, pr)| pr.id.clone())
            .collect();

        for pod in &pods {
            debug!(pod = %pod, instance_type = %offerings[type_idx].instance_type, "placement");
        }

        nodes.push(PotentialNode {
            offering: offerings[type_idx].clone(),
            pods,
        });
    }

    let total_cost: f64 = nodes.iter().map(|n| n.offering.cost_per_hour).sum();
    info!(
        nodes = nodes.len(),
        cost_per_hour = total_cost,
        unmet = unmet.len(),
        "solve complete",
    );

    if unmet.is_empty() {
        PlacementSolution::AllPlaced(nodes)
    } else {
        PlacementSolution::IncompletePlacement { nodes, unmet }
    }
}

/// Solve for all demands are met with the cheapest possible offerings
///
/// Minimise `sum(placements[demand][offering] for all offerings) + unscheduled[demand] == 1`
///
#[instrument(skip_all, fields(demands = demands.len(), bounded_offerings = bounded.len()))]
pub fn solve(
    demands: &[PodResources],
    bounded: &[BoundedOffering],
    options: &SolveOptions,
) -> Result<PlacementSolution, SolveError> {
    if demands.is_empty() {
        debug!("no demands to solve");
        return Ok(PlacementSolution::NoDemands);
    }
    if bounded.is_empty() {
        warn!(demands = demands.len(), "no offerings available, all demands will be unmet");
        let unmet = demands.to_vec();
        return Ok(PlacementSolution::IncompletePlacement {
            nodes: vec![],
            unmet,
        });
    }

    let offerings: Vec<Offering> = bounded.iter().map(|bo| bo.offering.clone()).collect();

    info!(
        demands = demands.len(),
        offerings = offerings.len(),
        "starting solve"
    );
    log_inputs(demands, &offerings);

    let candidate_offerings = build_candidate_offerings(bounded);
    debug!(
        candidates = candidate_offerings.len(),
        "candidate offerings (type x max_instances)"
    );

    let mut vars = variables!();
    let dv = create_decision_variables(&mut vars, demands.len(), &candidate_offerings);
    let objective = build_objective(
        &candidate_offerings,
        &offerings,
        &dv,
        options.unmet_demand_penalty,
    );

    let problem = vars
        .minimise(objective)
        .using(highs)
        .set_time_limit(options.time_limit_seconds);
    let problem = add_constraints(problem, demands, &offerings, &candidate_offerings, bounded, &dv);

    let solution = problem.solve()?;

    Ok(extract_solution(
        &solution,
        demands,
        &offerings,
        &candidate_offerings,
        &dv,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offering::{GpuModel, InstanceType, PodId, Resources};

    fn demand(name: &str, cpu: u32, memory_mib: u32) -> PodResources {
        PodResources {
            id: PodId {
                namespace: "default".into(),
                name: name.into(),
            },
            uid: format!("uid-{name}"),
            resources: Resources {
                cpu,
                memory_mib,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            pool: None,
        }
    }

    fn offering(name: &str, cpu: u32, memory_mib: u32, cost_per_hour: f64) -> Offering {
        Offering {
            instance_type: InstanceType(name.into()),
            resources: Resources {
                cpu,
                memory_mib,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour,
        }
    }

    fn opts() -> SolveOptions {
        SolveOptions::default()
    }

    fn bounded(o: Offering, max: u32) -> BoundedOffering {
        BoundedOffering {
            offering: o,
            max_instances: max,
        }
    }

    #[test]
    fn empty_demands() {
        assert_eq!(
            solve(
                &[],
                &[bounded(offering("cx22", 2, 4096, 0.01), 10)],
                &opts()
            ),
            Ok(PlacementSolution::NoDemands)
        );
    }

    #[test]
    fn empty_offerings() {
        let demands = vec![demand("pod-a", 2, 4096)];
        assert_eq!(
            solve(&demands, &[], &opts()),
            Ok(PlacementSolution::IncompletePlacement {
                nodes: vec![],
                unmet: demands
            })
        );
    }

    #[test]
    fn single_demand_single_offering() {
        let demands = vec![demand("pod-a", 2, 4096)];
        let offerings = vec![offering("cx22", 2, 4096, 0.01)];
        assert_eq!(
            solve(
                &demands,
                &[bounded(offerings[0].clone(), 10)],
                &opts()
            ),
            Ok(PlacementSolution::AllPlaced(vec![PotentialNode {
                offering: offerings[0].clone(),
                pods: vec![demands[0].id.clone()]
            }]))
        );
    }

    #[test]
    fn prefers_cheaper_offering() {
        let demands = vec![demand("pod-a", 2, 4096)];
        let offerings = vec![
            offering("expensive", 4, 8192, 1.00),
            offering("cheap", 2, 4096, 0.01),
        ];
        // Both can satisfy the demand; solver should succeed (cost preference is in objective)
        let bounded_offerings: Vec<_> =
            offerings.iter().map(|o| bounded(o.clone(), 10)).collect();
        assert_eq!(
            solve(&demands, &bounded_offerings, &opts()),
            Ok(PlacementSolution::AllPlaced(vec![PotentialNode {
                offering: offerings[1].clone(), // 0 => Expensive, 1 => Cheap
                pods: vec![demands[0].id.clone()]
            }]))
        );
    }

    #[test]
    fn multiple_demands_bin_packing() {
        let demands = vec![
            demand("pod-a", 1, 1024),
            demand("pod-b", 1, 1024),
            demand("pod-c", 1, 1024),
        ];
        let offerings = vec![
            offering("cx22", 2, 4096, 0.01),
            offering("10x-cx22", 20, 40960, 1.00),
        ];
        let bounded_offerings: Vec<_> =
            offerings.iter().map(|o| bounded(o.clone(), 10)).collect();

        let result = solve(&demands, &bounded_offerings, &opts()).unwrap();
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        // We require at least 2 nodes to fix 3vCPU onto a cx22, and a 10x-cx22 would be _wild_.
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().all(|n| n.offering == offerings[0]));

        // Every pod appears exactly once across all nodes
        let mut all_pods: Vec<_> = nodes.iter().flat_map(|n| &n.pods).collect();
        all_pods.sort_by_key(|p| &p.name);
        assert_eq!(all_pods.len(), 3);
        assert_eq!(all_pods[0].name, "pod-a");
        assert_eq!(all_pods[1].name, "pod-b");
        assert_eq!(all_pods[2].name, "pod-c");
    }

    #[test]
    fn build_candidate_offerings_layout() {
        let offerings = vec![
            bounded(offering("a", 2, 4096, 0.01), 3),
            bounded(offering("b", 4, 8192, 0.02), 3),
        ];
        let candidates = build_candidate_offerings(&offerings);
        assert_eq!(
            candidates,
            vec![(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
        );
    }

    #[test]
    fn build_candidate_offerings_respects_per_type_max() {
        let offerings = vec![
            bounded(offering("a", 2, 4096, 0.01), 2),
            bounded(offering("b", 4, 8192, 0.02), 1),
        ];
        let candidates = build_candidate_offerings(&offerings);
        assert_eq!(candidates, vec![(0, 0), (0, 1), (1, 0)]);
    }

    #[test]
    fn max_instances_limits_solver_output() {
        // 3 pods each needing their own 2-cpu node, but max_instances=2.
        // Solver should place 2 and leave 1 unmet.
        let demands = vec![
            demand("pod-a", 2, 4096),
            demand("pod-b", 2, 4096),
            demand("pod-c", 2, 4096),
        ];
        let bounded_offerings = vec![bounded(offering("cx22", 2, 4096, 0.01), 2)];

        let result = solve(&demands, &bounded_offerings, &opts()).unwrap();
        let PlacementSolution::IncompletePlacement { nodes, unmet } = result else {
            panic!("expected IncompletePlacement, got {result:?}");
        };
        assert_eq!(nodes.len(), 2);
        assert_eq!(unmet.len(), 1);
    }

    fn gpu_demand(name: &str, cpu: u32, memory_mib: u32, gpu: u32, model: GpuModel) -> PodResources {
        PodResources {
            id: PodId {
                namespace: "default".into(),
                name: name.into(),
            },
            uid: format!("uid-{name}"),
            resources: Resources {
                cpu,
                memory_mib,
                ephemeral_storage_gib: None,
                gpu,
                gpu_model: Some(model),
            },
            pool: None,
        }
    }

    fn gpu_offering(name: &str, cpu: u32, memory_mib: u32, gpu: u32, model: GpuModel, cost: f64) -> Offering {
        Offering {
            instance_type: InstanceType(name.into()),
            resources: Resources {
                cpu,
                memory_mib,
                ephemeral_storage_gib: None,
                gpu,
                gpu_model: Some(model),
            },
            cost_per_hour: cost,
        }
    }

    #[test]
    fn gpu_model_constraint_places_pods_on_correct_offering() {
        let demands = vec![
            gpu_demand("a100-pod", 4, 8192, 1, GpuModel::NvidiaA100),
            gpu_demand("t4-pod", 4, 8192, 1, GpuModel::NvidiaT4),
        ];
        let offerings = vec![
            gpu_offering("gpu-t4", 8, 16384, 1, GpuModel::NvidiaT4, 0.50),
            gpu_offering("gpu-a100", 8, 16384, 1, GpuModel::NvidiaA100, 2.00),
        ];
        let bounded_offerings: Vec<_> = offerings.iter().map(|o| bounded(o.clone(), 2)).collect();

        let result = solve(&demands, &bounded_offerings, &opts()).unwrap();
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        // Both pods should be placed, each on the matching GPU type.
        assert_eq!(nodes.len(), 2);
        for node in &nodes {
            let model = node.offering.resources.gpu_model.as_ref().unwrap();
            for pod_id in &node.pods {
                if pod_id.name == "a100-pod" {
                    assert_eq!(model, &GpuModel::NvidiaA100);
                } else if pod_id.name == "t4-pod" {
                    assert_eq!(model, &GpuModel::NvidiaT4);
                }
            }
        }
    }

    #[test]
    fn gpu_model_mismatch_leaves_pod_unmet() {
        // Pod needs an A100, but only T4 offerings exist.
        let demands = vec![gpu_demand("a100-pod", 4, 8192, 1, GpuModel::NvidiaA100)];
        let offerings = vec![gpu_offering("gpu-t4", 8, 16384, 1, GpuModel::NvidiaT4, 0.50)];
        let bounded_offerings: Vec<_> = offerings.iter().map(|o| bounded(o.clone(), 2)).collect();

        let result = solve(&demands, &bounded_offerings, &opts()).unwrap();
        let PlacementSolution::IncompletePlacement { nodes: _, unmet } = result else {
            panic!("expected IncompletePlacement, got {result:?}");
        };
        assert_eq!(unmet.len(), 1);
        assert_eq!(unmet[0].id.name, "a100-pod");
    }
}
