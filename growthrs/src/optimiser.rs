use good_lp::solvers::highs::highs;
use good_lp::{Expression, Solution, SolverModel, constraint, variable, variables};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::offering::{Offering, PodId, PodResources};

#[derive(Debug, PartialEq, Error)]
pub enum SolveError {
    #[error("solver resolution failed: {0}")]
    Resolution(#[from] good_lp::ResolutionError),
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

fn build_candidate_offerings(offering_types: &[Offering], max_instances: u32) -> Vec<(usize, u32)> {
    // (type_index, instance_index) pairs
    // e.g. node type 0 with 3 max_count -> [(0,0), (0,1), (0,2)]
    offering_types
        .iter()
        .enumerate()
        .flat_map(|(t, _nt)| (0..max_instances).map(move |i| (t, i)))
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
    for (i, d) in demands.iter().enumerate() {
        debug!(i, namespace = %d.id.namespace, name = %d.id.name, cpu = d.resources.cpu, memory_mib = d.resources.memory_mib, gpu = d.resources.gpu, "demand");
    }
    for (i, o) in offerings.iter().enumerate() {
        debug!(i, instance_type = %o.instance_type.0, cpu = o.resources.cpu, memory_mib = o.resources.memory_mib, cost_per_hour = o.cost_per_hour, "offering");
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
                .map(|(offering, _)| {
                    vars.add(
                        variable()
                            .binary()
                            .name(format!("placements_{demand}_{offering}")),
                    )
                })
                .collect()
        })
        .collect();

    let offering_active: Vec<_> = candidate_offerings
        .iter()
        .enumerate()
        .map(|(offering, _)| vars.add(variable().binary().name(format!("active_{offering}"))))
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
        .map(|(offering_index, (offering_type_index, _))| {
            dv.offering_active[offering_index] * offerings[*offering_type_index].cost_per_hour
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
    dv: &DecisionVariables,
) -> P {
    // Each pod can only be assigned to one node, or it's unscheduled.
    for demand in 0..demands.len() {
        let total: Expression = dv.placements[demand].iter().copied().sum();
        problem = problem.with(constraint!(total + dv.unmet_demands[demand] == 1));
    }

    // A pod can only be placed on an active (provisioned) offering.
    for demand in 0..demands.len() {
        for offering in 0..candidate_offerings.len() {
            problem = problem.with(constraint!(
                dv.placements[demand][offering] <= dv.offering_active[offering]
            ));
        }
    }

    // Capacity Requirements
    // TODO: Add GPU capacity constraints (gpu count and gpu_model matching)
    // TODO: Add ephemeral storage capacity constraints
    for (offering_idx, (offering_type, _)) in candidate_offerings.iter().enumerate() {
        let cpu_used: Expression = demands
            .iter()
            .enumerate()
            .map(|(demand, pod)| dv.placements[demand][offering_idx] * pod.resources.cpu as f64)
            .sum();
        let mem_used: Expression = demands
            .iter()
            .enumerate()
            .map(|(demand, pod)| {
                dv.placements[demand][offering_idx] * pod.resources.memory_mib as f64
            })
            .sum();
        problem = problem.with(constraint!(
            cpu_used <= offerings[*offering_type].resources.cpu as f64
        ));
        problem = problem.with(constraint!(
            mem_used <= offerings[*offering_type].resources.memory_mib as f64
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
            warn!(pod = %demand.id, "pod unschedulable after solve");
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
            info!(pod = %pod, instance_type = %offerings[type_idx].instance_type.0, "placement");
        }

        nodes.push(PotentialNode {
            offering: offerings[type_idx].clone(),
            pods,
        });
    }

    let total_cost: f64 = nodes.iter().map(|n| n.offering.cost_per_hour).sum();
    info!(
        active_nodes = nodes.len(),
        total_cost_per_hour = format!("{:.4}", total_cost),
        unmet = unmet.len(),
        "solve result",
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
pub fn solve(
    demands: &[PodResources],
    offerings: &[Offering],
    options: &SolveOptions,
) -> Result<PlacementSolution, SolveError> {
    info!(
        demands = demands.len(),
        offerings = offerings.len(),
        "starting solve"
    );

    if demands.is_empty() {
        info!("no demands to solve");
        return Ok(PlacementSolution::NoDemands);
    }
    if offerings.is_empty() {
        warn!("no offerings available — all demands will be unmet");
        for d in demands {
            warn!(pod = %d.id, "pod unschedulable (no offerings)");
        }
        let unmet = demands.to_vec();
        return Ok(PlacementSolution::IncompletePlacement {
            nodes: vec![],
            unmet,
        });
    }

    log_inputs(demands, offerings);

    // TODO: Identify sensible bounds for how many of each node type. Let's settle at 10 for now.
    let candidate_offerings = build_candidate_offerings(offerings, 10);
    debug!(
        candidates = candidate_offerings.len(),
        "built candidate offerings (type x max_instances)"
    );

    let mut vars = variables!();
    let dv = create_decision_variables(&mut vars, demands.len(), &candidate_offerings);
    let objective = build_objective(
        &candidate_offerings,
        offerings,
        &dv,
        options.unmet_demand_penalty,
    );

    let problem = vars
        .minimise(objective)
        .using(highs)
        .set_time_limit(options.time_limit_seconds);
    let problem = add_constraints(problem, demands, offerings, &candidate_offerings, &dv);

    debug!("solving ILP");
    let solution = problem.solve()?;
    info!("solve complete");

    Ok(extract_solution(
        &solution,
        demands,
        offerings,
        &candidate_offerings,
        &dv,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offering::{InstanceType, PodId, Resources};

    fn demand(name: &str, cpu: u32, memory_mib: u32) -> PodResources {
        PodResources {
            id: PodId {
                namespace: "default".into(),
                name: name.into(),
            },
            resources: Resources {
                cpu,
                memory_mib,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
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

    #[test]
    fn empty_demands() {
        assert_eq!(
            solve(&[], &[offering("cx22", 2, 4096, 0.01)], &opts()),
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
            solve(&demands, &offerings, &opts()),
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
        assert_eq!(
            solve(&demands, &offerings, &opts()),
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

        let result = solve(&demands, &offerings, &opts()).unwrap();
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
        let offerings = vec![offering("a", 2, 4096, 0.01), offering("b", 4, 8192, 0.02)];
        let candidates = build_candidate_offerings(&offerings, 3);
        assert_eq!(
            candidates,
            vec![(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
        );
    }
}
