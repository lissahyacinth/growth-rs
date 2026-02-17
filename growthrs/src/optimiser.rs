use good_lp::{Expression, Solution, SolverModel, constraint, default_solver, variable, variables};
use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::offering::Offering;
use crate::offering::PodResources;

fn build_candidate_offerings(offering_types: &[Offering], max_instances: u32) -> Vec<(usize, u32)> {
    // (type_index, instance_index) pairs
    // e.g. node type 0 with 3 max_count -> [(0,0), (0,1), (0,2)]
    offering_types
        .iter()
        .enumerate()
        .flat_map(|(t, nt)| (0..max_instances).map(move |i| (t, i)))
        .collect()
}

/// Solve for all demands are met with the cheapest possible offerings
///
/// Minimise `sum(placements[demand][offering] for all offerings) + unscheduled[demand] == 1`
///
pub(crate) fn solve(
    demands: &[PodResources],
    offerings: &[Offering],
) -> Result<(), Box<dyn std::error::Error>> {
    info!(demands = demands.len(), offerings = offerings.len(), "starting solve");
    for (i, d) in demands.iter().enumerate() {
        debug!(i, namespace = %d.namespace, name = %d.name, cpu = d.resources.cpu, memory_mib = d.resources.memory_mib, gpu = d.resources.gpu, "demand");
    }
    for (i, o) in offerings.iter().enumerate() {
        debug!(i, instance_type = %o.instance_type.0, cpu = o.resources.cpu, memory_mib = o.resources.memory_mib, cost_per_hour = o.cost_per_hour, "offering");
    }

    // TODO: Identify sensible bounds for how many of each node type. Let's settle at 10 for now.
    let candidate_offerings = build_candidate_offerings(offerings, 10);
    debug!(candidates = candidate_offerings.len(), "built candidate offerings (type x max_instances)");
    let mut vars = variables!();
    // placements[demand][offering]: demand placed on offering
    let placements: Vec<Vec<_>> = demands
        .iter()
        .enumerate()
        .map(|(demand, _)| {
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

    // offering_active[offering]: is this offering accepted
    let offering_active: Vec<_> = candidate_offerings
        .iter()
        .enumerate()
        .map(|(offering, _)| vars.add(variable().binary().name(format!("active_{offering}"))))
        .collect();

    // unmet_demands[demand]: demand d not met (soft failure)
    let unmet_demands: Vec<_> = demands
        .iter()
        .enumerate()
        .map(|(demand, _)| vars.add(variable().binary().name(format!("unmet_{demand}"))))
        .collect();

    // Objective - Minimise offering cost + also penalise for not being able to meet all demands
    let offering_cost: Expression = candidate_offerings
        .iter()
        .enumerate()
        .map(|(offering_index, (offering_type_index, _))| {
            offering_active[offering_index] * offerings[*offering_type_index].cost_per_hour
        })
        .sum();

    let unmet_demand_penalty: Expression = demands
        .iter()
        .enumerate()
        .map(|(p, _)| unmet_demands[p] * (1000.0)) // TODO: Take into account Pod Priority?
        .sum();

    let objective = offering_cost + unmet_demand_penalty;

    let mut problem = vars.minimise(objective).using(default_solver);

    // Constraints

    // Each pod can only be assigned to one node, or it's unscheduled.
    for demand in 0..demands.len() {
        let total: Expression = placements[demand].iter().copied().sum();
        problem = problem.with(constraint!(total + unmet_demands[demand] == 1));
    }

    // Each demand can only be met with one offering
    for demand in 0..demands.len() {
        for offering in 0..candidate_offerings.len() {
            problem = problem.with(constraint!(
                placements[demand][offering] <= offering_active[offering]
            ));
        }
    }

    // Capacity Requirements
    for offering in 0..candidate_offerings.len() {
        let (offering_type, _) = candidate_offerings[offering];
        let cpu_used: Expression = demands
            .iter()
            .enumerate()
            .map(|(demand, pod)| placements[demand][offering] * pod.resources.cpu as f64)
            .sum();
        let mem_used: Expression = demands
            .iter()
            .enumerate()
            .map(|(demand, pod)| placements[demand][offering] * pod.resources.memory_mib as f64)
            .sum();
        problem = problem.with(constraint!(
            cpu_used <= offerings[offering_type].resources.cpu as f64
        ));
        problem = problem.with(constraint!(
            mem_used <= offerings[offering_type].resources.memory_mib as f64
        ));
    }

    // TODO: Add AntiAffinity
    debug!("solving ILP");
    let solution = problem.solve()?;
    info!("solve complete");

    let mut offering_placement: HashMap<String, String> = HashMap::new();
    for demand in 0..demands.len() {
        if solution.value(unmet_demands[demand]) > 0.5 {
            warn!(namespace = %demands[demand].namespace, name = %demands[demand].name, "pod unschedulable after solve");
            continue;
        }
        for offering in 0..candidate_offerings.len() {
            if solution.value(placements[demand][offering]) > 0.5 {
                let (t, i) = candidate_offerings[offering];
                let node_label = format!("{}-{}", offerings[t].instance_type.0, i);
                offering_placement.insert(demands[demand].name.clone(), node_label.clone());
                info!(pod = %demands[demand].name, node = %node_label, "placement");
            }
        }
    }

    let active_nodes: Vec<_> = candidate_offerings
        .iter()
        .enumerate()
        .filter(|(n, _)| solution.value(offering_active[*n]) > 0.5)
        .map(|(_, (t, i))| format!("{}-{}", offerings[*t].instance_type.0, i))
        .collect();

    let total_cost: f64 = candidate_offerings
        .iter()
        .enumerate()
        .filter(|(n, _)| solution.value(offering_active[*n]) > 0.5)
        .map(|(_, (t, _))| offerings[*t].cost_per_hour)
        .sum();

    info!(?active_nodes, total_cost_per_hour = format!("{:.4}", total_cost), "solve result");
    Ok(())
}
