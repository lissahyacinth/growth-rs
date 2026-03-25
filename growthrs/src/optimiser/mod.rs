//! Greedy bin-packing solver for pod-to-node placement.
//!
//! Models the Kubernetes [scheduling framework] to decide which nodes to
//! provision — the same approach Karpenter uses in its scheduling simulation.
//! We run Filter → Score → Reserve on hypothetical nodes; kube-scheduler
//! handles actual pod binding once the nodes exist.
//!
//! | Kube phase   | Solver equivalent                          |
//! |-------------|--------------------------------------------|
//! | **Filter**  | `Resources::satisfies`, affinity, budget    |
//! | **Score**   | `score_existing_node`, `score_new_offering` |
//! | **Reserve** | `InstanceBudget::activate`                  |
//! | **Bind**    | *not here — kube-scheduler does this*       |
//!
//! [scheduling framework]: https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/

use std::collections::BTreeMap;

use tracing::{debug, info, instrument, warn};

mod affinity;

use crate::offering::{Offering, PodId, PodResources, Resources};

use affinity::{check_affinity_required, check_anti_affinity_required, preferred_affinity_score};

/// An offering paired with the maximum number of instances the pool allogrowthrs\src\optimiser\mod.rsws.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundedOffering {
    pub offering: Offering,
    /// Maximum instances of this type the pool permits (from `ServerTypeConfig.max`).
    pub max_instances: u32,
    /// Labels inherited from the owning NodePool (e.g. topology.kubernetes.io/zone).
    pub labels: BTreeMap<String, String>,
    /// Offerings sharing a `type_group` share `max_instances` across the group.
    /// Used when the same instance type appears in multiple zones — the pool's
    /// `max` is a total across all location variants.
    pub type_group: Option<String>,
}

/// A node that already exists or is in-flight. The solver treats it as
/// pre-seeded capacity: pods can be placed on it, but it is not included
/// in the output (it's already committed).
#[derive(Debug, Clone)]
pub struct ExistingNode {
    pub resources: Resources,
    pub labels: BTreeMap<String, String>,
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

/// A candidate node that has been activated by the scheduler.
struct ActiveNode {
    /// Index into the `bounded` slice this node was created from.
    /// For pre-seeded nodes this is 0 and must never be read.
    type_idx: usize,
    /// Total resources this node provides (used for scoring).
    total_resources: Resources,
    /// Remaining resources on this node.
    remaining: Resources,
    /// Pods placed on this node so far.
    pods: Vec<PodId>,
    /// Topology values for this node (e.g. zone, region) — copied from
    /// `BoundedOffering.labels`.
    topo: BTreeMap<String, String>,
}

/// Tracks how many instances of each type (and type_group) have been activated.
struct InstanceBudget {
    /// Per-type count of activated nodes.
    per_type: Vec<u32>,
    /// Per type_group count of activated nodes.  Key = group name.
    per_group: BTreeMap<String, u32>,
    /// Max per type_group.
    group_max: BTreeMap<String, u32>,
}

impl InstanceBudget {
    fn new(bounded: &[BoundedOffering]) -> Self {
        let per_type = vec![0u32; bounded.len()];
        let mut group_max: BTreeMap<String, u32> = BTreeMap::new();
        for bo in bounded {
            if let Some(ref g) = bo.type_group {
                group_max.entry(g.clone()).or_insert(bo.max_instances);
            }
        }
        Self {
            per_type,
            per_group: BTreeMap::new(),
            group_max,
        }
    }

    fn can_activate(&self, type_idx: usize, bounded: &[BoundedOffering]) -> bool {
        let bo = &bounded[type_idx];
        // Per-type limit (only for non-group offerings).
        if bo.type_group.is_none() && self.per_type[type_idx] >= bo.max_instances {
            return false;
        }
        // Type-group limit.
        if let Some(ref g) = bo.type_group {
            let used = self.per_group.get(g).copied().unwrap_or(0);
            let max = self.group_max.get(g).copied().unwrap_or(0);
            if used >= max {
                return false;
            }
        }
        true
    }

    fn activate(&mut self, type_idx: usize, bounded: &[BoundedOffering]) {
        self.per_type[type_idx] += 1;
        if let Some(ref g) = bounded[type_idx].type_group {
            *self.per_group.entry(g.clone()).or_insert(0) += 1;
        }
    }
}

/// Score an existing active node for placing `pod`. Returns `None` if the node
/// fails the filter (capacity, required affinity/anti-affinity), or `Some(score)`
/// where higher is better.
fn score_existing_node(
    pod: &PodResources,
    node: &ActiveNode,
    active_nodes: &[ActiveNode],
    demands: &[PodResources],
    placed_demand_indices: &[Vec<usize>],
    all_placed: &[bool],
) -> Option<f64> {
    if !node.remaining.satisfies(&pod.resources) {
        return None;
    }
    if check_anti_affinity_required(pod, &node.topo, active_nodes, demands, placed_demand_indices)
    {
        return None;
    }
    if check_affinity_required(
        pod,
        &node.topo,
        active_nodes,
        demands,
        placed_demand_indices,
        all_placed,
    ) {
        return None;
    }
    let affinity =
        preferred_affinity_score(pod, &node.topo, active_nodes, demands, placed_demand_indices);
    // Existing nodes have zero marginal cost — always prefer them over
    // activating a new node.  Tiebreak by tightness (prefer filling fuller
    // nodes first for better bin-packing).  Use the most-constrained
    // dimension (CPU or memory) so memory-bottlenecked nodes are also
    // preferred when nearly full.
    let cpu_frac = node.remaining.cpu as f64 / node.total_resources.cpu.max(1) as f64;
    let mem_frac =
        node.remaining.memory_mib as f64 / node.total_resources.memory_mib.max(1) as f64;
    let remaining_frac = cpu_frac.max(mem_frac);
    Some(affinity * 1000.0 + 100.0 - remaining_frac)
}

/// Score a new offering for placing `pod`. Returns `None` if the offering fails
/// the filter (resources, budget, required affinity/anti-affinity), or `Some(score)`.
fn score_new_offering(
    pod: &PodResources,
    bo: &BoundedOffering,
    type_idx: usize,
    budget: &InstanceBudget,
    bounded: &[BoundedOffering],
    active_nodes: &[ActiveNode],
    demands: &[PodResources],
    placed_demand_indices: &[Vec<usize>],
    all_placed: &[bool],
) -> Option<f64> {
    if !bo.offering.satisfies(&pod.resources) {
        return None;
    }
    if !budget.can_activate(type_idx, bounded) {
        return None;
    }
    if check_anti_affinity_required(pod, &bo.labels, active_nodes, demands, placed_demand_indices)
    {
        return None;
    }
    if check_affinity_required(
        pod,
        &bo.labels,
        active_nodes,
        demands,
        placed_demand_indices,
        all_placed,
    ) {
        return None;
    }
    let affinity =
        preferred_affinity_score(pod, &bo.labels, active_nodes, demands, placed_demand_indices);
    // Score by cost-per-CPU so the solver prefers cost-efficient offerings
    // over tiny cheap ones.  Larger nodes with good cost-per-CPU ratios pack
    // more pods and produce fewer nodes.
    let cost_per_cpu = bo.offering.cost_per_hour / bo.offering.resources.cpu.max(1) as f64;
    Some(affinity * 1000.0 - cost_per_cpu)
}

/// Filter-score scheduler. For each demand (sorted cheapest-fit-first),
/// either place it on an existing active node or activate a new one.
///
/// 1. **Filter**: eliminate offerings that can't satisfy resources.
/// 2. **Score**: rank by cost (cheapest first), affinity, bin-pack tightness.
/// 3. **Place**: assign demand to best candidate.
#[instrument(skip_all, fields(demands = demands.len(), bounded_offerings = bounded.len(), existing = existing.len()))]
pub fn solve(
    demands: &[PodResources],
    bounded: &[BoundedOffering],
    existing: &[ExistingNode],
) -> PlacementSolution {
    if demands.is_empty() {
        debug!("no demands to solve");
        return PlacementSolution::NoDemands;
    }
    if bounded.is_empty() {
        warn!(
            demands = demands.len(),
            "no offerings available, all demands will be unmet"
        );
        return PlacementSolution::IncompletePlacement {
            nodes: vec![],
            unmet: demands.to_vec(),
        };
    }

    let offerings: Vec<Offering> = bounded.iter().map(|bo| bo.offering.clone()).collect();

    // Sort demands largest-first (first-fit-decreasing bin packing).
    let mut demand_order: Vec<usize> = (0..demands.len()).collect();
    demand_order.sort_by(|&a, &b| {
        let ra = &demands[a].resources;
        let rb = &demands[b].resources;
        // Primary: CPU descending, secondary: memory descending.
        rb.cpu.cmp(&ra.cpu).then(rb.memory_mib.cmp(&ra.memory_mib))
    });

    // Pre-seed active nodes from existing in-flight capacity.
    // These sit at the front of active_nodes and are excluded from the output.
    let pre_seeded_count = existing.len();
    let mut active_nodes: Vec<ActiveNode> = existing
        .iter()
        .map(|e| ActiveNode {
            type_idx: 0, // sentinel — never read for pre-seeded nodes
            total_resources: e.resources.clone(),
            remaining: e.resources.clone(),
            pods: Vec::new(),
            topo: e.labels.clone(),
        })
        .collect();
    // For each active node, which demand indices have been placed on it.
    let mut placed_demand_indices: Vec<Vec<usize>> =
        vec![Vec::new(); pre_seeded_count];
    let mut budget = InstanceBudget::new(bounded);
    let mut unmet: Vec<PodResources> = Vec::new();
    let mut all_placed: Vec<bool> = vec![false; demands.len()];

    for &demand_idx in &demand_order {
        let pod = &demands[demand_idx];

        // Score both existing nodes and new offerings in one pass so that
        // preferred affinity/anti-affinity can correctly prefer a new node
        // over an existing one in the same topology zone.
        enum Candidate {
            Existing(usize),
            New(usize),
        }

        let mut best: Option<(Candidate, f64)> = None;

        for (node_idx, node) in active_nodes.iter().enumerate() {
            if let Some(score) = score_existing_node(
                pod,
                node,
                &active_nodes,
                demands,
                &placed_demand_indices,
                &all_placed,
            )
                && best.as_ref().is_none_or(|(_, s)| score > *s) {
                    best = Some((Candidate::Existing(node_idx), score));
                }
        }

        for (type_idx, bo) in bounded.iter().enumerate() {
            if let Some(score) = score_new_offering(
                pod,
                bo,
                type_idx,
                &budget,
                bounded,
                &active_nodes,
                demands,
                &placed_demand_indices,
                &all_placed,
            )
                && best.as_ref().is_none_or(|(_, s)| score > *s) {
                    best = Some((Candidate::New(type_idx), score));
                }
        }

        match best {
            Some((Candidate::Existing(node_idx), _)) => {
                place_on_node(
                    &mut active_nodes[node_idx],
                    &mut placed_demand_indices[node_idx],
                    demand_idx,
                    pod,
                );
                all_placed[demand_idx] = true;
            }
            Some((Candidate::New(type_idx), _)) => {
                let bo = &bounded[type_idx];
                budget.activate(type_idx, bounded);

                let total_resources = bo.offering.resources.clone();
                let mut remaining = total_resources.clone();
                remaining.subtract(&pod.resources);

                active_nodes.push(ActiveNode {
                    type_idx,
                    total_resources,
                    remaining,
                    pods: vec![pod.id.clone()],
                    topo: bo.labels.clone(),
                });
                placed_demand_indices.push(vec![demand_idx]);
                all_placed[demand_idx] = true;
            }
            None => {
                debug!(pod = %pod.id, "pod unschedulable — no offering fits or budget exhausted");
                unmet.push(pod.clone());
            }
        }
    }

    let new_nodes = &active_nodes[pre_seeded_count..];
    let total_placed: usize = new_nodes.iter().map(|n| n.pods.len()).sum();
    let total_cpu_capacity: u32 = new_nodes
        .iter()
        .map(|n| n.total_resources.cpu)
        .sum();
    let total_cpu_used: u32 = new_nodes
        .iter()
        .map(|n| n.total_resources.cpu - n.remaining.cpu)
        .sum();
    let utilisation_pct = if total_cpu_capacity > 0 {
        (total_cpu_used as f64 / total_cpu_capacity as f64) * 100.0
    } else {
        0.0
    };

    // Per-type breakdown (new nodes only).
    let mut type_counts: BTreeMap<&str, (u32, u32, u32)> = BTreeMap::new(); // (nodes, pods, cpu_used)
    for node in new_nodes {
        let name = offerings[node.type_idx].instance_type.0.as_str();
        let entry = type_counts.entry(name).or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += node.pods.len() as u32;
        entry.2 += node.total_resources.cpu - node.remaining.cpu;
    }
    for (name, (count, pods, cpu)) in &type_counts {
        debug!(
            instance_type = name,
            nodes = count,
            pods = pods,
            cpu_used = cpu,
            "offering breakdown",
        );
    }

    let pre_seeded_placed: usize = active_nodes[..pre_seeded_count]
        .iter()
        .map(|n| n.pods.len())
        .sum();

    let nodes: Vec<PotentialNode> = active_nodes
        .into_iter()
        .skip(pre_seeded_count)
        .map(|n| PotentialNode {
            offering: offerings[n.type_idx].clone(),
            pods: n.pods,
        })
        .collect();

    let total_cost: f64 = nodes.iter().map(|n| n.offering.cost_per_hour).sum();
    info!(
        demands = demands.len(),
        offering_types = bounded.len(),
        pre_seeded = pre_seeded_count,
        pre_seeded_placed = pre_seeded_placed,
        new_nodes = nodes.len(),
        placed = total_placed,
        unmet = unmet.len(),
        cost_per_hour = total_cost,
        cpu_utilisation_pct = format!("{utilisation_pct:.1}"),
        cpu_used = total_cpu_used,
        cpu_capacity = total_cpu_capacity,
        "solve complete",
    );

    if unmet.is_empty() {
        PlacementSolution::AllPlaced(nodes)
    } else {
        PlacementSolution::IncompletePlacement { nodes, unmet }
    }
}

fn place_on_node(
    node: &mut ActiveNode,
    placed: &mut Vec<usize>,
    demand_idx: usize,
    pod: &PodResources,
) {
    node.remaining.subtract(&pod.resources);
    node.pods.push(pod.id.clone());
    placed.push(demand_idx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offering::{
        AffinityConstraint, AffinityKind, AffinityStrength, GpuModel, InstanceType, Location,
        PodId, Region, Resources, Zone,
    };

    fn test_location() -> Location {
        Location {
            region: Region("eu-central".into()),
            zone: Some(Zone("fsn1-dc14".into())),
        }
    }

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
            pod_labels: BTreeMap::new(),
            affinity_constraints: vec![],
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
            location: test_location(),
        }
    }

    fn bounded(o: Offering, max: u32) -> BoundedOffering {
        BoundedOffering {
            offering: o,
            max_instances: max,
            labels: BTreeMap::new(),
            type_group: None,
        }
    }

    #[test]
    fn empty_demands() {
        assert_eq!(
            solve(&[], &[bounded(offering("cpx22", 2, 4096, 0.01), 10)], &[]),
            PlacementSolution::NoDemands
        );
    }

    #[test]
    fn empty_offerings() {
        let demands = vec![demand("pod-a", 2, 4096)];
        assert_eq!(
            solve(&demands, &[], &[]),
            PlacementSolution::IncompletePlacement {
                nodes: vec![],
                unmet: demands
            }
        );
    }

    #[test]
    fn single_demand_single_offering() {
        let demands = vec![demand("pod-a", 2, 4096)];
        let offerings = vec![offering("cpx22", 2, 4096, 0.01)];
        assert_eq!(
            solve(&demands, &[bounded(offerings[0].clone(), 10)], &[]),
            PlacementSolution::AllPlaced(vec![PotentialNode {
                offering: offerings[0].clone(),
                pods: vec![demands[0].id.clone()]
            }])
        );
    }

    #[test]
    fn max_instances_limits_solver_output() {
        // 3 pods each needing their own 2-cpu node, but max_instances=2.
        let demands = vec![
            demand("pod-a", 2, 4096),
            demand("pod-b", 2, 4096),
            demand("pod-c", 2, 4096),
        ];
        let bounded_offerings = vec![bounded(offering("cpx22", 2, 4096, 0.01), 2)];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::IncompletePlacement { nodes, unmet } = result else {
            panic!("expected IncompletePlacement, got {result:?}");
        };
        assert_eq!(nodes.len(), 2);
        assert_eq!(unmet.len(), 1);
    }

    fn gpu_demand(
        name: &str,
        cpu: u32,
        memory_mib: u32,
        gpu: u32,
        model: GpuModel,
    ) -> PodResources {
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
            pod_labels: BTreeMap::new(),
            affinity_constraints: vec![],
        }
    }

    fn gpu_offering(
        name: &str,
        cpu: u32,
        memory_mib: u32,
        gpu: u32,
        model: GpuModel,
        cost: f64,
    ) -> Offering {
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
            location: test_location(),
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

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

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
        let demands = vec![gpu_demand("a100-pod", 4, 8192, 1, GpuModel::NvidiaA100)];
        let offerings = vec![gpu_offering(
            "gpu-t4",
            8,
            16384,
            1,
            GpuModel::NvidiaT4,
            0.50,
        )];
        let bounded_offerings: Vec<_> = offerings.iter().map(|o| bounded(o.clone(), 2)).collect();

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::IncompletePlacement { nodes: _, unmet } = result else {
            panic!("expected IncompletePlacement, got {result:?}");
        };
        assert_eq!(unmet.len(), 1);
        assert_eq!(unmet[0].id.name, "a100-pod");
    }

    // ── Affinity test helpers ───────────────────────────────────────

    fn demand_with_anti_affinity(
        name: &str,
        cpu: u32,
        memory_mib: u32,
        app_label: &str,
        topology_key: &str,
    ) -> PodResources {
        demand_with_anti_affinity_strength(
            name,
            cpu,
            memory_mib,
            app_label,
            topology_key,
            AffinityStrength::Required,
        )
    }

    fn demand_with_anti_affinity_strength(
        name: &str,
        cpu: u32,
        memory_mib: u32,
        app_label: &str,
        topology_key: &str,
        strength: AffinityStrength,
    ) -> PodResources {
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
            pod_labels: BTreeMap::from([("app".to_string(), app_label.to_string())]),
            affinity_constraints: vec![AffinityConstraint {
                kind: AffinityKind::AntiAffinity,
                strength,
                topology_key: topology_key.to_string(),
                match_labels: BTreeMap::from([("app".to_string(), app_label.to_string())]),
            }],
        }
    }

    fn demand_with_affinity(
        name: &str,
        cpu: u32,
        memory_mib: u32,
        app_label: &str,
        topology_key: &str,
    ) -> PodResources {
        demand_with_affinity_strength(
            name,
            cpu,
            memory_mib,
            app_label,
            topology_key,
            AffinityStrength::Required,
        )
    }

    fn demand_with_affinity_strength(
        name: &str,
        cpu: u32,
        memory_mib: u32,
        app_label: &str,
        topology_key: &str,
        strength: AffinityStrength,
    ) -> PodResources {
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
            pod_labels: BTreeMap::from([("app".to_string(), app_label.to_string())]),
            affinity_constraints: vec![AffinityConstraint {
                kind: AffinityKind::Affinity,
                strength,
                topology_key: topology_key.to_string(),
                match_labels: BTreeMap::from([("app".to_string(), app_label.to_string())]),
            }],
        }
    }

    fn bounded_with_labels(
        o: Offering,
        max: u32,
        labels: BTreeMap<String, String>,
    ) -> BoundedOffering {
        BoundedOffering {
            offering: o,
            max_instances: max,
            labels,
            type_group: None,
        }
    }

    fn zone_labels(zone: &str) -> BTreeMap<String, String> {
        BTreeMap::from([("topology.kubernetes.io/zone".to_string(), zone.to_string())])
    }

    // ── Affinity unit tests ─────────────────────────────────────────

    #[test]
    fn anti_affinity_spreads_across_zones() {
        let topo = "topology.kubernetes.io/zone";
        let demands = vec![
            demand_with_anti_affinity("web-0", 1, 1024, "web", topo),
            demand_with_anti_affinity("web-1", 1, 1024, "web", topo),
            demand_with_anti_affinity("web-2", 1, 1024, "web", topo),
        ];
        let bounded_offerings = vec![
            bounded_with_labels(offering("cpx22-a", 2, 4096, 0.01), 1, zone_labels("zone-a")),
            bounded_with_labels(offering("cpx22-b", 2, 4096, 0.01), 1, zone_labels("zone-b")),
            bounded_with_labels(offering("cpx22-c", 2, 4096, 0.01), 1, zone_labels("zone-c")),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };
        assert_eq!(nodes.len(), 3, "each pod on a separate zone node");
        for node in &nodes {
            assert_eq!(node.pods.len(), 1);
        }
    }

    #[test]
    fn affinity_co_locates_pods() {
        let topo = "topology.kubernetes.io/zone";
        let demands = vec![
            demand_with_affinity("cache-0", 1, 1024, "cache", topo),
            demand_with_affinity("cache-1", 1, 1024, "cache", topo),
        ];
        let bounded_offerings = vec![
            bounded_with_labels(offering("cpx22-a", 2, 4096, 0.01), 2, zone_labels("zone-a")),
            bounded_with_labels(offering("cpx22-b", 2, 4096, 0.01), 2, zone_labels("zone-b")),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        // Both pods should end up in nodes of the same zone.
        let mut zones: Vec<_> = nodes
            .iter()
            .flat_map(|n| {
                let bo = bounded_offerings
                    .iter()
                    .find(|bo| bo.offering.instance_type == n.offering.instance_type)
                    .unwrap();
                n.pods.iter().map(move |_| {
                    bo.labels
                        .get("topology.kubernetes.io/zone")
                        .unwrap()
                        .clone()
                })
            })
            .collect();
        zones.dedup();
        assert_eq!(zones.len(), 1, "all pods should be in the same zone");
    }

    #[test]
    fn anti_affinity_insufficient_zones() {
        let topo = "topology.kubernetes.io/zone";
        let demands = vec![
            demand_with_anti_affinity("web-0", 1, 1024, "web", topo),
            demand_with_anti_affinity("web-1", 1, 1024, "web", topo),
            demand_with_anti_affinity("web-2", 1, 1024, "web", topo),
        ];
        let bounded_offerings = vec![
            bounded_with_labels(offering("cpx22-a", 2, 4096, 0.01), 1, zone_labels("zone-a")),
            bounded_with_labels(offering("cpx22-b", 2, 4096, 0.01), 1, zone_labels("zone-b")),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::IncompletePlacement { nodes, unmet } = result else {
            panic!("expected IncompletePlacement, got {result:?}");
        };
        assert_eq!(nodes.len(), 2, "2 pods placed in 2 zones");
        assert_eq!(unmet.len(), 1, "1 pod unmet");
    }

    #[test]
    fn anti_affinity_only_affects_matching() {
        let topo = "topology.kubernetes.io/zone";
        let mut demands = vec![
            demand_with_anti_affinity("web-0", 1, 1024, "web", topo),
            demand_with_anti_affinity("web-1", 1, 1024, "web", topo),
        ];
        demands.push(PodResources {
            id: PodId {
                namespace: "default".into(),
                name: "api-0".into(),
            },
            uid: "uid-api-0".into(),
            resources: Resources {
                cpu: 1,
                memory_mib: 1024,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            pool: None,
            pod_labels: BTreeMap::from([("app".to_string(), "api".to_string())]),
            affinity_constraints: vec![],
        });

        let bounded_offerings = vec![
            bounded_with_labels(offering("cpx22-a", 2, 4096, 0.01), 2, zone_labels("zone-a")),
            bounded_with_labels(offering("cpx22-b", 2, 4096, 0.01), 2, zone_labels("zone-b")),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        // web pods must be in different zones
        let web_zones: Vec<_> = nodes
            .iter()
            .flat_map(|n| {
                let bo = bounded_offerings
                    .iter()
                    .find(|bo| bo.offering.instance_type == n.offering.instance_type)
                    .unwrap();
                let zone = bo
                    .labels
                    .get("topology.kubernetes.io/zone")
                    .unwrap()
                    .clone();
                n.pods
                    .iter()
                    .filter(|p| p.name.starts_with("web"))
                    .map(move |_| zone.clone())
            })
            .collect();
        assert_eq!(web_zones.len(), 2, "2 web pods placed");
        assert_ne!(
            web_zones[0], web_zones[1],
            "web pods must be in different zones"
        );

        let all_pods: Vec<_> = nodes.iter().flat_map(|n| &n.pods).collect();
        assert!(
            all_pods.iter().any(|p| p.name == "api-0"),
            "api pod should be placed"
        );
    }

    // ── Bin-packing tests ─────────────────────────────────────────

    /// Helper: compute CPU utilisation across all nodes.
    fn cpu_utilisation(nodes: &[PotentialNode], demands: &[PodResources]) -> f64 {
        let total_capacity: u32 = nodes.iter().map(|n| n.offering.resources.cpu).sum();
        let total_used: u32 = nodes
            .iter()
            .flat_map(|n| &n.pods)
            .map(|pid| demands.iter().find(|d| d.id == *pid).unwrap().resources.cpu)
            .sum();
        if total_capacity == 0 {
            return 0.0;
        }
        total_used as f64 / total_capacity as f64
    }

    #[test]
    fn packing_prefers_existing_over_new_cheap_node() {
        // First pod needs 4 cpu (only ccx63 fits). Second pod at 2 cpu could
        // go on a new cpx22 ($0.01) or pack onto the existing ccx63 (44 cpu free).
        // Should pack onto the ccx63 since it's already activated.
        let demands = vec![demand("big", 4, 4096), demand("small", 2, 2048)];
        let bounded_offerings = vec![
            bounded(offering("ccx63", 48, 196608, 0.50), 10),
            bounded(offering("cpx22", 2, 4096, 0.01), 10),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        assert_eq!(
            nodes.len(),
            1,
            "small pod should pack onto existing ccx63, not activate a cpx22"
        );
        assert_eq!(nodes[0].pods.len(), 2);
    }

    #[test]
    fn packing_mixed_sizes_fills_tightly() {
        // 10 pods at 3 cpu + 10 pods at 1 cpu on 4-cpu nodes.
        // Optimal: each 3-cpu pod pairs with a 1-cpu pod → 10 nodes.
        let mut demands: Vec<_> = (0..10)
            .map(|i| demand(&format!("big-{i}"), 3, 1024))
            .collect();
        demands.extend((0..10).map(|i| demand(&format!("small-{i}"), 1, 1024)));

        let bounded_offerings = vec![bounded(offering("cpx22", 4, 8192, 0.01), 20)];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        assert_eq!(nodes.iter().map(|n| n.pods.len()).sum::<usize>(), 20);
        // Optimal is 10 nodes (each 3+1=4). Allow 1 extra.
        assert!(
            nodes.len() <= 11,
            "expected ~10 nodes for 3+1 pairing, got {} (poor packing)",
            nodes.len()
        );
        assert!(
            cpu_utilisation(&nodes, &demands) > 0.90,
            "expected >90% CPU utilisation, got {:.1}%",
            cpu_utilisation(&nodes, &demands) * 100.0
        );
    }

    // ── Placement-expectation tests ─────────────────────────────
    //
    // These pin the solver's behaviour with realistic Hetzner offerings
    // so we catch regressions in offering selection, node count, packing
    // density, and total cost.

    /// Hetzner-realistic shared-x86 offerings (CX series).
    fn hetzner_cx() -> Vec<Offering> {
        vec![
            offering("cpx22", 2, 4_096, 0.0066),
            offering("cx32", 4, 8_192, 0.0106),
            offering("cx42", 8, 16_384, 0.0170),
            offering("cx52", 16, 32_768, 0.0314),
        ]
    }

    /// Full Hetzner lineup: CX + CCX (dedicated).
    fn hetzner_full() -> Vec<Offering> {
        let mut v = hetzner_cx();
        v.extend([
            offering("ccx13", 2, 8_192, 0.0386),
            offering("ccx23", 4, 16_384, 0.0475),
            offering("ccx33", 8, 32_768, 0.0900),
            offering("ccx43", 16, 65_536, 0.1789),
            offering("ccx53", 32, 131_072, 0.3568),
            offering("ccx63", 48, 196_608, 0.5347),
        ]);
        v
    }

    /// Summarise placement: (offering_type → node_count).
    fn type_counts(nodes: &[PotentialNode]) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for n in nodes {
            *counts
                .entry(n.offering.instance_type.0.clone())
                .or_insert(0) += 1;
        }
        counts
    }

    fn total_cost(nodes: &[PotentialNode]) -> f64 {
        nodes.iter().map(|n| n.offering.cost_per_hour).sum()
    }

    #[test]
    fn placement_100_small_pods_full_hetzner() {
        // 100 pods × 1 cpu across the full CX+CCX lineup.
        // cx52 has best cost/cpu ($0.00196) → solver should pick it.
        // 16 cpu / 1 per pod = 16 pods per node → ceil(100/16) = 7 nodes.
        let demands: Vec<_> = (0..100)
            .map(|i| demand(&format!("pod-{i}"), 1, 512))
            .collect();
        let bounded_offerings: Vec<_> = hetzner_full()
            .into_iter()
            .map(|o| bounded(o, 100))
            .collect();

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        let counts = type_counts(&nodes);
        assert_eq!(
            counts.keys().collect::<Vec<_>>(),
            vec!["cx52"],
            "expected solver to use cx52 (best cost/cpu), got {counts:?}"
        );
        assert_eq!(
            nodes.len(),
            7,
            "expected 7 cx52 nodes for 100 pods, got {}",
            nodes.len()
        );
        assert!(
            cpu_utilisation(&nodes, &demands) > 0.85,
            "expected >85% CPU utilisation, got {:.1}%",
            cpu_utilisation(&nodes, &demands) * 100.0
        );
    }

    #[test]
    fn placement_mixed_workload_picks_efficient_node() {
        // 20 pods × 8 cpu + 50 pods × 2 cpu = 260 cpu total.
        // cx52 has best cost/cpu. 8-cpu pods: 2 per cx52 → 10 nodes.
        // Then 50 × 2 cpu onto remaining cx52 capacity (each fresh cx52
        // holds 8 two-cpu pods) → ceil(50/8) = 7 nodes. Total ~17 nodes.
        let mut demands: Vec<_> = (0..20)
            .map(|i| demand(&format!("big-{i}"), 8, 8192))
            .collect();
        demands.extend((0..50).map(|i| demand(&format!("small-{i}"), 2, 1024)));

        let bounded_offerings: Vec<_> = hetzner_full()
            .into_iter()
            .map(|o| bounded(o, 100))
            .collect();

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        let counts = type_counts(&nodes);
        assert_eq!(
            counts.keys().collect::<Vec<_>>(),
            vec!["cx52"],
            "expected only cx52 (best cost/cpu), got {counts:?}"
        );
        // Optimal: 260 cpu / 16 per cx52 = 17 nodes (16×16=256, need 1 more).
        assert!(
            nodes.len() <= 17,
            "expected ~17 cx52 nodes for 260 cpu, got {}",
            nodes.len()
        );
        assert!(
            cpu_utilisation(&nodes, &demands) > 0.90,
            "expected >90% CPU utilisation, got {:.1}%",
            cpu_utilisation(&nodes, &demands) * 100.0
        );
    }

    #[test]
    fn placement_500_pods_total_cost_beats_naive() {
        // 500 pods × 2 cpu = 1000 cpu. Full lineup available.
        // cx52: 500 / 8 per node = 63 nodes × $0.0314 = $1.98/hr.
        // Naive cpx22: 500 / 1 per node = 500 nodes × $0.0066 = $3.30/hr.
        // Solver should beat naive-cheapest-per-node by a wide margin.
        let demands: Vec<_> = (0..500)
            .map(|i| demand(&format!("pod-{i}"), 2, 1024))
            .collect();
        let bounded_offerings: Vec<_> = hetzner_full()
            .into_iter()
            .map(|o| bounded(o, 500))
            .collect();

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        let counts = type_counts(&nodes);
        assert_eq!(
            counts.keys().collect::<Vec<_>>(),
            vec!["cx52"],
            "expected cx52 for best cost/cpu, got {counts:?}"
        );

        let cost = total_cost(&nodes);
        let naive_cost = 500.0 * 0.0066; // 500 cpx22 nodes
        assert!(
            cost < naive_cost * 0.7,
            "expected total cost ({cost:.2}) to be <70% of naive ({naive_cost:.2})"
        );

        // 500 pods / 8 per cx52 = 63 nodes.
        assert!(
            nodes.len() <= 63,
            "expected <=63 nodes, got {}",
            nodes.len()
        );
        assert!(
            cpu_utilisation(&nodes, &demands) > 0.95,
            "expected >95% CPU utilisation, got {:.1}%",
            cpu_utilisation(&nodes, &demands) * 100.0
        );
    }

    #[test]
    fn placement_memory_heavy_pods_dont_overcommit() {
        // 50 pods × 1 cpu, 8192 MiB each. Memory is the bottleneck.
        // cx52 (16 cpu, 32768 MiB): fits 4 by memory, 16 by cpu → 4 pods/node.
        // 50 / 4 = 13 nodes. CPU utilisation will be low (~25%) but memory
        // will be ~100%. This tests that we don't try to cram 16 pods on a
        // node just because CPU says we can.
        let demands: Vec<_> = (0..50)
            .map(|i| demand(&format!("pod-{i}"), 1, 8192))
            .collect();
        let bounded_offerings: Vec<_> = hetzner_cx().into_iter().map(|o| bounded(o, 100)).collect();

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };

        // Verify no node is memory-overcommitted.
        for (i, node) in nodes.iter().enumerate() {
            let mem_used: u32 = node
                .pods
                .iter()
                .map(|pid| {
                    demands
                        .iter()
                        .find(|d| d.id == *pid)
                        .unwrap()
                        .resources
                        .memory_mib
                })
                .sum();
            assert!(
                mem_used <= node.offering.resources.memory_mib,
                "node {i} ({}) memory overcommit: {mem_used} MiB used > {} MiB capacity",
                node.offering.instance_type.0,
                node.offering.resources.memory_mib,
            );
        }

        // cx52 is best cost/cpu, but memory-limited to 4 pods/node → 13 nodes.
        let counts = type_counts(&nodes);
        assert_eq!(
            counts.keys().collect::<Vec<_>>(),
            vec!["cx52"],
            "expected cx52 (best cost/cpu), got {counts:?}"
        );
        assert_eq!(
            nodes.len(),
            13,
            "expected 13 nodes (4 pods each by memory), got {}",
            nodes.len()
        );
    }

    // ── Type-group tests ──────────────────────────────────────────

    fn bounded_with_type_group(
        o: Offering,
        max: u32,
        labels: BTreeMap<String, String>,
        group: &str,
    ) -> BoundedOffering {
        BoundedOffering {
            offering: o,
            max_instances: max,
            labels,
            type_group: Some(group.to_string()),
        }
    }

    #[test]
    fn type_group_shares_max_across_zones() {
        let demands = vec![
            demand("pod-a", 2, 4096),
            demand("pod-b", 2, 4096),
            demand("pod-c", 2, 4096),
        ];
        let bounded_offerings = vec![
            bounded_with_type_group(
                offering("cpx22", 2, 4096, 0.01),
                2,
                zone_labels("zone-a"),
                "pool/cpx22",
            ),
            bounded_with_type_group(
                offering("cpx22", 2, 4096, 0.01),
                2,
                zone_labels("zone-b"),
                "pool/cpx22",
            ),
            bounded_with_type_group(
                offering("cpx22", 2, 4096, 0.01),
                2,
                zone_labels("zone-c"),
                "pool/cpx22",
            ),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::IncompletePlacement { nodes, unmet } = result else {
            panic!("expected IncompletePlacement, got {result:?}");
        };
        assert_eq!(nodes.len(), 2, "shared max=2 enforced across zones");
        assert_eq!(unmet.len(), 1, "1 pod unmet due to shared cap");
    }

    // ── Preferred affinity tests ──────────────────────────────────

    #[test]
    fn preferred_anti_affinity_spreads_when_possible() {
        let topo = "topology.kubernetes.io/zone";
        let demands = vec![
            demand_with_anti_affinity_strength(
                "web-0",
                1,
                1024,
                "web",
                topo,
                AffinityStrength::Preferred,
            ),
            demand_with_anti_affinity_strength(
                "web-1",
                1,
                1024,
                "web",
                topo,
                AffinityStrength::Preferred,
            ),
            demand_with_anti_affinity_strength(
                "web-2",
                1,
                1024,
                "web",
                topo,
                AffinityStrength::Preferred,
            ),
        ];
        let bounded_offerings = vec![
            bounded_with_labels(offering("cpx22-a", 2, 4096, 0.01), 2, zone_labels("zone-a")),
            bounded_with_labels(offering("cpx22-b", 2, 4096, 0.01), 2, zone_labels("zone-b")),
            bounded_with_labels(offering("cpx22-c", 2, 4096, 0.01), 2, zone_labels("zone-c")),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };
        assert_eq!(nodes.len(), 3, "each pod on a separate zone node");
        for node in &nodes {
            assert_eq!(node.pods.len(), 1);
        }
    }

    #[test]
    fn preferred_anti_affinity_allows_co_location_when_forced() {
        let topo = "topology.kubernetes.io/zone";
        let demands = vec![
            demand_with_anti_affinity_strength(
                "web-0",
                1,
                1024,
                "web",
                topo,
                AffinityStrength::Preferred,
            ),
            demand_with_anti_affinity_strength(
                "web-1",
                1,
                1024,
                "web",
                topo,
                AffinityStrength::Preferred,
            ),
            demand_with_anti_affinity_strength(
                "web-2",
                1,
                1024,
                "web",
                topo,
                AffinityStrength::Preferred,
            ),
        ];
        let bounded_offerings = vec![
            bounded_with_labels(offering("cpx22-a", 2, 4096, 0.01), 2, zone_labels("zone-a")),
            bounded_with_labels(offering("cpx22-b", 2, 4096, 0.01), 2, zone_labels("zone-b")),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced (soft constraint allows co-location), got {result:?}");
        };
        let total_pods: usize = nodes.iter().map(|n| n.pods.len()).sum();
        assert_eq!(total_pods, 3, "all 3 pods placed despite only 2 zones");
    }

    #[test]
    fn preferred_affinity_co_locates_when_possible() {
        let topo = "topology.kubernetes.io/zone";
        let demands = vec![
            demand_with_affinity_strength(
                "cache-0",
                1,
                1024,
                "cache",
                topo,
                AffinityStrength::Preferred,
            ),
            demand_with_affinity_strength(
                "cache-1",
                1,
                1024,
                "cache",
                topo,
                AffinityStrength::Preferred,
            ),
        ];
        let bounded_offerings = vec![
            bounded_with_labels(offering("cpx22-a", 2, 4096, 0.01), 2, zone_labels("zone-a")),
            bounded_with_labels(offering("cpx22-b", 2, 4096, 0.01), 2, zone_labels("zone-b")),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };
        let mut zones: Vec<_> = nodes
            .iter()
            .flat_map(|n| {
                let bo = bounded_offerings
                    .iter()
                    .find(|bo| bo.offering.instance_type == n.offering.instance_type)
                    .unwrap();
                n.pods.iter().map(move |_| {
                    bo.labels
                        .get("topology.kubernetes.io/zone")
                        .unwrap()
                        .clone()
                })
            })
            .collect();
        zones.dedup();
        assert_eq!(zones.len(), 1, "preferred affinity should co-locate pods");
    }

    // ── Region-level affinity tests ───────────────────────────────

    fn region_zone_labels(region: &str, zone: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "topology.kubernetes.io/region".to_string(),
                region.to_string(),
            ),
            ("topology.kubernetes.io/zone".to_string(), zone.to_string()),
        ])
    }

    #[test]
    fn anti_affinity_on_region_spreads_across_regions() {
        let topo = "topology.kubernetes.io/region";
        let demands = vec![
            demand_with_anti_affinity("svc-0", 1, 1024, "svc", topo),
            demand_with_anti_affinity("svc-1", 1, 1024, "svc", topo),
        ];
        let bounded_offerings = vec![
            bounded_with_labels(
                offering("cpx22-eu", 2, 4096, 0.01),
                2,
                region_zone_labels("eu-central", "fsn1"),
            ),
            bounded_with_labels(
                offering("cpx22-us", 2, 4096, 0.01),
                2,
                region_zone_labels("us-east", "ash1"),
            ),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };
        assert_eq!(nodes.len(), 2, "one pod per region");
        let regions: Vec<_> = nodes
            .iter()
            .map(|n| {
                let bo = bounded_offerings
                    .iter()
                    .find(|bo| bo.offering.instance_type == n.offering.instance_type)
                    .unwrap();
                bo.labels
                    .get("topology.kubernetes.io/region")
                    .unwrap()
                    .clone()
            })
            .collect();
        assert_ne!(
            regions[0], regions[1],
            "pods should be in different regions"
        );
    }

    #[test]
    fn region_and_zone_affinity_combined() {
        let region_topo = "topology.kubernetes.io/region";
        let zone_topo = "topology.kubernetes.io/zone";
        let demands: Vec<_> = (0..2)
            .map(|i| PodResources {
                id: PodId {
                    namespace: "default".into(),
                    name: format!("web-{i}"),
                },
                uid: format!("uid-web-{i}"),
                resources: Resources {
                    cpu: 1,
                    memory_mib: 1024,
                    ephemeral_storage_gib: None,
                    gpu: 0,
                    gpu_model: None,
                },
                pool: None,
                pod_labels: BTreeMap::from([("app".to_string(), "web".to_string())]),
                affinity_constraints: vec![
                    AffinityConstraint {
                        kind: AffinityKind::Affinity,
                        strength: AffinityStrength::Required,
                        topology_key: region_topo.to_string(),
                        match_labels: BTreeMap::from([("app".to_string(), "web".to_string())]),
                    },
                    AffinityConstraint {
                        kind: AffinityKind::AntiAffinity,
                        strength: AffinityStrength::Required,
                        topology_key: zone_topo.to_string(),
                        match_labels: BTreeMap::from([("app".to_string(), "web".to_string())]),
                    },
                ],
            })
            .collect();

        let bounded_offerings = vec![
            bounded_with_labels(
                offering("cpx22-eu-z1", 2, 4096, 0.01),
                2,
                region_zone_labels("eu-central", "fsn1"),
            ),
            bounded_with_labels(
                offering("cpx22-eu-z2", 2, 4096, 0.01),
                2,
                region_zone_labels("eu-central", "nbg1"),
            ),
            bounded_with_labels(
                offering("cpx22-us-z1", 2, 4096, 0.01),
                2,
                region_zone_labels("us-east", "ash1"),
            ),
            bounded_with_labels(
                offering("cpx22-us-z2", 2, 4096, 0.01),
                2,
                region_zone_labels("us-east", "iad1"),
            ),
        ];

        let result = solve(&demands, &bounded_offerings, &[]);
        let PlacementSolution::AllPlaced(nodes) = result else {
            panic!("expected AllPlaced, got {result:?}");
        };
        assert_eq!(nodes.len(), 2, "each pod on its own node");

        let mut pod_zones: Vec<(&str, &str)> = Vec::new();
        for node in &nodes {
            let bo = bounded_offerings
                .iter()
                .find(|bo| bo.offering.instance_type == node.offering.instance_type)
                .unwrap();
            let region = bo
                .labels
                .get("topology.kubernetes.io/region")
                .unwrap()
                .as_str();
            let zone = bo
                .labels
                .get("topology.kubernetes.io/zone")
                .unwrap()
                .as_str();
            for _ in &node.pods {
                pod_zones.push((region, zone));
            }
        }
        assert_eq!(pod_zones.len(), 2);
        assert_eq!(pod_zones[0].0, pod_zones[1].0, "same region (affinity)");
        assert_ne!(
            pod_zones[0].1, pod_zones[1].1,
            "different zones (anti-affinity)"
        );
    }

    // ── Pre-seeded existing node tests ──────────────────────────────

    fn existing(cpu: u32, memory_mib: u32) -> ExistingNode {
        ExistingNode {
            resources: Resources {
                cpu,
                memory_mib,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            labels: zone_labels("zone-a"),
        }
    }

    #[test]
    fn pre_seeded_absorbs_all_demand() {
        // T1: Two small pods fit on one pre-seeded node → 0 new nodes.
        let demands = vec![demand("a", 1, 1024), demand("b", 1, 1024)];
        let bounded_offerings = vec![bounded(offering("cpx22", 2, 4096, 0.01), 10)];
        let existing = vec![existing(2, 4096)];

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        assert!(nodes.is_empty(), "all demand absorbed by pre-seeded node");
    }

    #[test]
    fn pre_seeded_partial_coverage() {
        // T2: Pre-seeded absorbs 2 pods, 1 overflows to a new node.
        let demands = vec![
            demand("a", 1, 1024),
            demand("b", 1, 1024),
            demand("c", 1, 1024),
        ];
        let bounded_offerings = vec![bounded(offering("cpx22", 2, 4096, 0.01), 10)];
        let existing = vec![existing(2, 4096)];

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        assert_eq!(nodes.len(), 1, "one new node for the overflow pod");
    }

    #[test]
    fn pre_seeded_pod_too_large() {
        // T3: Pod exceeds pre-seeded capacity → must activate a new node.
        let demands = vec![demand("big", 4, 8192)];
        let bounded_offerings = vec![bounded(offering("cx42", 8, 16384, 0.05), 10)];
        let existing = vec![existing(2, 4096)];

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        assert_eq!(nodes.len(), 1, "new node activated for oversized pod");
    }

    #[test]
    fn pre_seeded_do_not_consume_budget() {
        // T5: max_instances=1, one pre-seeded node exists. The solver
        // should still be able to activate 1 new node (budget is only
        // for new activations, pre-seeded are already counted in occupied_counts).
        let demands = vec![demand("a", 2, 4096), demand("b", 2, 4096)];
        let bounded_offerings = vec![bounded(offering("cpx22", 2, 4096, 0.01), 1)];
        let existing = vec![existing(2, 4096)];

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        assert_eq!(nodes.len(), 1, "budget allows 1 new node despite pre-seeded");
    }

    #[test]
    fn pre_seeded_excluded_from_output() {
        // T6: Pre-seeded nodes must not appear in PotentialNode output.
        let demands = vec![demand("a", 1, 1024), demand("b", 1, 1024)];
        let bounded_offerings = vec![bounded(offering("cpx22", 2, 4096, 0.01), 10)];
        let existing = vec![existing(2, 4096)];

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        // All pods fit on the pre-seeded node → output must be empty.
        assert!(
            nodes.is_empty(),
            "pre-seeded nodes must not appear in output"
        );
    }

    #[test]
    fn pre_seeded_preferred_affinity_scoring() {
        // T7: Two pods with preferred affinity. One pre-seeded node in zone-a,
        // two new offerings in zone-a and zone-b. Second pod should prefer
        // zone-a (matching the first pod placed on the pre-seeded node).
        let topo = "topology.kubernetes.io/zone";
        let demands = vec![
            demand_with_affinity_strength("cache-0", 1, 1024, "cache", topo, AffinityStrength::Preferred),
            demand_with_affinity_strength("cache-1", 2, 2048, "cache", topo, AffinityStrength::Preferred),
        ];
        let bounded_offerings = vec![
            bounded_with_labels(offering("cpx22-a", 2, 4096, 0.01), 2, zone_labels("zone-a")),
            bounded_with_labels(offering("cpx22-b", 2, 4096, 0.01), 2, zone_labels("zone-b")),
        ];
        // Pre-seeded node in zone-a (only fits cache-0, not cache-1).
        let existing = vec![ExistingNode {
            resources: Resources {
                cpu: 1,
                memory_mib: 1024,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            labels: zone_labels("zone-a"),
        }];

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        // cache-1 needs a new node — should prefer zone-a for affinity.
        assert_eq!(nodes.len(), 1);
        let new_zone = bounded_offerings
            .iter()
            .find(|bo| bo.offering.instance_type == nodes[0].offering.instance_type)
            .unwrap()
            .labels
            .get("topology.kubernetes.io/zone")
            .unwrap();
        assert_eq!(new_zone, "zone-a", "preferred affinity should pick zone-a");
    }

    #[test]
    fn pre_seeded_gpu_mismatch_not_absorbed() {
        // T8: GPU pod should not be placed on a non-GPU pre-seeded node.
        let demands = vec![gpu_demand("gpu-job", 1, 4096, 1, GpuModel::NvidiaT4)];
        let bounded_offerings = vec![bounded(
            gpu_offering("gpu-t4", 8, 16384, 1, GpuModel::NvidiaT4, 0.50),
            2,
        )];
        let existing = vec![existing(4, 16384)]; // no GPU

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        assert_eq!(
            nodes.len(),
            1,
            "GPU pod must not be absorbed by non-GPU pre-seeded node"
        );
    }

    #[test]
    fn pre_seeded_gpu_model_mismatch_not_absorbed() {
        // Pre-seeded node has 1x T4 GPU, pod needs 1x A100 GPU.
        // GPU count matches but model doesn't — pod must go to a new node.
        let demands = vec![gpu_demand("a100-job", 2, 4096, 1, GpuModel::NvidiaA100)];
        let bounded_offerings = vec![bounded(
            gpu_offering("gpu-a100", 8, 16384, 1, GpuModel::NvidiaA100, 2.00),
            2,
        )];
        let existing = vec![ExistingNode {
            resources: Resources {
                cpu: 8,
                memory_mib: 16384,
                ephemeral_storage_gib: None,
                gpu: 1,
                gpu_model: Some(GpuModel::NvidiaT4),
            },
            labels: zone_labels("zone-a"),
        }];

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        assert_eq!(
            nodes.len(),
            1,
            "GPU model mismatch — pod must not be absorbed by pre-seeded T4 node"
        );
    }

    #[test]
    fn ephemeral_storage_not_overcommitted() {
        // 3 pods each needing 10 GiB storage. Pre-seeded node has 20 GiB total.
        // Only 2 fit by storage; the third must overflow to a new node.
        let storage_demand = |name: &str| PodResources {
            id: PodId {
                namespace: "default".into(),
                name: name.into(),
            },
            uid: format!("uid-{name}"),
            resources: Resources {
                cpu: 1,
                memory_mib: 1024,
                ephemeral_storage_gib: Some(10),
                gpu: 0,
                gpu_model: None,
            },
            pool: None,
            pod_labels: BTreeMap::new(),
            affinity_constraints: vec![],
        };
        let demands = vec![storage_demand("a"), storage_demand("b"), storage_demand("c")];
        let storage_resources = Resources {
            cpu: 4,
            memory_mib: 8192,
            ephemeral_storage_gib: Some(20),
            gpu: 0,
            gpu_model: None,
        };
        let bounded_offerings = vec![bounded(
            Offering {
                instance_type: InstanceType("cx32".into()),
                resources: storage_resources.clone(),
                cost_per_hour: 0.01,
                location: test_location(),
            },
            10,
        )];
        let existing = vec![ExistingNode {
            resources: storage_resources,
            labels: zone_labels("zone-a"),
        }];

        let result = solve(&demands, &bounded_offerings, &existing);
        let nodes = match result {
            PlacementSolution::AllPlaced(n) => n,
            other => panic!("expected AllPlaced, got {other:?}"),
        };
        assert_eq!(
            nodes.len(),
            1,
            "third pod should overflow to new node (ephemeral storage limit)"
        );
    }
}
