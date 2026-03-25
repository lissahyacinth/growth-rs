use std::collections::BTreeMap;

use crate::offering::{AffinityKind, AffinityStrength, PodResources};

use super::ActiveNode;

fn labels_match(
    match_labels: &BTreeMap<String, String>,
    pod_labels: &BTreeMap<String, String>,
) -> bool {
    match_labels
        .iter()
        .all(|(k, v)| pod_labels.get(k) == Some(v))
}

/// Check if placing `pod` on a node in topology zone `node_topo` would violate
/// required anti-affinity, given what's already placed.
pub(super) fn check_anti_affinity_required(
    pod: &PodResources,
    node_topo: &BTreeMap<String, String>,
    active_nodes: &[ActiveNode],
    demands: &[PodResources],
    placed_demand_indices: &[Vec<usize>],
) -> bool {
    for ac in &pod.affinity_constraints {
        if ac.kind != AffinityKind::AntiAffinity || ac.strength != AffinityStrength::Required {
            continue;
        }
        let Some(my_zone_val) = node_topo.get(&ac.topology_key) else {
            continue;
        };
        for (node_idx, node) in active_nodes.iter().enumerate() {
            let Some(their_zone_val) = node.topo.get(&ac.topology_key) else {
                continue;
            };
            if my_zone_val != their_zone_val {
                continue;
            }
            // Same topology value — check if any placed pod on this node matches.
            for &demand_idx in &placed_demand_indices[node_idx] {
                if labels_match(&ac.match_labels, &demands[demand_idx].pod_labels) {
                    return true; // violation
                }
            }
        }
    }
    false
}

/// Check if placing `pod` on a node in topology zone `node_topo` would violate
/// required affinity (co-location). Returns true if a required affinity
/// constraint can NOT be satisfied (i.e. no matching pod is in the same zone).
///
/// This is a soft check: if no matching pod has been placed yet, we allow it
/// (the first pod has to go somewhere). We only reject if matching pods exist
/// but are all in OTHER zones.
pub(super) fn check_affinity_required(
    pod: &PodResources,
    node_topo: &BTreeMap<String, String>,
    active_nodes: &[ActiveNode],
    demands: &[PodResources],
    placed_demand_indices: &[Vec<usize>],
    _all_placed_set: &[bool],
) -> bool {
    for ac in &pod.affinity_constraints {
        if ac.kind != AffinityKind::Affinity || ac.strength != AffinityStrength::Required {
            continue;
        }
        let Some(my_zone_val) = node_topo.get(&ac.topology_key) else {
            continue;
        };
        // Find all placed pods that match our selector.
        let mut any_matching_placed = false;
        let mut any_in_same_zone = false;
        for (node_idx, node) in active_nodes.iter().enumerate() {
            for &demand_idx in &placed_demand_indices[node_idx] {
                if !labels_match(&ac.match_labels, &demands[demand_idx].pod_labels) {
                    continue;
                }
                any_matching_placed = true;
                if let Some(their_val) = node.topo.get(&ac.topology_key)
                    && their_val == my_zone_val
                {
                    any_in_same_zone = true;
                }
            }
        }
        // Also check unplaced demands that match — if none are placed yet, allow freely.
        if !any_matching_placed {
            continue;
        }
        if !any_in_same_zone {
            return true; // violation — matching pods exist but all in other zones.
        }
    }
    false
}

/// Score a candidate (active node or new offering) for preferred affinity/anti-affinity.
/// Higher score = better. Penalties are negative.
pub(super) fn preferred_affinity_score(
    pod: &PodResources,
    node_topo: &BTreeMap<String, String>,
    active_nodes: &[ActiveNode],
    demands: &[PodResources],
    placed_demand_indices: &[Vec<usize>],
) -> f64 {
    let mut score = 0.0;
    for ac in &pod.affinity_constraints {
        if ac.strength != AffinityStrength::Preferred {
            continue;
        }
        let Some(my_zone_val) = node_topo.get(&ac.topology_key) else {
            continue;
        };
        // Count matching pods in the same zone and total matching placed.
        let mut same_zone_count = 0usize;
        for (node_idx, node) in active_nodes.iter().enumerate() {
            let in_same_zone = node
                .topo
                .get(&ac.topology_key)
                .is_some_and(|v| v == my_zone_val);
            if !in_same_zone {
                continue;
            }
            for &demand_idx in &placed_demand_indices[node_idx] {
                if labels_match(&ac.match_labels, &demands[demand_idx].pod_labels) {
                    same_zone_count += 1;
                }
            }
        }
        match ac.kind {
            AffinityKind::AntiAffinity => {
                // Penalise co-location: more matching pods in same zone = worse.
                score -= same_zone_count as f64 * 100.0;
            }
            AffinityKind::Affinity => {
                // Reward co-location: more matching pods in same zone = better.
                score += same_zone_count as f64 * 100.0;
            }
        }
    }
    score
}
