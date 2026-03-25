use std::collections::{HashMap, HashSet};

use k8s_openapi::api::core::v1::{Node, Pod};

use crate::controller::pods::is_daemonset_pod;
use crate::offering::{INSTANCE_TYPE_LABEL, NodeReference, POOL_LABEL, pod_pool_selector};
use crate::resources::node_pool::{NodePool, ServerTypeConfig};
use crate::resources::node_removal_request::{NodeRemovalRequest, NodeRemovalRequestPhase};

/// Information about a pool's server types, used for min-count enforcement.
#[derive(Debug)]
pub struct PoolMinCounts {
    pub pool_name: String,
    pub server_types: Vec<ServerTypeConfig>,
}

impl PoolMinCounts {
    pub fn from_node_pools(pools: &[NodePool]) -> Vec<Self> {
        pools
            .iter()
            .filter_map(|np| {
                np.metadata.name.as_ref().map(|name| Self {
                    pool_name: name.clone(),
                    server_types: np.spec.server_types.clone(),
                })
            })
            .collect()
    }
}

/// Tracks per-(pool, instance_type) removal capacity to enforce min-count constraints.
///
/// Prevents removing more nodes than the pool allows by accounting for both
/// existing in-flight removals (active NRRs) and nodes selected during this scan.
struct RemovalBudget {
    node_counts: HashMap<(String, String), u32>, // (Pool, InstanceType) -> count
    min_lookup: HashMap<(String, String), u32>,  // (Pool, InstanceType) -> min count
    removal_count: HashMap<(String, String), u32>, // (Pool, InstanceType) -> removal count
}

impl RemovalBudget {
    fn new(
        nodes: &[Node],
        existing_nrrs: &[NodeRemovalRequest],
        pool_mins: &[PoolMinCounts],
    ) -> Self {
        let mut node_counts: HashMap<(String, String), u32> = HashMap::new();
        for node in nodes {
            let labels = node.metadata.labels.as_ref();
            if let (Some(pool), Some(itype)) = (
                labels.and_then(|l| l.get(POOL_LABEL)),
                labels.and_then(|l| l.get(INSTANCE_TYPE_LABEL)),
            ) {
                *node_counts
                    .entry((pool.clone(), itype.clone()))
                    .or_insert(0) += 1;
            }
        }

        let mut min_lookup: HashMap<(String, String), u32> = HashMap::new();
        for pm in pool_mins {
            for st in &pm.server_types {
                min_lookup.insert((pm.pool_name.clone(), st.name.clone()), st.min);
            }
        }

        let mut removal_count: HashMap<(String, String), u32> = HashMap::new();
        for nrr in existing_nrrs {
            if nrr.phase() == NodeRemovalRequestPhase::CouldNotRemove {
                continue;
            }
            *removal_count
                .entry((nrr.spec.pool.clone(), nrr.spec.instance_type.0.clone()))
                .or_insert(0) += 1;
        }

        Self {
            node_counts,
            min_lookup,
            removal_count,
        }
    }

    /// Check if removing one more node of this (pool, instance_type) is allowed,
    /// and if so, reserve the slot.
    fn can_reserve(&mut self, pool: &str, instance_type: &str) -> bool {
        let key = (pool.to_string(), instance_type.to_string());
        let current = self.node_counts.get(&key).copied().unwrap_or(0);
        let already_removing = self.removal_count.get(&key).copied().unwrap_or(0);
        let min = self.min_lookup.get(&key).copied().unwrap_or(0);

        if current.saturating_sub(already_removing) <= min {
            return false;
        }

        *self.removal_count.entry(key).or_insert(0) += 1;
        true
    }
}

/// Check whether a node is idle: no non-DaemonSet pods with a Growth pool selector
/// are running on it.
///
/// Only pods that opt into a Growth pool (via `nodeSelector` with `growth.vettrdev.com/pool`)
/// are considered workload. Non-Growth pods on Growth-managed nodes (e.g. monitoring agents,
/// pods scheduled without a Growth pool selector) are invisible to this check and will be
/// evicted when the node is removed during scale-down.
pub fn is_node_idle(node_name: &str, pods: &[Pod]) -> bool {
    !pods.iter().any(|pod| {
        let on_this_node =
            pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(node_name);
        // Pod must be on this node, not be placed as part of a DaemonSet,
        //  and have a Growth pool selector
        (on_this_node && !is_daemonset_pod(pod)) && pod_pool_selector(pod).is_some()
    })
}

/// Find Idle Growth-managed nodes, and provide them as 'Candidates' for Removal.
pub fn find_idle_nodes(
    nodes: &[Node],
    pods: &[Pod],
    existing_nrrs: &[NodeRemovalRequest],
    pool_mins: &[PoolMinCounts],
) -> Vec<NodeReference> {
    let tracked_nodes: HashSet<&str> = existing_nrrs
        .iter()
        .map(|nrr| nrr.spec.node_name.as_str())
        .collect();

    let mut budget = RemovalBudget::new(nodes, existing_nrrs, pool_mins);

    nodes
        .iter()
        .filter_map(|node| {
            let candidate = NodeReference::from_node(node)?;
            // It's not already being removed
            (!tracked_nodes.contains(candidate.node_name.as_str())
            // It's considered idle based on the pods on it
            && is_node_idle(&candidate.node_name, pods)
            // And we're not going to go below the pool min for this instance type
            //  by removing it
            && budget.can_reserve(&candidate.pool, &candidate.instance_type))
            .then_some(candidate)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use k8s_openapi::api::core::v1::{NodeSpec, NodeStatus, PodSpec};
    use kube::api::ObjectMeta;

    use crate::offering::{InstanceType, MANAGED_BY_LABEL, MANAGED_BY_VALUE};
    use crate::resources::node_pool::ServerTypeConfig;
    use crate::resources::node_removal_request::{NodeRemovalRequest, NodeRemovalRequestSpec};

    fn growth_node(name: &str, pool: &str, instance_type: &str) -> Node {
        Node {
            metadata: ObjectMeta {
                name: Some(name.into()),
                uid: Some(format!("uid-{name}")),
                labels: Some(BTreeMap::from([
                    (MANAGED_BY_LABEL.into(), MANAGED_BY_VALUE.into()),
                    (POOL_LABEL.into(), pool.into()),
                    (INSTANCE_TYPE_LABEL.into(), instance_type.into()),
                ])),
                ..Default::default()
            },
            spec: Some(NodeSpec::default()),
            status: Some(NodeStatus::default()),
        }
    }

    fn workload_pod(name: &str, node: &str, pool: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("default".into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some(node.into()),
                node_selector: Some(BTreeMap::from([(POOL_LABEL.into(), pool.into())])),
                containers: vec![],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn daemonset_pod(name: &str, node: &str) -> Pod {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("kube-system".into()),
                owner_references: Some(vec![OwnerReference {
                    kind: "DaemonSet".into(),
                    name: "kube-proxy".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some(node.into()),
                node_selector: Some(BTreeMap::from([(POOL_LABEL.into(), "default".into())])),
                containers: vec![],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn make_nrr(node_name: &str, pool: &str, instance_type: &str) -> NodeRemovalRequest {
        NodeRemovalRequest::new(
            &format!("nrr-{node_name}"),
            NodeRemovalRequestSpec {
                node_name: node_name.into(),
                pool: pool.into(),
                instance_type: InstanceType(instance_type.into()),
            },
        )
    }

    #[test]
    fn idle_node_with_no_pods() {
        let nodes = vec![growth_node("node-1", "default", "cpx22")];
        let pods: Vec<Pod> = vec![];
        assert!(is_node_idle("node-1", &pods));

        let idle = find_idle_nodes(&nodes, &pods, &[], &[]);
        assert_eq!(idle.len(), 1);
        assert_eq!(idle[0].node_name, "node-1");
    }

    #[test]
    fn node_with_workload_is_not_idle() {
        let pods = vec![workload_pod("pod-1", "node-1", "default")];
        assert!(!is_node_idle("node-1", &pods));

        let nodes = vec![growth_node("node-1", "default", "cpx22")];
        let idle = find_idle_nodes(&nodes, &pods, &[], &[]);
        assert!(idle.is_empty());
    }

    #[test]
    fn node_with_only_daemonset_pods_is_idle() {
        let pods = vec![daemonset_pod("kube-proxy-abc", "node-1")];
        assert!(is_node_idle("node-1", &pods));
    }

    #[test]
    fn already_tracked_node_is_skipped() {
        let nodes = vec![growth_node("node-1", "default", "cpx22")];
        let existing_nrrs = vec![make_nrr("node-1", "default", "cpx22")];
        let idle = find_idle_nodes(&nodes, &[], &existing_nrrs, &[]);
        assert!(idle.is_empty());
    }

    #[test]
    fn non_growth_node_is_skipped() {
        let mut node = growth_node("external-node", "default", "cpx22");
        node.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(MANAGED_BY_LABEL.into(), "other".into());
        let idle = find_idle_nodes(&[node], &[], &[], &[]);
        assert!(idle.is_empty());
    }

    #[test]
    fn min_count_prevents_removal() {
        let nodes = vec![growth_node("node-1", "default", "cpx22")];
        let pool_mins = vec![PoolMinCounts {
            pool_name: "default".into(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".into(),
                max: 10,
                min: 1,
            }],
        }];
        // Only 1 node, min=1 → can't remove.
        let idle = find_idle_nodes(&nodes, &[], &[], &pool_mins);
        assert!(idle.is_empty());
    }

    #[test]
    fn min_count_allows_removal_when_above_min() {
        let nodes = vec![
            growth_node("node-1", "default", "cpx22"),
            growth_node("node-2", "default", "cpx22"),
        ];
        let pool_mins = vec![PoolMinCounts {
            pool_name: "default".into(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".into(),
                max: 10,
                min: 1,
            }],
        }];
        // 2 nodes, min=1 → can remove 1.
        let idle = find_idle_nodes(&nodes, &[], &[], &pool_mins);
        assert_eq!(idle.len(), 1);
    }

    #[test]
    fn min_count_limits_batch_removal() {
        let nodes = vec![
            growth_node("node-1", "default", "cpx22"),
            growth_node("node-2", "default", "cpx22"),
            growth_node("node-3", "default", "cpx22"),
        ];
        let pool_mins = vec![PoolMinCounts {
            pool_name: "default".into(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".into(),
                max: 10,
                min: 2,
            }],
        }];
        // 3 nodes, min=2 → can remove 1.
        let idle = find_idle_nodes(&nodes, &[], &[], &pool_mins);
        assert_eq!(idle.len(), 1);
    }

    #[test]
    fn mixed_idle_and_busy_nodes() {
        let nodes = vec![
            growth_node("idle-1", "default", "cpx22"),
            growth_node("busy-1", "default", "cpx22"),
            growth_node("idle-2", "default", "cpx22"),
        ];
        let pods = vec![workload_pod("pod-1", "busy-1", "default")];
        let idle = find_idle_nodes(&nodes, &pods, &[], &[]);
        assert_eq!(idle.len(), 2);
        let names: Vec<&str> = idle.iter().map(|n| n.node_name.as_str()).collect();
        assert!(names.contains(&"idle-1"));
        assert!(names.contains(&"idle-2"));
    }

    #[test]
    fn pod_on_different_node_doesnt_affect_idle_check() {
        let pods = vec![workload_pod("pod-1", "node-2", "default")];
        assert!(is_node_idle("node-1", &pods));
    }

    #[test]
    fn could_not_remove_nrr_does_not_block_removal() {
        use crate::resources::node_removal_request::{
            NodeRemovalRequestPhase, NodeRemovalRequestStatus,
        };

        let nodes = vec![
            growth_node("node-1", "default", "cpx22"),
            growth_node("node-2", "default", "cpx22"),
        ];
        let pool_mins = vec![PoolMinCounts {
            pool_name: "default".into(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".into(),
                max: 10,
                min: 1,
            }],
        }];
        // node-1 has a CouldNotRemove NRR — it should NOT count as an in-flight removal.
        let mut nrr = make_nrr("node-1", "default", "cpx22");
        nrr.status = Some(NodeRemovalRequestStatus {
            phase: NodeRemovalRequestPhase::CouldNotRemove,
            removal_attempts: 3,
            last_transition_time: None,
        });
        // 2 nodes, min=1, 1 CouldNotRemove NRR (excluded) → can still remove 1.
        let idle = find_idle_nodes(&nodes, &[], &[nrr], &pool_mins);
        // node-1 is tracked (skipped), node-2 is idle and removable.
        assert_eq!(idle.len(), 1);
        assert_eq!(idle[0].node_name, "node-2");
    }
}
