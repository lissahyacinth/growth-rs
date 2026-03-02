use std::collections::HashSet;

use k8s_openapi::api::core::v1::{Node, Pod};

use crate::crds::node_pool::ServerTypeConfig;
use crate::crds::node_removal_request::{NodeRemovalRequest, NodeRemovalRequestPhase};
use crate::offering::{INSTANCE_TYPE_LABEL, POOL_LABEL, pod_pool_selector};

use super::super::pods::is_daemonset_pod;

/// A node that has been identified as idle and eligible for removal.
#[derive(Debug)]
pub struct IdleNode {
    pub node_name: String,
    pub node_uid: String,
    pub pool: String,
    pub instance_type: String,
}

/// Information about a pool's server types, used for min-count enforcement.
#[derive(Debug)]
pub struct PoolMinCounts {
    pub pool_name: String,
    pub server_types: Vec<ServerTypeConfig>,
}

/// Check whether a node is idle: no non-DaemonSet pods with a Growth pool selector
/// are running on it.
pub fn is_node_idle(node_name: &str, pods: &[Pod]) -> bool {
    !pods.iter().any(|pod| {
        // Pod must be on this node
        let on_this_node = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            == Some(node_name);

        if !on_this_node {
            return false;
        }

        // Skip DaemonSet pods
        if is_daemonset_pod(pod) {
            return false;
        }

        // Must have a Growth pool selector to count as workload
        pod_pool_selector(pod).is_some()
    })
}

/// Find Growth-managed nodes that are idle and not already tracked by a NodeRemovalRequest.
///
/// Respects `NodePool.server_types[].min` counts — won't mark a node idle if removing
/// it would bring the pool below its minimum for that instance type.
pub fn find_idle_nodes(
    nodes: &[Node],
    pods: &[Pod],
    existing_nrrs: &[NodeRemovalRequest],
    pool_mins: &[PoolMinCounts],
) -> Vec<IdleNode> {
    // Build set of node names already tracked by active NRRs.
    let tracked_nodes: HashSet<&str> = existing_nrrs
        .iter()
        .map(|nrr| nrr.spec.node_name.as_str())
        .collect();

    // Count current nodes per pool per instance type.
    let mut node_counts: std::collections::HashMap<(&str, &str), u32> =
        std::collections::HashMap::new();
    for node in nodes {
        let labels = node.metadata.labels.as_ref();
        if let (Some(pool), Some(itype)) = (
            labels.and_then(|l| l.get(POOL_LABEL)),
            labels.and_then(|l| l.get(INSTANCE_TYPE_LABEL)),
        ) {
            *node_counts
                .entry((pool.as_str(), itype.as_str()))
                .or_insert(0) += 1;
        }
    }

    // Build min-count lookup: (pool, instance_type) -> min.
    let mut min_lookup: std::collections::HashMap<(&str, &str), u32> =
        std::collections::HashMap::new();
    for pm in pool_mins {
        for st in &pm.server_types {
            min_lookup.insert((pm.pool_name.as_str(), st.name.as_str()), st.min);
        }
    }

    // Track how many nodes we've "reserved for removal" per (pool, instance_type)
    // so we don't go below min in a single scan.
    let mut removal_count: std::collections::HashMap<(&str, &str), u32> =
        std::collections::HashMap::new();

    // Count existing NRRs (Pending/Deprovisioning) against the removal budget so we
    // don't mark more nodes for removal than the pool's min allows.
    for nrr in existing_nrrs {
        if nrr.phase() == NodeRemovalRequestPhase::CouldNotRemove {
            continue; // Terminal — node still exists, counts toward pool capacity, not removals.
        }
        *removal_count
            .entry((nrr.spec.pool.as_str(), nrr.spec.instance_type.as_str()))
            .or_insert(0) += 1;
    }

    let mut idle = Vec::new();

    for node in nodes {
        let name = match node.metadata.name.as_deref() {
            Some(n) => n,
            None => continue,
        };
        let uid = match node.metadata.uid.as_deref() {
            Some(u) => u,
            None => continue,
        };

        // Must be Growth-managed.
        let labels = match node.metadata.labels.as_ref() {
            Some(l) => l,
            None => continue,
        };
        if labels.get("app.kubernetes.io/managed-by").map(|s| s.as_str()) != Some("growth") {
            continue;
        }

        let pool = match labels.get(POOL_LABEL) {
            Some(p) => p.as_str(),
            None => continue,
        };
        let instance_type = match labels.get(INSTANCE_TYPE_LABEL) {
            Some(it) => it.as_str(),
            None => continue,
        };

        // Already tracked by an NRR.
        if tracked_nodes.contains(name) {
            continue;
        }

        // Check if idle.
        if !is_node_idle(name, pods) {
            continue;
        }

        // Check min counts: current_count - already_removing > min.
        let current = node_counts.get(&(pool, instance_type)).copied().unwrap_or(0);
        let already_removing = removal_count.get(&(pool, instance_type)).copied().unwrap_or(0);
        let min = min_lookup.get(&(pool, instance_type)).copied().unwrap_or(0);

        if current.saturating_sub(already_removing) <= min {
            continue;
        }

        *removal_count.entry((pool, instance_type)).or_insert(0) += 1;

        idle.push(IdleNode {
            node_name: name.to_string(),
            node_uid: uid.to_string(),
            pool: pool.to_string(),
            instance_type: instance_type.to_string(),
        });
    }

    idle
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use k8s_openapi::api::core::v1::{NodeSpec, NodeStatus, PodSpec};
    use kube::api::ObjectMeta;

    use crate::crds::node_pool::ServerTypeConfig;
    use crate::crds::node_removal_request::{NodeRemovalRequest, NodeRemovalRequestSpec};

    fn growth_node(name: &str, pool: &str, instance_type: &str) -> Node {
        Node {
            metadata: ObjectMeta {
                name: Some(name.into()),
                uid: Some(format!("uid-{name}")),
                labels: Some(BTreeMap::from([
                    ("app.kubernetes.io/managed-by".into(), "growth".into()),
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
                instance_type: instance_type.into(),
            },
        )
    }

    #[test]
    fn idle_node_with_no_pods() {
        let nodes = vec![growth_node("node-1", "default", "cx22")];
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

        let nodes = vec![growth_node("node-1", "default", "cx22")];
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
        let nodes = vec![growth_node("node-1", "default", "cx22")];
        let existing_nrrs = vec![make_nrr("node-1", "default", "cx22")];
        let idle = find_idle_nodes(&nodes, &[], &existing_nrrs, &[]);
        assert!(idle.is_empty());
    }

    #[test]
    fn non_growth_node_is_skipped() {
        let mut node = growth_node("external-node", "default", "cx22");
        node.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("app.kubernetes.io/managed-by".into(), "other".into());
        let idle = find_idle_nodes(&[node], &[], &[], &[]);
        assert!(idle.is_empty());
    }

    #[test]
    fn min_count_prevents_removal() {
        let nodes = vec![growth_node("node-1", "default", "cx22")];
        let pool_mins = vec![PoolMinCounts {
            pool_name: "default".into(),
            server_types: vec![ServerTypeConfig {
                name: "cx22".into(),
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
            growth_node("node-1", "default", "cx22"),
            growth_node("node-2", "default", "cx22"),
        ];
        let pool_mins = vec![PoolMinCounts {
            pool_name: "default".into(),
            server_types: vec![ServerTypeConfig {
                name: "cx22".into(),
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
            growth_node("node-1", "default", "cx22"),
            growth_node("node-2", "default", "cx22"),
            growth_node("node-3", "default", "cx22"),
        ];
        let pool_mins = vec![PoolMinCounts {
            pool_name: "default".into(),
            server_types: vec![ServerTypeConfig {
                name: "cx22".into(),
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
            growth_node("idle-1", "default", "cx22"),
            growth_node("busy-1", "default", "cx22"),
            growth_node("idle-2", "default", "cx22"),
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
}
