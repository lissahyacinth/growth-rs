use std::collections::HashMap;

use k8s_openapi::api::core::v1::Pod;

use crate::offering::Offering;

/// Check whether a Pod has the `PodScheduled=False/Unschedulable` condition.
pub fn is_pod_unschedulable(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|conditions| {
            conditions.iter().any(|c| {
                c.type_ == "PodScheduled"
                    && c.status == "False"
                    && c.reason.as_deref() == Some("Unschedulable")
            })
        })
        .unwrap_or(false)
}

pub fn is_daemonset_pod(pod: &Pod) -> bool {
    pod.metadata
        .owner_references
        .as_ref()
        .map(|refs| refs.iter().any(|r| r.kind == "DaemonSet"))
        .unwrap_or(false)
}

/// Merge NodeRequest in-flight counts and existing node counts into a single occupied map.
pub fn merge_occupied_counts(
    nr_counts: HashMap<String, HashMap<String, u32>>,
    node_counts: HashMap<String, HashMap<String, u32>>,
) -> HashMap<String, HashMap<String, u32>> {
    let mut merged = node_counts;
    for (pool, types) in nr_counts {
        let pool_entry = merged.entry(pool).or_default();
        for (instance_type, count) in types {
            *pool_entry.entry(instance_type).or_insert(0) += count;
        }
    }
    merged
}

/// Look up zone for an in-flight NR by matching instance_type + region
/// against the provider offerings list.
pub fn lookup_zone(offerings: &[Offering], instance_type: &str, region: &str) -> Option<String> {
    offerings
        .iter()
        .find(|o| o.instance_type.0 == instance_type && o.location.region.0 == region)
        .and_then(|o| o.location.zone.as_ref().map(|z| z.0.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use k8s_openapi::api::core::v1::{PodCondition, PodStatus};
    use kube::api::ObjectMeta;

    use crate::offering::{InstanceType, Location, Region, Resources, Zone};

    fn pod_with_conditions(conditions: Option<Vec<PodCondition>>) -> Pod {
        Pod {
            status: Some(PodStatus {
                conditions,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // --- is_pod_unschedulable ---

    #[test]
    fn unschedulable_pod_detected() {
        let pod = pod_with_conditions(Some(vec![PodCondition {
            type_: "PodScheduled".into(),
            status: "False".into(),
            reason: Some("Unschedulable".into()),
            ..Default::default()
        }]));
        assert!(is_pod_unschedulable(&pod));
    }

    #[test]
    fn scheduled_pod_not_unschedulable() {
        let pod = pod_with_conditions(Some(vec![PodCondition {
            type_: "PodScheduled".into(),
            status: "True".into(),
            reason: None,
            ..Default::default()
        }]));
        assert!(!is_pod_unschedulable(&pod));
    }

    #[test]
    fn no_conditions_not_unschedulable() {
        let pod = pod_with_conditions(None);
        assert!(!is_pod_unschedulable(&pod));
    }

    #[test]
    fn no_status_not_unschedulable() {
        let pod = Pod {
            status: None,
            ..Default::default()
        };
        assert!(!is_pod_unschedulable(&pod));
    }

    #[test]
    fn wrong_reason_not_unschedulable() {
        let pod = pod_with_conditions(Some(vec![PodCondition {
            type_: "PodScheduled".into(),
            status: "False".into(),
            reason: Some("NetworkUnavailable".into()),
            ..Default::default()
        }]));
        assert!(!is_pod_unschedulable(&pod));
    }

    #[test]
    fn no_reason_not_unschedulable() {
        let pod = pod_with_conditions(Some(vec![PodCondition {
            type_: "PodScheduled".into(),
            status: "False".into(),
            reason: None,
            ..Default::default()
        }]));
        assert!(!is_pod_unschedulable(&pod));
    }

    // --- is_daemonset_pod ---

    #[test]
    fn daemonset_pod_detected() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
        let pod = Pod {
            metadata: ObjectMeta {
                owner_references: Some(vec![OwnerReference {
                    kind: "DaemonSet".into(),
                    name: "logging".into(),
                    api_version: "apps/v1".into(),
                    uid: "ds-uid".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(is_daemonset_pod(&pod));
    }

    #[test]
    fn replicaset_pod_not_daemonset() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
        let pod = Pod {
            metadata: ObjectMeta {
                owner_references: Some(vec![OwnerReference {
                    kind: "ReplicaSet".into(),
                    name: "web".into(),
                    api_version: "apps/v1".into(),
                    uid: "rs-uid".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(!is_daemonset_pod(&pod));
    }

    #[test]
    fn no_owner_refs_not_daemonset() {
        let pod = Pod::default();
        assert!(!is_daemonset_pod(&pod));
    }

    // --- merge_occupied_counts ---

    #[test]
    fn merge_disjoint_pools() {
        let nr = HashMap::from([("pool-a".into(), HashMap::from([("cpx22".into(), 2)]))]);
        let nodes = HashMap::from([("pool-b".into(), HashMap::from([("cpx22".into(), 1)]))]);
        let merged = merge_occupied_counts(nr, nodes);
        assert_eq!(merged["pool-a"]["cpx22"], 2);
        assert_eq!(merged["pool-b"]["cpx22"], 1);
    }

    #[test]
    fn merge_overlapping_pools_sums_counts() {
        let nr = HashMap::from([("default".into(), HashMap::from([("cpx22".into(), 3)]))]);
        let nodes = HashMap::from([("default".into(), HashMap::from([("cpx22".into(), 2)]))]);
        let merged = merge_occupied_counts(nr, nodes);
        assert_eq!(merged["default"]["cpx22"], 5);
    }

    #[test]
    fn merge_overlapping_pool_disjoint_types() {
        let nr = HashMap::from([("default".into(), HashMap::from([("cpx22".into(), 1)]))]);
        let nodes = HashMap::from([("default".into(), HashMap::from([("cx42".into(), 2)]))]);
        let merged = merge_occupied_counts(nr, nodes);
        assert_eq!(merged["default"]["cpx22"], 1);
        assert_eq!(merged["default"]["cx42"], 2);
    }

    #[test]
    fn merge_empty_inputs() {
        let merged = merge_occupied_counts(HashMap::new(), HashMap::new());
        assert!(merged.is_empty());
    }

    // --- lookup_zone ---

    fn test_offering(
        name: &str,
        region: &str,
        zone: Option<&str>,
    ) -> Offering {
        Offering {
            instance_type: InstanceType(name.into()),
            resources: Resources {
                cpu: 2,
                memory_mib: 4096,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.01,
            location: Location {
                region: Region(region.into()),
                zone: zone.map(|z| Zone(z.into())),
            },
        }
    }

    #[test]
    fn lookup_zone_exact_match() {
        let offerings = vec![test_offering("cpx22", "eu-central", Some("fsn1-dc14"))];
        assert_eq!(
            lookup_zone(&offerings, "cpx22", "eu-central"),
            Some("fsn1-dc14".into())
        );
    }

    #[test]
    fn lookup_zone_no_match() {
        let offerings = vec![test_offering("cpx22", "eu-central", Some("fsn1-dc14"))];
        assert_eq!(lookup_zone(&offerings, "cpx22", "us-west"), None);
    }

    #[test]
    fn lookup_zone_wrong_instance_type() {
        let offerings = vec![test_offering("cpx22", "eu-central", Some("fsn1-dc14"))];
        assert_eq!(lookup_zone(&offerings, "cx42", "eu-central"), None);
    }

    #[test]
    fn lookup_zone_offering_without_zone() {
        let offerings = vec![test_offering("cpx22", "eu-central", None)];
        assert_eq!(lookup_zone(&offerings, "cpx22", "eu-central"), None);
    }

    #[test]
    fn lookup_zone_multi_zone_returns_first() {
        // When multiple offerings match (instance_type, region) with different zones,
        // lookup_zone returns the first match. This is a known limitation — see TODO
        // "Store zone in NodeRequestSpec".
        let offerings = vec![
            test_offering("cpx22", "eu-central", Some("fsn1-dc14")),
            test_offering("cpx22", "eu-central", Some("nbg1-dc3")),
        ];
        assert_eq!(
            lookup_zone(&offerings, "cpx22", "eu-central"),
            Some("fsn1-dc14".into()),
            "returns first match when multiple zones exist"
        );
    }

    #[test]
    fn lookup_zone_empty_offerings() {
        assert_eq!(lookup_zone(&[], "cpx22", "eu-central"), None);
    }
}
