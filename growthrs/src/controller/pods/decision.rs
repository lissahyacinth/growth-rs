use std::collections::{BTreeMap, HashMap, HashSet};

use tracing::{debug, warn};

use crate::controller::errors::PodPoolError;
use crate::crds::node_pool::{LocationConstraint, ServerTypeConfig};
use crate::offering::{Offering, PodId, PodResources, Resources};
use crate::optimiser::{BoundedOffering, PlacementSolution, SolveError, solve};

/// In-flight NodeRequest capacity used for resource-based deduplication.
///
/// Each entry represents a Pending, Provisioning, or young-Unmet NodeRequest.
/// `subtract_in_flight` absorbs pods into this capacity so the solver only
/// sees residual demand.
#[derive(Debug, Clone)]
pub struct InFlightCapacity {
    /// Pool this NodeRequest belongs to.
    pub pool: String,
    /// Snapshot of the resources this NodeRequest provides.
    pub resources: Resources,
}

#[derive(Debug)]
/// An (unfilled) request for a single node
pub struct NodeRequestDemand {
    pub pool: String,
    pub pool_uid: String,
    pub target_offering: Offering,
    /// UIDs of the pods this node is being provisioned for.
    pub claimed_pod_uids: Vec<String>,
    /// Labels from the owning NodePool, to be applied to the provisioned node.
    pub labels: BTreeMap<String, String>,
}

/// Configuration for a single pool, derived from a NodePool CRD.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub name: String,
    pub uid: String,
    pub server_types: Vec<ServerTypeConfig>,
    /// Labels from the NodePool spec, applied to every node in this pool.
    pub labels: BTreeMap<String, String>,
    /// If set, only offerings matching at least one entry are eligible.
    pub locations: Option<Vec<LocationConstraint>>,
}

/// Result of a reconciliation pass.
#[derive(Debug)]
pub struct ReconcileResult {
    pub demands: Vec<NodeRequestDemand>,
    pub pod_errors: Vec<PodPoolError>,
}

pub struct ClusterState {
    /// Current demands within this reconcilliation decision
    pub demands: Vec<PodResources>,
    /// Offerings from the Provider - what we can use to fulfil demands
    pub offerings: Vec<Offering>,
    /// Number of occupied slots per pool per instance type.
    /// Includes existing nodes + Pending/Provisioning NodeRequests.
    pub occupied_counts: HashMap<String, HashMap<String, u32>>,
    /// Available pools generated from NodePool CRDs.
    pub pools: Vec<PoolConfig>,
}

/// Assign pod demands to offered pools based on their `pool` selector.
///
/// - Pod has `pool: Some(name)` and pool exists -> assigned
/// - Pod has `pool: Some(name)` and pool doesn't exist -> PodPoolError
/// - Pod has `pool: None` and "default" pool exists -> assigned to "default"
/// - Pod has `pool: None` and no "default" -> PodPoolError
pub fn assign_pods_to_pools(
    demands: &[PodResources],
    pools: &[PoolConfig],
) -> (HashMap<String, Vec<PodResources>>, Vec<PodPoolError>) {
    let pool_names: HashMap<&str, &PoolConfig> =
        pools.iter().map(|p| (p.name.as_str(), p)).collect();
    let has_default = pool_names.contains_key("default");

    let mut assigned: HashMap<String, Vec<PodResources>> = HashMap::new();
    let mut errors = Vec::new();

    for pod in demands {
        match &pod.pool {
            Some(name) => {
                if pool_names.contains_key(name.as_str()) {
                    assigned.entry(name.clone()).or_default().push(pod.clone());
                } else {
                    errors.push(PodPoolError {
                        pod_id: pod.id.clone(),
                        reason: format!("pool {name:?} not found"),
                    });
                }
            }
            None => {
                if has_default {
                    assigned
                        .entry("default".to_string())
                        .or_default()
                        .push(pod.clone());
                } else {
                    errors.push(PodPoolError {
                        pod_id: pod.id.clone(),
                        reason: "no pool selector and no \"default\" pool exists".to_string(),
                    });
                }
            }
        }
    }

    (assigned, errors)
}

/// Filter provider offerings to only those whose instance_type appears in the pool's server_types.
pub fn filter_offerings_for_pool(offerings: &[Offering], pool: &PoolConfig) -> Vec<Offering> {
    let allowed: HashSet<&str> = pool
        .server_types
        .iter()
        .map(|st| st.name.as_str())
        .collect();

    offerings
        .iter()
        .filter(|o| allowed.contains(o.instance_type.0.as_str()))
        .cloned()
        .collect()
}

/// Subtract in-flight NodeRequest capacity from pod demand.
///
/// For each in-flight NodeRequest, greedily absorbs pods (largest-first)
/// whose pool matches and whose resources fit. Returns the residual pods
/// that still need new NodeRequests.
///
/// Imprecision is expected: the simulation may absorb different pods than
/// Kubernetes actually schedules. The convergence model handles this —
/// idle removal cleans up over-provisioning, the next loop catches
/// under-provisioning.
pub fn subtract_in_flight(
    mut pods: Vec<PodResources>,
    in_flight: &[InFlightCapacity],
) -> Vec<PodResources> {
    if in_flight.is_empty() {
        return pods;
    }

    // Sort largest-first (by cpu descending, then memory descending) for greedy packing.
    pods.sort_by(|a, b| {
        b.resources
            .cpu
            .cmp(&a.resources.cpu)
            .then(b.resources.memory_mib.cmp(&a.resources.memory_mib))
    });

    for cap in in_flight {
        let mut remaining = cap.resources.clone();
        pods.retain(|pod| {
            let pool_matches = pod
                .pool
                .as_deref()
                .map_or(cap.pool == "default", |p| p == cap.pool);
            if pool_matches && remaining.satisfies(&pod.resources) {
                remaining.subtract(&pod.resources);
                false // absorbed by in-flight capacity
            } else {
                true // still needs placement
            }
        });
    }

    pods
}

pub fn reconcile_pods(state: ClusterState) -> Result<ReconcileResult, SolveError> {
    let (pods_by_pool, pod_errors) = assign_pods_to_pools(&state.demands, &state.pools);

    let pool_map: HashMap<&str, &PoolConfig> =
        state.pools.iter().map(|p| (p.name.as_str(), p)).collect();

    let mut all_demands = Vec::new();

    for (pool_name, pool_demands) in &pods_by_pool {
        debug!(pool = %pool_name, pods = pool_demands.len(), "pool demand");
        let pool = pool_map[pool_name.as_str()];

        if pool_demands.is_empty() {
            continue;
        }

        let pool_offerings = filter_offerings_for_pool(&state.offerings, pool);

        // Subtract occupied slots (existing nodes + pending/provisioning NodeRequests)
        // from each type's max so the solver only provisions what the pool
        // can still accept.
        let occupied = state.occupied_counts.get(pool_name.as_str());

        let max_by_type: HashMap<&str, u32> = pool
            .server_types
            .iter()
            .map(|st| {
                let occupied_count = occupied
                    .and_then(|m| m.get(st.name.as_str()))
                    .copied()
                    .unwrap_or(0);
                let remaining = st.max.saturating_sub(occupied_count);
                (st.name.as_str(), remaining)
            })
            .collect();

        let suitable: Vec<_> = pool_offerings
            .iter()
            .filter(|o| pool_demands.iter().any(|d| o.satisfies(&d.resources)))
            .filter(|o| {
                // Filter by location constraints: offering must match at least one entry.
                // No locations = all regions/zones allowed.
                if let Some(locations) = &pool.locations {
                    locations.iter().any(|loc| {
                        if loc.region != o.location.region.0 {
                            return false;
                        }
                        match (&loc.zones, &o.location.zone) {
                            (Some(zones), Some(z)) => zones.iter().any(|a| a == &z.0),
                            (Some(_), None) => false,
                            (None, _) => true,
                        }
                    })
                } else {
                    true
                }
            })
            .map(|o| {
                let remaining_max = max_by_type
                    .get(o.instance_type.0.as_str())
                    .copied()
                    .unwrap_or(0);

                // Derive topology labels from offering location.
                let mut labels = pool.labels.clone();
                labels.insert(
                    "topology.kubernetes.io/region".into(),
                    o.location.region.0.clone(),
                );
                if let Some(ref z) = o.location.zone {
                    labels.insert("topology.kubernetes.io/zone".into(), z.0.clone());
                }

                BoundedOffering {
                    max_instances: remaining_max,
                    offering: o.clone(),
                    labels,
                    type_group: Some(format!("{}/{}", pool_name, o.instance_type.0)),
                }
            })
            .collect();

        let solution = solve(pool_demands, &suitable)?;

        let (nodes, unmet) = match solution {
            PlacementSolution::NoDemands => continue,
            PlacementSolution::AllPlaced(nodes) => (nodes, vec![]),
            PlacementSolution::IncompletePlacement { nodes, unmet } => (nodes, unmet),
        };

        if !unmet.is_empty() {
            warn!(
                pool = %pool_name,
                unmet_count = unmet.len(),
                "incomplete placement — some pods could not be scheduled"
            );
        }

        // Build PodId → UID mapping so we can record which pods each NodeRequest claims.
        let uid_map: HashMap<&PodId, &str> = pool_demands
            .iter()
            .map(|pr| (&pr.id, pr.uid.as_str()))
            .collect();

        let new_demands: Vec<_> = nodes
            .into_iter()
            .map(|node| {
                let claimed_pod_uids = node
                    .pods
                    .iter()
                    .filter_map(|pid| uid_map.get(pid).map(|u| u.to_string()))
                    .collect();
                NodeRequestDemand {
                    pool: pool_name.clone(),
                    pool_uid: pool.uid.clone(),
                    target_offering: node.offering,
                    claimed_pod_uids,
                    labels: pool.labels.clone(),
                }
            })
            .collect();

        all_demands.extend(new_demands);
    }

    Ok(ReconcileResult {
        demands: all_demands,
        pod_errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use crate::offering::{InstanceType, PodId, Resources};

    fn res(cpu: u32, memory_mib: u32) -> Resources {
        Resources {
            cpu,
            memory_mib,
            ephemeral_storage_gib: None,
            gpu: 0,
            gpu_model: None,
        }
    }

    fn pod(name: &str, cpu: u32, memory_mib: u32) -> PodResources {
        PodResources {
            id: PodId::new("default", name),
            uid: format!("uid-{name}"),
            resources: res(cpu, memory_mib),
            pool: None,
            pod_labels: BTreeMap::new(),
            affinity_constraints: vec![],
        }
    }

    fn pod_with_pool(name: &str, cpu: u32, memory_mib: u32, pool: &str) -> PodResources {
        PodResources {
            id: PodId::new("default", name),
            uid: format!("uid-{name}"),
            resources: res(cpu, memory_mib),
            pool: Some(pool.to_string()),
            pod_labels: BTreeMap::new(),
            affinity_constraints: vec![],
        }
    }

    fn offering(name: &str, cpu: u32, memory_mib: u32, cost: f64) -> Offering {
        use crate::offering::{Location, Region, Zone};
        Offering {
            instance_type: InstanceType(name.into()),
            resources: res(cpu, memory_mib),
            cost_per_hour: cost,
            location: Location {
                region: Region("eu-central".into()),
                zone: Some(Zone("fsn1-dc14".into())),
            },
        }
    }

    fn default_pool(server_types: Vec<(&str, u32)>) -> PoolConfig {
        PoolConfig {
            name: "default".to_string(),
            uid: "default-uid".to_string(),
            server_types: server_types
                .into_iter()
                .map(|(name, max)| ServerTypeConfig {
                    name: name.to_string(),
                    max,
                    min: 0,
                })
                .collect(),
            labels: BTreeMap::new(),
            locations: None,
        }
    }

    fn default_state(demands: Vec<PodResources>, offerings: Vec<Offering>) -> ClusterState {
        let pool = default_pool(
            offerings
                .iter()
                .map(|o| (o.instance_type.0.as_str(), 100))
                .collect(),
        );
        ClusterState {
            demands,
            offerings,
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        }
    }

    #[test]
    fn all_demands_passed_to_solver() {
        // With claimed-pod-UID filtering happening upstream, reconcile_pods
        // receives only unclaimed pods and solves for all of them.
        let state = default_state(
            vec![pod("a", 1, 1024)],
            vec![offering("cpx22", 2, 4096, 0.01)],
        );
        let result = reconcile_pods(state).unwrap();
        assert_eq!(result.demands.len(), 1);
    }

    #[test]
    fn demands_carry_claimed_pod_uids() {
        let state = default_state(
            vec![pod("a", 1, 1024), pod("b", 1, 1024)],
            vec![offering("cpx22", 2, 4096, 0.01)],
        );
        let result = reconcile_pods(state).unwrap();
        // Both pods fit on one cpx22 node.
        assert_eq!(result.demands.len(), 1);
        let mut uids = result.demands[0].claimed_pod_uids.clone();
        uids.sort();
        assert_eq!(uids, vec!["uid-a", "uid-b"]);
    }

    // --- Pool assignment tests ---

    #[test]
    fn pod_with_selector_assigned_to_correct_pool() {
        let pools = vec![
            PoolConfig {
                name: "gpu".to_string(),
                uid: "gpu-uid".to_string(),
                server_types: vec![],
                labels: BTreeMap::new(),
                locations: None,
            },
            PoolConfig {
                name: "cpu".to_string(),
                uid: "cpu-uid".to_string(),
                server_types: vec![],
                labels: BTreeMap::new(),
                locations: None,
            },
        ];
        let demands = vec![pod_with_pool("a", 1, 1024, "gpu")];
        let (assigned, errors) = assign_pods_to_pools(&demands, &pools);

        assert!(errors.is_empty());
        assert_eq!(assigned.get("gpu").unwrap().len(), 1);
        assert!(assigned.get("cpu").is_none());
    }

    #[test]
    fn pod_with_missing_pool_produces_error() {
        let pools = vec![PoolConfig {
            name: "cpu".to_string(),
            uid: "cpu-uid".to_string(),
            server_types: vec![],
            labels: BTreeMap::new(),
            locations: None,
        }];
        let demands = vec![pod_with_pool("a", 1, 1024, "nonexistent")];
        let (assigned, errors) = assign_pods_to_pools(&demands, &pools);

        assert!(assigned.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].reason.contains("nonexistent"));
    }

    #[test]
    fn pod_without_selector_assigned_to_default() {
        let pools = vec![PoolConfig {
            name: "default".to_string(),
            uid: "default-uid".to_string(),
            server_types: vec![],
            labels: BTreeMap::new(),
            locations: None,
        }];
        let demands = vec![pod("a", 1, 1024)];
        let (assigned, errors) = assign_pods_to_pools(&demands, &pools);

        assert!(errors.is_empty());
        assert_eq!(assigned.get("default").unwrap().len(), 1);
    }

    #[test]
    fn pod_without_selector_and_no_default_produces_error() {
        let pools = vec![PoolConfig {
            name: "gpu-only".to_string(),
            uid: "gpu-uid".to_string(),
            server_types: vec![],
            labels: BTreeMap::new(),
            locations: None,
        }];
        let demands = vec![pod("a", 1, 1024)];
        let (_, errors) = assign_pods_to_pools(&demands, &pools);

        assert_eq!(errors.len(), 1);
        assert!(errors[0].reason.contains("default"));
    }

    // --- Offering filter tests ---

    #[test]
    fn filter_offerings_returns_only_pool_types() {
        let pool = PoolConfig {
            name: "small".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 10,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: None,
        };
        let offerings = vec![
            offering("cpx22", 2, 4096, 0.01),
            offering("cx42", 8, 16384, 0.05),
        ];
        let filtered = filter_offerings_for_pool(&offerings, &pool);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].instance_type.0, "cpx22");
    }

    // --- Full reconcile with pools ---

    #[test]
    fn reconcile_with_named_pool() {
        let pool = PoolConfig {
            name: "workers".to_string(),
            uid: "workers-uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 10,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: None,
        };
        let state = ClusterState {
            demands: vec![pod_with_pool("a", 1, 1024, "workers")],
            offerings: vec![offering("cpx22", 2, 4096, 0.01)],
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        assert_eq!(result.demands.len(), 1);
        assert_eq!(result.demands[0].pool, "workers");
        assert_eq!(result.demands[0].pool_uid, "workers-uid");
        assert!(result.pod_errors.is_empty());
    }

    #[test]
    fn reconcile_with_missing_pool_reports_error() {
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 10,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: None,
        };
        let state = ClusterState {
            demands: vec![pod_with_pool("a", 1, 1024, "nonexistent")],
            offerings: vec![offering("cpx22", 2, 4096, 0.01)],
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        assert!(result.demands.is_empty());
        assert_eq!(result.pod_errors.len(), 1);
    }

    #[test]
    fn occupied_slots_subtracted_from_pool_max() {
        // Pool max=2 for cpx22, 1 already occupied (existing node or in-flight NodeRequest).
        // 3 pods each need their own node → solver can only create 1 more.
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 2,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: None,
        };
        let state = ClusterState {
            demands: vec![pod("a", 2, 4096), pod("b", 2, 4096), pod("c", 2, 4096)],
            offerings: vec![offering("cpx22", 2, 4096, 0.01)],
            occupied_counts: HashMap::from([(
                "default".to_string(),
                HashMap::from([("cpx22".to_string(), 1)]),
            )]),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        // max=2, occupied=1 → solver may only provision 1 more node
        assert_eq!(result.demands.len(), 1);
    }

    // --- Location constraint filtering tests ---

    fn offering_in(
        name: &str,
        cpu: u32,
        memory_mib: u32,
        cost: f64,
        region: &str,
        zone: Option<&str>,
    ) -> Offering {
        use crate::offering::{Location, Region, Zone};
        Offering {
            instance_type: InstanceType(name.into()),
            resources: res(cpu, memory_mib),
            cost_per_hour: cost,
            location: Location {
                region: Region(region.into()),
                zone: zone.map(|z| Zone(z.into())),
            },
        }
    }

    #[test]
    fn location_region_only_filters_by_region() {
        use crate::crds::node_pool::LocationConstraint;
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: Some(vec![LocationConstraint {
                region: "us-west".to_string(),
                zones: None,
            }]),
        };
        let offerings = vec![
            offering_in("cpx22", 2, 4096, 0.01, "us-west", Some("a")),
            offering_in("cpx22", 2, 4096, 0.01, "us-west", Some("b")),
            offering_in("cpx22", 2, 4096, 0.01, "eu-central", Some("fsn1")),
        ];
        let state = ClusterState {
            demands: vec![pod("a", 1, 1024)],
            offerings,
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        assert_eq!(result.demands.len(), 1);
        // The selected offering must be in us-west
        assert!(
            result.demands[0]
                .target_offering
                .location
                .region
                .0
                .starts_with("us-west")
        );
    }

    #[test]
    fn location_region_plus_zones_filters_correctly() {
        use crate::crds::node_pool::LocationConstraint;
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: Some(vec![LocationConstraint {
                region: "us-west".to_string(),
                zones: Some(vec!["a".to_string()]),
            }]),
        };
        let offerings = vec![
            offering_in("cpx22", 2, 4096, 0.01, "us-west", Some("a")),
            offering_in("cpx22", 2, 4096, 0.01, "us-west", Some("b")),
            offering_in("cpx22", 2, 4096, 0.01, "eu-central", Some("a")),
        ];
        let state = ClusterState {
            demands: vec![pod("a", 1, 1024)],
            offerings,
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        assert_eq!(result.demands.len(), 1);
        let loc = &result.demands[0].target_offering.location;
        assert_eq!(loc.region.0, "us-west");
        assert_eq!(loc.zone.as_ref().unwrap().0, "a");
    }

    #[test]
    fn multi_region_mixed_zone_constraints() {
        use crate::crds::node_pool::LocationConstraint;
        // us-west4: zones a,b; us-west2: zone a only
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: Some(vec![
                LocationConstraint {
                    region: "us-west4".to_string(),
                    zones: Some(vec!["a".to_string(), "b".to_string()]),
                },
                LocationConstraint {
                    region: "us-west2".to_string(),
                    zones: Some(vec!["a".to_string()]),
                },
            ]),
        };
        let offerings = vec![
            offering_in("cpx22", 2, 4096, 0.01, "us-west4", Some("a")),
            offering_in("cpx22", 2, 4096, 0.01, "us-west4", Some("b")),
            offering_in("cpx22", 2, 4096, 0.01, "us-west2", Some("a")),
            offering_in("cpx22", 2, 4096, 0.01, "us-west2", Some("b")), // should be filtered
            offering_in("cpx22", 2, 4096, 0.01, "eu-central", Some("x")), // wrong region
        ];
        // 4 pods to force multiple nodes → proves multiple offerings pass
        let state = ClusterState {
            demands: vec![pod("a", 2, 4096), pod("b", 2, 4096), pod("c", 2, 4096)],
            offerings,
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        // All placed offerings must be in the allowed set
        for d in &result.demands {
            let loc = &d.target_offering.location;
            let region = &loc.region.0;
            let zone = loc.zone.as_ref().map(|z| z.0.as_str());
            match region.as_str() {
                "us-west4" => assert!(
                    zone == Some("a") || zone == Some("b"),
                    "unexpected zone {zone:?} in us-west4"
                ),
                "us-west2" => assert_eq!(zone, Some("a"), "only zone a in us-west2"),
                other => panic!("unexpected region {other}"),
            }
        }
    }

    #[test]
    fn no_locations_allows_all_offerings() {
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: None,
        };
        let offerings = vec![
            offering_in("cpx22", 2, 4096, 0.01, "us-west", Some("a")),
            offering_in("cpx22", 2, 4096, 0.01, "eu-central", Some("fsn1")),
        ];
        let state = ClusterState {
            demands: vec![pod("a", 1, 1024)],
            offerings,
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        assert_eq!(result.demands.len(), 1);
    }

    #[test]
    fn offering_without_zone_rejected_when_zones_specified() {
        use crate::crds::node_pool::LocationConstraint;
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: Some(vec![LocationConstraint {
                region: "us-west".to_string(),
                zones: Some(vec!["a".to_string()]),
            }]),
        };
        // Offering has no zone — should not pass a constraint that lists specific zones
        let offerings = vec![offering_in("cpx22", 2, 4096, 0.01, "us-west", None)];
        let state = ClusterState {
            demands: vec![pod("a", 1, 1024)],
            offerings,
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        assert!(
            result.demands.is_empty(),
            "zone-less offering should be rejected when zones are specified"
        );
    }

    #[test]
    fn offering_without_zone_accepted_when_zones_omitted() {
        use crate::crds::node_pool::LocationConstraint;
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: Some(vec![LocationConstraint {
                region: "us-west".to_string(),
                zones: None,
            }]),
        };
        // Offering has no zone — region-only constraint should accept it
        let offerings = vec![offering_in("cpx22", 2, 4096, 0.01, "us-west", None)];
        let state = ClusterState {
            demands: vec![pod("a", 1, 1024)],
            offerings,
            occupied_counts: HashMap::new(),
            pools: vec![pool],
        };
        let result = reconcile_pods(state).unwrap();
        assert_eq!(result.demands.len(), 1);
    }

    // --- subtract_in_flight tests ---

    fn in_flight(pool: &str, cpu: u32, memory_mib: u32) -> InFlightCapacity {
        InFlightCapacity {
            pool: pool.to_string(),
            resources: res(cpu, memory_mib),
        }
    }

    #[test]
    fn subtract_empty_in_flight_passes_all_pods() {
        let pods = vec![pod("a", 1, 1024), pod("b", 2, 2048)];
        let result = subtract_in_flight(pods, &[]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn subtract_exact_match_absorbs_all() {
        let pods = vec![pod("a", 1, 1024), pod("b", 1, 1024)];
        let cap = vec![in_flight("default", 2, 4096)];
        let result = subtract_in_flight(pods, &cap);
        assert!(result.is_empty(), "both pods should be absorbed");
    }

    #[test]
    fn subtract_partial_coverage_leaves_residual() {
        let pods = vec![pod("a", 1, 1024), pod("b", 1, 1024), pod("c", 1, 1024)];
        let cap = vec![in_flight("default", 2, 4096)];
        let result = subtract_in_flight(pods, &cap);
        assert_eq!(result.len(), 1, "one pod should remain");
    }

    #[test]
    fn subtract_pod_too_large_passes_through() {
        let pods = vec![pod("big", 4, 8192)];
        let cap = vec![in_flight("default", 2, 4096)];
        let result = subtract_in_flight(pods, &cap);
        assert_eq!(result.len(), 1, "oversized pod should not be absorbed");
    }

    #[test]
    fn subtract_respects_pool_boundaries() {
        let pods = vec![pod_with_pool("a", 1, 1024, "gpu")];
        let cap = vec![in_flight("cpu", 4, 8192)];
        let result = subtract_in_flight(pods, &cap);
        assert_eq!(result.len(), 1, "wrong pool should not absorb");
    }

    #[test]
    fn subtract_largest_first_packing() {
        let pods = vec![pod("small", 1, 1024), pod("big", 2, 2048)];
        let cap = vec![in_flight("default", 2, 4096)];
        let result = subtract_in_flight(pods, &cap);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id.name, "small");
    }

    #[test]
    fn subtract_gpu_demand_only_absorbed_by_gpu_capacity() {
        use crate::offering::GpuModel;
        let gpu_pod = PodResources {
            id: PodId::new("default", "gpu-job"),
            uid: "uid-gpu".into(),
            resources: Resources {
                cpu: 1,
                memory_mib: 4096,
                ephemeral_storage_gib: None,
                gpu: 1,
                gpu_model: Some(GpuModel::NvidiaT4),
            },
            pool: None,
            pod_labels: BTreeMap::new(),
            affinity_constraints: vec![],
        };
        let cap = vec![in_flight("default", 4, 16384)];
        let result = subtract_in_flight(vec![gpu_pod], &cap);
        assert_eq!(result.len(), 1, "GPU pod should not be absorbed by non-GPU capacity");
    }
}
