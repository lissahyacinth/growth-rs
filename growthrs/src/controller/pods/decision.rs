use std::collections::{BTreeMap, HashMap, HashSet};

use tracing::{debug, warn};

use crate::offering::{Offering, PodResources};
use crate::optimiser::{BoundedOffering, ExistingNode, PlacementSolution, solve};
use crate::resources::node_pool::{LocationConstraint, ServerTypeConfig};

/// Why a pod could not be assigned to any pool.
#[derive(Debug)]
pub enum PodPoolReason {
    /// Pod selected a pool that does not exist.
    PoolNotFound { requested: String },
    /// Pod has no pool selector and no "default" pool exists.
    NoPoolSelector,
}

impl std::fmt::Display for PodPoolReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PoolNotFound { requested } => write!(f, "pool {requested:?} not found"),
            Self::NoPoolSelector => {
                write!(f, "no pool selector and no \"default\" pool exists")
            }
        }
    }
}

/// A pod that could not be assigned to any pool.
#[derive(Debug)]
pub struct PodPoolError {
    pub pod_id: crate::offering::PodId,
    pub reason: PodPoolReason,
}

#[derive(Debug)]
/// An (unfilled) request for a single node
pub struct NodeRequestDemand {
    pub pool: String,
    pub pool_uid: String,
    pub target_offering: Offering,
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

/// Current state of Cluster demands and offerings.
pub struct ClusterState {
    /// All unschedulable pod demands (full list, not residual).
    pub demands: Vec<PodResources>,
    /// Offerings from the Provider - what we can use to fulfil demands
    pub offerings: Vec<Offering>,
    /// Number of occupied slots per pool per instance type.
    /// Includes existing nodes + Pending/Provisioning NodeRequests.
    pub occupied_counts: HashMap<String, HashMap<String, u32>>,
    /// Available pools generated from NodePool CRDs.
    pub pools: Vec<PoolConfig>,
    /// In-flight nodes per pool. The solver treats these as pre-seeded
    /// capacity: pods are placed on them first (zero marginal cost),
    /// and they are excluded from the output.
    pub in_flight_nodes: HashMap<String, Vec<ExistingNode>>,
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
                        reason: PodPoolReason::PoolNotFound {
                            requested: name.clone(),
                        },
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
                        reason: PodPoolReason::NoPoolSelector,
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

/// Build pre-seeded existing nodes for a pool, enriched with pool labels.
fn build_existing_nodes(
    in_flight_nodes: &HashMap<String, Vec<ExistingNode>>,
    pool_name: &str,
    pool_labels: &BTreeMap<String, String>,
) -> Vec<ExistingNode> {
    in_flight_nodes
        .get(pool_name)
        .map(|nodes| {
            nodes
                .iter()
                .map(|n| {
                    let mut labels = pool_labels.clone();
                    labels.extend(n.labels.clone());
                    ExistingNode {
                        resources: n.resources.clone(),
                        labels,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Build `BoundedOffering`s from pool offerings, filtering by demand satisfaction,
/// location constraints, and remaining capacity per instance type.
fn build_bounded_offerings(
    pool_offerings: &[Offering],
    pool_demands: &[PodResources],
    pool: &PoolConfig,
    max_by_type: &HashMap<&str, u32>,
    pool_name: &str,
) -> Vec<BoundedOffering> {
    pool_offerings
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
        .collect()
}

/// Solve placement for a single pool: filter offerings, apply capacity limits,
/// run the solver, and return the resulting node request demands.
///
/// Each Pool is separable as pods cannot be set to run on multiple pools.
fn solve_pool(
    pool_name: &str,
    pool_demands: &[PodResources],
    pool: &PoolConfig,
    offerings: &[Offering],
    occupied_counts: &HashMap<String, HashMap<String, u32>>,
    in_flight_nodes: &HashMap<String, Vec<ExistingNode>>,
) -> Vec<NodeRequestDemand> {
    let pool_offerings = filter_offerings_for_pool(offerings, pool);

    // Subtract occupied slots (existing nodes + pending/provisioning NodeRequests)
    // from each type's max so the solver only provisions what the pool
    // can still accept.
    let occupied = occupied_counts.get(pool_name);

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

    let suitable =
        build_bounded_offerings(&pool_offerings, pool_demands, pool, &max_by_type, pool_name);

    let existing = build_existing_nodes(in_flight_nodes, pool_name, &pool.labels);

    let solution = solve(pool_demands, &suitable, &existing);

    let (nodes, unmet) = match solution {
        PlacementSolution::NoDemands => return vec![],
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

    nodes
        .into_iter()
        .map(|node| NodeRequestDemand {
            pool: pool_name.to_string(),
            pool_uid: pool.uid.clone(),
            target_offering: node.offering,
        })
        .collect()
}

/// Reconcile pod demands against the cluster state, returning demands for nodes to fulfill them.
pub fn reconcile_pod_demand(state: ClusterState) -> ReconcileResult {
    let (pods_by_pool, pod_errors) = assign_pods_to_pools(&state.demands, &state.pools);

    let pool_map: HashMap<&str, &PoolConfig> =
        state.pools.iter().map(|p| (p.name.as_str(), p)).collect();

    let mut all_demands = Vec::new();

    for (pool_name, pool_demands) in &pods_by_pool {
        debug!(pool = %pool_name, pods = pool_demands.len(), "pool demand");
        if pool_demands.is_empty() {
            continue;
        }

        let pool = pool_map[pool_name.as_str()];
        all_demands.extend(solve_pool(
            pool_name,
            pool_demands,
            pool,
            &state.offerings,
            &state.occupied_counts,
            &state.in_flight_nodes,
        ));
    }

    ReconcileResult {
        demands: all_demands,
        pod_errors,
    }
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
            in_flight_nodes: HashMap::new(),
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
        let result = reconcile_pod_demand(state);
        assert_eq!(result.demands.len(), 1);
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
        assert!(
            matches!(errors[0].reason, PodPoolReason::PoolNotFound { ref requested } if requested == "nonexistent")
        );
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
        assert!(matches!(errors[0].reason, PodPoolReason::NoPoolSelector));
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
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
        use crate::resources::node_pool::LocationConstraint;
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
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
        use crate::resources::node_pool::LocationConstraint;
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
        assert_eq!(result.demands.len(), 1);
        let loc = &result.demands[0].target_offering.location;
        assert_eq!(loc.region.0, "us-west");
        assert_eq!(loc.zone.as_ref().unwrap().0, "a");
    }

    #[test]
    fn multi_region_mixed_zone_constraints() {
        use crate::resources::node_pool::LocationConstraint;
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
        assert_eq!(result.demands.len(), 1);
    }

    #[test]
    fn offering_without_zone_rejected_when_zones_specified() {
        use crate::resources::node_pool::LocationConstraint;
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
        assert!(
            result.demands.is_empty(),
            "zone-less offering should be rejected when zones are specified"
        );
    }

    #[test]
    fn offering_without_zone_accepted_when_zones_omitted() {
        use crate::resources::node_pool::LocationConstraint;
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
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
        assert_eq!(result.demands.len(), 1);
    }

    // --- In-flight pre-seeded tests ---

    #[test]
    fn in_flight_scoped_to_correct_pool() {
        // T9: In-flight nodes for "gpu" pool must not absorb "cpu" pool demand.
        use crate::optimiser::ExistingNode;

        let gpu_pool = PoolConfig {
            name: "gpu".to_string(),
            uid: "gpu-uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: None,
        };
        let cpu_pool = PoolConfig {
            name: "cpu".to_string(),
            uid: "cpu-uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "cpx22".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: None,
        };

        let state = ClusterState {
            demands: vec![pod_with_pool("a", 1, 1024, "cpu")],
            offerings: vec![offering("cpx22", 2, 4096, 0.01)],
            occupied_counts: HashMap::new(),
            pools: vec![gpu_pool, cpu_pool],
            in_flight_nodes: HashMap::from([(
                "gpu".to_string(),
                vec![ExistingNode {
                    resources: res(4, 8192),
                    labels: BTreeMap::new(),
                }],
            )]),
        };
        let result = reconcile_pod_demand(state);
        assert_eq!(
            result.demands.len(),
            1,
            "gpu pool's in-flight should not absorb cpu pool demand"
        );
    }

    #[test]
    fn no_matching_offerings_produces_zero_demands() {
        // Pool references "nonexistent" server type, but only "cpx22" offerings exist.
        // Solver should gracefully produce zero demands, not panic.
        let pool = PoolConfig {
            name: "default".to_string(),
            uid: "uid".to_string(),
            server_types: vec![ServerTypeConfig {
                name: "nonexistent".to_string(),
                max: 100,
                min: 0,
            }],
            labels: BTreeMap::new(),
            locations: None,
        };
        let state = ClusterState {
            demands: vec![pod("a", 1, 1024)],
            offerings: vec![offering("cpx22", 2, 4096, 0.01)],
            occupied_counts: HashMap::new(),
            pools: vec![pool],
            in_flight_nodes: HashMap::new(),
        };
        let result = reconcile_pod_demand(state);
        assert!(
            result.demands.is_empty(),
            "mismatched server types should produce no demands"
        );
        assert!(result.pod_errors.is_empty());
    }
}
