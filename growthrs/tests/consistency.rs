mod common;

use std::collections::{BTreeMap, HashMap};

use growthrs::controller::{
    ClusterState, InFlightCapacity, PoolConfig, reconcile_pods, subtract_in_flight,
};
use growthrs::crds::node_pool::ServerTypeConfig;
use growthrs::offering::PodResources;

use common::{pending_pod, test_offering};

/// When in-flight capacity covers all demand, reconcile_pods receives
/// an empty demand list and produces no new NodeRequests.
#[test]
fn in_flight_capacity_absorbs_demand() {
    let offerings = vec![test_offering("cpx22", 2, 4096, 0.01)];

    let pods = vec![
        pending_pod("pod-a", "1", "2048Mi"),
        pending_pod("pod-b", "1", "2048Mi"),
    ];
    let demands: Vec<_> = pods
        .iter()
        .map(|p| PodResources::from_pod(p).unwrap())
        .collect();

    let in_flight = vec![InFlightCapacity {
        pool: "default".to_string(),
        resources: offerings[0].resources.clone(),
    }];
    let residual = subtract_in_flight(demands, &in_flight);
    assert!(
        residual.is_empty(),
        "all demand absorbed by in-flight capacity"
    );

    let pool = PoolConfig {
        name: "default".to_string(),
        uid: "default-uid".to_string(),
        server_types: vec![ServerTypeConfig {
            name: "cpx22".to_string(),
            max: 100,
            min: 0,
        }],
        labels: BTreeMap::new(),
        locations: None,
    };
    let state = ClusterState {
        demands: residual,
        offerings,
        occupied_counts: HashMap::new(),
        pools: vec![pool],
    };
    let result = reconcile_pods(state).unwrap();
    assert!(
        result.demands.is_empty(),
        "no residual demand → no new NodeRequests"
    );
}

/// When in-flight capacity only partially covers demand, the solver
/// receives the residual and produces NodeRequests for the remainder.
#[test]
fn partial_in_flight_leaves_residual_for_solver() {
    let offerings = vec![test_offering("cpx22", 2, 4096, 0.01)];

    let pods = vec![
        pending_pod("pod-a", "1", "2048Mi"),
        pending_pod("pod-b", "1", "2048Mi"),
        pending_pod("pod-c", "1", "2048Mi"),
    ];
    let demands: Vec<_> = pods
        .iter()
        .map(|p| PodResources::from_pod(p).unwrap())
        .collect();

    let in_flight = vec![InFlightCapacity {
        pool: "default".to_string(),
        resources: offerings[0].resources.clone(),
    }];
    let residual = subtract_in_flight(demands, &in_flight);
    assert_eq!(residual.len(), 1, "one pod remains after absorption");

    let pool = PoolConfig {
        name: "default".to_string(),
        uid: "default-uid".to_string(),
        server_types: vec![ServerTypeConfig {
            name: "cpx22".to_string(),
            max: 100,
            min: 0,
        }],
        labels: BTreeMap::new(),
        locations: None,
    };
    let state = ClusterState {
        demands: residual,
        offerings,
        occupied_counts: HashMap::new(),
        pools: vec![pool],
    };
    let result = reconcile_pods(state).unwrap();
    assert_eq!(
        result.demands.len(),
        1,
        "one new NodeRequest for the residual pod"
    );
}

/// Expired Unmet NRs release their capacity — their resources are NOT
/// included in the in-flight list, so the same pods produce new NodeRequests.
#[test]
fn expired_unmet_releases_capacity() {
    let offerings = vec![test_offering("cpx22", 2, 4096, 0.01)];

    let pods = vec![
        pending_pod("pod-a", "1", "2048Mi"),
        pending_pod("pod-b", "1", "2048Mi"),
    ];
    let demands: Vec<_> = pods
        .iter()
        .map(|p| PodResources::from_pod(p).unwrap())
        .collect();

    let residual = subtract_in_flight(demands, &[]);
    assert_eq!(
        residual.len(),
        2,
        "all pods pass through with no in-flight capacity"
    );

    let pool = PoolConfig {
        name: "default".to_string(),
        uid: "default-uid".to_string(),
        server_types: vec![ServerTypeConfig {
            name: "cpx22".to_string(),
            max: 100,
            min: 0,
        }],
        labels: BTreeMap::new(),
        locations: None,
    };
    let state = ClusterState {
        demands: residual,
        offerings,
        occupied_counts: HashMap::new(),
        pools: vec![pool],
    };
    let result = reconcile_pods(state).unwrap();
    assert_eq!(
        result.demands.len(),
        1,
        "solver creates NR for the released pods"
    );
}
