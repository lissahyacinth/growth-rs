mod common;

use std::collections::{BTreeMap, HashMap};

use growthrs::controller::{ClusterState, PoolConfig, reconcile_pods};
use growthrs::crds::node_pool::ServerTypeConfig;
use growthrs::offering::PodResources;

use common::{pending_pod, test_offering};

#[test]
fn demands_include_claimed_pod_uids() {
    let offerings = vec![test_offering("cx22", 2, 4096, 0.01)];

    let pods = vec![
        pending_pod("pod-a", "1", "2048Mi"),
        pending_pod("pod-b", "1", "2048Mi"),
        pending_pod("pod-c", "1", "2048Mi"),
    ];
    let demands: Vec<_> = pods
        .iter()
        .map(|p| PodResources::from_pod(p).unwrap())
        .collect();

    let pool = PoolConfig {
        name: "default".to_string(),
        uid: "default-uid".to_string(),
        server_types: vec![ServerTypeConfig {
            name: "cx22".to_string(),
            max: 100,
            min: 0,
        }],
        labels: BTreeMap::new(),
        locations: None,
    };

    let state = ClusterState {
        demands,
        offerings,
        occupied_counts: HashMap::new(),
        pools: vec![pool],
    };
    let result = reconcile_pods(state).unwrap();

    // 3 pods at 1cpu each → 2 cx22 nodes (2cpu each).
    assert_eq!(result.demands.len(), 2);

    // Every demand carries the UIDs of the pods it claims.
    let mut all_uids: Vec<_> = result
        .demands
        .iter()
        .flat_map(|d| d.claimed_pod_uids.iter().cloned())
        .collect();
    all_uids.sort();
    assert_eq!(all_uids, vec!["uid-pod-a", "uid-pod-b", "uid-pod-c"]);
}

#[test]
fn second_reconcile_with_claimed_pods_filtered_creates_nothing() {
    let offerings = vec![test_offering("cx22", 2, 4096, 0.01)];

    let pods = vec![
        pending_pod("pod-a", "1", "2048Mi"),
        pending_pod("pod-b", "1", "2048Mi"),
    ];
    let demands: Vec<_> = pods
        .iter()
        .map(|p| PodResources::from_pod(p).unwrap())
        .collect();

    let pool = PoolConfig {
        name: "default".to_string(),
        uid: "default-uid".to_string(),
        server_types: vec![ServerTypeConfig {
            name: "cx22".to_string(),
            max: 100,
            min: 0,
        }],
        labels: BTreeMap::new(),
        locations: None,
    };

    // First reconciliation — pods are unclaimed.
    let state1 = ClusterState {
        demands: demands.clone(),
        offerings: offerings.clone(),
        occupied_counts: HashMap::new(),
        pools: vec![pool.clone()],
    };
    let result1 = reconcile_pods(state1).unwrap();
    assert_eq!(result1.demands.len(), 1, "should create 1 NR for 2 pods");

    // Second reconciliation — simulate gather_cluster_state filtering out
    // pods whose UIDs are now claimed by the NR from the first pass.
    // In production this happens in gather_cluster_state; here we pass
    // an empty demand list to represent that.
    let state2 = ClusterState {
        demands: vec![],
        offerings,
        occupied_counts: HashMap::new(),
        pools: vec![pool],
    };
    let result2 = reconcile_pods(state2).unwrap();
    assert!(
        result2.demands.is_empty(),
        "no unclaimed pods → no new NodeRequests"
    );
}

/// When a NodeRequest transitions to Unmet, its claimed pod UIDs are released.
/// gather_cluster_state skips Unmet NRs when building the claimed set, so
/// those pods re-enter demand and get new NodeRequests on the next loop.
///
/// This test simulates the sequence:
///   1. First reconcile → creates NRs claiming pods.
///   2. Those NRs go Unmet (provider couldn't fulfil).
///   3. Second reconcile with the same pods (no longer filtered) → creates new NRs.
#[test]
fn unmet_node_requests_release_claimed_pods() {
    let offerings = vec![test_offering("cx22", 2, 4096, 0.01)];

    let pods = vec![
        pending_pod("pod-a", "1", "2048Mi"),
        pending_pod("pod-b", "1", "2048Mi"),
    ];
    let demands: Vec<_> = pods
        .iter()
        .map(|p| PodResources::from_pod(p).unwrap())
        .collect();

    let pool = PoolConfig {
        name: "default".to_string(),
        uid: "default-uid".to_string(),
        server_types: vec![ServerTypeConfig {
            name: "cx22".to_string(),
            max: 100,
            min: 0,
        }],
        labels: BTreeMap::new(),
        locations: None,
    };

    // First reconciliation — produces NRs claiming both pods.
    let state1 = ClusterState {
        demands: demands.clone(),
        offerings: offerings.clone(),
        occupied_counts: HashMap::new(),
        pools: vec![pool.clone()],
    };
    let result1 = reconcile_pods(state1).unwrap();
    assert_eq!(result1.demands.len(), 1);
    assert!(!result1.demands[0].claimed_pod_uids.is_empty());

    // Those NRs transitioned to Unmet. get_in_flight_state skips Unmet NRs,
    // so the claimed set is empty. The same pods reappear as unclaimed demands.
    let state2 = ClusterState {
        demands: demands.clone(),
        offerings: offerings.clone(),
        occupied_counts: HashMap::new(),
        pools: vec![pool],
    };
    let result2 = reconcile_pods(state2).unwrap();
    assert_eq!(
        result2.demands.len(),
        1,
        "pods released by Unmet NRs should produce new NodeRequests"
    );

    // The new NRs claim the same pods again.
    let mut uids: Vec<_> = result2.demands[0].claimed_pod_uids.clone();
    uids.sort();
    assert_eq!(uids, vec!["uid-pod-a", "uid-pod-b"]);
}
