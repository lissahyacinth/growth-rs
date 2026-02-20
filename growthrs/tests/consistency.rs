mod common;

use growthrs::controller::{reconcile_pods, ClusterState};
use growthrs::offering::PodResources;

use common::{pending_pod, test_offering};

#[test]
fn duplicate_creates_when_pods_still_pending() {
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

    // First reconciliation — creates 2 NodeRequests for 3 pods.
    let state1 = ClusterState {
        demands: demands.clone(),
        offerings: offerings.clone(),
    };
    assert_eq!(reconcile_pods(state1).unwrap().len(), 2);

    // Second reconciliation — same pods still pending, creates 2 more.
    let state2 = ClusterState {
        demands,
        offerings,
    };
    assert_eq!(reconcile_pods(state2).unwrap().len(), 2);
}
