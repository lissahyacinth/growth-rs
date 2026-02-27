mod common;

use std::collections::HashMap;

use growthrs::controller::{ClusterState, PoolConfig, reconcile_pods};
use growthrs::node_pool::ServerTypeConfig;
use growthrs::offering::PodResources;

use common::{pending_pod, test_offering};

#[test]
fn forty_pods_two_offerings_all_placed() {
    let small = test_offering("small-2cpu", 2, 4096, 0.01);
    let medium = test_offering("medium-4cpu", 4, 8192, 0.018);

    let pods: Vec<_> = (0..40)
        .map(|i| pending_pod(&format!("pod-{i}"), "1", "512Mi"))
        .collect();

    let demands: Vec<_> = pods
        .iter()
        .map(|p| PodResources::from_pod(p).unwrap())
        .collect();

    let pool = PoolConfig {
        name: "default".to_string(),
        uid: "default-uid".to_string(),
        server_types: vec![
            ServerTypeConfig {
                name: "small-2cpu".to_string(),
                max: 20,
                min: 0,
            },
            ServerTypeConfig {
                name: "medium-4cpu".to_string(),
                max: 20,
                min: 0,
            },
        ],
    };

    let state = ClusterState {
        demands,
        offerings: vec![small, medium],
        occupied_counts: HashMap::new(),
        pools: vec![pool],
    };

    let result = reconcile_pods(state);
    assert!(result.is_ok(), "solver failed: {:?}", result.unwrap_err());

    let created = result.unwrap().demands.len();

    // Should need at most 20 nodes (worst case: all small, 2 pods each).
    assert!(
        created <= 20,
        "expected at most 20 NodeRequests, got {created}"
    );
    // Must create at least some nodes for 40 pods.
    assert!(
        created >= 1,
        "expected at least 1 NodeRequest, got {created}"
    );
}
