//! E2E affinity tests — require a KWOK-enabled cluster.
//!
//! Run with: `cargo test --manifest-path growthrs/Cargo.toml --features testing`
#![cfg(feature = "testing")]

use std::collections::BTreeMap;
use std::time::Duration;

use growthrs::offering::Resources;
use growthrs::testing;

fn cpx22_resources() -> Resources {
    Resources {
        cpu: 2,
        memory_mib: 4096,
        ephemeral_storage_gib: Some(40),
        gpu: 0,
        gpu_model: None,
    }
}

fn zone_labels(zone: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("topology.kubernetes.io/zone".into(), zone.into()),
        ("type".into(), "kwok".into()),
    ])
}

/// 3 KWOK nodes in 3 zones, 3 pods `app=web` with zone anti-affinity.
/// All 3 pods should schedule on different zone nodes.
#[tokio::test]
async fn e2e_anti_affinity_zone_spread() {
    let client = growthrs::testing::test_client().await;

    // Clean slate
    testing::nuke(client.clone()).await.unwrap();

    let res = cpx22_resources();
    let topo = "topology.kubernetes.io/zone";

    // Create 3 KWOK nodes in 3 zones
    testing::create_kwok_node(client.clone(), "node-a", &res, zone_labels("zone-a"))
        .await
        .unwrap();
    testing::create_kwok_node(client.clone(), "node-b", &res, zone_labels("zone-b"))
        .await
        .unwrap();
    testing::create_kwok_node(client.clone(), "node-c", &res, zone_labels("zone-c"))
        .await
        .unwrap();

    // Create 3 pods with zone anti-affinity
    for i in 0..3 {
        testing::create_pod_with_affinity(
            client.clone(),
            &format!("web-{i}"),
            "1",
            "512Mi",
            "web",
            topo,
            true, // anti-affinity
            None,
        )
        .await
        .unwrap();
    }

    // Verify all 3 pods get scheduled on different nodes
    let mut node_names = Vec::new();
    for i in 0..3 {
        let node = testing::wait_for_pod_scheduled(
            client.clone(),
            &format!("web-{i}"),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        node_names.push(node);
    }

    // All 3 pods on different nodes (which are in different zones)
    node_names.sort();
    node_names.dedup();
    assert_eq!(
        node_names.len(),
        3,
        "expected 3 different nodes, got: {node_names:?}"
    );

    // Teardown
    testing::nuke(client).await.unwrap();
}

/// 2 KWOK nodes in zone-a, 2 pods `app=cache` with zone affinity.
/// Both pods should schedule in the same zone.
#[tokio::test]
async fn e2e_affinity_co_location() {
    let client = growthrs::testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    let res = cpx22_resources();
    let topo = "topology.kubernetes.io/zone";

    // Create 2 nodes in zone-a and 1 in zone-b
    testing::create_kwok_node(client.clone(), "node-a1", &res, zone_labels("zone-a"))
        .await
        .unwrap();
    testing::create_kwok_node(client.clone(), "node-a2", &res, zone_labels("zone-a"))
        .await
        .unwrap();
    testing::create_kwok_node(client.clone(), "node-b1", &res, zone_labels("zone-b"))
        .await
        .unwrap();

    // Create 2 pods with zone affinity
    for i in 0..2 {
        testing::create_pod_with_affinity(
            client.clone(),
            &format!("cache-{i}"),
            "0.5",
            "512Mi",
            "cache",
            topo,
            false, // affinity (not anti)
            None,
        )
        .await
        .unwrap();
    }

    // Verify both pods get scheduled
    let node0 = testing::wait_for_pod_scheduled(client.clone(), "cache-0", Duration::from_secs(60))
        .await
        .unwrap();
    let node1 = testing::wait_for_pod_scheduled(client.clone(), "cache-1", Duration::from_secs(60))
        .await
        .unwrap();

    // Both pods should be on nodes in the same zone.
    // node-a1 and node-a2 are both in zone-a; node-b1 is in zone-b.
    let zone0 = if node0.starts_with("node-a") {
        "zone-a"
    } else {
        "zone-b"
    };
    let zone1 = if node1.starts_with("node-a") {
        "zone-a"
    } else {
        "zone-b"
    };
    assert_eq!(
        zone0, zone1,
        "both pods should be in same zone: {node0} ({zone0}) vs {node1} ({zone1})"
    );

    testing::nuke(client).await.unwrap();
}

/// 2 KWOK nodes in 2 zones, 3 pods with zone anti-affinity.
/// 2 pods should schedule, 1 stays Pending.
#[tokio::test]
async fn e2e_anti_affinity_insufficient_zones() {
    let client = growthrs::testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    let res = cpx22_resources();
    let topo = "topology.kubernetes.io/zone";

    // Only 2 zones
    testing::create_kwok_node(client.clone(), "node-a", &res, zone_labels("zone-a"))
        .await
        .unwrap();
    testing::create_kwok_node(client.clone(), "node-b", &res, zone_labels("zone-b"))
        .await
        .unwrap();

    // Create 3 pods with zone anti-affinity
    for i in 0..3 {
        testing::create_pod_with_affinity(
            client.clone(),
            &format!("web-{i}"),
            "1",
            "512Mi",
            "web",
            topo,
            true,
            None,
        )
        .await
        .unwrap();
    }

    // Wait for 2 pods to be scheduled
    let mut scheduled_count = 0;
    for i in 0..3 {
        match testing::wait_for_pod_scheduled(
            client.clone(),
            &format!("web-{i}"),
            Duration::from_secs(15),
        )
        .await
        {
            Ok(_) => scheduled_count += 1,
            Err(_) => {} // expected — one pod should remain unschedulable
        }
    }
    assert_eq!(
        scheduled_count, 2,
        "expected 2 scheduled pods (only 2 zones), got {scheduled_count}"
    );

    testing::nuke(client).await.unwrap();
}

/// 3 KWOK nodes all in zone-a (wrong plan for anti-affinity).
/// With zone anti-affinity, only 1 pod should schedule; 2 stay Pending.
/// Proves the harness detects divergence from a zone-spread plan.
#[tokio::test]
async fn e2e_divergence_all_same_zone() {
    let client = growthrs::testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    let res = cpx22_resources();
    let topo = "topology.kubernetes.io/zone";

    // All 3 nodes in the SAME zone
    testing::create_kwok_node(client.clone(), "node-a1", &res, zone_labels("zone-a"))
        .await
        .unwrap();
    testing::create_kwok_node(client.clone(), "node-a2", &res, zone_labels("zone-a"))
        .await
        .unwrap();
    testing::create_kwok_node(client.clone(), "node-a3", &res, zone_labels("zone-a"))
        .await
        .unwrap();

    // Create 3 pods with zone anti-affinity
    for i in 0..3 {
        testing::create_pod_with_affinity(
            client.clone(),
            &format!("web-{i}"),
            "1",
            "512Mi",
            "web",
            topo,
            true,
            None,
        )
        .await
        .unwrap();
    }

    // Only 1 should be scheduled (all nodes same zone, anti-affinity blocks others)
    let mut scheduled_count = 0;
    for i in 0..3 {
        match testing::wait_for_pod_scheduled(
            client.clone(),
            &format!("web-{i}"),
            Duration::from_secs(15),
        )
        .await
        {
            Ok(_) => scheduled_count += 1,
            Err(_) => {}
        }
    }
    assert_eq!(
        scheduled_count, 1,
        "expected only 1 pod scheduled (all nodes same zone), got {scheduled_count}"
    );

    testing::nuke(client).await.unwrap();
}
