//! E2E watcher restart consistency test — requires a KWOK-enabled cluster.
//!
//! Verifies that the pod watcher correctly recovers state after a
//! mid-reconcile crash, preventing duplicate NodeRequests for pods
//! already covered by existing NRs.
//!
//! Uses the `fail` crate's fail points to deterministically stop the
//! reconciler after exactly N NR creates. Three representative crash
//! points cover the interesting boundaries:
//! - **Early** (crash@3): most NRs still need creating on recovery
//! - **Mid** (crash@15): half-done state, equal split
//! - **Late** (crash@27): almost complete, recovery creates a few stragglers
//!
//! Run with:
//!   cargo test --manifest-path growthrs/Cargo.toml --features failpoints --test watcher_restart -- --nocapture
#![cfg(feature = "failpoints")]

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use growthrs::controller::{ControllerContext, run_pod_watcher_standalone};
use growthrs::crds::node_request::NodeRequest;
use growthrs::providers::kwok::KwokProvider;
use growthrs::providers::provider::Provider;
use growthrs::testing;

/// 60 pods at 1 cpu each → 30 cx22 nodes (2 cpu each).
const EXPECTED_NRS: usize = 30;
const POD_COUNT: u32 = 60;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

fn make_ctx(client: kube::Client) -> Arc<ControllerContext> {
    Arc::new(ControllerContext {
        client: client.clone(),
        provider: Provider::Kwok(KwokProvider::new(client)),
        provisioning_timeout: Duration::from_secs(300),
        scale_down: growthrs::controller::ScaleDownConfig::default(),
    })
}

/// Clean slate: nuke, create pool + pods, wait for all pods Unschedulable.
async fn setup_round(client: kube::Client, round: usize) {
    testing::nuke(client.clone()).await.unwrap();

    testing::create_node_pool(
        client.clone(),
        "default",
        vec![growthrs::crds::node_pool::ServerTypeConfig {
            name: "cx22".to_string(),
            max: EXPECTED_NRS as u32,
            min: 0,
        }],
        BTreeMap::new(),
    )
    .await
    .unwrap();

    testing::create_many_pods(
        client.clone(),
        &format!("r{round}"),
        POD_COUNT,
        "1",
        "512Mi",
        None,
        Some("default"),
    )
    .await
    .unwrap();

    wait_for_all_pods_unschedulable(client, POD_COUNT as usize).await;
}

/// Poll until at least `expected` pods have PodScheduled=False/Unschedulable.
async fn wait_for_all_pods_unschedulable(client: kube::Client, expected: usize) {
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::ListParams;
    use kube::Api;

    let pods_api: Api<Pod> = Api::all(client);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    loop {
        let lp = ListParams::default().fields("status.phase=Pending");
        let pods = pods_api.list(&lp).await.unwrap();
        let count = pods
            .iter()
            .filter(|pod| {
                pod.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conds| {
                        conds.iter().any(|c| {
                            c.type_ == "PodScheduled"
                                && c.status == "False"
                                && c.reason.as_deref() == Some("Unschedulable")
                        })
                    })
                    .unwrap_or(false)
            })
            .count();

        if count >= expected {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("only {count}/{expected} pods unschedulable after 60s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Poll NRs until the count is unchanged for `stable_duration`.
async fn wait_for_stable_nr_count(
    client: kube::Client,
    stable_duration: Duration,
) -> Vec<NodeRequest> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut last_count = 0usize;
    let mut stable_since = tokio::time::Instant::now();

    loop {
        let nrs = testing::list_node_requests(client.clone()).await.unwrap();
        if nrs.len() != last_count {
            last_count = nrs.len();
            stable_since = tokio::time::Instant::now();
        } else if tokio::time::Instant::now().duration_since(stable_since) >= stable_duration {
            return nrs;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("NR count did not stabilise within 60s (count={last_count})");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Assert: claimed_pod_uids are populated and no pod UID appears in more than one NR.
fn assert_no_duplicate_uids(nrs: &[NodeRequest], label: &str) {
    let total_claimed: usize = nrs.iter().map(|nr| nr.spec.claimed_pod_uids.len()).sum();
    assert!(
        total_claimed > 0,
        "{label}: {n} NRs but all have empty claimed_pod_uids — \
         CRD schema may be missing claimedPodUids",
        n = nrs.len(),
    );

    let all_uids: Vec<&str> = nrs
        .iter()
        .flat_map(|nr| nr.spec.claimed_pod_uids.iter().map(|s| s.as_str()))
        .collect();
    let unique: HashSet<&str> = all_uids.iter().copied().collect();
    assert_eq!(
        all_uids.len(),
        unique.len(),
        "{label}: duplicate pod UIDs across NodeRequests! \
         total={}, unique={}, nrs={}",
        all_uids.len(),
        unique.len(),
        nrs.len(),
    );

    println!("{label}: OK — {} NRs, {total_claimed} claimed UIDs", nrs.len());
}

/// Configure the fail point so the watcher exits after `crash_after` NR
/// creates, then spawn the watcher and wait for it to exit. Returns the
/// number of NodeRequests present after the crash.
async fn run_watcher_with_fault(client: kube::Client, crash_after: usize) -> usize {
    // "N*off->return" means: pass through N times, then return (trigger the
    // fail point closure) on every subsequent call.
    fail::cfg("reconcile_after_nr_create", &format!("{crash_after}*off->return")).unwrap();

    let ctx = make_ctx(client.clone());
    let handle = tokio::spawn(run_pod_watcher_standalone(ctx));

    match tokio::time::timeout(Duration::from_secs(30), handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("watcher error: {e}"),
        Ok(Err(e)) => panic!("watcher panicked: {e}"),
        Err(_) => panic!("watcher did not exit within 30s"),
    }

    fail::cfg("reconcile_after_nr_create", "off").unwrap();
    testing::list_node_requests(client).await.unwrap().len()
}

/// Spawn watcher without faults, wait for NRs to stabilise, return them.
async fn run_recovery_watcher(client: kube::Client) -> Vec<NodeRequest> {
    let ctx = make_ctx(client.clone());
    let handle = tokio::spawn(run_pod_watcher_standalone(ctx));
    let nrs = wait_for_stable_nr_count(client, Duration::from_secs(3)).await;
    handle.abort();
    let _ = handle.await;
    nrs
}

/// Crash the reconciler at early/mid/late points and verify the recovery
/// watcher produces exactly EXPECTED_NRS with no duplicate pod UIDs.
#[tokio::test]
async fn watcher_recovers_after_mid_reconcile_crash() {
    init_tracing();
    let _scenario = fail::FailScenario::setup();
    let client = growthrs::testing::test_client().await;

    for (round, crash_after) in [3, 15, 27].iter().enumerate() {
        println!("=== crash after {crash_after} NR creates ===");
        setup_round(client.clone(), round).await;

        let nrs_at_crash = run_watcher_with_fault(client.clone(), *crash_after).await;
        println!("  faulted with {nrs_at_crash} NRs (target {crash_after})");

        let nrs = run_recovery_watcher(client.clone()).await;
        assert_eq!(nrs.len(), EXPECTED_NRS, "expected {EXPECTED_NRS} NRs after recovery");
        assert_no_duplicate_uids(&nrs, &format!("crash@{crash_after}"));
    }

    testing::nuke(client).await.unwrap();
}
