//! E2E test for NRR cancellation convergence under partial failures.
//!
//! Verifies that `reconcile_node_removal_request` converges to a clean state
//! (no orphaned annotations, no lingering NRRs) after any single-step failure
//! in the three-operation cancellation sequence:
//!   1. remove_removal_candidate_annotation
//!   2. remove_delete_at_annotation
//!   3. delete_nrr
//!
//! Because `cancel_nrr` uses `?`, it bails on the first failure. The three
//! reachable scenarios and their expected residual state:
//!
//! ```text
//! Step failed | candidate ann. | delete-at ann. | NRR exists | Retryable?
//! ------------|----------------|----------------|------------|----------
//!      1      | present        | present        | yes        | yes (nothing changed)
//!      2      | gone           | present        | yes        | yes (idempotent remove)
//!      3      | gone           | gone           | yes        | yes (idempotent removes)
//! ```
//!
//! In all cases the NRR persists (delete is last), so the next reconcile retries.
//! Retries exercise Kubernetes merge-patch idempotency against a real KWOK
//! cluster — patching `null` on an already-absent annotation must be a no-op.
//!
//! Run with:
//!   cargo test --manifest-path growthrs/Cargo.toml --features failpoints --test cancel_nrr_convergence -- --nocapture
#![cfg(feature = "failpoints")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Node;
use kube::api::Api;

use growthrs::config::ControllerContext;
use growthrs::controller::node_removal::{reconcile_node_removal_request, scan_idle_nodes};
use growthrs::offering::{
    DELETE_AT_ANNOTATION, INSTANCE_TYPE_LABEL, MANAGED_BY_LABEL, MANAGED_BY_VALUE, POOL_LABEL,
    REMOVAL_CANDIDATE_ANNOTATION,
};
use growthrs::providers::kwok::KwokProvider;
use growthrs::providers::provider::Provider;
use growthrs::resources::node_pool::ServerTypeConfig;
use growthrs::resources::node_removal_request::NodeRemovalRequest;
use growthrs::testing;

fn make_ctx(client: kube::Client) -> Arc<ControllerContext> {
    Arc::new(ControllerContext {
        client: client.clone(),
        provider: Provider::Kwok(KwokProvider::new(client)),
        provisioning_timeout: Duration::from_secs(300),
        scale_down: growthrs::config::ScaleDownConfig {
            cooling_off_duration: Duration::from_secs(0),
            max_removal_attempts: 3,
            unmet_ttl: Duration::from_secs(120),
        },
        clock: Arc::new(growthrs::clock::SystemClock),
    })
}

/// Set up: clean cluster, create a NodePool, create a Growth-managed idle node,
/// run scan to create the NRR + annotations, then place a workload pod on the
/// node so the next reconcile will want to cancel the NRR.
async fn setup(client: kube::Client) {
    testing::nuke(client.clone()).await.unwrap();

    testing::create_node_pool(
        client.clone(),
        "default",
        vec![ServerTypeConfig {
            name: "cpx22".to_string(),
            max: 10,
            min: 0,
        }],
        BTreeMap::new(),
    )
    .await
    .unwrap();

    testing::create_kwok_node(
        client.clone(),
        "cancel-test-node",
        &growthrs::offering::Resources {
            cpu: 2,
            memory_mib: 4096,
            ephemeral_storage_gib: None,
            gpu: 0,
            gpu_model: None,
        },
        BTreeMap::from([
            (MANAGED_BY_LABEL.into(), MANAGED_BY_VALUE.into()),
            (POOL_LABEL.into(), "default".into()),
            (INSTANCE_TYPE_LABEL.into(), "cpx22".into()),
        ]),
    )
    .await
    .unwrap();

    // First scan: node is idle → creates NRR + annotations.
    let ctx = make_ctx(client.clone());
    scan_idle_nodes(ctx).await.unwrap();

    let nrrs = testing::list_node_removal_requests(client.clone())
        .await
        .unwrap();
    assert_eq!(nrrs.len(), 1, "setup: expected 1 NRR after initial scan");

    // Place a workload pod so the node is no longer idle.
    testing::create_pod(
        client.clone(),
        "cancel-test-pod",
        "100m",
        "128Mi",
        None,
        Some("default"),
    )
    .await
    .unwrap();

    // Wait for pod to be scheduled to the KWOK node.
    testing::wait_for_pod_scheduled(client, "cancel-test-pod", Duration::from_secs(30))
        .await
        .expect("pod should be scheduled to the KWOK node");
}

/// Fetch the single NRR from the cluster.
async fn get_nrr(client: &kube::Client) -> NodeRemovalRequest {
    let nrrs = testing::list_node_removal_requests(client.clone())
        .await
        .unwrap();
    assert_eq!(nrrs.len(), 1, "expected exactly 1 NRR");
    nrrs.into_iter().next().unwrap()
}

async fn node_annotations(client: &kube::Client, name: &str) -> BTreeMap<String, String> {
    let api: Api<Node> = Api::all(client.clone());
    api.get(name)
        .await
        .ok()
        .and_then(|n| n.metadata.annotations)
        .unwrap_or_default()
}

async fn assert_converged(client: &kube::Client, label: &str) {
    let nrrs = testing::list_node_removal_requests(client.clone())
        .await
        .unwrap();
    assert!(
        nrrs.is_empty(),
        "{label}: expected 0 NRRs, found {}",
        nrrs.len()
    );

    let annotations = node_annotations(client, "cancel-test-node").await;
    assert!(
        !annotations.contains_key(REMOVAL_CANDIDATE_ANNOTATION),
        "{label}: removal-candidate annotation should be gone"
    );
    assert!(
        !annotations.contains_key(DELETE_AT_ANNOTATION),
        "{label}: delete-at annotation should be gone"
    );
}

struct Scenario {
    label: &'static str,
    failpoint: &'static str,
    /// Whether the removal-candidate annotation should still be present after the faulted reconcile.
    expect_candidate_present: bool,
    /// Whether the delete-at annotation should still be present after the faulted reconcile.
    expect_delete_at_present: bool,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        label: "step1 (remove_candidate fails)",
        failpoint: "cancel_nrr_remove_candidate",
        expect_candidate_present: true,
        expect_delete_at_present: true,
    },
    Scenario {
        label: "step2 (remove_delete_at fails)",
        failpoint: "cancel_nrr_remove_delete_at",
        expect_candidate_present: false,
        expect_delete_at_present: true,
    },
    Scenario {
        label: "step3 (delete_nrr fails)",
        failpoint: "cancel_nrr_delete_nrr",
        expect_candidate_present: false,
        expect_delete_at_present: false,
    },
];

#[tokio::test]
async fn e2e_cancel_nrr_converges_after_partial_failure() {
    testing::init_tracing();
    let _fail_scenario = fail::FailScenario::setup();
    let client = testing::test_client().await;

    for scenario in SCENARIOS {
        println!("=== {} ===", scenario.label);
        setup(client.clone()).await;

        // Inject failure and reconcile the NRR — should return Err.
        fail::cfg(scenario.failpoint, "return").unwrap();
        let ctx = make_ctx(client.clone());
        let nrr = get_nrr(&client).await;
        let result = reconcile_node_removal_request(Arc::new(nrr), ctx).await;
        assert!(
            result.is_err(),
            "{}: expected Err from faulted reconcile, got Ok",
            scenario.label
        );

        // NRR must persist — cancel_nrr bails before delete_nrr on any failure.
        let nrrs = testing::list_node_removal_requests(client.clone())
            .await
            .unwrap();
        assert_eq!(nrrs.len(), 1, "{}: NRR should persist", scenario.label);

        // Verify which annotations remain based on how far cancel_nrr got.
        let annotations = node_annotations(&client, "cancel-test-node").await;
        assert_eq!(
            annotations.contains_key(REMOVAL_CANDIDATE_ANNOTATION),
            scenario.expect_candidate_present,
            "{}: removal-candidate annotation",
            scenario.label,
        );
        assert_eq!(
            annotations.contains_key(DELETE_AT_ANNOTATION),
            scenario.expect_delete_at_present,
            "{}: delete-at annotation",
            scenario.label,
        );

        // Disable fault, retry — must converge to clean state.
        // This exercises merge-patch idempotency: annotation removes that already
        // succeeded are re-issued and must be no-ops on the real API server.
        fail::cfg(scenario.failpoint, "off").unwrap();
        let ctx = make_ctx(client.clone());
        let nrr = get_nrr(&client).await;
        let result = reconcile_node_removal_request(Arc::new(nrr), ctx).await;
        assert!(
            result.is_ok(),
            "{}: expected Ok from retry reconcile, got Err: {}",
            scenario.label,
            result.unwrap_err(),
        );
        assert_converged(&client, scenario.label).await;
    }

    testing::nuke(client).await.unwrap();
}
