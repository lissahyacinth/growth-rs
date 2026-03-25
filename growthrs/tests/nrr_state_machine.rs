//! Integration tests for the [NodeRemovalRequest](../src/controller/node_removal) state machine (`decide_phase`).
//!
//! Tests cover every branch of the NRR reconciler using a KWOK cluster for real
//! K8s API operations and FakeProvider for scripted provider responses.
//!
//! ```text
//! Test                              | Phase          | Scenario
//! ----------------------------------|----------------|----------------------------------
//! cooling_off_not_elapsed           | Pending        | Node idle, cooling-off not done → requeue
//! cooling_off_elapsed_to_deprov     | Pending        | Node idle, cooling-off done → Deprovisioning
//! node_404_triggers_provider_delete | Pending        | K8s Node gone → provider.delete() + cleanup
//! provider_delete_fails_requeues    | Pending        | provider.delete() fails → requeue
//! deprov_not_found_cleans_up        | Deprovisioning | provider.status()=NotFound → cleanup
//! deprov_max_attempts_terminal      | Deprovisioning | attempts >= max → CouldNotRemove
//! deprov_status_fails_requeues      | Deprovisioning | provider.status() error → requeue
//! could_not_remove_is_terminal      | CouldNotRemove | No state change, await_change
//! ```
//!
//! Run with:
//!   cargo test --manifest-path growthrs/Cargo.toml --features testing --test nrr_state_machine -- --nocapture
#![cfg(feature = "testing")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kube::api::Api;
use kube::runtime::controller::Action;

use growthrs::clock::{Clock, TestClock};
use growthrs::config::{ControllerContext, ScaleDownConfig};
use growthrs::controller::node_removal::reconcile_node_removal_request;
use growthrs::offering::{INSTANCE_TYPE_LABEL, MANAGED_BY_LABEL, MANAGED_BY_VALUE, POOL_LABEL};
use growthrs::providers::fake::{DeleteBehavior, FakeProvider, StatusBehavior};
use growthrs::providers::provider::{Provider, ProviderStatus};
use growthrs::resources::node_removal_request::{
    NodeRemovalRequest, NodeRemovalRequestPhase, NodeRemovalRequestStatus,
};
use growthrs::testing;

/// Check that an action represents a requeue (not await_change).
fn is_requeue(action: &Action) -> bool {
    *action != Action::await_change()
}

fn make_ctx(
    client: kube::Client,
    provider: FakeProvider,
    cooling_off: Duration,
    clock: Arc<dyn Clock>,
    max_removal_attempts: u32,
) -> Arc<ControllerContext> {
    Arc::new(ControllerContext {
        client: client.clone(),
        provider: Provider::Fake(provider),
        provisioning_timeout: Duration::from_secs(300),
        scale_down: ScaleDownConfig {
            cooling_off_duration: cooling_off,
            max_removal_attempts,
            unmet_ttl: Duration::from_secs(120),
        },
        clock,
    })
}

fn growth_labels() -> BTreeMap<String, String> {
    BTreeMap::from([
        (MANAGED_BY_LABEL.into(), MANAGED_BY_VALUE.into()),
        (POOL_LABEL.into(), "default".into()),
        (INSTANCE_TYPE_LABEL.into(), "cpx22".into()),
    ])
}

async fn create_idle_node(client: kube::Client, name: &str) {
    testing::create_kwok_node(
        client,
        name,
        &growthrs::offering::Resources {
            cpu: 2,
            memory_mib: 4096,
            ephemeral_storage_gib: None,
            gpu: 0,
            gpu_model: None,
        },
        growth_labels(),
    )
    .await
    .unwrap();
}

async fn create_nrr(
    client: kube::Client,
    node_name: &str,
    node_uid: Option<&str>,
    phase: NodeRemovalRequestPhase,
) -> NodeRemovalRequest {
    let now = k8s_openapi::jiff::Timestamp::now();
    growthrs::controller::node_removal::create_node_removal_request(
        client, node_name, node_uid, "default", "cpx22", phase, now,
    )
    .await
    .unwrap()
}

async fn get_nrr(client: &kube::Client, name: &str) -> Option<NodeRemovalRequest> {
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    api.get_opt(name).await.unwrap()
}

async fn patch_nrr_status(
    client: &kube::Client,
    name: &str,
    phase: NodeRemovalRequestPhase,
    removal_attempts: u32,
) {
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    let status = NodeRemovalRequestStatus {
        phase,
        removal_attempts,
        last_transition_time: None,
    };
    let patch = serde_json::json!({ "status": status });
    api.patch_status(
        name,
        &kube::api::PatchParams::apply("growthrs-test"),
        &kube::api::Patch::Merge(patch),
    )
    .await
    .unwrap();
}

/// Get the node UID for owner reference setup.
async fn node_uid(client: &kube::Client, name: &str) -> String {
    let api: Api<k8s_openapi::api::core::v1::Node> = Api::all(client.clone());
    api.get(name).await.unwrap().metadata.uid.unwrap()
}

#[tokio::test]
async fn e2e_cooling_off_not_elapsed() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    create_idle_node(client.clone(), "co-node").await;
    let uid = node_uid(&client, "co-node").await;
    create_nrr(
        client.clone(),
        "co-node",
        Some(&uid),
        NodeRemovalRequestPhase::Pending,
    )
    .await;

    // Cooling-off = 5 minutes → reconcile should requeue, not delete.
    let provider = FakeProvider::new();
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(client.clone(), provider, Duration::from_secs(300), clock, 3);

    let nrr = get_nrr(&client, "nrr-co-node").await.unwrap();
    let action = reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // Should requeue (not await_change) — cooling-off is still active.
    assert!(is_requeue(&action), "expected requeue, got await_change");

    // NRR should still exist in Pending phase.
    let nrr = get_nrr(&client, "nrr-co-node").await.unwrap();
    assert_eq!(nrr.phase(), NodeRemovalRequestPhase::Pending);

    // Provider should not have been called.
    if let Provider::Fake(ref f) = ctx.provider {
        assert!(
            f.delete_calls().is_empty(),
            "provider.delete should not be called during cooling-off"
        );
    }

    testing::nuke(client).await.unwrap();
}

#[tokio::test]
/// Idle Node is removed after cooling-off period.
async fn e2e_idle_node_removal() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    create_idle_node(client.clone(), "co-node").await;
    let uid = node_uid(&client, "co-node").await;
    create_nrr(
        client.clone(),
        "co-node",
        Some(&uid),
        NodeRemovalRequestPhase::Pending,
    )
    .await;

    // Cooling-off = 5 minutes → reconcile should requeue, not delete.
    let provider = FakeProvider::new();
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(
        client.clone(),
        provider,
        Duration::from_secs(300),
        clock.clone(),
        3,
    );

    let nrr = get_nrr(&client, "nrr-co-node").await.unwrap();
    // Set clock forwards 5 minutes.
    clock.tick(Duration::from_mins(5));
    let _ = reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // NRR should be in Deprovisioning phase.
    let nrr = get_nrr(&client, "nrr-co-node").await.unwrap();
    assert_eq!(nrr.phase(), NodeRemovalRequestPhase::Deprovisioning);

    // Provider should not have been called.
    if let Provider::Fake(ref f) = ctx.provider {
        assert!(
            !f.delete_calls().is_empty(),
            "provider.delete should be called to delete the node"
        );
    }

    testing::nuke(client).await.unwrap();
}

#[tokio::test]
async fn e2e_cooling_off_elapsed_to_deprovisioning() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    create_idle_node(client.clone(), "deprov-node").await;
    let uid = node_uid(&client, "deprov-node").await;
    create_nrr(
        client.clone(),
        "deprov-node",
        Some(&uid),
        NodeRemovalRequestPhase::Pending,
    )
    .await;

    // Cooling-off = 0 → should proceed immediately.
    let provider = FakeProvider::new().with_default_delete(DeleteBehavior::Succeed);
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(client.clone(), provider, Duration::from_secs(0), clock, 3);

    let nrr = get_nrr(&client, "nrr-deprov-node").await.unwrap();
    reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // NRR should transition to Deprovisioning.
    let nrr = get_nrr(&client, "nrr-deprov-node").await.unwrap();
    assert_eq!(nrr.phase(), NodeRemovalRequestPhase::Deprovisioning);
    assert_eq!(nrr.status.as_ref().unwrap().removal_attempts, 1);

    // Provider.delete should have been called.
    if let Provider::Fake(ref f) = ctx.provider {
        assert_eq!(f.delete_calls().len(), 1);
    }

    testing::nuke(client).await.unwrap();
}

#[tokio::test]
async fn e2e_node_404_triggers_provider_delete() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    // Create NRR without a K8s Node — the taint will hit 404.
    create_nrr(
        client.clone(),
        "ghost-node",
        None,
        NodeRemovalRequestPhase::Pending,
    )
    .await;

    let provider = FakeProvider::new().with_default_delete(DeleteBehavior::Succeed);
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(client.clone(), provider, Duration::from_secs(0), clock, 3);

    let nrr = get_nrr(&client, "nrr-ghost-node").await.unwrap();
    reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // Provider.delete should have been called (to clean up the VM).
    if let Provider::Fake(ref f) = ctx.provider {
        assert_eq!(
            f.delete_calls().len(),
            1,
            "provider.delete must be called when K8s Node is 404"
        );
    }

    // NRR should be deleted (finalizer removed, then NRR deleted).
    let nrr = get_nrr(&client, "nrr-ghost-node").await;
    assert!(
        nrr.is_none(),
        "NRR should be deleted after node-404 cleanup"
    );

    testing::nuke(client).await.unwrap();
}

#[tokio::test]
async fn e2e_provider_delete_fails_requeues() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    create_idle_node(client.clone(), "fail-node").await;
    let uid = node_uid(&client, "fail-node").await;
    create_nrr(
        client.clone(),
        "fail-node",
        Some(&uid),
        NodeRemovalRequestPhase::Pending,
    )
    .await;

    // Script provider.delete to fail.
    let provider =
        FakeProvider::new().on_next_delete(DeleteBehavior::Fail("simulated failure".into()));
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(client.clone(), provider, Duration::from_secs(0), clock, 3);

    let nrr = get_nrr(&client, "nrr-fail-node").await.unwrap();
    let action = reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // Should requeue for retry.
    assert!(
        is_requeue(&action),
        "expected requeue after provider.delete failure"
    );

    // NRR should still be Pending (no phase transition on delete failure).
    let nrr = get_nrr(&client, "nrr-fail-node").await.unwrap();
    assert_eq!(nrr.phase(), NodeRemovalRequestPhase::Pending);

    testing::nuke(client).await.unwrap();
}

#[tokio::test]
async fn deprovisioning_not_found_cleans_up() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    create_idle_node(client.clone(), "done-node").await;
    let uid = node_uid(&client, "done-node").await;
    create_nrr(
        client.clone(),
        "done-node",
        Some(&uid),
        NodeRemovalRequestPhase::Deprovisioning,
    )
    .await;

    // Provider says node is gone.
    let provider =
        FakeProvider::new().with_default_status(StatusBehavior::Return(ProviderStatus::NotFound));
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(client.clone(), provider, Duration::from_secs(0), clock, 3);

    let nrr = get_nrr(&client, "nrr-done-node").await.unwrap();
    reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // NRR should be deleted.
    let nrr = get_nrr(&client, "nrr-done-node").await;
    assert!(
        nrr.is_none(),
        "NRR should be cleaned up when provider confirms NotFound"
    );

    testing::nuke(client).await.unwrap();
}

#[tokio::test]
async fn deprovisioning_max_attempts_goes_terminal() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    create_idle_node(client.clone(), "stuck-node").await;
    let uid = node_uid(&client, "stuck-node").await;
    create_nrr(
        client.clone(),
        "stuck-node",
        Some(&uid),
        NodeRemovalRequestPhase::Deprovisioning,
    )
    .await;

    // Set removal_attempts to max (3).
    patch_nrr_status(
        &client,
        "nrr-stuck-node",
        NodeRemovalRequestPhase::Deprovisioning,
        3,
    )
    .await;

    // Provider says node still exists.
    let provider =
        FakeProvider::new().with_default_status(StatusBehavior::Return(ProviderStatus::Running));
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(client.clone(), provider, Duration::from_secs(0), clock, 3);

    let nrr = get_nrr(&client, "nrr-stuck-node").await.unwrap();
    reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // Should transition to CouldNotRemove.
    let nrr = get_nrr(&client, "nrr-stuck-node").await.unwrap();
    assert_eq!(nrr.phase(), NodeRemovalRequestPhase::CouldNotRemove);

    testing::nuke(client).await.unwrap();
}

#[tokio::test]
async fn deprovisioning_status_error_requeues() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    create_idle_node(client.clone(), "err-node").await;
    let uid = node_uid(&client, "err-node").await;
    create_nrr(
        client.clone(),
        "err-node",
        Some(&uid),
        NodeRemovalRequestPhase::Deprovisioning,
    )
    .await;

    // Provider status check fails.
    let provider = FakeProvider::new()
        .on_next_status(StatusBehavior::InternalError("provider unreachable".into()));
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(client.clone(), provider, Duration::from_secs(0), clock, 3);

    let nrr = get_nrr(&client, "nrr-err-node").await.unwrap();
    let action = reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // Should requeue.
    assert!(
        is_requeue(&action),
        "expected requeue after provider.status error"
    );

    // NRR should still be Deprovisioning.
    let nrr = get_nrr(&client, "nrr-err-node").await.unwrap();
    assert_eq!(nrr.phase(), NodeRemovalRequestPhase::Deprovisioning);

    testing::nuke(client).await.unwrap();
}

#[tokio::test]
async fn could_not_remove_is_terminal() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    create_idle_node(client.clone(), "terminal-node").await;
    let uid = node_uid(&client, "terminal-node").await;
    create_nrr(
        client.clone(),
        "terminal-node",
        Some(&uid),
        NodeRemovalRequestPhase::Pending,
    )
    .await;

    // Patch directly to CouldNotRemove.
    patch_nrr_status(
        &client,
        "nrr-terminal-node",
        NodeRemovalRequestPhase::CouldNotRemove,
        3,
    )
    .await;

    let provider = FakeProvider::new();
    let clock = Arc::new(TestClock::new());
    let ctx = make_ctx(client.clone(), provider, Duration::from_secs(0), clock, 3);

    let nrr = get_nrr(&client, "nrr-terminal-node").await.unwrap();
    let action = reconcile_node_removal_request(Arc::new(nrr), ctx.clone())
        .await
        .unwrap();

    // Should await_change (no requeue — terminal state).
    assert_eq!(
        action,
        Action::await_change(),
        "CouldNotRemove should not requeue"
    );

    // No provider calls.
    if let Provider::Fake(ref f) = ctx.provider {
        assert!(f.delete_calls().is_empty());
        assert!(f.status_calls().is_empty());
    }

    testing::nuke(client).await.unwrap();
}
