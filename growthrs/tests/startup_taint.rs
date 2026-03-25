//! E2E test for the startup taint lifecycle.
//!
//! Verifies that KWOK nodes are created with the `growth.vettrdev.com/unregistered: NoExecute`
//! taint and that the node watcher correctly removes it when the node becomes Ready.
//!
//! Run with:
//!   cargo test --manifest-path growthrs/Cargo.toml --features testing --test startup_taint -- --nocapture
#![cfg(feature = "testing")]

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::Node;
use kube::Api;
use kube::runtime::controller::Action;

use growthrs::controller::node::reconcile_node_request;
use growthrs::offering::{
    INSTANCE_TYPE_LABEL, InstanceType, Location, MANAGED_BY_LABEL, MANAGED_BY_VALUE,
    NODE_REQUEST_LABEL, Offering, POOL_LABEL, Region, Resources, STARTUP_TAINT_KEY, Zone,
};
use growthrs::providers::fake::FakeProvider;
use growthrs::providers::kwok::KwokProvider;
use growthrs::providers::provider::{InstanceConfig, Provider};
use growthrs::resources::node_request::{NodeRequest, NodeRequestPhase, NodeRequestSpec};
use growthrs::testing;

fn cpx22_offering() -> Offering {
    Offering {
        instance_type: InstanceType("cpx22".into()),
        resources: Resources {
            cpu: 2,
            memory_mib: 4096,
            ephemeral_storage_gib: Some(40),
            gpu: 0,
            gpu_model: None,
        },
        cost_per_hour: 0.01,
        location: Location {
            region: Region("eu-central".into()),
            zone: Some(Zone("fsn1-dc14".into())),
        },
    }
}

/// KWOK node created via the provider has the startup taint, and the node watcher
/// reconciler removes it when the NodeRequest is in Provisioning phase.
#[tokio::test]
async fn e2e_kwok_node_has_startup_taint_removed_on_ready() {
    testing::init_tracing();
    let client = testing::test_client().await;
    testing::nuke(client.clone()).await.unwrap();

    // Create a KWOK node via the provider (this is what the controller does).
    let kwok = KwokProvider::new(client.clone());
    let config = InstanceConfig {
        labels: BTreeMap::from([
            (MANAGED_BY_LABEL.into(), MANAGED_BY_VALUE.into()),
            (POOL_LABEL.into(), "taint-pool".into()),
            (INSTANCE_TYPE_LABEL.into(), "cpx22".into()),
            (NODE_REQUEST_LABEL.into(), "nr-taint-test".into()),
        ]),
    };
    let offering = cpx22_offering();
    kwok.create("taint-test-node".into(), &offering, &config)
        .await
        .unwrap();

    // Verify the node was created with the startup taint.
    let nodes: Api<Node> = Api::all(client.clone());
    let node = nodes.get("taint-test-node").await.unwrap();
    assert!(
        testing::has_taint(&node, STARTUP_TAINT_KEY, "NoExecute"),
        "node should have the startup taint"
    );

    // Create a NodeRequest, then patch its status to Provisioning via the status
    // subresource (Kubernetes ignores status on POST /create).
    let nr_api: Api<NodeRequest> = Api::all(client.clone());
    let nr = NodeRequest {
        metadata: kube::api::ObjectMeta {
            name: Some("nr-taint-test".into()),
            ..Default::default()
        },
        spec: NodeRequestSpec {
            node_id: "taint-test-node".into(),
            target_offering: InstanceType("cpx22".into()),
            location: Region("eu-central".into()),
            resources: offering.resources,
        },
        status: None,
    };
    nr_api
        .create(&kube::api::PostParams::default(), &nr)
        .await
        .unwrap();

    let status_patch = serde_json::json!({
        "status": {
            "phase": "Provisioning",
            "events": [],
            "lastTransitionTime": null
        }
    });
    nr_api
        .patch_status(
            "nr-taint-test",
            &kube::api::PatchParams::apply("growthrs-test"),
            &kube::api::Patch::Merge(status_patch),
        )
        .await
        .unwrap();

    // Call the reconciler directly (simulates what the node watcher mapper triggers).
    let ctx = testing::make_test_ctx(client.clone(), Provider::Fake(FakeProvider::new()));
    let nr_obj = Arc::new(nr_api.get("nr-taint-test").await.unwrap());
    let action = reconcile_node_request(nr_obj, ctx).await.unwrap();
    assert_eq!(action, Action::await_change());

    // Verify the startup taint was removed.
    let node = nodes.get("taint-test-node").await.unwrap();
    assert!(
        !testing::has_taint(&node, STARTUP_TAINT_KEY, "NoExecute"),
        "startup taint should have been removed"
    );

    // Verify the NodeRequest transitioned to Ready.
    let nr = nr_api.get("nr-taint-test").await.unwrap();
    assert_eq!(nr.phase(), NodeRequestPhase::Ready);

    testing::nuke(client).await.unwrap();
}
