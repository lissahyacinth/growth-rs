use futures_util::StreamExt;
use kube::runtime::reflector::ObjectRef;
use kube::runtime::{Controller, watcher};

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Node;
use kube::Api;
use kube::api::PatchParams;
use kube::runtime::controller::Action;
use tracing::{debug, info, instrument, warn};

use crate::offering::{MANAGED_BY_SELECTOR, NODE_REQUEST_LABEL, STARTUP_TAINT_KEY};
use crate::resources::node_request::{NodeRequest, NodeRequestPhase};

use super::{ControllerContext, ControllerError, update_node_request_phase};

/// Map Node events to NodeRequest reconciles.
///
/// Triggers when a node has the `growth.vettrdev.com/node-request` label
/// (set via cloud-init --node-label or by KWOK) and is Ready.
fn is_growth_node_ready(node: Node) -> Option<ObjectRef<NodeRequest>> {
    let labels = node.metadata.labels.as_ref()?;
    let nr_name = labels.get(NODE_REQUEST_LABEL)?;

    let ready = node
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|conditions| {
            conditions
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
        .unwrap_or(false);

    ready.then(|| ObjectRef::new(nr_name.as_str()))
}

pub(crate) async fn node_controller(ctx: Arc<ControllerContext>) -> Result<(), ControllerError> {
    let nrs = Api::<NodeRequest>::all(ctx.client.clone());
    let mut stream = std::pin::pin!(
        Controller::new(nrs, watcher::Config::default())
            .watches(
                Api::<Node>::all(ctx.client.clone()),
                watcher::Config::default().labels(MANAGED_BY_SELECTOR),
                is_growth_node_ready,
            )
            .run(reconcile_node_request, error_policy, ctx)
    );
    while let Some(result) = stream.next().await {
        let (obj, _) = result.map_err(ControllerError::from_controller_error)?;
        debug!(name = %obj.name, "reconciled node");
    }
    Ok(())
}

// Reconcile paths:
//  - NR not Provisioning (e.g. Ready on re-trigger) → phase guard, no-op
//  - NR Provisioning + Node Ready (first event)     → mapper fires, we transition below
//  - NR missing or Node not Ready                   → mapper returns None, never reaches here
#[instrument(skip_all, fields(nr = obj.metadata.name.as_deref().unwrap_or("<unknown>")))]
pub async fn reconcile_node_request(
    obj: Arc<NodeRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ControllerError> {
    let name = obj.metadata.name.as_deref().unwrap_or("<unknown>");
    if obj.phase() != NodeRequestPhase::Provisioning {
        return Ok(Action::await_change());
    }

    // is_growth_node_ready already verified Ready=True before dispatching here.
    // Remove the startup taint before transitioning — unblocks the scheduler.
    info!(name, node_id = %obj.spec.node_id, "node ready, removing startup taint and transitioning NodeRequest");
    remove_startup_taint(&ctx.client, &obj.spec.node_id).await?;
    let now = ctx.clock.now();
    update_node_request_phase(&ctx.client, name, NodeRequestPhase::Ready, now).await?;
    Ok(Action::await_change())
}

/// Remove the `growth.vettrdev.com/unregistered: NoExecute` startup taint from a node.
///
/// Reads the current taints, filters out the startup taint, and patches back.
/// 404 is treated as success (node already gone).
async fn remove_startup_taint(
    client: &kube::Client,
    node_name: &str,
) -> Result<(), ControllerError> {
    let api: Api<Node> = Api::all(client.clone());
    let node = match api.get(node_name).await {
        Ok(n) => n,
        Err(kube::Error::Api(ref resp)) if resp.code == 404 => {
            debug!(
                node_name,
                "node already deleted, skipping startup taint removal"
            );
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let remaining: Vec<_> = node
        .spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|ts| {
            ts.iter()
                .filter(|t| !(t.key == STARTUP_TAINT_KEY && t.effect == "NoExecute"))
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    let taints_value = if remaining.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::to_value(&remaining).expect("taint serialization cannot fail")
    };

    let patch = serde_json::json!({ "spec": { "taints": taints_value } });
    match api
        .patch(
            node_name,
            &PatchParams::default(),
            &kube::api::Patch::Merge(patch),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref resp)) if resp.code == 404 => {
            debug!(
                node_name,
                "node already deleted, skipping startup taint removal"
            );
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

pub(super) fn error_policy(
    _nr: Arc<NodeRequest>,
    error: &ControllerError,
    _ctx: Arc<ControllerContext>,
) -> Action {
    warn!(%error, "node reconcile failed, requeuing");
    Action::requeue(Duration::from_secs(5))
}
