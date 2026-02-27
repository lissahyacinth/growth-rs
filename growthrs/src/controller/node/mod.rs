use anyhow::Context;
use futures_util::StreamExt;
use kube::runtime::reflector::ObjectRef;
use kube::runtime::{Controller, watcher};

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Node;
use kube::Api;
use kube::runtime::controller::Action;
use tracing::{debug, info, warn};

use crate::node_request::{NodeRequest, NodeRequestPhase};
use crate::offering::NODE_REQUEST_LABEL;

use super::{ControllerContext, update_node_request_phase};

fn is_node_ready(node: Node) -> Option<ObjectRef<NodeRequest>> {
    let conditions = node.status.as_ref()?.conditions.as_ref()?;
    let ready = conditions
        .iter()
        .any(|c| c.type_ == "Ready" && c.status == "True");
    ready
        .then(|| {
            node.metadata
                .labels
                .unwrap_or_default()
                .get(NODE_REQUEST_LABEL)
                .map(String::from)
        })
        .flatten()
        .map(|label| ObjectRef::new(label.as_str()))
}

pub(crate) async fn node_controller(ctx: Arc<ControllerContext>) -> anyhow::Result<()> {
    let nrs = Api::<NodeRequest>::all(ctx.client.clone());
    let mut stream = std::pin::pin!(
        Controller::new(nrs, watcher::Config::default())
            .watches(
                Api::<Node>::all(ctx.client.clone()),
                watcher::Config::default().labels("app.kubernetes.io/managed-by=growth"),
                is_node_ready,
            )
            .run(reconcile_node_request, error_policy, ctx)
    );
    while let Some(result) = stream.next().await {
        let (obj, _) = result.context("node controller stream error")?;
        debug!(name = %obj.name, "reconciled node");
    }
    Ok(())
}

async fn reconcile_node_request(
    obj: Arc<NodeRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, kube::Error> {
    let name = obj.metadata.name.as_deref().unwrap_or("<unknown>");
    if obj.phase() == NodeRequestPhase::Provisioning {
        info!(name, node_id = %obj.spec.node_id, "node ready, transitioning NodeRequest");
        update_node_request_phase(&ctx.client, name, NodeRequestPhase::Ready).await?;
    }
    Ok(Action::await_change())
}

pub(super) fn error_policy(
    _nr: Arc<NodeRequest>,
    error: &kube::Error,
    _ctx: Arc<ControllerContext>,
) -> Action {
    warn!(%error, "node reconcile failed, requeuing");
    Action::requeue(Duration::from_secs(5))
}
