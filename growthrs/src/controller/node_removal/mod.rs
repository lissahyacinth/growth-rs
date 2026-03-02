pub mod decision;
mod helpers;

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::ListParams;
use kube::runtime::controller::Action;
use kube::Api;
use tracing::{debug, info, instrument, warn};

use crate::crds::node_pool::NodePool;
use crate::crds::node_removal_request::{
    NodeRemovalRequest, NodeRemovalRequestPhase, create_node_removal_request,
};
use crate::providers::provider::{NodeId, ProviderStatus};

use super::{ControllerContext, ReconcileError};

use decision::{IdleNode, PoolMinCounts, find_idle_nodes, is_node_idle};
use helpers::{
    annotate_delete_at, annotate_removal_candidate, apply_scale_down_taint,
    delete_kubernetes_node, delete_nrr, remove_delete_at_annotation,
    remove_removal_candidate_annotation, update_nrr_phase,
};

const IDLE_SCAN_INTERVAL: Duration = Duration::from_secs(30);

/// Phase transition returned by `decide_phase`.
type StateChange = Option<(NodeRemovalRequestPhase, Option<u32>)>;

/// Per-object NRR reconciler — driven by the kube Controller runtime.
#[instrument(skip_all, fields(nrr = nrr.metadata.name.as_deref().unwrap_or("<unknown>")))]
pub(super) async fn reconcile_node_removal_request(
    nrr: Arc<NodeRemovalRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ReconcileError> {
    let name = nrr.metadata.name.as_deref().unwrap_or("<unknown>");

    let (state_change, action) = decide_phase(&nrr, &ctx).await?;

    if let Some((phase, removal_attempts)) = state_change {
        info!(name, from = %nrr.phase(), to = %phase, "transitioning NodeRemovalRequest");
        update_nrr_phase(&ctx.client, name, phase, removal_attempts).await?;
    }

    Ok(action)
}

async fn decide_phase(
    nrr: &NodeRemovalRequest,
    ctx: &ControllerContext,
) -> Result<(StateChange, Action), ReconcileError> {
    let name = nrr.metadata.name.as_deref().unwrap_or("<unknown>");
    let node_name = &nrr.spec.node_name;

    match nrr.phase() {
        NodeRemovalRequestPhase::Pending => {
            // Check if the cooling-off period has elapsed.
            let created = nrr.metadata.creation_timestamp.as_ref();
            let elapsed = created
                .map(|ts| {
                    let now = k8s_openapi::jiff::Timestamp::now();
                    now.duration_since(ts.0)
                })
                .unwrap_or(k8s_openapi::jiff::SignedDuration::ZERO);

            let cooling_off = k8s_openapi::jiff::SignedDuration::from_secs(
                ctx.scale_down.cooling_off_duration.as_secs() as i64,
            );

            // At each check-in, ensure node remains idle. If no longer idle, cancel removal request.
            if !check_node_still_idle(&ctx.client, node_name).await? {
                info!(name, node = %node_name, "node no longer idle during cooling-off, cancelling NRR");
                cancel_nrr(&ctx.client, name, node_name).await?;
                return Ok((None, Action::await_change()));
            }

            if elapsed < cooling_off {
                // Still in cooling-off, recheck later.
                let remaining = cooling_off
                    .checked_sub(elapsed)
                    .unwrap_or(k8s_openapi::jiff::SignedDuration::ZERO);
                let requeue_secs = remaining.as_secs().max(5) as u64;
                return Ok((None, Action::requeue(Duration::from_secs(requeue_secs))));
            }

            // Cooling-off elapsed: apply taint, set delete-at, call provider.delete(),
            // and transition directly to Deprovisioning.
            info!(name, node = %node_name, "cooling-off elapsed, applying taint and starting deprovisioning");
            if let Err(e) = apply_scale_down_taint(&ctx.client, node_name).await {
                if is_not_found_error(&e) {
                    info!(name, "node already deleted, cleaning up NRR");
                    delete_nrr(&ctx.client, name).await?;
                    return Ok((None, Action::await_change()));
                }
                return Err(e.into());
            }

            let now = k8s_openapi::jiff::Timestamp::now().to_string();
            let _ = annotate_delete_at(&ctx.client, node_name, &now).await;

            let node_id = NodeId(node_name.to_string());
            match ctx.provider.delete(&node_id).await {
                Ok(()) => Ok((
                    Some((NodeRemovalRequestPhase::Deprovisioning, Some(1))),
                    Action::await_change(),
                )),
                Err(e) => {
                    warn!(name, %e, "provider delete failed, will retry");
                    Ok((None, Action::requeue(Duration::from_secs(30))))
                }
            }
        }

        NodeRemovalRequestPhase::Deprovisioning => {
            let node_id = NodeId(node_name.to_string());
            match ctx.provider.status(&node_id).await {
                Ok(ProviderStatus::NotFound) => {
                    info!(name, node = %node_name, "provider confirms node gone, cleaning up");
                    // Delete the K8s Node object and the NRR.
                    let _ = delete_kubernetes_node(&ctx.client, node_name).await;
                    delete_nrr(&ctx.client, name).await?;
                    Ok((None, Action::await_change()))
                }
                Ok(_) => {
                    // Node still exists at provider — retry delete.
                    let attempts = nrr
                        .status
                        .as_ref()
                        .map(|s| s.removal_attempts)
                        .unwrap_or(0);

                    if attempts >= ctx.scale_down.max_removal_attempts {
                        warn!(name, attempts, "max removal attempts exceeded");
                        return Ok((
                            Some((NodeRemovalRequestPhase::CouldNotRemove, Some(attempts))),
                            Action::await_change(),
                        ));
                    }

                    // Retry delete.
                    if let Err(e) = ctx.provider.delete(&node_id).await {
                        warn!(name, %e, "provider delete retry failed");
                    }
                    Ok((
                        Some((NodeRemovalRequestPhase::Deprovisioning, Some(attempts + 1))),
                        Action::requeue(Duration::from_secs(15)),
                    ))
                }
                Err(e) => {
                    warn!(name, %e, "provider status check failed during deprovisioning");
                    Ok((None, Action::requeue(Duration::from_secs(30))))
                }
            }
        }

        NodeRemovalRequestPhase::CouldNotRemove => {
            // Terminal state — do nothing, await manual intervention or change.
            Ok((None, Action::await_change()))
        }
    }
}

/// Error policy for the NRR controller.
pub(super) fn error_policy(
    nrr: Arc<NodeRemovalRequest>,
    error: &ReconcileError,
    _ctx: Arc<ControllerContext>,
) -> Action {
    let name = nrr.metadata.name.as_deref().unwrap_or("<unknown>");
    warn!(name, %error, "NodeRemovalRequest reconcile failed, requeuing");
    Action::requeue(Duration::from_secs(10))
}

/// Check if a node is still idle by listing pods on it.
async fn check_node_still_idle(client: &kube::Client, node_name: &str) -> Result<bool, ReconcileError> {
    let pods_api: Api<Pod> = Api::all(client.clone());
    let lp = ListParams::default().fields(&format!("spec.nodeName={node_name}"));
    let pods: Vec<Pod> = pods_api.list(&lp).await?.into_iter().collect();
    Ok(is_node_idle(node_name, &pods))
}

/// Cancel an NRR: remove annotations and delete the NRR.
async fn cancel_nrr(
    client: &kube::Client,
    nrr_name: &str,
    node_name: &str,
) -> Result<(), ReconcileError> {
    let _ = remove_removal_candidate_annotation(client, node_name).await;
    let _ = remove_delete_at_annotation(client, node_name).await;
    delete_nrr(client, nrr_name).await?;
    Ok(())
}

fn is_not_found_error(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(resp) if resp.code == 404)
}

/// Periodic batch scan that detects newly-idle nodes and creates NRRs,
/// or cancels existing NRRs if pods have appeared on previously-idle nodes.
pub(super) async fn run_idle_node_scanner(ctx: &ControllerContext) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(IDLE_SCAN_INTERVAL).await;

        if let Err(e) = scan_idle_nodes(ctx).await {
            warn!(error = %e, "idle node scan failed, will retry next interval");
        }
    }
}

async fn scan_idle_nodes(ctx: &ControllerContext) -> anyhow::Result<()> {
    let nodes_api: Api<Node> = Api::all(ctx.client.clone());
    let pods_api: Api<Pod> = Api::all(ctx.client.clone());
    let nrr_api: Api<NodeRemovalRequest> = Api::all(ctx.client.clone());
    let np_api: Api<NodePool> = Api::all(ctx.client.clone());

    // List Growth-managed nodes.
    let nodes: Vec<Node> = nodes_api
        .list(&ListParams::default().labels("app.kubernetes.io/managed-by=growth"))
        .await?
        .into_iter()
        .collect();

    let all_pods: Vec<Pod> = pods_api.list(&ListParams::default()).await?.into_iter().collect();
    let existing_nrrs: Vec<NodeRemovalRequest> =
        nrr_api.list(&ListParams::default()).await?.into_iter().collect();
    let node_pools: Vec<NodePool> = np_api.list(&ListParams::default()).await?.into_iter().collect();

    // Build pool min counts.
    let pool_mins: Vec<PoolMinCounts> = node_pools
        .iter()
        .filter_map(|np| {
            np.metadata.name.as_ref().map(|name| PoolMinCounts {
                pool_name: name.clone(),
                server_types: np.spec.server_types.clone(),
            })
        })
        .collect();

    // Cancel NRRs for nodes that are no longer idle (Pending only).
    for nrr in &existing_nrrs {
        if nrr.phase() != NodeRemovalRequestPhase::Pending {
            continue;
        }

        let node_name = &nrr.spec.node_name;
        let nrr_name = match nrr.metadata.name.as_deref() {
            Some(n) => n,
            None => continue,
        };

        if !is_node_idle(node_name, &all_pods) {
            info!(
                nrr = nrr_name,
                node = %node_name,
                "pods appeared on node, cancelling NRR"
            );
            let _ = remove_removal_candidate_annotation(&ctx.client, node_name).await;
            let _ = remove_delete_at_annotation(&ctx.client, node_name).await;
            let _ = delete_nrr(&ctx.client, nrr_name).await;
        }
    }

    // Find newly idle nodes and create NRRs.
    let idle_nodes: Vec<IdleNode> =
        find_idle_nodes(&nodes, &all_pods, &existing_nrrs, &pool_mins);

    let new_idle_count = idle_nodes.len();

    for idle in idle_nodes {
        let nrr_name = format!("nrr-{}", idle.node_name);
        info!(
            node = %idle.node_name,
            pool = %idle.pool,
            instance_type = %idle.instance_type,
            "detected idle node, creating NRR"
        );

        match create_node_removal_request(
            ctx.client.clone(),
            &idle.node_name,
            Some(&idle.node_uid),
            &idle.pool,
            &idle.instance_type,
            NodeRemovalRequestPhase::Pending,
        )
        .await
        {
            Ok(_) => {
                // Annotate the node as a removal candidate and set delete-at.
                let _ = annotate_removal_candidate(&ctx.client, &idle.node_name, &nrr_name).await;
                let delete_at = {
                    let cooling_off_secs = ctx.scale_down.cooling_off_duration.as_secs() as i64;
                    let now = k8s_openapi::jiff::Timestamp::now();
                    now.checked_add(k8s_openapi::jiff::SignedDuration::from_secs(cooling_off_secs))
                        .unwrap_or(now)
                        .to_string()
                };
                let _ = annotate_delete_at(&ctx.client, &idle.node_name, &delete_at).await;
            }
            Err(kube::Error::Api(ref resp)) if resp.code == 409 => {
                debug!(
                    node = %idle.node_name,
                    "NRR already exists (conflict), skipping"
                );
            }
            Err(e) => {
                warn!(
                    node = %idle.node_name,
                    error = %e,
                    "failed to create NRR"
                );
            }
        }
    }

    let active_count = existing_nrrs
        .iter()
        .filter(|nrr| {
            let p = nrr.phase();
            p != NodeRemovalRequestPhase::CouldNotRemove
        })
        .count();

    if active_count > 0 || new_idle_count > 0 {
        debug!(
            active_nrrs = active_count,
            new_idle = new_idle_count,
            "idle scan complete"
        );
    }

    Ok(())
}
