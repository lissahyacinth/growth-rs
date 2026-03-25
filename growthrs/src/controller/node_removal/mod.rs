pub mod decision;
pub(crate) mod helpers;

pub use helpers::create_node_removal_request;

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::Api;
use kube::api::ListParams;
use kube::runtime::controller::Action;
use kube::runtime::{Controller, watcher};
use tracing::{debug, info, instrument, warn};

use crate::offering::{MANAGED_BY_SELECTOR, NRR_FINALIZER};
use crate::providers::provider::{NodeId, ProviderStatus};
use crate::resources::node_pool::NodePool;
use crate::resources::node_removal_request::{NodeRemovalRequest, NodeRemovalRequestPhase};

use super::{ControllerContext, ControllerError, is_kube_not_found};

use crate::offering::NodeReference;
use decision::{PoolMinCounts, find_idle_nodes, is_node_idle};
use helpers::{
    annotate_delete_at, annotate_removal_candidate, apply_scale_down_taint, delete_kubernetes_node,
    delete_nrr, remove_delete_at_annotation, remove_nrr_finalizer,
    remove_removal_candidate_annotation, update_nrr_phase,
};

const IDLE_SCAN_INTERVAL: Duration = Duration::from_secs(30);
const PROVIDER_DELETE_RETRY: Duration = Duration::from_secs(30);
const DEPROVISIONING_REQUEUE: Duration = Duration::from_secs(15);
const NRR_ERROR_REQUEUE: Duration = Duration::from_secs(10);

/// Phase transition returned by `decide_phase`.
type StateChange = Option<(NodeRemovalRequestPhase, Option<u32>)>;

/// Cluster state snapshot needed for a single idle-node scan cycle.
struct IdleScanState {
    nodes: Vec<Node>,
    pods: Vec<Pod>,
    nrrs: Vec<NodeRemovalRequest>,
    pool_mins: Vec<PoolMinCounts>,
}

impl IdleScanState {
    // TODO: Verify each resource type is listed exactly once per scan cycle.
    //       If additional callers appear, consider caching or sharing the result
    //       to avoid redundant API round-trips.
    async fn collect(client: &kube::Client) -> Result<Self, kube::Error> {
        let nodes_api: Api<Node> = Api::all(client.clone());
        let pods_api: Api<Pod> = Api::all(client.clone());
        let nrr_api: Api<NodeRemovalRequest> = Api::all(client.clone());
        let np_api: Api<NodePool> = Api::all(client.clone());

        let (nodes, pods, nrrs, node_pools) = tokio::try_join!(
            async {
                nodes_api
                    .list(&ListParams::default().labels(MANAGED_BY_SELECTOR))
                    .await
                    .map(|l| l.into_iter().collect::<Vec<_>>())
            },
            async {
                pods_api
                    .list(&ListParams::default())
                    .await
                    .map(|l| l.into_iter().collect::<Vec<_>>())
            },
            async {
                nrr_api
                    .list(&ListParams::default())
                    .await
                    .map(|l| l.into_iter().collect::<Vec<_>>())
            },
            async {
                np_api
                    .list(&ListParams::default())
                    .await
                    .map(|l| l.into_iter().collect::<Vec<_>>())
            },
        )?;

        Ok(Self {
            nodes,
            pods,
            nrrs,
            pool_mins: PoolMinCounts::from_node_pools(&node_pools),
        })
    }
}

/// Run the per-object NodeRemovalRequest controller.
pub(crate) async fn run_node_removal_request_controller(
    ctx: Arc<ControllerContext>,
) -> Result<(), ControllerError> {
    let nrrs: Api<NodeRemovalRequest> = Api::all(ctx.client.clone());
    let config = watcher::Config::default();
    let mut stream = std::pin::pin!(Controller::new(nrrs, config).run(
        reconcile_node_removal_request,
        error_policy,
        ctx.clone(),
    ));
    while let Some(result) = stream.next().await {
        let (obj, _) = result.map_err(ControllerError::from_controller_error)?;
        debug!(name = %obj.name, "reconciled NodeRemovalRequest")
    }
    Ok(())
}

#[instrument(skip_all, fields(nrr = nrr.metadata.name.as_deref().unwrap_or("<unknown>")))]
#[cfg(not(feature = "testing"))]
pub(super) async fn reconcile_node_removal_request(
    nrr: Arc<NodeRemovalRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ControllerError> {
    reconcile_node_removal_request_inner(nrr, ctx).await
}

#[instrument(skip_all, fields(nrr = nrr.metadata.name.as_deref().unwrap_or("<unknown>")))]
#[cfg(feature = "testing")]
pub async fn reconcile_node_removal_request(
    nrr: Arc<NodeRemovalRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ControllerError> {
    reconcile_node_removal_request_inner(nrr, ctx).await
}

async fn reconcile_node_removal_request_inner(
    nrr: Arc<NodeRemovalRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ControllerError> {
    let name = nrr.metadata.name.as_deref().unwrap_or("<unknown>");

    // Finalizer gate: if the NRR is being deleted (by GC or explicitly) and our
    // finalizer is still present, ensure provider cleanup runs before allowing
    // the deletion to complete.
    let has_finalizer = nrr
        .metadata
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|s| s == NRR_FINALIZER));

    if nrr.metadata.deletion_timestamp.is_some() && has_finalizer {
        let node_name = &nrr.spec.node_name;
        let node_id = NodeId(node_name.to_string());
        info!(name, node = %node_name, "NRR being deleted, running finalizer cleanup");

        // Best-effort provider delete — the VM may already be gone.
        if let Err(e) = ctx.provider.delete(&node_id).await {
            warn!(name, %e, "provider delete during finalizer cleanup failed, will retry");
            return Ok(Action::requeue(PROVIDER_DELETE_RETRY));
        }

        // Best-effort K8s node delete — may already be gone (which triggered GC).
        if let Err(e) = delete_kubernetes_node(&ctx.client, node_name).await {
            warn!(name, %e, "k8s node delete during finalizer cleanup failed, will retry");
            return Ok(Action::requeue(NRR_ERROR_REQUEUE));
        }

        remove_nrr_finalizer(&ctx.client, name).await?;
        return Ok(Action::await_change());
    }

    let now = ctx.clock.now();
    let (state_change, action) = decide_phase(&nrr, &ctx, now).await?;

    if let Some((phase, removal_attempts)) = state_change {
        info!(name, from = %nrr.phase(), to = %phase, "transitioning NodeRemovalRequest");
        update_nrr_phase(&ctx.client, name, phase, removal_attempts, now).await?;
    }

    Ok(action)
}

/// Error policy for the NRR controller.
pub(super) fn error_policy(
    nrr: Arc<NodeRemovalRequest>,
    error: &ControllerError,
    _ctx: Arc<ControllerContext>,
) -> Action {
    let name = nrr.metadata.name.as_deref().unwrap_or("<unknown>");
    warn!(name, %error, "NodeRemovalRequest reconcile failed, requeuing");
    Action::requeue(NRR_ERROR_REQUEUE)
}

/// Periodic batch scan that detects newly-idle nodes and creates NRRs,
/// or cancels existing NRRs if pods have appeared on previously-idle nodes.
pub(super) async fn run_idle_node_scanner(
    ctx: Arc<ControllerContext>,
) -> Result<(), ControllerError> {
    loop {
        tokio::time::sleep(IDLE_SCAN_INTERVAL).await;
        if let Err(e) = scan_idle_nodes(ctx.clone()).await {
            warn!(error = %e, "idle node scan failed, will retry next interval");
        }
    }
}

/// Run a single idle-node scan cycle.
#[cfg(not(feature = "testing"))]
async fn scan_idle_nodes(ctx: Arc<ControllerContext>) -> Result<(), ControllerError> {
    scan_idle_nodes_inner(ctx).await
}

/// Run a single idle-node scan cycle (public for integration testing with fault injection).
#[cfg(feature = "testing")]
pub async fn scan_idle_nodes(ctx: Arc<ControllerContext>) -> Result<(), ControllerError> {
    scan_idle_nodes_inner(ctx).await
}

async fn scan_idle_nodes_inner(ctx: Arc<ControllerContext>) -> Result<(), ControllerError> {
    let state = IdleScanState::collect(&ctx.client).await?;

    // Find newly idle nodes and create NodeRemovalRequest.
    let idle_nodes: Vec<NodeReference> =
        find_idle_nodes(&state.nodes, &state.pods, &state.nrrs, &state.pool_mins);

    let now = ctx.clock.now();

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
            now,
        )
        .await
        {
            Ok(created) => {
                let nrr_name = created.metadata.name.as_deref().unwrap_or(&nrr_name);
                // Annotate the node as a removal candidate and set delete-at.
                // Annotations are informational — the reconciler uses creation_timestamp,
                // not annotations, for timing. Failures are logged and do not abort the batch.
                if let Err(e) =
                    annotate_removal_candidate(&ctx.client, &idle.node_name, nrr_name).await
                {
                    warn!(node = %idle.node_name, error = %e, "failed to annotate removal candidate, continuing");
                }
                let delete_at = {
                    let cooling_off_secs = ctx.scale_down.cooling_off_duration.as_secs() as i64;
                    now.checked_add(k8s_openapi::jiff::SignedDuration::from_secs(
                        cooling_off_secs,
                    ))
                    .unwrap_or(now)
                    .to_string()
                };
                if let Err(e) = annotate_delete_at(&ctx.client, &idle.node_name, &delete_at).await {
                    warn!(node = %idle.node_name, error = %e, "failed to annotate delete-at, continuing");
                }
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
    Ok(())
}

async fn decide_phase(
    nrr: &NodeRemovalRequest,
    ctx: &ControllerContext,
    now: k8s_openapi::jiff::Timestamp,
) -> Result<(StateChange, Action), ControllerError> {
    let name = nrr
        .metadata
        .name
        .as_deref()
        .ok_or(ControllerError::MissingName("NodeRemovalRequest"))?;
    let node_name = &nrr.spec.node_name;

    match nrr.phase() {
        NodeRemovalRequestPhase::Pending => {
            // Check if the cooling-off period has elapsed.
            let created = nrr.metadata.creation_timestamp.as_ref();
            let elapsed = created
                .map(|ts| now.duration_since(ts.0))
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

            // Cooling-off elapsed: apply taint, call provider.delete(),
            // and transition directly to Deprovisioning.
            // (delete-at annotation was already set at NRR creation time by the scanner.)
            info!(name, node = %node_name, "cooling-off elapsed, applying taint and starting deprovisioning");
            if let Err(e) = apply_scale_down_taint(&ctx.client, node_name).await {
                if is_kube_not_found(&e) {
                    info!(name, "node already deleted, ensuring provider cleanup");
                    // Node gone from K8s — ensure the provider VM is cleaned up too.
                    let node_id = NodeId(node_name.to_string());
                    if let Err(e) = ctx.provider.delete(&node_id).await {
                        warn!(name, %e, "provider delete failed during node-404 cleanup, will retry");
                        return Ok((None, Action::requeue(PROVIDER_DELETE_RETRY)));
                    }
                    remove_nrr_finalizer(&ctx.client, name).await?;
                    delete_nrr(&ctx.client, name).await?;
                    return Ok((None, Action::await_change()));
                }
                return Err(e.into());
            }

            let node_id = NodeId(node_name.to_string());
            match ctx.provider.delete(&node_id).await {
                Ok(()) => Ok((
                    Some((NodeRemovalRequestPhase::Deprovisioning, Some(1))),
                    Action::await_change(),
                )),
                Err(e) => {
                    warn!(name, %e, "provider delete failed, will retry");
                    Ok((None, Action::requeue(PROVIDER_DELETE_RETRY)))
                }
            }
        }

        NodeRemovalRequestPhase::Deprovisioning => {
            let node_id = NodeId(node_name.to_string());
            match ctx.provider.status(&node_id).await {
                Ok(ProviderStatus::NotFound) => {
                    info!(name, node = %node_name, "provider confirms node gone, cleaning up");
                    // Delete K8s Node first, then remove the finalizer so the NRR
                    // can be deleted. If any step fails, the NRR persists and the
                    // reconciler retries (delete_kubernetes_node handles 404 → Ok).
                    delete_kubernetes_node(&ctx.client, node_name).await?;
                    remove_nrr_finalizer(&ctx.client, name).await?;
                    delete_nrr(&ctx.client, name).await?;
                    Ok((None, Action::await_change()))
                }
                Ok(_) => {
                    // Node still exists at provider — retry delete.
                    let attempts = nrr.status.as_ref().map(|s| s.removal_attempts).unwrap_or(0);

                    if attempts >= ctx.scale_down.max_removal_attempts {
                        warn!(name, attempts, "max removal attempts exceeded");
                        return Ok((
                            Some((NodeRemovalRequestPhase::CouldNotRemove, Some(attempts))),
                            Action::await_change(),
                        ));
                    }
                    if let Err(e) = ctx.provider.delete(&node_id).await {
                        warn!(name, %e, "provider delete retry failed");
                    }
                    Ok((
                        Some((NodeRemovalRequestPhase::Deprovisioning, Some(attempts + 1))),
                        Action::requeue(DEPROVISIONING_REQUEUE),
                    ))
                }
                Err(e) => {
                    warn!(name, %e, "provider status check failed during deprovisioning");
                    Ok((None, Action::requeue(PROVIDER_DELETE_RETRY)))
                }
            }
        }

        NodeRemovalRequestPhase::CouldNotRemove => {
            // Terminal state — do nothing, await manual intervention or change.
            Ok((None, Action::await_change()))
        }
    }
}

/// Check if a node is still idle by listing pods on it.
async fn check_node_still_idle(
    client: &kube::Client,
    node_name: &str,
) -> Result<bool, ControllerError> {
    let pods_api: Api<Pod> = Api::all(client.clone());
    let lp = ListParams::default().fields(&format!("spec.nodeName={node_name}"));
    let pods: Vec<Pod> = pods_api.list(&lp).await?.into_iter().collect();
    Ok(is_node_idle(node_name, &pods))
}

/// Cancel a Node Removal Request.
///
/// Removes annotations on the node, clears the finalizer, and deletes the NRR.
/// The finalizer must be removed before deletion so that the delete is immediate
/// and does not trigger the finalizer cleanup path (which would call
/// `provider.delete()` on a node we want to keep).
///
/// The NRR delete is last because it is the "commit" — if it succeeds the
/// cancellation is done; if an earlier step fails we bail via `?` and the NRR
/// still exists, so the next reconcile retries the whole sequence.
async fn cancel_nrr(
    client: &kube::Client,
    nrr_name: &str,
    node_name: &str,
) -> Result<(), ControllerError> {
    remove_removal_candidate_annotation(client, node_name).await?;
    remove_delete_at_annotation(client, node_name).await?;
    remove_nrr_finalizer(client, nrr_name).await?;
    delete_nrr(client, nrr_name).await?;
    Ok(())
}
