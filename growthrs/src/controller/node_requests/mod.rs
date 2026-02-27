mod helpers;

use std::sync::Arc;
use std::time::Duration;

use kube::runtime::controller::Action;
use tracing::{info, instrument, warn};

use crate::node_request::{NodeRequest, NodeRequestPhase};
use crate::providers::provider::{NodeId, ProviderStatus};

use super::{ControllerContext, ReconcileError, update_node_request_phase};

use helpers::{attempt_provision, delete_node_request};

const PROVISIONING_REQUEUE: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub enum ProvisionOutcome {
    Created,
    AlreadyCreated,
    NoMatchingOffering,
    OfferingUnavailable(String),
}

#[instrument(skip_all, fields(nr = nr.metadata.name.as_deref().unwrap_or("<unknown>")))]
pub(super) async fn reconcile_node_request(
    nr: Arc<NodeRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ReconcileError> {
    let name = nr.metadata.name.as_deref().unwrap_or("<unknown>");
    let (next_phase, action) = decide_phase(&nr, &ctx).await?;

    if let Some(phase) = next_phase {
        info!(name, from = %nr.phase(), to = %phase, "transitioning NodeRequest");
        update_node_request_phase(&ctx.client, name, phase).await?;
    }

    Ok(action)
}

async fn decide_phase(
    nr: &NodeRequest,
    ctx: &ControllerContext,
) -> Result<(Option<NodeRequestPhase>, Action), ReconcileError> {
    let name = nr.metadata.name.as_deref().unwrap_or("<unknown>");

    match nr.phase() {
        NodeRequestPhase::Pending => match attempt_provision(nr, ctx).await? {
            ProvisionOutcome::Created | ProvisionOutcome::AlreadyCreated => {
                Ok((Some(NodeRequestPhase::Provisioning), Action::requeue(PROVISIONING_REQUEUE)))
            }
            ProvisionOutcome::NoMatchingOffering
            | ProvisionOutcome::OfferingUnavailable(_) => {
                Ok((Some(NodeRequestPhase::Unmet), Action::await_change()))
            }
        },
        NodeRequestPhase::Provisioning => {
            match ctx.provider.status(&NodeId(nr.spec.node_id.clone())).await {
                Ok(ProviderStatus::Failed { .. }) | Ok(ProviderStatus::NotFound) => {
                    Ok((Some(NodeRequestPhase::Unmet), Action::await_change()))
                }
                Ok(ProviderStatus::Creating) | Ok(ProviderStatus::Running) => {
                    // TODO: timeout → Deprovisioning
                    Ok((None, Action::requeue(PROVISIONING_REQUEUE)))
                }
                Err(e) => {
                    warn!(name, %e, "provider status check failed");
                    Ok((None, Action::requeue(PROVISIONING_REQUEUE)))
                }
            }
        }
        NodeRequestPhase::Ready => Ok((None, Action::await_change())),
        NodeRequestPhase::Unmet => {
            // TODO: delete this CRD if TTL is met to allow other Nodes to spin up.
            Ok((None, Action::await_change()))
        }
        NodeRequestPhase::Deprovisioning => {
            match ctx.provider.status(&NodeId(nr.spec.node_id.clone())).await {
                Ok(ProviderStatus::NotFound) => {
                    info!(name, "provider confirms node gone, deleting CRD");
                    delete_node_request(&ctx.client, name).await?;
                    Ok((None, Action::await_change()))
                }
                _ => {
                    // TODO: stall detection / alerting
                    Ok((None, Action::requeue(Duration::from_secs(30))))
                }
            }
        }
    }
}

pub(super) fn error_policy(
    nr: Arc<NodeRequest>,
    error: &ReconcileError,
    _ctx: Arc<ControllerContext>,
) -> Action {
    let name = nr.metadata.name.as_deref().unwrap_or("<unknown>");
    warn!(name, %error, "NodeRequest reconcile failed, requeuing");
    Action::requeue(Duration::from_secs(5))
}
