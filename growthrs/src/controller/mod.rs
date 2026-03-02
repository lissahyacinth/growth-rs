pub mod node;
pub mod node_removal;
pub mod node_requests;
pub mod pods;
mod config;
mod helpers;
pub use config::ScaleDownConfig;
pub(crate) use helpers::update_node_request_phase;
use helpers::wait_for_crds;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::Controller;
use kube::runtime::watcher;
use kube::{Api, Client};
use tokio::time::{Instant, sleep};
use tracing::{debug, info, warn};

use crate::controller::node::node_controller;
use crate::crds::node_removal_request::NodeRemovalRequest;
use crate::crds::node_request::NodeRequest;
use crate::optimiser;
use crate::providers::provider::Provider;

// Re-export for external consumers (integration tests, main.rs).
pub use pods::{
    ClusterState, NodeRequestDemand, PodPoolError, PoolConfig, ReconcileResult,
    gather_pending_claims, reconcile_unschedulable_pods, reconcile_pods,
};

/// Shared context for the controller reconciler.
pub struct ControllerContext {
    pub client: Client,
    pub provider: Provider,
    pub provisioning_timeout: Duration,
    pub scale_down: ScaleDownConfig,
}

/// Error type for reconciliation failures.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Kube(#[from] kube::Error),
    #[error(transparent)]
    Solver(#[from] optimiser::SolveError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("fault injection triggered after {0} NR creates")]
    FaultInjected(usize),
}

/// Watch Pending pods and reconcile in batched windows.
///
/// Events are coalesced so that a burst of pods becoming unschedulable
/// produces a single reconcile against fresh API state, avoiding duplicate
/// NodeRequests from stale informer caches.
async fn run_pod_watcher(ctx: &ControllerContext) -> Result<()> {
    let pods: Api<Pod> = Api::all(ctx.client.clone());
    let config = watcher::Config::default().fields("status.phase=Pending");
    let mut stream = std::pin::pin!(watcher::watcher(pods, config));

    let timeout = Duration::from_millis(500);
    let max_window = Duration::from_secs(10);
    let mut max_delay = ::std::pin::pin!(sleep(Duration::from_millis(0))); // Immediately expire this.
    let mut delay = ::std::pin::pin!(sleep(timeout));
    let mut pending = false;

    let mut pending_claims = pods::gather_pending_claims(&ctx.client).await?;

    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(event)) => {
                        if matches!(event, watcher::Event::Apply(_) | watcher::Event::InitApply(_)) {
                            if !pending {
                                max_delay.as_mut().reset(Instant::now() + max_window);
                            }
                            pending = true;
                            delay.as_mut().reset(Instant::now() + timeout);
                        }
                    },
                    Some(Err(e)) => {
                        warn!(error = %e, "pod watcher stream error")
                    }
                    None => break,
                }
            }
            _ = &mut delay, if pending => {
                pending = false;
                info!(trigger = "batch_timeout", "starting pod reconciliation");
                match pods::reconcile_unschedulable_pods(ctx.client.clone(), &ctx.provider, &mut pending_claims).await {
                    Ok(()) => {}
                    Err(ReconcileError::FaultInjected(n)) => {
                        warn!(n, "fault injection triggered, exiting watcher");
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(error = %e, "pod reconciliation failed");
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
            _ = &mut max_delay, if pending => {
                pending = false;
                info!(trigger = "max_window", "starting pod reconciliation");
                match pods::reconcile_unschedulable_pods(ctx.client.clone(), &ctx.provider, &mut pending_claims).await {
                    Ok(()) => {}
                    Err(ReconcileError::FaultInjected(n)) => {
                        warn!(n, "fault injection triggered, exiting watcher");
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(error = %e, "pod reconciliation failed");
                        sleep(Duration::from_secs(5)).await;
                    }
                }
                delay.as_mut().reset(Instant::now() + timeout);
            }
        }
    }
    Ok(())
}

/// Run only the pod watcher — exposed for integration tests that need
/// to spawn and abort the watcher to simulate crash/restart cycles.
pub async fn run_pod_watcher_standalone(ctx: Arc<ControllerContext>) -> Result<()> {
    run_pod_watcher(&ctx).await
}

/// Run the per-object NodeRequest controller.
async fn run_node_request_controller(ctx: Arc<ControllerContext>) -> Result<()> {
    let nrs: Api<NodeRequest> = Api::all(ctx.client.clone());
    let config = watcher::Config::default();
    let mut stream = std::pin::pin!(Controller::new(nrs, config).run(
        node_requests::reconcile_node_request,
        node_requests::error_policy,
        ctx.clone(),
    ));
    while let Some(result) = stream.next().await {
        let (obj, _) = result.context("node_request controller stream error")?;
        debug!(name = %obj.name, "reconciled NodeRequest")
    }
    Ok(())
}

/// Run the per-object NodeRemovalRequest controller.
async fn run_node_removal_request_controller(ctx: Arc<ControllerContext>) -> Result<()> {
    let nrrs: Api<NodeRemovalRequest> = Api::all(ctx.client.clone());
    let config = watcher::Config::default();
    let mut stream = std::pin::pin!(Controller::new(nrrs, config).run(
        node_removal::reconcile_node_removal_request,
        node_removal::error_policy,
        ctx.clone(),
    ));
    while let Some(result) = stream.next().await {
        let (obj, _) = result.context("node_removal_request controller stream error")?;
        debug!(name = %obj.name, "reconciled NodeRemovalRequest")
    }
    Ok(())
}

/// Run the event-driven controllers.
///
/// Starts watches for Pending Pods, NodeRequests, Nodes, and NodeRemovalRequests concurrently.
/// Also runs the periodic idle-node scanner for scale-down.
pub async fn run(ctx: ControllerContext) -> Result<(), anyhow::Error> {
    wait_for_crds(ctx.client.clone()).await?;
    info!("all CRDs established, starting controller watches");

    let ctx = Arc::new(ctx);

    tokio::select! {
        res = run_pod_watcher(&ctx) => {
            res.context("pod watcher failed")?
        }
        res = run_node_request_controller(ctx.clone()) => {
            res.context("node_request controller failed")?
        }
        res = node_controller(ctx.clone()) => {
            res.context("node controller failed")?
        }
        res = run_node_removal_request_controller(ctx.clone()) => {
            res.context("node_removal_request controller failed")?
        }
        res = node_removal::run_idle_node_scanner(&ctx) => {
            res.context("idle node scanner failed")?
        }
    }
    Ok(())
}
