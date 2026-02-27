pub mod node;
pub mod node_requests;
pub mod pods;
mod helpers;
pub(crate) use helpers::update_node_request_phase;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::runtime::Controller;
use kube::runtime::watcher;
use kube::{Api, Client};
use tokio::time::{Instant, sleep};
use tracing::{debug, info, warn};

use crate::controller::node::node_controller;
use crate::node_request::{NodeRequest};
use crate::optimiser;
use crate::providers::provider::Provider;

// Re-export for external consumers (integration tests, main.rs).
pub use pods::{
    ClusterState, NodeRequestDemand, PodPoolError, PoolConfig, ReconcileResult,
    reconcile_unschedulable_pods, reconcile_pods,
};

const CUSTOM_RESOURCE_DEFINITIONS: [&'static str; 3] = [
    "noderequests.growth.vettrdev.com",
    "nodepools.growth.vettrdev.com",
    "noderemovalrequests.growth.vettrdev.com",
];

/// Shared context for the controller reconciler.
pub struct ControllerContext {
    pub client: Client,
    pub provider: Provider,
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
}

/// Loop until CRDs are installed on Cluster
async fn wait_for_crds(client: Client) -> Result<()> {
    let api: Api<CustomResourceDefinition> = Api::all(client);
    let mut crd_stablised: Vec<bool> = vec![false; CUSTOM_RESOURCE_DEFINITIONS.len()];
    loop {
        for (crd_idx, crd_name) in CUSTOM_RESOURCE_DEFINITIONS.iter().enumerate() {
            if crd_stablised[crd_idx] {
                continue;
            }
            if let Some(crd) = api.get_opt(crd_name).await? {
                let established = crd
                    .status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conditions| {
                        conditions
                            .iter()
                            .any(|c| c.type_ == "Established" && c.status == "True")
                    })
                    .unwrap_or(false);
                if established {
                    crd_stablised[crd_idx] = true;
                    if crd_stablised.iter().all(|f| *f) {
                        return Ok(());
                    }
                }
            }
        }
        let missing_crds: String = CUSTOM_RESOURCE_DEFINITIONS
            .iter()
            .zip(crd_stablised.iter())
            .filter_map(|(crd, stabilised)| {
                if !*stabilised {
                    Some(crd.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        warn!(missing = %missing_crds, "CRDs not yet established, retrying in 5s");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Watch Pending pods and reconcile in batched windows.
///
/// Events are coalesced so that a burst of pods becoming unschedulable
/// produces a single reconcile against fresh API state, avoiding duplicate
/// NodeRequests from stale informer caches.
async fn run_pod_watcher(client: Client, provider: &Provider) -> Result<()> {
    let pods: Api<Pod> = Api::all(client.clone());
    let config = watcher::Config::default().fields("status.phase=Pending");
    let mut stream = std::pin::pin!(watcher::watcher(pods, config));

    let timeout = Duration::from_millis(500);
    let max_window = Duration::from_secs(10);
    let mut max_delay = ::std::pin::pin!(sleep(Duration::from_millis(0))); // Immediately expire this.
    let mut delay = ::std::pin::pin!(sleep(timeout));
    let mut pending = false;

    let mut pending_claims = pods::PendingClaims::new();

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
                if let Err(e) = pods::reconcile_unschedulable_pods(client.clone(), provider, &mut pending_claims).await {
                    warn!(error = %e, "pod reconciliation failed");
                    sleep(Duration::from_secs(5)).await;
                }
            }
            _ = &mut max_delay, if pending => {
                pending = false;
                info!(trigger = "max_window", "starting pod reconciliation");
                if let Err(e) = pods::reconcile_unschedulable_pods(client.clone(), provider, &mut pending_claims).await {
                    warn!(error = %e, "pod reconciliation failed");
                    sleep(Duration::from_secs(5)).await;
                }
                delay.as_mut().reset(Instant::now() + timeout);
            }
        }
    }
    Ok(())
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

/// Run the event-driven controllers.
///
/// Starts watches for Pending Pods and NodeRequests concurrently.
pub async fn run(ctx: ControllerContext) -> Result<(), anyhow::Error> {
    wait_for_crds(ctx.client.clone()).await?;
    info!("all CRDs established, starting controller watches");

    let ctx = Arc::new(ctx);

    tokio::select! {
        res = run_pod_watcher(ctx.client.clone(), &ctx.provider) => {
            res.context("pod watcher failed")?
        }
        res = run_node_request_controller(ctx.clone()) => {
            res.context("node_request controller failed")?
        }
        res = node_controller(ctx.clone()) => {
            res.context("node controller failed")?
        }
    }
    Ok(())
}
