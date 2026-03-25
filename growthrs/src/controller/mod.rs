pub mod errors;
pub mod healthcheck;
pub(crate) mod helpers;
#[cfg(not(feature = "testing"))]
pub(crate) mod node;
#[cfg(feature = "testing")]
pub mod node;
#[cfg(not(feature = "testing"))]
pub(crate) mod node_removal;
#[cfg(feature = "testing")]
pub mod node_removal;
pub(crate) mod node_requests;
pub mod pods;
pub use errors::ControllerError;
use helpers::wait_for_crds;
pub(crate) use helpers::{is_kube_not_found, update_node_request_phase};
pub use pods::PodPoolError;

use std::sync::Arc;

use tracing::info;

use crate::config::ControllerContext;
use crate::controller::node::node_controller;
use crate::controller::node_removal::run_node_removal_request_controller;
use crate::controller::node_requests::run_node_request_controller;

// Re-export for external consumers (integration tests, main.rs).
pub use pods::watcher::run_pod_watcher;
pub use pods::{
    ClusterState, NodeRequestDemand, PoolConfig, ReconcileResult, init_unconfirmed_creates,
    reconcile_pod_demand, reconcile_unschedulable_pods,
};

/// Run the event-driven controllers + watchers.
///
/// Starts watches for Pending Pods, NodeRequests, Ready Nodes, NodeRemovalRequests, and Node Removals concurrently.
/// Also runs the periodic idle-node scanner for scale-down.
pub async fn run(ctx: Arc<ControllerContext>) -> Result<(), ControllerError> {
    wait_for_crds(ctx.client.clone()).await?;
    info!("all CRDs established, validating pools against provider offerings");
    helpers::validate_pool_offerings(&ctx.client, &ctx.provider).await?;

    tokio::select! {
        // Watch for unschedulable pods, creating NodeRequests as appropriate
        res = pods::watcher::run_pod_watcher(ctx.clone()) => {
            res.map_err(|e| e.with_context("pod watcher failed"))?;
            tracing::warn!("Pod Watcher exited unexpectedly");
        }
        // Provision nodes from NodeRequests
        res = run_node_request_controller(ctx.clone()) => {
            res.map_err(|e| e.with_context("node_request controller failed"))?;
            tracing::warn!("Node Request Watcher exited unexpectedly");
        }
        // Mark NodeRequests as Ready as Nodes join Cluster
        res = node_controller(ctx.clone()) => {
            res.map_err(|e| e.with_context("node controller failed"))?;
            tracing::warn!("Node Controller Watcher exited unexpectedly");
        }
        // Scale down idle Nodes by creating NodeRemovalRequests
        res = node_removal::run_idle_node_scanner(ctx.clone()) => {
            res.map_err(|e| e.with_context("idle node scanner failed"))?;
            tracing::warn!("Node Removal Request Watcher exited unexpectedly");
        }
        // Drive NodeRemovalRequests through until Node deletion.
        res = run_node_removal_request_controller(ctx.clone()) => {
            res.map_err(|e| e.with_context("node_removal_request controller failed"))?;
            tracing::warn!("Node Removal Watcher exited unexpectedly");
        }
    }
    Ok(())
}
