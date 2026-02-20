use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use k8s_openapi::api::core::v1::{Container, Pod, PodSpec, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{DeleteParams, ListParams, ObjectMeta, PostParams};
use kube::runtime::Controller;
use kube::runtime::controller::Action;
use kube::runtime::watcher;
use kube::{Api, Client};
use tracing::{info, warn};

use crate::node_request::{NodeRequestSpec, create_node_request};
use crate::offering::{Offering, PodResources};
use crate::optimiser::{self, SolveError, SolveOptions, solve};
use crate::providers::provider::Provider;

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

#[derive(Debug)]
pub struct NodeRequestDemand {
    pub pool: String,
    pub target_offering: Offering,
}

impl NodeRequestDemand {
    pub fn new(pool: impl Into<String>, target_offering: Offering) -> Self {
        Self {
            pool: pool.into(),
            target_offering,
        }
    }
}

pub struct ClusterState {
    pub demands: Vec<PodResources>,
    pub offerings: Vec<Offering>,
}

/// Check whether a Pod has the `PodScheduled=False/Unschedulable` condition.
pub fn is_pod_unschedulable(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|conditions| {
            conditions.iter().any(|c| {
                c.type_ == "PodScheduled"
                    && c.status == "False"
                    && c.reason.as_deref() == Some("Unschedulable")
            })
        })
        .unwrap_or(false)
}

fn is_daemonset_pod(pod: &Pod) -> bool {
    pod.metadata
        .owner_references
        .as_ref()
        .map(|refs| refs.iter().any(|r| r.kind == "DaemonSet"))
        .unwrap_or(false)
}

async fn get_unschedulable_pods(client: Client) -> Result<Vec<Pod>> {
    let pods: Api<Pod> = Api::all(client.clone());
    let lp = ListParams::default().fields("status.phase=Pending");
    Ok(pods
        .list(&lp)
        .await?
        .into_iter()
        // DaemonSet pods target every node, including nodes that cannot
        // run them — we don't need to scale anything to satisfy them.
        .filter(|pod| is_pod_unschedulable(pod) && !is_daemonset_pod(pod))
        .collect())
}

async fn gather_cluster_state(client: &Client, provider: &Provider) -> Result<ClusterState> {
    let unschedulable_pods = get_unschedulable_pods(client.clone()).await?;
    let offerings = provider.offerings().await;
    // Parse resource demands from all pods
    let demands: Vec<_> = unschedulable_pods
        .iter()
        .map(PodResources::from_pod)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(ClusterState { demands, offerings })
}

impl ClusterState {
    pub fn suitable_offerings(&self) -> Vec<Offering> {
        self.offerings
            .iter()
            .filter(|offering| {
                self.demands
                    .iter()
                    .any(|demand| offering.satisfies(&demand.resources))
            })
            .cloned()
            .collect()
    }
}

pub fn reconcile_pods(state: ClusterState) -> Result<Vec<NodeRequestDemand>, SolveError> {
    let mut demands: Vec<NodeRequestDemand> = vec![];
    let options = SolveOptions::default();
    match solve(&state.demands, &state.suitable_offerings(), &options)? {
        crate::optimiser::PlacementSolution::AllPlaced(nodes) => {
            for node in nodes {
                // TODO: Unfake pools.
                demands.push(NodeRequestDemand::new(
                    "PoolsAreFake",
                    node.offering.clone(),
                ));
            }
        }
        crate::optimiser::PlacementSolution::NoDemands => {}
        crate::optimiser::PlacementSolution::IncompletePlacement { nodes, unmet } => {
            warn!(
                unmet_count = unmet.len(),
                "incomplete placement — some pods could not be scheduled"
            );
            for node in nodes {
                // TODO: Unfake pools.
                demands.push(NodeRequestDemand::new(
                    "PoolsAreFake",
                    node.offering.clone(),
                ));
            }
        }
    }
    Ok(demands)
}

/// Reconcile a single Pod event.
async fn reconcile_pod(
    pod: Arc<Pod>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ReconcileError> {
    let pod_name = pod.metadata.name.as_deref().unwrap_or("<unknown>");

    if !is_pod_unschedulable(&pod) {
        return Ok(Action::await_change());
    }

    info!(
        pod = pod_name,
        "pod is unschedulable, running reconciliation"
    );
    for node_request_action in
        reconcile_pods(gather_cluster_state(&ctx.client, &ctx.provider).await?)?
    {
        create_node_request(ctx.client.clone(), node_request_action.pool.as_str(), NodeRequestSpec {
            target_offering: node_request_action.target_offering.instance_type.to_string(),
        }).await?;
    }

    // Requeue after 30s — a safety net for when provider provisioning
    // is still in progress and we need to re-check.
    Ok(Action::requeue(Duration::from_secs(30)))
}

/// Back off on reconciliation errors.
fn error_policy(_pod: Arc<Pod>, error: &ReconcileError, _ctx: Arc<ControllerContext>) -> Action {
    warn!(%error, "reconcile failed, requeuing");
    Action::requeue(Duration::from_secs(5))
}

/// One-shot reconcile: gather state, solve, and create any needed NodeRequests.
pub async fn controller_loop_single(
    client: Client,
    provider: &Provider,
) -> Result<(), ReconcileError> {
    let state = gather_cluster_state(&client, provider).await?;
    for demand in reconcile_pods(state)? {
        create_node_request(client.clone(), &demand.pool, NodeRequestSpec {
            target_offering: demand.target_offering.instance_type.to_string(),
        })
        .await?;
    }
    Ok(())
}

/// Run the event-driven controller.
///
/// Watches Pending Pods and triggers reconciliation on changes.
pub async fn run(ctx: ControllerContext) {
    let pods: Api<Pod> = Api::all(ctx.client.clone());
    let pod_config = watcher::Config::default().fields("status.phase=Pending");

    let ctx = Arc::new(ctx);

    // TODO: Add .watches(nodes, ...) once NodeRequests exist, so that
    // Node readiness events can advance the NodeRequest state machine.
    Controller::new(pods, pod_config)
        .run(reconcile_pod, error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => info!("reconciled {obj:?}"),
                Err(e) => warn!("controller error: {e:?}"),
            }
        })
        .await;
}

/// Create a test pod with the given resource requests - we should move this to a test module.
/// The pod will sit Pending/Unschedulable until a node can satisfy it.
pub async fn create_test_pod(
    client: Client,
    name: &str,
    cpu: &str,
    memory: &str,
    gpu: Option<u32>,
) -> Result<()> {
    let pods: Api<Pod> = Api::default_namespaced(client);

    let mut requests = BTreeMap::from([
        ("cpu".into(), Quantity(cpu.into())),
        ("memory".into(), Quantity(memory.into())),
    ]);
    if let Some(n) = gpu {
        requests.insert("nvidia.com/gpu".into(), Quantity(n.to_string()));
    }

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/managed-by".into(),
                "growth-test".into(),
            )])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "worker".into(),
                image: Some("busybox".into()),
                command: Some(vec!["sleep".into(), "infinity".into()]),
                resources: Some(ResourceRequirements {
                    requests: Some(requests),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &pod).await?;
    info!(pod = name, cpu, memory, gpu = ?gpu, "created test pod");
    Ok(())
}

/// Delete a test pod by name.
pub async fn delete_test_pod(client: Client, name: &str) -> Result<()> {
    let pods: Api<Pod> = Api::default_namespaced(client);
    pods.delete(name, &DeleteParams::default()).await?;
    info!(pod = name, "deleted test pod");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    use http::{Request, Response};
    use k8s_openapi::api::core::v1::{PodCondition, PodStatus};
    use kube::client::Body;

    use crate::offering::{InstanceType, Offering, Resources};
    use crate::providers::fake::FakeProvider;
    use crate::providers::provider::Provider;

    type ApiServerHandle = tower_test::mock::Handle<Request<Body>, Response<Body>>;

    /// Create a mock kube::Client backed by a tower-test mock service.
    fn mock_client() -> (Client, ApiServerHandle) {
        let (mock_svc, handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(mock_svc, "default");
        (client, handle)
    }

    /// Build a JSON response body for a Kubernetes list API call.
    fn pod_list_response(pods: Vec<Pod>) -> Response<Body> {
        let list = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PodList",
            "metadata": { "resourceVersion": "1" },
            "items": pods,
        });
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&list).unwrap()))
            .unwrap()
    }

    /// Build a mock response for a NodeRequest create (POST) call.
    fn node_request_create_response() -> Response<Body> {
        let nr = serde_json::json!({
            "apiVersion": "growth/v1alpha1",
            "kind": "NodeRequest",
            "metadata": {
                "name": "test-nr",
                "namespace": "default",
                "resourceVersion": "1",
                "uid": "00000000-0000-0000-0000-000000000000"
            },
            "spec": { "target_offering": "test" }
        });
        Response::builder()
            .status(201)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&nr).unwrap()))
            .unwrap()
    }

    /// Build a Pod that looks pending + unschedulable to `get_unschedulable_pods`.
    fn make_pending_unschedulable_pod(name: &str, cpu: &str, memory: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("default".into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "worker".into(),
                    image: Some("busybox".into()),
                    resources: Some(ResourceRequirements {
                        requests: Some(BTreeMap::from([
                            ("cpu".into(), Quantity(cpu.into())),
                            ("memory".into(), Quantity(memory.into())),
                        ])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Pending".into()),
                conditions: Some(vec![PodCondition {
                    type_: "PodScheduled".into(),
                    status: "False".into(),
                    reason: Some("Unschedulable".into()),
                    message: Some("insufficient resources".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn test_offering(name: &str, cpu: u32, memory_mib: u32, cost: f64) -> Offering {
        Offering {
            instance_type: InstanceType(name.into()),
            resources: Resources {
                cpu,
                memory_mib,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: cost,
        }
    }

    /// Spawn a mock API server that handles pod-list and NodeRequest create requests.
    ///
    /// Returns a counter tracking how many NodeRequests were created.
    fn spawn_mock_api(mut handle: ApiServerHandle, pods: Vec<Pod>) -> Arc<AtomicUsize> {
        let nr_count = Arc::new(AtomicUsize::new(0));
        let nr_count_inner = nr_count.clone();
        tokio::spawn(async move {
            while let Some((request, send)) = handle.next_request().await {
                let path = request.uri().path().to_string();
                if path.contains("/pods") {
                    send.send_response(pod_list_response(pods.clone()));
                } else if path.contains("noderequests") {
                    nr_count_inner.fetch_add(1, Ordering::SeqCst);
                    send.send_response(node_request_create_response());
                } else {
                    panic!("unexpected request: {path}");
                }
            }
        });
        nr_count
    }

    // ── Test scenarios ───────────────────────────────────────────────

    #[tokio::test]
    async fn no_pending_pods_does_nothing() {
        let (client, handle) = mock_client();
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cx22", 2, 4096, 0.01)]),
        );

        let nr_count = spawn_mock_api(handle, vec![]);

        let result = controller_loop_single(client, &provider).await;
        assert!(result.is_ok());
        assert_eq!(nr_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn one_pending_pod_creates_one_node_request() {
        let (client, handle) = mock_client();
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cx22", 2, 4096, 0.01)]),
        );

        let pod = make_pending_unschedulable_pod("test-pod", "1", "2048Mi");
        let nr_count = spawn_mock_api(handle, vec![pod]);

        let result = controller_loop_single(client, &provider).await;
        assert!(result.is_ok());
        assert_eq!(nr_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn multiple_pods_bin_packed_into_multiple_node_requests() {
        let (client, handle) = mock_client();
        // Offering fits 2 pods (2 cpu, 4Gi) but each pod needs 1 cpu + 2Gi.
        // 3 pods → need 2 nodes.
        let offering = test_offering("cx22", 2, 4096, 0.01);
        let provider = Provider::Fake(FakeProvider::new().with_offerings(vec![offering]));

        let pods = vec![
            make_pending_unschedulable_pod("pod-a", "1", "2048Mi"),
            make_pending_unschedulable_pod("pod-b", "1", "2048Mi"),
            make_pending_unschedulable_pod("pod-c", "1", "2048Mi"),
        ];
        let nr_count = spawn_mock_api(handle, pods);

        let result = controller_loop_single(client, &provider).await;
        assert!(result.is_ok());
        assert_eq!(
            nr_count.load(Ordering::SeqCst),
            2,
            "expected 2 NodeRequests for 3 small pods"
        );
    }

    #[tokio::test]
    async fn offerings_sequence_changes_between_calls() {
        // First call: only a small offering. Second call: larger offering available.
        let small = test_offering("small", 1, 1024, 0.005);
        let large = test_offering("large", 4, 8192, 0.02);

        let provider = Provider::Fake(FakeProvider::new().with_offerings_sequence(vec![
            vec![small.clone()],
            vec![small.clone(), large.clone()],
        ]));

        // First call — only small offerings available.
        let (client1, handle1) = mock_client();
        let pod = make_pending_unschedulable_pod("pod-1", "1", "512Mi");
        let nr_count1 = spawn_mock_api(handle1, vec![pod]);
        controller_loop_single(client1, &provider).await.unwrap();
        assert_eq!(nr_count1.load(Ordering::SeqCst), 1);

        // Second call — large offering now available too. Pod needs 3 cpu.
        let (client2, handle2) = mock_client();
        let pod = make_pending_unschedulable_pod("pod-2", "3", "4096Mi");
        let nr_count2 = spawn_mock_api(handle2, vec![pod]);
        controller_loop_single(client2, &provider).await.unwrap();
        assert_eq!(nr_count2.load(Ordering::SeqCst), 1);
    }
}
