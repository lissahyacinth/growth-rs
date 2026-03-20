mod decision;
mod helpers;
pub(crate) mod watcher;
pub use decision::*;
pub use helpers::{is_daemonset_pod, is_pod_unschedulable};

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::ListParams;
use kube::{Api, Client};
use tokio::time::Instant;
use tracing::{debug, instrument, warn};

use crate::controller::errors::ControllerError;
use crate::crds::node_pool::NodePool;
use crate::crds::node_request::{
    NodeRequest, NodeRequestPhase, NodeRequestSpec, create_node_request,
};
use crate::offering::{INSTANCE_TYPE_LABEL, MANAGED_BY_SELECTOR, POOL_LABEL};
use crate::providers::provider::Provider;

use helpers::merge_occupied_counts;

/// In-memory cache of recently-created NodeRequest capacity.
///
/// Bridges the gap between NodeRequest creation (POST) and the next API list
/// reflecting it. Unlike the old PendingClaims (which tracked per-pod UIDs),
/// this tracks per-NodeRequest capacity — no pod-level binding.
#[derive(Default)]
pub struct RecentCreates {
    entries: Vec<RecentEntry>,
}

struct RecentEntry {
    /// NodeRequest name (for drain matching against API list).
    nr_name: String,
    capacity: InFlightCapacity,
    created_at: Instant,
}

/// Entries older than this are expired regardless of API state.
const RECENT_CREATES_TTL: Duration = Duration::from_secs(60);

impl RecentCreates {
    /// Record a recently-created NodeRequest's capacity.
    pub fn record(&mut self, nr_name: String, capacity: InFlightCapacity) {
        self.entries.push(RecentEntry {
            nr_name,
            capacity,
            created_at: Instant::now(),
        });
    }

    /// Drain entries that the API now reflects and expire stale entries.
    pub fn drain_reflected(&mut self, api_nr_names: &HashSet<String>) {
        let now = Instant::now();
        let before = self.entries.len();
        self.entries.retain(|e| {
            !api_nr_names.contains(&e.nr_name)
                && now.duration_since(e.created_at) < RECENT_CREATES_TTL
        });
        let drained = before - self.entries.len();
        if drained > 0 {
            debug!(
                drained,
                remaining = self.entries.len(),
                "drained recent_creates"
            );
        }
    }

    /// Get all cached in-flight capacities.
    pub fn capacities(&self) -> Vec<InFlightCapacity> {
        self.entries.iter().map(|e| e.capacity.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

async fn get_unschedulable_pods(client: Client) -> Result<Vec<Pod>, ControllerError> {
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

/// Fetch all NodePool CRDs and map them to PoolConfig.
async fn get_node_pools(client: Client) -> Result<Vec<PoolConfig>, ControllerError> {
    let api: Api<NodePool> = Api::all(client);
    let lp = ListParams::default();
    Ok(api
        .list(&lp)
        .await?
        .into_iter()
        .filter_map(|np| {
            let name = np.metadata.name?;
            let uid = np.metadata.uid?;
            Some(PoolConfig {
                name,
                uid,
                server_types: np.spec.server_types,
                labels: np.spec.labels,
                locations: np.spec.locations,
            })
        })
        .collect())
}

/// Result of scanning NodeRequests for in-flight state.
struct InFlightScan {
    /// Capacity to feed into subtract_in_flight.
    in_flight: Vec<InFlightCapacity>,
    /// Pending/Provisioning NR counts for occupied_counts.
    nr_counts: HashMap<String, HashMap<String, u32>>,
    /// Names of all NodeRequests seen, for draining RecentCreates.
    api_nr_names: HashSet<String>,
}

async fn scan_node_requests(
    client: Client,
    unmet_ttl: Duration,
    now: k8s_openapi::jiff::Timestamp,
) -> Result<InFlightScan, ControllerError> {
    use crate::controller::node_requests::is_unmet_expired;

    let api: Api<NodeRequest> = Api::all(client);
    let lp = ListParams::default();
    let mut in_flight = Vec::new();
    let mut nr_counts: HashMap<String, HashMap<String, u32>> = HashMap::new();
    let mut api_nr_names = HashSet::new();

    for nr in api.list(&lp).await? {
        if let Some(name) = nr.metadata.name.as_ref() {
            api_nr_names.insert(name.clone());
        }

        let phase = nr.phase();

        let pool_name = nr
            .metadata
            .owner_references
            .as_ref()
            .and_then(|refs| {
                refs.iter()
                    .find(|r| r.kind == "NodePool")
                    .map(|r| r.name.clone())
            })
            .unwrap_or_default();

        let capacity = InFlightCapacity {
            pool: pool_name.clone(),
            resources: nr.spec.resources.clone(),
        };

        match phase {
            NodeRequestPhase::Pending | NodeRequestPhase::Provisioning => {
                in_flight.push(capacity);
                *nr_counts
                    .entry(pool_name)
                    .or_default()
                    .entry(nr.spec.target_offering.clone())
                    .or_insert(0) += 1;
            }
            NodeRequestPhase::Unmet => {
                if !is_unmet_expired(&nr, unmet_ttl, now) {
                    in_flight.push(capacity);
                }
            }
            NodeRequestPhase::Ready => {}
        }
    }

    Ok(InFlightScan {
        in_flight,
        nr_counts,
        api_nr_names,
    })
}

/// Count existing Growth-managed nodes per pool per instance type.
///
/// Nodes are identified by the `growth.vettrdev.com/pool` and
/// `growth.vettrdev.com/instance-type` labels set during provisioning.
async fn get_node_counts(
    client: Client,
) -> Result<HashMap<String, HashMap<String, u32>>, ControllerError> {
    let nodes: Api<Node> = Api::all(client);
    let lp = ListParams::default().labels(MANAGED_BY_SELECTOR);
    let mut counts: HashMap<String, HashMap<String, u32>> = HashMap::new();

    for node in nodes.list(&lp).await? {
        let labels = node.metadata.labels.as_ref();
        let Some(pool) = labels.and_then(|l| l.get(POOL_LABEL)) else {
            continue;
        };
        let Some(instance_type) = labels.and_then(|l| l.get(INSTANCE_TYPE_LABEL)) else {
            continue;
        };
        *counts
            .entry(pool.clone())
            .or_default()
            .entry(instance_type.clone())
            .or_insert(0) += 1;
    }

    debug!(?counts, "existing node counts by pool");
    Ok(counts)
}

async fn gather_cluster_state(
    client: &Client,
    provider: &Provider,
    recent_creates: &mut RecentCreates,
    unmet_ttl: Duration,
    now: k8s_openapi::jiff::Timestamp,
) -> Result<ClusterState, ControllerError> {
    let unschedulable_pods = get_unschedulable_pods(client.clone()).await?;
    let offerings = provider.offerings().await;
    let scan = scan_node_requests(client.clone(), unmet_ttl, now).await?;
    let node_counts = get_node_counts(client.clone()).await?;
    let pools = get_node_pools(client.clone()).await?;

    recent_creates.drain_reflected(&scan.api_nr_names);

    let all_demands: Vec<_> = unschedulable_pods
        .iter()
        .map(|p| {
            crate::offering::PodResources::from_pod(p)
                .map_err(|e| ControllerError::ConfigError(e.into()))
        })
        .collect::<std::result::Result<Vec<_>, ControllerError>>()?;

    let mut in_flight = scan.in_flight;
    in_flight.extend(recent_creates.capacities());

    let demands = subtract_in_flight(all_demands, &in_flight);

    let occupied_counts = merge_occupied_counts(scan.nr_counts, node_counts);

    debug!(
        total_unschedulable = unschedulable_pods.len(),
        in_flight_nrs = in_flight.len(),
        recent_creates = recent_creates.len(),
        residual_demands = demands.len(),
        "resource-based dedup complete"
    );

    Ok(ClusterState {
        demands,
        offerings,
        occupied_counts,
        pools,
    })
}

/// Build an empty RecentCreates cache.
///
/// On startup, the API list in the first reconcile loop will provide the
/// full in-flight state. RecentCreates only bridges the gap between NR
/// creation and the *next* API list, so it starts empty.
pub fn init_recent_creates() -> RecentCreates {
    RecentCreates::default()
}

/// One-shot reconcile: gather state, solve, and create any needed NodeRequests.
#[instrument(skip_all, fields(reconcile_id = %uuid::Uuid::new_v4()))]
#[allow(unused_variables, unused_assignments)]
pub async fn reconcile_unschedulable_pods(
    client: Client,
    provider: &Provider,
    recent_creates: &mut RecentCreates,
    unmet_ttl: Duration,
    now: k8s_openapi::jiff::Timestamp,
) -> Result<(), ControllerError> {
    let state = gather_cluster_state(&client, provider, recent_creates, unmet_ttl, now)
        .await
        .map_err(|e| ControllerError::Other(e.into()))?;
    let result = reconcile_pods(state)?;

    for err in &result.pod_errors {
        warn!(pod = %err.pod_id, reason = %err.reason, "pod could not be assigned to a pool");
    }

    for (nr_creates, demand) in result.demands.into_iter().enumerate() {
        fail::fail_point!("reconcile_after_nr_create", |_| {
            Err(ControllerError::FaultInjected(nr_creates))
        });

        let created = create_node_request(
            client.clone(),
            &demand.pool,
            &demand.pool_uid,
            NodeRequestSpec {
                target_offering: demand.target_offering.instance_type.to_string(),
                location: demand.target_offering.location.region.0.clone(),
                resources: demand.target_offering.resources.clone(),
                node_id: format!("growth-{}", uuid::Uuid::new_v4()),
            },
        )
        .await?;
        let nr_name = created.metadata.name.unwrap_or_default();
        recent_creates.record(
            nr_name,
            InFlightCapacity {
                pool: demand.pool,
                resources: demand.target_offering.resources,
            },
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use http::{Request, Response};

    use k8s_openapi::api::core::v1::{
        Container, Pod, PodCondition, PodSpec, PodStatus, ResourceRequirements,
    };
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use kube::Client;
    use kube::api::ObjectMeta;
    use kube::client::Body;

    use crate::offering::{InstanceType, Offering, Resources};
    use crate::providers::fake::FakeProvider;
    use crate::providers::provider::Provider;

    use super::{RecentCreates, reconcile_unschedulable_pods};

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
            "apiVersion": "growth.vettrdev.com/v1alpha1",
            "kind": "NodeRequest",
            "metadata": {
                "name": "test-nr",
                "namespace": "default",
                "resourceVersion": "1",
                "uid": "00000000-0000-0000-0000-000000000000"
            },
            "spec": {
                "nodeID": "test-node-1",
                "targetOffering": "test",
                "location": "eu-central",
                "resources": {
                    "cpu": 2,
                    "memoryMib": 4096,
                    "ephemeralStorageGib": null,
                    "gpu": 0,
                    "gpuModel": null
                }
            }
        });
        Response::builder()
            .status(201)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&nr).unwrap()))
            .unwrap()
    }

    /// Build a mock response for a NodeRequest list call.
    fn node_request_list_response(items: &[serde_json::Value]) -> Response<Body> {
        let list = serde_json::json!({
            "apiVersion": "growth.vettrdev.com/v1alpha1",
            "kind": "NodeRequestList",
            "metadata": { "resourceVersion": "1" },
            "items": items
        });
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&list).unwrap()))
            .unwrap()
    }

    /// Build a NodeRequest JSON value with a given name and phase.
    fn make_nr_json(name: &str, phase: &str) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "growth.vettrdev.com/v1alpha1",
            "kind": "NodeRequest",
            "metadata": {
                "name": name,
                "resourceVersion": "1",
                "uid": format!("nr-uid-{name}"),
                "ownerReferences": [{
                    "apiVersion": "growth.vettrdev.com/v1alpha1",
                    "kind": "NodePool",
                    "name": "default",
                    "uid": "default-pool-uid"
                }]
            },
            "spec": {
                "nodeID": format!("growth-{name}"),
                "targetOffering": "cpx22",
                "location": "eu-central",
                "resources": {
                    "cpu": 2,
                    "memoryMib": 4096,
                    "ephemeralStorageGib": null,
                    "gpu": 0,
                    "gpuModel": null
                }
            },
            "status": {
                "phase": phase
            }
        })
    }

    /// Build a mock response for a NodePool list call with a default pool.
    fn node_pool_list_response(offering_names: &[&str]) -> Response<Body> {
        let server_types: Vec<_> = offering_names
            .iter()
            .map(|name| {
                serde_json::json!({
                    "name": name,
                    "max": 100,
                    "min": 0
                })
            })
            .collect();
        let list = serde_json::json!({
            "apiVersion": "growth.vettrdev.com/v1alpha1",
            "kind": "NodePoolList",
            "metadata": { "resourceVersion": "1" },
            "items": [{
                "apiVersion": "growth.vettrdev.com/v1alpha1",
                "kind": "NodePool",
                "metadata": {
                    "name": "default",
                    "resourceVersion": "1",
                    "uid": "default-pool-uid"
                },
                "spec": {
                    "serverTypes": server_types
                }
            }]
        });
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&list).unwrap()))
            .unwrap()
    }

    /// Build a Pod that looks pending + unschedulable to `get_unschedulable_pods`.
    fn make_pending_unschedulable_pod(name: &str, cpu: &str, memory: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("default".into()),
                uid: Some(format!("uid-{name}")),
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
        use crate::offering::{Location, Region, Zone};
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
            location: Location {
                region: Region("eu-central".into()),
                zone: Some(Zone("fsn1-dc14".into())),
            },
        }
    }

    fn node_list_response() -> Response<Body> {
        let list = serde_json::json!({
            "apiVersion": "v1",
            "kind": "NodeList",
            "metadata": { "resourceVersion": "1" },
            "items": []
        });
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&list).unwrap()))
            .unwrap()
    }

    /// Spawn a mock API server that handles pod-list, NodeRequest, NodePool, and Node requests.
    ///
    /// Returns a counter tracking how many NodeRequests were created.
    fn spawn_mock_api(
        handle: ApiServerHandle,
        pods: Vec<Pod>,
        offering_names: Vec<&'static str>,
    ) -> Arc<AtomicUsize> {
        spawn_mock_api_with_nrs(handle, pods, offering_names, vec![])
    }

    /// Like `spawn_mock_api` but with a configurable NodeRequest list response.
    fn spawn_mock_api_with_nrs(
        mut handle: ApiServerHandle,
        pods: Vec<Pod>,
        offering_names: Vec<&'static str>,
        nr_items: Vec<serde_json::Value>,
    ) -> Arc<AtomicUsize> {
        let nr_count = Arc::new(AtomicUsize::new(0));
        let nr_count_inner = nr_count.clone();
        tokio::spawn(async move {
            while let Some((request, send)) = handle.next_request().await {
                let path = request.uri().path().to_string();
                let method = request.method().clone();
                if path.contains("/pods") {
                    send.send_response(pod_list_response(pods.clone()));
                } else if path.contains("nodepools") && method == http::Method::GET {
                    send.send_response(node_pool_list_response(&offering_names));
                } else if path.contains("noderequests") && method == http::Method::GET {
                    send.send_response(node_request_list_response(&nr_items));
                } else if path.contains("noderequests") && method == http::Method::POST {
                    nr_count_inner.fetch_add(1, Ordering::SeqCst);
                    send.send_response(node_request_create_response());
                } else if path.contains("/nodes") && method == http::Method::GET {
                    send.send_response(node_list_response());
                } else {
                    panic!("unexpected request: {method} {path}");
                }
            }
        });
        nr_count
    }

    #[tokio::test]
    async fn no_pending_pods_does_nothing() {
        let (client, handle) = mock_client();
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cpx22", 2, 4096, 0.01)]),
        );

        let nr_count = spawn_mock_api(handle, vec![], vec!["cpx22"]);

        let result = reconcile_unschedulable_pods(
            client,
            &provider,
            &mut RecentCreates::default(),
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(nr_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn one_pending_pod_creates_one_node_request() {
        let (client, handle) = mock_client();
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cpx22", 2, 4096, 0.01)]),
        );

        let pod = make_pending_unschedulable_pod("test-pod", "1", "2048Mi");
        let nr_count = spawn_mock_api(handle, vec![pod], vec!["cpx22"]);

        let result = reconcile_unschedulable_pods(
            client,
            &provider,
            &mut RecentCreates::default(),
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(nr_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn multiple_pods_bin_packed_into_multiple_node_requests() {
        let (client, handle) = mock_client();
        // Offering fits 2 pods (2 cpu, 4Gi) but each pod needs 1 cpu + 2Gi.
        // 3 pods → need 2 nodes.
        let offering = test_offering("cpx22", 2, 4096, 0.01);
        let provider = Provider::Fake(FakeProvider::new().with_offerings(vec![offering]));

        let pods = vec![
            make_pending_unschedulable_pod("pod-a", "1", "2048Mi"),
            make_pending_unschedulable_pod("pod-b", "1", "2048Mi"),
            make_pending_unschedulable_pod("pod-c", "1", "2048Mi"),
        ];
        let nr_count = spawn_mock_api(handle, pods, vec!["cpx22"]);

        let result = reconcile_unschedulable_pods(
            client,
            &provider,
            &mut RecentCreates::default(),
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await;
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
        let nr_count1 = spawn_mock_api(handle1, vec![pod], vec!["small", "large"]);
        reconcile_unschedulable_pods(
            client1,
            &provider,
            &mut RecentCreates::default(),
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await
        .unwrap();
        assert_eq!(nr_count1.load(Ordering::SeqCst), 1);

        // Second call — large offering now available too. Pod needs 3 cpu.
        let (client2, handle2) = mock_client();
        let pod = make_pending_unschedulable_pod("pod-2", "3", "4096Mi");
        let nr_count2 = spawn_mock_api(handle2, vec![pod], vec!["small", "large"]);
        reconcile_unschedulable_pods(
            client2,
            &provider,
            &mut RecentCreates::default(),
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await
        .unwrap();
        assert_eq!(nr_count2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn recent_creates_prevent_duplicate_node_requests() {
        // Two sequential calls share the same recent_creates. The mock API
        // always returns an empty NodeRequest list (simulating API lag), so only the
        // in-memory cache prevents the second call from creating duplicates.
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cpx22", 2, 4096, 0.01)]),
        );
        let pod = make_pending_unschedulable_pod("dup-pod", "1", "2048Mi");

        let mut recent_creates = RecentCreates::default();

        // First call — should create 1 NodeRequest and populate recent_creates.
        let (client1, handle1) = mock_client();
        let nr_count1 = spawn_mock_api(handle1, vec![pod.clone()], vec!["cpx22"]);
        reconcile_unschedulable_pods(
            client1,
            &provider,
            &mut recent_creates,
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            nr_count1.load(Ordering::SeqCst),
            1,
            "first call should create 1 NodeRequest"
        );
        assert!(
            !recent_creates.is_empty(),
            "recent_creates should have capacity recorded after first call"
        );

        // Second call — same pod still pending, empty NodeRequest list from API,
        // but recent_creates capacity absorbed the pod demand via subtract_in_flight.
        let (client2, handle2) = mock_client();
        let nr_count2 = spawn_mock_api(handle2, vec![pod], vec!["cpx22"]);
        reconcile_unschedulable_pods(
            client2,
            &provider,
            &mut recent_creates,
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            nr_count2.load(Ordering::SeqCst),
            0,
            "second call should create 0 NodeRequests — recent_creates prevents duplicates"
        );
    }

    #[tokio::test]
    async fn recent_creates_drained_when_api_catches_up() {
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cpx22", 2, 4096, 0.01)]),
        );
        let pod = make_pending_unschedulable_pod("drain-pod", "1", "2048Mi");

        let mut recent_creates = RecentCreates::default();

        // First call — empty NodeRequest list, creates 1 NodeRequest, populates recent_creates.
        let (client1, handle1) = mock_client();
        let nr_count1 = spawn_mock_api(handle1, vec![pod.clone()], vec!["cpx22"]);
        reconcile_unschedulable_pods(
            client1,
            &provider,
            &mut recent_creates,
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await
        .unwrap();
        assert_eq!(nr_count1.load(Ordering::SeqCst), 1);
        assert!(
            !recent_creates.is_empty(),
            "recent_creates should have an entry after first call"
        );

        // Second call — API now reflects the NodeRequest (Pending phase). The API-listed NR
        // absorbs the demand via in-flight resource subtraction, and recent_creates is drained
        // because its name matches an API entry. No new NodeRequest created.
        let (client2, handle2) = mock_client();
        let nr_items = vec![make_nr_json("test-nr", "Pending")];
        let nr_count2 = spawn_mock_api_with_nrs(handle2, vec![pod], vec!["cpx22"], nr_items);
        reconcile_unschedulable_pods(
            client2,
            &provider,
            &mut recent_creates,
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await
        .unwrap();
        assert_eq!(nr_count2.load(Ordering::SeqCst), 0);
        assert!(
            recent_creates.is_empty(),
            "recent_creates should be drained once the API reflects the NodeRequest"
        );
    }

    #[tokio::test]
    async fn unmet_nr_releases_pods_back_to_demand() {
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cpx22", 2, 4096, 0.01)]),
        );
        let pod = make_pending_unschedulable_pod("retry-pod", "1", "2048Mi");

        let mut recent_creates = RecentCreates::default();

        // An Unmet NodeRequest exists in the API. With unmet_ttl=0 it is immediately
        // expired and does not contribute to in-flight capacity, so the pod re-enters demand.
        let (client, handle) = mock_client();
        let nr_items = vec![make_nr_json("nr-unmet", "Unmet")];
        let nr_count = spawn_mock_api_with_nrs(handle, vec![pod], vec!["cpx22"], nr_items);
        reconcile_unschedulable_pods(
            client,
            &provider,
            &mut recent_creates,
            Duration::from_secs(0),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            nr_count.load(Ordering::SeqCst),
            1,
            "pod should re-enter demand and produce a new NodeRequest"
        );
        assert!(
            !recent_creates.is_empty(),
            "new NodeRequest should populate recent_creates"
        );
    }

    /// When the only NodePool in the cluster has no metadata.name,
    /// `get_node_pools` should skip it. The pending pod then has no pool
    /// to bind to, so zero NodeRequests are created.
    #[tokio::test]
    async fn nameless_pool_is_skipped() {
        let (client, mut handle) = mock_client();
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cpx22", 2, 4096, 0.01)]),
        );

        let pod = make_pending_unschedulable_pod("orphan-pod", "1", "2048Mi");
        let nr_count = Arc::new(AtomicUsize::new(0));
        let nr_count_inner = nr_count.clone();

        // Mock API: returns a NodePool list where the only pool has no name.
        tokio::spawn(async move {
            while let Some((request, send)) = handle.next_request().await {
                let path = request.uri().path().to_string();
                let method = request.method().clone();
                if path.contains("/pods") {
                    send.send_response(pod_list_response(vec![pod.clone()]));
                } else if path.contains("nodepools") && method == http::Method::GET {
                    // Pool with name omitted — should be skipped by filter_map.
                    let list = serde_json::json!({
                        "apiVersion": "growth.vettrdev.com/v1alpha1",
                        "kind": "NodePoolList",
                        "metadata": { "resourceVersion": "1" },
                        "items": [{
                            "apiVersion": "growth.vettrdev.com/v1alpha1",
                            "kind": "NodePool",
                            "metadata": {
                                "resourceVersion": "1",
                                "uid": "pool-uid-no-name"
                            },
                            "spec": {
                                "serverTypes": [{ "name": "cpx22", "max": 100, "min": 0 }]
                            }
                        }]
                    });
                    send.send_response(
                        Response::builder()
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&list).unwrap()))
                            .unwrap(),
                    );
                } else if path.contains("noderequests") && method == http::Method::GET {
                    send.send_response(node_request_list_response(&[]));
                } else if path.contains("noderequests") && method == http::Method::POST {
                    nr_count_inner.fetch_add(1, Ordering::SeqCst);
                    send.send_response(node_request_create_response());
                } else if path.contains("/nodes") && method == http::Method::GET {
                    send.send_response(node_list_response());
                } else {
                    panic!("unexpected request: {method} {path}");
                }
            }
        });

        let result = reconcile_unschedulable_pods(
            client,
            &provider,
            &mut RecentCreates::default(),
            Duration::from_secs(120),
            k8s_openapi::jiff::Timestamp::now(),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(
            nr_count.load(Ordering::SeqCst),
            0,
            "nameless pool should be skipped — pod has no pool, so no NodeRequest is created"
        );
    }
}
