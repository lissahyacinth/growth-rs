mod decision;
pub use decision::*;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::ListParams;
use kube::{Api, Client};
use tokio::time::Instant;
use tracing::{debug, instrument, warn};

use crate::crds::node_pool::NodePool;
use crate::crds::node_request::{NodeRequest, NodeRequestPhase, NodeRequestSpec, create_node_request};
use crate::offering::{INSTANCE_TYPE_LABEL, POOL_LABEL};
use crate::providers::provider::Provider;

use super::ReconcileError;

/// In-memory cache of pod UIDs claimed by recently-created NodeRequests.
///
/// Bridges the gap between NR creation (POST) and the next API list reflecting
/// it, preventing duplicate NRs across back-to-back reconciliation loops.
pub struct PendingClaims {
    claims: HashMap<String, Instant>,
}

/// Entries older than this are expired regardless of API state, preventing
/// permanent suppression if an NR is externally deleted before the next
/// successful reconcile.
const PENDING_CLAIMS_TTL: Duration = Duration::from_secs(60);

impl PendingClaims {
    pub fn new() -> Self {
        Self {
            claims: HashMap::new(),
        }
    }

    /// Record pod UIDs as recently claimed. Called after creating a NodeRequest.
    pub fn record(&mut self, uids: impl IntoIterator<Item = String>) {
        let now = Instant::now();
        self.claims.extend(uids.into_iter().map(|uid| (uid, now)));
    }

    /// Drain entries that the API now reflects and expire stale entries.
    ///
    /// - UIDs present in `api_uids` are removed (the API caught up).
    /// - UIDs older than `PENDING_CLAIMS_TTL` are removed (safety valve).
    pub fn drain_reflected(&mut self, api_uids: &HashSet<String>) {
        let now = Instant::now();
        let before = self.claims.len();
        self.claims.retain(|uid, inserted_at| {
            !api_uids.contains(uid)
                && now.duration_since(*inserted_at) < PENDING_CLAIMS_TTL
        });
        let drained = before - self.claims.len();
        if drained > 0 {
            debug!(drained, remaining = self.claims.len(), "drained pending_claims");
        }
    }

    /// Check if a UID is in the pending claims cache.
    pub fn contains(&self, uid: &str) -> bool {
        self.claims.contains_key(uid)
    }

    pub fn len(&self) -> usize {
        self.claims.len()
    }

    pub fn is_empty(&self) -> bool {
        self.claims.is_empty()
    }
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

/// Fetch all NodePool CRDs and map them to PoolConfig.
async fn get_node_pools(client: Client) -> Result<Vec<PoolConfig>> {
    let api: Api<NodePool> = Api::all(client);
    let lp = ListParams::default();
    Ok(api
        .list(&lp)
        .await?
        .into_iter()
        .filter_map(|np| {
            let name = match np.metadata.name.clone() {
                Some(n) => n,
                None => {
                    warn!("skipping NodePool with missing name");
                    return None;
                }
            };
            let uid = match np.metadata.uid.clone() {
                Some(u) => u,
                None => {
                    warn!(pool = %name, "skipping NodePool with missing uid");
                    return None;
                }
            };
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

/// Snapshot of in-flight NodeRequest state used during reconciliation.
struct InFlightState {
    /// Pending/Provisioning NR counts: pool name → instance type → count.
    /// Combined with node counts to compute total occupied slots.
    nr_counts: HashMap<String, HashMap<String, u32>>,
    /// Pod UIDs claimed by every NR except Unmet (Pending, Provisioning, Ready, Deprovisioning).
    /// Pods with these UIDs are excluded from demand so we never double-provision.
    claimed_pod_uids: HashSet<String>,
    /// Pod UIDs from ALL NRs including Unmet, used to drain the in-memory
    /// pending_claims cache once the API reflects newly-created NRs.
    all_nr_uids: HashSet<String>,
}

/// Scan all NodeRequests to build the in-flight state.
///
/// - `claimed_pod_uids`: collected from every non-Unmet NR (including Ready),
///   so pods stay "claimed" until the NR either fails or is deleted.
/// - `nr_counts`: only Pending/Provisioning NRs, counted by pool + instance type.
async fn get_in_flight_state(client: Client) -> Result<InFlightState> {
    let api: Api<NodeRequest> = Api::all(client);
    let lp = ListParams::default();
    let mut nr_counts: HashMap<String, HashMap<String, u32>> = HashMap::new();
    let mut claimed_pod_uids = HashSet::new();
    let mut all_nr_uids = HashSet::new();

    for nr in api.list(&lp).await? {
        let phase = nr.phase();

        // Collect UIDs from ALL NRs (including Unmet) so the in-memory
        // pending_claims cache can be drained once the API catches up.
        all_nr_uids.extend(nr.spec.claimed_pod_uids.iter().cloned());

        // Terminal failure — release claimed pods back to the pool.
        if phase == NodeRequestPhase::Unmet {
            continue;
        }

        // Collect claimed pod UIDs from all non-terminal NRs.
        claimed_pod_uids.extend(nr.spec.claimed_pod_uids.iter().cloned());

        // Only count Pending/Provisioning for in-flight resource tracking.
        // Ready NRs are already reflected in the node count.
        if matches!(phase, NodeRequestPhase::Ready | NodeRequestPhase::Deprovisioning) {
            continue;
        }

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

        *nr_counts
            .entry(pool_name)
            .or_default()
            .entry(nr.spec.target_offering.clone())
            .or_insert(0) += 1;
    }

    Ok(InFlightState {
        nr_counts,
        claimed_pod_uids,
        all_nr_uids,
    })
}

/// Count existing Growth-managed nodes per pool per instance type.
///
/// Nodes are identified by the `growth.vettrdev.com/pool` and
/// `growth.vettrdev.com/instance-type` labels set during provisioning.
async fn get_node_counts(client: Client) -> Result<HashMap<String, HashMap<String, u32>>> {
    let nodes: Api<Node> = Api::all(client);
    let lp = ListParams::default().labels("app.kubernetes.io/managed-by=growth");
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

/// Merge NR in-flight counts and existing node counts into a single occupied map.
fn merge_occupied_counts(
    nr_counts: HashMap<String, HashMap<String, u32>>,
    node_counts: HashMap<String, HashMap<String, u32>>,
) -> HashMap<String, HashMap<String, u32>> {
    let mut merged = node_counts;
    for (pool, types) in nr_counts {
        let pool_entry = merged.entry(pool).or_default();
        for (instance_type, count) in types {
            *pool_entry.entry(instance_type).or_insert(0) += count;
        }
    }
    merged
}

async fn gather_cluster_state(
    client: &Client,
    provider: &Provider,
    pending_claims: &mut PendingClaims,
) -> Result<ClusterState> {
    let unschedulable_pods = get_unschedulable_pods(client.clone()).await?;
    let offerings = provider.offerings().await;
    let in_flight_state = get_in_flight_state(client.clone()).await?;
    let node_counts = get_node_counts(client.clone()).await?;
    let pools = get_node_pools(client.clone()).await?;

    pending_claims.drain_reflected(&in_flight_state.all_nr_uids);

    // Parse resource demands, filtering out pods already claimed by a NodeRequest
    // OR still in the in-memory pending_claims cache (bridges API lag).
    let demands: Vec<_> = unschedulable_pods
        .iter()
        .map(crate::offering::PodResources::from_pod)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|pr| {
            !in_flight_state.claimed_pod_uids.contains(&pr.uid)
                && !pending_claims.contains(&pr.uid)
        })
        .collect();

    let occupied_counts = merge_occupied_counts(in_flight_state.nr_counts, node_counts);

    debug!(
        total_unschedulable = unschedulable_pods.len(),
        claimed = in_flight_state.claimed_pod_uids.len(),
        pending_claims = pending_claims.len(),
        unclaimed_demands = demands.len(),
        "filtered claimed pods"
    );

    Ok(ClusterState {
        demands,
        offerings,
        occupied_counts,
        pools,
    })
}

/// Build initial PendingClaims from existing NodeRequests in the cluster.
///
/// On startup, seeds the cache with pod UIDs claimed by non-Unmet NRs so
/// the first reconcile doesn't duplicate work already tracked by the API.
pub async fn gather_pending_claims(client: &Client) -> Result<PendingClaims> {
    let api: Api<NodeRequest> = Api::all(client.clone());
    let nrs = api.list(&ListParams::default()).await?;
    let mut claims = PendingClaims::new();
    let uids: Vec<String> = nrs
        .into_iter()
        .filter(|nr| nr.phase() != NodeRequestPhase::Unmet)
        .flat_map(|nr| nr.spec.claimed_pod_uids)
        .collect();
    if !uids.is_empty() {
        debug!(count = uids.len(), "seeded pending_claims from existing NodeRequests");
        claims.record(uids);
    }
    Ok(claims)
}

/// One-shot reconcile: gather state, solve, and create any needed NodeRequests.
#[instrument(skip_all, fields(reconcile_id = %uuid::Uuid::new_v4()))]
#[allow(unused_variables, unused_assignments)] // nr_creates used only with failpoints
pub async fn reconcile_unschedulable_pods(
    client: Client,
    provider: &Provider,
    pending_claims: &mut PendingClaims,
) -> Result<(), ReconcileError> {
    let state = gather_cluster_state(&client, provider, pending_claims).await?;
    let result = reconcile_pods(state)?;

    for err in &result.pod_errors {
        warn!(pod = %err.pod_id, reason = %err.reason, "pod could not be assigned to a pool");
    }

    let mut nr_creates = 0usize;
    for demand in result.demands {
        fail::fail_point!("reconcile_after_nr_create", |_| {
            Err(ReconcileError::FaultInjected(nr_creates))
        });

        let claimed_uids = demand.claimed_pod_uids.clone();
        create_node_request(
            client.clone(),
            &demand.pool,
            &demand.pool_uid,
            NodeRequestSpec {
                target_offering: demand.target_offering.instance_type.to_string(),
                resources: demand.target_offering.resources.clone(),
                node_id: format!("growth-{}", uuid::Uuid::new_v4()),
                claimed_pod_uids: demand.claimed_pod_uids,
            },
        )
        .await?;
        nr_creates += 1;
        // API create succeeded — record UIDs so the next reconcile (which may
        // run before the API list reflects this NR) still filters these pods.
        pending_claims.record(claimed_uids);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    use super::{PendingClaims, reconcile_unschedulable_pods};

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
                "resources": {
                    "cpu": 2,
                    "memoryMib": 4096,
                    "ephemeralStorageGib": null,
                    "gpu": 0,
                    "gpuModel": null
                },
                "claimedPodUids": []
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

    /// Build a NodeRequest JSON value with a given phase and claimed pod UIDs.
    fn make_nr_json(name: &str, phase: &str, claimed_uids: &[&str]) -> serde_json::Value {
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
                "targetOffering": "cx22",
                "resources": {
                    "cpu": 2,
                    "memoryMib": 4096,
                    "ephemeralStorageGib": null,
                    "gpu": 0,
                    "gpuModel": null
                },
                "claimedPodUids": claimed_uids
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

    /// Like `spawn_mock_api` but with a configurable NR list response.
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
            FakeProvider::new().with_offerings(vec![test_offering("cx22", 2, 4096, 0.01)]),
        );

        let nr_count = spawn_mock_api(handle, vec![], vec!["cx22"]);

        let result =
            reconcile_unschedulable_pods(client, &provider, &mut PendingClaims::new()).await;
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
        let nr_count = spawn_mock_api(handle, vec![pod], vec!["cx22"]);

        let result =
            reconcile_unschedulable_pods(client, &provider, &mut PendingClaims::new()).await;
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
        let nr_count = spawn_mock_api(handle, pods, vec!["cx22"]);

        let result =
            reconcile_unschedulable_pods(client, &provider, &mut PendingClaims::new()).await;
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
        reconcile_unschedulable_pods(client1, &provider, &mut PendingClaims::new())
            .await
            .unwrap();
        assert_eq!(nr_count1.load(Ordering::SeqCst), 1);

        // Second call — large offering now available too. Pod needs 3 cpu.
        let (client2, handle2) = mock_client();
        let pod = make_pending_unschedulable_pod("pod-2", "3", "4096Mi");
        let nr_count2 = spawn_mock_api(handle2, vec![pod], vec!["small", "large"]);
        reconcile_unschedulable_pods(client2, &provider, &mut PendingClaims::new())
            .await
            .unwrap();
        assert_eq!(nr_count2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pending_claims_prevent_duplicate_node_requests() {
        // Two sequential calls share the same pending_claims. The mock API
        // always returns an empty NR list (simulating API lag), so only the
        // in-memory cache prevents the second call from creating duplicates.
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cx22", 2, 4096, 0.01)]),
        );
        let pod = make_pending_unschedulable_pod("dup-pod", "1", "2048Mi");

        let mut pending_claims = PendingClaims::new();

        // First call — should create 1 NR and populate pending_claims.
        let (client1, handle1) = mock_client();
        let nr_count1 = spawn_mock_api(handle1, vec![pod.clone()], vec!["cx22"]);
        reconcile_unschedulable_pods(client1, &provider, &mut pending_claims)
            .await
            .unwrap();
        assert_eq!(nr_count1.load(Ordering::SeqCst), 1, "first call should create 1 NR");
        assert!(
            pending_claims.contains("uid-dup-pod"),
            "pending_claims should contain the pod UID after creation"
        );

        // Second call — same pod still pending, empty NR list from API,
        // but pending_claims should filter it out.
        let (client2, handle2) = mock_client();
        let nr_count2 = spawn_mock_api(handle2, vec![pod], vec!["cx22"]);
        reconcile_unschedulable_pods(client2, &provider, &mut pending_claims)
            .await
            .unwrap();
        assert_eq!(
            nr_count2.load(Ordering::SeqCst),
            0,
            "second call should create 0 NRs — pending_claims prevents duplicates"
        );
    }

    #[tokio::test]
    async fn pending_claims_drained_when_api_catches_up() {
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cx22", 2, 4096, 0.01)]),
        );
        let pod = make_pending_unschedulable_pod("drain-pod", "1", "2048Mi");

        let mut pending_claims = PendingClaims::new();

        // First call — empty NR list, creates 1 NR, populates pending_claims.
        let (client1, handle1) = mock_client();
        let nr_count1 = spawn_mock_api(handle1, vec![pod.clone()], vec!["cx22"]);
        reconcile_unschedulable_pods(client1, &provider, &mut pending_claims)
            .await
            .unwrap();
        assert_eq!(nr_count1.load(Ordering::SeqCst), 1);
        assert!(pending_claims.contains("uid-drain-pod"));

        // Second call — API now reflects the NR (Pending phase, claiming this UID).
        // pending_claims should be drained, and the NR's claimed_pod_uids filters
        // the pod from demand. No new NR created.
        let (client2, handle2) = mock_client();
        let nr_items = vec![make_nr_json("nr-1", "Pending", &["uid-drain-pod"])];
        let nr_count2 = spawn_mock_api_with_nrs(handle2, vec![pod], vec!["cx22"], nr_items);
        reconcile_unschedulable_pods(client2, &provider, &mut pending_claims)
            .await
            .unwrap();
        assert_eq!(nr_count2.load(Ordering::SeqCst), 0);
        assert!(
            pending_claims.is_empty(),
            "pending_claims should be drained once the API reflects the NR"
        );
    }

    #[tokio::test]
    async fn unmet_nr_releases_pods_back_to_demand() {
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cx22", 2, 4096, 0.01)]),
        );
        let pod = make_pending_unschedulable_pod("retry-pod", "1", "2048Mi");

        let mut pending_claims = PendingClaims::new();
        // Seed pending_claims as if a previous reconcile created an NR for this pod.
        pending_claims.record(["uid-retry-pod".to_string()]);

        // The NR is now Unmet — its UIDs appear in all_nr_uids (draining the cache)
        // but NOT in claimed_pod_uids, so the pod re-enters demand.
        let (client, handle) = mock_client();
        let nr_items = vec![make_nr_json("nr-unmet", "Unmet", &["uid-retry-pod"])];
        let nr_count = spawn_mock_api_with_nrs(handle, vec![pod], vec!["cx22"], nr_items);
        reconcile_unschedulable_pods(client, &provider, &mut pending_claims)
            .await
            .unwrap();
        assert_eq!(
            nr_count.load(Ordering::SeqCst),
            1,
            "pod should re-enter demand and produce a new NR"
        );
        // The UID is back in pending_claims because the new NR was just created.
        // The old Unmet NR's drain happened, then the new create re-populated it.
        assert!(
            pending_claims.contains("uid-retry-pod"),
            "new NR should re-populate pending_claims"
        );
    }

    /// When the only NodePool in the cluster has no metadata.name,
    /// `get_node_pools` should skip it. The pending pod then has no pool
    /// to bind to, so zero NodeRequests are created.
    #[tokio::test]
    async fn nameless_pool_is_skipped() {
        let (client, mut handle) = mock_client();
        let provider = Provider::Fake(
            FakeProvider::new().with_offerings(vec![test_offering("cx22", 2, 4096, 0.01)]),
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
                                "serverTypes": [{ "name": "cx22", "max": 100, "min": 0 }]
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

        let result =
            reconcile_unschedulable_pods(client, &provider, &mut PendingClaims::new()).await;
        assert!(result.is_ok());
        assert_eq!(
            nr_count.load(Ordering::SeqCst),
            0,
            "nameless pool should be skipped — pod has no pool, so no NR is created"
        );
    }
}
