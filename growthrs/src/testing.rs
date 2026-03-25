//! Shared test helpers for both integration tests and the `test_pod` binary.
//!
//! Gated behind `#[cfg(any(test, feature = "testing"))]` in `lib.rs`.

use k8s_openapi::api::core::v1::{
    Affinity, Container, Node, NodeCondition, NodeStatus, Pod, PodAffinityTerm, PodAntiAffinity,
    PodSpec, ResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{DeleteParams, ListParams, ObjectMeta, PostParams};
use kube::{Api, Client, Config};

use crate::resources::node_pool::{NodePool, NodePoolSpec, ServerTypeConfig};

use crate::config::ControllerContext;
use crate::offering::{MANAGED_BY_LABEL, MANAGED_BY_SELECTOR, POOL_LABEL, Resources};
use crate::providers::kwok::to_capacity;
use crate::providers::provider::Provider;
use crate::resources::node_removal_request::NodeRemovalRequest;
use crate::resources::node_request::NodeRequest;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

/// Default kubeconfig context used for integration tests.
///
/// Override with `KUBE_CONTEXT` env var to target a different cluster.
const DEFAULT_TEST_CONTEXT: &str = "default";

/// Build a kube client for E2E tests.
///
/// Context selection (in priority order):
/// 1. `KUBE_CONTEXT` env var — use a specific kubeconfig context.
/// 2. Falls back to `rancher-desktop` context.
///
/// If `KUBE_PROXY_URL` is also set, the cluster URL is overridden to that
/// endpoint (useful for toxiproxy / stunnel interposition).
pub async fn test_client() -> Client {
    let context =
        std::env::var("KUBE_CONTEXT").unwrap_or_else(|_| DEFAULT_TEST_CONTEXT.to_string());

    let mut config = Config::from_kubeconfig(&kube::config::KubeConfigOptions {
        context: Some(context.clone()),
        ..Default::default()
    })
    .await
    .unwrap_or_else(|e| panic!("failed to load kubeconfig context {context:?}: {e}"));

    if let Ok(url) = std::env::var("KUBE_PROXY_URL") {
        config.cluster_url = url.parse().expect("KUBE_PROXY_URL is not a valid URL");
        config.accept_invalid_certs = true;
    }

    Client::try_from(config).unwrap()
}

/// Initialise tracing for tests. Safe to call multiple times (subsequent calls are no-ops).
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

/// Build a [`ControllerContext`] for integration tests.
///
/// Uses the given provider and default timeouts / scale-down config.
/// Tests needing custom `ScaleDownConfig` should construct `ControllerContext` directly.
pub fn make_test_ctx(client: Client, provider: Provider) -> Arc<ControllerContext> {
    Arc::new(ControllerContext {
        client,
        provider,
        provisioning_timeout: Duration::from_secs(300),
        scale_down: crate::config::ScaleDownConfig::default(),
        clock: Arc::new(crate::clock::SystemClock),
    })
}

/// Check whether a node carries a specific taint (by key and effect).
pub fn has_taint(node: &Node, key: &str, effect: &str) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|ts| ts.iter().any(|t| t.key == key && t.effect == effect))
        .unwrap_or(false)
}

/// Validate that a string looks like a Kubernetes resource quantity.
pub fn validate_quantity(value: &str, field: &str) -> Result<()> {
    anyhow::ensure!(
        !value.is_empty() && value.as_bytes()[0].is_ascii_digit(),
        "{field} value {value:?} is not a valid Kubernetes quantity \
         (expected e.g. \"1\", \"500m\", \"4096Mi\", \"2Gi\")"
    );
    Ok(())
}

/// Parse a server type spec like "cpx22:10" or "cpx22:2:10" (name:max or name:min:max).
pub fn parse_server_type(s: &str) -> Result<ServerTypeConfig> {
    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [name, max] => Ok(ServerTypeConfig {
            name: name.to_string(),
            max: max.parse()?,
            min: 0,
        }),
        [name, min, max] => Ok(ServerTypeConfig {
            name: name.to_string(),
            max: max.parse()?,
            min: min.parse()?,
        }),
        _ => anyhow::bail!("invalid server type spec {s:?}, expected NAME:MAX or NAME:MIN:MAX"),
    }
}

/// Create a test pod with resource requests.
pub async fn create_pod(
    client: Client,
    name: &str,
    cpu: &str,
    memory: &str,
    gpu: Option<u32>,
    pool: Option<&str>,
) -> Result<()> {
    validate_quantity(cpu, "cpu")?;
    validate_quantity(memory, "memory")?;

    let pods: Api<Pod> = Api::default_namespaced(client);

    let mut requests = BTreeMap::from([
        ("cpu".into(), Quantity(cpu.into())),
        ("memory".into(), Quantity(memory.into())),
    ]);
    if let Some(n) = gpu {
        requests.insert("nvidia.com/gpu".into(), Quantity(n.to_string()));
    }

    let node_selector = pool.map(|p| BTreeMap::from([(POOL_LABEL.to_string(), p.to_string())]));

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            labels: Some(BTreeMap::from([(
                MANAGED_BY_LABEL.into(),
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
            node_selector,
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &pod).await?;
    println!("created test pod {name} (cpu={cpu}, memory={memory}, gpu={gpu:?}, pool={pool:?})");
    Ok(())
}

/// Create multiple test pods with a common prefix.
///
/// Each pod gets a UUIDv7-based suffix (time-ordered) so repeated
/// invocations never collide with existing pods.
pub async fn create_many_pods(
    client: Client,
    prefix: &str,
    count: u32,
    cpu: &str,
    memory: &str,
    gpu: Option<u32>,
    pool: Option<&str>,
) -> Result<()> {
    for _ in 0..count {
        let id = uuid::Uuid::now_v7();
        let name = format!("{prefix}-{id}");
        create_pod(client.clone(), &name, cpu, memory, gpu, pool).await?;
    }
    println!("created {count} pods with prefix {prefix:?}");
    Ok(())
}

/// Create a NodePool CRD.
pub async fn create_node_pool(
    client: Client,
    name: &str,
    server_types: Vec<ServerTypeConfig>,
    labels: BTreeMap<String, String>,
) -> Result<()> {
    let api: Api<NodePool> = Api::all(client);
    let np = NodePool::new(
        name,
        NodePoolSpec {
            server_types,
            labels,
            locations: None,
            node_class_ref: None,
        },
    );
    api.create(&PostParams::default(), &np).await?;
    println!("created NodePool {name}");
    Ok(())
}

/// Delete all growth-test resources: pods, nodes, NodeRequests, NodePools.
/// Waits for resources to be fully removed to avoid "object is being deleted" race conditions.
pub async fn nuke(client: Client) -> Result<()> {
    // Delete test pods (managed-by=growth-test)
    let pods: Api<Pod> = Api::all(client.clone());
    let lp = ListParams::default().labels(&format!("{MANAGED_BY_LABEL}=growth-test"));
    let pod_list = pods.list(&lp).await?;
    let pod_count = pod_list.items.len();
    for pod in &pod_list {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        let name = pod.metadata.name.as_deref().unwrap_or("?");
        let namespaced: Api<Pod> = Api::namespaced(client.clone(), ns);
        let _ = namespaced
            .delete(name, &DeleteParams::default().grace_period(0))
            .await;
    }
    println!("deleted {pod_count} test pods");

    // Delete growth-managed nodes (managed-by=growth)
    let nodes: Api<Node> = Api::all(client.clone());
    let lp = ListParams::default().labels(MANAGED_BY_SELECTOR);
    let node_list = nodes.list(&lp).await?;
    let node_count = node_list.items.len();
    for node in &node_list {
        let name = node.metadata.name.as_deref().unwrap_or("?");
        let _ = nodes.delete(name, &DeleteParams::default()).await;
    }
    println!("deleted {node_count} growth-managed nodes");

    // Delete KWOK test nodes (managed-by=growth-test)
    let lp = ListParams::default().labels(&format!("{MANAGED_BY_LABEL}=growth-test"));
    let node_list = nodes.list(&lp).await?;
    let node_count = node_list.items.len();
    for node in &node_list {
        let name = node.metadata.name.as_deref().unwrap_or("?");
        let _ = nodes.delete(name, &DeleteParams::default()).await;
    }
    println!("deleted {node_count} growth-test nodes");

    // Delete all NodeRemovalRequests (strip finalizers first to avoid blocking on terminating NRRs)
    let nrrs: Api<NodeRemovalRequest> = Api::all(client.clone());
    let nrr_list = nrrs.list(&ListParams::default()).await?;
    let nrr_count = nrr_list.items.len();
    for nrr in &nrr_list {
        let name = nrr.metadata.name.as_deref().unwrap_or("?");
        if nrr
            .metadata
            .finalizers
            .as_ref()
            .is_some_and(|f| !f.is_empty())
        {
            let patch = serde_json::json!({ "metadata": { "finalizers": null } });
            let _ = nrrs
                .patch(
                    name,
                    &kube::api::PatchParams::apply("growthrs-test"),
                    &kube::api::Patch::Merge(patch),
                )
                .await;
        }
        let _ = nrrs.delete(name, &DeleteParams::default()).await;
    }
    println!("deleted {nrr_count} NodeRemovalRequests");

    // Delete all NodeRequests
    let nrs: Api<NodeRequest> = Api::all(client.clone());
    let nr_list = nrs.list(&ListParams::default()).await?;
    let nr_count = nr_list.items.len();
    for nr in &nr_list {
        let name = nr.metadata.name.as_deref().unwrap_or("?");
        let _ = nrs.delete(name, &DeleteParams::default()).await;
    }
    println!("deleted {nr_count} NodeRequests");

    // Delete all NodePools
    let nps: Api<NodePool> = Api::all(client.clone());
    let np_list = nps.list(&ListParams::default()).await?;
    let np_count = np_list.items.len();
    for np in &np_list {
        let name = np.metadata.name.as_deref().unwrap_or("?");
        let _ = nps.delete(name, &DeleteParams::default()).await;
    }
    println!("deleted {np_count} NodePools");

    // Wait for pods, nodes, and NRRs to be fully removed (avoids "object is being deleted" races)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let lp_test = ListParams::default().labels(&format!("{MANAGED_BY_LABEL}=growth-test"));
        let lp_growth = ListParams::default().labels(MANAGED_BY_SELECTOR);
        let remaining_test_pods = pods.list(&lp_test).await?.items.len();
        let remaining_test_nodes = nodes.list(&lp_test).await?.items.len();
        let remaining_growth_nodes = nodes.list(&lp_growth).await?.items.len();
        let remaining_nrrs = nrrs.list(&ListParams::default()).await?.items.len();
        let total =
            remaining_test_pods + remaining_test_nodes + remaining_growth_nodes + remaining_nrrs;
        if total == 0 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            println!(
                "warning: nuke timed out waiting for deletion ({remaining_test_pods} test pods, \
                 {remaining_test_nodes} test nodes, {remaining_growth_nodes} growth nodes, \
                 {remaining_nrrs} NRRs remaining)"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}

/// Create a KWOK (fake) node with specified resources and labels.
/// Create a KWOK node for test infrastructure.
///
/// These nodes simulate already-ready infrastructure — they are NOT going through
/// GrowthRS's provisioning lifecycle, so they don't carry the startup taint
/// (`growth.vettrdev.com/unregistered:NoExecute`). We set `Ready=True` explicitly
/// to prevent the node lifecycle controller from adding `unreachable` taints.
pub async fn create_kwok_node(
    client: Client,
    name: &str,
    resources: &Resources,
    extra_labels: BTreeMap<String, String>,
) -> Result<()> {
    let nodes: Api<Node> = Api::all(client);

    let mut capacity = to_capacity(resources);
    capacity.insert("pods".into(), Quantity("110".into()));
    let allocatable = capacity.clone();

    let mut labels = BTreeMap::from([
        ("type".into(), "kwok".into()),
        (MANAGED_BY_LABEL.into(), "growth-test".into()),
    ]);
    labels.extend(extra_labels);

    let now = k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        k8s_openapi::jiff::Timestamp::now(),
    );

    let node = Node {
        metadata: ObjectMeta {
            name: Some(name.into()),
            labels: Some(labels),
            annotations: Some(BTreeMap::from([(
                "kwok.x-k8s.io/node".into(),
                "fake".into(),
            )])),
            ..Default::default()
        },
        status: Some(NodeStatus {
            capacity: Some(capacity),
            allocatable: Some(allocatable),
            conditions: Some(vec![NodeCondition {
                type_: "Ready".into(),
                status: "True".into(),
                last_heartbeat_time: Some(now.clone()),
                last_transition_time: Some(now),
                reason: Some("KwokReady".into()),
                message: Some("KWOK test node".into()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        spec: None,
    };

    nodes.create(&PostParams::default(), &node).await?;
    println!("created KWOK node {name}");
    Ok(())
}

/// Create a pod with anti-affinity or affinity rules.
///
/// `app_label` is set as both a pod label and the matchLabels selector.
/// `topology_key` is typically `topology.kubernetes.io/zone`.
/// If `anti` is true, creates anti-affinity; otherwise affinity.
pub async fn create_pod_with_affinity(
    client: Client,
    name: &str,
    cpu: &str,
    memory: &str,
    app_label: &str,
    topology_key: &str,
    anti: bool,
    pool: Option<&str>,
) -> Result<()> {
    validate_quantity(cpu, "cpu")?;
    validate_quantity(memory, "memory")?;

    let pods: Api<Pod> = Api::default_namespaced(client);

    let requests = BTreeMap::from([
        ("cpu".into(), Quantity(cpu.into())),
        ("memory".into(), Quantity(memory.into())),
    ]);

    let mut node_selector = pool.map(|p| BTreeMap::from([(POOL_LABEL.to_string(), p.to_string())]));
    // Ensure pods only land on KWOK nodes
    node_selector
        .get_or_insert_with(BTreeMap::new)
        .insert("type".into(), "kwok".into());

    let pod_labels = BTreeMap::from([
        ("app".into(), app_label.into()),
        (MANAGED_BY_LABEL.into(), "growth-test".into()),
    ]);

    let affinity_term = PodAffinityTerm {
        topology_key: topology_key.into(),
        label_selector: Some(LabelSelector {
            match_labels: Some(BTreeMap::from([("app".into(), app_label.into())])),
            ..Default::default()
        }),
        ..Default::default()
    };

    let affinity = if anti {
        Affinity {
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: Some(vec![affinity_term]),
                ..Default::default()
            }),
            ..Default::default()
        }
    } else {
        Affinity {
            pod_affinity: Some(k8s_openapi::api::core::v1::PodAffinity {
                required_during_scheduling_ignored_during_execution: Some(vec![affinity_term]),
                ..Default::default()
            }),
            ..Default::default()
        }
    };

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            labels: Some(pod_labels),
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
            node_selector,
            affinity: Some(affinity),
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &pod).await?;
    println!(
        "created pod {name} with {}affinity (app={app_label}, topo={topology_key})",
        if anti { "anti-" } else { "" }
    );
    Ok(())
}

/// List all NodeRequests in the cluster.
pub async fn list_node_requests(client: Client) -> Result<Vec<NodeRequest>> {
    let api: Api<NodeRequest> = Api::all(client);
    Ok(api
        .list(&ListParams::default())
        .await?
        .into_iter()
        .collect())
}

/// Poll until at least `min_count` NodeRequests exist, or timeout.
pub async fn wait_for_node_requests(
    client: Client,
    min_count: usize,
    timeout: Duration,
) -> Result<Vec<NodeRequest>> {
    let api: Api<NodeRequest> = Api::all(client);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let nrs: Vec<NodeRequest> = api
            .list(&ListParams::default())
            .await?
            .into_iter()
            .collect();
        if nrs.len() >= min_count {
            return Ok(nrs);
        }
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!(
                "timeout waiting for at least {min_count} NodeRequests (found {})",
                nrs.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Poll until a pod has `spec.nodeName` set (i.e. it was scheduled), or timeout.
pub async fn wait_for_pod_scheduled(
    client: Client,
    name: &str,
    timeout: Duration,
) -> Result<String> {
    let pods: Api<Pod> = Api::default_namespaced(client);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("timeout waiting for pod {name} to be scheduled");
        }

        let pod = pods.get(name).await?;
        if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
            println!("pod {name} scheduled on {node_name}");
            return Ok(node_name.clone());
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Confirm a pod stays Pending/Unschedulable for the given duration.
pub async fn wait_for_pod_unschedulable(
    client: Client,
    name: &str,
    observe_duration: Duration,
) -> Result<()> {
    let pods: Api<Pod> = Api::default_namespaced(client);
    let deadline = tokio::time::Instant::now() + observe_duration;

    loop {
        if tokio::time::Instant::now() > deadline {
            // Pod stayed unschedulable for the entire duration — success.
            return Ok(());
        }

        let pod = pods.get(name).await?;
        if pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .is_some()
        {
            anyhow::bail!("pod {name} was unexpectedly scheduled");
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// List all NodeRemovalRequests in the cluster.
pub async fn list_node_removal_requests(client: Client) -> Result<Vec<NodeRemovalRequest>> {
    let api: Api<NodeRemovalRequest> = Api::all(client);
    Ok(api
        .list(&ListParams::default())
        .await?
        .into_iter()
        .collect())
}
