use std::collections::BTreeMap;

use anyhow::Result;
use k8s_openapi::api::core::v1::{Container, Pod, PodSpec, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{DeleteParams, ListParams, ObjectMeta, PostParams};
use kube::{Api, Client};

use tracing::{debug, info};

use crate::offering::PodResources;
use crate::optimiser::solve;
use crate::providers::provider::Provider;

/// Retrieve pods that are unschedulable by virtue of unmet resources
pub async fn get_unschedulable_pods(client: Client) -> Result<Vec<Pod>> {
    let pods: Api<Pod> = Api::all(client.clone());
    let lp = ListParams::default().fields("status.phase=Pending");
    Ok(pods
        .list(&lp)
        .await?
        .into_iter()
        .filter(|pod| {
            let is_unschedulable = pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conditions| {
                    conditions.iter().any(|c| {
                        c.type_ == "PodScheduled"
                            && c.status == "False"
                            && c.reason.as_deref() == Some("Unschedulable")
                    })
                })
                .unwrap_or(false);

            // DaemonSet pods target every node, including nodes that cannot run them. Nevertheless - we don't need to scale anything to satisfy them.
            let is_daemonset = pod
                .metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| r.kind == "DaemonSet"))
                .unwrap_or(false);

            is_unschedulable && !is_daemonset
        })
        .collect())
}

/// Controller Pattern for adding/removing Nodes
///
/// Can only accept a single provider, no known clusters combine providers.
pub async fn controller_loop(client: Client, provider: &Provider) -> Result<()> {
    let unschedulable_pods = get_unschedulable_pods(client).await?;
    info!(count = unschedulable_pods.len(), "found unschedulable pods");
    // Find appropriate nodes from offerings
    let offerings = provider.offerings().await;
    // Parse resource demands from all pods
    let demands: Vec<_> = unschedulable_pods
        .iter()
        .map(PodResources::from_pod)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    // Filter offerings down to what can satisfy any pod
    let suitable_offerings: Vec<_> = offerings
        .into_iter()
        .filter(|offering| {
            demands
                .iter()
                .any(|demand| offering.satisfies(&demand.resources))
        })
        .collect();
    info!(
        count = suitable_offerings.len(),
        "matched suitable offerings"
    );
    for offering in &suitable_offerings {
        debug!(instance_type = %offering.instance_type.0, cpu = offering.resources.cpu, memory_mib = offering.resources.memory_mib, cost_per_hour = offering.cost_per_hour, "suitable offering");
    }
    // TODO: Match up offerings against `NodeGroupWithPriority` - which ensures users decide what we actually request.
    // TODO: Sum existing capacity from Nodes in `Pending`/`Provisioning` State.
    // TODO: Gang Scheduling support?

    solve(demands.as_slice(), suitable_offerings.as_slice()).unwrap();
    Ok(())
}

/// Create a test pod with the given resource requests.
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
