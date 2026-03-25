use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::jiff::Timestamp;
use kube::Client;
use kube::api::{Api, DeleteParams, ObjectMeta, PatchParams, PostParams};
use serde_json;
use tracing::info;

use crate::offering::{
    DELETE_AT_ANNOTATION, InstanceType, NRR_FINALIZER, REMOVAL_CANDIDATE_ANNOTATION,
    SCALE_DOWN_TAINT_KEY,
};
use crate::resources::node_removal_request::{
    NodeRemovalRequest, NodeRemovalRequestPhase, NodeRemovalRequestSpec, NodeRemovalRequestStatus,
};

/// Construct a synthetic kube API error for fault injection.
#[allow(dead_code)]
fn injected_error(message: &str) -> kube::Error {
    kube::Error::Api(
        kube::core::Status::failure(message, "InternalError")
            .with_code(500)
            .boxed(),
    )
}

/// Apply the `growth.vettrdev.com/scale-down: NoSchedule` taint to a node.
pub async fn apply_scale_down_taint(client: &Client, node_name: &str) -> Result<(), kube::Error> {
    let api: Api<Node> = Api::all(client.clone());
    let patch = serde_json::json!({
        "spec": {
            "taints": [{
                "key": SCALE_DOWN_TAINT_KEY,
                "value": "",
                "effect": "NoSchedule"
            }]
        }
    });
    api.patch(
        node_name,
        &PatchParams::apply("growthrs"),
        &kube::api::Patch::Strategic(patch),
    )
    .await?;
    Ok(())
}

/// Add the removal-candidate annotation to a node.
pub async fn annotate_removal_candidate(
    client: &Client,
    node_name: &str,
    nrr_name: &str,
) -> Result<(), kube::Error> {
    let api: Api<Node> = Api::all(client.clone());
    let patch = serde_json::json!({
        "metadata": {
            "annotations": {
                REMOVAL_CANDIDATE_ANNOTATION: nrr_name
            }
        }
    });
    api.patch(
        node_name,
        &PatchParams::apply("growthrs"),
        &kube::api::Patch::Merge(patch),
    )
    .await?;
    Ok(())
}

/// Add the delete-at annotation to a node.
pub async fn annotate_delete_at(
    client: &Client,
    node_name: &str,
    delete_at_rfc3339: &str,
) -> Result<(), kube::Error> {
    let api: Api<Node> = Api::all(client.clone());
    let patch = serde_json::json!({
        "metadata": {
            "annotations": {
                DELETE_AT_ANNOTATION: delete_at_rfc3339
            }
        }
    });
    api.patch(
        node_name,
        &PatchParams::apply("growthrs"),
        &kube::api::Patch::Merge(patch),
    )
    .await?;
    Ok(())
}

/// Remove the delete-at annotation from a node.
pub async fn remove_delete_at_annotation(
    client: &Client,
    node_name: &str,
) -> Result<(), kube::Error> {
    fail::fail_point!("cancel_nrr_remove_delete_at", |_| {
        Err(injected_error("injected: remove_delete_at_annotation"))
    });
    let api: Api<Node> = Api::all(client.clone());
    let patch = serde_json::json!({
        "metadata": {
            "annotations": {
                DELETE_AT_ANNOTATION: null
            }
        }
    });
    api.patch(
        node_name,
        &PatchParams::apply("growthrs"),
        &kube::api::Patch::Merge(patch),
    )
    .await?;
    Ok(())
}

/// Remove the removal-candidate annotation from a node.
pub async fn remove_removal_candidate_annotation(
    client: &Client,
    node_name: &str,
) -> Result<(), kube::Error> {
    fail::fail_point!("cancel_nrr_remove_candidate", |_| {
        Err(injected_error(
            "injected: remove_removal_candidate_annotation",
        ))
    });
    let api: Api<Node> = Api::all(client.clone());
    let patch = serde_json::json!({
        "metadata": {
            "annotations": {
                REMOVAL_CANDIDATE_ANNOTATION: null
            }
        }
    });
    api.patch(
        node_name,
        &PatchParams::apply("growthrs"),
        &kube::api::Patch::Merge(patch),
    )
    .await?;
    Ok(())
}

/// Update the phase (and optionally removal_attempts) of a NodeRemovalRequest via SSA Merge Patch.
pub async fn update_nrr_phase(
    client: &Client,
    name: &str,
    phase: NodeRemovalRequestPhase,
    removal_attempts: Option<u32>,
    now: Timestamp,
) -> Result<(), kube::Error> {
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    let status = NodeRemovalRequestStatus {
        phase,
        removal_attempts: removal_attempts.unwrap_or(0),
        last_transition_time: Some(Time(now)),
    };
    let patch = serde_json::json!({ "status": status });
    api.patch_status(
        name,
        &PatchParams::apply("growthrs"),
        &kube::api::Patch::Merge(patch),
    )
    .await?;
    Ok(())
}

/// Delete a Kubernetes Node object.
pub async fn delete_kubernetes_node(client: &Client, node_name: &str) -> Result<(), kube::Error> {
    let api: Api<Node> = Api::all(client.clone());
    match api.delete(node_name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref resp)) if resp.code == 404 => Ok(()),
        Err(e) => Err(e),
    }
}

/// Remove the provider-cleanup finalizer from a NodeRemovalRequest.
///
/// Reads the current object, filters out the finalizer, and patches. This is
/// safe if the finalizer is already absent (the patch is a no-op).
pub async fn remove_nrr_finalizer(client: &Client, name: &str) -> Result<(), kube::Error> {
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    let nrr = api.get(name).await?;
    let finalizers: Vec<String> = nrr
        .metadata
        .finalizers
        .unwrap_or_default()
        .into_iter()
        .filter(|f| f != NRR_FINALIZER)
        .collect();
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": if finalizers.is_empty() { serde_json::Value::Null } else { serde_json::json!(finalizers) }
        }
    });
    api.patch(
        name,
        &PatchParams::apply("growthrs"),
        &kube::api::Patch::Merge(patch),
    )
    .await?;
    Ok(())
}

/// Create a NodeRemovalRequest for a given node.
///
/// The name is generated as `nrr-{node_name}`.
/// When `node_uid` is `Some`, an ownerReference is set pointing to the Node so
/// that garbage collection cleans up the NRR if the Node is deleted externally.
/// When `node_uid` is `None` (e.g. failed scale-up where no K8s Node exists),
/// owner references are omitted.
///
/// A finalizer (`growth.vettrdev.com/provider-cleanup`) is always added to
/// guarantee that `provider.delete()` runs before the NRR can be removed by
/// garbage collection or explicit deletion.
///
/// If `initial_phase` is not `Pending`, a status patch is applied immediately
/// after creation (K8s ignores `.status` in POST for resources with a status
/// subresource).
pub async fn create_node_removal_request(
    client: Client,
    node_name: &str,
    node_uid: Option<&str>,
    pool: &str,
    instance_type: &str,
    initial_phase: NodeRemovalRequestPhase,
    now: Timestamp,
) -> kube::Result<NodeRemovalRequest> {
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    let name = format!("nrr-{node_name}");
    let spec = NodeRemovalRequestSpec {
        node_name: node_name.to_string(),
        pool: pool.to_string(),
        instance_type: InstanceType(instance_type.to_string()),
    };
    let mut nrr = NodeRemovalRequest::new(&name, spec);
    let owner_references = node_uid.map(|uid| {
        vec![
            k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                name: node_name.to_string(),
                uid: uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(false),
            },
        ]
    });
    nrr.metadata = ObjectMeta {
        name: Some(name.clone()),
        owner_references,
        finalizers: Some(vec![NRR_FINALIZER.to_string()]),
        ..Default::default()
    };
    let created = api.create(&PostParams::default(), &nrr).await?;

    if initial_phase != NodeRemovalRequestPhase::Pending {
        let status = NodeRemovalRequestStatus {
            phase: initial_phase.clone(),
            removal_attempts: if initial_phase == NodeRemovalRequestPhase::Deprovisioning {
                1
            } else {
                0
            },
            last_transition_time: Some(Time(now)),
        };
        let patch = serde_json::json!({ "status": status });
        api.patch_status(
            &name,
            &PatchParams::apply("growthrs"),
            &kube::api::Patch::Merge(patch),
        )
        .await?;
    }

    info!(
        name = %name,
        node = %node_name,
        pool = %pool,
        instance_type = %instance_type,
        phase = %initial_phase,
        "created NodeRemovalRequest"
    );
    Ok(created)
}

/// Delete a NodeRemovalRequest.
pub async fn delete_nrr(client: &Client, name: &str) -> Result<(), kube::Error> {
    fail::fail_point!("cancel_nrr_delete_nrr", |_| {
        Err(injected_error("injected: delete_nrr"))
    });
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref resp)) if resp.code == 404 => Ok(()),
        Err(e) => Err(e),
    }
}
