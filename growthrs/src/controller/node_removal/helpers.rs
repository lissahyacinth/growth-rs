use k8s_openapi::api::core::v1::{Node, Taint};
use kube::api::{Api, DeleteParams, PatchParams};
use kube::Client;

use crate::crds::node_removal_request::{
    NodeRemovalRequest, NodeRemovalRequestPhase, NodeRemovalRequestStatus,
};
use crate::offering::{DELETE_AT_ANNOTATION, REMOVAL_CANDIDATE_ANNOTATION, SCALE_DOWN_TAINT_KEY};

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

/// Remove the `growth.vettrdev.com/scale-down` taint from a node.
pub async fn remove_scale_down_taint(
    client: &Client,
    node_name: &str,
) -> Result<(), kube::Error> {
    let api: Api<Node> = Api::all(client.clone());
    let node = api.get(node_name).await?;
    let taints: Vec<Taint> = node
        .spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|t| {
            t.iter()
                .filter(|t| t.key != SCALE_DOWN_TAINT_KEY)
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    let patch = serde_json::json!({
        "spec": {
            "taints": if taints.is_empty() { serde_json::Value::Null } else { serde_json::to_value(&taints).unwrap() }
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
) -> Result<(), kube::Error> {
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    let status = NodeRemovalRequestStatus {
        phase,
        removal_attempts: removal_attempts.unwrap_or(0),
        last_transition_time: Some(
            k8s_openapi::jiff::Timestamp::now()
                .to_string(),
        ),
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

/// Delete a NodeRemovalRequest.
pub async fn delete_nrr(client: &Client, name: &str) -> Result<(), kube::Error> {
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref resp)) if resp.code == 404 => Ok(()),
        Err(e) => Err(e),
    }
}
