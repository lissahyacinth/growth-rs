use kube::api::{Api, ObjectMeta, PostParams};
use kube::{Client, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json;
use tracing::info;

/// Spec for a NodeRemovalRequest — a request to remove a single node.
///
/// NodeRemovalRequests track individual node removal through a state machine:
/// Pending → Deprovisioning | CouldNotRemove
///
/// They are owned by the Node being removed (when one exists) and cleaned up
/// when the Node is deleted. For failed scale-up nodes where no K8s Node object
/// exists, owner references are omitted.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "growth.vettrdev.com",
    version = "v1alpha1",
    kind = "NodeRemovalRequest"
)]
#[kube(status = "NodeRemovalRequestStatus")]
#[serde(rename_all = "camelCase")]
pub struct NodeRemovalRequestSpec {
    /// The name of the Kubernetes Node to be removed.
    pub node_name: String,
    /// The NodePool this node belongs to.
    pub pool: String,
    /// The instance type of the node being removed.
    pub instance_type: String,
}

/// Create a NodeRemovalRequest for a given node.
///
/// The name is generated as `nrr-{node_name}`.
/// When `node_uid` is `Some`, an ownerReference is set pointing to the Node so
/// that garbage collection cleans up the NRR if the Node is deleted externally.
/// When `node_uid` is `None` (e.g. failed scale-up where no K8s Node exists),
/// owner references are omitted.
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
) -> kube::Result<NodeRemovalRequest> {
    let api: Api<NodeRemovalRequest> = Api::all(client.clone());
    let name = format!("nrr-{node_name}");
    let spec = NodeRemovalRequestSpec {
        node_name: node_name.to_string(),
        pool: pool.to_string(),
        instance_type: instance_type.to_string(),
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
        ..Default::default()
    };
    let created = api.create(&PostParams::default(), &nrr).await?;

    if initial_phase != NodeRemovalRequestPhase::Pending {
        let status = NodeRemovalRequestStatus {
            phase: initial_phase.clone(),
            removal_attempts: if initial_phase == NodeRemovalRequestPhase::Deprovisioning { 1 } else { 0 },
            last_transition_time: Some(
                k8s_openapi::jiff::Timestamp::now().to_string(),
            ),
        };
        let patch = serde_json::json!({ "status": status });
        api.patch_status(
            &name,
            &kube::api::PatchParams::apply("growthrs"),
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

impl NodeRemovalRequest {
    /// Returns the current phase, treating a missing status as Pending.
    pub fn phase(&self) -> NodeRemovalRequestPhase {
        self.status
            .as_ref()
            .map(|s| s.phase.clone())
            .unwrap_or_default()
    }
}

/// Phase of a NodeRemovalRequest through its lifecycle.
///
/// - `Pending` — initial state, cooling-off period before deprovisioning.
/// - `Deprovisioning` — taint applied, provider.delete() called, polling for completion.
/// - `CouldNotRemove` — terminal failure after max retries.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum NodeRemovalRequestPhase {
    #[default]
    Pending,
    Deprovisioning,
    CouldNotRemove,
}

impl std::fmt::Display for NodeRemovalRequestPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Deprovisioning => write!(f, "Deprovisioning"),
            Self::CouldNotRemove => write!(f, "CouldNotRemove"),
        }
    }
}

/// Status of a NodeRemovalRequest.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeRemovalRequestStatus {
    /// Current phase of the NodeRemovalRequest.
    #[serde(default)]
    pub phase: NodeRemovalRequestPhase,
    /// Number of times removal has been attempted.
    #[serde(default)]
    pub removal_attempts: u32,
    /// Timestamp of the last phase transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

