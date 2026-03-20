use kube::api::{Api, ObjectMeta, PostParams};
use kube::{Client, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::offering::Resources;

/// Spec for a NodeRequest — a request to provision a single node.
///
/// NodeRequests track individual node provisioning through a state machine:
/// Pending → Provisioning → Ready | Unmet | Deprovisioning
///
/// They are owned by a NodePool and cleaned up via TTL once Ready or Unmet.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "growth.vettrdev.com",
    version = "v1alpha1",
    kind = "NodeRequest"
)]
#[kube(status = "NodeRequestStatus")]
#[serde(rename_all = "camelCase")]
pub struct NodeRequestSpec {
    /// The pre-determined name for this upcoming node.
    #[serde(rename = "nodeID")]
    pub node_id: String,
    /// The offering (instance type) to provision, e.g. "hetzner-cax11".
    pub target_offering: String,
    /// Provider location/region to provision in, e.g. "nbg1".
    pub location: String,
    /// Snapshot of the resources this offering provides, captured at creation time.
    pub resources: Resources,

}

/// Create a NodeRequest in Pending phase for a given pool and offering.
///
/// The name is generated as `{pool}-{uuid}` per the RFC naming convention.
/// An ownerReference is set pointing to the NodePool so that garbage collection
/// cleans up NodeRequests when the pool is deleted.
pub async fn create_node_request(
    client: Client,
    pool: &str,
    pool_uid: &str,
    spec: NodeRequestSpec,
) -> kube::Result<NodeRequest> {
    let api: Api<NodeRequest> = Api::all(client);
    let name = format!("{pool}-{}", uuid::Uuid::new_v4());
    let mut nr = NodeRequest::new(&name, spec);
    nr.metadata = ObjectMeta {
        name: Some(name.clone()),
        owner_references: Some(vec![
            k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                api_version: "growth.vettrdev.com/v1alpha1".to_string(),
                kind: "NodePool".to_string(),
                name: pool.to_string(),
                uid: pool_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            },
        ]),
        ..Default::default()
    };
    let created = api.create(&PostParams::default(), &nr).await?;
    info!(
        name = %name,
        pool = %pool,
        offering = %nr.spec.target_offering,
        node_id = %nr.spec.node_id,
        "created NodeRequest"
    );
    Ok(created)
}

impl NodeRequest {
    /// Returns the current phase, treating a missing status as Pending.
    pub fn phase(&self) -> NodeRequestPhase {
        self.status
            .as_ref()
            .map(|s| s.phase.clone())
            .unwrap_or_default()
    }
}

/// Phase of a NodeRequest through its lifecycle.
///
/// - `Pending` — initial state, waiting to be sent to the provider.
/// - `Provisioning` — provider accepted the request, node is being created.
/// - `Ready` — node joined the cluster successfully.
/// - `Unmet` — provider couldn't fulfil the request (no capacity). TTL-based cleanup.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum NodeRequestPhase {
    #[default]
    Pending,
    Provisioning,
    Ready,
    Unmet,
}

impl std::fmt::Display for NodeRequestPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Provisioning => write!(f, "Provisioning"),
            Self::Ready => write!(f, "Ready"),
            Self::Unmet => write!(f, "Unmet"),
        }
    }
}

/// A recorded event on a NodeRequest.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NodeRequestEvent {
    pub at: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Status of a NodeRequest.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct NodeRequestStatus {
    /// Current phase of the NodeRequest.
    #[serde(default)]
    pub phase: NodeRequestPhase,
    /// Event log.
    #[serde(default)]
    pub events: Vec<NodeRequestEvent>,
    /// Timestamp of the most recent phase transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
}
