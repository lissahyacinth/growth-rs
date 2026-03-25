use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::offering::InstanceType;

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
    kind = "NodeRemovalRequest",
    shortname = "nrr",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Node", "type": "string", "jsonPath": ".spec.nodeName"}"#,
    printcolumn = r#"{"name": "Attempts", "type": "integer", "jsonPath": ".status.removalAttempts"}"#
)]
#[kube(status = "NodeRemovalRequestStatus")]
#[serde(rename_all = "camelCase")]
pub struct NodeRemovalRequestSpec {
    /// The name of the Kubernetes Node to be removed.
    pub node_name: String,
    /// The NodePool this node belongs to.
    pub pool: String,
    /// The instance type of the node being removed.
    pub instance_type: InstanceType,
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
    pub last_transition_time: Option<Time>,
}
