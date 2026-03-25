use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::offering::{InstanceType, Region, Resources};

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
    kind = "NodeRequest",
    shortname = "nr",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Offering", "type": "string", "jsonPath": ".spec.targetOffering"}"#
)]
#[kube(status = "NodeRequestStatus")]
#[serde(rename_all = "camelCase")]
pub struct NodeRequestSpec {
    /// The pre-determined name for this upcoming node.
    #[serde(rename = "nodeID")]
    pub node_id: String,
    /// The offering (instance type) to provision, e.g. "hetzner-cax11".
    pub target_offering: InstanceType,
    /// Provider location/region to provision in, e.g. "nbg1".
    pub location: Region,
    /// Snapshot of the resources this offering provides, captured at creation time.
    pub resources: Resources,
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
#[serde(rename_all = "camelCase")]
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
