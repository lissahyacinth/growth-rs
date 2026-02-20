use kube::api::{Api, PostParams};
use kube::{Client, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Spec for a NodeRequest — a request to provision a single node.
///
/// NodeRequests track individual node provisioning through a state machine:
/// Pending → Provisioning → Ready | Unmet
///
/// They are owned by a NodePool and cleaned up via TTL once Ready or Unmet.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "growth",
    version = "v1alpha1",
    kind = "NodeRequest",
    namespaced
)]
#[kube(status = "NodeRequestStatus")]
pub struct NodeRequestSpec {
    /// The offering (instance type) to provision, e.g. "hetzner-cax11".
    pub target_offering: String,
}

/// Create a NodeRequest in Pending phase for a given pool and offering.
///
/// The name is generated as `{pool}-{uuid}` per the RFC naming convention.
pub async fn create_node_request(
    client: Client,
    pool: &str,
    spec: NodeRequestSpec,
) -> kube::Result<NodeRequest> {
    let api: Api<NodeRequest> = Api::namespaced(client, "default");
    let name = format!("{pool}-{}", uuid::Uuid::new_v4());
    let nr = NodeRequest::new(&name, spec);
    let created = api.create(&PostParams::default(), &nr).await?;
    info!(name = %name, "created NodeRequest");
    Ok(created)
}

/// Phase of a NodeRequest through its lifecycle.
///
/// - `Pending` — initial state, waiting to be sent to the provider.
/// - `Provisioning` — provider accepted the request, node is being created.
/// - `Ready` — node joined the cluster successfully.
/// - `Unmet` — provider couldn't fulfil the request (no capacity). TTL-based cleanup.
/// - `Deprovisioning` — node failed readiness check, being torn down.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum NodeRequestPhase {
    #[default]
    Pending,
    Provisioning,
    Ready,
    Unmet,
    Deprovisioning,
}

impl std::fmt::Display for NodeRequestPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Provisioning => write!(f, "Provisioning"),
            Self::Ready => write!(f, "Ready"),
            Self::Unmet => write!(f, "Unmet"),
            Self::Deprovisioning => write!(f, "Deprovisioning"),
        }
    }
}

/// A timestamped event on a NodeRequest.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NodeRequestEvent {
    /// When the event occurred (RFC 3339 timestamp).
    pub at: String,
    /// Short name of the event, e.g. "nodeRequested", "nodeProvisioned".
    pub name: String,
    /// Optional reason/detail for failure events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Status of a NodeRequest.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct NodeRequestStatus {
    /// Current phase of the NodeRequest.
    #[serde(default)]
    pub phase: NodeRequestPhase,
    /// Prospective node ID, set when the provider accepts the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Ordered list of lifecycle events.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<NodeRequestEvent>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use kube::CustomResourceExt;

    #[test]
    fn crd_generates_valid_schema() {
        let crd = NodeRequest::crd();
        assert_eq!(crd.metadata.name.as_deref(), Some("noderequests.growth"));

        let spec = &crd.spec;
        assert_eq!(spec.group, "growth");
        assert_eq!(spec.names.kind, "NodeRequest");
        assert_eq!(spec.names.plural, "noderequests");
    }

    #[test]
    fn status_defaults_to_pending() {
        let status = NodeRequestStatus::default();
        assert_eq!(status.phase, NodeRequestPhase::Pending);
        assert!(status.node_id.is_none());
        assert!(status.events.is_empty());
    }

    #[test]
    fn phase_display() {
        assert_eq!(NodeRequestPhase::Pending.to_string(), "Pending");
        assert_eq!(NodeRequestPhase::Provisioning.to_string(), "Provisioning");
        assert_eq!(NodeRequestPhase::Ready.to_string(), "Ready");
        assert_eq!(NodeRequestPhase::Unmet.to_string(), "Unmet");
        assert_eq!(
            NodeRequestPhase::Deprovisioning.to_string(),
            "Deprovisioning"
        );
    }

    #[test]
    fn spec_roundtrips_through_json() {
        let spec = NodeRequestSpec {
            target_offering: "hetzner-cax11".to_string(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: NodeRequestSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.target_offering, "hetzner-cax11");
    }

    #[test]
    fn status_roundtrips_through_json() {
        let status = NodeRequestStatus {
            phase: NodeRequestPhase::Provisioning,
            node_id: Some("node-abc".to_string()),
            events: vec![NodeRequestEvent {
                at: "2026-01-01T21:07:52Z".to_string(),
                name: "nodeRequested".to_string(),
                reason: None,
            }],
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: NodeRequestStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.phase, NodeRequestPhase::Provisioning);
        assert_eq!(back.node_id.as_deref(), Some("node-abc"));
        assert_eq!(back.events.len(), 1);
        assert_eq!(back.events[0].name, "nodeRequested");
    }
}
