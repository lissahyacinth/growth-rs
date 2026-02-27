use kube::api::{Api, ObjectMeta, PostParams};
use kube::{Client, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::offering::Resources;

/// Spec for a NodeRequest — a request to provision a single node.
///
/// NodeRequests track individual node provisioning through a state machine:
/// Pending → Provisioning → Ready | Unmet
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
    /// Snapshot of the resources this offering provides, captured at creation time.
    pub resources: Resources,
    /// UIDs of the pods this NodeRequest was created to satisfy.
    /// Used to prevent duplicate provisioning across reconcile loops.
    #[serde(default)]
    pub claimed_pod_uids: Vec<String>,
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
        claimed_pods = nr.spec.claimed_pod_uids.len(),
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

/// A recorded event on a NodeRequest.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NodeRequestEvent {
    pub at: String,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::offering::Resources;
    use kube::CustomResourceExt;

    #[test]
    fn crd_generates_valid_schema() {
        let crd = NodeRequest::crd();
        assert_eq!(
            crd.metadata.name.as_deref(),
            Some("noderequests.growth.vettrdev.com")
        );

        let spec = &crd.spec;
        assert_eq!(spec.group, "growth.vettrdev.com");
        assert_eq!(spec.names.kind, "NodeRequest");
        assert_eq!(spec.names.plural, "noderequests");
    }

    #[test]
    fn status_defaults_to_pending() {
        let status = NodeRequestStatus::default();
        assert_eq!(status.phase, NodeRequestPhase::Pending);
        assert!(status.events.is_empty());
    }

    #[test]
    fn phase_display() {
        assert_eq!(NodeRequestPhase::Pending.to_string(), "Pending");
        assert_eq!(NodeRequestPhase::Provisioning.to_string(), "Provisioning");
        assert_eq!(NodeRequestPhase::Ready.to_string(), "Ready");
        assert_eq!(NodeRequestPhase::Unmet.to_string(), "Unmet");
    }

    #[test]
    fn spec_roundtrips_through_json() {
        let spec = NodeRequestSpec {
            target_offering: "hetzner-cax11".to_string(),
            resources: Resources {
                cpu: 2,
                memory_mib: 4096,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            node_id: "example".to_string(),
            claimed_pod_uids: vec!["uid-1".to_string(), "uid-2".to_string()],
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: NodeRequestSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.target_offering, "hetzner-cax11");
        assert_eq!(back.resources.cpu, 2);
        assert_eq!(back.resources.memory_mib, 4096);
        assert_eq!(back.claimed_pod_uids, vec!["uid-1", "uid-2"]);
    }

    #[test]
    fn full_node_request_serialization() {
        let nr = NodeRequest::new(
            "test-nr",
            NodeRequestSpec {
                target_offering: "cx22".to_string(),
                resources: Resources {
                    cpu: 2,
                    memory_mib: 4096,
                    ephemeral_storage_gib: None,
                    gpu: 0,
                    gpu_model: None,
                },
                node_id: "example".to_string(),
                claimed_pod_uids: vec![],
            },
        );
        let json = serde_json::to_value(&nr).unwrap();
        let resources = &json["spec"]["resources"];
        assert_eq!(resources["memoryMib"], 4096);
        assert_eq!(resources["cpu"], 2);
    }

    #[test]
    fn status_roundtrips_through_json() {
        let status = NodeRequestStatus {
            phase: NodeRequestPhase::Provisioning,
            events: vec![NodeRequestEvent {
                at: "2026-01-01T21:07:52Z".to_string(),
                name: "nodeRequested".to_string(),
                reason: None,
            }],
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: NodeRequestStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.phase, NodeRequestPhase::Provisioning);
        assert_eq!(back.events.len(), 1);
        assert_eq!(back.events[0].name, "nodeRequested");
    }
}
