use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec for a NodePool — declares which server types are available for scaling.
///
/// Each NodePool lists one or more server types that the autoscaler may provision.
/// Pods are matched to pools via the `growth.dev/pool` nodeSelector label.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(group = "growth.vettrdev.com", version = "v1alpha1", kind = "NodePool")]
#[kube(status = "NodePoolStatus")]
#[serde(rename_all = "camelCase")]
pub struct NodePoolSpec {
    /// Server types available in this pool, each with scaling limits.
    pub server_types: Vec<ServerTypeConfig>,
}

/// Configuration for a single server type within a NodePool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ServerTypeConfig {
    /// Must match an Offering's instance_type (e.g. "hetzner-cax11").
    pub name: String,
    /// Maximum number of nodes of this type the pool may provision.
    pub max: u32,
    /// Minimum number of nodes to keep warm. Enforcement is a follow-up.
    #[serde(default)]
    pub min: u32,
}

/// Status of a NodePool.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodePoolStatus {
    /// Number of active (Pending + Provisioning + Ready) NodeRequests owned by this pool.
    #[serde(default)]
    pub active_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    use kube::CustomResourceExt;

    #[test]
    fn crd_generates_valid_schema() {
        let crd = NodePool::crd();
        assert_eq!(
            crd.metadata.name.as_deref(),
            Some("nodepools.growth.vettrdev.com")
        );

        let spec = &crd.spec;
        assert_eq!(spec.group, "growth.vettrdev.com");
        assert_eq!(spec.names.kind, "NodePool");
        assert_eq!(spec.names.plural, "nodepools");
    }

    #[test]
    fn status_defaults_to_zero_active() {
        let status = NodePoolStatus::default();
        assert_eq!(status.active_count, 0);
    }

    #[test]
    fn min_defaults_to_zero() {
        let json = r#"{"name": "cx22", "max": 5}"#;
        let config: ServerTypeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "cx22");
        assert_eq!(config.max, 5);
        assert_eq!(config.min, 0);
    }

    #[test]
    fn spec_roundtrips_through_json() {
        let spec = NodePoolSpec {
            server_types: vec![
                ServerTypeConfig {
                    name: "cx22".to_string(),
                    max: 10,
                    min: 0,
                },
                ServerTypeConfig {
                    name: "cx32".to_string(),
                    max: 5,
                    min: 2,
                },
            ],
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: NodePoolSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.server_types.len(), 2);
        assert_eq!(back.server_types[0].name, "cx22");
        assert_eq!(back.server_types[0].max, 10);
        assert_eq!(back.server_types[1].min, 2);
    }

    #[test]
    fn full_node_pool_serialization() {
        let np = NodePool::new(
            "my-pool",
            NodePoolSpec {
                server_types: vec![ServerTypeConfig {
                    name: "cx22".to_string(),
                    max: 10,
                    min: 0,
                }],
            },
        );
        let json = serde_json::to_value(&np).unwrap();
        let server_types = &json["spec"]["serverTypes"];
        assert_eq!(server_types[0]["name"], "cx22");
        assert_eq!(server_types[0]["max"], 10);
    }
}
