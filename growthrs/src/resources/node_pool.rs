use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Reference to a provider-specific NodeClass (e.g. HetznerNodeClass).
///
/// `group` and `kind` identify which CRD type to look up. Defaults to
/// `growth.vettrdev.com` / `HetznerNodeClass` so existing manifests that
/// only specify `name` continue to work.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeClassRef {
    /// API group of the NodeClass resource.
    #[serde(default = "default_node_class_group")]
    pub group: String,
    /// Kind of the NodeClass resource.
    #[serde(default = "default_node_class_kind")]
    pub kind: String,
    /// Name of the NodeClass resource.
    pub name: String,
}

fn default_node_class_group() -> String {
    "growth.vettrdev.com".to_string()
}

fn default_node_class_kind() -> String {
    "HetznerNodeClass".to_string()
}

/// Spec for a NodePool — declares which server types are available for scaling.
///
/// Each NodePool lists one or more server types that the autoscaler may provision.
/// Pods are matched to pools via the `growth.vettrdev.com/pool` nodeSelector label.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(group = "growth.vettrdev.com", version = "v1alpha1", kind = "NodePool", shortname = "np")]
#[serde(rename_all = "camelCase")]
pub struct NodePoolSpec {
    /// Server types available in this pool, each with scaling limits.
    pub server_types: Vec<ServerTypeConfig>,
    /// Labels applied to every node provisioned from this pool.
    /// Commonly used for topology labels (e.g. `topology.kubernetes.io/zone`).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// If set, only offerings matching at least one entry are eligible.
    /// Each entry scopes allowed zones to a specific region.
    /// `None` means all regions and zones.
    #[serde(default)]
    pub locations: Option<Vec<LocationConstraint>>,
    /// Optional reference to a provider-specific NodeClass for instance config.
    #[serde(default)]
    pub node_class_ref: Option<NodeClassRef>,
}

/// A region with optional zone restrictions.
///
/// An offering matches if its region equals `region` AND either `zones` is
/// `None` (all zones in that region) or the offering's zone is in the list.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LocationConstraint {
    pub region: String,
    /// If `None`, all zones in this region are allowed.
    #[serde(default)]
    pub zones: Option<Vec<String>>,
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

