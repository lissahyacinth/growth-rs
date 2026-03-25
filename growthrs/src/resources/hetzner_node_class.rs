use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::resources::user_data::UserDataConfig;

/// Spec for a HetznerNodeClass — provider-specific instance configuration.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "growth.vettrdev.com",
    version = "v1alpha1",
    kind = "HetznerNodeClass",
    shortname = "hnc"
)]
#[serde(rename_all = "camelCase")]
pub struct HetznerNodeClassSpec {
    /// [OS image for the server](https://docs.hetzner.cloud/reference/cloud#tag/servers/create_server.body.image), e.g. "ubuntu-24.04"
    pub image: String,
    /// [Hetzner SSH key names](https://docs.hetzner.cloud/reference/cloud#tag/servers/create_server.body.ssh_keys) to install on the server.
    #[serde(default)]
    pub ssh_key_names: Vec<String>,
    /// [User-data template](https://docs.hetzner.cloud/reference/cloud#tag/servers/create_server.body.user_data) configuration with variable substitution.
    pub user_data: UserDataConfig,
    /// [Hetzner network IDs](https://docs.hetzner.cloud/reference/cloud#tag/servers/create_server.body.networks) to attach to the server's private interface.
    #[serde(default)]
    pub network_ids: Vec<i64>,
    /// [Hetzner firewall IDs](https://docs.hetzner.cloud/reference/cloud#tag/servers/create_server.body.firewalls) to apply to the server.
    // TODO: Do we want to abstract out networking into its own block?
    #[serde(default)]
    pub firewall_ids: Vec<i64>,
    /// Whether to enable IPv4 on the public interface.
    #[serde(default)]
    pub enable_ipv4: Option<bool>,
    /// Whether to enable IPv6 on the public interface.
    #[serde(default)]
    pub enable_ipv6: Option<bool>,
    /// Labels that will be [visible in Hetzner Cloud](https://docs.hetzner.cloud/reference/cloud#tag/servers/create_server.body.labels) for the resource. This will be in addition
    /// to the default labels that are added to all servers, {"managed-by": "growth", }, and
    /// labels from the NodePool this is attached to.
    #[serde(default)]
    pub hetzner_labels: BTreeMap<String, String>,
}
