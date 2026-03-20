use std::collections::BTreeMap;

/// Hetzner-specific instance configuration resolved from a HetznerNodeClass CRD.
///
/// Contains all the fields needed to create a Hetzner Cloud server that are
/// NOT part of the generic `InstanceConfig` (which only carries labels).
pub struct HetznerCreateConfig {
    /// Resolved user-data (cloud-init / Talos config) with all variables substituted.
    pub user_data: Option<String>,
    /// OS image for the server (e.g. "ubuntu-24.04").
    pub image: String,
    /// Hetzner SSH key names to install on the server.
    pub ssh_key_names: Vec<String>,
    /// Hetzner network IDs to attach to the server's private interface.
    pub network_ids: Vec<i64>,
    /// Hetzner firewall IDs to apply to the server.
    pub firewall_ids: Vec<i64>,
    /// Whether to enable IPv4 on the public interface.
    pub enable_ipv4: Option<bool>,
    /// Whether to enable IPv6 on the public interface.
    pub enable_ipv6: Option<bool>,
    /// Extra labels to apply on the Hetzner server (separate from K8s node labels).
    pub hetzner_labels: BTreeMap<String, String>,
}
