use std::collections::HashMap;

use hcloud::apis::configuration::Configuration;
use hcloud::apis::server_types_api::{self, ListServerTypesParams};
use hcloud::apis::servers_api::{self, CreateServerParams, DeleteServerParams, ListServersParams};
use hcloud::models::{
    CreateServerRequest, CreateServerRequestFirewalls, CreateServerRequestPublicNet, Server,
    ServerType,
};
use tracing::{debug, error, info, warn};

use crate::offering::{InstanceType, Location, Offering, Region, Resources};
use crate::providers::{provider::{InstanceConfig, NodeId, ProviderError, ProviderStatus}};
use crate::providers::hetzner::config::HetznerCreateConfig;
pub mod config;


pub struct HetznerProvider {
    config: Configuration,
}

impl HetznerProvider {
    pub fn new(token: String) -> Self {
        let mut config = Configuration::new();
        config.bearer_access_token = Some(token);
        Self { config }
    }
}

/// Convert a Hetzner `ServerType` into one `Offering` per non-deprecated location.
pub fn convert_server_type(st: &ServerType) -> Vec<Offering> {
    if st.deprecated == Some(true) {
        return Vec::new();
    }
    if let Some(Some(ref _dep)) = st.deprecation {
        return Vec::new();
    }

    let cpu = st.cores as u32;
    let memory_mib = (st.memory * 1024.0) as u32;
    let disk_gib = st.disk as u32;

    // Build a location→price map from the prices array.
    let price_map: HashMap<&str, f64> = st
        .prices
        .iter()
        .filter_map(|p| {
            p.price_hourly
                .gross
                .parse::<f64>()
                .ok()
                .map(|cost| (p.location.as_str(), cost))
        })
        .collect();

    st.locations
        .iter()
        .filter(|loc| {
            // Skip locations that are themselves deprecated.
            loc.deprecation.is_none()
        })
        .map(|loc| {
            let cost = price_map.get(loc.name.as_str()).copied().unwrap_or(0.0);
            Offering {
                instance_type: InstanceType(st.name.clone()),
                resources: Resources {
                    cpu,
                    memory_mib,
                    ephemeral_storage_gib: Some(disk_gib),
                    gpu: 0,
                    gpu_model: None,
                },
                cost_per_hour: cost,
                location: Location {
                    region: Region(loc.name.clone()),
                    zone: None,
                },
            }
        })
        .collect()
}

impl From<&Server> for ProviderStatus {
    fn from(value: &Server) -> Self {
        match value.status {
            hcloud::models::server::Status::Initializing
            | hcloud::models::server::Status::Starting
            | hcloud::models::server::Status::Rebuilding
            | hcloud::models::server::Status::Migrating => ProviderStatus::Creating,
            hcloud::models::server::Status::Running => ProviderStatus::Running,
            hcloud::models::server::Status::Deleting | hcloud::models::server::Status::Stopping => {
                ProviderStatus::Removing
            }
            hcloud::models::server::Status::Off | hcloud::models::server::Status::Unknown => {
                ProviderStatus::NotFound
            }
        }
    }
}

/// Map a Hetzner `Server` to a `ProviderStatus`.
fn map_server_status(server: &Server) -> ProviderStatus {
    ProviderStatus::from(server)
}

impl HetznerProvider {
    pub async fn offerings(&self) -> Vec<Offering> {
        let mut all_offerings = Vec::new();
        let mut page = 1i64;

        loop {
            let params = ListServerTypesParams {
                name: None,
                page: Some(page),
                per_page: Some(50),
            };

            match server_types_api::list_server_types(&self.config, params).await {
                Ok(resp) => {
                    if resp.server_types.is_empty() {
                        break;
                    }
                    for st in &resp.server_types {
                        all_offerings.extend(convert_server_type(st));
                    }
                    // Check if we got a full page; if not, we're done.
                    if resp.server_types.len() < 50 {
                        break;
                    }
                    page += 1;
                }
                Err(e) => {
                    warn!(error = %e, page, "failed to list server types from Hetzner API");
                    break;
                }
            }
        }

        info!(count = all_offerings.len(), "fetched Hetzner offerings");
        all_offerings
    }

    // Helper function for finding a server for deletion etc.
    async fn resolve_server_by_name(&self, name: &str) -> Result<Option<Server>, ProviderError> {
        let params = ListServersParams {
            name: Some(name.to_string()),
            ..Default::default()
        };

        let resp = servers_api::list_servers(&self.config, params)
            .await
            .map_err(|e| ProviderError::Internal(anyhow::anyhow!("list_servers failed: {e}")))?;

        Ok(resp.servers.into_iter().next())
    }

    pub async fn create(
        &self,
        node_id: String,
        offering: &Offering,
        config: &InstanceConfig,
        hetzner_config: &HetznerCreateConfig,
    ) -> Result<NodeId, ProviderError> {
        let image = hetzner_config.image.clone();

        let mut labels: HashMap<String, String> = hetzner_config
            .hetzner_labels
            .iter()
            .chain(config.labels.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        labels.insert("managed-by".to_string(), "growth".to_string());

        info!(
            node_id = %node_id,
            instance_type = %offering.instance_type,
            location = %offering.location.region.0,
            image = %image,
            "creating Hetzner server"
        );

        let request = CreateServerRequest {
            image,
            name: node_id.clone(),
            server_type: offering.instance_type.0.clone(),
            location: Some(offering.location.region.0.clone()),
            ssh_keys: if hetzner_config.ssh_key_names.is_empty() {
                None
            } else {
                Some(hetzner_config.ssh_key_names.clone())
            },
            user_data: hetzner_config.user_data.clone(),
            labels: Some(labels),
            automount: None,
            datacenter: None,
            firewalls: if hetzner_config.firewall_ids.is_empty() {
                None
            } else {
                Some(
                    hetzner_config
                        .firewall_ids
                        .iter()
                        .map(|&id| CreateServerRequestFirewalls { firewall: id })
                        .collect(),
                )
            },
            networks: if hetzner_config.network_ids.is_empty() {
                None
            } else {
                Some(hetzner_config.network_ids.clone())
            },
            placement_group: None,
            public_net: if hetzner_config.enable_ipv4.is_some()
                || hetzner_config.enable_ipv6.is_some()
            {
                Some(Box::new(CreateServerRequestPublicNet {
                    enable_ipv4: hetzner_config.enable_ipv4,
                    enable_ipv6: hetzner_config.enable_ipv6,
                    ipv4: None,
                    ipv6: None,
                }))
            } else {
                None
            },
            start_after_create: Some(true),
            volumes: None,
        };

        debug!(
            name = %request.name,
            server_type = %request.server_type,
            image = %request.image,
            location = ?request.location,
            // This is KeyID, so it's safe to show.
            ssh_keys = ?request.ssh_keys,
            networks = ?request.networks,
            firewalls = ?request.firewalls,
            labels = ?request.labels,
            has_user_data = request.user_data.is_some(),
            "CreateServerRequest"
        );

        let params = CreateServerParams {
            create_server_request: request,
        };

        match servers_api::create_server(&self.config, params).await {
            Ok(resp) => {
                debug!(
                    node_id = %node_id,
                    server_id = resp.server.id,
                    "Hetzner server created"
                );
                Ok(NodeId(node_id))
            }
            Err(hcloud::apis::Error::ResponseError(ref content)) => {
                let body = &content.content;
                if content.status.as_u16() == 409 && body.contains("uniqueness_error") {
                    // Server already exists with this name — treat as idempotent success.
                    info!(node_id = %node_id, "server already exists (409 uniqueness), treating as success");
                    Ok(NodeId(node_id))
                } else if body.contains("resource_limit_exceeded") {
                    error!("Resource limits exceeded for {} in {:?}", offering.instance_type, offering.location.region.0);
                    Err(ProviderError::OfferingUnavailable(format!(
                        "resource limit exceeded for {}",
                        offering.instance_type
                    )))
                } else if body.contains("resource_unavailable") {
                    Err(ProviderError::OfferingUnavailable(format!(
                        "{} unavailable in {} (location may be disabled by Hetzner)",
                        offering.instance_type, offering.location.region.0
                    )))
                } else {
                    Err(ProviderError::CreationFailed {
                        message: format!("Hetzner API error {}: {body}", content.status),
                    })
                }
            }
            Err(e) => Err(ProviderError::CreationFailed {
                message: format!("Hetzner create_server failed: {e}"),
            }),
        }
    }

    pub async fn delete(&self, node_id: &NodeId) -> Result<(), ProviderError> {
        info!(node_id = %node_id.0, "deleting Hetzner server");

        let server = self.resolve_server_by_name(&node_id.0).await?;
        let Some(server) = server else {
            debug!(node_id = %node_id.0, "server not found, treating as already deleted");
            return Ok(());
        };

        let params = DeleteServerParams { id: server.id };
        servers_api::delete_server(&self.config, params)
            .await
            .map_err(|e| ProviderError::DeletionFailed {
                message: format!("Hetzner delete_server failed: {e}"),
            })?;

        debug!(node_id = %node_id.0, server_id = server.id, "Hetzner server deleted");
        Ok(())
    }

    pub async fn status(&self, node_id: &NodeId) -> Result<ProviderStatus, ProviderError> {
        let server = self.resolve_server_by_name(&node_id.0).await?;
        match server {
            Some(s) => Ok(map_server_status(&s)),
            None => Ok(ProviderStatus::NotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hcloud::models::{Price, PricePerTime, ServerTypeLocation};

    fn make_price(gross: &str) -> Box<Price> {
        Box::new(Price::new(gross.to_string(), gross.to_string()))
    }

    fn make_server_type(name: &str, cores: i32, memory: f64, disk: f64) -> ServerType {
        ServerType {
            name: name.to_string(),
            cores,
            memory,
            disk,
            id: 1,
            description: "test".to_string(),
            deprecated: Some(false),
            deprecation: None,
            architecture: hcloud::models::Architecture::X86,
            cpu_type: hcloud::models::server_type::CpuType::Shared,
            storage_type: hcloud::models::server_type::StorageType::Local,
            prices: vec![PricePerTime {
                location: "fsn1".to_string(),
                price_hourly: make_price("0.0066"),
                price_monthly: make_price("3.49"),
                included_traffic: 0,
                price_per_tb_traffic: make_price("1.00"),
            }],
            locations: vec![ServerTypeLocation {
                id: 1,
                name: "fsn1".to_string(),
                deprecation: None,
            }],
            category: None,
        }
    }

    #[test]
    fn convert_server_type_basic() {
        let st = make_server_type("cpx22", 2, 4.0, 40.0);
        let offerings = convert_server_type(&st);
        assert_eq!(offerings.len(), 1);
        let o = &offerings[0];
        assert_eq!(o.instance_type.0, "cpx22");
        assert_eq!(o.resources.cpu, 2);
        assert_eq!(o.resources.memory_mib, 4096);
        assert_eq!(o.resources.ephemeral_storage_gib, Some(40));
        assert_eq!(o.location.region.0, "fsn1");
        assert!(o.location.zone.is_none());
        assert!((o.cost_per_hour - 0.0066).abs() < 0.0001);
    }

    #[test]
    fn convert_server_type_multiple_locations() {
        let mut st = make_server_type("cx32", 4, 8.0, 80.0);
        st.locations.push(ServerTypeLocation {
            id: 2,
            name: "nbg1".to_string(),
            deprecation: None,
        });
        st.prices.push(PricePerTime {
            location: "nbg1".to_string(),
            price_hourly: make_price("0.0070"),
            price_monthly: make_price("3.99"),
            included_traffic: 0,
            price_per_tb_traffic: make_price("1.00"),
        });

        let offerings = convert_server_type(&st);
        assert_eq!(offerings.len(), 2);
        assert_eq!(offerings[0].location.region.0, "fsn1");
        assert_eq!(offerings[1].location.region.0, "nbg1");
    }

    #[test]
    fn convert_server_type_deprecated_is_skipped() {
        let mut st = make_server_type("cx11", 1, 2.0, 20.0);
        st.deprecated = Some(true);
        assert!(convert_server_type(&st).is_empty());
    }

    #[test]
    fn convert_server_type_deprecation_info_is_skipped() {
        let mut st = make_server_type("cx11", 1, 2.0, 20.0);
        st.deprecated = Some(false);
        st.deprecation = Some(Some(Box::new(hcloud::models::DeprecationInfo::new(
            "2025-01-01".to_string(),
            "2025-06-01".to_string(),
        ))));
        assert!(convert_server_type(&st).is_empty());
    }

    #[test]
    fn convert_server_type_deprecated_location_skipped() {
        let mut st = make_server_type("cpx22", 2, 4.0, 40.0);
        st.locations.push(ServerTypeLocation {
            id: 2,
            name: "old-dc".to_string(),
            deprecation: Some(Box::new(hcloud::models::DeprecationInfo::new(
                "2024-01-01".to_string(),
                "2024-06-01".to_string(),
            ))),
        });

        let offerings = convert_server_type(&st);
        assert_eq!(offerings.len(), 1);
        assert_eq!(offerings[0].location.region.0, "fsn1");
    }

    #[test]
    fn convert_server_type_memory_conversion() {
        // 0.5 GB = 512 MiB
        let st = make_server_type("tiny", 1, 0.5, 10.0);
        let offerings = convert_server_type(&st);
        assert_eq!(offerings[0].resources.memory_mib, 512);
    }
}
