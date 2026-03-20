use std::collections::{BTreeMap, HashMap};

use hcloud::apis::configuration::Configuration;
use hcloud::apis::server_types_api::{self, ListServerTypesParams};
use hcloud::apis::servers_api::{self, CreateServerParams, DeleteServerParams, ListServersParams};
use hcloud::models::{
    CreateServerRequest, CreateServerRequestFirewalls, CreateServerRequestPublicNet, Server,
    ServerType,
};
use kube::runtime::reflector::Lookup;
use kube::{Api, Client};
use tracing::{debug, info, warn};

use crate::controller::helpers::{read_configmap_key, read_secret_key};
use crate::crds::hetzner_node_class::{
    HetznerNodeClass, HetznerNodeClassCondition, HetznerNodeClassStatus,
};
use crate::crds::user_data::{RESERVED_DYNAMIC_VARS, UserDataError, resolve_template};
use crate::offering::{InstanceType, Location, Offering, Region, Resources};
use crate::providers::provider::{InstanceConfig, NodeId, ProviderError, ProviderStatus};

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
            // This is fairly opinionated, but for the benefit of knowing if a server _exists_, knowing it's being deleted is sufficient.
            hcloud::models::server::Status::Deleting | hcloud::models::server::Status::Stopping => {
                ProviderStatus::NotFound
            }
            hcloud::models::server::Status::Off | hcloud::models::server::Status::Unknown => {
                ProviderStatus::Failed {
                    reason: format!("server status: {:?}", value.status),
                }
            }
        }
    }
}

/// Map a Hetzner `Server` to a `ProviderStatus`.
fn map_server_status(server: &Server) -> ProviderStatus {
    ProviderStatus::from(server)
}

/// Build the per-node dynamic variables from the offering and labels.
///
/// These are the reserved dynamic variables (REGION, LOCATION, INSTANCE_TYPE,
/// NODE_LABELS) that are injected at provision time regardless of whether the
/// template came from a HetznerNodeClass or not.
fn build_dynamic_vars(
    offering: &Offering,
    labels: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    // Build --node-label flags so the k3s agent gets pool, node-request, and
    // instance-type labels that controllers and nodeSelector matching depend on.
    // Kubelet rejects `kubernetes.io/` and `k8s.io/` labels via --node-labels
    // (except an allowed set). Those are applied post-join by the node controller.
    let node_labels_str = labels
        .iter()
        /*         .filter(|(k, _)| {
                let domain = k.split('/').next().unwrap_or("");
                !domain.ends_with("kubernetes.io") && !domain.ends_with("k8s.io")
            })
        */ // Removed until we can find evidence the offending labels *are* inserted.
        .map(|(k, v)| format!("--node-label={k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");

    vec![
        ("REGION".to_string(), offering.location.region.0.clone()),
        ("LOCATION".to_string(), offering.location.region.0.clone()),
        (
            "INSTANCE_TYPE".to_string(),
            offering.instance_type.0.clone(),
        ),
        ("NODE_LABELS".to_string(), node_labels_str),
    ]
}

/// Write a status condition to a HetznerNodeClass resource.
async fn write_status_condition(
    client: &Client,
    hnc_name: &str,
    condition: HetznerNodeClassCondition,
) {
    let api: Api<HetznerNodeClass> = Api::all(client.clone());
    let status = HetznerNodeClassStatus {
        conditions: vec![condition],
    };
    let patch = serde_json::json!({ "status": status });
    if let Err(e) = api
        .patch_status(
            hnc_name,
            &kube::api::PatchParams::apply("growthrs"),
            &kube::api::Patch::Merge(patch),
        )
        .await
    {
        warn!(hnc = hnc_name, %e, "failed to write HetznerNodeClass status condition");
    }
}

impl HetznerProvider {
    /// Resolve provider-specific configuration from a HetznerNodeClass CRD.
    ///
    /// Reads the HetznerNodeClass, resolves the user-data template (ConfigMap +
    /// Secret variables + dynamic per-node variables), and returns the full
    /// `HetznerCreateConfig`.
    ///
    /// Fails if the pool has no `nodeClassRef` (Hetzner pools require one),
    /// the referenced CRD is missing, or template resolution fails.
    pub async fn resolve_create_config(
        &self,
        client: &Client,
        node_class: HetznerNodeClass,
        offering: &Offering,
        labels: &BTreeMap<String, String>,
    ) -> Result<HetznerCreateConfig, ProviderError> {
        // Read the template from the referenced ConfigMap.
        let tpl_ref = &node_class.spec.user_data.template_ref;
        let template = read_configmap_key(client, &tpl_ref.namespace, &tpl_ref.name, &tpl_ref.key)
            .await
            .map_err(|e: UserDataError| ProviderError::CreationFailed {
                message: e.to_string(),
            })?;

        // Read all variable values from Secrets and validate no reserved name collisions.
        let mut secret_vars = Vec::new();
        if let Some(vars) = &node_class.spec.user_data.variables {
            for var in vars {
                if RESERVED_DYNAMIC_VARS.contains(&var.name.as_str()) {
                    return Err(ProviderError::CreationFailed {
                        message: UserDataError::ReservedNameCollision {
                            name: var.name.clone(),
                        }
                        .to_string(),
                    });
                }
                let value = read_secret_key(
                    client,
                    &var.secret_ref.namespace,
                    &var.secret_ref.name,
                    &var.secret_ref.key,
                )
                .await
                .map_err(|e: UserDataError| ProviderError::Internal(anyhow::anyhow!(e)))?;
                secret_vars.push((var.name.clone(), value));
            }
        }

        // Resolve user-data: merge secret vars + dynamic per-node vars, then resolve.
        let dynamic_vars = build_dynamic_vars(offering, labels);
        let mut all_vars = secret_vars;
        for (name, value) in dynamic_vars {
            let marker = format!("{{{{ {name} }}}}");
            if template.contains(&marker) {
                all_vars.push((name, value));
            }
        }
        let user_data =
            resolve_template(&template, &all_vars).map_err(|e| ProviderError::CreationFailed {
                message: e.to_string(),
            })?;

        // Write status condition if network_ids is empty (11B).
        if node_class.spec.network_ids.is_empty() {
            warn!("HetznerNodeClass has no network_ids — servers will have public networking only");
            write_status_condition(
                client,
                // TODO: Refactor to remove this unwrap.
                &node_class.name().unwrap(),
                HetznerNodeClassCondition {
                    r#type: "NetworkConfigured".to_string(),
                    status: "False".to_string(),
                    message: "No network_ids configured — servers will have public networking only"
                        .to_string(),
                },
            )
            .await;
        } else {
            write_status_condition(
                client,
                // TODO: Refactor to remove this unwrap
                &node_class.name().unwrap(),
                HetznerNodeClassCondition {
                    r#type: "NetworkConfigured".to_string(),
                    status: "True".to_string(),
                    message: format!(
                        "{} network(s) configured",
                        node_class.spec.network_ids.len()
                    ),
                },
            )
            .await;
        }

        // TODO: Write an Into to convert from HetznerNodeClass -> HetznerCreateConfig
        Ok(HetznerCreateConfig {
            user_data: Some(user_data),
            image: node_class.spec.image.clone(),
            ssh_key_names: node_class.spec.ssh_key_names.clone(),
            network_ids: node_class.spec.network_ids.clone(),
            firewall_ids: node_class.spec.firewall_ids.clone(),
            enable_ipv4: node_class.spec.enable_ipv4,
            enable_ipv6: node_class.spec.enable_ipv6,
            hetzner_labels: node_class.spec.hetzner_labels.clone(),
        })
    }

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

    #[test]
    fn map_server_status_running() {
        let mut server = test_server();
        server.status = hcloud::models::server::Status::Running;
        assert_eq!(map_server_status(&server), ProviderStatus::Running);
    }

    #[test]
    fn map_server_status_initializing() {
        let mut server = test_server();
        server.status = hcloud::models::server::Status::Initializing;
        assert_eq!(map_server_status(&server), ProviderStatus::Creating);
    }

    #[test]
    fn map_server_status_starting() {
        let mut server = test_server();
        server.status = hcloud::models::server::Status::Starting;
        assert_eq!(map_server_status(&server), ProviderStatus::Creating);
    }

    #[test]
    fn map_server_status_deleting() {
        let mut server = test_server();
        server.status = hcloud::models::server::Status::Deleting;
        assert_eq!(map_server_status(&server), ProviderStatus::NotFound);
    }

    #[test]
    fn map_server_status_off_is_failed() {
        let mut server = test_server();
        server.status = hcloud::models::server::Status::Off;
        assert!(matches!(
            map_server_status(&server),
            ProviderStatus::Failed { .. }
        ));
    }

    #[test]
    fn hetzner_requires_node_class_ref() {
        let provider = HetznerProvider::new("fake-token".into());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async {
            let (mock_svc, _) = tower_test::mock::pair::<
                http::Request<kube::client::Body>,
                http::Response<kube::client::Body>,
            >();
            let client = Client::new(mock_svc, "default");
            let offering = Offering {
                instance_type: InstanceType("cpx22".into()),
                resources: Resources {
                    cpu: 2,
                    memory_mib: 4096,
                    ephemeral_storage_gib: None,
                    gpu: 0,
                    gpu_model: None,
                },
                cost_per_hour: 0.01,
                location: Location {
                    region: Region("fsn1".into()),
                    zone: None,
                },
            };
            provider
                .resolve_create_config(&client, &None, &offering, &BTreeMap::new())
                .await
        });
        assert!(matches!(result, Err(ProviderError::MissingConfig { .. })));
    }

    /// Minimal Server for testing map_server_status.
    fn test_server() -> Server {
        let location = hcloud::models::Location {
            id: 1,
            name: "fsn1".to_string(),
            description: "Falkenstein DC Park 1".to_string(),
            city: "Falkenstein".to_string(),
            country: "DE".to_string(),
            latitude: 50.47612,
            longitude: 12.37044,
            network_zone: "eu-central".to_string(),
        };
        let dc_server_types = hcloud::models::DataCenterServerTypes {
            available: vec![],
            available_for_migration: vec![],
            supported: vec![],
        };
        let datacenter = hcloud::models::DataCenter {
            id: 1,
            name: "fsn1-dc14".to_string(),
            description: "Falkenstein 1 DC14".to_string(),
            location: Box::new(location),
            server_types: Box::new(dc_server_types),
        };
        Server {
            id: 1,
            name: "test".to_string(),
            status: hcloud::models::server::Status::Running,
            created: "2025-01-01T00:00:00+00:00".to_string(),
            public_net: Box::new(hcloud::models::ServerPublicNet {
                floating_ips: vec![],
                firewalls: None,
                ipv4: None,
                ipv6: None,
            }),
            server_type: Box::new(make_server_type("cpx22", 2, 4.0, 40.0)),
            datacenter: Box::new(datacenter),
            image: None,
            iso: None,
            labels: HashMap::new(),
            locked: false,
            protection: Box::new(hcloud::models::ServerProtection::new(false, false)),
            rescue_enabled: false,
            backup_window: None,
            included_traffic: None,
            ingoing_traffic: None,
            outgoing_traffic: None,
            load_balancers: None,
            primary_disk_size: 40,
            private_net: vec![],
            volumes: None,
            placement_group: None,
        }
    }
}
