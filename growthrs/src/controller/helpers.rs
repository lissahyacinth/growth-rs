use std::collections::HashSet;
use std::time::Duration;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::ListParams;
use kube::{Api, Client, api::PatchParams};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::controller::errors::{ConfigError, ControllerError};
use crate::resources::user_data::UserDataError;

use crate::providers::provider::Provider;
use crate::resources::node_pool::NodePool;
use crate::resources::node_request::{NodeRequest, NodeRequestPhase, NodeRequestStatus};

const CUSTOM_RESOURCE_DEFINITIONS: [&str; 4] = [
    "noderequests.growth.vettrdev.com",
    "nodepools.growth.vettrdev.com",
    "noderemovalrequests.growth.vettrdev.com",
    "hetznernodeclasses.growth.vettrdev.com",
];

/// Check whether a `kube::Error` is a 404 Not Found API response.
pub(crate) fn is_kube_not_found(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(resp) if resp.code == 404)
}

pub(crate) async fn update_node_request_phase(
    client: &Client,
    name: &str,
    phase: NodeRequestPhase,
    now: k8s_openapi::jiff::Timestamp,
) -> Result<(), kube::Error> {
    let api: Api<NodeRequest> = Api::all(client.clone());
    let status = NodeRequestStatus {
        phase,
        last_transition_time: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(now)),
        ..Default::default()
    };
    let patch = serde_json::json!({ "status": status });
    api.patch_status(
        name,
        &PatchParams::apply("growthrs"),
        &kube::api::Patch::Merge(patch),
    )
    .await?;
    Ok(())
}

/// Loop until CRDs are installed on Cluster
pub(super) async fn wait_for_crds(client: Client) -> Result<(), ControllerError> {
    let api: Api<CustomResourceDefinition> = Api::all(client);
    let mut crd_stablised: Vec<bool> = vec![false; CUSTOM_RESOURCE_DEFINITIONS.len()];
    loop {
        for (crd_idx, crd_name) in CUSTOM_RESOURCE_DEFINITIONS.iter().enumerate() {
            if crd_stablised[crd_idx] {
                continue;
            }
            if let Some(crd) = api.get_opt(crd_name).await? {
                let established = crd
                    .status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conditions| {
                        conditions
                            .iter()
                            .any(|c| c.type_ == "Established" && c.status == "True")
                    })
                    .unwrap_or(false);
                if established {
                    crd_stablised[crd_idx] = true;
                    if crd_stablised.iter().all(|f| *f) {
                        return Ok(());
                    }
                }
            }
        }
        let missing_crds: String = CUSTOM_RESOURCE_DEFINITIONS
            .iter()
            .zip(crd_stablised.iter())
            .filter_map(|(crd, stabilised)| {
                if !*stabilised {
                    Some(crd.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        warn!(missing = %missing_crds, "CRDs not yet established, retrying in 5s");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Fetch all NodePools and provider offerings, then warn about any pool server
/// types or locations that don't exist in the provider catalog.
pub(super) async fn validate_pool_offerings(
    client: &Client,
    provider: &Provider,
) -> Result<(), ControllerError> {
    let offerings = provider.offerings().await;
    let known_types: HashSet<&str> = offerings
        .iter()
        .map(|o| o.instance_type.0.as_str())
        .collect();
    info!(count = known_types.len(), "fetched provider offerings");

    let api: Api<NodePool> = Api::all(client.clone());
    for pool in api.list(&ListParams::default()).await? {
        let pool_name = pool
            .metadata
            .name
            .ok_or(ControllerError::MissingName("NodePool"))?;

        for st in &pool.spec.server_types {
            if !known_types.contains(st.name.as_str()) {
                return Err(ConfigError::ServerTypeUnavailableError(format!(
                    "{} from {} not found in provider catalog",
                    st.name.clone(),
                    pool_name.clone(),
                )))?;
            }

            // Validate locations: if the pool constrains locations, check each
            // region and zone actually has an offering for this server type.
            if let Some(locations) = &pool.spec.locations {
                for loc in locations {
                    let region_match = offerings
                        .iter()
                        .any(|o| o.instance_type.0 == st.name && o.location.region.0 == loc.region);
                    if !region_match {
                        return Err(ConfigError::ServerTypeUnavailableError(format!(
                            "{} from {} not available in region {}",
                            st.name, pool_name, loc.region
                        )))?;
                    }

                    if let Some(zones) = &loc.zones {
                        for zone in zones {
                            let zone_match = offerings.iter().any(|o| {
                                o.instance_type.0 == st.name
                                    && o.location.region.0 == loc.region
                                    && o.location.zone.as_ref().map(|z| z.0.as_str())
                                        == Some(zone.as_str())
                            });
                            if !zone_match {
                                return Err(ConfigError::ServerTypeUnavailableError(format!(
                                    "{} from {} not available in zone {} (region {})",
                                    st.name, pool_name, zone, loc.region
                                )))?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Read a single key from a Kubernetes Secret.
pub(crate) async fn read_secret_key(
    client: &Client,
    namespace: &str,
    name: &str,
    key: &str,
) -> Result<String, UserDataError> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api
        .get(name)
        .await
        .map_err(|e| UserDataError::SecretReadFailed {
            secret_name: name.to_string(),
            key: key.to_string(),
            reason: e.to_string(),
        })?;

    let data = secret
        .data
        .as_ref()
        .and_then(|d: &std::collections::BTreeMap<String, k8s_openapi::ByteString>| d.get(key))
        .ok_or_else(|| UserDataError::SecretReadFailed {
            secret_name: name.to_string(),
            key: key.to_string(),
            reason: format!("key {key:?} not found in secret"),
        })?;

    String::from_utf8(data.0.clone()).map_err(|e| UserDataError::SecretReadFailed {
        secret_name: name.to_string(),
        key: key.to_string(),
        reason: format!("value is not valid UTF-8: {e}"),
    })
}

/// Read a single key from a Kubernetes ConfigMap.
pub(crate) async fn read_configmap_key(
    client: &Client,
    namespace: &str,
    name: &str,
    key: &str,
) -> Result<String, UserDataError> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let cm = api
        .get(name)
        .await
        .map_err(|e| UserDataError::ConfigMapReadFailed {
            configmap_name: name.to_string(),
            key: key.to_string(),
            reason: e.to_string(),
        })?;

    cm.data
        .as_ref()
        .and_then(|d: &std::collections::BTreeMap<String, String>| d.get(key))
        .cloned()
        .ok_or_else(|| UserDataError::ConfigMapReadFailed {
            configmap_name: name.to_string(),
            key: key.to_string(),
            reason: format!("key {key:?} not found in configmap"),
        })
}
