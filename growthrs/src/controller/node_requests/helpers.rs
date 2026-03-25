use std::collections::BTreeMap;

use kube::{Api, Client};
use tracing::{debug, info, warn};

use kube::api::{ObjectMeta, PostParams};

use crate::controller::helpers::{read_configmap_key, read_secret_key};
use crate::offering::{
    INSTANCE_TYPE_LABEL, MANAGED_BY_LABEL, MANAGED_BY_VALUE, NODE_REQUEST_LABEL, Offering,
    POOL_LABEL,
};
use crate::providers::hetzner::config::HetznerCreateConfig;
use crate::providers::provider::{InstanceConfig, Provider, ProviderCreateConfig, ProviderError};
use crate::resources::hetzner_node_class::HetznerNodeClass;
use crate::resources::node_pool::NodePool;
use crate::resources::node_request::{NodeRequest, NodeRequestSpec};
use crate::resources::user_data::{
    RESERVED_DYNAMIC_VARS, UserDataConfig, UserDataError, build_dynamic_vars, resolve_template,
};

use super::ControllerContext;

impl UserDataConfig {
    /// Resolve the user-data template: read ConfigMap + Secret values, merge
    /// dynamic per-node variables, and perform template substitution.
    ///
    /// This is the imperative-shell entry point for template resolution.
    /// All K8s I/O (ConfigMap/Secret reads) happens here; the actual
    /// substitution is delegated to the pure `resolve_template()`.
    pub(crate) async fn resolve(
        &self,
        client: &Client,
        offering: &Offering,
        labels: &BTreeMap<String, String>,
    ) -> Result<String, UserDataError> {
        let tpl_ref = &self.template_ref;
        let template =
            read_configmap_key(client, &tpl_ref.namespace, &tpl_ref.name, &tpl_ref.key).await?;

        let mut secret_vars = Vec::new();
        for var in &self.variables {
            if RESERVED_DYNAMIC_VARS.contains(&var.name.as_str()) {
                return Err(UserDataError::ReservedNameCollision {
                    name: var.name.clone(),
                });
            }
            let value = read_secret_key(
                client,
                &var.secret_ref.namespace,
                &var.secret_ref.name,
                &var.secret_ref.key,
            )
            .await?;
            secret_vars.push((var.name.clone(), value));
        }

        let dynamic_vars = build_dynamic_vars(offering, labels);
        let mut all_vars = secret_vars;
        for (name, value) in dynamic_vars {
            let marker = format!("{{{{ {name} }}}}");
            if template.contains(&marker) {
                all_vars.push((name, value));
            }
        }

        resolve_template(&template, &all_vars)
    }
}

#[derive(Debug)]
pub enum ProvisionOutcome {
    Created,
    NoMatchingOffering,
    OfferingUnavailable,
}
use crate::controller::errors::ControllerError;

/// Information extracted from the owning NodePool.
struct PoolInfo {
    labels: BTreeMap<String, String>,
    node_class_ref: Option<crate::resources::node_pool::NodeClassRef>,
}

/// Look up the owning NodePool's labels and node_class_ref from the NodeRequest's ownerReference.
///
/// Errors if the NodeRequest has no ownerReference to a NodePool (invariant
/// violation per RFC) or the referenced NodePool no longer exists. Propagates
/// kube API errors so the caller can requeue on transient failures.
async fn get_pool_info(client: &Client, nr: &NodeRequest) -> Result<PoolInfo, ControllerError> {
    let nr_name = nr.metadata.name.as_deref().unwrap_or("<unknown>");
    let pool_name = nr
        .metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.kind == "NodePool"))
        .map(|r| r.name.clone());

    let Some(name) = pool_name else {
        return Err(ControllerError::Other(anyhow::anyhow!(
            "NodeRequest {nr_name:?} has no ownerReference to a NodePool"
        )));
    };

    let api: Api<NodePool> = Api::all(client.clone());
    match api.get_opt(&name).await {
        Ok(Some(np)) => Ok(PoolInfo {
            labels: np.spec.labels,
            node_class_ref: np.spec.node_class_ref,
        }),
        Ok(None) => Err(ControllerError::Other(anyhow::anyhow!(
            "owning NodePool {name:?} not found for NodeRequest {nr_name:?}"
        ))),
        Err(e) => Err(ControllerError::Kube(e)),
    }
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
        "created NodeRequest"
    );
    Ok(created)
}

pub(super) async fn attempt_provision(
    nr: &NodeRequest,
    ctx: &ControllerContext,
) -> Result<ProvisionOutcome, ControllerError> {
    let name = nr
        .metadata
        .name
        .as_deref()
        .ok_or(ControllerError::MissingName("NodeRequest"))?;
    let offerings = ctx.provider.offerings().await;
    let Some(offering) = offerings.iter().find(|o| {
        o.instance_type == nr.spec.target_offering && o.location.region == nr.spec.location
    }) else {
        warn!(
            name,
            target_offering = %nr.spec.target_offering,
            location = %nr.spec.location,
            "no matching offering found in provider catalog"
        );
        return Ok(ProvisionOutcome::NoMatchingOffering);
    };

    let pool_info = get_pool_info(&ctx.client, nr).await?;
    // Build generic labels (provider-agnostic).
    let config = build_labels(nr, &pool_info.labels);

    // Resolve provider-specific config from CRDs at the controller level.
    // The controller matches on the provider variant to decide which
    // NodeClass CRD to fetch, keeping provider-specific types out of
    // the generic Provider interface.
    let provider_config = match &ctx.provider {
        Provider::Hetzner(_) => {
            let class_ref = pool_info
                .node_class_ref
                .as_ref()
                .ok_or(ControllerError::Other(anyhow::anyhow!(
                    "Hetzner pools require a nodeClassRef"
                )))?;
            let node_class = kube::Api::<HetznerNodeClass>::all(ctx.client.clone())
                .get(&class_ref.name)
                .await
                .map_err(|e| {
                    ControllerError::Other(anyhow::anyhow!(
                        "HetznerNodeClass {:?} not found: {e}",
                        class_ref.name
                    ))
                })?;

            let user_data = node_class
                .spec
                .user_data
                .resolve(&ctx.client, offering, &config.labels)
                .await
                .map_err(|e| {
                    ControllerError::Other(anyhow::anyhow!("user-data resolution failed: {e}"))
                })?;

            if node_class.spec.network_ids.is_empty() {
                warn!(
                    "HetznerNodeClass has no network_ids — servers will have public networking only"
                );
            }

            ProviderCreateConfig::Hetzner(HetznerCreateConfig {
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
        Provider::Kwok(_) | Provider::Fake(_) => ProviderCreateConfig::None,
    };

    info!(
        name,
        offering = %offering.instance_type,
        node_id = %nr.spec.node_id,
        "requesting node from provider"
    );

    match ctx
        .provider
        .create(nr.spec.node_id.clone(), offering, &config, &provider_config)
        .await
    {
        Ok(_) => {
            debug!(name, node_id = %nr.spec.node_id, "provider accepted create request");
            Ok(ProvisionOutcome::Created)
        }
        Err(ProviderError::OfferingUnavailable(reason)) => {
            warn!(
                name,
                offering = %offering.instance_type,
                reason = %reason,
                "offering unavailable from provider"
            );
            Ok(ProvisionOutcome::OfferingUnavailable)
        }
        Err(e) => Err(ControllerError::Other(e.into())),
    }
}

/// Build the generic InstanceConfig (labels only) for a NodeRequest.
///
/// Pure function — no I/O, no ControllerContext dependency.
fn build_labels(nr: &NodeRequest, pool_labels: &BTreeMap<String, String>) -> InstanceConfig {
    let mut labels = pool_labels.clone();
    labels.insert(MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string());
    if let Some(pool_name) = nr
        .metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.kind == "NodePool"))
        .map(|r| r.name.clone())
    {
        labels.insert(POOL_LABEL.to_string(), pool_name);
    }
    labels.insert(
        NODE_REQUEST_LABEL.to_string(),
        nr.metadata
            .name
            .as_deref()
            .unwrap_or("<unknown>")
            .to_string(),
    );
    labels.insert(
        INSTANCE_TYPE_LABEL.to_string(),
        nr.spec.target_offering.0.clone(),
    );

    InstanceConfig { labels }
}

pub(super) async fn delete_node_request(client: Client, name: &str) -> Result<(), kube::Error> {
    let api: Api<NodeRequest> = Api::all(client);
    match api.delete(name, &Default::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref resp)) if resp.code == 404 => {
            warn!(name, "NodeRequest already deleted, nothing to clean up");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offering::{InstanceType, Region, Resources};
    use crate::resources::node_request::{NodeRequestPhase, NodeRequestSpec, NodeRequestStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn test_nr() -> NodeRequest {
        NodeRequest {
            metadata: ObjectMeta {
                name: Some("nr-test".into()),
                ..Default::default()
            },
            spec: NodeRequestSpec {
                node_id: "node-1".into(),
                target_offering: InstanceType("cpx22".into()),
                location: Region("fsn1".into()),
                resources: Resources {
                    cpu: 3,
                    memory_mib: 4096,
                    ephemeral_storage_gib: None,
                    gpu: 0,
                    gpu_model: None,
                },
            },
            status: Some(NodeRequestStatus {
                phase: NodeRequestPhase::Pending,
                events: vec![],
                last_transition_time: None,
            }),
        }
    }

    #[test]
    fn build_labels_includes_managed_by() {
        let nr = test_nr();
        let config = build_labels(&nr, &BTreeMap::new());
        assert_eq!(
            config.labels.get(MANAGED_BY_LABEL).unwrap(),
            MANAGED_BY_VALUE
        );
    }

    #[test]
    fn build_labels_includes_instance_type() {
        let nr = test_nr();
        let config = build_labels(&nr, &BTreeMap::new());
        assert_eq!(
            config
                .labels
                .get("growth.vettrdev.com/instance-type")
                .unwrap(),
            "cpx22"
        );
    }

    #[test]
    fn build_labels_includes_pool_labels() {
        let nr = test_nr();
        let mut pool_labels = BTreeMap::new();
        pool_labels.insert("custom-label".into(), "custom-value".into());
        let config = build_labels(&nr, &pool_labels);
        assert_eq!(config.labels.get("custom-label").unwrap(), "custom-value");
    }

    #[test]
    fn build_labels_includes_node_request_name() {
        let nr = test_nr();
        let config = build_labels(&nr, &BTreeMap::new());
        assert_eq!(
            config
                .labels
                .get("growth.vettrdev.com/node-request")
                .unwrap(),
            "nr-test"
        );
    }
}
