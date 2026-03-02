use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use tracing::{debug, info, warn};

use crate::crds::hetzner_node_class::{HetznerNodeClass, UserDataError, resolve_template};
use crate::crds::node_pool::{NodeClassRef, NodePool};
use crate::crds::node_request::NodeRequest;
use crate::offering::{INSTANCE_TYPE_LABEL, NODE_REQUEST_LABEL, POOL_LABEL};
use crate::providers::provider::{InstanceConfig, ProviderError};

use super::{ControllerContext, ProvisionOutcome, ReconcileError};

/// Information extracted from the owning NodePool.
struct PoolInfo {
    labels: BTreeMap<String, String>,
    node_class_ref: Option<NodeClassRef>,
}

/// Look up the owning NodePool's labels and node_class_ref from the NR's ownerReference.
async fn get_pool_info(client: &Client, nr: &NodeRequest) -> PoolInfo {
    let pool_name = nr
        .metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.kind == "NodePool"))
        .map(|r| r.name.clone());

    let Some(name) = pool_name else {
        return PoolInfo {
            labels: BTreeMap::new(),
            node_class_ref: None,
        };
    };

    let api: Api<NodePool> = Api::all(client.clone());
    match api.get_opt(&name).await {
        Ok(Some(np)) => PoolInfo {
            labels: np.spec.labels,
            node_class_ref: np.spec.node_class_ref,
        },
        Ok(None) => {
            debug!(pool = %name, "owning NodePool not found, using empty labels");
            PoolInfo {
                labels: BTreeMap::new(),
                node_class_ref: None,
            }
        }
        Err(e) => {
            warn!(pool = %name, error = %e, "failed to fetch NodePool, using defaults");
            PoolInfo {
                labels: BTreeMap::new(),
                node_class_ref: None,
            }
        }
    }
}

/// Read a single key from a Kubernetes Secret.
async fn read_secret_key(
    client: &Client,
    namespace: &str,
    name: &str,
    key: &str,
) -> Result<String, UserDataError> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api.get(name).await.map_err(|e| UserDataError::SecretReadFailed {
        secret_name: name.to_string(),
        key: key.to_string(),
        reason: e.to_string(),
    })?;

    let data = secret
        .data
        .as_ref()
        .and_then(|d| d.get(key))
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

/// Resolve user-data for a pool's node class reference.
///
/// Returns `Ok(None)` if no node class ref is set.
/// Returns `Ok(Some(resolved))` with the fully-substituted template.
/// Returns `Err` if the HetznerNodeClass is missing, Secrets can't be read,
/// or template resolution fails.
async fn resolve_user_data_for_pool(
    client: &Client,
    node_class_ref: &Option<NodeClassRef>,
) -> Result<Option<String>, UserDataError> {
    let Some(class_ref) = node_class_ref else {
        return Ok(None);
    };

    let api: Api<HetznerNodeClass> = Api::all(client.clone());
    let hnc = api
        .get(&class_ref.name)
        .await
        .map_err(|_| UserDataError::NodeClassNotFound {
            name: class_ref.name.clone(),
        })?;

    // Read the template from the referenced Secret.
    let tpl_ref = &hnc.spec.user_data.template_ref;
    let template = read_secret_key(client, &tpl_ref.namespace, &tpl_ref.name, &tpl_ref.key).await?;

    // Read all variable values.
    let mut variables = Vec::new();
    if let Some(vars) = &hnc.spec.user_data.variables {
        for var in vars {
            let value = read_secret_key(
                client,
                &var.secret_ref.namespace,
                &var.secret_ref.name,
                &var.secret_ref.key,
            )
            .await?;
            variables.push((var.name.clone(), value));
        }
    }

    let resolved = resolve_template(&template, &variables)?;
    Ok(Some(resolved))
}

pub(super) async fn attempt_provision(
    nr: &NodeRequest,
    ctx: &ControllerContext,
) -> Result<ProvisionOutcome, ReconcileError> {
    let name = nr.metadata.name.as_deref().unwrap_or("<unknown>");
    let offerings = ctx.provider.offerings().await;
    let Some(offering) = offerings
        .iter()
        .find(|o| o.instance_type.0 == nr.spec.target_offering)
    else {
        warn!(
            name,
            target_offering = %nr.spec.target_offering,
            "no matching offering found in provider catalog"
        );
        return Ok(ProvisionOutcome::NoMatchingOffering);
    };

    let pool_info = get_pool_info(&ctx.client, nr).await;

    let user_data = match resolve_user_data_for_pool(&ctx.client, &pool_info.node_class_ref).await
    {
        Ok(ud) => ud,
        Err(e) => {
            return Err(ReconcileError::Other(anyhow::anyhow!(
                "user-data resolution failed: {e}"
            )));
        }
    };

    let config = build_instance_config(nr, &pool_info.labels, user_data);

    info!(
        name,
        offering = %offering.instance_type,
        node_id = %nr.spec.node_id,
        "requesting node from provider"
    );

    match ctx
        .provider
        .create(nr.spec.node_id.clone(), offering, &config)
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
            Ok(ProvisionOutcome::OfferingUnavailable(reason))
        }
        Err(e) => Err(ReconcileError::Other(e.into())),
    }
}

pub(crate) fn build_instance_config(
    nr: &NodeRequest,
    pool_labels: &BTreeMap<String, String>,
    user_data: Option<String>,
) -> InstanceConfig {
    let mut labels = pool_labels.clone();
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
        nr.spec.target_offering.clone(),
    );
    InstanceConfig { labels, user_data }
}

pub(super) async fn delete_node_request(client: &Client, name: &str) -> Result<(), kube::Error> {
    let api: Api<NodeRequest> = Api::all(client.clone());
    match api.delete(name, &Default::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref resp)) if resp.code == 404 => {
            warn!(name, "NodeRequest already deleted, nothing to clean up");
            Ok(())
        }
        Err(e) => Err(e),
    }
}
