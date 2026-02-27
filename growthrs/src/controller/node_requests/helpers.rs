use std::collections::BTreeMap;

use kube::{Api, Client};
use tracing::{debug, info, warn};

use crate::node_request::NodeRequest;
use crate::offering::{INSTANCE_TYPE_LABEL, NODE_REQUEST_LABEL, POOL_LABEL};
use crate::providers::provider::{InstanceConfig, ProviderError};

use super::{ControllerContext, ProvisionOutcome, ReconcileError};

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

    let config = build_instance_config(nr);

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

fn build_instance_config(nr: &NodeRequest) -> InstanceConfig {
    let mut labels = BTreeMap::new();
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
    InstanceConfig { labels }
}

pub(super) async fn delete_node_request(client: &Client, name: &str) -> Result<(), kube::Error> {
    let api: Api<NodeRequest> = Api::all(client.clone());
    api.delete(name, &Default::default()).await?;
    Ok(())
}
