use std::collections::BTreeMap;

use kube::{Api, Client};
use tracing::{debug, info, warn};

use crate::crds::node_pool::NodePool;
use crate::crds::node_request::NodeRequest;
use crate::offering::{
    INSTANCE_TYPE_LABEL, MANAGED_BY_LABEL, MANAGED_BY_VALUE, NODE_REQUEST_LABEL, POOL_LABEL,
};
use crate::providers::provider::{InstanceConfig, ProviderError};

use super::{ControllerContext, ProvisionOutcome};
use crate::controller::errors::ControllerError;

/// Information extracted from the owning NodePool.
struct PoolInfo {
    labels: BTreeMap<String, String>,
    node_class_ref: Option<crate::crds::node_pool::NodeClassRef>,
}

/// Look up the owning NodePool's labels and node_class_ref from the NodeRequest's ownerReference.
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

pub(super) async fn attempt_provision(
    nr: &NodeRequest,
    ctx: &ControllerContext,
) -> Result<ProvisionOutcome, ControllerError> {
    let name = nr.metadata.name.as_deref().unwrap_or("<unknown>");
    let offerings = ctx.provider.offerings().await;
    let Some(offering) = offerings.iter().find(|o| {
        o.instance_type.0 == nr.spec.target_offering && o.location.region.0 == nr.spec.location
    }) else {
        warn!(
            name,
            target_offering = %nr.spec.target_offering,
            location = %nr.spec.location,
            "no matching offering found in provider catalog"
        );
        return Ok(ProvisionOutcome::NoMatchingOffering);
    };

    let pool_info = get_pool_info(&ctx.client, nr).await;

    // Build generic labels (provider-agnostic).
    let config = build_labels(nr, &pool_info.labels);

    // Resolve provider-specific config from CRDs.
    let provider_config = ctx
        .provider
        .resolve_create_config(
            &ctx.client,
            &pool_info.node_class_ref,
            offering,
            &config.labels,
        )
        .await
        .map_err(|e| {
            ControllerError::Other(anyhow::anyhow!("provider config resolution failed: {e}"))
        })?;

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
        nr.spec.target_offering.clone(),
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
    use crate::crds::node_request::{NodeRequestPhase, NodeRequestSpec, NodeRequestStatus};
    use crate::offering::Resources;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn test_nr() -> NodeRequest {
        NodeRequest {
            metadata: ObjectMeta {
                name: Some("nr-test".into()),
                ..Default::default()
            },
            spec: NodeRequestSpec {
                node_id: "node-1".into(),
                target_offering: "cpx22".into(),
                location: "fsn1".into(),
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

    #[test]
    fn reserved_name_collision_is_validated_at_resolve_time() {
        use crate::crds::hetzner_node_class::RESERVED_DYNAMIC_VARS;

        assert!(RESERVED_DYNAMIC_VARS.contains(&"REGION"));
        assert!(RESERVED_DYNAMIC_VARS.contains(&"LOCATION"));
        assert!(RESERVED_DYNAMIC_VARS.contains(&"INSTANCE_TYPE"));
        assert!(RESERVED_DYNAMIC_VARS.contains(&"NODE_LABELS"));

        // Verify the error type exists and formats correctly.
        let err = crate::crds::hetzner_node_class::UserDataError::ReservedNameCollision {
            name: "REGION".into(),
        };
        assert!(err.to_string().contains("REGION"));
        assert!(err.to_string().contains("reserved"));
    }
}
