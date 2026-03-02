use std::time::Duration;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{Api, Client, api::PatchParams};
use tokio::time::sleep;
use tracing::warn;

use crate::crds::node_request::{NodeRequest, NodeRequestPhase, NodeRequestStatus};

const CUSTOM_RESOURCE_DEFINITIONS: [&'static str; 4] = [
    "noderequests.growth.vettrdev.com",
    "nodepools.growth.vettrdev.com",
    "noderemovalrequests.growth.vettrdev.com",
    "hetznernodeclasses.growth.vettrdev.com"
];

pub(crate) async fn update_node_request_phase(
    client: &Client,
    name: &str,
    phase: NodeRequestPhase,
) -> Result<(), kube::Error> {
    let api: Api<NodeRequest> = Api::all(client.clone());
    let status = NodeRequestStatus {
        phase,
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
pub(super) async fn wait_for_crds(client: Client) -> anyhow::Result<()> {
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
