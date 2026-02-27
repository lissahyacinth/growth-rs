use kube::{Api, Client, api::PatchParams};

use crate::node_request::{NodeRequest, NodeRequestPhase, NodeRequestStatus};

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
