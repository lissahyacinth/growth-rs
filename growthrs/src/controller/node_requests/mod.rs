mod helpers;

use std::sync::Arc;
use std::time::Duration;

use kube::runtime::controller::Action;
use tracing::{info, instrument, warn};

use crate::crds::node_removal_request::{NodeRemovalRequestPhase, create_node_removal_request};
use crate::crds::node_request::{NodeRequest, NodeRequestPhase};
use crate::providers::provider::{NodeId, ProviderStatus};

use super::{ControllerContext, ReconcileError, update_node_request_phase};

use helpers::{attempt_provision, delete_node_request};

const PROVISIONING_REQUEUE: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub enum ProvisionOutcome {
    Created,
    AlreadyCreated,
    NoMatchingOffering,
    OfferingUnavailable(String),
}

#[instrument(skip_all, fields(nr = nr.metadata.name.as_deref().unwrap_or("<unknown>")))]
pub(super) async fn reconcile_node_request(
    nr: Arc<NodeRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ReconcileError> {
    let name = nr.metadata.name.as_deref().unwrap_or("<unknown>");

    let (next_phase, action) = match decide_phase(&nr, &ctx).await {
        Ok(result) => result,
        Err(ref e) if is_not_found(e) => {
            warn!(name, "NodeRequest was deleted externally, skipping reconcile");
            return Ok(Action::await_change());
        }
        Err(e) => return Err(e),
    };

    if let Some(phase) = next_phase {
        info!(name, from = %nr.phase(), to = %phase, "transitioning NodeRequest");
        if let Err(e) = update_node_request_phase(&ctx.client, name, phase).await {
            if is_kube_not_found(&e) {
                warn!(name, "NodeRequest was deleted externally during phase transition");
                return Ok(Action::await_change());
            }
            return Err(e.into());
        }
    }

    Ok(action)
}

fn is_not_found(err: &ReconcileError) -> bool {
    matches!(err, ReconcileError::Kube(kube::Error::Api(resp)) if resp.code == 404)
}

fn is_kube_not_found(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(resp) if resp.code == 404)
}

async fn decide_phase(
    nr: &NodeRequest,
    ctx: &ControllerContext,
) -> Result<(Option<NodeRequestPhase>, Action), ReconcileError> {
    let name = nr.metadata.name.as_deref().unwrap_or("<unknown>");

    match nr.phase() {
        NodeRequestPhase::Pending => match attempt_provision(nr, ctx).await? {
            ProvisionOutcome::Created | ProvisionOutcome::AlreadyCreated => {
                Ok((Some(NodeRequestPhase::Provisioning), Action::requeue(PROVISIONING_REQUEUE)))
            }
            ProvisionOutcome::NoMatchingOffering
            | ProvisionOutcome::OfferingUnavailable(_) => {
                Ok((Some(NodeRequestPhase::Unmet), Action::await_change()))
            }
        },
        NodeRequestPhase::Provisioning => {
            match ctx.provider.status(&NodeId(nr.spec.node_id.clone())).await {
                Ok(ProviderStatus::Failed { .. }) | Ok(ProviderStatus::NotFound) => {
                    Ok((Some(NodeRequestPhase::Unmet), Action::await_change()))
                }
                Ok(ProviderStatus::Creating) | Ok(ProviderStatus::Running) => {
                    if is_provisioning_expired(nr, ctx.provisioning_timeout) {
                        warn!(name, node_id = %nr.spec.node_id, "provisioning timeout exceeded, creating NRR for orphan node");
                        let pool = pool_name_from_nr(nr);
                        if let Err(e) = create_node_removal_request(
                            ctx.client.clone(),
                            &nr.spec.node_id,
                            None,
                            &pool,
                            &nr.spec.target_offering,
                            NodeRemovalRequestPhase::Deprovisioning,
                        )
                        .await
                        {
                            warn!(name, %e, "failed to create NRR for orphan node, will retry");
                            return Ok((None, Action::requeue(PROVISIONING_REQUEUE)));
                        }
                        Ok((Some(NodeRequestPhase::Unmet), Action::await_change()))
                    } else {
                        Ok((None, Action::requeue(PROVISIONING_REQUEUE)))
                    }
                }
                Err(e) => {
                    warn!(name, %e, "provider status check failed");
                    Ok((None, Action::requeue(PROVISIONING_REQUEUE)))
                }
            }
        }
        NodeRequestPhase::Ready => Ok((None, Action::await_change())),
        NodeRequestPhase::Unmet => {
            // TODO: delete this CRD if TTL is met to allow other Nodes to spin up.
            Ok((None, Action::await_change()))
        }
        NodeRequestPhase::Deprovisioning => {
            // Legacy: NRs should no longer enter this phase. Clean up gracefully.
            warn!(name, "found NR in Deprovisioning phase (legacy), deleting CRD");
            delete_node_request(&ctx.client, name).await?;
            Ok((None, Action::await_change()))
        }
    }
}

pub(super) fn error_policy(
    nr: Arc<NodeRequest>,
    error: &ReconcileError,
    _ctx: Arc<ControllerContext>,
) -> Action {
    let name = nr.metadata.name.as_deref().unwrap_or("<unknown>");
    warn!(name, %error, "NodeRequest reconcile failed, requeuing");
    Action::requeue(Duration::from_secs(5))
}

/// Extract the pool name from the NR's ownerReference to NodePool.
fn pool_name_from_nr(nr: &NodeRequest) -> String {
    nr.metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.kind == "NodePool"))
        .map(|r| r.name.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Check whether the NodeRequest has been alive longer than the provisioning timeout.
///
/// Uses `creation_timestamp` as the start time since Pending→Provisioning is near-instant.
fn is_provisioning_expired(nr: &NodeRequest, timeout: Duration) -> bool {
    let Some(created) = nr.metadata.creation_timestamp.as_ref() else {
        return false;
    };
    let now = k8s_openapi::jiff::Timestamp::now();
    let elapsed = now.duration_since(created.0);
    elapsed > k8s_openapi::jiff::SignedDuration::from_secs(timeout.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use http::{Request, Response};
    use kube::Client;
    use kube::api::ObjectMeta;
    use kube::client::Body;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

    use crate::crds::node_request::{NodeRequest, NodeRequestPhase, NodeRequestSpec, NodeRequestStatus};
    use crate::offering::Resources;
    use crate::providers::fake::{DeleteBehavior, FakeProvider, StatusBehavior};
    use crate::providers::provider::{Provider, ProviderStatus};

    use super::*;

    fn mock_client() -> Client {
        let (mock_svc, _handle) =
            tower_test::mock::pair::<Request<Body>, Response<Body>>();
        Client::new(mock_svc, "default")
    }

    fn make_provisioning_nr(name: &str, node_id: &str, created_ago: Duration) -> NodeRequest {
        let ts = k8s_openapi::jiff::Timestamp::now()
            .checked_sub(k8s_openapi::jiff::SignedDuration::from_secs(
                created_ago.as_secs() as i64,
            ))
            .unwrap();
        NodeRequest {
            metadata: ObjectMeta {
                name: Some(name.into()),
                creation_timestamp: Some(Time(ts)),
                ..Default::default()
            },
            spec: NodeRequestSpec {
                node_id: node_id.into(),
                target_offering: "cx22".into(),
                resources: Resources {
                    cpu: 2,
                    memory_mib: 4096,
                    ephemeral_storage_gib: None,
                    gpu: 0,
                    gpu_model: None,
                },
                claimed_pod_uids: vec![],
            },
            status: Some(NodeRequestStatus {
                phase: NodeRequestPhase::Provisioning,
                events: vec![],
            }),
        }
    }

    fn make_ctx_with_provider(provider: FakeProvider) -> ControllerContext {
        ControllerContext {
            client: mock_client(),
            provider: Provider::Fake(provider),
            provisioning_timeout: Duration::from_secs(300),
            scale_down: crate::controller::ScaleDownConfig::default(),
        }
    }

    #[tokio::test]
    async fn provisioning_timeout_retries_when_nrr_creation_fails() {
        // With a mock client, NRR creation will fail (no API server),
        // so the NR stays in Provisioning and requeues for retry.
        let provider = FakeProvider::new()
            .with_default_status(StatusBehavior::Return(ProviderStatus::Creating));
        let ctx = make_ctx_with_provider(provider.clone());

        let nr = make_provisioning_nr("nr-timeout", "node-1", Duration::from_secs(360));
        let (phase, action) = decide_phase(&nr, &ctx).await.unwrap();

        assert_eq!(phase, None);
        assert_eq!(action, Action::requeue(PROVISIONING_REQUEUE));
        // No direct provider.delete() — that's now the NRR's job.
        assert!(provider.delete_calls().is_empty());
    }

    #[tokio::test]
    async fn provisioning_no_timeout_when_within_window() {
        let provider = FakeProvider::new()
            .with_default_status(StatusBehavior::Return(ProviderStatus::Creating));
        let ctx = make_ctx_with_provider(provider.clone());

        let nr = make_provisioning_nr("nr-fresh", "node-2", Duration::from_secs(60));
        let (phase, action) = decide_phase(&nr, &ctx).await.unwrap();

        assert_eq!(phase, None);
        assert_eq!(action, Action::requeue(PROVISIONING_REQUEUE));
        assert!(provider.delete_calls().is_empty());
    }

    #[tokio::test]
    async fn provisioning_timeout_delete_failure_stays_provisioning() {
        let provider = FakeProvider::new()
            .with_default_status(StatusBehavior::Return(ProviderStatus::Creating))
            .on_next_delete(DeleteBehavior::Fail("provider error".into()));
        let ctx = make_ctx_with_provider(provider.clone());

        let nr = make_provisioning_nr("nr-del-fail", "node-3", Duration::from_secs(360));
        let (phase, action) = decide_phase(&nr, &ctx).await.unwrap();

        assert_eq!(phase, None);
        assert_eq!(action, Action::requeue(PROVISIONING_REQUEUE));
    }

    #[tokio::test]
    async fn provisioning_failed_status_goes_unmet_regardless_of_timeout() {
        let provider = FakeProvider::new().with_default_status(StatusBehavior::Return(
            ProviderStatus::Failed {
                reason: "vm crashed".into(),
            },
        ));
        let ctx = make_ctx_with_provider(provider.clone());

        let nr = make_provisioning_nr("nr-failed", "node-4", Duration::from_secs(360));
        let (phase, action) = decide_phase(&nr, &ctx).await.unwrap();

        assert_eq!(phase, Some(NodeRequestPhase::Unmet));
        assert_eq!(action, Action::await_change());
        assert!(provider.delete_calls().is_empty());
    }

    /// When the kube API returns a 500 for the NodePool GET (e.g. network
    /// blip), `attempt_provision` should still succeed — provisioning the
    /// node with empty labels rather than crashing or returning an error.
    #[tokio::test]
    async fn provision_succeeds_with_empty_labels_on_pool_fetch_error() {
        use crate::offering::{InstanceType, Location, Offering, Region, Zone};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

        type ApiServerHandle = tower_test::mock::Handle<Request<Body>, Response<Body>>;

        fn mock_client_with_handle() -> (Client, ApiServerHandle) {
            let (mock_svc, handle) =
                tower_test::mock::pair::<Request<Body>, Response<Body>>();
            (Client::new(mock_svc, "default"), handle)
        }

        let (client, mut handle) = mock_client_with_handle();
        let provider = FakeProvider::new().with_offerings(vec![Offering {
            instance_type: InstanceType("cx22".into()),
            resources: Resources {
                cpu: 2,
                memory_mib: 4096,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.01,
            location: Location {
                region: Region("eu-central".into()),
                zone: Some(Zone("fsn1-dc14".into())),
            },
        }]);

        let ctx = ControllerContext {
            client,
            provider: Provider::Fake(provider),
            provisioning_timeout: Duration::from_secs(300),
            scale_down: crate::controller::ScaleDownConfig::default(),
        };

        let nr = NodeRequest {
            metadata: ObjectMeta {
                name: Some("nr-label-err".into()),
                owner_references: Some(vec![OwnerReference {
                    api_version: "growth.vettrdev.com/v1alpha1".into(),
                    kind: "NodePool".into(),
                    name: "my-pool".into(),
                    uid: "pool-uid-123".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            spec: NodeRequestSpec {
                node_id: "growth-test-node".into(),
                target_offering: "cx22".into(),
                resources: Resources {
                    cpu: 2,
                    memory_mib: 4096,
                    ephemeral_storage_gib: None,
                    gpu: 0,
                    gpu_model: None,
                },
                claimed_pod_uids: vec![],
            },
            status: None,
        };

        // Mock API returns 500 for the NodePool GET request.
        tokio::spawn(async move {
            let (request, send) = handle.next_request().await.expect("expected NodePool GET");
            assert!(
                request.uri().path().contains("nodepools"),
                "expected request to nodepools, got {}",
                request.uri().path()
            );
            let body = r#"{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"internal error","reason":"InternalError","code":500}"#;
            send.send_response(
                Response::builder()
                    .status(500)
                    .header("content-type", "application/json")
                    .body(Body::from(body.as_bytes().to_vec()))
                    .unwrap(),
            );
        });

        let result = helpers::attempt_provision(&nr, &ctx).await;
        assert!(
            matches!(result, Ok(ProvisionOutcome::Created)),
            "expected Created despite pool label fetch failure, got {result:?}"
        );
    }
}
