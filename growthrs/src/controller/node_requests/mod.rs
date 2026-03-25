pub(crate) mod helpers;

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use kube::Api;
use kube::runtime::controller::Action;
use kube::runtime::{Controller, watcher};
use tracing::{debug, error, info, instrument, warn};

use crate::config::ControllerContext;
use crate::controller::node_removal::helpers::create_node_removal_request;
use crate::controller::node_requests::helpers::delete_node_request;
use crate::controller::{ControllerError, is_kube_not_found, update_node_request_phase};
use crate::providers::provider::{NodeId, ProviderStatus};
use crate::resources::node_removal_request::NodeRemovalRequestPhase;
use crate::resources::node_request::{NodeRequest, NodeRequestPhase};

use helpers::{ProvisionOutcome, attempt_provision};

const PROVISIONING_REQUEUE: Duration = Duration::from_secs(60);
const NR_ERROR_REQUEUE: Duration = Duration::from_secs(5);

/// Run the per-object NodeRequest controller.
pub(crate) async fn run_node_request_controller(
    ctx: Arc<ControllerContext>,
) -> Result<(), ControllerError> {
    let nrs: Api<NodeRequest> = Api::all(ctx.client.clone());
    let config = watcher::Config::default();
    let mut stream = std::pin::pin!(Controller::new(nrs, config).run(
        crate::controller::node_requests::reconcile_node_request,
        crate::controller::node_requests::error_policy,
        ctx.clone(),
    ));
    while let Some(result) = stream.next().await {
        let (obj, _) = result.map_err(ControllerError::from_controller_error)?;
        debug!(name = %obj.name, "reconciled NodeRequest")
    }
    Ok(())
}

#[instrument(skip_all, fields(nr = nr.metadata.name.as_deref().unwrap_or("<unknown>")))]
pub(super) async fn reconcile_node_request(
    nr: Arc<NodeRequest>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, ControllerError> {
    let name = nr
        .metadata
        .name
        .as_deref()
        .ok_or(ControllerError::MissingName("NodeRequest"))?;

    let (next_phase, action) = match decide_phase(&nr, &ctx).await {
        Ok(result) => result,
        Err(ControllerError::Kube(ref e)) if is_kube_not_found(e) => {
            warn!(
                name,
                "expected NodeRequest cannot be found, skipping reconcile"
            );
            return Ok(Action::await_change());
        }
        Err(e) => return Err(e),
    };

    if let Some(phase) = next_phase {
        info!(name, from = %nr.phase(), to = %phase, "transitioning NodeRequest");
        let now = ctx.clock.now();
        if let Err(e) = update_node_request_phase(&ctx.client, name, phase, now).await {
            if is_kube_not_found(&e) {
                warn!(
                    name,
                    "expected NodeRequest cannot be found, skipping phase transition"
                );
                return Ok(Action::await_change());
            }
            return Err(e.into());
        }
    }
    Ok(action)
}

async fn decide_phase(
    nr: &NodeRequest,
    ctx: &ControllerContext,
) -> Result<(Option<NodeRequestPhase>, Action), ControllerError> {
    let name = nr
        .metadata
        .name
        .as_deref()
        .ok_or(ControllerError::MissingName("NodeRequest"))?;

    match nr.phase() {
        NodeRequestPhase::Pending => match attempt_provision(nr, ctx).await? {
            ProvisionOutcome::Created => Ok((
                Some(NodeRequestPhase::Provisioning),
                Action::requeue(PROVISIONING_REQUEUE),
            )),
            ProvisionOutcome::NoMatchingOffering | ProvisionOutcome::OfferingUnavailable => {
                Ok((Some(NodeRequestPhase::Unmet), Action::await_change()))
            }
        },
        NodeRequestPhase::Provisioning => {
            let now = ctx.clock.now();
            match ctx.provider.status(&NodeId(nr.spec.node_id.clone())).await {
                Ok(ProviderStatus::Failed { .. })
                | Ok(ProviderStatus::Removing)
                | Ok(ProviderStatus::NotFound) => {
                    Ok((Some(NodeRequestPhase::Unmet), Action::await_change()))
                }
                Ok(ProviderStatus::Creating) | Ok(ProviderStatus::Running) => {
                    if is_provisioning_expired(nr, ctx.provisioning_timeout, now) {
                        warn!(name, node_id = %nr.spec.node_id, "provisioning timeout exceeded, creating NRR for orphan node");
                        let pool = pool_name_from_nr(nr)?;
                        if let Err(e) = create_node_removal_request(
                            ctx.client.clone(),
                            &nr.spec.node_id,
                            None,
                            &pool,
                            &nr.spec.target_offering.0,
                            NodeRemovalRequestPhase::Deprovisioning,
                            now,
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
            let now = ctx.clock.now();
            if is_unmet_expired(nr, ctx.scale_down.unmet_ttl, now) {
                info!(name, "Unmet TTL expired, deleting NodeRequest");
                delete_node_request(ctx.client.clone(), name).await?;
                Ok((None, Action::await_change()))
            } else {
                let remaining = remaining_unmet_ttl(nr, ctx.scale_down.unmet_ttl, now);
                Ok((None, Action::requeue(remaining)))
            }
        }
    }
}

pub(super) fn error_policy(
    nr: Arc<NodeRequest>,
    error: &ControllerError,
    _ctx: Arc<ControllerContext>,
) -> Action {
    match nr.metadata.name.as_deref() {
        Some(name) => {
            warn!(name, %error, "NodeRequest reconcile failed, requeuing");
        }
        None => {
            error!(
                "NodeRequest reconcile failed on a NodeRequest that lacks a name, indicating an issue with the CRD"
            )
        }
    }
    Action::requeue(NR_ERROR_REQUEUE)
}

/// Extract the pool name from the NodeRequest's ownerReference to NodePool.
fn pool_name_from_nr(nr: &NodeRequest) -> Result<String, ControllerError> {
    nr.metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.kind == "NodePool"))
        .map(|r| r.name.clone())
        .ok_or(ControllerError::Other(anyhow::anyhow!(
            "NodeRequest missing ownerReference to NodePool"
        )))
}

/// Check whether the NodeRequest has been alive longer than the provisioning timeout.
///
/// Uses `creation_timestamp` as the start time since Pending→Provisioning is near-instant.
fn is_provisioning_expired(
    nr: &NodeRequest,
    timeout: Duration,
    now: k8s_openapi::jiff::Timestamp,
) -> bool {
    let Some(created) = nr.metadata.creation_timestamp.as_ref() else {
        return false;
    };
    let elapsed = now.duration_since(created.0);
    elapsed > k8s_openapi::jiff::SignedDuration::from_secs(timeout.as_secs() as i64)
}

/// Check whether an Unmet NodeRequest's TTL has expired.
///
/// Uses `last_transition_time` (set when phase changes to Unmet).
/// Missing timestamp is treated as expired — GC immediately.
pub(crate) fn is_unmet_expired(
    nr: &NodeRequest,
    ttl: Duration,
    now: k8s_openapi::jiff::Timestamp,
) -> bool {
    let Some(transition_time) = nr
        .status
        .as_ref()
        .and_then(|s| s.last_transition_time.as_ref())
    else {
        return true;
    };
    let elapsed = now.duration_since(transition_time.0);
    elapsed > k8s_openapi::jiff::SignedDuration::from_secs(ttl.as_secs() as i64)
}

/// Compute remaining time before the Unmet TTL expires, for requeue scheduling.
fn remaining_unmet_ttl(
    nr: &NodeRequest,
    ttl: Duration,
    now: k8s_openapi::jiff::Timestamp,
) -> Duration {
    let Some(transition_time) = nr
        .status
        .as_ref()
        .and_then(|s| s.last_transition_time.as_ref())
    else {
        return Duration::ZERO;
    };
    let elapsed = now.duration_since(transition_time.0);
    let ttl_signed = k8s_openapi::jiff::SignedDuration::from_secs(ttl.as_secs() as i64);
    let remaining = ttl_signed
        .checked_sub(elapsed)
        .unwrap_or(k8s_openapi::jiff::SignedDuration::ZERO);
    Duration::from_secs(remaining.as_secs().max(0) as u64)
}

#[cfg(test)]
mod tests {
    use http::{Request, Response};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use kube::Client;
    use kube::api::ObjectMeta;
    use kube::client::Body;

    use crate::clock::SystemClock;
    use crate::config::ScaleDownConfig;
    use crate::offering::Resources;
    use crate::offering::{InstanceType, Region};
    use crate::providers::fake::{FakeProvider, StatusBehavior};
    use crate::providers::provider::{Provider, ProviderStatus};
    use crate::resources::node_request::{
        NodeRequest, NodeRequestPhase, NodeRequestSpec, NodeRequestStatus,
    };

    use super::*;

    fn mock_client() -> Client {
        let (mock_svc, _handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
        Client::new(mock_svc, "default")
    }

    fn make_provisioning_nr(name: &str, node_id: &str, created_ago: Duration) -> NodeRequest {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

        let ts = k8s_openapi::jiff::Timestamp::now()
            .checked_sub(k8s_openapi::jiff::SignedDuration::from_secs(
                created_ago.as_secs() as i64,
            ))
            .unwrap();
        NodeRequest {
            metadata: ObjectMeta {
                name: Some(name.into()),
                creation_timestamp: Some(Time(ts)),
                owner_references: Some(vec![OwnerReference {
                    api_version: "growth.vettrdev.com/v1alpha1".into(),
                    kind: "NodePool".into(),
                    name: "test-pool".into(),
                    uid: "pool-uid-test".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            spec: NodeRequestSpec {
                node_id: node_id.into(),
                target_offering: InstanceType("cpx22".into()),
                location: Region("eu-central".into()),
                resources: Resources {
                    cpu: 2,
                    memory_mib: 4096,
                    ephemeral_storage_gib: None,
                    gpu: 0,
                    gpu_model: None,
                },
            },
            status: Some(NodeRequestStatus {
                phase: NodeRequestPhase::Provisioning,
                events: vec![],
                last_transition_time: None,
            }),
        }
    }

    fn make_ctx_with_provider(provider: FakeProvider) -> ControllerContext {
        ControllerContext {
            client: mock_client(),
            provider: Provider::Fake(provider),
            provisioning_timeout: Duration::from_secs(300),
            scale_down: ScaleDownConfig::default(),
            clock: Arc::new(SystemClock),
        }
    }

    #[tokio::test]
    async fn provisioning_timeout_retries_when_nrr_creation_fails() {
        // With a mock client, NRR creation will fail (no API server),
        // so the NodeRequest stays in Provisioning and requeues for retry.
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
    /// blip), `attempt_provision` propagates the error so the controller
    /// can requeue and retry rather than silently provisioning with no config.
    #[tokio::test]
    async fn provision_returns_error_on_pool_fetch_failure() {
        use crate::offering::{InstanceType, Location, Offering, Region, Zone};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

        type ApiServerHandle = tower_test::mock::Handle<Request<Body>, Response<Body>>;

        fn mock_client_with_handle() -> (Client, ApiServerHandle) {
            let (mock_svc, handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
            (Client::new(mock_svc, "default"), handle)
        }

        let (client, mut handle) = mock_client_with_handle();
        let provider = FakeProvider::new().with_offerings(vec![Offering {
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
                region: Region("eu-central".into()),
                zone: Some(Zone("fsn1-dc14".into())),
            },
        }]);

        let ctx = ControllerContext {
            client,
            provider: Provider::Fake(provider),
            provisioning_timeout: Duration::from_secs(300),
            scale_down: ScaleDownConfig::default(),
            clock: Arc::new(SystemClock),
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
                target_offering: InstanceType("cpx22".into()),
                location: Region("eu-central".into()),
                resources: Resources {
                    cpu: 2,
                    memory_mib: 4096,
                    ephemeral_storage_gib: None,
                    gpu: 0,
                    gpu_model: None,
                },
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
            result.is_err(),
            "expected error on pool fetch failure, got {result:?}"
        );
    }
}
