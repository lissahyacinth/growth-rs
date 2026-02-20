use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::offering::Offering;
use crate::providers::provider::{InstanceConfig, NodeId, ProviderError};

/// What happens on the next `create()` call.
#[derive(Debug, Clone)]
pub enum CreateBehavior {
    /// Happy path — returns Ok(NodeId).
    Succeed,
    /// Returns Ok(NodeId) but the node never appears in the cluster.
    SucceedButNodeNeverJoins,
    /// Returns Ok(NodeId) after sleeping for the given duration.
    SucceedAfterDelay(Duration),
    /// The offering isn't available (sold out, wrong region, etc).
    OfferingUnavailable,
    /// General creation failure.
    CreationFailed(String),
    /// Node was created but never joined the cluster within timeout.
    JoinTimeout,
    /// Network/API blowup.
    InternalError(String),
}

/// What happens on the next `delete()` call.
#[derive(Debug, Clone)]
pub enum DeleteBehavior {
    /// Node removed successfully.
    Succeed,
    /// Returns Ok(()) but the node persists (silent no-op).
    Noop,
    /// Deletion failed.
    Fail(String),
}

/// How `offerings()` behaves.
#[derive(Debug, Clone)]
pub enum OfferingsBehavior {
    /// Returns the same set every call.
    Static(Vec<Offering>),
    /// Returns successive elements; sticks on the last one when exhausted.
    Sequence(VecDeque<Vec<Offering>>),
}

/// Logged record of a `create()` call.
#[derive(Debug, Clone)]
pub struct CreateCall {
    pub offering: Offering,
    pub result_node_id: Option<NodeId>,
}

/// Logged record of a `delete()` call.
#[derive(Debug, Clone)]
pub struct DeleteCall {
    pub node_id: NodeId,
}

/// Interior state behind the Arc<Mutex<_>>.
#[derive(Debug)]
pub(crate) struct FakeProviderState {
    offerings_behavior: OfferingsBehavior,
    create_behaviors: VecDeque<CreateBehavior>,
    delete_behaviors: VecDeque<DeleteBehavior>,
    default_create: CreateBehavior,
    default_delete: DeleteBehavior,
    pub create_calls: Vec<CreateCall>,
    pub delete_calls: Vec<DeleteCall>,
}

/// A deterministic, in-memory provider for testing failure modes.
///
/// Each call to `create()`/`delete()` pops the next behavior from a queue.
/// When the queue is empty, the configured default applies.
#[derive(Debug, Clone)]
pub struct FakeProvider {
    state: Arc<Mutex<FakeProviderState>>,
    next_id: Arc<AtomicU64>,
}

impl FakeProvider {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeProviderState {
                offerings_behavior: OfferingsBehavior::Static(vec![]),
                create_behaviors: VecDeque::new(),
                delete_behaviors: VecDeque::new(),
                default_create: CreateBehavior::Succeed,
                default_delete: DeleteBehavior::Succeed,
                create_calls: Vec::new(),
                delete_calls: Vec::new(),
            })),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    // ── Builder methods ──────────────────────────────────────────────

    pub fn with_offerings(self, offerings: Vec<Offering>) -> Self {
        self.state.lock().unwrap().offerings_behavior = OfferingsBehavior::Static(offerings);
        self
    }

    pub fn with_offerings_sequence(self, seq: Vec<Vec<Offering>>) -> Self {
        self.state.lock().unwrap().offerings_behavior =
            OfferingsBehavior::Sequence(seq.into_iter().collect());
        self
    }

    pub fn on_next_create(self, behavior: CreateBehavior) -> Self {
        self.state
            .lock()
            .unwrap()
            .create_behaviors
            .push_back(behavior);
        self
    }

    pub fn on_next_delete(self, behavior: DeleteBehavior) -> Self {
        self.state
            .lock()
            .unwrap()
            .delete_behaviors
            .push_back(behavior);
        self
    }

    pub fn with_default_create(self, behavior: CreateBehavior) -> Self {
        self.state.lock().unwrap().default_create = behavior;
        self
    }

    pub fn with_default_delete(self, behavior: DeleteBehavior) -> Self {
        self.state.lock().unwrap().default_delete = behavior;
        self
    }

    // ── Introspection ────────────────────────────────────────────────

    pub fn create_calls(&self) -> Vec<CreateCall> {
        self.state.lock().unwrap().create_calls.clone()
    }

    pub fn delete_calls(&self) -> Vec<DeleteCall> {
        self.state.lock().unwrap().delete_calls.clone()
    }

    // ── Provider implementation ──────────────────────────────────────

    fn next_node_id(&self) -> NodeId {
        let n = self.next_id.fetch_add(1, Ordering::Relaxed);
        NodeId(format!("fake-node-{n}"))
    }

    pub async fn offerings(&self) -> Vec<Offering> {
        let mut state = self.state.lock().unwrap();
        match &mut state.offerings_behavior {
            OfferingsBehavior::Static(v) => v.clone(),
            OfferingsBehavior::Sequence(seq) => {
                if seq.len() > 1 {
                    seq.pop_front().unwrap()
                } else {
                    // Stick on the last element.
                    seq.front().cloned().unwrap_or_default()
                }
            }
        }
    }

    pub async fn create(
        &self,
        offering: &Offering,
        _config: &InstanceConfig,
    ) -> Result<NodeId, ProviderError> {
        let behavior = {
            let mut state = self.state.lock().unwrap();
            state
                .create_behaviors
                .pop_front()
                .unwrap_or_else(|| state.default_create.clone())
        };

        let result = match behavior {
            CreateBehavior::Succeed | CreateBehavior::SucceedButNodeNeverJoins => {
                Ok(self.next_node_id())
            }
            CreateBehavior::SucceedAfterDelay(d) => {
                tokio::time::sleep(d).await;
                Ok(self.next_node_id())
            }
            CreateBehavior::OfferingUnavailable => Err(ProviderError::OfferingUnavailable(
                format!("{} not available", offering.instance_type.0),
            )),
            CreateBehavior::CreationFailed(msg) => {
                Err(ProviderError::CreationFailed { message: msg })
            }
            CreateBehavior::JoinTimeout => Err(ProviderError::JoinTimeout { node_id: None }),
            CreateBehavior::InternalError(msg) => {
                Err(ProviderError::Internal(anyhow::anyhow!(msg)))
            }
        };

        // Log the call.
        let result_node_id = result.as_ref().ok().cloned();
        self.state.lock().unwrap().create_calls.push(CreateCall {
            offering: offering.clone(),
            result_node_id,
        });

        result
    }

    pub async fn delete(&self, node_id: &NodeId) -> Result<(), ProviderError> {
        let behavior = {
            let mut state = self.state.lock().unwrap();
            state
                .delete_behaviors
                .pop_front()
                .unwrap_or_else(|| state.default_delete.clone())
        };

        self.state.lock().unwrap().delete_calls.push(DeleteCall {
            node_id: node_id.clone(),
        });

        match behavior {
            DeleteBehavior::Succeed | DeleteBehavior::Noop => Ok(()),
            DeleteBehavior::Fail(msg) => Err(ProviderError::CreationFailed { message: msg }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offering::{InstanceType, Resources};

    fn test_offering() -> Offering {
        Offering {
            instance_type: InstanceType("test-instance".into()),
            resources: Resources {
                cpu: 2,
                memory_mib: 4096,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.01,
        }
    }

    #[tokio::test]
    async fn default_create_succeeds() {
        let provider = FakeProvider::new().with_offerings(vec![test_offering()]);
        let result = provider.create(&test_offering(), &InstanceConfig {}).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().0, "fake-node-1");
    }

    #[tokio::test]
    async fn queued_behaviors_are_consumed_in_order() {
        let provider = FakeProvider::new()
            .on_next_create(CreateBehavior::OfferingUnavailable)
            .on_next_create(CreateBehavior::Succeed);

        let first = provider.create(&test_offering(), &InstanceConfig {}).await;
        assert!(first.is_err());

        let second = provider.create(&test_offering(), &InstanceConfig {}).await;
        assert!(second.is_ok());
    }

    #[tokio::test]
    async fn falls_back_to_default_when_queue_empty() {
        let provider = FakeProvider::new()
            .with_default_create(CreateBehavior::JoinTimeout)
            .on_next_create(CreateBehavior::Succeed);

        let first = provider.create(&test_offering(), &InstanceConfig {}).await;
        assert!(first.is_ok());

        let second = provider.create(&test_offering(), &InstanceConfig {}).await;
        assert!(matches!(second, Err(ProviderError::JoinTimeout { .. })));
    }

    #[tokio::test]
    async fn create_calls_are_logged() {
        let provider = FakeProvider::new();
        let offering = test_offering();
        provider
            .create(&offering, &InstanceConfig {})
            .await
            .unwrap();
        provider
            .create(&offering, &InstanceConfig {})
            .await
            .unwrap();

        let calls = provider.create_calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].offering, offering);
        assert!(calls[1].result_node_id.is_some());
    }

    #[tokio::test]
    async fn each_create_returns_distinct_node_id() {
        let provider = FakeProvider::new();
        let offering = test_offering();
        let id1 = provider
            .create(&offering, &InstanceConfig {})
            .await
            .unwrap();
        let id2 = provider
            .create(&offering, &InstanceConfig {})
            .await
            .unwrap();
        let id3 = provider
            .create(&offering, &InstanceConfig {})
            .await
            .unwrap();
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[tokio::test]
    async fn delete_default_succeeds() {
        let provider = FakeProvider::new();
        let result = provider.delete(&NodeId("fake-node-1".into())).await;
        assert!(result.is_ok());
        assert_eq!(provider.delete_calls().len(), 1);
    }

    #[tokio::test]
    async fn delete_fail_behavior() {
        let provider = FakeProvider::new().on_next_delete(DeleteBehavior::Fail("boom".into()));
        let result = provider.delete(&NodeId("fake-node-1".into())).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn offerings_sequence_advances() {
        let provider = FakeProvider::new().with_offerings_sequence(vec![
            vec![test_offering()],
            vec![], // second call returns empty
        ]);

        let first = provider.offerings().await;
        assert_eq!(first.len(), 1);

        let second = provider.offerings().await;
        assert_eq!(second.len(), 0);

        // Sticks on last
        let third = provider.offerings().await;
        assert_eq!(third.len(), 0);
    }
}
