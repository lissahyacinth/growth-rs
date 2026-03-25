mod types;

pub use types::*;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::offering::Offering;
use crate::providers::provider::{InstanceConfig, NodeId, ProviderError, ProviderStatus};

/// A queue of scripted behaviors with a default fallback, plus a call log.
///
/// Each provider operation (create, delete, status) uses one of these.
/// `next()` pops the front of the queue; when empty, clones the default.
#[derive(Debug)]
struct BehaviorQueue<B: Clone, C> {
    queue: VecDeque<B>,
    default: B,
    calls: Vec<C>,
}

impl<B: Clone, C: Clone> BehaviorQueue<B, C> {
    fn new(default: B) -> Self {
        Self {
            queue: VecDeque::new(),
            default,
            calls: Vec::new(),
        }
    }

    fn next(&mut self) -> B {
        self.queue
            .pop_front()
            .unwrap_or_else(|| self.default.clone())
    }

    fn push(&mut self, behavior: B) {
        self.queue.push_back(behavior);
    }

    fn set_default(&mut self, behavior: B) {
        self.default = behavior;
    }

    fn log(&mut self, call: C) {
        self.calls.push(call);
    }

    fn calls(&self) -> Vec<C> {
        self.calls.clone()
    }
}

/// Interior state behind the Arc<Mutex<_>>.
#[derive(Debug)]
pub(crate) struct FakeProviderState {
    offerings_behavior: OfferingsBehavior,
    create: BehaviorQueue<CreateBehavior, CreateCall>,
    delete: BehaviorQueue<DeleteBehavior, DeleteCall>,
    status: BehaviorQueue<StatusBehavior, StatusCall>,
}

/// A deterministic, in-memory provider for testing failure modes.
///
/// Each call to `create()`/`delete()`/`status()` pops the next behavior from
/// a queue. When the queue is empty, the configured default applies.
#[derive(Debug, Clone)]
pub struct FakeProvider {
    state: Arc<Mutex<FakeProviderState>>,
}

impl Default for FakeProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeProvider {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeProviderState {
                offerings_behavior: OfferingsBehavior::Static(vec![]),
                create: BehaviorQueue::new(CreateBehavior::Succeed),
                delete: BehaviorQueue::new(DeleteBehavior::Succeed),
                status: BehaviorQueue::new(StatusBehavior::Return(ProviderStatus::Running)),
            })),
        }
    }
}

impl FakeProvider {
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
        self.state.lock().unwrap().create.push(behavior);
        self
    }

    pub fn on_next_delete(self, behavior: DeleteBehavior) -> Self {
        self.state.lock().unwrap().delete.push(behavior);
        self
    }

    pub fn on_next_status(self, behavior: StatusBehavior) -> Self {
        self.state.lock().unwrap().status.push(behavior);
        self
    }

    pub fn with_default_create(self, behavior: CreateBehavior) -> Self {
        self.state.lock().unwrap().create.set_default(behavior);
        self
    }

    pub fn with_default_delete(self, behavior: DeleteBehavior) -> Self {
        self.state.lock().unwrap().delete.set_default(behavior);
        self
    }

    pub fn with_default_status(self, behavior: StatusBehavior) -> Self {
        self.state.lock().unwrap().status.set_default(behavior);
        self
    }
}

impl FakeProvider {
    pub fn create_calls(&self) -> Vec<CreateCall> {
        self.state.lock().unwrap().create.calls()
    }

    pub fn delete_calls(&self) -> Vec<DeleteCall> {
        self.state.lock().unwrap().delete.calls()
    }

    pub fn status_calls(&self) -> Vec<StatusCall> {
        self.state.lock().unwrap().status.calls()
    }
}

impl FakeProvider {
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
        node_id: String,
        offering: &Offering,
        config: &InstanceConfig,
    ) -> Result<NodeId, ProviderError> {
        let behavior = self.state.lock().unwrap().create.next();
        let node_id = NodeId(node_id);

        let result = match behavior {
            CreateBehavior::Succeed | CreateBehavior::SucceedButNodeNeverJoins => Ok(node_id),
            CreateBehavior::SucceedAfterDelay(d) => {
                tokio::time::sleep(d).await;
                Ok(node_id)
            }
            CreateBehavior::OfferingUnavailable => Err(ProviderError::OfferingUnavailable(
                format!("{} not available", offering.instance_type),
            )),
            CreateBehavior::CreationFailed(msg) => {
                Err(ProviderError::CreationFailed { message: msg })
            }
            CreateBehavior::JoinTimeout => Err(ProviderError::JoinTimeout { node_id: None }),
            CreateBehavior::InternalError(msg) => {
                Err(ProviderError::Internal(anyhow::anyhow!(msg)))
            }
        };

        let result_node_id = result.as_ref().ok().cloned();
        self.state.lock().unwrap().create.log(CreateCall {
            offering: offering.clone(),
            result_node_id,
            config_labels: config.labels.clone(),
        });

        result
    }

    pub async fn delete(&self, node_id: &NodeId) -> Result<(), ProviderError> {
        let behavior = self.state.lock().unwrap().delete.next();

        self.state.lock().unwrap().delete.log(DeleteCall {
            node_id: node_id.clone(),
        });

        match behavior {
            DeleteBehavior::Succeed | DeleteBehavior::Noop => Ok(()),
            DeleteBehavior::Fail(msg) => Err(ProviderError::DeletionFailed { message: msg }),
        }
    }

    pub async fn status(&self, node_id: &NodeId) -> Result<ProviderStatus, ProviderError> {
        let behavior = self.state.lock().unwrap().status.next();

        self.state.lock().unwrap().status.log(StatusCall {
            node_id: node_id.clone(),
        });

        match behavior {
            StatusBehavior::Return(status) => Ok(status),
            StatusBehavior::InternalError(msg) => {
                Err(ProviderError::Internal(anyhow::anyhow!(msg)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offering::{InstanceType, Location, Region, Resources, Zone};

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
            location: Location {
                region: Region("eu-central".into()),
                zone: Some(Zone("fsn1-dc14".into())),
            },
        }
    }

    // ── BehaviorQueue generic tests ────────────────────────────────────

    #[test]
    fn queue_pops_in_order() {
        let mut q = BehaviorQueue::<u32, ()>::new(0);
        q.push(1);
        q.push(2);
        q.push(3);
        assert_eq!(q.next(), 1);
        assert_eq!(q.next(), 2);
        assert_eq!(q.next(), 3);
    }

    #[test]
    fn queue_falls_back_to_default() {
        let mut q = BehaviorQueue::<u32, ()>::new(42);
        q.push(1);
        assert_eq!(q.next(), 1);
        assert_eq!(q.next(), 42);
        assert_eq!(q.next(), 42);
    }

    #[test]
    fn queue_set_default_changes_fallback() {
        let mut q = BehaviorQueue::<u32, ()>::new(0);
        q.set_default(99);
        assert_eq!(q.next(), 99);
    }

    #[test]
    fn queue_logs_and_retrieves_calls() {
        let mut q = BehaviorQueue::<(), String>::new(());
        q.log("first".into());
        q.log("second".into());
        assert_eq!(q.calls(), vec!["first".to_string(), "second".to_string()]);
    }

    // ── Create: behavior → Result mapping ──────────────────────────────

    #[tokio::test]
    async fn create_succeed_returns_node_id() {
        let provider = FakeProvider::new();
        let result = provider
            .create("my-node".into(), &test_offering(), &InstanceConfig::default())
            .await;
        assert_eq!(result.unwrap().0, "my-node");
    }

    #[tokio::test]
    async fn create_offering_unavailable() {
        let provider = FakeProvider::new().on_next_create(CreateBehavior::OfferingUnavailable);
        let result = provider
            .create("n".into(), &test_offering(), &InstanceConfig::default())
            .await;
        assert!(matches!(result, Err(ProviderError::OfferingUnavailable(_))));
    }

    #[tokio::test]
    async fn create_creation_failed() {
        let provider =
            FakeProvider::new().on_next_create(CreateBehavior::CreationFailed("boom".into()));
        let result = provider
            .create("n".into(), &test_offering(), &InstanceConfig::default())
            .await;
        assert!(matches!(
            result,
            Err(ProviderError::CreationFailed { message }) if message == "boom"
        ));
    }

    #[tokio::test]
    async fn create_join_timeout() {
        let provider = FakeProvider::new().on_next_create(CreateBehavior::JoinTimeout);
        let result = provider
            .create("n".into(), &test_offering(), &InstanceConfig::default())
            .await;
        assert!(matches!(result, Err(ProviderError::JoinTimeout { .. })));
    }

    #[tokio::test]
    async fn create_internal_error() {
        let provider =
            FakeProvider::new().on_next_create(CreateBehavior::InternalError("fail".into()));
        let result = provider
            .create("n".into(), &test_offering(), &InstanceConfig::default())
            .await;
        assert!(matches!(result, Err(ProviderError::Internal(_))));
    }

    #[tokio::test]
    async fn create_calls_are_logged() {
        let provider = FakeProvider::new();
        let offering = test_offering();
        provider
            .create("node-1".into(), &offering, &InstanceConfig::default())
            .await
            .unwrap();
        provider
            .create("node-2".into(), &offering, &InstanceConfig::default())
            .await
            .unwrap();

        let calls = provider.create_calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].offering, offering);
        assert!(calls[1].result_node_id.is_some());
    }

    // ── Delete: behavior → Result mapping ──────────────────────────────

    #[tokio::test]
    async fn delete_succeed() {
        let provider = FakeProvider::new();
        let result = provider.delete(&NodeId("n".into())).await;
        assert!(result.is_ok());
        assert_eq!(provider.delete_calls().len(), 1);
    }

    #[tokio::test]
    async fn delete_fail() {
        let provider = FakeProvider::new().on_next_delete(DeleteBehavior::Fail("boom".into()));
        let result = provider.delete(&NodeId("n".into())).await;
        assert!(matches!(
            result,
            Err(ProviderError::DeletionFailed { message }) if message == "boom"
        ));
    }

    #[tokio::test]
    async fn delete_noop_returns_ok() {
        let provider = FakeProvider::new().on_next_delete(DeleteBehavior::Noop);
        assert!(provider.delete(&NodeId("n".into())).await.is_ok());
    }

    // ── Status: behavior → Result mapping ──────────────────────────────

    #[tokio::test]
    async fn status_returns_given_status() {
        let provider =
            FakeProvider::new().on_next_status(StatusBehavior::Return(ProviderStatus::Creating));
        let result = provider.status(&NodeId("n".into())).await.unwrap();
        assert_eq!(result, ProviderStatus::Creating);
        assert_eq!(provider.status_calls().len(), 1);
    }

    #[tokio::test]
    async fn status_internal_error() {
        let provider =
            FakeProvider::new().on_next_status(StatusBehavior::InternalError("boom".into()));
        let result = provider.status(&NodeId("n".into())).await;
        assert!(matches!(result, Err(ProviderError::Internal(_))));
    }

    // ── Offerings ──────────────────────────────────────────────────────

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
