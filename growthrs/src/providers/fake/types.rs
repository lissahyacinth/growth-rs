use std::collections::BTreeMap;
use std::time::Duration;

use crate::offering::Offering;
use crate::providers::provider::{NodeId, ProviderStatus};

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

/// What happens on the next `status()` call.
#[derive(Debug, Clone)]
pub enum StatusBehavior {
    /// Returns the given ProviderStatus.
    Return(ProviderStatus),
    /// Network/API blowup.
    InternalError(String),
}

/// How `offerings()` behaves.
#[derive(Debug, Clone)]
pub enum OfferingsBehavior {
    /// Returns the same set every call.
    Static(Vec<Offering>),
    /// Returns successive elements; sticks on the last one when exhausted.
    Sequence(std::collections::VecDeque<Vec<Offering>>),
}

/// Logged record of a `create()` call.
#[derive(Debug, Clone)]
pub struct CreateCall {
    pub offering: Offering,
    pub result_node_id: Option<NodeId>,
    pub config_labels: BTreeMap<String, String>,
}

/// Logged record of a `delete()` call.
#[derive(Debug, Clone)]
pub struct DeleteCall {
    pub node_id: NodeId,
}

/// Logged record of a `status()` call.
#[derive(Debug, Clone)]
pub struct StatusCall {
    pub node_id: NodeId,
}
