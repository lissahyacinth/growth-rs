use std::collections::BTreeMap;

use crate::offering::Offering;
use crate::providers::fake::FakeProvider;
use crate::providers::kwok::KwokProvider;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// Configuration for an Instance
#[derive(Default)]
pub struct InstanceConfig {
    /// Extra labels to apply to the created node.
    pub labels: BTreeMap<String, String>,
    /// Resolved user-data to pass to the provider (cloud-init, Talos config, etc.).
    pub user_data: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// The provider couldn't create the resource at all.
    /// Bad permissions, quota exceeded, invalid config, etc.
    #[error("creation failed: {message}")]
    CreationFailed { message: String },

    /// The provider couldn't delete the resource.
    #[error("deletion failed: {message}")]
    DeletionFailed { message: String },

    /// Resource was created but the node never joined the cluster.
    /// The provider should attempt cleanup before returning this.
    #[error("node failed to join cluster within timeout: {node_id:?}")]
    JoinTimeout { node_id: Option<NodeId> },

    /// The requested offering isn't available (sold out, wrong region, etc).
    #[error("offering unavailable: {0}")]
    OfferingUnavailable(String),

    /// Required config field missing for this provider.
    /// e.g. EKS without iam_identity, Hetzner without a way to bootstrap.
    #[error("missing required config: {field}")]
    MissingConfig { field: &'static str },

    /// The requested provider name doesn't match any known implementation.
    #[error("unknown provider: {0}")]
    UnknownProvider(String),

    /// Underlying API/network error.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Infrastructure-level status of a VM as reported by the provider.
///
/// This says nothing about whether a K8s Node has joined the cluster —
/// that's exclusively the Node Watcher's domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderStatus {
    /// VM is still being built/booted.
    Creating,
    /// VM is running (says nothing about K8s).
    Running,
    /// VM definitively failed.
    Failed { reason: String },
    /// Provider has no record of this VM.
    NotFound,
}

/// Provide Nodes from a given Provider - i.e. GCP, Hetzner, KWOK
/// The provider's responsibility is to join a node to the cluster, or for the joining to fail loudly.
pub enum Provider {
    Kwok(KwokProvider),
    Fake(FakeProvider),
}

impl Provider {
    /// Build a provider by name.  Errors if the name is unrecognised.
    pub fn from_name(name: &str, client: kube::Client) -> Result<Self, ProviderError> {
        match name {
            "kwok" => Ok(Self::Kwok(KwokProvider::new(client))),
            other => Err(ProviderError::UnknownProvider(other.to_string())),
        }
    }

    pub async fn offerings(&self) -> Vec<Offering> {
        match self {
            Self::Kwok(p) => p.offerings().await,
            Self::Fake(p) => p.offerings().await,
        }
    }

    /// Asynchronously request a node be created
    pub async fn create(
        &self,
        node_id: String,
        offering: &Offering,
        config: &InstanceConfig,
    ) -> Result<NodeId, ProviderError> {
        match self {
            Self::Kwok(p) => p.create(node_id, offering, config).await,
            Self::Fake(p) => p.create(node_id, offering, config).await,
        }
    }

    /// Delete a node by its ID
    pub async fn delete(&self, node_id: &NodeId) -> Result<(), ProviderError> {
        match self {
            Self::Kwok(p) => p.delete(node_id).await,
            Self::Fake(p) => p.delete(node_id).await,
        }
    }

    /// Query the infrastructure-level status of a VM.
    pub async fn status(&self, node_id: &NodeId) -> Result<ProviderStatus, ProviderError> {
        match self {
            Self::Kwok(p) => p.status(node_id).await,
            Self::Fake(p) => p.status(node_id).await,
        }
    }
}
