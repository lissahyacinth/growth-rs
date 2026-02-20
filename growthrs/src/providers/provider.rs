use crate::offering::Offering;
use crate::providers::fake::FakeProvider;
use crate::providers::kwok::KwokProvider;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// Configuration for an Instance
pub struct InstanceConfig {}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// The provider couldn't create the resource at all.
    /// Bad permissions, quota exceeded, invalid config, etc.
    #[error("creation failed: {message}")]
    CreationFailed { message: String },

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

    /// Underlying API/network error.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Provide Nodes from a given Provider - i.e. GCP, Hetzner, KWOK
/// The provider's responsibility is to join a node to the cluster, or for the joining to fail loudly.
pub enum Provider {
    Kwok(KwokProvider),
    Fake(FakeProvider),
}

impl Provider {
    pub async fn offerings(&self) -> Vec<Offering> {
        match self {
            Self::Kwok(p) => p.offerings().await,
            Self::Fake(p) => p.offerings().await,
        }
    }

    /// Asynchronously request a node be created
    pub async fn create(
        &self,
        offering: &Offering,
        config: &InstanceConfig,
    ) -> Result<NodeId, ProviderError> {
        match self {
            Self::Kwok(p) => p.create(offering, config).await,
            Self::Fake(p) => p.create(offering, config).await,
        }
    }

    /// Delete a node by its ID
    pub async fn delete(&self, node_id: &NodeId) -> Result<(), ProviderError> {
        match self {
            Self::Kwok(p) => p.delete(node_id).await,
            Self::Fake(p) => p.delete(node_id).await,
        }
    }
}
