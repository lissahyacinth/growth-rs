use crate::offering::Offering;
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
pub(crate) enum Provider {
    Kwok(KwokProvider),
}

impl Provider {
    pub async fn offerings(&self) -> Vec<Offering> {
        match self {
            Self::Kwok(p) => p.offerings().await,
        }
    }

    pub async fn create(
        &self,
        offering: &Offering,
        config: &InstanceConfig,
    ) -> Result<NodeId, ProviderError> {
        match self {
            Self::Kwok(p) => p.create(offering, config).await,
        }
    }
}
