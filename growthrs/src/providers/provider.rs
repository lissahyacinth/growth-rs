use std::collections::BTreeMap;

use crate::offering::Offering;
use crate::providers::fake::FakeProvider;
use crate::providers::hetzner::HetznerProvider;
use crate::providers::hetzner::config::HetznerCreateConfig;
use crate::providers::kwok::KwokProvider;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// Generic instance configuration shared across all providers.
///
/// Contains only provider-agnostic fields. Provider-specific configuration
/// (networking, images, SSH keys, etc.) lives in `ProviderCreateConfig`.
#[derive(Default)]
pub struct InstanceConfig {
    /// Extra labels to apply to the created node.
    pub labels: BTreeMap<String, String>,
}

/// Provider-specific configuration resolved from CRDs at provision time.
///
/// Each variant carries the configuration needed by a specific provider's
/// `create()` method. Produced by the controller's NodeClass resolution
/// and consumed by `Provider::create()`.
pub enum ProviderCreateConfig {
    /// No provider-specific config needed (KWOK, Fake).
    None,
    /// Hetzner Cloud instance configuration (from HetznerNodeClass CRD).
    Hetzner(HetznerCreateConfig),
}

/// Configuration needed to construct a `Provider`.
pub struct ProviderConfig {
    pub kube_client: kube::Client,
    /// Hetzner Cloud API Token
    pub hcloud_token: Option<String>,
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
    /// VM is still being built/starting up.
    Creating,
    /// VM is running (says nothing about K8s).
    Running,
    /// VM is being Deleted or Stopped.
    Removing,
    /// Provider cannot find the VM - i.e. it has been deleted.
    NotFound,
    /// VM failed for a Provider Specific Reason.
    Failed { reason: String },
}

/// Provide Nodes from a given Provider - i.e. Hetzner, KWOK
/// The provider's responsibility is to join a node to the cluster, or for the joining to fail loudly.
pub enum Provider {
    /// [Kwok](https://kwok.sigs.k8s.io/) providers allow for testing on simulated nodes
    Kwok(KwokProvider),
    /// Fake Providers are used internally for rapid testing
    Fake(FakeProvider),
    /// [Hetzner](https://www.hetzner.com/cloud) Nodes
    Hetzner(HetznerProvider),
}

pub(crate) enum ProviderName {
    Kwok,
    Fake,
    Hetzner,
}

impl ProviderName {
    pub(crate) fn from_name(name: &str) -> Result<Self, ProviderError> {
        match name.to_ascii_lowercase().as_str() {
            "kwok" => Ok(ProviderName::Kwok),
            "fake" => Ok(ProviderName::Fake),
            "hetzner" => Ok(ProviderName::Hetzner),
            _ => Err(ProviderError::UnknownProvider(name.to_string())),
        }
    }
}

impl Provider {
    /// Build a provider by name. Errors if the name is unrecognised.
    pub fn from_name(name: &str, config: ProviderConfig) -> Result<Self, ProviderError> {
        match name {
            "kwok" => Ok(Self::Kwok(KwokProvider::new(config.kube_client))),
            "hetzner" => {
                let token = config.hcloud_token.ok_or(ProviderError::MissingConfig {
                    field: "HCLOUD_TOKEN",
                })?;
                Ok(Self::Hetzner(HetznerProvider::new(token)))
            }
            other => Err(ProviderError::UnknownProvider(other.to_string())),
        }
    }

    // TODO: This should be cached.
    pub async fn offerings(&self) -> Vec<Offering> {
        match self {
            Self::Kwok(p) => p.offerings().await,
            Self::Fake(p) => p.offerings().await,
            Self::Hetzner(p) => p.offerings().await,
        }
    }

    /// Asynchronously request a node be created.
    pub async fn create(
        &self,
        node_id: String,
        offering: &Offering,
        config: &InstanceConfig,
        provider_config: &ProviderCreateConfig,
    ) -> Result<NodeId, ProviderError> {
        match self {
            Self::Kwok(p) => p.create(node_id, offering, config).await,
            Self::Fake(p) => p.create(node_id, offering, config).await,
            Self::Hetzner(p) => {
                let hetzner_config = match provider_config {
                    ProviderCreateConfig::Hetzner(c) => c,
                    _ => {
                        return Err(ProviderError::MissingConfig {
                            field: "HetznerCreateConfig",
                        });
                    }
                };
                p.create(node_id, offering, config, hetzner_config).await
            }
        }
    }

    /// Delete a node by its ID
    pub async fn delete(&self, node_id: &NodeId) -> Result<(), ProviderError> {
        match self {
            Self::Kwok(p) => p.delete(node_id).await,
            Self::Fake(p) => p.delete(node_id).await,
            Self::Hetzner(p) => p.delete(node_id).await,
        }
    }

    /// Query the infrastructure-level status of a VM.
    pub async fn status(&self, node_id: &NodeId) -> Result<ProviderStatus, ProviderError> {
        match self {
            Self::Kwok(p) => p.status(node_id).await,
            Self::Fake(p) => p.status(node_id).await,
            Self::Hetzner(p) => p.status(node_id).await,
        }
    }
}
