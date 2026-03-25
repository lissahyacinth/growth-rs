use envconfig::Envconfig;
use kube::Client;
use std::{sync::Arc, time::Duration};

use crate::{
    clock::{Clock, SystemClock},
    controller::errors::ConfigError,
    providers::{
        fake::FakeProvider,
        hetzner::HetznerProvider,
        kwok::KwokProvider,
        provider::{Provider, ProviderName},
    },
};

#[derive(Envconfig)]
struct RawConfig {
    #[envconfig(from = "GROWTH_PROVIDER")]
    /// Provider being used (kwok/fake/hetzner)
    pub provider: String,
    #[envconfig(from = "GROWTH_PROVISIONING_TIMEOUT")]
    /// Provisioning timeout in seconds
    pub provisioning_timeout: u64,
}

#[derive(Envconfig)]
struct HetznerConfig {
    #[envconfig(from = "HCLOUD_TOKEN")]
    pub token: String,
    // SSH keys, image, etc.
}

/// Configuration for scale-down behavior.
#[derive(Envconfig)]
struct ScaleDownConfigBuilder {
    #[envconfig(from = "GROWTH_COOLING_DURATION")]
    cooling_off_duration: u64,
    #[envconfig(from = "GROWTH_REMOVAL_ATTEMPTS")]
    max_removal_attempts: u32,
    #[envconfig(from = "GROWTH_UNMET_TTL")]
    unmet_ttl: u64,
}

impl ScaleDownConfigBuilder {
    pub fn build(&self) -> ScaleDownConfig {
        ScaleDownConfig {
            cooling_off_duration: Duration::from_secs(self.cooling_off_duration),
            max_removal_attempts: self.max_removal_attempts,
            unmet_ttl: Duration::from_secs(self.unmet_ttl),
        }
    }
}

pub struct ScaleDownConfig {
    /// How long a node must be idle before deprovisioning begins (default 15s).
    pub cooling_off_duration: Duration,
    /// Maximum number of provider.delete() retries before giving up.
    pub max_removal_attempts: u32,
    /// How long an Unmet NodeRequest keeps its pods claimed before deletion (default 120s).
    pub unmet_ttl: Duration,
}

impl Default for ScaleDownConfig {
    fn default() -> Self {
        Self {
            cooling_off_duration: Duration::from_secs(15),
            max_removal_attempts: 5,
            unmet_ttl: Duration::from_secs(120),
        }
    }
}

/// Shared context for the controller reconciler.
pub struct ControllerContext {
    pub client: Client,
    pub provider: Provider,
    pub provisioning_timeout: Duration,
    pub scale_down: ScaleDownConfig,
    pub clock: Arc<dyn Clock>,
}

impl ControllerContext {
    pub fn new(client: kube::Client) -> Result<Self, ConfigError> {
        let raw = RawConfig::init_from_env()?;
        let scale_down = ScaleDownConfigBuilder::init_from_env()?.build();
        let provisioning_timeout = std::time::Duration::from_secs(raw.provisioning_timeout);

        let provider = match ProviderName::from_name(&raw.provider)
            .map_err(|e| ConfigError::Other(e.to_string()))?
        {
            ProviderName::Kwok => Provider::Kwok(KwokProvider::new(client.clone())),
            ProviderName::Fake => Provider::Fake(FakeProvider::new()),
            ProviderName::Hetzner => {
                let hetzner_config = HetznerConfig::init_from_env()?;
                Provider::Hetzner(HetznerProvider::new(hetzner_config.token))
            }
        };

        Ok(ControllerContext {
            client,
            provider,
            provisioning_timeout,
            scale_down,
            clock: Arc::new(SystemClock),
        })
    }
}
