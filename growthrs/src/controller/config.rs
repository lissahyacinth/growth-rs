use std::time::Duration;

/// Configuration for scale-down behavior.
pub struct ScaleDownConfig {
    /// How long a node must be idle before deprovisioning begins (default 15s).
    pub cooling_off_duration: Duration,
    /// Maximum number of provider.delete() retries before giving up.
    pub max_removal_attempts: u32,
}

impl Default for ScaleDownConfig {
    fn default() -> Self {
        Self {
            cooling_off_duration: Duration::from_secs(15),
            max_removal_attempts: 5,
        }
    }
}
