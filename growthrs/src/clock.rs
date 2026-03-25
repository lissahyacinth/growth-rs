#[cfg(feature = "testing")]
use std::sync::atomic::AtomicU64;

use k8s_openapi::jiff::Timestamp;

/// Abstraction over wall-clock time
///
/// Production code uses `SystemClock`, and tests can inject a `FakeClock`
/// to make time-dependent logic deterministic.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// Production clock — delegates to `jiff::Timestamp::now()`.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }
}

#[cfg(feature = "testing")]
pub struct TestClock {
    delta: AtomicU64, // Represents seconds - Atomic so we can modify in tests.
}

#[cfg(feature = "testing")]
impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp::now()
            + std::time::Duration::from_secs(self.delta.load(std::sync::atomic::Ordering::Relaxed))
    }
}

#[cfg(feature = "testing")]
impl TestClock {
    pub fn new() -> Self {
        Self {
            delta: AtomicU64::new(0),
        }
    }

    /// Advance the clock by the given duration.
    pub fn tick(&self, duration: std::time::Duration) {
        self.delta
            .fetch_add(duration.as_secs(), std::sync::atomic::Ordering::Relaxed);
    }
}
