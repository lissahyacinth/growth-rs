use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::runtime::watcher;
use tokio::time::{Instant, sleep};
use tracing::{info, warn};

use crate::controller::pods;
use crate::controller::{ControllerContext, ControllerError};

enum TriggerType {
    Batch,
    MaxWindow,
}

const TIMEOUT: Duration = Duration::from_millis(500);
const MAX_WINDOW: Duration = Duration::from_secs(10);

/// Watch Pending pods and reconcile in batched windows, producing NodeRequests.
///
/// Events are coalesced so that a burst of pods becoming unschedulable
/// produces a single reconcile against fresh API state, avoiding duplicate
/// NodeRequests from stale informer caches.
pub async fn run_pod_watcher(ctx: Arc<ControllerContext>) -> Result<(), ControllerError> {
    let pods: Api<Pod> = Api::all(ctx.client.clone());
    let config = watcher::Config::default().fields("status.phase=Pending");
    let mut stream = std::pin::pin!(watcher::watcher(pods, config));

    // When an event is received, wait up to `TIMEOUT` without another event arriving.
    let mut delay = ::std::pin::pin!(sleep(TIMEOUT));
    // When an event is received, set max_delay, ensuring we act at least every `MAX_WINDOW`.
    let mut max_delay = ::std::pin::pin!(sleep(Duration::from_millis(0))); // Immediately expire this.

    let mut pending = false;
    let mut trigger: Option<TriggerType> = None;

    let mut recent_creates = pods::init_recent_creates();

    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(event)) => {
                        if matches!(event, watcher::Event::Apply(_) | watcher::Event::InitApply(_)) {
                            if !pending {
                                max_delay.as_mut().reset(Instant::now() + MAX_WINDOW);
                            }
                            pending = true;
                            delay.as_mut().reset(Instant::now() + TIMEOUT);
                        }
                    },
                    Some(Err(e)) => {
                        warn!(error = %e, "pod watcher stream error")
                    }
                    None => break,
                }
            }
            _ = &mut delay, if pending => {
                pending = false;
                info!(trigger = "batch_timeout", "starting pod reconciliation");
                trigger = Some(TriggerType::Batch);
            }
            _ = &mut max_delay, if pending => {
                pending = false;
                info!(trigger = "max_window", "starting pod reconciliation");
                trigger = Some(TriggerType::MaxWindow);
                delay.as_mut().reset(Instant::now() + TIMEOUT);
            }
        }
        if trigger.is_some() {
            match pods::reconcile_unschedulable_pods(
                ctx.client.clone(),
                &ctx.provider,
                &mut recent_creates,
                ctx.scale_down.unmet_ttl,
                ctx.clock.now(),
            )
            .await
            {
                Ok(()) => {}
                Err(ControllerError::FaultInjected(n)) => {
                    warn!(n, "fault injection triggered, exiting watcher");
                    return Ok(());
                }
                Err(e) => {
                    warn!(error = %e, "pod reconciliation failed");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
    Ok(())
}
