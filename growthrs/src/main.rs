use kube::Client;
use tracing::{error, info};

use growthrs::controller::{self, ControllerContext};
use growthrs::providers::provider::Provider;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "growthrs=info".parse().unwrap()),
        )
        .compact()
        .with_target(false)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let provider_name = std::env::var("GROWTH_PROVIDER").unwrap_or_else(|_| "kwok".to_string());
    info!(version = env!("CARGO_PKG_VERSION"), provider = %provider_name, "growthrs starting");

    let client = Client::try_default().await.unwrap();
    let provider = match Provider::from_name(&provider_name, client.clone()) {
        Ok(p) => p,
        Err(e) => {
            error!(error = %e, "failed to create provider");
            std::process::exit(1);
        }
    };
    let ctx = ControllerContext {
        client,
        provider,
        provisioning_timeout: std::time::Duration::from_secs(300),
        scale_down: controller::ScaleDownConfig::default(),
    };

    // TODO: Handle CTRL-C (SIGINT) and SIGHUP/SIGTERM for graceful shutdown —
    // drain in-flight NodeRequests, cancel pending provisions, and clean up resources.

    // The controller watches for Pending Pod events and reconciles automatically.
    // To trigger manually, create an unschedulable pod — the watcher will pick it up.
    // For a one-shot reconcile without the watcher, use controller::controller_loop_single().
    if let Err(e) = controller::run(ctx).await {
        error!(error = %e, "controller failed");
        std::process::exit(1)
    }
}
