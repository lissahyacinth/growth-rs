use kube::Client;
use tracing::{error, info};

use growthrs::controller::{self, ControllerContext};
use growthrs::providers::kwok::KwokProvider;
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

    info!(version = env!("CARGO_PKG_VERSION"), provider = "kwok", "growthrs starting");

    let client = Client::try_default().await.unwrap();
    // TODO: Automatically select Provider from configuration.
    let provider = Provider::Kwok(KwokProvider::new(client.clone()));
    let ctx = ControllerContext { client, provider };

    // The controller watches for Pending Pod events and reconciles automatically.
    // To trigger manually, create an unschedulable pod — the watcher will pick it up.
    // For a one-shot reconcile without the watcher, use controller::controller_loop_single().
    if let Err(e) = controller::run(ctx).await {
        error!(error = %e, "controller failed");
        std::process::exit(1)
    }
}
