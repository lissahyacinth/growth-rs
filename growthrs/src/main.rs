use std::process::ExitCode;
use std::sync::Arc;

use growthrs::controller::errors::ControllerError;
use growthrs::controller::healthcheck;
use growthrs::{config::ControllerContext, controller};
use kube::Client;

async fn start_controller() -> Result<(), ControllerError> {
    let client = Client::try_default().await?;
    let controller_context = Arc::new(ControllerContext::new(client)?);

    // TODO: Gracefully drain in-flight NodeRequests, cancel pending provisions,
    // and clean up resources.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received shutdown signal, stopping controllers");
        }
        res = controller::run(controller_context) => res?,
        res = healthcheck::healthcheck() => {
            res.map_err(|e| ControllerError::Other(e))?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "growthrs=info".parse().unwrap()),
        )
        .compact()
        .with_target(false)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    if let Err(e) = start_controller().await {
        tracing::error!("GrowthRS exiting: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
