use std::thread::sleep;
use std::time::Duration;

use kube::Client;

use crate::controller::{controller_loop, create_test_pod, delete_test_pod};
use crate::providers::kwok::KwokProvider;
use crate::providers::provider::{InstanceConfig, Provider};

mod controller;
mod offering;
mod optimiser;
mod providers;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "growthrs=debug".parse().unwrap()),
        )
        .init();

    let client = Client::try_default().await.unwrap();
    let provider = Provider::Kwok(KwokProvider::new(client.clone()));
    //let offerings = provider.offerings().await.into_iter().filter(|offering| offering.resources.cpu >= 48 && offering.resources.memory_mib >= 65536).next();
    //provider.create(&offerings.unwrap(), &InstanceConfig {}).await.unwrap();
    // Delete - throwing away errors
    delete_test_pod(client.clone(), "gpu-test")
        .await
        .unwrap_or(());
    // Test pod is rather large,
    create_test_pod(client.clone(), "gpu-test", "48", "64Gi", None)
        .await
        .unwrap();
    sleep(Duration::from_secs(5));
    controller_loop(client.clone(), &provider).await.unwrap();
    delete_test_pod(client.clone(), "gpu-test")
        .await
        .unwrap_or(());
}
