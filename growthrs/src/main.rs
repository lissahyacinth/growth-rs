use crate::{offering::Region, providers::kwok::KwokProvider};
use kube::Client;
use crate::providers::provider::{InstanceConfig, Provider};

mod offering;
mod providers;

#[tokio::main]
async fn main() {
    let client = Client::try_default().await.unwrap();
    let provider = KwokProvider::new(client.clone());
    let offerings = provider.offerings(&Region("".to_string())).await;
    let config = InstanceConfig {};
    provider.create(&offerings[0], &config).await.unwrap();
}
