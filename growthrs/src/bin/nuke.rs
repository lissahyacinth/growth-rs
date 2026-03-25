//! Remove all GrowthRS Resources from a default Cluster
//!
//! Usage: cargo run --bin nuke
use anyhow::Result;
use kube::Client;

use growthrs::testing;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::try_default().await?;
    testing::nuke(client).await?;
    Ok(())
}
