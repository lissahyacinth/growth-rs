//! Create Test Resources for a Cluster
//!
//! Usage: cargo run --bin test_pod --features="testing"
//!
//! Create a Test Pool;
//! cargo run --bin test_pod --features="testing" create-pool <name> <type:max> [<type:min:max> ...]
//!
//! KWOK is pegged to Hetzner - so we can still use Hetzner server types
//! i.e. `cargo run --bin test_pod --features="testing" create-pool test-pool cpx22:5`
//!
//! Create a Test Pod;
//! cargo run --bin test_pod --features="testing" create <name> <cpu> <memory> [--gpu <n>] [--pool <pool>]
//!
//! i.e. `cargo run --bin test_pod --features="testing" create test-pod 1 1024 --gpu 1 --pool test-pool`
use std::collections::BTreeMap;

use anyhow::Result;
use k8s_openapi::api::core::v1::Pod;
use kube::api::DeleteParams;
use kube::{Api, Client};

use growthrs::resources::node_pool::{NodePool, ServerTypeConfig};
use growthrs::testing;

async fn delete_test_pod(client: Client, name: &str) -> Result<()> {
    let pods: Api<Pod> = Api::default_namespaced(client);
    pods.delete(name, &DeleteParams::default()).await?;
    println!("deleted test pod {name}");
    Ok(())
}

async fn delete_node_pool(client: Client, name: &str) -> Result<()> {
    let api: Api<NodePool> = Api::all(client);
    api.delete(name, &DeleteParams::default()).await?;
    println!("deleted NodePool {name}");
    Ok(())
}

fn usage() -> ! {
    eprintln!("Usage:");
    eprintln!("  test_pod create <name> <cpu> <memory> [--gpu <n>] [--pool <pool>]");
    eprintln!(
        "  test_pod create-many <count> <cpu> <memory> [--gpu <n>] [--pool <pool>] [--prefix <p>]"
    );
    eprintln!("  test_pod delete <name>");
    eprintln!("  test_pod create-pool <name> <type:max> [<type:min:max> ...]");
    eprintln!("  test_pod delete-pool <name>");
    eprintln!("  test_pod nuke");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  test_pod create my-pod 2 4096Mi");
    eprintln!("  test_pod create gpu-pod 4 8192Mi --gpu 1 --pool gpu-workers");
    eprintln!("  test_pod create-many 20 1 512Mi --pool default");
    eprintln!("  test_pod create-many 32 1 4096Mi --pool cpu-workers --prefix worker");
    eprintln!("  test_pod delete my-pod");
    eprintln!("  test_pod create-pool default cpx22:10 cpx31:5");
    eprintln!("  test_pod create-pool gpu-workers gpu-a100:0:3");
    eprintln!("  test_pod delete-pool default");
    eprintln!("  test_pod nuke                          # delete all growth resources");
    std::process::exit(1);
}

fn find_flag<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter().position(|a| a == flag).map(|i| {
        args.get(i + 1)
            .unwrap_or_else(|| panic!("{flag} requires a value"))
            .as_str()
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        usage();
    }

    let client = Client::try_default().await?;

    match args[0].as_str() {
        "create" => {
            if args.len() < 4 {
                usage();
            }
            let name = &args[1];
            let cpu = &args[2];
            let memory = &args[3];
            let gpu =
                find_flag(&args, "--gpu").map(|v| v.parse::<u32>().expect("gpu must be a number"));
            let pool = find_flag(&args, "--pool");
            testing::create_pod(client, name, cpu, memory, gpu, pool).await?;
        }
        "create-many" => {
            if args.len() < 4 {
                usage();
            }
            let count: u32 = args[1].parse().expect("count must be a number");
            let cpu = &args[2];
            let memory = &args[3];
            let gpu =
                find_flag(&args, "--gpu").map(|v| v.parse::<u32>().expect("gpu must be a number"));
            let pool = find_flag(&args, "--pool");
            let prefix = find_flag(&args, "--prefix").unwrap_or("pod");
            testing::create_many_pods(client, prefix, count, cpu, memory, gpu, pool).await?;
        }
        "delete" => {
            if args.len() < 2 {
                usage();
            }
            delete_test_pod(client, &args[1]).await?;
        }
        "nuke" => {
            testing::nuke(client).await?;
        }
        "create-pool" => {
            if args.len() < 3 {
                usage();
            }
            let name = &args[1];
            let server_types: Vec<ServerTypeConfig> = args[2..]
                .iter()
                .map(|s| testing::parse_server_type(s))
                .collect::<Result<_>>()?;
            testing::create_node_pool(client, name, server_types, BTreeMap::new()).await?;
        }
        "delete-pool" => {
            if args.len() < 2 {
                usage();
            }
            delete_node_pool(client, &args[1]).await?;
        }
        _ => usage(),
    }

    Ok(())
}
