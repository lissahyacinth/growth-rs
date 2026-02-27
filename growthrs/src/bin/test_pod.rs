use std::collections::BTreeMap;

use anyhow::Result;
use k8s_openapi::api::core::v1::{Container, Node, Pod, PodSpec, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{DeleteParams, ListParams, ObjectMeta, PostParams};
use kube::{Api, Client};

use growthrs::node_pool::NodePool;
use growthrs::node_pool::{NodePoolSpec, ServerTypeConfig};
use growthrs::node_request::NodeRequest;
use growthrs::offering::POOL_LABEL;

/// Validate that a string looks like a Kubernetes resource quantity.
/// Catches common mistakes like forgetting the unit suffix on memory.
fn validate_quantity(value: &str, field: &str) -> Result<()> {
    // Must start with a digit and not be empty.
    anyhow::ensure!(
        !value.is_empty() && value.as_bytes()[0].is_ascii_digit(),
        "{field} value {value:?} is not a valid Kubernetes quantity \
         (expected e.g. \"1\", \"500m\", \"4096Mi\", \"2Gi\")"
    );
    Ok(())
}

async fn create_test_pod(
    client: Client,
    name: &str,
    cpu: &str,
    memory: &str,
    gpu: Option<u32>,
    pool: Option<&str>,
) -> Result<()> {
    validate_quantity(cpu, "cpu")?;
    validate_quantity(memory, "memory")?;

    let pods: Api<Pod> = Api::default_namespaced(client);

    let mut requests = BTreeMap::from([
        ("cpu".into(), Quantity(cpu.into())),
        ("memory".into(), Quantity(memory.into())),
    ]);
    if let Some(n) = gpu {
        requests.insert("nvidia.com/gpu".into(), Quantity(n.to_string()));
    }

    let node_selector = pool.map(|p| BTreeMap::from([(POOL_LABEL.to_string(), p.to_string())]));

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/managed-by".into(),
                "growth-test".into(),
            )])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "worker".into(),
                image: Some("busybox".into()),
                command: Some(vec!["sleep".into(), "infinity".into()]),
                resources: Some(ResourceRequirements {
                    requests: Some(requests),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            node_selector,
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &pod).await?;
    println!("created test pod {name} (cpu={cpu}, memory={memory}, gpu={gpu:?}, pool={pool:?})");
    Ok(())
}

async fn delete_test_pod(client: Client, name: &str) -> Result<()> {
    let pods: Api<Pod> = Api::default_namespaced(client);
    pods.delete(name, &DeleteParams::default()).await?;
    println!("deleted test pod {name}");
    Ok(())
}

/// Parse a server type spec like "cx22:10" or "cx22:2:10" (name:max or name:min:max).
fn parse_server_type(s: &str) -> Result<ServerTypeConfig> {
    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [name, max] => Ok(ServerTypeConfig {
            name: name.to_string(),
            max: max.parse()?,
            min: 0,
        }),
        [name, min, max] => Ok(ServerTypeConfig {
            name: name.to_string(),
            max: max.parse()?,
            min: min.parse()?,
        }),
        _ => anyhow::bail!("invalid server type spec {s:?}, expected NAME:MAX or NAME:MIN:MAX"),
    }
}

async fn create_node_pool(
    client: Client,
    name: &str,
    server_types: Vec<ServerTypeConfig>,
) -> Result<()> {
    let api: Api<NodePool> = Api::all(client);
    let np = NodePool::new(name, NodePoolSpec { server_types });
    api.create(&PostParams::default(), &np).await?;
    println!("created NodePool {name}");
    Ok(())
}

async fn delete_node_pool(client: Client, name: &str) -> Result<()> {
    let api: Api<NodePool> = Api::all(client);
    api.delete(name, &DeleteParams::default()).await?;
    println!("deleted NodePool {name}");
    Ok(())
}

async fn create_many_pods(
    client: Client,
    prefix: &str,
    count: u32,
    cpu: &str,
    memory: &str,
    gpu: Option<u32>,
    pool: Option<&str>,
) -> Result<()> {
    for i in 0..count {
        let name = format!("{prefix}-{i}");
        create_test_pod(client.clone(), &name, cpu, memory, gpu, pool).await?;
    }
    println!("created {count} pods with prefix {prefix:?}");
    Ok(())
}

async fn nuke(client: Client) -> Result<()> {
    // Delete test pods (managed-by=growth-test)
    let pods: Api<Pod> = Api::all(client.clone());
    let lp = ListParams::default().labels("app.kubernetes.io/managed-by=growth-test");
    let pod_list = pods.list(&lp).await?;
    let pod_count = pod_list.items.len();
    for pod in pod_list {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        let name = pod.metadata.name.as_deref().unwrap_or("?");
        let namespaced: Api<Pod> = Api::namespaced(client.clone(), ns);
        namespaced.delete(name, &DeleteParams::default()).await?;
    }
    println!("deleted {pod_count} test pods");

    // Delete growth-managed nodes (managed-by=growth)
    let nodes: Api<Node> = Api::all(client.clone());
    let lp = ListParams::default().labels("app.kubernetes.io/managed-by=growth");
    let node_list = nodes.list(&lp).await?;
    let node_count = node_list.items.len();
    for node in node_list {
        let name = node.metadata.name.as_deref().unwrap_or("?");
        nodes.delete(name, &DeleteParams::default()).await?;
    }
    println!("deleted {node_count} growth-managed nodes");

    // Delete all NodeRequests
    let nrs: Api<NodeRequest> = Api::all(client.clone());
    let nr_list = nrs.list(&ListParams::default()).await?;
    let nr_count = nr_list.items.len();
    for nr in nr_list {
        let name = nr.metadata.name.as_deref().unwrap_or("?");
        nrs.delete(name, &DeleteParams::default()).await?;
    }
    println!("deleted {nr_count} NodeRequests");

    // Delete all NodePools
    let nps: Api<NodePool> = Api::all(client.clone());
    let np_list = nps.list(&ListParams::default()).await?;
    let np_count = np_list.items.len();
    for np in np_list {
        let name = np.metadata.name.as_deref().unwrap_or("?");
        nps.delete(name, &DeleteParams::default()).await?;
    }
    println!("deleted {np_count} NodePools");

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
    eprintln!("  test_pod create-pool default cx22:10 cpx31:5");
    eprintln!("  test_pod create-pool gpu-workers gpu-a100:0:3");
    eprintln!("  test_pod delete-pool default");
    eprintln!("  test_pod nuke                          # delete all growth resources");
    std::process::exit(1);
}

fn find_flag<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter().position(|a| a == flag).map(|i| {
        args.get(i + 1)
            .expect(&format!("{flag} requires a value"))
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
            create_test_pod(client, name, cpu, memory, gpu, pool).await?;
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
            create_many_pods(client, prefix, count, cpu, memory, gpu, pool).await?;
        }
        "delete" => {
            if args.len() < 2 {
                usage();
            }
            delete_test_pod(client, &args[1]).await?;
        }
        "nuke" => {
            nuke(client).await?;
        }
        "create-pool" => {
            if args.len() < 3 {
                usage();
            }
            let name = &args[1];
            let server_types: Vec<ServerTypeConfig> = args[2..]
                .iter()
                .map(|s| parse_server_type(s))
                .collect::<Result<_>>()?;
            create_node_pool(client, name, server_types).await?;
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
