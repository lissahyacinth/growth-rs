//! Generate CRD YAML manifests from the Rust type definitions.
//!
//! Usage: cargo run --bin gen_crds > deploy/crds.yaml

use kube::CustomResourceExt;

use growthrs::resources::hetzner_node_class::HetznerNodeClass;
use growthrs::resources::node_pool::NodePool;
use growthrs::resources::node_removal_request::NodeRemovalRequest;
use growthrs::resources::node_request::NodeRequest;

fn main() {
    let crds = vec![
        serde_yaml::to_string(&NodeRequest::crd()).unwrap(),
        serde_yaml::to_string(&NodePool::crd()).unwrap(),
        serde_yaml::to_string(&NodeRemovalRequest::crd()).unwrap(),
        serde_yaml::to_string(&HetznerNodeClass::crd()).unwrap(),
    ];
    for (i, crd) in crds.iter().enumerate() {
        if i > 0 {
            println!("---");
        }
        print!("{crd}");
    }
}
