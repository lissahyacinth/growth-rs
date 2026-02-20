use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Node, NodeStatus};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{DeleteParams, ObjectMeta, PostParams};
use kube::{Api, Client};

use crate::offering::{GpuModel, InstanceType, Offering, Region, Resources};
use crate::providers::provider::{InstanceConfig, NodeId, ProviderError};

fn offering(name: &str, cpu: u32, memory_mib: u32, disk_gib: u32, cost_per_hour: f64) -> Offering {
    Offering {
        instance_type: InstanceType(name.into()),
        resources: Resources {
            cpu,
            memory_mib,
            ephemeral_storage_gib: Some(disk_gib),
            gpu: 0,
            gpu_model: None,
        },
        cost_per_hour,
    }
}

fn gpu_offering(
    name: &str,
    cpu: u32,
    memory_mib: u32,
    disk_gib: u32,
    gpu: u32,
    gpu_model: GpuModel,
    cost_per_hour: f64,
) -> Offering {
    Offering {
        instance_type: InstanceType(name.into()),
        resources: Resources {
            cpu,
            memory_mib,
            ephemeral_storage_gib: Some(disk_gib),
            gpu,
            gpu_model: Some(gpu_model),
        },
        cost_per_hour,
    }
}

fn to_capacity(res: &Resources) -> BTreeMap<String, Quantity> {
    let mut cap = BTreeMap::from([
        ("cpu".into(), Quantity(res.cpu.to_string())),
        ("memory".into(), Quantity(format!("{}Mi", res.memory_mib))),
    ]);
    if let Some(gib) = res.ephemeral_storage_gib {
        cap.insert("ephemeral-storage".into(), Quantity(format!("{gib}Gi")));
    }
    if res.gpu > 0 {
        cap.insert("nvidia.com/gpu".into(), Quantity(res.gpu.to_string()));
    }
    cap
}

/// Kubernetes With Out Kubelet Provider
pub struct KwokProvider {
    client: Client,
}

impl KwokProvider {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl KwokProvider {
    pub async fn offerings(&self) -> Vec<Offering> {
        vec![
            // CX – Shared x86                              $/hr
            offering("cx22", 2, 4_096, 40, 0.0066),
            offering("cx32", 4, 8_192, 80, 0.0106),
            offering("cx42", 8, 16_384, 160, 0.0170),
            offering("cx52", 16, 32_768, 320, 0.0314),
            // CPX – Shared AMD
            offering("cpx12", 2, 2_048, 40, 0.0122),
            offering("cpx22", 3, 4_096, 80, 0.0226),
            offering("cpx32", 4, 8_192, 160, 0.0299),
            offering("cpx42", 8, 16_384, 256, 0.0362),
            offering("cpx52", 16, 32_768, 360, 0.0515),
            // CAX – ARM (Ampere)
            offering("cax11", 2, 4_096, 40, 0.0074),
            offering("cax21", 4, 8_192, 80, 0.0122),
            offering("cax31", 8, 16_384, 160, 0.0226),
            offering("cax41", 16, 32_768, 320, 0.0443),
            // CCX – Dedicated x86
            offering("ccx13", 2, 8_192, 80, 0.0386),
            offering("ccx23", 4, 16_384, 160, 0.0475),
            offering("ccx33", 8, 32_768, 240, 0.0900),
            offering("ccx43", 16, 65_536, 360, 0.1789),
            offering("ccx53", 32, 131_072, 600, 0.3568),
            offering("ccx63", 48, 196_608, 960, 0.5347),
            // GPU (fictional, for testing GPU scheduling)
            gpu_offering(
                "gpu-a100-1",
                12,
                131_072,
                200,
                1,
                GpuModel::NvidiaA100,
                2.21,
            ),
            gpu_offering(
                "gpu-a100-4",
                48,
                524_288,
                800,
                4,
                GpuModel::NvidiaA100,
                8.84,
            ),
        ]
    }
    pub async fn create(
        &self,
        offering: &Offering,
        _config: &InstanceConfig,
    ) -> Result<NodeId, ProviderError> {
        let mut capacity = to_capacity(&offering.resources);
        capacity.insert("pods".into(), Quantity("110".into()));
        let allocatable = capacity.clone();

        let nodes: Api<Node> = Api::all(self.client.clone());
        let node = Node {
            metadata: ObjectMeta {
                name: Some(format!("growth-kwok-{}", uuid::Uuid::new_v4())),
                labels: Some(BTreeMap::from([
                    ("type".into(), "kwok".into()),
                    ("app.kubernetes.io/managed-by".into(), "growth".into()),
                ])),
                annotations: Some(BTreeMap::from([(
                    "kwok.x-k8s.io/node".into(),
                    "fake".into(),
                )])),
                ..Default::default()
            },
            status: Some(NodeStatus {
                capacity: Some(capacity),
                allocatable: Some(allocatable),
                ..Default::default()
            }),
            spec: None,
        };
        let created = nodes
            .create(&PostParams::default(), &node)
            .await
            .map_err(|e| ProviderError::CreationFailed {
                message: e.to_string(),
            })?;
        let name = created.metadata.name.unwrap();
        Ok(NodeId(name))
    }

    pub async fn delete(&self, node_id: &NodeId) -> Result<(), ProviderError> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        nodes
            .delete(&node_id.0, &DeleteParams::default())
            .await
            .map_err(|e| ProviderError::CreationFailed {
                message: e.to_string(),
            })?;
        Ok(())
    }
}
