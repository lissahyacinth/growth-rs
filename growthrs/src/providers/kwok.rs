use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Node, NodeSpec, NodeStatus, Taint};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{DeleteParams, ObjectMeta, PostParams};
use kube::{Api, Client};
use tracing::{debug, info};

use crate::offering::{
    GpuModel, InstanceType, Location, MANAGED_BY_LABEL, MANAGED_BY_VALUE, Offering, Region,
    Resources, STARTUP_TAINT_KEY, Zone,
};
use crate::providers::provider::{InstanceConfig, NodeId, ProviderError, ProviderStatus};

/// Hetzner-like zone names used by the KWOK provider for testing.
const ZONES: &[(&str, &str)] = &[
    ("eu-central", "fsn1-dc14"),
    ("eu-central", "nbg1-dc3"),
    ("eu-central", "hel1-dc2"),
];

fn offering(
    name: &str,
    cpu: u32,
    memory_mib: u32,
    disk_gib: u32,
    cost_per_hour: f64,
    location: Location,
) -> Offering {
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
        location,
    }
}

fn gpu_offering(
    name: &str,
    resources: Resources,
    cost_per_hour: f64,
    location: Location,
) -> Offering {
    Offering {
        instance_type: InstanceType(name.into()),
        resources,
        cost_per_hour,
        location,
    }
}

pub(crate) fn to_capacity(res: &Resources) -> BTreeMap<String, Quantity> {
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
        /// (name, cpu, mem_mib, disk_gib, cost/hr)
        const CPU_TYPES: &[(&str, u32, u32, u32, f64)] = &[
            // CX – Shared x86
            ("cpx22", 2, 4_096, 40, 0.0066),
            ("cx32", 4, 8_192, 80, 0.0106),
            ("cx42", 8, 16_384, 160, 0.0170),
            ("cx52", 16, 32_768, 320, 0.0314),
            // CPX – Shared AMD
            ("cpx12", 2, 2_048, 40, 0.0122),
            ("cpx22", 3, 4_096, 80, 0.0226),
            ("cpx32", 4, 8_192, 160, 0.0299),
            ("cpx42", 8, 16_384, 256, 0.0362),
            ("cpx52", 16, 32_768, 360, 0.0515),
            // CAX – ARM (Ampere)
            ("cax11", 2, 4_096, 40, 0.0074),
            ("cax21", 4, 8_192, 80, 0.0122),
            ("cax31", 8, 16_384, 160, 0.0226),
            ("cax41", 16, 32_768, 320, 0.0443),
            // CCX – Dedicated x86
            ("ccx13", 2, 8_192, 80, 0.0386),
            ("ccx23", 4, 16_384, 160, 0.0475),
            ("ccx33", 8, 32_768, 240, 0.0900),
            ("ccx43", 16, 65_536, 360, 0.1789),
            ("ccx53", 32, 131_072, 600, 0.3568),
            ("ccx63", 48, 196_608, 960, 0.5347),
        ];

        /// (name, cpu, mem_mib, disk_gib, gpu, model, cost/hr)
        const GPU_TYPES: &[(&str, u32, u32, u32, u32, f64)] = &[
            ("gpu-a100-1", 12, 131_072, 200, 1, 2.21),
            ("gpu-a100-4", 48, 524_288, 800, 4, 8.84),
        ];

        let mut offerings = Vec::new();

        for &(region, zone) in ZONES {
            let loc = Location {
                region: Region(region.into()),
                zone: Some(Zone(zone.into())),
            };
            for &(name, cpu, mem, disk, cost) in CPU_TYPES {
                offerings.push(offering(name, cpu, mem, disk, cost, loc.clone()));
            }
            for &(name, cpu, mem, disk, gpu, cost) in GPU_TYPES {
                offerings.push(gpu_offering(
                    name,
                    Resources {
                        cpu,
                        memory_mib: mem,
                        ephemeral_storage_gib: Some(disk),
                        gpu,
                        gpu_model: Some(GpuModel::NvidiaA100),
                    },
                    cost,
                    loc.clone(),
                ));
            }
        }

        offerings
    }
    pub async fn create(
        &self,
        node_id: String,
        offering: &Offering,
        config: &InstanceConfig,
    ) -> Result<NodeId, ProviderError> {
        info!(
            node_id = %node_id,
            instance_type = %offering.instance_type,
            cpu = offering.resources.cpu,
            memory_mib = offering.resources.memory_mib,
            "creating KWOK node"
        );
        let mut capacity = to_capacity(&offering.resources);
        capacity.insert("pods".into(), Quantity("110".into()));
        let allocatable = capacity.clone();

        let mut labels = BTreeMap::from([
            ("type".into(), "kwok".into()),
            (MANAGED_BY_LABEL.into(), MANAGED_BY_VALUE.into()),
        ]);
        labels.extend(config.labels.clone());

        let nodes: Api<Node> = Api::all(self.client.clone());
        let node = Node {
            metadata: ObjectMeta {
                name: Some(node_id),
                labels: Some(labels),
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
            spec: Some(NodeSpec {
                taints: Some(vec![Taint {
                    key: STARTUP_TAINT_KEY.into(),
                    value: None,
                    effect: "NoExecute".into(),
                    time_added: None,
                }]),
                ..Default::default()
            }),
        };
        let created = nodes
            .create(&PostParams::default(), &node)
            .await
            .map_err(|e| ProviderError::CreationFailed {
                message: e.to_string(),
            })?;
        let name = created.metadata.name.unwrap();
        debug!(node_id = %name, "KWOK node created");
        Ok(NodeId(name))
    }

    pub async fn delete(&self, node_id: &NodeId) -> Result<(), ProviderError> {
        info!(node_id = %node_id.0, "deleting KWOK node");
        let nodes: Api<Node> = Api::all(self.client.clone());
        nodes
            .delete(&node_id.0, &DeleteParams::default())
            .await
            .map_err(|e| ProviderError::DeletionFailed {
                message: e.to_string(),
            })?;
        Ok(())
    }

    pub async fn status(&self, node_id: &NodeId) -> Result<ProviderStatus, ProviderError> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        match nodes
            .get_opt(&node_id.0)
            .await
            .map_err(|e| ProviderError::Internal(e.into()))?
        {
            Some(_) => Ok(ProviderStatus::Running),
            None => Ok(ProviderStatus::NotFound),
        }
    }
}
