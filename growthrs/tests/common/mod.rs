use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Container, Pod, PodCondition, PodSpec, PodStatus, ResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ObjectMeta;

use growthrs::offering::{InstanceType, Offering, Resources};

pub fn pending_pod(name: &str, cpu: &str, memory: &str) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "worker".into(),
                image: Some("busybox".into()),
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([
                        ("cpu".into(), Quantity(cpu.into())),
                        ("memory".into(), Quantity(memory.into())),
                    ])),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some("Pending".into()),
            conditions: Some(vec![PodCondition {
                type_: "PodScheduled".into(),
                status: "False".into(),
                reason: Some("Unschedulable".into()),
                message: Some("insufficient resources".into()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn test_offering(name: &str, cpu: u32, memory_mib: u32, cost: f64) -> Offering {
    Offering {
        instance_type: InstanceType(name.into()),
        resources: Resources {
            cpu,
            memory_mib,
            ephemeral_storage_gib: None,
            gpu: 0,
            gpu_model: None,
        },
        cost_per_hour: cost,
    }
}
