use std::num::ParseIntError;

use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("failed to parse quantity \"{raw}\": {source}")]
pub struct QuantityParseError {
    raw: String,
    source: ParseIntError,
}

/// (Instance) Offering
#[derive(Debug, Clone, PartialEq)]
pub struct Offering {
    pub instance_type: InstanceType,
    pub resources: Resources,
    /// Hourly cost in USD.
    pub cost_per_hour: f64,
}

/// Where the instance physically lives.
/// Both fields are provider-specific strings, but they're separate types
/// so you can't accidentally swap them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Location {
    pub region: Region,
    /// Zone within the region. Not all providers/offerings have zones.
    pub zone: Option<Zone>,
}

/// Newtype wrappers — prevents mixing up region/zone/instance_type strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Region(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Zone(pub String);

/// The provider's native identifier for this instance type.
/// Opaque to the caller — only the provider adapter interprets it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstanceType(pub String);

impl InstanceType {
    pub(crate) fn to_string(&self) -> String {
        self.0.clone()
    }
}

/// Unique identity of a pod (namespace + name).
/// Prevents accidental swaps between the two string fields
/// and provides a consistent `Display` format for logging and map keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodId {
    pub namespace: String,
    pub name: String,
}

impl PodId {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for PodId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PodResources {
    pub id: PodId,
    pub resources: Resources,
}

/// Resources available on an instance type.
/// This is what lets you write `offerings.iter().filter(|o| o.resources.cpu >= 4)`
/// instead of looking up "e2-medium" in a spreadsheet.
#[derive(Debug, Clone, PartialEq)]
pub struct Resources {
    /// vCPU count.
    pub cpu: u32,
    /// Memory in MiB. MiB not GiB — avoids the 0.5GiB rounding problem
    /// (e.g. t3.nano = 512 MiB, not 0.5 GiB).
    pub memory_mib: u32,
    /// Included ephemeral storage in GiB. None if not applicable (e.g. Hetzner
    /// bundles it into server_type but it's not separately configurable).
    pub ephemeral_storage_gib: Option<u32>,
    /// GPU count. 0 for non-GPU instances.
    pub gpu: u32,
    /// GPU model identifier when gpu > 0.
    pub gpu_model: Option<GpuModel>,
}

impl Offering {
    pub fn satisfies(&self, need: &Resources) -> bool {
        // TODO: Account for available memory vs provided memory
        self.resources.cpu >= need.cpu
            && self.resources.memory_mib >= need.memory_mib
            && self.resources.gpu >= need.gpu
            && match &need.gpu_model {
                Some(needed_gpu_model) => match &self.resources.gpu_model {
                    Some(provided_gpu_model) => needed_gpu_model == provided_gpu_model,
                    None => false,
                },
                None => true,
            }
            && match need.ephemeral_storage_gib {
                Some(required_storage) => match self.resources.ephemeral_storage_gib {
                    Some(provided_storage) => provided_storage >= required_storage,
                    None => false,
                },
                None => true,
            }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GpuModel {
    NvidiaT4,
    NvidiaA100,
    NvidiaL4,
    NvidiaH100,
    NvidiaA10G,
    Other(String),
}

/// Parse a Kubernetes CPU quantity into whole vCPU count (rounds up).
/// Handles: bare integers ("4"), millicores ("500m").
fn parse_cpu(q: &Quantity) -> Result<u32, QuantityParseError> {
    let s = &q.0;
    let map_err = |e| QuantityParseError {
        raw: s.clone(),
        source: e,
    };
    if let Some(millis) = s.strip_suffix('m') {
        let m: u32 = millis.parse().map_err(map_err)?;
        Ok(m.div_ceil(1000))
    } else {
        Ok(s.parse().map_err(map_err)?)
    }
}

/// Parse a Kubernetes memory quantity into MiB (rounds up).
/// Handles: Gi, Mi, Ki, and bare bytes.
fn parse_memory_mib(q: &Quantity) -> Result<u32, QuantityParseError> {
    let s = &q.0;
    let map_err = |e| QuantityParseError {
        raw: s.clone(),
        source: e,
    };
    if let Some(v) = s.strip_suffix("Gi") {
        let n: u32 = v.parse().map_err(map_err)?;
        Ok(n * 1024)
    } else if let Some(v) = s.strip_suffix("Mi") {
        Ok(v.parse().map_err(map_err)?)
    } else if let Some(v) = s.strip_suffix("Ki") {
        let n: u32 = v.parse().map_err(map_err)?;
        Ok(n.div_ceil(1024))
    } else {
        let n: u64 = s.parse().map_err(map_err)?;
        Ok((n.div_ceil(1024 * 1024)) as u32)
    }
}

/// Parse a Kubernetes ephemeral-storage quantity into GiB (rounds up).
fn parse_storage_gib(q: &Quantity) -> Result<u32, QuantityParseError> {
    let s = &q.0;
    let map_err = |e| QuantityParseError {
        raw: s.clone(),
        source: e,
    };
    if let Some(v) = s.strip_suffix("Gi") {
        Ok(v.parse().map_err(map_err)?)
    } else if let Some(v) = s.strip_suffix("Mi") {
        let n: u32 = v.parse().map_err(map_err)?;
        Ok(n.div_ceil(1024))
    } else if let Some(v) = s.strip_suffix("Ki") {
        let n: u64 = v.parse().map_err(map_err)?;
        Ok((n.div_ceil(1024 * 1024)) as u32)
    } else {
        let n: u64 = s.parse().map_err(map_err)?;
        Ok((n.div_ceil(1024 * 1024 * 1024)) as u32)
    }
}

impl PodResources {
    /// Build a `PodResources` from a Kubernetes Pod, extracting name/namespace
    /// and summing resource requests across all containers.
    pub fn from_pod(pod: &Pod) -> Result<PodResources, QuantityParseError> {
        Ok(PodResources {
            id: PodId {
                namespace: pod.metadata.namespace.clone().unwrap_or_default(),
                name: pod.metadata.name.clone().unwrap_or_default(),
            },
            resources: Resources::from_pod(pod)?,
        })
    }
}

impl Resources {
    /// Extract total resource requests from a Pod by summing across all containers.
    // TODO: Account for init containers. Kubernetes effective request is
    // max(max(each init container), sum(regular containers)) per resource dimension.
    pub fn from_pod(pod: &Pod) -> Result<Resources, QuantityParseError> {
        let mut cpu = 0u32;
        let mut memory_mib = 0u32;
        let mut gpu = 0u32;
        let mut ephemeral_storage_gib = None;

        let containers = pod
            .spec
            .as_ref()
            .map(|s| s.containers.as_slice())
            .unwrap_or_default();

        for container in containers {
            let Some(resources) = container.resources.as_ref() else {
                continue;
            };
            let Some(requests) = resources.requests.as_ref() else {
                continue;
            };

            if let Some(q) = requests.get("cpu") {
                cpu += parse_cpu(q)?;
            }
            if let Some(q) = requests.get("memory") {
                memory_mib += parse_memory_mib(q)?;
            }
            if let Some(q) = requests.get("nvidia.com/gpu") {
                gpu += q.0.parse::<u32>().map_err(|e| QuantityParseError {
                    raw: q.0.clone(),
                    source: e,
                })?;
            }
            if let Some(q) = requests.get("ephemeral-storage") {
                let gib = parse_storage_gib(q)?;
                *ephemeral_storage_gib.get_or_insert(0) += gib;
            }
        }

        Ok(Resources {
            cpu,
            memory_mib,
            ephemeral_storage_gib,
            gpu,
            // TODO: Include specific GPU Models
            gpu_model: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use k8s_openapi::api::core::v1::{Container, PodSpec, ResourceRequirements};

    fn q(s: &str) -> Quantity {
        Quantity(s.to_string())
    }

    #[test]
    fn parse_cpu_whole_cores() {
        assert_eq!(parse_cpu(&q("4")).unwrap(), 4);
        assert_eq!(parse_cpu(&q("1")).unwrap(), 1);
        assert_eq!(parse_cpu(&q("0")).unwrap(), 0);
    }

    #[test]
    fn parse_cpu_millicores() {
        assert_eq!(parse_cpu(&q("1000m")).unwrap(), 1);
        assert_eq!(parse_cpu(&q("500m")).unwrap(), 1); // rounds up
        assert_eq!(parse_cpu(&q("250m")).unwrap(), 1); // rounds up
        assert_eq!(parse_cpu(&q("1500m")).unwrap(), 2); // rounds up
        assert_eq!(parse_cpu(&q("2000m")).unwrap(), 2);
    }

    #[test]
    fn parse_cpu_invalid_is_err() {
        assert!(parse_cpu(&q("abc")).is_err());
        assert!(parse_cpu(&q("")).is_err());
        assert!(parse_cpu(&q("xm")).is_err());
    }

    #[test]
    fn parse_memory_gi() {
        assert_eq!(parse_memory_mib(&q("8Gi")).unwrap(), 8192);
        assert_eq!(parse_memory_mib(&q("1Gi")).unwrap(), 1024);
    }

    #[test]
    fn parse_memory_mi() {
        assert_eq!(parse_memory_mib(&q("512Mi")).unwrap(), 512);
        assert_eq!(parse_memory_mib(&q("256Mi")).unwrap(), 256);
    }

    #[test]
    fn parse_memory_ki() {
        assert_eq!(parse_memory_mib(&q("1024Ki")).unwrap(), 1); // exactly 1 MiB
        assert_eq!(parse_memory_mib(&q("1048576Ki")).unwrap(), 1024); // 1 GiB in Ki
        assert_eq!(parse_memory_mib(&q("512Ki")).unwrap(), 1); // rounds up
    }

    #[test]
    fn parse_memory_bare_bytes() {
        assert_eq!(parse_memory_mib(&q("1073741824")).unwrap(), 1024); // 1 GiB in bytes
        assert_eq!(parse_memory_mib(&q("0")).unwrap(), 0);
    }

    #[test]
    fn parse_memory_invalid_is_err() {
        assert!(parse_memory_mib(&q("abc")).is_err());
        assert!(parse_memory_mib(&q("")).is_err());
        assert!(parse_memory_mib(&q("xGi")).is_err());
    }

    fn make_container(cpu: &str, memory: &str) -> Container {
        let mut requests = BTreeMap::new();
        requests.insert("cpu".to_string(), q(cpu));
        requests.insert("memory".to_string(), q(memory));
        Container {
            name: "test".to_string(),
            resources: Some(ResourceRequirements {
                requests: Some(requests),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn make_pod(containers: Vec<Container>) -> Pod {
        Pod {
            spec: Some(PodSpec {
                containers,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn from_pod_single_container() {
        let pod = make_pod(vec![make_container("2", "4Gi")]);
        let r = Resources::from_pod(&pod).unwrap();
        assert_eq!(r.cpu, 2);
        assert_eq!(r.memory_mib, 4096);
        assert_eq!(r.gpu, 0);
        assert_eq!(r.ephemeral_storage_gib, None);
        assert_eq!(r.gpu_model, None);
    }

    #[test]
    fn from_pod_multi_container_sums() {
        let pod = make_pod(vec![
            make_container("2", "1Gi"),
            make_container("1", "512Mi"),
        ]);
        let r = Resources::from_pod(&pod).unwrap();
        assert_eq!(r.cpu, 3);
        assert_eq!(r.memory_mib, 1024 + 512);
    }

    #[test]
    fn from_pod_no_spec_returns_zero() {
        let pod = Pod::default();
        let r = Resources::from_pod(&pod).unwrap();
        assert_eq!(r.cpu, 0);
        assert_eq!(r.memory_mib, 0);
    }

    #[test]
    fn from_pod_no_resources_returns_zero() {
        let pod = make_pod(vec![Container {
            name: "bare".to_string(),
            ..Default::default()
        }]);
        let r = Resources::from_pod(&pod).unwrap();
        assert_eq!(r.cpu, 0);
        assert_eq!(r.memory_mib, 0);
    }

    #[test]
    fn from_pod_with_gpu() {
        let mut requests = BTreeMap::new();
        requests.insert("cpu".to_string(), q("4"));
        requests.insert("memory".to_string(), q("8Gi"));
        requests.insert("nvidia.com/gpu".to_string(), q("2"));
        let container = Container {
            name: "gpu-worker".to_string(),
            resources: Some(ResourceRequirements {
                requests: Some(requests),
                ..Default::default()
            }),
            ..Default::default()
        };
        let pod = make_pod(vec![container]);
        let r = Resources::from_pod(&pod).unwrap();
        assert_eq!(r.cpu, 4);
        assert_eq!(r.memory_mib, 8192);
        assert_eq!(r.gpu, 2);
    }

    #[test]
    fn from_pod_invalid_cpu_is_err() {
        let pod = make_pod(vec![make_container("garbage", "4Gi")]);
        assert!(Resources::from_pod(&pod).is_err());
    }

    #[test]
    fn from_pod_invalid_memory_is_err() {
        let pod = make_pod(vec![make_container("2", "notmemory")]);
        assert!(Resources::from_pod(&pod).is_err());
    }

    #[test]
    fn satisfies_exact_match() {
        let offering = Offering {
            instance_type: InstanceType("cx21".to_string()),
            resources: Resources {
                cpu: 2,
                memory_mib: 4096,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.0066,
        };
        let demand = Resources {
            cpu: 2,
            memory_mib: 4096,
            ephemeral_storage_gib: None,
            gpu: 0,
            gpu_model: None,
        };
        assert!(offering.satisfies(&demand));
    }

    #[test]
    fn satisfies_offering_larger_than_demand() {
        let offering = Offering {
            instance_type: InstanceType("cx31".to_string()),
            resources: Resources {
                cpu: 4,
                memory_mib: 8192,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.0106,
        };
        let demand = Resources {
            cpu: 2,
            memory_mib: 4096,
            ephemeral_storage_gib: None,
            gpu: 0,
            gpu_model: None,
        };
        assert!(offering.satisfies(&demand));
    }

    #[test]
    fn satisfies_rejects_insufficient_cpu() {
        let offering = Offering {
            instance_type: InstanceType("cx11".to_string()),
            resources: Resources {
                cpu: 1,
                memory_mib: 2048,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.0044,
        };
        let demand = Resources {
            cpu: 2,
            memory_mib: 1024,
            ephemeral_storage_gib: None,
            gpu: 0,
            gpu_model: None,
        };
        assert!(!offering.satisfies(&demand));
    }

    #[test]
    fn satisfies_roundtrip_from_pod() {
        let pod = make_pod(vec![make_container("2", "4Gi")]);
        let demand = Resources::from_pod(&pod).unwrap();

        let good_offering = Offering {
            instance_type: InstanceType("cx31".to_string()),
            resources: Resources {
                cpu: 4,
                memory_mib: 8192,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.0106,
        };
        let small_offering = Offering {
            instance_type: InstanceType("cx11".to_string()),
            resources: Resources {
                cpu: 1,
                memory_mib: 2048,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.0044,
        };

        assert!(good_offering.satisfies(&demand));
        assert!(!small_offering.satisfies(&demand));
    }
}
