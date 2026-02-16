/// (Instance) Offering
#[derive(Debug, Clone)]
pub struct Offering {
    pub instance_type: InstanceType,
    pub resources: Resources,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GpuModel {
    NvidiaT4,
    NvidiaA100,
    NvidiaL4,
    NvidiaH100,
    NvidiaA10G,
    Other(String),
}
