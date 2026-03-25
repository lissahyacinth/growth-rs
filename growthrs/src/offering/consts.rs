/// Label key used to match pods to NodePools via nodeSelector.
pub const POOL_LABEL: &str = "growth.vettrdev.com/pool";
pub const NODE_REQUEST_LABEL: &str = "growth.vettrdev.com/node-request";
pub const INSTANCE_TYPE_LABEL: &str = "growth.vettrdev.com/instance-type";
/// Label identifying nodes managed by the growth operator.
pub const MANAGED_BY_LABEL: &str = "growth.vettrdev.com/managed-by";
pub const MANAGED_BY_VALUE: &str = "growth";
/// Label selector for listing growth-managed nodes.
pub const MANAGED_BY_SELECTOR: &str = "growth.vettrdev.com/managed-by=growth";
/// NVIDIA GPU Feature Discovery label for the GPU product/model.
pub const GPU_PRODUCT_LABEL: &str = "nvidia.com/gpu.product";
/// Annotation set on nodes that are candidates for removal.
pub const REMOVAL_CANDIDATE_ANNOTATION: &str = "growth.vettrdev.com/removal-candidate";
/// Taint key applied to nodes being scaled down (NoSchedule effect).
pub const SCALE_DOWN_TAINT_KEY: &str = "growth.vettrdev.com/scale-down";
/// Taint key applied to nodes at creation time (NoExecute effect).
/// Removed by the node watcher when the node becomes Ready.
pub const STARTUP_TAINT_KEY: &str = "growth.vettrdev.com/unregistered";
/// Annotation recording when a node is scheduled for deletion (RFC 3339 timestamp).
pub const DELETE_AT_ANNOTATION: &str = "growth.vettrdev.com/delete-at";
/// Finalizer added to NodeRemovalRequests to guarantee provider cleanup before deletion.
pub const NRR_FINALIZER: &str = "growth.vettrdev.com/provider-cleanup";
