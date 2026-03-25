use k8s_openapi::api::core::v1::Node;

use super::{INSTANCE_TYPE_LABEL, MANAGED_BY_LABEL, MANAGED_BY_VALUE, POOL_LABEL};

/// A Growth-managed node identified by its metadata and pool membership.
#[derive(Debug)]
pub struct NodeReference {
    pub node_name: String,
    pub node_uid: String,
    pub pool: String,
    pub instance_type: String,
}

impl NodeReference {
    /// Extract Growth-managed node info from a Node, returning None if the node
    /// is missing required metadata or is not managed by Growth.
    pub fn from_node(node: &Node) -> Option<Self> {
        let name = node.metadata.name.as_deref()?;
        let uid = node.metadata.uid.as_deref()?;
        let labels = node.metadata.labels.as_ref()?;

        if labels.get(MANAGED_BY_LABEL).map(|s| s.as_str()) != Some(MANAGED_BY_VALUE) {
            return None;
        }

        let pool = labels.get(POOL_LABEL)?.as_str();
        let instance_type = labels.get(INSTANCE_TYPE_LABEL)?.as_str();

        Some(Self {
            node_name: name.to_string(),
            node_uid: uid.to_string(),
            pool: pool.to_string(),
            instance_type: instance_type.to_string(),
        })
    }
}
