use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use crate::offering::QuantityParseError;

/// Parse a Kubernetes CPU quantity into whole vCPU count (rounds up).
/// Handles: bare integers ("4"), millicores ("500m").
pub(crate) fn parse_cpu(q: &Quantity) -> Result<u32, QuantityParseError> {
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
pub(crate) fn parse_memory_mib(q: &Quantity) -> Result<u32, QuantityParseError> {
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
pub(crate) fn parse_storage_gib(q: &Quantity) -> Result<u32, QuantityParseError> {
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
