use std::collections::BTreeMap;

use kube::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::controller::helpers::{read_configmap_key, read_secret_key};
use crate::offering::Offering;

/// Dynamic variable names injected per-node at provision time.
/// User-declared variables must not collide with these.
pub const RESERVED_DYNAMIC_VARS: &[&str] = &["REGION", "LOCATION", "INSTANCE_TYPE", "NODE_LABELS"];

/// Reference to a specific key in a Kubernetes ConfigMap.
///
/// Used for non-sensitive data like user-data templates. Namespace must be
/// specified explicitly since HetznerNodeClass is cluster-scoped.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapKeyRef {
    pub name: String,
    pub namespace: String,
    pub key: String,
}

/// Reference to a specific key in a Kubernetes Secret.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    pub name: String,
    pub namespace: String,
    pub key: String,
}

/// A single template variable: a name and a Secret reference for its value.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TemplateVariable {
    /// Placeholder name — used as `{{ name }}` in the template.
    pub name: String,
    /// Where to read the substitution value from.
    pub secret_ref: SecretKeyRef,
}

/// Template + variable configuration for user-data.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UserDataConfig {
    /// ConfigMap containing the raw template (e.g. cloud-init YAML).
    pub template_ref: ConfigMapKeyRef,
    /// Named variables whose values replace `{{ name }}` placeholders.
    /// Defaults - [[RESERVED_DYNAMIC_VARS]]
    #[serde(default)]
    pub variables: Option<Vec<TemplateVariable>>,
}

#[derive(Debug, thiserror::Error)]
pub enum UserDataError {
    #[error(
        "variable {variable:?} defined but placeholder {{{{ {variable} }}}} not found in template"
    )]
    MarkerNotFound { variable: String },
    #[error("unresolved placeholder {{{{ {name} }}}} in template — no variable defined for it")]
    UnresolvedPlaceholder { name: String },
    #[error("failed to read secret {secret_name}/{key}: {reason}")]
    SecretReadFailed {
        secret_name: String,
        key: String,
        reason: String,
    },
    #[error("failed to read configmap {configmap_name}/{key}: {reason}")]
    ConfigMapReadFailed {
        configmap_name: String,
        key: String,
        reason: String,
    },
    #[error(
        "variable {name:?} collides with reserved dynamic variable — reserved names: REGION, LOCATION, INSTANCE_TYPE, NODE_LABELS"
    )]
    ReservedNameCollision { name: String },
}

/// Replace all `{{ VARIABLE }}` placeholders in the template.
///
/// Fails if:
/// - A defined variable's placeholder is absent from the template.
/// - Unresolved `{{ X }}` placeholders remain after substitution.
pub fn resolve_template(
    template: &str,
    variables: &[(String, String)],
) -> Result<String, UserDataError> {
    let mut result = template.to_string();

    for (name, value) in variables {
        let marker = format!("{{{{ {name} }}}}");
        if !result.contains(&marker) {
            return Err(UserDataError::MarkerNotFound {
                variable: name.clone(),
            });
        }
        result = result.replace(&marker, value);
    }

    // Check for any remaining unresolved placeholders.
    if let Some(start) = result.find("{{ ")
        && let Some(end) = result[start..].find(" }}")
    {
        let name = &result[start + 3..start + end];
        return Err(UserDataError::UnresolvedPlaceholder {
            name: name.to_string(),
        });
    }

    Ok(result)
}

/// Build the per-node dynamic variables from the offering and labels.
///
/// These are the reserved dynamic variables (REGION, LOCATION, INSTANCE_TYPE,
/// NODE_LABELS) that are injected at provision time regardless of provider.
pub(crate) fn build_dynamic_vars(
    offering: &Offering,
    labels: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let node_labels_str = labels
        .iter()
        .map(|(k, v)| format!("--node-label={k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");

    vec![
        ("REGION".to_string(), offering.location.region.0.clone()),
        ("LOCATION".to_string(), offering.location.region.0.clone()),
        (
            "INSTANCE_TYPE".to_string(),
            offering.instance_type.0.clone(),
        ),
        ("NODE_LABELS".to_string(), node_labels_str),
    ]
}

impl UserDataConfig {
    /// Resolve the user-data template: read ConfigMap + Secret values, merge
    /// dynamic per-node variables, and perform template substitution.
    ///
    /// This is the imperative-shell entry point for template resolution.
    /// All K8s I/O (ConfigMap/Secret reads) happens here; the actual
    /// substitution is delegated to the pure `resolve_template()`.
    pub(crate) async fn resolve(
        &self,
        client: &Client,
        offering: &Offering,
        labels: &BTreeMap<String, String>,
    ) -> Result<String, UserDataError> {
        // 1. Read template from ConfigMap.
        let tpl_ref = &self.template_ref;
        let template =
            read_configmap_key(client, &tpl_ref.namespace, &tpl_ref.name, &tpl_ref.key).await?;

        // 2. Read secret variable values + validate reserved name collisions.
        let mut secret_vars = Vec::new();
        if let Some(vars) = &self.variables {
            for var in vars {
                if RESERVED_DYNAMIC_VARS.contains(&var.name.as_str()) {
                    return Err(UserDataError::ReservedNameCollision {
                        name: var.name.clone(),
                    });
                }
                let value = read_secret_key(
                    client,
                    &var.secret_ref.namespace,
                    &var.secret_ref.name,
                    &var.secret_ref.key,
                )
                .await?;
                secret_vars.push((var.name.clone(), value));
            }
        }

        // 3. Merge secret vars + dynamic per-node vars, resolve template.
        let dynamic_vars = build_dynamic_vars(offering, labels);
        let mut all_vars = secret_vars;
        for (name, value) in dynamic_vars {
            let marker = format!("{{{{ {name} }}}}");
            if template.contains(&marker) {
                all_vars.push((name, value));
            }
        }

        resolve_template(&template, &all_vars)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_template_success() {
        let template = "server={{ SERVER_URL }}\ntoken={{ JOIN_TOKEN }}\n";
        let vars = vec![
            (
                "SERVER_URL".to_string(),
                "https://k8s.example.com:6443".to_string(),
            ),
            ("JOIN_TOKEN".to_string(), "abc123".to_string()),
        ];
        let result = resolve_template(template, &vars).unwrap();
        assert_eq!(
            result,
            "server=https://k8s.example.com:6443\ntoken=abc123\n"
        );
    }

    #[test]
    fn resolve_template_no_variables() {
        let template = "#!/bin/bash\necho hello\n";
        let result = resolve_template(template, &[]).unwrap();
        assert_eq!(result, template);
    }

    #[test]
    fn resolve_template_unused_variable_errors() {
        let template = "token={{ JOIN_TOKEN }}\n";
        let vars = vec![
            ("JOIN_TOKEN".to_string(), "abc".to_string()),
            ("UNUSED".to_string(), "val".to_string()),
        ];
        let err = resolve_template(template, &vars).unwrap_err();
        assert!(
            matches!(err, UserDataError::MarkerNotFound { ref variable } if variable == "UNUSED"),
            "expected MarkerNotFound for UNUSED, got {err}"
        );
    }

    #[test]
    fn build_dynamic_vars_includes_all_reserved() {
        use crate::offering::{InstanceType, Location, Offering, Region, Resources};

        let offering = Offering {
            instance_type: InstanceType("cpx22".into()),
            resources: Resources {
                cpu: 2,
                memory_mib: 4096,
                ephemeral_storage_gib: None,
                gpu: 0,
                gpu_model: None,
            },
            cost_per_hour: 0.01,
            location: Location {
                region: Region("fsn1".into()),
                zone: None,
            },
        };
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("growth.vettrdev.com/pool".into(), "default".into());

        let vars = build_dynamic_vars(&offering, &labels);
        let names: Vec<&str> = vars.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"REGION"));
        assert!(names.contains(&"LOCATION"));
        assert!(names.contains(&"INSTANCE_TYPE"));
        assert!(names.contains(&"NODE_LABELS"));

        let instance_type_val = vars.iter().find(|(n, _)| n == "INSTANCE_TYPE").unwrap();
        assert_eq!(instance_type_val.1, "cpx22");
    }

    #[test]
    fn resolve_template_unresolved_placeholder_errors() {
        let template = "token={{ JOIN_TOKEN }}\nserver={{ SERVER_URL }}\n";
        let vars = vec![("JOIN_TOKEN".to_string(), "abc".to_string())];
        let err = resolve_template(template, &vars).unwrap_err();
        assert!(
            matches!(err, UserDataError::UnresolvedPlaceholder { ref name } if name == "SERVER_URL"),
            "expected UnresolvedPlaceholder for SERVER_URL, got {err}"
        );
    }
}
