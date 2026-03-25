use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::offering::{Offering, STARTUP_TAINT_KEY};

/// Dynamic variable names injected per-node at provision time.
/// User-declared variables must not collide with these.
pub const RESERVED_DYNAMIC_VARS: &[&str] = &[
    "REGION",
    "INSTANCE_TYPE",
    "NODE_LABELS",
    "NODE_TAINTS",
];

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
    pub variables: Vec<TemplateVariable>,
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
        "variable {name:?} collides with reserved dynamic variable — reserved names: {reserved}",
        reserved = RESERVED_DYNAMIC_VARS.join(", ")
    )]
    ReservedNameCollision { name: String },
}

/// Replace all `{{ VARIABLE }}` placeholders in the template.
///
/// Substitution is sequential: each variable's value is inserted before the
/// next variable is processed. This means a variable value containing
/// `{{ OTHER }}` will be substituted if `OTHER` is a later variable.
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
    let mut search_from = 0;
    while let Some(rel_start) = result[search_from..].find("{{ ") {
        let start = search_from + rel_start;
        if let Some(rel_end) = result[start + 3..].find(" }}") {
            let name = &result[start + 3..start + 3 + rel_end];
            return Err(UserDataError::UnresolvedPlaceholder {
                name: name.to_string(),
            });
        }
        // No closing `}}` found — not a placeholder, skip past `{{ `.
        search_from = start + 3;
    }

    Ok(result)
}

/// Build the per-node dynamic variables from the offering and labels.
///
/// These are the reserved dynamic variables (REGION, INSTANCE_TYPE,
/// NODE_LABELS, NODE_TAINTS) that are injected at provision time regardless
/// of provider.
pub(crate) fn build_dynamic_vars(
    offering: &Offering,
    labels: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let node_labels_str = labels
        .iter()
        .map(|(k, v)| format!("--node-label={k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");

    let node_taints_str = format!("--register-with-taints={STARTUP_TAINT_KEY}=:NoExecute");

    vec![
        ("REGION".to_string(), offering.location.region.0.clone()),
        (
            "INSTANCE_TYPE".to_string(),
            offering.instance_type.0.clone(),
        ),
        ("NODE_LABELS".to_string(), node_labels_str),
        ("NODE_TAINTS".to_string(), node_taints_str),
    ]
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

        assert_eq!(
            vars.len(),
            RESERVED_DYNAMIC_VARS.len(),
            "build_dynamic_vars should produce exactly one entry per reserved var"
        );
        for &reserved in RESERVED_DYNAMIC_VARS {
            assert!(
                names.contains(&reserved),
                "missing reserved dynamic var: {reserved}"
            );
        }

        let instance_type_val = vars.iter().find(|(n, _)| n == "INSTANCE_TYPE").unwrap();
        assert_eq!(instance_type_val.1, "cpx22");
    }

    #[test]
    fn build_dynamic_vars_node_taints_has_register_with_taints_flag() {
        use crate::offering::{InstanceType, Location, Offering, Region, Resources};

        let offering = Offering {
            instance_type: InstanceType("cax11".into()),
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
        let labels = std::collections::BTreeMap::new();

        let vars = build_dynamic_vars(&offering, &labels);
        let node_taints = vars.iter().find(|(n, _)| n == "NODE_TAINTS").unwrap();
        assert_eq!(
            node_taints.1,
            "--register-with-taints=growth.vettrdev.com/unregistered=:NoExecute"
        );
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

    #[test]
    fn resolve_template_unresolved_after_substitution_names_correct_variable() {
        // Regression: the unresolved placeholder check must not greedily match
        // across multiple `{{ }}` pairs. When the first var is substituted and
        // the second is not, the error must name the second variable.
        let template = "a={{ A }}\nb={{ B }}\n";
        let vars = vec![("A".to_string(), "val_a".to_string())];
        let err = resolve_template(template, &vars).unwrap_err();
        assert!(
            matches!(err, UserDataError::UnresolvedPlaceholder { ref name } if name == "B"),
            "expected UnresolvedPlaceholder for B, got {err}"
        );
    }

    #[test]
    fn resolve_template_value_containing_placeholder_syntax_is_substituted() {
        // Documents current behavior: variable values are not escaped, so a
        // value containing `{{ X }}` will be substituted if X is a later
        // variable. This is sequential pass-through, not a bug.
        let template = "config={{ OUTER }}\n";
        let vars = vec![
            ("OUTER".to_string(), "prefix-{{ INNER }}-suffix".to_string()),
            ("INNER".to_string(), "resolved".to_string()),
        ];
        let result = resolve_template(template, &vars).unwrap();
        assert_eq!(result, "config=prefix-resolved-suffix\n");
    }
}
