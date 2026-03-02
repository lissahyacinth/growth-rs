use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Reference to a specific key in a Kubernetes Secret.
///
/// Because HetznerNodeClass is cluster-scoped, the namespace must be
/// specified explicitly — it cannot be inferred from the resource itself.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    pub name: String,
    pub namespace: String,
    pub key: String,
}

/// Spec for a HetznerNodeClass — provider-specific instance configuration.
///
/// Declares the OS image, SSH keys, and user-data template with variable
/// substitution. Users supply their own cloud-init / Talos / etc. template
/// and GrowthRS only injects named variable values at provision time.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "growth.vettrdev.com",
    version = "v1alpha1",
    kind = "HetznerNodeClass"
)]
#[kube(status = "HetznerNodeClassStatus")]
#[serde(rename_all = "camelCase")]
pub struct HetznerNodeClassSpec {
    /// OS image for the server, e.g. "ubuntu-24.04" or "talos-v1.9".
    pub image: String,
    /// Hetzner SSH key names to install on the server.
    pub ssh_key_names: Vec<String>,
    /// User-data template configuration with variable substitution.
    pub user_data: UserDataConfig,
}

/// Template + variable configuration for user-data.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UserDataConfig {
    /// Secret containing the raw template (e.g. cloud-init YAML).
    pub template_ref: SecretKeyRef,
    /// Named variables whose values replace `{{ name }}` placeholders.
    #[serde(default)]
    pub variables: Option<Vec<TemplateVariable>>,
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

/// Status of a HetznerNodeClass (reserved for future use).
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HetznerNodeClassStatus {}

#[derive(Debug, thiserror::Error)]
pub enum UserDataError {
    #[error("variable {variable:?} defined but placeholder {{{{ {variable} }}}} not found in template")]
    MarkerNotFound { variable: String },
    #[error("unresolved placeholder {{{{ {name} }}}} in template — no variable defined for it")]
    UnresolvedPlaceholder { name: String },
    #[error("failed to read secret {secret_name}/{key}: {reason}")]
    SecretReadFailed {
        secret_name: String,
        key: String,
        reason: String,
    },
    #[error("HetznerNodeClass {name:?} not found")]
    NodeClassNotFound { name: String },
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
    if let Some(start) = result.find("{{ ") {
        if let Some(end) = result[start..].find(" }}") {
            let name = &result[start + 3..start + end];
            return Err(UserDataError::UnresolvedPlaceholder {
                name: name.to_string(),
            });
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_template_success() {
        let template = "server={{ SERVER_URL }}\ntoken={{ JOIN_TOKEN }}\n";
        let vars = vec![
            ("SERVER_URL".to_string(), "https://k8s.example.com:6443".to_string()),
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
