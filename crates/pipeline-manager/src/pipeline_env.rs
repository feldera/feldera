use std::collections::BTreeMap;

/// Environment variable names that users cannot override in pipeline configuration.
pub const ENV_VAR_BLOCKLIST: &[&str] = &["HOSTNAME", "RUST_LOG", "TOKIO_WORKER_THREADS"];

/// Environment variable prefixes that users cannot override in pipeline configuration.
pub const ENV_VAR_PREFIX_BLOCKLIST: &[&str] = &["FELDERA_", "KUBERNETES_", "TOKIO_"];

/// Validate user-provided pipeline environment variables and
/// avoid overrides of certain ENV 'namespaces'.
///
/// The 'blocked' names are often set by feldera itself and can cause
/// unexpected behavior when overridden with this mechanism by the
/// user.
///
/// - Don't allow anything starting with `FELDERA_`
/// - Don't allow anything starting with `KUBERNETES_`
/// - Don't allow anything starting with `TOKIO_`
/// - Don't allow explicit control variables such as `RUST_LOG`
pub fn validate_pipeline_env(env: &BTreeMap<String, String>) -> Result<(), String> {
    let forbidden: Vec<String> = env
        .keys()
        .filter(|key| {
            ENV_VAR_BLOCKLIST.contains(&key.as_str())
                || ENV_VAR_PREFIX_BLOCKLIST
                    .iter()
                    .any(|prefix| key.starts_with(prefix))
        })
        .cloned()
        .collect();

    if forbidden.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the following environment variable(s) are reserved and cannot be overridden: {}",
            forbidden.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::validate_pipeline_env;
    use std::collections::BTreeMap;

    #[test]
    fn accepts_non_reserved_env_vars() {
        let env = BTreeMap::from([("TEST_ENV".to_string(), "x".to_string())]);
        assert!(validate_pipeline_env(&env).is_ok());
    }

    #[test]
    fn rejects_feldera_env_vars() {
        let env = BTreeMap::from([("FELDERA_TEST_ENV".to_string(), "x".to_string())]);
        assert!(validate_pipeline_env(&env).is_err());
    }

    #[test]
    fn rejects_reserved_env_vars() {
        let env = BTreeMap::from([
            ("TOKIO_WORKER_THREADS".to_string(), "1".to_string()),
            ("RUST_LOG".to_string(), "debug".to_string()),
        ]);
        assert!(validate_pipeline_env(&env).is_err());
    }

    #[test]
    fn rejects_kubernetes_env_vars() {
        let env = BTreeMap::from([("KUBERNETES_SERVICE_HOST".to_string(), "x".to_string())]);
        assert!(validate_pipeline_env(&env).is_err());
    }
}
