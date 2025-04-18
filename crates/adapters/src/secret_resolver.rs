use feldera_types::config::ConnectorConfig;
use feldera_types::secret_ref::{MaybeSecretRef, MaybeSecretRefParseError, SecretRef};
use serde_yaml::{Mapping, Value};
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use thiserror::Error as ThisError;

/// Path of the default secrets directory.
pub const DEFAULT_SECRETS_DIRECTORY_PATH: &str = "/etc/feldera-secrets";

#[derive(Debug, Clone, PartialEq, Eq, ThisError)]
pub enum SecretRefResolutionError {
    #[error("{e}")]
    MaybeSecretRefParseFailed { e: MaybeSecretRefParseError },
    #[error("secret reference '{secret_ref}' resolution failed: file '{path}' does exist but unable to read it (this might be due to insufficient permissions or content is not UTF-8)")]
    CannotReadSecretFile { secret_ref: SecretRef, path: String },
    #[error("secret reference '{secret_ref}' resolution failed: file '{path}' does not exist")]
    SecretFileDoesNotExist { secret_ref: SecretRef, path: String },
    #[error("secret reference '{secret_ref}' resolution failed: path '{path}' is not a file")]
    SecretPathIsNotFile { secret_ref: SecretRef, path: String },
    #[error("secret resolution led to a duplicate key in the mapping, which should not happen")]
    DuplicateKeyInMapping,
    #[error("unable to serialize connector configuration: {error}")]
    SerializationFailed { error: String },
    #[error("unable to deserialize connector configuration (error omitted)")]
    DeserializationFailed,
}

/// Resolves the secret references of the connector configuration.
pub fn resolve_secret_references_in_connector_config(
    secrets_dir: &Path,
    connector_config: &ConnectorConfig,
) -> Result<ConnectorConfig, SecretRefResolutionError> {
    let yaml_value = serde_yaml::to_value(connector_config).map_err(|e| {
        SecretRefResolutionError::SerializationFailed {
            error: e.to_string(),
        }
    })?;
    let yaml_value_resolved = resolve_secret_references_in_yaml(secrets_dir, yaml_value)?;
    let connector_config_resolved = serde_yaml::from_value(yaml_value_resolved)
        .map_err(|_e| SecretRefResolutionError::DeserializationFailed)?;
    Ok(connector_config_resolved)
}

/// Resolves recursively the secret references in the YAML.
fn resolve_secret_references_in_yaml(
    secrets_dir: &Path,
    value: Value,
) -> Result<Value, SecretRefResolutionError> {
    Ok(match value {
        Value::Null => Value::Null,
        Value::Bool(b) => Value::Bool(b),
        Value::Number(n) => Value::Number(n),
        Value::String(s) => {
            Value::String(resolve_potential_secret_reference_string(secrets_dir, s)?)
        }
        Value::Sequence(seq) => Value::Sequence(
            seq.into_iter()
                .map(|v| resolve_secret_references_in_yaml(secrets_dir, v))
                .collect::<Result<Vec<Value>, SecretRefResolutionError>>()?,
        ),
        Value::Mapping(mapping) => {
            let mut new_mapping = Mapping::new();
            for (k, v) in mapping.into_iter() {
                if let Some(_existing) =
                    new_mapping.insert(k, resolve_secret_references_in_yaml(secrets_dir, v)?)
                {
                    return Err(SecretRefResolutionError::DuplicateKeyInMapping);
                }
            }
            Value::Mapping(new_mapping)
        }
        Value::Tagged(mut tag_val) => {
            tag_val.value = resolve_secret_references_in_yaml(secrets_dir, tag_val.value)?;
            Value::Tagged(tag_val)
        }
    })
}

/// Resolves a string which can potentially be a secret reference.
fn resolve_potential_secret_reference_string(
    secrets_dir: &Path,
    s: String,
) -> Result<String, SecretRefResolutionError> {
    match MaybeSecretRef::new(s) {
        Ok(maybe_secret_ref) => match maybe_secret_ref {
            MaybeSecretRef::String(plain_str) => Ok(plain_str),
            MaybeSecretRef::SecretRef(secret_ref) => match &secret_ref {
                SecretRef::Kubernetes { name, data_key } => {
                    // Secret reference: `${secret:kubernetes:<name>/<data key>}`
                    // File location: `<secrets dir>/kubernetes/<name>/<data key>`
                    let path = Path::new(secrets_dir)
                        .join("kubernetes")
                        .join(name)
                        .join(data_key);

                    // If the file does not exist or is not a file, produce a custom error informing
                    // of this fact rather than generically stating we are unable to read.
                    if path.is_file() {
                        match fs::read_to_string(&path) {
                            Ok(content) => Ok(content),
                            Err(_e) => {
                                // The read error is not part of the error returned to prevent
                                // displaying any of the secret content
                                Err(SecretRefResolutionError::CannotReadSecretFile {
                                    secret_ref,
                                    path: path.display().to_string(),
                                })
                            }
                        }
                    } else if path.exists() {
                        Err(SecretRefResolutionError::SecretPathIsNotFile {
                            secret_ref,
                            path: path.display().to_string(),
                        })
                    } else {
                        Err(SecretRefResolutionError::SecretFileDoesNotExist {
                            secret_ref,
                            path: path.display().to_string(),
                        })
                    }
                }
            },
        },
        Err(e) => Err(SecretRefResolutionError::MaybeSecretRefParseFailed { e }),
    }
}

#[cfg(test)]
mod tests {
    use crate::secret_resolver::{
        resolve_potential_secret_reference_string, resolve_secret_references_in_connector_config,
        resolve_secret_references_in_yaml, SecretRefResolutionError,
    };
    use feldera_types::config::{ConnectorConfig, TransportConfig};
    use feldera_types::secret_ref::MaybeSecretRef;
    use serde_json::json;
    use std::fs::{create_dir_all, File};
    use std::io::Write;

    #[test]
    fn resolve_kubernetes_secret_success() {
        // Create file at: <tempdir>/kubernetes/a/b
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let name_dir = &dir_path.join("kubernetes").join("a");
        create_dir_all(name_dir).unwrap();
        let data_key_file_path = &name_dir.join("b");
        let mut file = File::create(data_key_file_path).unwrap();
        file.write_all(b"example").unwrap();

        // Resolve secret: ${secret:kubernetes:a/b}
        assert_eq!(
            resolve_potential_secret_reference_string(
                dir_path,
                "${secret:kubernetes:a/b}".to_string()
            )
            .unwrap(),
            "example"
        );
    }

    #[test]
    fn resolve_kubernetes_secret_max_size_success() {
        // Create file at: <tempdir>/kubernetes/a.../b...
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let name_dir = &dir_path.join("kubernetes").join("a".repeat(63));
        create_dir_all(name_dir).unwrap();
        let data_key_file_path = &name_dir.join("b".repeat(255));
        let mut file = File::create(data_key_file_path).unwrap();
        file.write_all(b"example").unwrap();

        // Resolve secret: ${secret:kubernetes:a.../b...}
        assert_eq!(
            resolve_potential_secret_reference_string(
                dir_path,
                format!(
                    "${{secret:kubernetes:{}/{}}}",
                    "a".repeat(63),
                    "b".repeat(255)
                )
            )
            .unwrap(),
            "example"
        );
    }

    #[test]
    fn resolve_kubernetes_secret_path_not_a_file() {
        // Create directory at: <tempdir>/kubernetes/a/b
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let data_key_file_path = &dir_path.join("kubernetes").join("a").join("b");
        create_dir_all(data_key_file_path).unwrap();

        // Resolve secret: ${secret:kubernetes:a/b}
        let MaybeSecretRef::SecretRef(secret_ref) =
            MaybeSecretRef::new("${secret:kubernetes:a/b}".to_string()).unwrap()
        else {
            unreachable!();
        };
        assert_eq!(
            resolve_potential_secret_reference_string(
                dir_path,
                "${secret:kubernetes:a/b}".to_string()
            )
            .unwrap_err(),
            SecretRefResolutionError::SecretPathIsNotFile {
                secret_ref,
                path: data_key_file_path.display().to_string()
            }
        );
    }

    #[test]
    fn resolve_kubernetes_secret_file_does_not_exist() {
        // Do not create file at: <tempdir>/kubernetes/a/b
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let name_dir = &dir_path.join("kubernetes").join("a");
        create_dir_all(name_dir).unwrap();
        let data_key_file_path = &name_dir.join("b");

        // Resolve secret: ${secret:kubernetes:a/b}
        let MaybeSecretRef::SecretRef(secret_ref) =
            MaybeSecretRef::new("${secret:kubernetes:a/b}".to_string()).unwrap()
        else {
            unreachable!();
        };
        assert_eq!(
            resolve_potential_secret_reference_string(
                dir_path,
                "${secret:kubernetes:a/b}".to_string()
            )
            .unwrap_err(),
            SecretRefResolutionError::SecretFileDoesNotExist {
                secret_ref,
                path: data_key_file_path.display().to_string()
            }
        );
    }

    #[test]
    fn resolve_secret_ref_cannot_read_file() {
        // Create file with non UTF-8 content at: <tempdir>/kubernetes/a/b
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let name_dir = &dir_path.join("kubernetes").join("a");
        create_dir_all(name_dir).unwrap();
        let data_key_file_path = &name_dir.join("b");
        let mut file = File::create(data_key_file_path).unwrap();
        file.write_all(&[255, 255]).unwrap();

        // Resolve secret: ${secret:kubernetes:a/b}
        let MaybeSecretRef::SecretRef(secret_ref) =
            MaybeSecretRef::new("${secret:kubernetes:a/b}".to_string()).unwrap()
        else {
            unreachable!();
        };
        assert_eq!(
            resolve_potential_secret_reference_string(
                dir_path,
                "${secret:kubernetes:a/b}".to_string()
            )
            .unwrap_err(),
            SecretRefResolutionError::CannotReadSecretFile {
                secret_ref,
                path: data_key_file_path.display().to_string()
            }
        );
    }

    #[test]
    fn secret_resolution_yaml() {
        // Create file at:
        // - <tempdir>/kubernetes/a/b
        // - <tempdir>/kubernetes/c/d
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let name_dir = &dir_path.join("kubernetes").join("a");
        create_dir_all(name_dir).unwrap();
        let data_key_file_path = &name_dir.join("b");
        let mut file = File::create(data_key_file_path).unwrap();
        file.write_all(b"example1").unwrap();
        let name_dir = &dir_path.join("kubernetes").join("c");
        create_dir_all(name_dir).unwrap();
        let data_key_file_path = &name_dir.join("d");
        let mut file = File::create(data_key_file_path).unwrap();
        file.write_all(b"example2").unwrap();

        // Resolve secrets in YAML
        let input = r#"
        a: null
        b: false,
        c: 123
        d: "val1"
        e: [1, "2"]
        f:
          f1: 1
          f2: "val2"
        g: !str "val3"
        "${secret:kubernetes:a/b}": 123
        s1: "${secret:kubernetes:a/b}"
        s2: ["${secret:kubernetes:a/b}"]
        s3:
          s31: "${secret:kubernetes:a/b}"
          s32: ["${secret:kubernetes:a/b}", "${secret:kubernetes:c/d}"]
        s4: !str "${secret:kubernetes:c/d}"
        "#;
        let expectation = r#"
        a: null
        b: false,
        c: 123
        d: "val1"
        e: [1, "2"]
        f:
          f1: 1
          f2: "val2"
        g: !str "val3"
        "${secret:kubernetes:a/b}": 123
        s1: "example1"
        s2: ["example1"]
        s3:
          s31: "example1"
          s32: ["example1", "example2"]
        s4: !str "example2"
        "#;
        assert_eq!(
            resolve_secret_references_in_yaml(dir_path, serde_yaml::from_str(input).unwrap())
                .unwrap(),
            serde_yaml::from_str::<serde_yaml::Value>(expectation).unwrap()
        );
    }

    #[test]
    fn secret_resolution_connector_config() {
        // Create file at:
        // - <tempdir>/kubernetes/a/b
        // - <tempdir>/kubernetes/c/d
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let name_dir = &dir_path.join("kubernetes").join("a");
        create_dir_all(name_dir).unwrap();
        let data_key_file_path = &name_dir.join("b");
        let mut file = File::create(data_key_file_path).unwrap();
        file.write_all(b"example1").unwrap();
        let name_dir = &dir_path.join("kubernetes").join("c");
        create_dir_all(name_dir).unwrap();
        let data_key_file_path = &name_dir.join("d");
        let mut file = File::create(data_key_file_path).unwrap();
        file.write_all(b"example2").unwrap();

        // Resolve a connector configuration
        let connector_config_json = json!({
            "transport": {
              "name": "datagen",
              "config": {
                "plan": [{
                    "limit": 2,
                    "fields": {
                        "col1": { "values": [1, 2] },
                        "col2": { "values": ["${secret:kubernetes:a/b}", "${secret:kubernetes:c/d}"] }
                    }
                }]
              }
            }
        });
        let connector_config: ConnectorConfig =
            serde_json::from_value(connector_config_json).unwrap();
        let connector_config_secrets_resolved =
            resolve_secret_references_in_connector_config(dir_path, &connector_config).unwrap();
        let TransportConfig::Datagen(datagen_input_config) =
            connector_config_secrets_resolved.transport
        else {
            unreachable!();
        };
        assert_eq!(
            datagen_input_config.plan[0].fields["col2"]
                .values
                .as_ref()
                .unwrap(),
            &vec![json!("example1"), json!("example2")]
        );
    }
}
