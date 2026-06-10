use crate::db::error::DBError;
use crate::db::types::program::{ProgramConfig, ProgramInfo};
use crate::pipeline_env::validate_pipeline_env;
use feldera_types::config::{PipelineConfig, RuntimeConfig};
use feldera_types::runtime_status::StorageStatusDetails;
use regex::Regex;
use serde::Serialize;
use thiserror::Error as ThisError;
use tracing::error;

// Utility functions related to types which are stored in the database.
// The functions center around serialization, deserialization and validation.

/// Pattern for non-empty string containing lowercase (a-z), uppercase (A-Z),
/// number (0-9), underscore (_) or hyphen (-) characters.
const PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN: &str = r"^[a-zA-Z0-9_-]+$";

/// Description of the non-empty alphanumeric-underscore-hyphen pattern.
const PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN_DESCRIPTION: &str = "be non-empty and only \
contain lowercase (a-z), uppercase (A-Z), number (0-9), underscore (_) or hyphen (-) characters";

/// The pattern is almost the same as for Kubernetes label values but slightly stricter.
/// Pattern for non-empty string containing lowercase (a-z), uppercase (A-Z),
/// number (0-9), underscore (_) or hyphen (-) characters, but cannot start or end
/// with hyphen or underscore. In addition to Kubernetes label constraints, we do not
/// allow dot characters or empty strings.
pub const PATTERN_KUBERNETES_LABEL_VALUE: &str = r"^([A-Za-z0-9][-A-Za-z0-9_]*)?[A-Za-z0-9]$";

/// Description of the Kubernetes label value pattern.
pub const PATTERN_KUBERNETES_LABEL_VALUE_DESCRIPTION: &str = "be non-empty and only contain \
lowercase (a-z), uppercase (A-Z), number (0-9), underscore (_) or hyphen (-) characters, but \
cannot start or end with hyphen or underscore";

/// Maximum API key name length.
pub(crate) const MAXIMUM_API_KEY_NAME_LENGTH: usize = 100;

/// Maximum pipeline name length.
/// It is limited to 63 to make it fit in a Kubernetes label value.
pub(crate) const MAXIMUM_PIPELINE_NAME_LENGTH: usize = 63;

/// Maximum connector name length.
pub(crate) const MAXIMUM_CONNECTOR_NAME_LENGTH: usize = 100;

/// Checks the provided API key name is valid.
pub fn validate_api_key_name(name: &str) -> Result<(), DBError> {
    validate_name(
        name,
        MAXIMUM_API_KEY_NAME_LENGTH,
        PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN,
        PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN_DESCRIPTION,
    )
}

/// Checks the provided pipeline name is valid.
pub fn validate_pipeline_name(name: &str) -> Result<(), DBError> {
    validate_name(
        name,
        MAXIMUM_PIPELINE_NAME_LENGTH,
        PATTERN_KUBERNETES_LABEL_VALUE,
        PATTERN_KUBERNETES_LABEL_VALUE_DESCRIPTION,
    )
}

/// Checks the provided connector name is valid.
pub fn validate_connector_name(name: &str) -> Result<(), DBError> {
    validate_name(
        name,
        MAXIMUM_CONNECTOR_NAME_LENGTH,
        PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN,
        PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN_DESCRIPTION,
    )
}

/// Checks whether the provided name is valid.
/// The constraints are as follows:
/// - It cannot be empty
/// - It must be at most `length_limit` characters long
/// - It must contain characters that follow the `pattern`
fn validate_name(
    name: &str,
    length_limit: usize,
    pattern: &str,
    pattern_description: &str,
) -> Result<(), DBError> {
    if name.is_empty() {
        Err(DBError::EmptyName)
    } else if name.len() > length_limit {
        Err(DBError::TooLongName {
            name: name.to_string(),
            length: name.len(),
            maximum: length_limit,
        })
    } else {
        let re = Regex::new(pattern).expect("Pattern for name must be valid");
        if re.is_match(name) {
            Ok(())
        } else {
            Err(DBError::NameDoesNotMatchPattern {
                name: name.to_string(),
                pattern: pattern.to_string(),
                pattern_description: pattern_description.to_string(),
            })
        }
    }
}

/// Errors that can happen when deserializing a generic JSON value
/// to its actual object and performing any validation.
#[derive(Debug, Serialize, ThisError)]
pub enum ValidationError {
    #[error("could not deserialize due to: {0}")]
    DeserializationFailed(String),
    #[error("enterprise feature: {0}")]
    EnterpriseFeature(String),
    #[error("invalid pipeline environment: {0}")]
    InvalidPipelineEnv(String),
}

/// Deserializes generic JSON value into [`RuntimeConfig`] and performs any additional validation.
/// It should log an error if it was not used to validate initial user input.
pub(crate) fn validate_runtime_config(
    value: &serde_json::Value,
    log_if_invalid: bool,
) -> Result<RuntimeConfig, ValidationError> {
    let deserialize_result = serde_json::from_value::<RuntimeConfig>(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    match deserialize_result {
        Ok(runtime_config) => {
            #[cfg(not(feature = "feldera-enterprise"))]
            if runtime_config.fault_tolerance.is_enabled() {
                let e = ValidationError::EnterpriseFeature("fault tolerance".to_string());
                if log_if_invalid {
                    error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid runtime configuration due to: {e}");
                }
                return Err(e);
            }
            if let Err(e) = validate_pipeline_env(&runtime_config.env) {
                let e = ValidationError::InvalidPipelineEnv(e);
                if log_if_invalid {
                    error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid runtime configuration due to: {e}");
                }
                return Err(e);
            }
            Ok(runtime_config)
        }
        Err(e) => {
            if log_if_invalid {
                error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid runtime configuration due to: {e}");
            }
            Err(e)
        }
    }
}

/// Deserializes generic JSON value into [`ProgramConfig`] and performs any additional validation.
/// It should log an error if it was not used to validate initial user input.
pub(crate) fn validate_program_config(
    value: &serde_json::Value,
    log_if_invalid: bool,
) -> Result<ProgramConfig, ValidationError> {
    let deserialize_result = serde_json::from_value(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    if let Err(e) = &deserialize_result {
        if log_if_invalid {
            error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid program configuration due to: {e}");
        }
    }
    deserialize_result
}

/// Deserializes the generic JSON value into [`ProgramInfo`] and performs any additional validation.
pub(crate) fn validate_program_info(
    value: &serde_json::Value,
) -> Result<ProgramInfo, ValidationError> {
    let deserialize_result = serde_json::from_value(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    if let Err(e) = &deserialize_result {
        error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid program information due to: {e}");
    }
    deserialize_result
}

/// Deserializes the generic JSON value into [`PipelineConfig`] and performs any additional validation.
pub(crate) fn validate_deployment_config(
    value: &serde_json::Value,
) -> Result<PipelineConfig, ValidationError> {
    let deserialize_result = serde_json::from_value::<PipelineConfig>(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    match deserialize_result {
        Ok(deployment_config) => {
            if let Err(e) = validate_pipeline_env(&deployment_config.global.env) {
                let e = ValidationError::InvalidPipelineEnv(e);
                error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid deployment configuration due to: {e}");
                Err(e)
            } else {
                Ok(deployment_config)
            }
        }
        Err(e) => {
            error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid deployment configuration due to: {e}");
            Err(e)
        }
    }
}

/// Deserializes the generic JSON value into [`StorageStatusDetails`] and performs any additional validation.
pub(crate) fn validate_storage_status_details(
    value: &serde_json::Value,
) -> Result<StorageStatusDetails, ValidationError> {
    let deserialize_result = serde_json::from_value(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    if let Err(e) = &deserialize_result {
        error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid storage status details due to: {e}");
    }
    deserialize_result
}

#[cfg(test)]
mod tests {
    use super::{
        validate_api_key_name, validate_connector_name, validate_deployment_config,
        validate_pipeline_name, validate_program_config, validate_program_info,
        validate_runtime_config, ValidationError, MAXIMUM_API_KEY_NAME_LENGTH,
        MAXIMUM_CONNECTOR_NAME_LENGTH, MAXIMUM_PIPELINE_NAME_LENGTH,
        PATTERN_KUBERNETES_LABEL_VALUE, PATTERN_KUBERNETES_LABEL_VALUE_DESCRIPTION,
        PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN,
        PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN_DESCRIPTION,
    };
    use crate::db::error::DBError;
    use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramInfo};
    use feldera_types::config::{PipelineConfig, RuntimeConfig};
    use feldera_types::program_schema::ProgramSchema;
    use serde_json::json;

    #[test]
    fn test_valid_names() {
        // API key name
        let valid = vec![
            "a",
            "z",
            "A",
            "Z",
            "0",
            "9",
            "A-a",
            "A_a",
            "-",
            "_",
            "_-",
            "_a-",
            "exampleExample",
            "example-1",
            "example-of-this",
            "Aa0_-",
            "example_2",
            "Example",
            "EXAMPLE_example-example1234",
        ];
        for name in valid {
            assert!(validate_api_key_name(name).is_ok());
        }
        assert!(validate_api_key_name(&"a".repeat(MAXIMUM_API_KEY_NAME_LENGTH)).is_ok());
        assert!(validate_api_key_name(&"a".repeat(MAXIMUM_API_KEY_NAME_LENGTH - 1)).is_ok());

        // Pipeline name
        let valid = vec![
            "a",
            "z",
            "A",
            "Z",
            "0",
            "9",
            "A-a",
            "A_a",
            "exampleExample",
            "example-1",
            "example-of-this",
            "Aa0",
            "example_2",
            "Example",
            "EXAMPLE_example-example1234",
        ];
        for name in valid {
            assert!(validate_pipeline_name(name).is_ok());
        }
        assert!(validate_pipeline_name(&"a".repeat(MAXIMUM_PIPELINE_NAME_LENGTH)).is_ok());
        assert!(validate_pipeline_name(&"a".repeat(MAXIMUM_PIPELINE_NAME_LENGTH - 1)).is_ok());

        // Connector name
        let valid = vec![
            "a",
            "z",
            "A",
            "Z",
            "0",
            "9",
            "A-a",
            "A_a",
            "-",
            "_",
            "_-",
            "_a-",
            "exampleExample",
            "example-1",
            "example-of-this",
            "Aa0_-",
            "example_2",
            "Example",
            "EXAMPLE_example-example1234",
        ];
        for name in valid {
            assert!(validate_connector_name(name).is_ok());
        }
        assert!(validate_connector_name(&"a".repeat(MAXIMUM_CONNECTOR_NAME_LENGTH)).is_ok());
        assert!(validate_connector_name(&"a".repeat(MAXIMUM_CONNECTOR_NAME_LENGTH - 1)).is_ok());
    }

    #[test]
    fn test_invalid_names() {
        // API key name
        assert!(matches!(validate_api_key_name(""), Err(DBError::EmptyName)));
        let just_exceeds_max_string = "a".repeat(MAXIMUM_API_KEY_NAME_LENGTH + 1);
        assert!(
            matches!(validate_api_key_name(&just_exceeds_max_string), Err(DBError::TooLongName {
            name, length, maximum
        }) if name == just_exceeds_max_string && length == MAXIMUM_API_KEY_NAME_LENGTH + 1 && maximum == MAXIMUM_API_KEY_NAME_LENGTH)
        );
        let invalid_due_to_pattern =
            vec!["%", "$", "abc@", "example example", "a.b", ".", " ", "%20"];
        for invalid_name in invalid_due_to_pattern {
            assert!(
                matches!(validate_api_key_name(invalid_name), Err(DBError::NameDoesNotMatchPattern {
                    name,
                    pattern,
                    pattern_description,
                }) if name == invalid_name && pattern == PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN && pattern_description == PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN_DESCRIPTION)
            );
        }

        // Pipeline name
        assert!(matches!(
            validate_pipeline_name(""),
            Err(DBError::EmptyName)
        ));
        let just_exceeds_max_string = "a".repeat(MAXIMUM_PIPELINE_NAME_LENGTH + 1);
        assert!(
            matches!(validate_pipeline_name(&just_exceeds_max_string), Err(DBError::TooLongName {
            name, length, maximum
        }) if name == just_exceeds_max_string && length == MAXIMUM_PIPELINE_NAME_LENGTH + 1 && maximum == MAXIMUM_PIPELINE_NAME_LENGTH)
        );
        let invalid_due_to_pattern = vec![
            "%",
            "$",
            "abc@",
            "example example",
            " ",
            "%20",
            "-",
            "_",
            "_-",
            "_a-",
            "Aa0_-",
            "_Aa0-",
            "a.b",
            ".",
        ];
        for invalid_name in invalid_due_to_pattern {
            assert!(
                matches!(validate_pipeline_name(invalid_name), Err(DBError::NameDoesNotMatchPattern {
                    name,
                    pattern,
                    pattern_description,
                }) if name == invalid_name && pattern == PATTERN_KUBERNETES_LABEL_VALUE && pattern_description == PATTERN_KUBERNETES_LABEL_VALUE_DESCRIPTION)
            );
        }

        // Connector name
        assert!(matches!(
            validate_connector_name(""),
            Err(DBError::EmptyName)
        ));
        let just_exceeds_max_string = "a".repeat(MAXIMUM_CONNECTOR_NAME_LENGTH + 1);
        assert!(
            matches!(validate_connector_name(&just_exceeds_max_string), Err(DBError::TooLongName {
            name, length, maximum
        }) if name == just_exceeds_max_string && length == MAXIMUM_CONNECTOR_NAME_LENGTH + 1 && maximum == MAXIMUM_CONNECTOR_NAME_LENGTH)
        );
        let invalid_due_to_pattern =
            vec!["%", "$", "abc@", "example example", "a.b", ".", " ", "%20"];
        for invalid_name in invalid_due_to_pattern {
            assert!(
                matches!(validate_connector_name(invalid_name), Err(DBError::NameDoesNotMatchPattern {
                    name,
                    pattern,
                    pattern_description,
                }) if name == invalid_name && pattern == PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN && pattern_description == PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN_DESCRIPTION)
            );
        }
    }

    #[test]
    fn runtime_config_validation() {
        // RuntimeConfig -> JSON -> RuntimeConfig is the same as original
        let runtime_config = RuntimeConfig::default();
        let value = serde_json::to_value(runtime_config.clone()).unwrap();
        assert_eq!(
            runtime_config,
            validate_runtime_config(&value, true).unwrap()
        );

        // Invalid JSON for RuntimeConfig
        assert!(matches!(
            validate_runtime_config(&json!({ "workers": "not-a-number" }), true),
            Err(ValidationError::DeserializationFailed(_))
        ));

        #[cfg(feature = "feldera-enterprise")]
        assert!(
            validate_runtime_config(&json!({ "fault_tolerance": {} }), true)
                .unwrap()
                .fault_tolerance
                .model
                .is_some()
        );

        #[cfg(not(feature = "feldera-enterprise"))]
        assert!(matches!(
            validate_runtime_config(&json!({ "fault_tolerance": {} }), true),
            Err(ValidationError::EnterpriseFeature(s)) if s == "fault tolerance"
        ));

        assert!(matches!(
            validate_runtime_config(&json!({ "env": { "TOKIO_WORKER_THREADS": "1" } }), true),
            Err(ValidationError::InvalidPipelineEnv(_))
        ));
    }

    #[test]
    fn program_config_validation() {
        // ProgramConfig -> JSON -> ProgramConfig is the same as original
        let program_config = ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
            cache: false,
            runtime_version: None,
        };
        let value = serde_json::to_value(program_config.clone()).unwrap();
        assert_eq!(
            program_config,
            validate_program_config(&value, true).unwrap()
        );

        // Invalid JSON for ProgramConfig
        assert!(matches!(
            validate_program_config(&json!({ "profile": "non-existent-profile" }), true),
            Err(ValidationError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn program_info_validation() {
        // ProgramInfo -> JSON -> ProgramInfo is the same as original
        let program_info = ProgramInfo {
            schema: serde_json::to_value(ProgramSchema {
                inputs: vec![],
                outputs: vec![],
            })
            .unwrap(),
            main_rust: "".to_string(),
            udf_stubs: "".to_string(),
            input_connectors: Default::default(),
            output_connectors: Default::default(),
            dataflow: None,
        };
        let value = serde_json::to_value(program_info.clone()).unwrap();
        assert_eq!(program_info, validate_program_info(&value).unwrap());

        // Invalid JSON for ProgramInfo
        assert!(matches!(
            validate_program_info(&json!({ "main_rust": 123 })),
            Err(ValidationError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn deployment_config_validation() {
        // PipelineConfig -> JSON -> PipelineConfig is the same as original
        let deployment_config = PipelineConfig {
            global: Default::default(),
            multihost: None,
            name: None,
            given_name: None,
            storage_config: None,
            secrets_dir: None,
            inputs: Default::default(),
            outputs: Default::default(),
            program_ir: None,
        };
        let value = serde_json::to_value(deployment_config.clone()).unwrap();
        assert_eq!(
            deployment_config,
            validate_deployment_config(&value).unwrap()
        );

        // Invalid JSON for PipelineConfig
        assert!(matches!(
            validate_deployment_config(&json!({ "name": 123 })),
            Err(ValidationError::DeserializationFailed(_))
        ));

        assert!(matches!(
            validate_deployment_config(
                &json!({ "workers": 8, "env": { "TOKIO_WORKER_THREADS": "1" } })
            ),
            Err(ValidationError::InvalidPipelineEnv(_))
        ));
    }
}
