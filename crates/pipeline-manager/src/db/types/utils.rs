use crate::db::error::DBError;
use crate::db::types::program::{ProgramConfig, ProgramInfo};
use feldera_types::config::{PipelineConfig, RuntimeConfig};
use log::error;
use regex::Regex;
use serde::Serialize;
use thiserror::Error as ThisError;

// Utility functions related to types which are stored in the database.
// The functions center around serialization, deserialization and validation.

/// Pattern that every name must adhere to.
pub const PATTERN_VALID_NAME: &str = r"^[a-zA-Z0-9_-]+$";

/// Maximum name length.
pub const MAXIMUM_NAME_LENGTH: usize = 100;

/// Checks whether the provided name is valid.
/// The constraints are as follows:
/// - It cannot be empty
/// - It must be at most 100 characters long
/// - It must contain only characters which are lowercase (a-z), uppercase (A-Z),
//    numbers (0-9), underscores (_) or hyphens (-)
pub fn validate_name(name: &str) -> Result<(), DBError> {
    if name.is_empty() {
        Err(DBError::EmptyName)
    } else if name.len() > MAXIMUM_NAME_LENGTH {
        Err(DBError::TooLongName {
            name: name.to_string(),
            length: name.len(),
            maximum: MAXIMUM_NAME_LENGTH,
        })
    } else {
        let re = Regex::new(PATTERN_VALID_NAME).expect("Pattern for name must be valid");
        if re.is_match(name) {
            Ok(())
        } else {
            Err(DBError::NameDoesNotMatchPattern {
                name: name.to_string(),
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
    let deserialize_result = serde_json::from_value(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    if let Err(e) = &deserialize_result {
        error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid deployment configuration due to: {e}");
    }
    deserialize_result
}

#[cfg(test)]
mod tests {
    use super::{
        validate_deployment_config, validate_name, validate_program_config, validate_program_info,
        validate_runtime_config, ValidationError,
    };
    use crate::db::error::DBError;
    use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramInfo};
    use feldera_types::config::{PipelineConfig, RuntimeConfig};
    use feldera_types::program_schema::ProgramSchema;
    use serde_json::json;

    #[test]
    fn test_valid_names() {
        let valid = vec![
            "a",
            "z",
            "A",
            "Z",
            "0",
            "9",
            "-",
            "_",
            "exampleExample",
            "example-1",
            "example-of-this",
            "Aa0_-",
            "example_2",
            "Example",
            "EXAMPLE_example-example1234",
        ];
        for name in valid {
            assert!(validate_name(name).is_ok());
        }
    }

    #[test]
    fn test_invalid_names() {
        assert!(matches!(validate_name(""), Err(DBError::EmptyName)));
        assert!(
            matches!(validate_name("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), Err(DBError::TooLongName {
            name, length, maximum
        }) if name == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" && length == 101 && maximum == 100)
        );
        let invalid_due_to_pattern = vec!["%", "$", "abc@", "example example", " ", "%20"];
        for invalid_name in invalid_due_to_pattern {
            assert!(
                matches!(validate_name(invalid_name), Err(DBError::NameDoesNotMatchPattern {
                name
            }) if name == invalid_name)
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
            schema: ProgramSchema {
                inputs: vec![],
                outputs: vec![],
            },
            main_rust: "".to_string(),
            udf_stubs: "".to_string(),
            input_connectors: Default::default(),
            output_connectors: Default::default(),
            dataflow: serde_json::Value::Null,
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
            name: None,
            storage_config: None,
            inputs: Default::default(),
            outputs: Default::default(),
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
    }
}
