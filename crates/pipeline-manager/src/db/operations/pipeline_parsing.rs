use crate::db::error::DBError;
use crate::db::types::pipeline::{
    parse_string_as_bootstrap_policy, parse_string_as_runtime_desired_status,
    parse_string_as_runtime_status, ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring,
    PipelineId,
};
use crate::db::types::program::{ProgramError, ProgramStatus};
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::utils::{
    validate_deployment_config, validate_program_config, validate_program_info,
    validate_runtime_config, validate_storage_status_details,
};
use crate::db::types::version::Version;
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus, RuntimeStatus};
use tokio_postgres::Row;
use tracing::error;
use uuid::Uuid;

pub const PIPELINE_COLUMNS_ALL: &str =
    "p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.platform_version, p.runtime_config,
     p.program_code, p.udf_rust, p.udf_toml, p.program_config, p.program_version, p.program_status,
     p.program_status_since, p.program_error, p.program_info,
     p.program_binary_source_checksum, p.program_binary_integrity_checksum, p.program_info_integrity_checksum,
     p.deployment_error, p.deployment_config, p.deployment_location, p.refresh_version,
     p.storage_status, p.storage_status_details, p.deployment_id, p.deployment_initial,
     p.deployment_resources_status, p.deployment_resources_status_details, p.deployment_resources_status_since,
     p.deployment_resources_desired_status, p.deployment_resources_desired_status_since,
     p.deployment_runtime_status, p.deployment_runtime_status_details, p.deployment_runtime_status_since,
     p.deployment_runtime_desired_status, p.deployment_runtime_desired_status_since, p.bootstrap_policy
     ";

pub const PIPELINE_COLUMNS_MONITORING: &str =
    "p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.platform_version,
     p.program_config, p.program_version, p.program_status, p.program_status_since,
     p.deployment_error, p.deployment_location, p.refresh_version,
     p.storage_status, p.storage_status_details, p.deployment_id, p.deployment_initial,
     p.deployment_resources_status, p.deployment_resources_status_since,
     p.deployment_resources_desired_status, p.deployment_resources_desired_status_since,
     p.deployment_runtime_status, p.deployment_runtime_status_details, p.deployment_runtime_status_since,
     p.deployment_runtime_desired_status, p.deployment_runtime_desired_status_since, p.bootstrap_policy
     ";

#[rustfmt::skip]
pub fn parse_pipeline_row_all(row: &Row) -> Result<ExtendedPipelineDescr, DBError> {
    Ok(ExtendedPipelineDescr {
        id: parse_from_row_id(row),
        name: parse_from_row_name(row),
        description: parse_from_row_description(row),
        created_at: parse_from_row_created_at(row),
        version: parse_from_row_version(row),
        platform_version: parse_from_row_platform_version(row),
        runtime_config: parse_from_row_runtime_config(row)?,
        program_code: parse_from_row_program_code(row),
        udf_rust: parse_from_row_udf_rust(row),
        udf_toml: parse_from_row_udf_toml(row),
        program_config: parse_from_row_program_config(row)?,
        program_version: parse_from_row_program_version(row),
        program_status: parse_from_row_program_status(row)?,
        program_status_since: parse_from_row_program_status_since(row),
        program_error: parse_from_row_program_error(row),
        program_info: parse_from_row_program_info(row)?,
        program_binary_source_checksum: parse_from_row_program_binary_source_checksum(row),
        program_binary_integrity_checksum: parse_from_row_program_binary_integrity_checksum(row),
        program_info_integrity_checksum: parse_from_row_program_info_integrity_checksum(row),
        deployment_error: parse_from_row_deployment_error(row)?,
        deployment_config: parse_from_row_deployment_config(row)?,
        deployment_location: parse_from_row_deployment_location(row),
        refresh_version: parse_from_row_refresh_version(row),
        storage_status: parse_from_row_storage_status(row)?,
        storage_status_details: parse_from_row_storage_status_details(row)?,
        deployment_id: parse_from_row_deployment_id(row),
        deployment_initial: parse_from_row_deployment_initial(row)?,
        bootstrap_policy: parse_from_row_bootstrap_policy(row)?,
        deployment_resources_status: parse_from_row_deployment_resources_status(row)?,
        deployment_resources_status_since: parse_from_row_deployment_resources_status_since(row),
        deployment_resources_status_details: parse_from_row_deployment_resources_status_details(row)?,
        deployment_resources_desired_status: parse_from_row_deployment_resources_desired_status(row)?,
        deployment_resources_desired_status_since: parse_from_row_deployment_resources_desired_status_since(row),
        deployment_runtime_status: parse_from_row_deployment_runtime_status(row)?,
        deployment_runtime_status_details: parse_from_row_deployment_runtime_status_details(row)?,
        deployment_runtime_status_since: parse_from_row_deployment_runtime_status_since(row),
        deployment_runtime_desired_status: parse_from_row_deployment_runtime_desired_status(row)?,
        deployment_runtime_desired_status_since: parse_from_row_deployment_runtime_desired_status_since(row),
    })
}

#[rustfmt::skip]
pub fn parse_pipeline_row_monitoring(row: &Row) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
    Ok(ExtendedPipelineDescrMonitoring {
        id: parse_from_row_id(row),
        name: parse_from_row_name(row),
        description: parse_from_row_description(row),
        created_at: parse_from_row_created_at(row),
        version: parse_from_row_version(row),
        platform_version: parse_from_row_platform_version(row),
        program_config: parse_from_row_program_config(row)?,
        program_version: parse_from_row_program_version(row),
        program_status: parse_from_row_program_status(row)?,
        program_status_since: parse_from_row_program_status_since(row),
        deployment_error: parse_from_row_deployment_error(row)?,
        deployment_location: parse_from_row_deployment_location(row),
        refresh_version: parse_from_row_refresh_version(row),
        storage_status: parse_from_row_storage_status(row)?,
        storage_status_details: parse_from_row_storage_status_details(row)?,
        deployment_id: parse_from_row_deployment_id(row),
        deployment_initial: parse_from_row_deployment_initial(row)?,
        bootstrap_policy: parse_from_row_bootstrap_policy(row)?,
        deployment_resources_status: parse_from_row_deployment_resources_status(row)?,
        deployment_resources_status_since: parse_from_row_deployment_resources_status_since(row),
        deployment_resources_desired_status: parse_from_row_deployment_resources_desired_status(row)?,
        deployment_resources_desired_status_since: parse_from_row_deployment_resources_desired_status_since(row),
        deployment_runtime_status: parse_from_row_deployment_runtime_status(row)?,
        deployment_runtime_status_details: parse_from_row_deployment_runtime_status_details(row)?,
        deployment_runtime_status_since: parse_from_row_deployment_runtime_status_since(row),
        deployment_runtime_desired_status: parse_from_row_deployment_runtime_desired_status(row)?,
        deployment_runtime_desired_status_since: parse_from_row_deployment_runtime_desired_status_since(row),
    })
}

fn parse_from_row_id(row: &Row) -> PipelineId {
    PipelineId(row.get("id"))
}

fn parse_from_row_name(row: &Row) -> String {
    row.get("name")
}

fn parse_from_row_description(row: &Row) -> String {
    row.get("description")
}

fn parse_from_row_created_at(row: &Row) -> DateTime<Utc> {
    row.get("created_at")
}

fn parse_from_row_version(row: &Row) -> Version {
    Version(row.get("version"))
}

fn parse_from_row_platform_version(row: &Row) -> String {
    row.get("platform_version")
}

fn parse_from_row_runtime_config(row: &Row) -> Result<serde_json::Value, DBError> {
    // Runtime configuration: RuntimeConfig
    let runtime_config = deserialize_json_value(row.get("runtime_config"))?;
    let _ = validate_runtime_config(&runtime_config, true); // Prints to log if validation failed
    Ok(runtime_config)
}

fn parse_from_row_program_code(row: &Row) -> String {
    row.get("program_code")
}

fn parse_from_row_udf_rust(row: &Row) -> String {
    row.get("udf_rust")
}

fn parse_from_row_udf_toml(row: &Row) -> String {
    row.get("udf_toml")
}

fn parse_from_row_program_config(row: &Row) -> Result<serde_json::Value, DBError> {
    // Program configuration: ProgramConfig
    let program_config = deserialize_json_value(row.get("program_config"))?;
    let _ = validate_program_config(&program_config, true); // Prints to log if validation failed
    Ok(program_config)
}

fn parse_from_row_program_version(row: &Row) -> Version {
    Version(row.get("program_version"))
}

fn parse_from_row_program_status(row: &Row) -> Result<ProgramStatus, DBError> {
    row.get::<_, String>("program_status").try_into()
}

fn parse_from_row_program_status_since(row: &Row) -> DateTime<Utc> {
    row.get("program_status_since")
}

/// Deserializes the string of JSON as an [`ProgramError`] with a default if deserialization fails.
fn parse_from_row_program_error(row: &Row) -> ProgramError {
    deserialize_program_error_with_default(&row.get::<_, String>("program_error"))
}

fn parse_from_row_program_info(row: &Row) -> Result<Option<serde_json::Value>, DBError> {
    // Program information: ProgramInfo
    let program_info = match row.get::<_, Option<String>>("program_info") {
        None => None,
        Some(s) => Some(deserialize_json_value(&s)?),
    };
    if let Some(value) = &program_info {
        let _ = validate_program_info(value); // Prints to log if validation failed
    }
    Ok(program_info)
}

fn parse_from_row_program_binary_source_checksum(row: &Row) -> Option<String> {
    row.get("program_binary_source_checksum")
}

fn parse_from_row_program_binary_integrity_checksum(row: &Row) -> Option<String> {
    row.get("program_binary_integrity_checksum")
}

fn parse_from_row_program_info_integrity_checksum(row: &Row) -> Option<String> {
    row.get("program_info_integrity_checksum")
}

fn parse_from_row_deployment_error(row: &Row) -> Result<Option<ErrorResponse>, DBError> {
    // Deployment error: ErrorResponse
    let deployment_error = match row.get::<_, Option<String>>("deployment_error") {
        None => None,
        Some(s) => Some(deserialize_error_response(&s)?),
    };
    Ok(deployment_error)
}

fn parse_from_row_deployment_config(row: &Row) -> Result<Option<serde_json::Value>, DBError> {
    // Deployment configuration: PipelineConfig
    let deployment_config = match row.get::<_, Option<String>>("deployment_config") {
        None => None,
        Some(s) => Some(deserialize_json_value(&s)?),
    };
    if let Some(value) = &deployment_config {
        let _ = validate_deployment_config(value); // Prints to log if validation failed
    }
    Ok(deployment_config)
}

fn parse_from_row_deployment_location(row: &Row) -> Option<String> {
    row.get("deployment_location")
}

fn parse_from_row_refresh_version(row: &Row) -> Version {
    Version(row.get("refresh_version"))
}

fn parse_from_row_storage_status(row: &Row) -> Result<StorageStatus, DBError> {
    row.get::<_, String>("storage_status").try_into()
}

fn parse_from_row_storage_status_details(row: &Row) -> Result<Option<serde_json::Value>, DBError> {
    // Storage status details: StorageStatusDetails
    let storage_status_details = match row.get::<_, Option<String>>("storage_status_details") {
        None => None,
        Some(s) => Some(deserialize_json_value(&s)?),
    };
    if let Some(value) = &storage_status_details {
        let _ = validate_storage_status_details(value); // Prints to log if validation failed
    }
    Ok(storage_status_details)
}

fn parse_from_row_deployment_id(row: &Row) -> Option<Uuid> {
    row.get("deployment_id")
}

fn parse_from_row_deployment_initial(row: &Row) -> Result<Option<RuntimeDesiredStatus>, DBError> {
    Ok(match row.get::<_, Option<String>>("deployment_initial") {
        None => None,
        Some(s) => Some(parse_string_as_runtime_desired_status(s)?),
    })
}

fn parse_from_row_deployment_resources_status(row: &Row) -> Result<ResourcesStatus, DBError> {
    row.get::<_, String>("deployment_resources_status")
        .try_into()
}

fn parse_from_row_deployment_resources_status_details(
    row: &Row,
) -> Result<Option<serde_json::Value>, DBError> {
    // Deployment resources status details
    Ok(
        match row.get::<_, Option<String>>("deployment_resources_status_details") {
            None => None,
            Some(s) => Some(deserialize_json_value(&s)?),
        },
    )
}

fn parse_from_row_deployment_resources_status_since(row: &Row) -> DateTime<Utc> {
    row.get("deployment_resources_status_since")
}

fn parse_from_row_deployment_resources_desired_status(
    row: &Row,
) -> Result<ResourcesDesiredStatus, DBError> {
    row.get::<_, String>("deployment_resources_desired_status")
        .try_into()
}

fn parse_from_row_deployment_resources_desired_status_since(row: &Row) -> DateTime<Utc> {
    row.get("deployment_resources_desired_status_since")
}

fn parse_from_row_deployment_runtime_status(row: &Row) -> Result<Option<RuntimeStatus>, DBError> {
    Ok(
        match row.get::<_, Option<String>>("deployment_runtime_status") {
            None => None,
            Some(s) => Some(parse_string_as_runtime_status(s)?),
        },
    )
}

fn parse_from_row_deployment_runtime_status_details(
    row: &Row,
) -> Result<Option<serde_json::Value>, DBError> {
    Ok(
        match row.get::<_, Option<String>>("deployment_runtime_status_details") {
            None => None,
            Some(s) => Some(deserialize_json_value(&s)?),
        },
    )
}

fn parse_from_row_deployment_runtime_status_since(row: &Row) -> Option<DateTime<Utc>> {
    row.get::<_, Option<DateTime<Utc>>>("deployment_runtime_status_since")
}

fn parse_from_row_deployment_runtime_desired_status(
    row: &Row,
) -> Result<Option<RuntimeDesiredStatus>, DBError> {
    Ok(
        match row.get::<_, Option<String>>("deployment_runtime_desired_status") {
            None => None,
            Some(s) => Some(parse_string_as_runtime_desired_status(s)?),
        },
    )
}

fn parse_from_row_deployment_runtime_desired_status_since(row: &Row) -> Option<DateTime<Utc>> {
    row.get("deployment_runtime_desired_status_since")
}

fn parse_from_row_bootstrap_policy(row: &Row) -> Result<Option<BootstrapPolicy>, DBError> {
    Ok(match row.get::<_, Option<String>>("bootstrap_policy") {
        None => None,
        Some(s) => Some(parse_string_as_bootstrap_policy(s)?),
    })
}

/// Parses string as a JSON value.
fn deserialize_json_value(s: &str) -> Result<serde_json::Value, DBError> {
    serde_json::from_str::<serde_json::Value>(s).map_err(|e| DBError::InvalidJsonData {
        data: s.to_string(),
        error: format!("unable to deserialize data string as JSON due to: {e}"),
    })
}

/// Serializes the [`ProgramError`] as a string of JSON.
pub fn serialize_program_error(program_error: &ProgramError) -> Result<String, DBError> {
    serde_json::to_string(program_error).map_err(|e| DBError::FailedToSerializeProgramError {
        error: e.to_string(),
    })
}

/// Serializes the [`ErrorResponse`] as a string of JSON.
pub(crate) fn serialize_error_response(error_response: &ErrorResponse) -> Result<String, DBError> {
    serde_json::to_string(error_response).map_err(|e| DBError::FailedToSerializeErrorResponse {
        error: e.to_string(),
    })
}

/// Deserializes the string of JSON as an [`ErrorResponse`].
pub fn deserialize_error_response(s: &str) -> Result<ErrorResponse, DBError> {
    let json_value = deserialize_json_value(s)?;
    let error_response: ErrorResponse =
        serde_json::from_value(json_value.clone()).map_err(|e| DBError::InvalidErrorResponse {
            value: json_value,
            error: e.to_string(),
        })?;
    Ok(error_response)
}

/// Deserializes the string of JSON as an [`ProgramError`].
fn deserialize_program_error(s: &str) -> Result<ProgramError, DBError> {
    let json_value = deserialize_json_value(s)?;
    let program_error: ProgramError =
        serde_json::from_value(json_value.clone()).map_err(|e| DBError::InvalidProgramError {
            value: json_value,
            error: e.to_string(),
        })?;
    Ok(program_error)
}

/// Deserializes the string of JSON as an [`ProgramError`] with a default if deserialization fails.
/// TODO: can be removed once no longer needed as fallback
fn deserialize_program_error_with_default(s: &str) -> ProgramError {
    deserialize_program_error(s).unwrap_or_else(|e| {
        error!("Backward incompatibility detected: the following string:\n{s}\n\n... is not a valid program error due to: {e}");
        ProgramError {
            sql_compilation: None,
            rust_compilation: None,
            system_error: None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        deserialize_error_response, deserialize_json_value, deserialize_program_error,
        deserialize_program_error_with_default, serialize_error_response, serialize_program_error,
        PIPELINE_COLUMNS_ALL, PIPELINE_COLUMNS_MONITORING,
    };
    use crate::db::error::DBError;
    use crate::db::types::pipeline::{
        ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineId,
    };
    use crate::db::types::program::{
        ConnectorGenerationError, ProgramError, ProgramStatus, RustCompilationInfo,
        SqlCompilationInfo, SqlCompilerMessage,
    };
    use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
    use crate::db::types::storage::StorageStatus;
    use crate::db::types::version::Version;
    use feldera_types::error::ErrorResponse;
    use feldera_types::program_schema::SourcePosition;
    use itertools::Itertools;
    use serde_json::json;
    use std::borrow::Cow;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    #[test]
    fn fields_consistency() {
        // Check that only the necessary fields are retrieved, and that the coverage of the fields
        // meets expectations (e.g., monitoring fields is a subset of all fields).

        // All columns
        let mut vec_columns_all: Vec<String> = PIPELINE_COLUMNS_ALL
            .split(",")
            .map(|s| s.split(".").collect_vec()[1].trim().to_string())
            .collect();
        vec_columns_all.sort();
        let set_columns_all = BTreeSet::from_iter(vec_columns_all.iter());

        // Monitoring columns
        let mut vec_columns_monitoring: Vec<String> = PIPELINE_COLUMNS_MONITORING
            .split(",")
            .map(|s| s.split(".").collect_vec()[1].trim().to_string())
            .collect();
        vec_columns_monitoring.sort();
        let set_columns_monitoring = BTreeSet::from_iter(vec_columns_monitoring.iter());

        // Monitoring is subset of all columns
        for col in &set_columns_monitoring {
            assert!(
                set_columns_all.contains(col),
                "{col} is missing in {set_columns_monitoring:?}"
            );
        }

        // Descriptor that should be the result of all fields
        let all_descr = serde_json::to_value(ExtendedPipelineDescr {
            id: PipelineId(Uuid::from_u128(1)),
            name: "".to_string(),
            description: "".to_string(),
            created_at: Default::default(),
            version: Version(1),
            platform_version: "".to_string(),
            runtime_config: Default::default(),
            program_code: "".to_string(),
            udf_rust: "".to_string(),
            udf_toml: "".to_string(),
            program_config: Default::default(),
            program_version: Version(2),
            program_status: ProgramStatus::Pending,
            program_status_since: Default::default(),
            program_error: ProgramError {
                sql_compilation: None,
                rust_compilation: None,
                system_error: None,
            },
            program_info: None,
            program_binary_source_checksum: None,
            program_binary_integrity_checksum: None,
            program_info_integrity_checksum: None,
            deployment_error: None,
            deployment_config: None,
            deployment_location: None,
            refresh_version: Version(3),
            storage_status: StorageStatus::Cleared,
            storage_status_details: None,
            deployment_id: None,
            deployment_initial: None,
            bootstrap_policy: None,
            deployment_resources_status: ResourcesStatus::Stopped,
            deployment_resources_status_since: Default::default(),
            deployment_resources_status_details: None,
            deployment_resources_desired_status: ResourcesDesiredStatus::Stopped,
            deployment_resources_desired_status_since: Default::default(),
            deployment_runtime_status: None,
            deployment_runtime_status_details: None,
            deployment_runtime_status_since: None,
            deployment_runtime_desired_status: None,
            deployment_runtime_desired_status_since: None,
        })
        .unwrap();
        let mut all_obj_keys = all_descr
            .as_object()
            .unwrap()
            .keys()
            .map(|s| s.to_string())
            .collect_vec();
        all_obj_keys.push("tenant_id".to_string());
        all_obj_keys.sort();
        assert_eq!(all_obj_keys, vec_columns_all);

        // Descriptor that should be the result of monitoring fields
        let monitoring_descr = serde_json::to_value(ExtendedPipelineDescrMonitoring {
            id: PipelineId(Uuid::from_u128(1)),
            name: "".to_string(),
            description: "".to_string(),
            created_at: Default::default(),
            version: Version(1),
            platform_version: "".to_string(),
            program_config: Default::default(),
            program_version: Version(2),
            program_status: ProgramStatus::Pending,
            program_status_since: Default::default(),
            deployment_error: None,
            deployment_location: None,
            refresh_version: Version(3),
            storage_status: StorageStatus::Cleared,
            storage_status_details: None,
            deployment_id: None,
            deployment_initial: None,
            deployment_resources_status: ResourcesStatus::Stopped,
            deployment_resources_status_since: Default::default(),
            deployment_resources_desired_status: ResourcesDesiredStatus::Stopped,
            deployment_resources_desired_status_since: Default::default(),
            deployment_runtime_status: None,
            deployment_runtime_status_details: None,
            deployment_runtime_status_since: None,
            deployment_runtime_desired_status: None,
            bootstrap_policy: None,
            deployment_runtime_desired_status_since: None,
        })
        .unwrap();
        let mut monitoring_obj_keys = monitoring_descr
            .as_object()
            .unwrap()
            .keys()
            .map(|s| s.to_string())
            .collect_vec();
        monitoring_obj_keys.push("tenant_id".to_string());
        monitoring_obj_keys.sort();
        assert_eq!(monitoring_obj_keys, vec_columns_monitoring);
    }

    #[test]
    fn json_value_deserialization() {
        // Valid
        for (s, expected) in [
            ("\"\"", json!("")),
            ("\"a\"", json!("a")),
            ("123", json!(123)),
            ("{}", json!({})),
            ("[1, 2, 3]", json!([1, 2, 3])),
            ("[1, { \"a\": 2 }, 3]", json!([1, { "a": 2 }, 3])),
            ("{\"a\": 1, \"b\": \"c\"}", json!({ "a": 1, "b": "c"})),
        ] {
            match deserialize_json_value(s) {
                Ok(value) => {
                    assert_eq!(
                        value, expected,
                        "Deserializing '{s}': resulting JSON does not match"
                    );
                }
                Err(e) => {
                    panic!("Deserializing '{s}': unable to deserialize as JSON: {e}");
                }
            }
        }

        // Invalid
        for s in [
            "",
            "\"a", // String not terminated
            "a: 1",
            "a: b",
            "a: \n- b\n- c",
        ] {
            assert!(matches!(
                deserialize_json_value(s).unwrap_err(),
                DBError::InvalidJsonData { data: _, error: _ }
            ));
        }
    }

    #[test]
    fn error_response_de_serialization() {
        // ErrorResponse -> JSON string -> ErrorResponse is the same as original
        let error_response = ErrorResponse {
            message: "Example error response message".to_string(),
            error_code: Cow::from("Abc".to_string()),
            details: json!({}),
        };
        let data = serialize_error_response(&error_response).unwrap();
        assert_eq!(error_response, deserialize_error_response(&data).unwrap());

        // Valid JSON for ErrorResponse
        assert_eq!(
            deserialize_error_response(
                "{\"message\": \"a\", \"error_code\": \"b\", \"details\": { \"c\": 1 } }"
            )
            .unwrap(),
            ErrorResponse {
                message: "a".to_string(),
                error_code: Cow::from("b".to_string()),
                details: json!({ "c": 1 }),
            }
        );

        // Invalid JSON for ErrorResponse (misses mandatory fields)
        assert!(matches!(
            deserialize_error_response("{}"),
            Err(DBError::InvalidErrorResponse { value: _, error: _ })
        ));
    }

    #[test]
    fn program_error_de_serialization() {
        // ProgramError -> JSON string -> ProgramError is the same as original
        let program_error = ProgramError {
            sql_compilation: Some(SqlCompilationInfo {
                exit_code: 123,
                messages: vec![SqlCompilerMessage::new_from_connector_generation_error(
                    ConnectorGenerationError::InvalidPropertyValue {
                        position: SourcePosition {
                            start_line_number: 4,
                            start_column: 5,
                            end_line_number: 6,
                            end_column: 7,
                        },
                        relation: "relation-example".to_string(),
                        key: "key-example".to_string(),
                        value: "value-example".to_string(),
                        reason: Box::new("reason-example".to_string()),
                    },
                )],
            }),
            rust_compilation: Some(RustCompilationInfo {
                exit_code: 89,
                stdout: "stdout-example".to_string(),
                stderr: "stderr-example".to_string(),
            }),
            system_error: Some("system-error-example".to_string()),
        };
        let data = serialize_program_error(&program_error).unwrap();
        assert_eq!(program_error, deserialize_program_error(&data).unwrap());

        // Valid JSON for ProgramError
        assert_eq!(
            deserialize_program_error("{}").unwrap(),
            ProgramError {
                sql_compilation: None,
                rust_compilation: None,
                system_error: None,
            }
        );
        assert_eq!(
            deserialize_program_error(
                "{ \"sql_compilation\": { \"exit_code\": 0, \"messages\": [] } }"
            )
            .unwrap(),
            ProgramError {
                sql_compilation: Some(SqlCompilationInfo {
                    exit_code: 0,
                    messages: vec![],
                }),
                rust_compilation: None,
                system_error: None,
            }
        );
        assert_eq!(
            deserialize_program_error(
                "{ \"sql_compilation\": { \"exit_code\": 12, \"messages\": [] } }"
            )
            .unwrap(),
            ProgramError {
                sql_compilation: Some(SqlCompilationInfo {
                    exit_code: 12,
                    messages: vec![],
                }),
                rust_compilation: None,
                system_error: None,
            }
        );
        assert_eq!(
            deserialize_program_error(
                "{ \"sql_compilation\": { \"exit_code\": 0, \"messages\": [] }, \"rust_compilation\": { \"exit_code\": 0, \"stdout\": \"\", \"stderr\": \"\" } }"
            )
            .unwrap(),
            ProgramError {
                sql_compilation: Some(SqlCompilationInfo {
                    exit_code: 0,
                    messages: vec![],
                }),
                rust_compilation: Some(RustCompilationInfo {
                    exit_code: 0,
                    stdout: "".to_string(),
                    stderr: "".to_string(),
                }),
                system_error: None,
            }
        );
        assert_eq!(
            deserialize_program_error(
                "{ \"sql_compilation\": { \"exit_code\": 2, \"messages\": [] }, \"rust_compilation\": { \"exit_code\": 3, \"stdout\": \"a\", \"stderr\": \"b\" } }"
            )
            .unwrap(),
            ProgramError {
                sql_compilation: Some(SqlCompilationInfo {
                    exit_code: 2,
                    messages: vec![],
                }),
                rust_compilation: Some(RustCompilationInfo {
                    exit_code: 3,
                    stdout: "a".to_string(),
                    stderr: "b".to_string(),
                }),
                system_error: None,
            }
        );
        assert_eq!(
            deserialize_program_error("{ \"system_error\": \"example\" }").unwrap(),
            ProgramError {
                sql_compilation: None,
                rust_compilation: None,
                system_error: Some("example".to_string()),
            }
        );
        assert_eq!(
            deserialize_program_error("{ \"system_error\": \"c\" }").unwrap(),
            ProgramError {
                sql_compilation: None,
                rust_compilation: None,
                system_error: Some("c".to_string()),
            }
        );
        assert_eq!(
            deserialize_program_error(
                "{ \"sql_compilation\": { \"exit_code\": 0, \"messages\": [] }, \"system_error\": \"example\" }"
            )
            .unwrap(),
            ProgramError {
                sql_compilation: Some(SqlCompilationInfo {
                    exit_code: 0,
                    messages: vec![],
                }),
                rust_compilation: None,
                system_error: Some("example".to_string()),
            }
        );
        assert_eq!(
            deserialize_program_error(
                "{ \"sql_compilation\": { \"exit_code\": 123, \"messages\": [] }, \"system_error\": \"abc\" }"
            )
            .unwrap(),
            ProgramError {
                sql_compilation: Some(SqlCompilationInfo {
                    exit_code: 123,
                    messages: vec![],
                }),
                rust_compilation: None,
                system_error: Some("abc".to_string()),
            }
        );

        // Invalid JSON for ProgramError
        assert!(matches!(
            deserialize_program_error(""),
            Err(DBError::InvalidJsonData { data: _, error: _ })
        ));
        assert!(matches!(
            deserialize_program_error("invalid"),
            Err(DBError::InvalidJsonData { data: _, error: _ })
        ));
        assert!(matches!(
            deserialize_program_error("{\"system_error\": 123}"),
            Err(DBError::InvalidProgramError { value: _, error: _ })
        ));

        // Should pass for the deserialization with default
        let default_program_error = ProgramError {
            sql_compilation: None,
            rust_compilation: None,
            system_error: None,
        };
        assert_eq!(
            deserialize_program_error_with_default(""),
            default_program_error
        );
        assert_eq!(
            deserialize_program_error_with_default("invalid"),
            default_program_error
        );
        assert_eq!(
            deserialize_program_error_with_default("{\"system_error\": 123}"),
            default_program_error
        );
    }
}
