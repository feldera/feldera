use crate::db::error::DBError;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_unique_violation,
};
use crate::db::types::pipeline::{
    validate_deployment_desired_status_transition, validate_deployment_status_transition,
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDescr, PipelineDesiredStatus,
    PipelineId, PipelineStatus,
};
use crate::db::types::program::{
    validate_program_status_transition, ProgramError, ProgramStatus, RustCompilationInfo,
    SqlCompilationInfo,
};
use crate::db::types::storage::{validate_storage_status_transition, StorageStatus};
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::{
    validate_deployment_config, validate_name, validate_program_config, validate_program_info,
    validate_runtime_config,
};
use crate::db::types::version::Version;
use deadpool_postgres::Transaction;
use feldera_types::error::ErrorResponse;
use log::error;
use tokio_postgres::Row;
use uuid::Uuid;

/// Parses string as a JSON value.
fn deserialize_json_value(s: &str) -> Result<serde_json::Value, DBError> {
    serde_json::from_str::<serde_json::Value>(s).map_err(|e| DBError::InvalidJsonData {
        data: s.to_string(),
        error: format!("unable to deserialize data string as JSON due to: {e}"),
    })
}

/// Serializes the [`ProgramError`] as a string of JSON.
fn serialize_program_error(program_error: &ProgramError) -> Result<String, DBError> {
    serde_json::to_string(program_error).map_err(|e| DBError::FailedToSerializeProgramError {
        error: e.to_string(),
    })
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

/// Serializes the [`ErrorResponse`] as a string of JSON.
fn serialize_error_response(error_response: &ErrorResponse) -> Result<String, DBError> {
    serde_json::to_string(error_response).map_err(|e| DBError::FailedToSerializeErrorResponse {
        error: e.to_string(),
    })
}

/// Deserializes the string of JSON as an [`ErrorResponse`].
fn deserialize_error_response(s: &str) -> Result<ErrorResponse, DBError> {
    let json_value = deserialize_json_value(s)?;
    let error_response: ErrorResponse =
        serde_json::from_value(json_value.clone()).map_err(|e| DBError::InvalidErrorResponse {
            value: json_value,
            error: e.to_string(),
        })?;
    Ok(error_response)
}

/// All pipeline columns.
const RETRIEVE_PIPELINE_COLUMNS: &str =
    "p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.platform_version, p.runtime_config,
     p.program_code, p.udf_rust, p.udf_toml, p.program_config, p.program_version, p.program_status,
     p.program_status_since, p.program_error, p.program_info,
     p.program_binary_source_checksum, p.program_binary_integrity_checksum,
     p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
     p.deployment_error, p.deployment_config, p.deployment_location, p.refresh_version, p.suspend_info, p.storage_status";

/// Converts a pipeline table row to its extended descriptor with all fields.
///
/// Backward compatibility:
/// - Fields `runtime_config`, `program_config`, `program_info`, `deployment_config` and `suspend_info`
///   are deserialized as JSON values -- they are not deserialized as their actual types.
///   Backward incompatible changes in those actual types will not prevent retrieval of
///   pipelines as long as they are serialized as valid JSON.
/// - All other fields are deserialized as their actual types.
///   Backwards incompatible changes therein will prevent retrieval of pipelines
///   because an error will be returned instead.
fn row_to_extended_pipeline_descriptor(row: &Row) -> Result<ExtendedPipelineDescr, DBError> {
    assert_eq!(row.len(), 28);

    // Runtime configuration: RuntimeConfig
    let runtime_config = deserialize_json_value(row.get(7))?;
    let _ = validate_runtime_config(&runtime_config, true); // Prints to log if validation failed

    // Program configuration: ProgramConfig
    let program_config = deserialize_json_value(row.get(11))?;
    let _ = validate_program_config(&program_config, true); // Prints to log if validation failed

    // Program error: ProgramError
    let program_error = deserialize_program_error_with_default(&row.get::<_, String>(15));

    // Program information: ProgramInfo
    let program_info = match row.get::<_, Option<String>>(16) {
        None => None,
        Some(s) => Some(deserialize_json_value(&s)?),
    };
    if let Some(value) = &program_info {
        let _ = validate_program_info(value); // Prints to log if validation failed
    }

    // Deployment error: ErrorResponse
    let deployment_error = match row.get::<_, Option<String>>(22) {
        None => None,
        Some(s) => Some(deserialize_error_response(&s)?),
    };

    // Deployment configuration: PipelineConfig
    let deployment_config = match row.get::<_, Option<String>>(23) {
        None => None,
        Some(s) => Some(deserialize_json_value(&s)?),
    };
    if let Some(value) = &deployment_config {
        let _ = validate_deployment_config(value); // Prints to log if validation failed
    }

    // Suspend information
    let suspend_info = match row.get::<_, Option<String>>(26) {
        None => None,
        Some(s) => Some(deserialize_json_value(&s)?),
    };

    Ok(ExtendedPipelineDescr {
        id: PipelineId(row.get(0)),
        // tenant_id is not used
        name: row.get(2),
        description: row.get(3),
        created_at: row.get(4),
        version: Version(row.get(5)),
        platform_version: row.get(6),
        runtime_config,
        program_code: row.get(8),
        udf_rust: row.get(9),
        udf_toml: row.get(10),
        program_config,
        program_version: Version(row.get(12)),
        program_status: row.get::<_, String>(13).try_into()?,
        program_status_since: row.get(14),
        program_error,
        program_info,
        program_binary_source_checksum: row.get(17),
        program_binary_integrity_checksum: row.get(18),
        deployment_status: row.get::<_, String>(19).try_into()?,
        deployment_status_since: row.get(20),
        deployment_desired_status: row.get::<_, String>(21).try_into()?,
        deployment_error,
        deployment_config,
        deployment_location: row.get(24),
        refresh_version: Version(row.get(25)),
        suspend_info,
        storage_status: row.get::<_, String>(27).try_into()?,
    })
}

/// Pipeline columns relevant to monitoring.
const RETRIEVE_PIPELINE_MONITORING_COLUMNS: &str =
    "p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.platform_version,
     p.program_version, p.program_status, p.program_status_since,
     p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
     p.deployment_error, p.deployment_location, p.refresh_version, p.storage_status";

/// Converts a pipeline table row to its extended descriptor with only fields relevant to monitoring.
fn row_to_extended_pipeline_descriptor_monitoring(
    row: &Row,
) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
    assert_eq!(row.len(), 17);
    // Deployment error: ErrorResponse
    let deployment_error = match row.get::<_, Option<String>>(13) {
        None => None,
        Some(s) => Some(deserialize_error_response(&s)?),
    };
    Ok(ExtendedPipelineDescrMonitoring {
        id: PipelineId(row.get(0)),
        // tenant_id is not used
        name: row.get(2),
        description: row.get(3),
        created_at: row.get(4),
        version: Version(row.get(5)),
        platform_version: row.get(6),
        program_version: Version(row.get(7)),
        program_status: row.get::<_, String>(8).try_into()?,
        program_status_since: row.get(9),
        deployment_status: row.get::<_, String>(10).try_into()?,
        deployment_status_since: row.get(11),
        deployment_desired_status: row.get::<_, String>(12).try_into()?,
        deployment_error,
        deployment_location: row.get(14),
        refresh_version: Version(row.get(15)),
        storage_status: row.get::<_, String>(16).try_into()?,
    })
}

pub(crate) async fn list_pipelines(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<ExtendedPipelineDescr>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_COLUMNS}
             FROM pipeline AS p
             WHERE p.tenant_id = $1
             ORDER BY p.id ASC"
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[&tenant_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(row_to_extended_pipeline_descriptor(&row)?);
    }
    Ok(result)
}

pub(crate) async fn list_pipelines_for_monitoring(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<ExtendedPipelineDescrMonitoring>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_MONITORING_COLUMNS}
             FROM pipeline AS p
             WHERE p.tenant_id = $1
             ORDER BY p.id ASC"
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[&tenant_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(row_to_extended_pipeline_descriptor_monitoring(&row)?);
    }
    Ok(result)
}

pub(crate) async fn get_pipeline(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<ExtendedPipelineDescr, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_COLUMNS}
             FROM pipeline AS p
             WHERE p.tenant_id = $1 AND p.name = $2
            "
        ))
        .await?;
    let row = txn.query_opt(&stmt, &[&tenant_id.0, &name]).await?.ok_or(
        DBError::UnknownPipelineName {
            pipeline_name: name.to_string(),
        },
    )?;
    row_to_extended_pipeline_descriptor(&row)
}

pub(crate) async fn get_pipeline_for_monitoring(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_MONITORING_COLUMNS}
             FROM pipeline AS p
             WHERE p.tenant_id = $1 AND p.name = $2
            "
        ))
        .await?;
    let row = txn.query_opt(&stmt, &[&tenant_id.0, &name]).await?.ok_or(
        DBError::UnknownPipelineName {
            pipeline_name: name.to_string(),
        },
    )?;
    row_to_extended_pipeline_descriptor_monitoring(&row)
}

pub async fn get_pipeline_by_id(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<ExtendedPipelineDescr, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_COLUMNS}
             FROM pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = $2
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0])
        .await?
        .ok_or(DBError::UnknownPipeline { pipeline_id })?;
    row_to_extended_pipeline_descriptor(&row)
}

pub async fn get_pipeline_by_id_for_monitoring(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_MONITORING_COLUMNS}
             FROM pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = $2
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0])
        .await?
        .ok_or(DBError::UnknownPipeline { pipeline_id })?;
    row_to_extended_pipeline_descriptor_monitoring(&row)
}

pub(crate) async fn new_pipeline(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    new_id: Uuid,
    platform_version: &str,
    pipeline: PipelineDescr,
) -> Result<(PipelineId, Version), DBError> {
    validate_name(&pipeline.name)?;
    // Validate runtime configuration JSON when deserializing it
    // and reserialize it to have it contain current default values
    let runtime_config =
        validate_runtime_config(&pipeline.runtime_config, false).map_err(|error| {
            DBError::InvalidRuntimeConfig {
                value: pipeline.runtime_config.clone(),
                error,
            }
        })?;
    let runtime_config = serde_json::to_value(&runtime_config).map_err(|e| {
        DBError::FailedToSerializeRuntimeConfig {
            error: e.to_string(),
        }
    })?;

    // Validate program configuration JSON when deserializing it
    // and reserialize it to have it contain current default values
    let program_config =
        validate_program_config(&pipeline.program_config, false).map_err(|error| {
            DBError::InvalidProgramConfig {
                value: pipeline.program_config.clone(),
                error,
            }
        })?;
    let program_config = serde_json::to_value(&program_config).map_err(|e| {
        DBError::FailedToSerializeProgramConfig {
            error: e.to_string(),
        }
    })?;

    let stmt = txn

        .prepare_cached(
            "INSERT INTO pipeline (id, tenant_id, name, description, created_at, version, platform_version, runtime_config,
                                   program_code, udf_rust, udf_toml, program_config, program_version, program_status,
                                   program_status_since, program_error, program_info,
                                   program_binary_source_checksum, program_binary_integrity_checksum,
                                   deployment_status, deployment_status_since, deployment_desired_status,
                                   deployment_error, deployment_config, deployment_location, uses_json, refresh_version, suspend_info, storage_status)
            VALUES ($1, $2, $3, $4, now(), $5, $6, $7,
                    $8, $9, $10, $11, $12, $13,
                    now(), $14, NULL,
                    NULL, NULL,
                    $15, now(), $16,
                    NULL, NULL, NULL, TRUE, $17, NULL, $18)",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_id,                             // $1: id
            &tenant_id.0,                        // $2: tenant_id
            &pipeline.name,                      // $3: name
            &pipeline.description,               // $4: description
            &Version(1).0,                       // $5: version
            &platform_version.to_string(),       // $6: platform_version
            &runtime_config.to_string(),         // $7: runtime_config
            &pipeline.program_code,              // $8: program_code
            &pipeline.udf_rust,                  // $9: udf_rust
            &pipeline.udf_toml,                  // $10: udf_toml
            &program_config.to_string(),         // $11: program_config
            &Version(1).0,                       // $12: program_version
            &ProgramStatus::Pending.to_string(), // $13: program_status
            // $14: program_error
            &serialize_program_error(&ProgramError {
                sql_compilation: None,
                rust_compilation: None,
                system_error: None,
            })?,
            &PipelineStatus::Stopped.to_string(), // $15: deployment_status
            &PipelineStatus::Stopped.to_string(), // $16: deployment_desired_status
            &Version(1).0,                        // $17: refresh_version
            &StorageStatus::Cleared.to_string(),  // $18: storage_status
        ],
    )
    .await
    .map_err(maybe_unique_violation)
    .map_err(|e| maybe_tenant_id_foreign_key_constraint_err(e, tenant_id))?;
    Ok((PipelineId(new_id), Version(1)))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_pipeline(
    txn: &Transaction<'_>,
    is_compiler_update: bool,
    tenant_id: TenantId,
    original_name: &str,
    name: &Option<String>,
    description: &Option<String>,
    platform_version: &str,
    runtime_config: &Option<serde_json::Value>,
    program_code: &Option<String>,
    udf_rust: &Option<String>,
    udf_toml: &Option<String>,
    program_config: &Option<serde_json::Value>,
) -> Result<Version, DBError> {
    if let Some(name) = name {
        validate_name(name)?;
    }

    // Validate runtime configuration JSON when deserializing it
    // and reserialize it to have it contain current default values
    let runtime_config = match runtime_config {
        None => None,
        Some(runtime_config) => {
            let runtime_config =
                validate_runtime_config(runtime_config, false).map_err(|error| {
                    DBError::InvalidRuntimeConfig {
                        value: runtime_config.clone(),
                        error,
                    }
                })?;
            Some(serde_json::to_value(&runtime_config).map_err(|e| {
                DBError::FailedToSerializeRuntimeConfig {
                    error: e.to_string(),
                }
            })?)
        }
    };

    // Validate program configuration JSON when deserializing it
    // and reserialize it to have it contain current default values
    let program_config = match program_config {
        None => None,
        Some(program_config) => {
            let program_config =
                validate_program_config(program_config, false).map_err(|error| {
                    DBError::InvalidProgramConfig {
                        value: program_config.clone(),
                        error,
                    }
                })?;
            Some(serde_json::to_value(&program_config).map_err(|e| {
                DBError::FailedToSerializeProgramConfig {
                    error: e.to_string(),
                }
            })?)
        }
    };

    // Fetch current pipeline to decide how to update.
    // This will also return an error if the pipeline does not exist.
    let current = get_pipeline(txn, tenant_id, original_name).await?;

    // Pipeline update is allowed if either:
    // - Current status is `Stopped` AND desired status is `Stopped`
    // - Current status is `Stopped` AND desired status is `Paused` or `Running` AND it is the
    //   compiler doing the update to bump platform version (the early start mechanism)
    if !matches!(
        (
            is_compiler_update,
            current.deployment_status,
            current.deployment_desired_status
        ),
        (_, PipelineStatus::Stopped, PipelineDesiredStatus::Stopped)
            | (
                true,
                PipelineStatus::Stopped,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running
            ),
    ) {
        return Err(DBError::UpdateRestrictedToStopped);
    }

    // If it is a compiler update, then the only thing it must change is the platform_version
    assert!(
        !is_compiler_update
            || (platform_version != current.platform_version
                && name.is_none()
                && description.is_none()
                && runtime_config.is_none()
                && program_code.is_none()
                && udf_rust.is_none()
                && udf_toml.is_none()
                && program_config.is_none())
    );

    // If nothing changes in any of the core fields, return the current version
    if (name.is_none() || name.as_ref().is_some_and(|v| *v == current.name))
        && (description.is_none()
            || description
                .as_ref()
                .is_some_and(|v| *v == current.description))
        && (platform_version == current.platform_version)
        && (runtime_config.is_none()
            || runtime_config
                .as_ref()
                .is_some_and(|v| *v == current.runtime_config))
        && (program_code.is_none()
            || program_code
                .as_ref()
                .is_some_and(|v| *v == current.program_code))
        && (udf_rust.is_none() || udf_rust.as_ref().is_some_and(|v| *v == current.udf_rust))
        && (udf_toml.is_none() || udf_toml.as_ref().is_some_and(|v| *v == current.udf_toml))
        && (program_config.is_none()
            || program_config
                .as_ref()
                .is_some_and(|v| *v == current.program_config))
    {
        return Ok(current.version);
    }

    // Check if backfill avoidance is enabled in dev_tweaks
    let backfill_avoidance_enabled = current
        .runtime_config
        .get("dev_tweaks")
        .and_then(|dev_tweaks| dev_tweaks.get("backfill_avoidance"))
        .and_then(|backfill| backfill.as_bool())
        .unwrap_or(false);

    // Certain edits are restricted to cleared storage
    if current.storage_status != StorageStatus::Cleared {
        let mut not_allowed = vec![];
        if name.as_ref().is_some_and(|v| *v != current.name) {
            not_allowed.push("`name`")
        }
        if description
            .as_ref()
            .is_some_and(|v| *v != current.description)
        {
            not_allowed.push("`description`")
        }
        // `platform_version` can be updated
        // Some fields of `runtime_config` are not allowed to be updated
        if let Some(runtime_config) = &runtime_config {
            if runtime_config.get("workers") != current.runtime_config.get("workers") {
                not_allowed.push("`runtime_config.workers`");
            }
            if runtime_config.get("fault_tolerance")
                != current.runtime_config.get("fault_tolerance")
            {
                not_allowed.push("`runtime_config.fault_tolerance`");
            }
            if runtime_config
                .get("resources")
                .map(|v| v.get("storage_mb_max"))
                != current
                    .runtime_config
                    .get("resources")
                    .map(|v| v.get("storage_mb_max"))
            {
                not_allowed.push("`runtime_config.resources.storage_mb_max`");
            }
        }
        if !backfill_avoidance_enabled
            && program_code
                .as_ref()
                .is_some_and(|v| *v != current.program_code)
        {
            not_allowed.push("`program_code`")
        }
        if !backfill_avoidance_enabled && udf_rust.as_ref().is_some_and(|v| *v != current.udf_rust)
        {
            not_allowed.push("`udf_rust`")
        }
        if !backfill_avoidance_enabled && udf_toml.as_ref().is_some_and(|v| *v != current.udf_toml)
        {
            not_allowed.push("`udf_toml`")
        }
        if !backfill_avoidance_enabled
            && program_config
                .as_ref()
                .is_some_and(|v| *v != current.program_config)
        {
            not_allowed.push("`program_config`")
        }
        if !not_allowed.is_empty() {
            return Err(DBError::EditRestrictedToClearedStorage {
                not_allowed: not_allowed.iter_mut().map(|s| s.to_string()).collect(),
            });
        }
    }

    // Otherwise, one of the fields is non-null and different, and as such it should be updated
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET name = COALESCE($1, name),
                     description = COALESCE($2, description),
                     platform_version = $3,
                     runtime_config = COALESCE($4, runtime_config),
                     program_code = COALESCE($5, program_code),
                     udf_rust = COALESCE($6, udf_rust),
                     udf_toml = COALESCE($7, udf_toml),
                     program_config = COALESCE($8, program_config),
                     version = version + 1,
                     refresh_version = refresh_version + 1
                 WHERE tenant_id = $9 AND name = $10",
        )
        .await?;
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &name,
                &description,
                &platform_version.to_string(),
                &runtime_config.as_ref().map(|v| v.to_string()),
                &program_code,
                &udf_rust,
                &udf_toml,
                &program_config.as_ref().map(|v| v.to_string()),
                &tenant_id.0,
                &original_name,
            ],
        )
        .await
        .map_err(maybe_unique_violation)?;
    assert_eq!(rows_affected, 1); // The row must exist as it has been retrieved before

    // If the program changed, the program version must be incremented,
    // the schema unset and the status back to pending.
    let program_changed = (platform_version != current.platform_version)
        || program_code
            .as_ref()
            .is_some_and(|v| *v != current.program_code)
        || udf_rust.as_ref().is_some_and(|v| *v != current.udf_rust)
        || udf_toml.as_ref().is_some_and(|v| *v != current.udf_toml)
        || program_config
            .as_ref()
            .is_some_and(|v| *v != current.program_config);
    if program_changed {
        let stmt = txn
            .prepare_cached(
                "UPDATE pipeline
                 SET program_version = program_version + 1,
                     program_info = NULL,
                     program_status = $1,
                     program_status_since = now(),
                     program_error = $2,
                     program_binary_source_checksum = NULL,
                     program_binary_integrity_checksum = NULL
                 WHERE tenant_id = $3 AND name = $4",
            )
            .await?;
        let rows_affected = txn
            .execute(
                &stmt,
                &[
                    &ProgramStatus::Pending.to_string(),
                    &serialize_program_error(&ProgramError {
                        sql_compilation: None,
                        rust_compilation: None,
                        system_error: None,
                    })?,
                    &tenant_id.0,
                    &name.clone().unwrap_or(original_name.to_string()), // The name has at this point has potentially been changed from the original name to a new name
                ],
            )
            .await?;
        assert_eq!(rows_affected, 1); // The row must exist as it has been retrieved before
    }

    Ok(Version(current.version.0 + 1))
}

pub(crate) async fn delete_pipeline(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<PipelineId, DBError> {
    let current = get_pipeline(txn, tenant_id, name).await?;

    // Pipeline deletion is only possible if it is fully stopped
    if current.deployment_status != PipelineStatus::Stopped
        || current.deployment_desired_status != PipelineDesiredStatus::Stopped
    {
        return Err(DBError::DeleteRestrictedToFullyStopped);
    }
    if current.storage_status != StorageStatus::Cleared {
        return Err(DBError::DeleteRestrictedToClearedStorage);
    }

    let stmt = txn
        .prepare_cached("DELETE FROM pipeline WHERE tenant_id = $1 AND name = $2")
        .await?;
    let res = txn.execute(&stmt, &[&tenant_id.0, &name]).await?;
    if res > 0 {
        Ok(current.id)
    } else {
        Err(DBError::UnknownPipelineName {
            pipeline_name: name.to_string(),
        })
    }
}

/// Sets pipeline program status.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn set_program_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    program_version_guard: Version,
    new_program_status: &ProgramStatus,
    new_program_error_sql_compilation: &Option<SqlCompilationInfo>,
    new_program_error_rust_compilation: &Option<RustCompilationInfo>,
    new_program_error_system_error: &Option<String>,
    new_program_info: &Option<serde_json::Value>,
    new_program_binary_source_checksum: &Option<String>,
    new_program_binary_integrity_checksum: &Option<String>,
) -> Result<(), DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // Only if the program whose status is being transitioned is the same one can it be updated
    if current.program_version != program_version_guard {
        return Err(DBError::OutdatedProgramVersion {
            outdated_version: program_version_guard,
            latest_version: current.program_version,
        });
    }

    // Check that the transition from the current status to the new status is permitted
    validate_program_status_transition(current.program_status, *new_program_status)?;

    // Pipeline program status update is only possible if it is stopped.
    // The desired status does not necessarily have to be stopped,
    // in order to accommodate the compilation during early start.
    if current.deployment_status != PipelineStatus::Stopped {
        return Err(DBError::ProgramStatusUpdateRestrictedToStopped);
    }

    // Determine new values depending on where to transition to
    // Note that None becomes NULL as there is no coalescing in the query.
    let (
        final_program_error_sql_compilation,
        final_program_error_rust_compilation,
        final_program_error_system_error,
        final_program_info,
        final_program_binary_source_checksum,
        final_program_binary_integrity_checksum,
        increment_refresh_version,
    ) = match &new_program_status {
        ProgramStatus::Pending => {
            assert!(
                new_program_error_sql_compilation.is_none()
                    && new_program_error_rust_compilation.is_none()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
            );
            (None, None, None, None, None, None, true)
        }
        ProgramStatus::CompilingSql => {
            assert!(
                new_program_error_sql_compilation.is_none()
                    && new_program_error_rust_compilation.is_none()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
            );
            (None, None, None, None, None, None, false)
        }
        ProgramStatus::SqlCompiled => {
            assert!(
                new_program_error_sql_compilation.is_some()
                    && new_program_error_rust_compilation.is_none()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_some()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
            );
            (
                new_program_error_sql_compilation.clone(),
                None,
                None,
                new_program_info.clone(),
                None,
                None,
                true,
            )
        }
        ProgramStatus::CompilingRust => {
            assert!(
                new_program_error_sql_compilation.is_none()
                    && new_program_error_rust_compilation.is_none()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
            );
            (
                current.program_error.sql_compilation,
                None,
                None,
                current.program_info,
                None,
                None,
                false,
            )
        }
        ProgramStatus::Success => {
            assert!(
                new_program_error_sql_compilation.is_none()
                    && new_program_error_rust_compilation.is_some()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_none()
                    && new_program_binary_source_checksum.is_some()
                    && new_program_binary_integrity_checksum.is_some()
            );
            (
                current.program_error.sql_compilation,
                new_program_error_rust_compilation.clone(),
                None,
                current.program_info,
                new_program_binary_source_checksum.clone(),
                new_program_binary_integrity_checksum.clone(),
                true,
            )
        }
        ProgramStatus::SqlError => {
            assert!(
                new_program_error_sql_compilation.is_some()
                    && new_program_error_rust_compilation.is_none()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
            );
            (
                new_program_error_sql_compilation.clone(),
                None,
                None,
                None,
                None,
                None,
                true,
            )
        }
        ProgramStatus::RustError => {
            assert!(
                new_program_error_sql_compilation.is_none()
                    && new_program_error_rust_compilation.is_some()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
            );
            (
                current.program_error.sql_compilation,
                new_program_error_rust_compilation.clone(),
                None,
                current.program_info,
                None,
                None,
                true,
            )
        }
        ProgramStatus::SystemError => {
            assert!(
                new_program_error_sql_compilation.is_none()
                    && new_program_error_rust_compilation.is_none()
                    && new_program_error_system_error.is_some()
                    && new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
            );
            (
                current.program_error.sql_compilation,
                current.program_error.rust_compilation,
                new_program_error_system_error.clone(),
                current.program_info,
                None,
                None,
                true,
            )
        }
    };

    // Final program error
    let final_program_error = ProgramError {
        sql_compilation: final_program_error_sql_compilation,
        rust_compilation: final_program_error_rust_compilation,
        system_error: final_program_error_system_error,
    };

    // Validate that the new or existing program information is valid
    if let Some(program_info) = &final_program_info {
        let _ =
            validate_program_info(program_info).map_err(|error| DBError::InvalidProgramInfo {
                value: program_info.clone(),
                error,
            })?;
    }

    // Final refresh version
    let final_refresh_version = if increment_refresh_version {
        Version(current.refresh_version.0 + 1)
    } else {
        current.refresh_version
    };

    // Perform query
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
             SET program_status = $1,
                 program_status_since = now(),
                 program_error = $2,
                 program_info = $3,
                 program_binary_source_checksum = $4,
                 program_binary_integrity_checksum = $5,
                 refresh_version = $6
             WHERE tenant_id = $7 AND id = $8",
        )
        .await?;
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &new_program_status.to_string(),                 // $1: program_status
                &serialize_program_error(&final_program_error)?, // $2: program_error
                &final_program_info.as_ref().map(|v| v.to_string()), // $3: program_info
                &final_program_binary_source_checksum, // $4: program_binary_source_checksum
                &final_program_binary_integrity_checksum, // $5: program_binary_integrity_checksum
                &final_refresh_version.0,              // $6: refresh_version
                &tenant_id.0,                          // $7: tenant_id
                &pipeline_id.0,                        // $8: id
            ],
        )
        .await?;
    if rows_affected > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownPipeline { pipeline_id })
    }
}

/// Sets pipeline deployment desired status.
pub(crate) async fn set_deployment_desired_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: &str,
    new_desired_status: PipelineDesiredStatus,
) -> Result<PipelineId, DBError> {
    let current = get_pipeline(txn, tenant_id, pipeline_name).await?;

    // Check that the desired status can be set
    validate_deployment_desired_status_transition(
        current.deployment_status,
        current.deployment_desired_status,
        new_desired_status,
    )?;

    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
             SET deployment_desired_status = $1
             WHERE tenant_id = $2 AND id = $3",
        )
        .await?;
    let modified_rows = txn
        .execute(
            &stmt,
            &[&new_desired_status.to_string(), &tenant_id.0, &current.id.0],
        )
        .await?;
    if modified_rows > 0 {
        Ok(current.id)
    } else {
        Err(DBError::UnknownPipeline {
            pipeline_id: current.id,
        })
    }
}

/// Sets pipeline deployment status.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn set_deployment_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
    new_deployment_status: PipelineStatus,
    new_deployment_error: Option<ErrorResponse>,
    new_deployment_config: Option<serde_json::Value>,
    new_deployment_location: Option<String>,
    new_suspend_info: Option<serde_json::Value>,
) -> Result<(), DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // Use the version guard to check that the deployment is the intended one
    if current.version != version_guard {
        return Err(DBError::OutdatedPipelineVersion {
            outdated_version: version_guard,
            latest_version: current.version,
        });
    }

    // Check that the transition from the current status to the new status is permitted
    validate_deployment_status_transition(
        current.storage_status,
        current.deployment_status,
        new_deployment_status,
    )?;

    // Due to early start, the following do not require a successfully compiled program:
    // (1) Stopped -> Stopping
    // (2) Stopping -> Stopped
    // The above occurs because in early start, the program is not (yet) successfully
    // compiled and the runner is awaiting it to transition to Provisioning.
    //
    // All other deployment status transitions require a successfully compiled program.
    if !matches!(
        (current.deployment_status, new_deployment_status),
        (PipelineStatus::Stopped, PipelineStatus::Stopping)
            | (PipelineStatus::Stopping, PipelineStatus::Stopped)
    ) && current.program_status != ProgramStatus::Success
    {
        return Err(DBError::TransitionRequiresCompiledProgram {
            current: current.deployment_status,
            transition_to: new_deployment_status,
        });
    }

    // Determine the final values of the additional fields using the current default.
    // Note that None becomes NULL as there is no coalescing in the query.

    // Deployment error is set when becoming...
    // - Stopping: maybe a value
    // - Stopped: existing value
    // - Otherwise: NULL
    let final_deployment_error = if new_deployment_status == PipelineStatus::Stopping {
        new_deployment_error
    } else if new_deployment_status == PipelineStatus::Stopped {
        assert!(new_deployment_error.is_none());
        current.deployment_error
    } else if new_deployment_status == PipelineStatus::Provisioning {
        assert!(new_deployment_error.is_none());
        None
    } else {
        assert!(current.deployment_error.is_none());
        assert!(new_deployment_error.is_none());
        None
    };

    // Deployment configuration is set when becoming...
    // - Provisioning: a value
    // - Stopping: NULL
    // - Otherwise: current value
    let final_deployment_config = if new_deployment_status == PipelineStatus::Provisioning {
        assert!(new_deployment_config.is_some());
        new_deployment_config
    } else if new_deployment_status == PipelineStatus::Stopping {
        assert!(new_deployment_config.is_none());
        None
    } else {
        assert!(new_deployment_config.is_none());
        current.deployment_config
    };

    // Validate that the new or existing deployment configuration is valid
    if let Some(deployment_config) = &final_deployment_config {
        let _ = validate_deployment_config(deployment_config).map_err(|error| {
            DBError::InvalidDeploymentConfig {
                value: deployment_config.clone(),
                error,
            }
        })?;
    }

    // Deployment location is set when becoming...
    // - Initializing: a value
    // - Stopping: NULL
    // - Otherwise: current value
    let final_deployment_location = if new_deployment_status == PipelineStatus::Initializing {
        assert!(new_deployment_location.is_some());
        new_deployment_location
    } else if new_deployment_status == PipelineStatus::Stopping {
        assert!(new_deployment_location.is_none());
        None
    } else {
        assert!(new_deployment_location.is_none());
        current.deployment_location
    };

    // Suspend information is set when becoming...
    // - Stopping: maybe a value
    // - Stopped: current value
    // - Otherwise: NULL
    let final_suspend_info = if new_deployment_status == PipelineStatus::Stopping {
        new_suspend_info
    } else if new_deployment_status == PipelineStatus::Stopped {
        assert!(new_suspend_info.is_none());
        current.suspend_info
    } else {
        assert!(new_suspend_info.is_none());
        None
    };

    // Execute query
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET deployment_status = $1,
                     deployment_status_since = now(),
                     deployment_error = $2,
                     deployment_config = $3,
                     deployment_location = $4,
                     suspend_info = $5
                 WHERE tenant_id = $6 AND id = $7",
        )
        .await?;
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &new_deployment_status.to_string(),
                &match final_deployment_error {
                    None => None,
                    Some(v) => Some(serialize_error_response(&v)?),
                },
                &final_deployment_config.map(|v| v.to_string()),
                &final_deployment_location,
                &final_suspend_info.map(|v| v.to_string()),
                &tenant_id.0,
                &pipeline_id.0,
            ],
        )
        .await?;
    if rows_affected > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownPipeline { pipeline_id })
    }
}

/// Sets pipeline storage status.
pub(crate) async fn set_storage_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    new_storage_status: StorageStatus,
) -> Result<(), DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // Check that the transition is permitted
    validate_storage_status_transition(
        current.deployment_status,
        current.storage_status,
        new_storage_status,
    )?;

    // Execute query
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET storage_status = $1
                 WHERE tenant_id = $2 AND id = $3",
        )
        .await?;
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &new_storage_status.to_string(),
                &tenant_id.0,
                &pipeline_id.0,
            ],
        )
        .await?;
    if rows_affected > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownPipeline { pipeline_id })
    }
}

/// Lists pipeline ids across all tenants.
pub(crate) async fn list_pipeline_ids_across_all_tenants(
    txn: &Transaction<'_>,
) -> Result<Vec<(TenantId, PipelineId)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.tenant_id, p.id
             FROM pipeline AS p
             ORDER BY p.id ASC
            ",
        )
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
    let mut pipelines = Vec::with_capacity(rows.len());
    for row in rows {
        let tenant_id = TenantId(row.get(0));
        let pipeline_id = PipelineId(row.get(1));
        pipelines.push((tenant_id, pipeline_id));
    }
    Ok(pipelines)
}

/// Retrieves a list of all pipelines across all tenants.
/// The descriptors only have the fields relevant to monitoring.
pub(crate) async fn list_pipelines_across_all_tenants_for_monitoring(
    txn: &Transaction<'_>,
) -> Result<Vec<(TenantId, ExtendedPipelineDescrMonitoring)>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_MONITORING_COLUMNS}
             FROM pipeline AS p
             ORDER BY p.id ASC
            "
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push((
            TenantId(row.get(1)),
            row_to_extended_pipeline_descriptor_monitoring(&row)?,
        ));
    }
    Ok(result)
}

/// Retrieves the pipeline which is stopped, whose program status has been Pending
/// for the longest, and is of the current platform version. Returns `None` if none is found.
pub(crate) async fn get_next_sql_compilation(
    txn: &Transaction<'_>,
    platform_version: &str,
) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_COLUMNS}
             FROM pipeline AS p
             WHERE p.deployment_status = 'stopped'
                   AND p.program_status = 'pending'
                   AND p.platform_version = $1
             ORDER BY p.program_status_since ASC, p.id ASC
             LIMIT 1
            "
        ))
        .await?;
    let row = txn
        .query_opt(
            &stmt,
            &[
                &platform_version.to_string(), // $1: platform_version
            ],
        )
        .await?;
    match row {
        None => Ok(None),
        Some(row) => Ok(Some((
            TenantId(row.get(1)),
            row_to_extended_pipeline_descriptor(&row)?,
        ))),
    }
}

/// Retrieves the pipeline which is stopped, whose program status has been SqlCompiled
/// for the longest, and is of the current platform version. Returns `None` if none is found.
pub(crate) async fn get_next_rust_compilation(
    txn: &Transaction<'_>,
    platform_version: &str,
) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_COLUMNS}
             FROM pipeline AS p
             WHERE p.deployment_status = 'stopped'
                   AND p.program_status = 'sql_compiled'
                   AND p.platform_version = $1
             ORDER BY p.program_status_since ASC, p.id ASC
             LIMIT 1
            "
        ))
        .await?;
    let row = txn
        .query_opt(
            &stmt,
            &[
                &platform_version.to_string(), // $1: platform_version
            ],
        )
        .await?;
    match row {
        None => Ok(None),
        Some(row) => Ok(Some((
            TenantId(row.get(1)),
            row_to_extended_pipeline_descriptor(&row)?,
        ))),
    }
}

/// Retrieves the list of successfully compiled pipeline programs (pipeline identifier, program version,
/// program binary source checksum, program binary integrity checksum) across all tenants.
pub(crate) async fn list_pipeline_programs_across_all_tenants(
    txn: &Transaction<'_>,
) -> Result<Vec<(PipelineId, Version, String, String)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.program_version, p.program_binary_source_checksum, p.program_binary_integrity_checksum
             FROM pipeline AS p
             WHERE p.program_status = 'success'
             ORDER BY p.id ASC
            ",
        )
        .await?;
    let rows = txn.query(&stmt, &[]).await?;
    Ok(rows
        .iter()
        .map(|row| {
            (
                PipelineId(row.get(0)),
                Version(row.get(1)),
                row.get::<_, String>(2),
                row.get::<_, String>(3),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::db::error::DBError;
    use crate::db::operations::pipeline::{
        deserialize_error_response, deserialize_json_value, deserialize_program_error,
        deserialize_program_error_with_default, serialize_error_response, serialize_program_error,
    };
    use crate::db::types::program::{
        ConnectorGenerationError, ProgramError, RustCompilationInfo, SqlCompilationInfo,
        SqlCompilerMessage,
    };
    use feldera_types::error::ErrorResponse;
    use feldera_types::program_schema::SourcePosition;
    use serde_json::json;
    use std::borrow::Cow;

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
