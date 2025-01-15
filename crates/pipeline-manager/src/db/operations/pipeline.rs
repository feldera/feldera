use crate::db::error::DBError;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_unique_violation,
};
use crate::db::types::pipeline::{
    validate_deployment_desired_status_transition, validate_deployment_status_transition,
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDescr, PipelineDesiredStatus,
    PipelineId, PipelineStatus,
};
use crate::db::types::program::{validate_program_status_transition, ProgramStatus};
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::{
    validate_deployment_config, validate_name, validate_program_config, validate_program_info,
    validate_runtime_config,
};
use crate::db::types::version::Version;
use deadpool_postgres::Transaction;
use feldera_types::error::ErrorResponse;
use tokio_postgres::Row;
use uuid::Uuid;

/// Parses string as a JSON value.
fn deserialize_json_value(s: &str) -> Result<serde_json::Value, DBError> {
    serde_json::from_str::<serde_json::Value>(s).map_err(|e| DBError::InvalidJsonData {
        data: s.to_string(),
        error: format!("unable to deserialize data string as JSON due to: {e}"),
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
     p.program_binary_source_checksum, p.program_binary_integrity_checksum, p.program_binary_url,
     p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
     p.deployment_error, p.deployment_config, p.deployment_location";

/// Converts a pipeline table row to its extended descriptor with all fields.
///
/// Backward compatibility:
/// - Fields `runtime_config`, `program_config`, `program_info`, and `deployment_config`
///   are deserialized as JSON values -- they are not deserialized as their actual types.
///   Backward incompatible changes in those actual types will not prevent retrieval of
///   pipelines as long as they are serialized as valid JSON.
/// - All other fields are deserialized as their actual types.
///   Backwards incompatible changes therein will prevent retrieval of pipelines
///   because an error will be returned instead.
fn row_to_extended_pipeline_descriptor(row: &Row) -> Result<ExtendedPipelineDescr, DBError> {
    assert_eq!(row.len(), 26);
    let program_info = match row.get::<_, Option<String>>(16) {
        None => None,
        Some(s) => Some(deserialize_json_value(&s)?),
    };
    let deployment_error = match row.get::<_, Option<String>>(23) {
        None => None,
        Some(s) => Some(deserialize_error_response(&s)?),
    };
    let deployment_config = match row.get::<_, Option<String>>(24) {
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
        runtime_config: deserialize_json_value(row.get(7))?,
        program_code: row.get(8),
        udf_rust: row.get(9),
        udf_toml: row.get(10),
        program_config: deserialize_json_value(row.get(11))?,
        program_version: Version(row.get(12)),
        program_status: ProgramStatus::from_columns(row.get(13), row.get(15))?,
        program_status_since: row.get(14),
        program_info,
        program_binary_source_checksum: row.get(17),
        program_binary_integrity_checksum: row.get(18),
        program_binary_url: row.get(19),
        deployment_status: row.get::<_, String>(20).try_into()?,
        deployment_status_since: row.get(21),
        deployment_desired_status: row.get::<_, String>(22).try_into()?,
        deployment_error,
        deployment_config,
        deployment_location: row.get(25),
    })
}

/// Pipeline columns relevant to monitoring.
const RETRIEVE_PIPELINE_MONITORING_COLUMNS: &str =
    "p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.platform_version,
     p.program_version, p.program_status, p.program_status_since, p.program_error,
     p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
     p.deployment_error, p.deployment_location";

/// Converts a pipeline table row to its extended descriptor with only fields relevant to monitoring.
fn row_to_extended_pipeline_descriptor_monitoring(
    row: &Row,
) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
    assert_eq!(row.len(), 16);
    let deployment_error = match row.get::<_, Option<String>>(14) {
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
        program_status: ProgramStatus::from_columns(row.get(8), row.get(10))?,
        program_status_since: row.get(9),
        deployment_status: row.get::<_, String>(11).try_into()?,
        deployment_status_since: row.get(12),
        deployment_desired_status: row.get::<_, String>(13).try_into()?,
        deployment_error,
        deployment_location: row.get(15),
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
    let _ = validate_runtime_config(&pipeline.runtime_config, false).map_err(|error| {
        DBError::InvalidRuntimeConfig {
            value: pipeline.runtime_config.clone(),
            error,
        }
    })?;
    let _ = validate_program_config(&pipeline.program_config, false).map_err(|error| {
        DBError::InvalidProgramConfig {
            value: pipeline.program_config.clone(),
            error,
        }
    })?;
    let stmt = txn

        .prepare_cached(
            "INSERT INTO pipeline (id, tenant_id, name, description, created_at, version, platform_version, runtime_config,
                                   program_code, udf_rust, udf_toml, program_config, program_version, program_status,
                                   program_status_since, program_error, program_info,
                                   program_binary_source_checksum, program_binary_integrity_checksum, program_binary_url,
                                   deployment_status, deployment_status_since, deployment_desired_status,
                                   deployment_error, deployment_config, deployment_location, uses_json)
            VALUES ($1, $2, $3, $4, now(), $5, $6, $7,
                    $8, $9, $10, $11, $12, $13,
                    now(), NULL, NULL,
                    NULL, NULL, NULL,
                    $14, now(), $15,
                    NULL, NULL, NULL, TRUE)",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_id,                                // $1: id
            &tenant_id.0,                           // $2: tenant_id
            &pipeline.name,                         // $3: name
            &pipeline.description,                  // $4: description
            &Version(1).0,                          // $5: version
            &platform_version.to_string(),          // $6: platform_version
            &pipeline.runtime_config.to_string(),   // $7: runtime_config
            &pipeline.program_code,                 // $8: program_code
            &pipeline.udf_rust,                     // $9: udf_rust
            &pipeline.udf_toml,                     // $10: udf_toml
            &pipeline.program_config.to_string(),   // $11: program_config
            &Version(1).0,                          // $12: program_version
            &ProgramStatus::Pending.to_columns().0, // $13: program_status
            &PipelineStatus::Shutdown.to_string(),  // $14: deployment_status
            &PipelineStatus::Shutdown.to_string(),  // $15: deployment_desired_status
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
    if let Some(runtime_config) = runtime_config {
        let _ = validate_runtime_config(runtime_config, false).map_err(|error| {
            DBError::InvalidRuntimeConfig {
                value: runtime_config.clone(),
                error,
            }
        })?;
    }
    if let Some(program_config) = program_config {
        let _ = validate_program_config(program_config, false).map_err(|error| {
            DBError::InvalidProgramConfig {
                value: program_config.clone(),
                error,
            }
        })?;
    }

    // Fetch current pipeline to decide how to update.
    // This will also return an error if the pipeline does not exist.
    let current = get_pipeline(txn, tenant_id, original_name).await?;

    // Pipeline update is only possible if it is shutdown.
    // The user cannot edit a pipeline with a desired status which is not shutdown,
    // but the compiler can still in order to bump the `platform_version`.
    if current.deployment_status != PipelineStatus::Shutdown
        || (!is_compiler_update
            && current.deployment_desired_status != PipelineDesiredStatus::Shutdown)
    {
        return Err(DBError::CannotUpdateNonShutdownPipeline);
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
                     version = version + 1
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
                     program_error = NULL,
                     program_binary_url = NULL
                 WHERE tenant_id = $2 AND name = $3",
            )
            .await?;
        let rows_affected = txn
            .execute(
                &stmt,
                &[
                    &ProgramStatus::Pending.to_columns().0,
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

    // Pipeline deletion is only possible if it is fully shutdown
    if current.deployment_status != PipelineStatus::Shutdown
        || current.deployment_desired_status != PipelineDesiredStatus::Shutdown
    {
        return Err(DBError::CannotDeleteNonShutdownPipeline);
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
    new_program_info: &Option<serde_json::Value>,
    new_program_binary_source_checksum: &Option<String>,
    new_program_binary_integrity_checksum: &Option<String>,
    new_program_binary_url: &Option<String>,
) -> Result<(), DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // Pipeline program status update is only possible if it is shutdown.
    // The desired status does not necessarily have to be shutdown,
    // in order to accommodate the compilation during early start.
    if current.deployment_status != PipelineStatus::Shutdown {
        return Err(DBError::CannotUpdateProgramStatusOfNonShutdownPipeline);
    }

    // Only if the program whose status is being transitioned is the same one can it be updated
    if current.program_version != program_version_guard {
        return Err(DBError::OutdatedProgramVersion {
            outdated_version: program_version_guard,
            latest_version: current.program_version,
        });
    }

    // Check that the transition from the current status to the new status is permitted
    validate_program_status_transition(&current.program_status, new_program_status)?;

    // Determine new values depending on where to transition to
    // Note that None becomes NULL as there is no coalescing in the query.
    let (
        final_program_info,
        final_program_binary_source_checksum,
        final_program_binary_integrity_checksum,
        final_program_binary_url,
    ) = match &new_program_status {
        ProgramStatus::Pending => {
            assert!(
                new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
                    && new_program_binary_url.is_none()
            );
            (None, None, None, None)
        }
        ProgramStatus::CompilingSql => {
            assert!(
                new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
                    && new_program_binary_url.is_none()
            );
            (None, None, None, None)
        }
        ProgramStatus::SqlCompiled => {
            assert!(
                new_program_info.is_some()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
                    && new_program_binary_url.is_none()
            );
            (new_program_info.clone(), None, None, None)
        }
        ProgramStatus::CompilingRust => {
            assert!(
                new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
                    && new_program_binary_url.is_none()
            );
            (current.program_info, None, None, None)
        }
        ProgramStatus::Success => {
            assert!(
                new_program_info.is_none()
                    && new_program_binary_source_checksum.is_some()
                    && new_program_binary_integrity_checksum.is_some()
                    && new_program_binary_url.is_some()
            );
            (
                current.program_info,
                new_program_binary_source_checksum.clone(),
                new_program_binary_integrity_checksum.clone(),
                new_program_binary_url.clone(),
            )
        }
        ProgramStatus::SqlError(_) => (None, None, None, None),
        ProgramStatus::RustError(_) => (current.program_info, None, None, None),
        ProgramStatus::SystemError(_) => (current.program_info, None, None, None),
    };

    // Validate that the new or existing program information is valid
    if let Some(program_info) = &final_program_info {
        let _ =
            validate_program_info(program_info).map_err(|error| DBError::InvalidProgramInfo {
                value: program_info.clone(),
                error,
            })?;
    }

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
                 program_binary_url = $6
             WHERE tenant_id = $7 AND id = $8",
        )
        .await?;
    let new_program_status_columns = new_program_status.to_columns();
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &new_program_status_columns.0,
                &new_program_status_columns.1,
                &final_program_info.as_ref().map(|v| v.to_string()),
                &final_program_binary_source_checksum,
                &final_program_binary_integrity_checksum,
                &final_program_binary_url,
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

/// Sets pipeline deployment desired status.
pub(crate) async fn set_deployment_desired_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: &str,
    new_desired_status: PipelineDesiredStatus,
) -> Result<(), DBError> {
    let current = get_pipeline(txn, tenant_id, pipeline_name).await?;

    // Check that the desired status can be set
    validate_deployment_desired_status_transition(
        &current.deployment_status,
        &current.deployment_desired_status,
        &new_desired_status,
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
        Ok(())
    } else {
        Err(DBError::UnknownPipeline {
            pipeline_id: current.id,
        })
    }
}

/// Sets pipeline deployment status.
pub(crate) async fn set_deployment_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    new_deployment_status: PipelineStatus,
    new_deployment_error: Option<ErrorResponse>,
    new_deployment_config: Option<serde_json::Value>,
    new_deployment_location: Option<String>,
) -> Result<(), DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // Due to early start, the following do not require a successfully compiled program:
    // (1) Shutdown -> Failed
    // (2) Failed -> ShuttingDown
    // (3) ShuttingDown -> Shutdown
    // The above occurs because in early start, the program is not (yet) successfully
    // compiled and the runner is awaiting it to transition to Provisioning.
    //
    // All other deployment status transitions require a successfully compiled program.
    if !matches!(
        (current.deployment_status, new_deployment_status),
        (PipelineStatus::Shutdown, PipelineStatus::Failed)
            | (PipelineStatus::Failed, PipelineStatus::ShuttingDown)
            | (PipelineStatus::ShuttingDown, PipelineStatus::Shutdown)
    ) && current.program_status != ProgramStatus::Success
    {
        return Err(DBError::TransitionRequiresCompiledProgram {
            current: current.deployment_status,
            transition_to: new_deployment_status,
        });
    }

    // Check that the transition from the current status to the new status is permitted
    validate_deployment_status_transition(&current.deployment_status, &new_deployment_status)?;

    // Determine the final values of the additional fields using the current default.
    // Note that None becomes NULL as there is no coalescing in the query.

    // Deployment error is set when becoming...
    // - Failed: a value
    // - ShuttingDown: NULL
    // - Otherwise: NULL
    let final_deployment_error = if new_deployment_status == PipelineStatus::Failed {
        assert!(new_deployment_error.is_some());
        new_deployment_error
    } else if new_deployment_status == PipelineStatus::ShuttingDown {
        assert!(new_deployment_error.is_none());
        None
    } else {
        assert!(current.deployment_error.is_none());
        assert!(new_deployment_error.is_none());
        None
    };

    // Deployment configuration is set when becoming...
    // - Provisioning: a value
    // - ShuttingDown: NULL
    // - Otherwise: current value
    let final_deployment_config = if new_deployment_status == PipelineStatus::Provisioning {
        new_deployment_config
    } else if new_deployment_status == PipelineStatus::ShuttingDown {
        None
    } else {
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
    // - ShuttingDown: NULL
    // - Otherwise: current value
    let final_deployment_location = if new_deployment_status == PipelineStatus::Initializing {
        assert!(new_deployment_location.is_some());
        new_deployment_location
    } else if new_deployment_status == PipelineStatus::ShuttingDown {
        assert!(new_deployment_location.is_none());
        None
    } else {
        assert!(new_deployment_location.is_none());
        current.deployment_location
    };

    // Execute query
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET deployment_status = $1,
                     deployment_status_since = now(),
                     deployment_error = $2,
                     deployment_config = $3,
                     deployment_location = $4
                 WHERE tenant_id = $5 AND id = $6",
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

/// Retrieves the pipeline which is shutdown, whose program status has been Pending
/// for the longest, and is of the current platform version. Returns `None` if none is found.
pub(crate) async fn get_next_sql_compilation(
    txn: &Transaction<'_>,
    platform_version: &str,
) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_COLUMNS}
             FROM pipeline AS p
             WHERE p.deployment_status = 'shutdown'
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

/// Retrieves the pipeline which is shutdown, whose program status has been SqlCompiled
/// for the longest, and is of the current platform version. Returns `None` if none is found.
pub(crate) async fn get_next_rust_compilation(
    txn: &Transaction<'_>,
    platform_version: &str,
) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_PIPELINE_COLUMNS}
             FROM pipeline AS p
             WHERE p.deployment_status = 'shutdown'
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
        deserialize_error_response, deserialize_json_value, serialize_error_response,
    };
    use feldera_types::error::ErrorResponse;
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
}
