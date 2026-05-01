use crate::api::support_data_collector::SupportBundleData;
use crate::db::error::DBError;
use crate::db::operations::pipeline_monitor::new_pipeline_monitor_event;
use crate::db::operations::pipeline_parsing::{
    parse_pipeline_row_all, parse_pipeline_row_event_info, parse_pipeline_row_monitoring,
    serialize_error_response, serialize_program_error, PIPELINE_COLUMNS_ALL,
    PIPELINE_COLUMNS_EVENT_INFO, PIPELINE_COLUMNS_MONITORING,
};
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_unique_violation,
};
use crate::db::types::pipeline::{
    bootstrap_policy_to_string, runtime_desired_status_to_string, runtime_status_to_string,
    ExtendedPipelineDescr, ExtendedPipelineDescrEventInfo, ExtendedPipelineDescrMonitoring,
    PipelineDescr, PipelineId,
};
use crate::db::types::program::{
    validate_program_status_transition, ProgramError, ProgramStatus, RustCompilationInfo,
    SqlCompilationInfo,
};
use crate::db::types::resources_status::{
    validate_resources_desired_status_transition, validate_resources_status_transition,
    ResourcesDesiredStatus, ResourcesStatus,
};
use crate::db::types::storage::{validate_storage_status_transition, StorageStatus};
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::{
    validate_deployment_config, validate_name, validate_program_config, validate_program_info,
    validate_runtime_config, validate_storage_status_details,
};
use crate::db::types::version::Version;
use deadpool_postgres::Transaction;
use feldera_types::error::ErrorResponse;
use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus, RuntimeStatus};
use rmp_serde::{from_slice, to_vec};
use serde_json::json;
use tokio_postgres::Row;
use tracing::warn;
use uuid::Uuid;

pub(crate) async fn list_pipelines(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<ExtendedPipelineDescr>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_COLUMNS_ALL}
             FROM pipeline AS p
             WHERE p.tenant_id = $1
             ORDER BY p.id ASC"
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[&tenant_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(parse_pipeline_row_all(&row)?);
    }
    Ok(result)
}

pub(crate) async fn list_pipelines_for_monitoring(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<ExtendedPipelineDescrMonitoring>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_COLUMNS_MONITORING}
             FROM pipeline AS p
             WHERE p.tenant_id = $1
             ORDER BY p.id ASC"
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[&tenant_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(parse_pipeline_row_monitoring(&row)?);
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
            "SELECT {PIPELINE_COLUMNS_ALL}
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
    parse_pipeline_row_all(&row)
}

pub(crate) async fn get_pipeline_for_monitoring(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_COLUMNS_MONITORING}
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
    parse_pipeline_row_monitoring(&row)
}

async fn internal_get_pipeline_by_id(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    fields: &'static str,
) -> Result<Row, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {fields}
             FROM pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = $2
            "
        ))
        .await?;
    txn.query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0])
        .await?
        .ok_or(DBError::UnknownPipeline { pipeline_id })
}

pub async fn get_pipeline_by_id(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<ExtendedPipelineDescr, DBError> {
    let row =
        internal_get_pipeline_by_id(txn, tenant_id, pipeline_id, PIPELINE_COLUMNS_ALL).await?;
    parse_pipeline_row_all(&row)
}

pub async fn get_pipeline_by_id_for_monitoring(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
    let row = internal_get_pipeline_by_id(txn, tenant_id, pipeline_id, PIPELINE_COLUMNS_MONITORING)
        .await?;
    parse_pipeline_row_monitoring(&row)
}

pub async fn get_pipeline_by_id_for_event_info(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<ExtendedPipelineDescrEventInfo, DBError> {
    let row = internal_get_pipeline_by_id(txn, tenant_id, pipeline_id, PIPELINE_COLUMNS_EVENT_INFO)
        .await?;
    parse_pipeline_row_event_info(&row)
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
                                   deployment_error, deployment_config, deployment_location, uses_json, refresh_version, storage_status, storage_status_details,
                                   deployment_id, deployment_initial,
                                   deployment_resources_status, deployment_resources_status_since,
                                   deployment_resources_desired_status, deployment_resources_desired_status_since,
                                   deployment_runtime_status, deployment_runtime_status_since,
                                   deployment_runtime_desired_status, deployment_runtime_desired_status_since
                                   )
            VALUES ($1, $2, $3, $4, now(), $5, $6, $7,
                    $8, $9, $10, $11, $12, $13,
                    now(), $14, NULL,
                    NULL, NULL,
                    NULL, NULL, NULL, TRUE, $15, $16, NULL,
                    NULL, NULL,
                    $17, now(),
                    $18, now(),
                    NULL, NULL,
                    NULL, NULL)",
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
            &Version(1).0,                                // $15: refresh_version
            &StorageStatus::Cleared.to_string(),          // $16: storage_status
            &ResourcesStatus::Stopped.to_string(),        // $17: deployment_resources_status
            &ResourcesDesiredStatus::Stopped.to_string(), // $18: deployment_resources_desired_status
        ],
    )
    .await
    .map_err(maybe_unique_violation)
    .map_err(|e| maybe_tenant_id_foreign_key_constraint_err(e, tenant_id))?;
    new_pipeline_monitor_event(txn, tenant_id, PipelineId(new_id), Uuid::now_v7()).await?;
    Ok((PipelineId(new_id), Version(1)))
}

/// Modify pipeline.
///
/// # Arguments
///
/// * `is_compiler_update` - if true, indicates that the update is being performed by the compiler server.
/// * `platform_version` - the currently running Feldera platform version. If `bump_platform_version` is true,
///   pipeline's platform_version will be set to this value.
/// * `bump_platform_version` - if true, the platform_version of the pipeline will be updated to the
///   provided `platform_version`. In addition, the platform_version will be updated unconditionally
///   if the program code or program settings are getting updated by this request.
/// * Other arguments correspond to fields that can be updated. If an argument is `None`, the corresponding
///   field is not updated.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_pipeline(
    txn: &Transaction<'_>,
    is_compiler_update: bool,
    tenant_id: TenantId,
    original_name: &str,
    name: &Option<String>,
    description: &Option<String>,
    platform_version: &str,
    mut bump_platform_version: bool,
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
    // - Current status is `Stopped` AND desired status is `Provisioned` AND it is the
    //   compiler doing the update to bump platform version (the early start mechanism)
    if !matches!(
        (
            is_compiler_update,
            current.deployment_resources_status,
            current.deployment_resources_desired_status
        ),
        (_, ResourcesStatus::Stopped, ResourcesDesiredStatus::Stopped)
            | (
                true,
                ResourcesStatus::Stopped,
                ResourcesDesiredStatus::Provisioned
            ),
    ) {
        return Err(DBError::UpdateRestrictedToStopped);
    }

    // If it is a compiler update, then the only thing it must change is the platform_version
    assert!(
        !is_compiler_update
            || (bump_platform_version
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
        && (!bump_platform_version || platform_version == current.platform_version.as_str())
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
            let one = json!(1);
            if runtime_config.get("hosts").unwrap_or(&one)
                != current.runtime_config.get("hosts").unwrap_or(&one)
            {
                not_allowed.push("`runtime_config.hosts`");
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

        if !not_allowed.is_empty() {
            return Err(DBError::EditRestrictedToClearedStorage {
                not_allowed: not_allowed.iter_mut().map(|s| s.to_string()).collect(),
            });
        }
    }

    // If the program changed, make the following changes:
    // - increment program version
    // - update platform_version to the current version,
    // - unset the schema
    // - reset the status back to pending.
    let program_changed = (bump_platform_version
        && platform_version != current.platform_version.as_str())
        || program_code
            .as_ref()
            .is_some_and(|v| *v != current.program_code)
        || udf_rust.as_ref().is_some_and(|v| *v != current.udf_rust)
        || udf_toml.as_ref().is_some_and(|v| *v != current.udf_toml)
        || program_config
            .as_ref()
            .is_some_and(|v| *v != current.program_config);

    if program_changed {
        bump_platform_version = true;
    }

    // Otherwise, one of the fields is non-null and different, and as such it should be updated
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET name = COALESCE($1, name),
                     description = COALESCE($2, description),
                     platform_version = COALESCE($3, platform_version),
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
                &if bump_platform_version {
                    Some(platform_version.to_string())
                } else {
                    None
                },
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

    new_pipeline_monitor_event(txn, tenant_id, current.id, Uuid::now_v7()).await?;
    Ok(Version(current.version.0 + 1))
}

pub(crate) async fn delete_pipeline(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<PipelineId, DBError> {
    let current = get_pipeline(txn, tenant_id, name).await?;

    // Pipeline deletion is only possible if it is fully stopped
    if current.deployment_resources_status != ResourcesStatus::Stopped
        || current.deployment_resources_desired_status != ResourcesDesiredStatus::Stopped
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
    new_program_info_integrity_checksum: &Option<String>,
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
    if current.deployment_resources_status != ResourcesStatus::Stopped {
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
        final_program_info_integrity_checksum,
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
                    && new_program_info_integrity_checksum.is_none()
            );
            (None, None, None, None, None, None, None, true)
        }
        ProgramStatus::CompilingSql => {
            assert!(
                new_program_error_sql_compilation.is_none()
                    && new_program_error_rust_compilation.is_none()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_none()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
                    && new_program_info_integrity_checksum.is_none()
            );
            (None, None, None, None, None, None, None, false)
        }
        ProgramStatus::SqlCompiled => {
            assert!(
                new_program_error_sql_compilation.is_some()
                    && new_program_error_rust_compilation.is_none()
                    && new_program_error_system_error.is_none()
                    && new_program_info.is_some()
                    && new_program_binary_source_checksum.is_none()
                    && new_program_binary_integrity_checksum.is_none()
                    && new_program_info_integrity_checksum.is_none()
            );
            (
                new_program_error_sql_compilation.clone(),
                None,
                None,
                new_program_info.clone(),
                None,
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
                    && new_program_info_integrity_checksum.is_none()
            );
            (
                current.program_error.sql_compilation,
                None,
                None,
                current.program_info,
                None,
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
                    && new_program_info_integrity_checksum.is_some()
            );
            (
                current.program_error.sql_compilation,
                new_program_error_rust_compilation.clone(),
                None,
                current.program_info,
                new_program_binary_source_checksum.clone(),
                new_program_binary_integrity_checksum.clone(),
                new_program_info_integrity_checksum.clone(),
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
                    && new_program_info_integrity_checksum.is_none()
            );
            (
                new_program_error_sql_compilation.clone(),
                None,
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
                    && new_program_info_integrity_checksum.is_none()
            );
            (
                current.program_error.sql_compilation,
                new_program_error_rust_compilation.clone(),
                None,
                current.program_info,
                None,
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
                    && new_program_info_integrity_checksum.is_none()
            );
            (
                current.program_error.sql_compilation,
                current.program_error.rust_compilation,
                new_program_error_system_error.clone(),
                current.program_info,
                None,
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
                 program_info_integrity_checksum = $6,
                 refresh_version = $7
             WHERE tenant_id = $8 AND id = $9",
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
                &final_program_info_integrity_checksum, // $6: program_info_integrity_checksum
                &final_refresh_version.0,              // $7: refresh_version
                &tenant_id.0,                          // $8: tenant_id
                &pipeline_id.0,                        // $9: id
            ],
        )
        .await?;
    if rows_affected > 0 {
        new_pipeline_monitor_event(txn, tenant_id, pipeline_id, Uuid::now_v7()).await?;
        Ok(())
    } else {
        Err(DBError::UnknownPipeline { pipeline_id })
    }
}

/// Checks a certain precondition for the input parameters of the database operation.
fn check_precondition(condition: bool, info: &str) -> Result<(), DBError> {
    if condition {
        Ok(())
    } else {
        Err(DBError::PreconditionViolation(info.to_string()))
    }
}

/// Dismisses the deployment error.
pub(crate) async fn dismiss_deployment_error(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: &str,
) -> Result<(), DBError> {
    let current = get_pipeline(txn, tenant_id, pipeline_name).await?;

    // If an error has to be cleared, then it is only possible if it is fully stopped
    if (current.deployment_resources_status != ResourcesStatus::Stopped
        || current.deployment_resources_desired_status != ResourcesDesiredStatus::Stopped)
        && current.deployment_error.is_some()
    {
        return Err(DBError::DismissErrorRestrictedToFullyStopped);
    }

    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
             SET deployment_error = NULL
             WHERE tenant_id = $1 AND id = $2",
        )
        .await?;
    let modified_rows = txn.execute(&stmt, &[&tenant_id.0, &current.id.0]).await?;
    if modified_rows > 0 {
        new_pipeline_monitor_event(txn, tenant_id, current.id, Uuid::now_v7()).await?;
        Ok(())
    } else {
        Err(DBError::UnknownPipeline {
            pipeline_id: current.id,
        })
    }
}

/// Sets pipeline desired resources status.
pub(crate) async fn set_deployment_resources_desired_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: &str,
    new_desired_status: ResourcesDesiredStatus,
    initial_runtime_desired_status: Option<RuntimeDesiredStatus>,
    bootstrap_policy: Option<BootstrapPolicy>,
    dismiss_error: bool,
) -> Result<PipelineId, DBError> {
    let current = get_pipeline(txn, tenant_id, pipeline_name).await?;

    // Deployment error will be automatically dismissed if this is wanted
    let final_deployment_error = if dismiss_error {
        None
    } else {
        current.deployment_error
    };

    // Check that the desired status can be set
    validate_resources_desired_status_transition(
        current.storage_status,
        current.deployment_resources_status,
        current.deployment_resources_desired_status,
        final_deployment_error.clone(),
        new_desired_status,
    )?;

    // DESIRED PROVISIONED INFORMATION
    // Fields that give information about provisioned when setting it as desired.
    // - Stopped: current value unless currently still stopped, in which case it becomes NULL.
    //            This happens when the user calls /start followed immediately by /stop before the
    //            runner notices. Otherwise, another (not desired) status transition will set it
    //            to NULL.
    // - Provisioned: new value
    let (final_deployment_initial, final_bootstrap_policy) = match new_desired_status {
        ResourcesDesiredStatus::Stopped => {
            check_precondition(
                initial_runtime_desired_status.is_none(),
                "initial_runtime_desired_status should be None when becoming desired Stopped",
            )?;
            check_precondition(
                bootstrap_policy.is_none(),
                "bootstrap_policy should be None when becoming desired Stopped",
            )?;
            if current.deployment_resources_status == ResourcesStatus::Stopped {
                (None, None)
            } else {
                (current.deployment_initial, current.bootstrap_policy)
            }
        }
        ResourcesDesiredStatus::Provisioned => {
            check_precondition(
                initial_runtime_desired_status.is_some(),
                "initial_runtime_desired_status should be Some when becoming desired Provisioned",
            )?;
            check_precondition(
                bootstrap_policy.is_some(),
                "bootstrap_policy should be Some when becoming desired Provisioned",
            )?;
            (initial_runtime_desired_status, bootstrap_policy)
        }
    };

    // If the current initial desired runtime status is already set, it cannot be changed
    if let Some(current_initial) = current.deployment_initial {
        if let Some(new_initial) = final_deployment_initial {
            if current_initial != new_initial {
                return Err(DBError::InitialImmutableUnlessStopped);
            }
        }
    }

    // If the current bootstrap policy is already set, it cannot be changed
    if let Some(current_bootstrap_policy) = current.bootstrap_policy {
        if let Some(new_bootstrap_policy) = final_bootstrap_policy {
            if current_bootstrap_policy != new_bootstrap_policy {
                return Err(DBError::BootstrapPolicyImmutableUnlessStopped);
            }
        }
    }

    // Desired status cannot be set to standby if no file backend is configured
    let is_file_backend = current
        .runtime_config
        .get("storage")
        .and_then(|v| v.get("backend"))
        .and_then(|v| v.get("name"))
        .map(|v| v.as_str() == Some("file"))
        .unwrap_or(false);
    let is_sync_configured = current
        .runtime_config
        .get("storage")
        .and_then(|v| v.get("backend"))
        .and_then(|v| v.get("config"))
        .and_then(|v| v.get("sync"))
        .map(|v| !v.is_null())
        .unwrap_or(false);
    if final_deployment_initial == Some(RuntimeDesiredStatus::Standby)
        && (!is_file_backend || !is_sync_configured)
    {
        return Err(DBError::InitialStandbyNotAllowed);
    }

    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
             SET deployment_resources_desired_status = $1,
                 deployment_resources_desired_status_since = CASE WHEN deployment_resources_desired_status = $1::VARCHAR THEN deployment_resources_desired_status_since ELSE NOW() END,
                 deployment_initial = $2,
                 bootstrap_policy = $3,
                 deployment_error = $4
             WHERE tenant_id = $5 AND id = $6",
        )
        .await?;
    let modified_rows = txn
        .execute(
            &stmt,
            &[
                &new_desired_status.to_string(),
                &final_deployment_initial.map(runtime_desired_status_to_string),
                &final_bootstrap_policy.map(bootstrap_policy_to_string),
                &match final_deployment_error {
                    None => None,
                    Some(v) => Some(serialize_error_response(&v)?),
                },
                &tenant_id.0,
                &current.id.0,
            ],
        )
        .await?;
    if modified_rows > 0 {
        new_pipeline_monitor_event(txn, tenant_id, current.id, Uuid::now_v7()).await?;
        Ok(current.id)
    } else {
        Err(DBError::UnknownPipeline {
            pipeline_id: current.id,
        })
    }
}

/// Checks the version guard and that the resources status transition is valid.
/// Returns the current pipeline row in the database.
async fn check_version_guard_and_transition_deployment_resources_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
    new_deployment_resources_status: ResourcesStatus,
    remain: bool,
) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
    let current = get_pipeline_by_id_for_monitoring(txn, tenant_id, pipeline_id).await?;

    // Use the version guard to check that the deployment is the intended one
    if current.version != version_guard {
        return Err(DBError::OutdatedPipelineVersion {
            outdated_version: version_guard,
            latest_version: current.version,
        });
    }

    // Check transition is permitted by the state machine for resources status
    validate_resources_status_transition(
        current.storage_status,
        current.deployment_resources_status,
        new_deployment_resources_status,
    )?;

    // If the resources status should remain the same
    if remain && current.deployment_resources_status != new_deployment_resources_status {
        return Err(DBError::InvalidResourcesStatusRemain {
            current_status: current.deployment_resources_status,
            new_status: new_deployment_resources_status,
        });
    }

    // Generally, the pipeline must be successfully compiled to be able to transition resources
    // status. However, the runner before transitioning from Stopped to Provisioning does a final
    // check to make sure the compilation artifacts exists. If not all exist, compilation is
    // triggered again.
    //
    // As such either (a) the pipeline is successfully compiled, or (b) the resources status
    // transition is one of:
    // (1) Stopped -> Stopping
    // (2) Stopping -> Stopped
    if !matches!(
        (
            current.deployment_resources_status,
            new_deployment_resources_status
        ),
        (ResourcesStatus::Stopped, ResourcesStatus::Stopping)
            | (ResourcesStatus::Stopping, ResourcesStatus::Stopped)
    ) && current.program_status != ProgramStatus::Success
    {
        return Err(DBError::TransitionRequiresCompiledProgram {
            current: current.deployment_resources_status,
            transition_to: new_deployment_resources_status,
        });
    }

    Ok(current)
}

pub(crate) async fn set_deployment_resources_status_stopped(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
) -> Result<(), DBError> {
    check_version_guard_and_transition_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        version_guard,
        ResourcesStatus::Stopped,
        false,
    )
    .await?;
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    set_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        ResourcesStatus::Stopped,
        None,
        None,
        None,
        None,
        current.deployment_error,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

pub(crate) async fn set_deployment_resources_status_provisioning(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
    new_deployment_id: Uuid,
    new_deployment_config: serde_json::Value,
) -> Result<(), DBError> {
    check_version_guard_and_transition_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        version_guard,
        ResourcesStatus::Provisioning,
        false,
    )
    .await?;
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    set_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        ResourcesStatus::Provisioning,
        None, // This will become Some again by the `remains_` function
        None,
        None,
        None,
        None,
        Some(new_deployment_id),
        Some(new_deployment_config),
        None,
        None,
        current.deployment_initial,
        current.bootstrap_policy,
    )
    .await
}

pub(crate) async fn remain_deployment_resources_status_provisioning(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
    new_deployment_resources_status_details: serde_json::Value,
) -> Result<(), DBError> {
    check_version_guard_and_transition_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        version_guard,
        ResourcesStatus::Provisioning,
        true,
    )
    .await?;

    remain_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        Some(new_deployment_resources_status_details),
        None,
        None,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn set_deployment_resources_status_provisioned(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
    new_deployment_location: String,
    new_deployment_resources_status_details: serde_json::Value,
    new_deployment_runtime_status: RuntimeStatus,
    new_deployment_runtime_status_details: serde_json::Value,
    new_deployment_runtime_desired_status: RuntimeDesiredStatus,
) -> Result<(), DBError> {
    check_version_guard_and_transition_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        version_guard,
        ResourcesStatus::Provisioned,
        false,
    )
    .await?;
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    set_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        ResourcesStatus::Provisioned,
        Some(new_deployment_resources_status_details),
        Some(new_deployment_runtime_status),
        Some(new_deployment_runtime_status_details),
        Some(new_deployment_runtime_desired_status),
        None,
        current.deployment_id,
        current.deployment_config,
        Some(new_deployment_location),
        None,
        current.deployment_initial,
        current.bootstrap_policy,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn remain_deployment_resources_status_provisioned(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
    new_deployment_resources_status_details: serde_json::Value,
    new_deployment_runtime_status: RuntimeStatus,
    new_deployment_runtime_status_details: serde_json::Value,
    new_deployment_runtime_desired_status: RuntimeDesiredStatus,
    new_storage_status_details: Option<serde_json::Value>,
) -> Result<(), DBError> {
    check_version_guard_and_transition_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        version_guard,
        ResourcesStatus::Provisioned,
        true,
    )
    .await?;

    remain_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        Some(new_deployment_resources_status_details),
        Some(new_deployment_runtime_status),
        Some(new_deployment_runtime_status_details),
        Some(new_deployment_runtime_desired_status),
        new_storage_status_details,
    )
    .await
}

pub(crate) async fn set_deployment_resources_status_stopping(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
    new_deployment_error: Option<ErrorResponse>,
    new_storage_status_details: Option<serde_json::Value>,
) -> Result<(), DBError> {
    check_version_guard_and_transition_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        version_guard,
        ResourcesStatus::Stopping,
        false,
    )
    .await?;
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    set_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        ResourcesStatus::Stopping,
        None, // If stopping fails, this will become Some again by the `remains_` function
        None,
        None,
        None,
        new_deployment_error,
        current.deployment_id,
        current.deployment_config,
        None,
        new_storage_status_details,
        None,
        None,
    )
    .await
}

pub(crate) async fn remain_deployment_resources_status_stopping(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    version_guard: Version,
    new_deployment_resources_status_details: serde_json::Value,
) -> Result<(), DBError> {
    check_version_guard_and_transition_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        version_guard,
        ResourcesStatus::Stopping,
        true,
    )
    .await?;

    remain_deployment_resources_status(
        txn,
        tenant_id,
        pipeline_id,
        Some(new_deployment_resources_status_details),
        None,
        None,
        None,
        None,
    )
    .await
}

/// Does not change the deployment resources status, but does update a subset of the fields that can
/// be updated while the status stays the same.
///
/// **Important:** only `new_storage_status_details` is coalesced. All other arguments are NOT
/// coalesced, meaning that if they are `None`, then the field will become NULL.
#[allow(clippy::too_many_arguments)]
async fn set_deployment_resources_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    final_deployment_resources_status: ResourcesStatus,
    final_deployment_resources_status_details: Option<serde_json::Value>,
    final_deployment_runtime_status: Option<RuntimeStatus>,
    final_deployment_runtime_status_details: Option<serde_json::Value>,
    final_deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    final_deployment_error: Option<ErrorResponse>,
    final_deployment_id: Option<Uuid>,
    final_deployment_config: Option<serde_json::Value>,
    final_deployment_location: Option<String>,
    new_storage_status_details: Option<serde_json::Value>,
    final_deployment_initial: Option<RuntimeDesiredStatus>,
    final_bootstrap_policy: Option<BootstrapPolicy>,
) -> Result<(), DBError> {
    // Validate that the new or existing deployment configuration is valid
    if let Some(deployment_config) = &final_deployment_config {
        let _ = validate_deployment_config(deployment_config).map_err(|error| {
            DBError::InvalidDeploymentConfig {
                value: deployment_config.clone(),
                error,
            }
        })?;
    }

    // Validate that the new storage status details is valid
    if let Some(storage_status_details) = &new_storage_status_details {
        let _ = validate_storage_status_details(storage_status_details).map_err(|error| {
            DBError::InvalidStorageStatusDetails {
                value: storage_status_details.clone(),
                error,
            }
        })?;
    }

    // Execute query
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET deployment_error = $1,
                     deployment_config = $2,
                     deployment_location = $3,
                     storage_status_details = COALESCE($4::VARCHAR, storage_status_details),
                     deployment_id = $5,
                     deployment_initial = $6,
                     bootstrap_policy = $7,
                     deployment_resources_status = $8,
                     deployment_resources_status_details = $9::VARCHAR,
                     deployment_resources_status_since = CASE WHEN deployment_resources_status = $8::VARCHAR THEN deployment_resources_status_since ELSE NOW() END,
                     deployment_runtime_status = $10::VARCHAR,
                     deployment_runtime_status_details = $11::VARCHAR,
                     deployment_runtime_status_since = CASE WHEN $10::VARCHAR IS NULL THEN NULL ELSE (CASE WHEN deployment_runtime_status = $10::VARCHAR THEN deployment_runtime_status_since ELSE NOW() END) END,
                     deployment_runtime_desired_status = $12::VARCHAR,
                     deployment_runtime_desired_status_since = CASE WHEN $12::VARCHAR IS NULL THEN NULL ELSE (CASE WHEN deployment_runtime_desired_status = $12::VARCHAR THEN deployment_runtime_desired_status_since ELSE NOW() END) END,
                     refresh_version = refresh_version + 1
                 WHERE tenant_id = $13 AND id = $14",
        )
        .await?;
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &match final_deployment_error {
                    None => None,
                    Some(v) => Some(serialize_error_response(&v)?),
                }, // $1: deployment_error
                &final_deployment_config.map(|v| v.to_string()), // $2: deployment_config
                &final_deployment_location,                      // $3: deployment_location
                &new_storage_status_details.map(|v| v.to_string()), // $4: storage_status_details
                &final_deployment_id,                            // $5: deployment_id
                &final_deployment_initial.map(runtime_desired_status_to_string), // $6: deployment_initial
                &final_bootstrap_policy.map(bootstrap_policy_to_string), // $7: bootstrap_policy
                &final_deployment_resources_status.to_string(), // $8: deployment_resources_status,
                &final_deployment_resources_status_details.map(|v| v.to_string()), // $9: deployment_resources_status_details,
                &final_deployment_runtime_status.map(runtime_status_to_string), // $10: deployment_runtime_status,
                &final_deployment_runtime_status_details.map(|v| v.to_string()), // $11: deployment_runtime_status_details,
                &final_deployment_runtime_desired_status.map(runtime_desired_status_to_string), // $12: deployment_runtime_desired_status,
                &tenant_id.0,   // $13: tenant_id
                &pipeline_id.0, // $14: id
            ],
        )
        .await?;
    if rows_affected > 0 {
        new_pipeline_monitor_event(txn, tenant_id, pipeline_id, Uuid::now_v7()).await?;
        Ok(())
    } else {
        Err(DBError::UnknownPipeline { pipeline_id })
    }
}

/// Does not change the deployment resources status, but does update a subset of the fields that can
/// be updated while the status stays the same.
///
/// **Important:** the arguments are coalesced, meaning that if they are `None`, then the existing
/// value remains (rather than setting it to NULL).
#[allow(clippy::too_many_arguments)]
async fn remain_deployment_resources_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    new_deployment_resources_status_details: Option<serde_json::Value>,
    new_deployment_runtime_status: Option<RuntimeStatus>,
    new_deployment_runtime_status_details: Option<serde_json::Value>,
    new_deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    new_storage_status_details: Option<serde_json::Value>,
) -> Result<(), DBError> {
    // Validate that the new storage status details is valid
    if let Some(storage_status_details) = &new_storage_status_details {
        let _ = validate_storage_status_details(storage_status_details).map_err(|error| {
            DBError::InvalidStorageStatusDetails {
                value: storage_status_details.clone(),
                error,
            }
        })?;
    }

    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET deployment_resources_status_details = COALESCE($1::VARCHAR, deployment_resources_status_details),
                     deployment_runtime_status = COALESCE($2::VARCHAR, deployment_runtime_status),
                     deployment_runtime_status_details = COALESCE($3::VARCHAR, deployment_runtime_status_details),
                     deployment_runtime_status_since = CASE WHEN $2::VARCHAR IS NULL THEN deployment_runtime_status_since ELSE (CASE WHEN deployment_runtime_status = $2::VARCHAR THEN deployment_runtime_status_since ELSE NOW() END) END,
                     deployment_runtime_desired_status = COALESCE($4::VARCHAR, deployment_runtime_desired_status),
                     deployment_runtime_desired_status_since = CASE WHEN $4::VARCHAR IS NULL THEN deployment_runtime_desired_status_since ELSE (CASE WHEN deployment_runtime_desired_status = $4::VARCHAR THEN deployment_runtime_desired_status_since ELSE NOW() END) END,
                     storage_status_details = COALESCE($5::VARCHAR, storage_status_details),
                     refresh_version = refresh_version + 1
                 WHERE tenant_id = $6 AND id = $7",
        )
        .await?;
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &new_deployment_resources_status_details
                    .as_ref()
                    .map(|v| v.to_string()), // $1: deployment_resources_status_details,
                &new_deployment_runtime_status.map(runtime_status_to_string), // $2: deployment_runtime_status,
                &new_deployment_runtime_status_details
                    .as_ref()
                    .map(|v| v.to_string()), // $3: deployment_runtime_status_details,
                &new_deployment_runtime_desired_status.map(runtime_desired_status_to_string), // $4: deployment_runtime_desired_status,
                &new_storage_status_details.map(|v| v.to_string()), // $5: storage_status_details
                &tenant_id.0,                                       // $6: tenant_id
                &pipeline_id.0,                                     // $7: id
            ],
        )
        .await?;
    if rows_affected > 0 {
        new_pipeline_monitor_event(txn, tenant_id, pipeline_id, Uuid::now_v7()).await?;
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
        current.deployment_resources_status,
        current.deployment_resources_desired_status,
        current.storage_status,
        new_storage_status,
    )?;

    // If the storage is (getting) cleared, its details are no longer relevant
    let storage_status_details = if new_storage_status == StorageStatus::Clearing
        || new_storage_status == StorageStatus::Cleared
    {
        None
    } else {
        current.storage_status_details
    };

    // Execute query
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET storage_status = $1,
                     storage_status_details = $2
                 WHERE tenant_id = $3 AND id = $4",
        )
        .await?;
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &new_storage_status.to_string(),
                &storage_status_details.map(|v| v.to_string()),
                &tenant_id.0,
                &pipeline_id.0,
            ],
        )
        .await?;
    if rows_affected > 0 {
        new_pipeline_monitor_event(txn, tenant_id, pipeline_id, Uuid::now_v7()).await?;
        Ok(())
    } else {
        Err(DBError::UnknownPipeline { pipeline_id })
    }
}

/// Increments notify counter such that any LISTEN mechanism is appraised.
pub(crate) async fn increment_notify_counter(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: &str,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
                 SET notify_counter = notify_counter + 1
                 WHERE tenant_id = $1 AND name = $2",
        )
        .await?;
    let rows_affected = txn.execute(&stmt, &[&tenant_id.0, &pipeline_name]).await?;
    if rows_affected > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownPipelineName {
            pipeline_name: pipeline_name.to_string(),
        })
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
            "SELECT p.tenant_id, {PIPELINE_COLUMNS_MONITORING}
             FROM pipeline AS p
             ORDER BY p.id ASC
            "
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push((
            TenantId(row.get("tenant_id")),
            parse_pipeline_row_monitoring(&row)?,
        ));
    }
    Ok(result)
}

/// Retrieves the pipeline which is stopped, whose program status has been Pending
/// for the longest, and is of the current platform version. Returns `None` if none is found.
pub(crate) async fn get_next_sql_compilation(
    txn: &Transaction<'_>,
    platform_version: &str,
    worker_id: usize,
    total_workers: usize,
) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
    // The expression `abs(('x' || substr(replace(p.id::text, '-', ''), 1, 16))::bit(64)::bigint) % $2) = $3`
    // converts the first 8 bytes of the UUID to a bigint, takes its absolute value,
    // and computes the modulo with the total number of workers.
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT p.tenant_id, {PIPELINE_COLUMNS_ALL}
             FROM pipeline AS p
             WHERE p.deployment_resources_status = 'stopped'
                   AND p.program_status = 'pending'
                   AND p.platform_version = $1
                   AND (abs(('x' || substr(replace(p.id::text, '-', ''), 1, 16))::bit(64)::bigint) % $2) = $3
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
                &(total_workers as i64),       // $2: total_workers
                &(worker_id as i64),           // $3: worker_id
            ],
        )
        .await?;
    match row {
        None => Ok(None),
        Some(row) => Ok(Some((
            TenantId(row.get("tenant_id")),
            parse_pipeline_row_all(&row)?,
        ))),
    }
}

/// Retrieves the pipeline which is stopped, whose program status has been SqlCompiled
/// for the longest, and is of the current platform version. Returns `None` if none is found.
pub(crate) async fn get_next_rust_compilation(
    txn: &Transaction<'_>,
    platform_version: &str,
    worker_id: usize,
    total_workers: usize,
) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
    // The expression `abs(('x' || substr(replace(p.id::text, '-', ''), 1, 16))::bit(64)::bigint) % $2) = $3`
    // converts the first 8 bytes of the UUID to a bigint, takes its absolute value,
    // and computes the modulo with the total number of workers.
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT p.tenant_id, {PIPELINE_COLUMNS_ALL}
             FROM pipeline AS p
             WHERE p.deployment_resources_status = 'stopped'
                   AND p.program_status = 'sql_compiled'
                   AND p.platform_version = $1
                   AND (abs(('x' || substr(replace(p.id::text, '-', ''), 1, 16))::bit(64)::bigint) % $2) = $3
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
                &(total_workers as i64),       // $2: total_workers
                &(worker_id as i64),           // $3: worker_id
            ],
        )
        .await?;
    match row {
        None => Ok(None),
        Some(row) => Ok(Some((
            TenantId(row.get("tenant_id")),
            parse_pipeline_row_all(&row)?,
        ))),
    }
}

/// Retrieves the list of successfully compiled pipeline programs (pipeline identifier, program version,
/// program binary source checksum, program binary integrity checksum, program info integrity checksum) AND pipeline programs that
/// are currently being compiled (pipeline identifier, program version) across all tenants.
pub(crate) async fn list_pipeline_programs_across_all_tenants(
    txn: &Transaction<'_>,
) -> Result<
    Vec<(
        PipelineId,
        Version,
        Option<String>,
        Option<String>,
        Option<String>,
    )>,
    DBError,
> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.program_version, p.program_binary_source_checksum, p.program_binary_integrity_checksum, p.program_info_integrity_checksum
             FROM pipeline AS p
             WHERE p.program_status = 'success' OR p.program_status = 'compiling_rust'
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
                row.get::<_, Option<String>>(2),
                row.get::<_, Option<String>>(3),
                row.get::<_, Option<String>>(4),
            )
        })
        .collect())
}

/// Store a support data collection entry
pub(crate) async fn store_support_data_collection(
    txn: &Transaction<'_>,
    pipeline_id: PipelineId,
    tenant_id: TenantId,
    support_bundle: &SupportBundleData,
) -> Result<(), DBError> {
    let query = r#"
        INSERT INTO support_data_collections
        (pipeline_id, tenant_id, collected_at, support_bundle_data)
        VALUES ($1, $2, $3, $4)
    "#;

    // Serialize the support bundle data using MessagePack
    let serialized_data = to_vec(support_bundle).map_err(|e| DBError::InvalidJsonData {
        data: "support_bundle_data".to_string(),
        error: format!("Failed to serialize support bundle data: {}", e),
    })?;

    let stmt = txn.prepare_cached(query).await?;
    txn.execute(
        &stmt,
        &[
            &pipeline_id.0,
            &tenant_id.0,
            &support_bundle.time.naive_utc(),
            &serialized_data,
        ],
    )
    .await?;

    Ok(())
}

/// Clean up old support data collections for a pipeline.
///
/// Returns the number of deleted collections.
pub(crate) async fn cleanup_old_support_data_collections(
    txn: &Transaction<'_>,
    pipeline_id: PipelineId,
    retention_count: i64,
) -> Result<u64, DBError> {
    let query = r#"
        DELETE FROM support_data_collections
        WHERE pipeline_id = $1
        AND id NOT IN (
            SELECT id FROM support_data_collections
            WHERE pipeline_id = $1
            ORDER BY collected_at DESC
            LIMIT $2
        )
    "#;

    let stmt = txn.prepare_cached(query).await?;
    let r = txn
        .execute(&stmt, &[&pipeline_id.0, &retention_count])
        .await?;

    Ok(r)
}

/// Get support bundle data for a pipeline.
pub(crate) async fn get_support_bundle_data(
    txn: &Transaction<'_>,
    pipeline_id: PipelineId,
    limit: u64,
) -> Result<Vec<SupportBundleData>, DBError> {
    let query = r#"
        SELECT support_bundle_data
        FROM support_data_collections
        WHERE pipeline_id = $1
        ORDER BY collected_at DESC
        LIMIT $2
    "#;

    let limit = limit as i64;
    let rows = txn.query(query, &[&pipeline_id.0, &limit]).await?;

    let mut bundles = Vec::new();
    for row in rows {
        let data: Vec<u8> = row.get("support_bundle_data");
        let bundle = from_slice(&data).map_err(|e| DBError::InvalidJsonData {
            data: "<support_bundle_binary_data>".to_string(),
            error: format!("Failed to deserialize bundle data: {}", e),
        });
        match bundle {
            Ok(bundle) => bundles.push(bundle),
            Err(e) => {
                warn!(
                    pipeline_id = %pipeline_id,
                    pipeline = "N/A",
                    "Skipped support bundle data: {e}"
                );
            }
        }
    }

    Ok(bundles)
}
