use crate::db::error::DBError;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_unique_violation,
};
use crate::db::types::common::{validate_name, Version};
use crate::db::types::pipeline::{
    validate_deployment_desired_status_transition, validate_deployment_status_transition,
    ExtendedPipelineDescr, PipelineDescr, PipelineDesiredStatus, PipelineId, PipelineStatus,
};
use crate::db::types::program::{
    validate_program_status_transition, ProgramConfig, ProgramInfo, ProgramStatus,
};
use crate::db::types::tenant::TenantId;
use deadpool_postgres::Transaction;
use feldera_types::config::{PipelineConfig, RuntimeConfig};
use feldera_types::error::ErrorResponse;
use tokio_postgres::Row;
use uuid::Uuid;

/// Converts a pipeline table row to its extended descriptor.
fn row_to_extended_pipeline_descriptor(row: &Row) -> Result<ExtendedPipelineDescr, DBError> {
    assert_eq!(row.len(), 23);
    let program_info_str = row.get::<_, Option<String>>(15);
    let deployment_config_str = row.get::<_, Option<String>>(21);
    let deployment_error_str = row.get::<_, Option<String>>(20);
    Ok(ExtendedPipelineDescr {
        id: PipelineId(row.get(0)),
        // tenant_id is not used
        name: row.get(2),
        description: row.get(3),
        created_at: row.get(4),
        version: Version(row.get(5)),
        runtime_config: RuntimeConfig::from_yaml(row.get(6)),
        program_code: row.get(7),
        udf_rust: row.get(8),
        udf_toml: row.get(9),
        program_config: ProgramConfig::from_yaml(row.get(10)),
        program_version: Version(row.get(11)),
        program_status: ProgramStatus::from_columns(row.get(12), row.get(14))?,
        program_status_since: row.get(13),
        program_info: program_info_str.map(|s| ProgramInfo::from_yaml(&s)),
        program_binary_url: row.get(16),
        deployment_status: row.get::<_, String>(17).try_into()?,
        deployment_status_since: row.get(18),
        deployment_desired_status: row.get::<_, String>(19).try_into()?,
        deployment_error: deployment_error_str.map(|s| ErrorResponse::from_yaml(&s)),
        deployment_config: deployment_config_str.map(|s| PipelineConfig::from_yaml(&s)),
        deployment_location: row.get(22),
    })
}

pub(crate) async fn list_pipelines(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<ExtendedPipelineDescr>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.udf_rust, p.udf_toml, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_info, p.program_binary_url,
                    p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
                    p.deployment_error, p.deployment_config, p.deployment_location
             FROM pipeline AS p
             WHERE p.tenant_id = $1
             ORDER BY p.id
            ",
        )
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[&tenant_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(row_to_extended_pipeline_descriptor(&row)?);
    }
    Ok(result)
}

pub(crate) async fn get_pipeline(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<ExtendedPipelineDescr, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.udf_rust, p.udf_toml, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_info, p.program_binary_url,
                    p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
                    p.deployment_error, p.deployment_config, p.deployment_location
             FROM pipeline AS p
             WHERE p.tenant_id = $1 AND p.name = $2
            ",
        )
        .await?;
    let row = txn.query_opt(&stmt, &[&tenant_id.0, &name]).await?.ok_or(
        DBError::UnknownPipelineName {
            pipeline_name: name.to_string(),
        },
    )?;
    row_to_extended_pipeline_descriptor(&row)
}

pub async fn get_pipeline_by_id(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
) -> Result<ExtendedPipelineDescr, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.udf_rust, p.udf_toml, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_info, p.program_binary_url,
                    p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
                    p.deployment_error, p.deployment_config, p.deployment_location
             FROM pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = $2
            ",
        )
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0])
        .await?
        .ok_or(DBError::UnknownPipeline { pipeline_id })?;
    row_to_extended_pipeline_descriptor(&row)
}

pub(crate) async fn new_pipeline(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    new_id: Uuid,
    pipeline: PipelineDescr,
) -> Result<(PipelineId, Version), DBError> {
    validate_name(&pipeline.name)?;
    let stmt = txn

        .prepare_cached(
            "INSERT INTO pipeline (id, tenant_id, name, description, created_at, version, runtime_config,
                                   program_code, udf_rust, udf_toml, program_config, program_version, program_status,
                                   program_status_since, program_error, program_info, program_binary_url,
                                   deployment_status, deployment_status_since, deployment_desired_status,
                                   deployment_error, deployment_config, deployment_location)
            VALUES ($1, $2, $3, $4, now(), $5, $6,
                    $7, $8, $9, $10, $11, $12,
                    now(), NULL, NULL, NULL,
                    $13, now(), $14,
                    NULL, NULL, NULL)",
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
            &pipeline.runtime_config.to_yaml(),     // $6: runtime_config
            &pipeline.program_code,                 // $7: program_code
            &pipeline.udf_rust,                     // $8: udf_rust
            &pipeline.udf_toml,                     // $9: udf_toml
            &pipeline.program_config.to_yaml(),     // $10: program_config
            &Version(1).0,                          // $11: program_version
            &ProgramStatus::Pending.to_columns().0, // $12: program_status
            &PipelineStatus::Shutdown.to_string(),  // $13: deployment_status
            &PipelineStatus::Shutdown.to_string(),  // $14: deployment_desired_status
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
    tenant_id: TenantId,
    original_name: &str,
    name: &Option<String>,
    description: &Option<String>,
    runtime_config: &Option<RuntimeConfig>,
    program_code: &Option<String>,
    udf_rust: &Option<String>,
    udf_toml: &Option<String>,
    program_config: &Option<ProgramConfig>,
) -> Result<Version, DBError> {
    if let Some(name) = name {
        validate_name(name)?;
    }

    // Fetch current pipeline to decide how to update.
    // This will also return an error if the pipeline does not exist.
    let current = get_pipeline(txn, tenant_id, original_name).await?;

    // The pipeline must be fully shutdown to be updated
    if !current.is_fully_shutdown() {
        return Err(DBError::CannotUpdateNonShutdownPipeline);
    }

    // If nothing changes in any of the core fields, return the current version
    if (name.is_none() || name.as_ref().is_some_and(|v| *v == current.name))
        && (description.is_none()
            || description
                .as_ref()
                .is_some_and(|v| *v == current.description))
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
                     runtime_config = COALESCE($3, runtime_config),
                     program_code = COALESCE($4, program_code),
                     udf_rust = COALESCE($5, udf_rust),
                     udf_toml = COALESCE($6, udf_toml),
                     program_config = COALESCE($7, program_config),
                     version = version + 1
                 WHERE tenant_id = $8 AND name = $9",
        )
        .await?;
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &name,
                &description,
                &runtime_config.as_ref().map(|v| v.to_yaml()),
                &program_code,
                &udf_rust,
                &udf_toml,
                &program_config.as_ref().map(|v| v.to_yaml()),
                &tenant_id.0,
                &original_name,
            ],
        )
        .await
        .map_err(maybe_unique_violation)?;
    assert_eq!(rows_affected, 1); // The row must exist as it has been retrieved before

    // If the program changed, the program version must be incremented,
    // the schema unset and the status back to pending.
    let program_changed = program_code
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
    if !current.is_fully_shutdown() {
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
pub(crate) async fn set_program_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    program_version_guard: Version,
    new_program_status: &ProgramStatus,
    new_program_info: &Option<ProgramInfo>,
    new_program_binary_url: &Option<String>,
) -> Result<(), DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // The pipeline must be fully shutdown to be have program status updated
    if !current.is_fully_shutdown() {
        return Err(DBError::CannotUpdateNonShutdownPipeline);
    }

    // Only if the program whose status is being transitioned is the same one can it be updated
    if current.program_version != program_version_guard {
        return Err(DBError::OutdatedProgramVersion {
            latest_version: current.program_version,
        });
    }

    // Check that the transition from the current status to the new status is permitted
    validate_program_status_transition(&current.program_status, new_program_status)?;

    // Determine new values depending on where to transition to
    // Note that None becomes NULL as there is no coalescing in the query.
    let (final_program_info, final_program_binary_url) = match &new_program_status {
        ProgramStatus::Pending => {
            assert!(new_program_info.is_none() && new_program_binary_url.is_none());
            (None, None)
        }
        ProgramStatus::CompilingSql => {
            assert!(new_program_info.is_none() && new_program_binary_url.is_none());
            (None, None)
        }
        ProgramStatus::CompilingRust => {
            assert!(new_program_info.is_some() && new_program_binary_url.is_none());
            (new_program_info.clone(), None)
        }
        ProgramStatus::Success => {
            assert!(new_program_info.is_none() && new_program_binary_url.is_some());
            (current.program_info, new_program_binary_url.clone())
        }
        ProgramStatus::SqlError(_) => (None, None),
        ProgramStatus::RustError(_) => (current.program_info, None),
        ProgramStatus::SystemError(_) => (current.program_info, None),
    };

    // Perform query
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
             SET program_status = $1,
                 program_status_since = now(),
                 program_error = $2,
                 program_info = $3,
                 program_binary_url = $4
             WHERE tenant_id = $5 AND id = $6",
        )
        .await?;
    let new_program_status_columns = new_program_status.to_columns();
    let rows_affected = txn
        .execute(
            &stmt,
            &[
                &new_program_status_columns.0,
                &new_program_status_columns.1,
                &final_program_info.as_ref().map(|v| v.to_yaml()),
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
    // The pipeline must be fully compiled to change deployment desired status
    let current = get_pipeline(txn, tenant_id, pipeline_name).await?;

    // Check that the desired status can be set
    validate_deployment_desired_status_transition(
        &current.deployment_status,
        &current.deployment_desired_status,
        &new_desired_status,
    )?;

    // The program must be fully compiled for any desired status besides shutdown
    if new_desired_status != PipelineDesiredStatus::Shutdown {
        // Program must not have failed to compiled
        if current.program_status.has_failed_to_compile() {
            return Err(DBError::ProgramFailedToCompile);
        }

        // Program must be fully compiled (and not still ongoing)
        // Checked after compilation failure check, to give a more insightful error message.
        if !current.program_status.is_fully_compiled() {
            return Err(DBError::ProgramNotYetCompiled);
        }

        // If fully compiled, the following assertions must be upheld
        assert!(current.program_info.is_some());
        assert!(current.program_binary_url.is_some());
    }

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
    new_deployment_config: Option<PipelineConfig>,
    new_deployment_location: Option<String>,
) -> Result<(), DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // Program must not have failed to compiled
    if current.program_status.has_failed_to_compile() {
        return Err(DBError::ProgramFailedToCompile);
    }

    // Program must be fully compiled (and not still ongoing)
    // Checked after compilation failure check, to give a more insightful error message.
    if !current.program_status.is_fully_compiled() {
        return Err(DBError::ProgramNotYetCompiled);
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
                &final_deployment_error.map(|v| v.to_yaml()),
                &final_deployment_config.map(|v| v.to_yaml()),
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
             ORDER BY p.id
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

/// Lists pipelines across all tenants.
pub(crate) async fn list_pipelines_across_all_tenants(
    txn: &Transaction<'_>,
) -> Result<Vec<(TenantId, ExtendedPipelineDescr)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id,p. tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.udf_rust, p.udf_toml, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_info, p.program_binary_url,
                    p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
                    p.deployment_error, p.deployment_config, p.deployment_location
             FROM pipeline AS p
             ORDER BY p.id
            ",
        )
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push((
            TenantId(row.get(1)),
            row_to_extended_pipeline_descriptor(&row)?,
        ));
    }
    Ok(result)
}

/// Retrieves the pipeline whose program status has been Pending for the longest.
pub(crate) async fn get_next_pipeline_program_to_compile(
    txn: &Transaction<'_>,
) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.udf_rust, p.udf_toml, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_info, p.program_binary_url,
                    p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
                    p.deployment_error, p.deployment_config, p.deployment_location
             FROM pipeline AS p
             WHERE p.program_status = 'pending'
             ORDER BY p.program_status_since ASC, p.id ASC
             LIMIT 1
            "
        )
        .await?;
    let row = txn.query_opt(&stmt, &[]).await?;
    match row {
        None => Ok(None),
        Some(row) => Ok(Some((
            TenantId(row.get(1)),
            row_to_extended_pipeline_descriptor(&row)?,
        ))),
    }
}

/// Checks whether the provided pipeline program version is in use.
/// It is in use if the pipeline exists and the program version provided is the current one.
/// If the pipeline does not exist or the program version is not the latest, false is returned.
pub(crate) async fn is_pipeline_program_in_use(
    txn: &Transaction<'_>,
    pipeline_id: PipelineId,
    program_version: Version,
) -> Result<bool, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.program_version
             FROM pipeline AS p
             WHERE p.id = $1
            ",
        )
        .await?;
    let row = txn.query_opt(&stmt, &[&pipeline_id.0]).await?;
    match row {
        None => Ok(false),
        Some(row) => Ok(Version(row.get(0)) == program_version),
    }
}
