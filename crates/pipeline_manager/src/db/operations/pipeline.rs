use crate::db::error::DBError;
use crate::db::operations::utils::{
    convert_unix_timestamp_to_datetime, maybe_tenant_id_foreign_key_constraint_err,
    maybe_unique_violation,
};
use crate::db::types::common::Version;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDescr, PipelineId, PipelineStatus,
};
use crate::db::types::program::{ProgramConfig, ProgramStatus};
use crate::db::types::tenant::TenantId;
use deadpool_postgres::Transaction;
use pipeline_types::config::{
    InputEndpointConfig, OutputEndpointConfig, PipelineConfig, RuntimeConfig,
};
use pipeline_types::error::ErrorResponse;
use pipeline_types::program_schema::ProgramSchema;
use std::borrow::Cow;
use std::collections::BTreeMap;
use tokio_postgres::Row;
use uuid::Uuid;

/// Converts a pipeline table row to its extended descriptor.
fn row_to_extended_pipeline_descriptor(
    row: &Row,
) -> Result<ExtendedPipelineDescr<String>, DBError> {
    assert_eq!(row.len(), 21);
    let program_schema_str = row.get::<_, Option<String>>(13);
    let deployment_config_str = row.get::<_, Option<String>>(19);
    let deployment_error_str = row.get::<_, Option<String>>(18);
    Ok(ExtendedPipelineDescr {
        id: PipelineId(row.get(0)),
        // tenant_id is not used
        name: row.get(2),
        description: row.get(3),
        created_at: convert_unix_timestamp_to_datetime("pipeline.created_at", row.get(4))?,
        version: Version(row.get(5)),
        runtime_config: RuntimeConfig::from_yaml(row.get(6)),
        program_code: row.get(7),
        program_config: ProgramConfig::from_yaml(row.get(8)),
        program_version: Version(row.get(9)),
        program_status: ProgramStatus::from_columns(row.get(10), row.get(12))?,
        program_status_since: convert_unix_timestamp_to_datetime(
            "pipeline.program_status_since",
            row.get(11),
        )?,
        program_schema: program_schema_str.map(|s| ProgramSchema::from_yaml(&s)),
        program_binary_url: row.get(14),
        deployment_status: row.get::<_, String>(15).try_into()?,
        deployment_status_since: convert_unix_timestamp_to_datetime(
            "pipeline.deployment_status_since",
            row.get(16),
        )?,
        deployment_desired_status: row.get::<_, String>(17).try_into()?,
        deployment_error: deployment_error_str.map(|s| ErrorResponse::from_yaml(&s)),
        deployment_config: deployment_config_str.map(|s| PipelineConfig::from_yaml(&s)),
        deployment_location: row.get(20),
    })
}

/// Generates the pipeline configuration derived from the runtime configuration and program schema.
pub fn generate_pipeline_config(
    pipeline_id: PipelineId,
    runtime_config: &RuntimeConfig,
    _program_schema: &ProgramSchema,
) -> Result<PipelineConfig, DBError> {
    // Input and output connectors
    let inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig> = BTreeMap::new();
    // TODO: populate inputs based on program_schema
    let outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig> = BTreeMap::new();
    // TODO: populate outputs based on program_schema

    Ok(PipelineConfig {
        name: Some(format!("pipeline-{pipeline_id}")),
        global: runtime_config.clone(),
        storage_config: None, // Set by the runner based on global field
        inputs,
        outputs,
    })
}

pub(crate) async fn list_pipelines(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<ExtendedPipelineDescr<String>>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_schema, p.program_binary_url,
                    p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
                    p.deployment_error, p.deployment_config, p.deployment_location
             FROM pipeline AS p
             WHERE p.tenant_id = $1
             ORDER BY p.created_at
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
) -> Result<ExtendedPipelineDescr<String>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_schema, p.program_binary_url,
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
) -> Result<ExtendedPipelineDescr<String>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_schema, p.program_binary_url,
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
    let stmt = txn

        .prepare_cached(
            "INSERT INTO pipeline (id, tenant_id, name, description, created_at, version, runtime_config,
                                   program_code, program_config, program_version, program_status,
                                   program_status_since, program_error, program_schema, program_binary_url,
                                   deployment_status, deployment_status_since, deployment_desired_status,
                                   deployment_error, deployment_config, deployment_location)
            VALUES ($1, $2, $3, $4, extract(epoch from now()), $5, $6,
                    $7, $8, $9, $10,
                    extract(epoch from now()), NULL, NULL, NULL,
                    $11, extract(epoch from now()), $12,
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
            &pipeline.program_config.to_yaml(),     // $8: program_config
            &Version(1).0,                          // $9: program_version
            &ProgramStatus::Pending.to_columns().0, // $10: program_status
            &PipelineStatus::Shutdown.to_string(),  // $11: deployment_status
            &PipelineStatus::Shutdown.to_string(),  // $12: deployment_desired_status
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
    program_config: &Option<ProgramConfig>,
) -> Result<Version, DBError> {
    // Fetch current pipeline to decide how to update.
    // This will also return an error if the pipeline does not exist.
    let current = get_pipeline(txn, tenant_id, original_name).await?;

    // The pipeline must be fully shutdown to be updated
    if !current.is_fully_shutdown() {
        return Err(DBError::CannotUpdateRunningPipeline);
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
                     program_config = COALESCE($5, program_config),
                     version = version + 1
                 WHERE tenant_id = $6 AND name = $7",
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
        || program_config
            .as_ref()
            .is_some_and(|v| *v != current.program_config);
    if program_changed {
        let stmt = txn
            .prepare_cached(
                "UPDATE pipeline
                 SET program_version = program_version + 1,
                     program_schema = NULL,
                     program_status = $1,
                     program_status_since = extract(epoch from now()),
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
        return Err(DBError::CannotDeleteRunningPipeline);
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
    new_program_schema: &Option<ProgramSchema>,
    new_program_binary_url: &Option<String>,
) -> Result<(), DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // The pipeline must be fully shutdown to be have program status updated
    if !current.is_fully_shutdown() {
        return Err(DBError::CannotUpdateRunningPipeline);
    }

    // Only if the program whose status is being transitioned is the same one can it be updated
    if current.program_version != program_version_guard {
        return Err(DBError::OutdatedProgramVersion {
            latest_version: current.program_version,
        });
    }

    // Check that the transition from the current status to the new status is permitted
    let valid_transition = matches!(
        (current.program_status.clone(), new_program_status),
        (ProgramStatus::Pending, ProgramStatus::CompilingSql)
            | (ProgramStatus::Pending, ProgramStatus::SystemError(_))
            | (ProgramStatus::CompilingSql, ProgramStatus::Pending)
            | (ProgramStatus::CompilingSql, ProgramStatus::CompilingRust)
            | (ProgramStatus::CompilingSql, ProgramStatus::SqlError(_))
            | (ProgramStatus::CompilingSql, ProgramStatus::SystemError(_))
            | (ProgramStatus::CompilingRust, ProgramStatus::Pending)
            | (ProgramStatus::CompilingRust, ProgramStatus::Success)
            | (ProgramStatus::CompilingRust, ProgramStatus::RustError(_))
            | (ProgramStatus::CompilingRust, ProgramStatus::SystemError(_))
    );
    if !valid_transition {
        return Err(DBError::InvalidProgramStatusTransition {
            current: current.program_status,
            transition_to: new_program_status.clone(),
        });
    }

    // Determine new values depending on where to transition to
    // Note that None becomes NULL as there is no coalescing in the query.
    let (final_program_schema, final_program_binary_url) = match &new_program_status {
        ProgramStatus::Pending => {
            assert!(new_program_schema.is_none() && new_program_binary_url.is_none());
            (None, None)
        }
        ProgramStatus::CompilingSql => {
            assert!(new_program_schema.is_none() && new_program_binary_url.is_none());
            (None, None)
        }
        ProgramStatus::CompilingRust => {
            assert!(new_program_schema.is_some() && new_program_binary_url.is_none());
            (new_program_schema.clone(), None)
        }
        ProgramStatus::Success => {
            assert!(new_program_schema.is_none() && new_program_binary_url.is_some());
            (current.program_schema, new_program_binary_url.clone())
        }
        ProgramStatus::SqlError(_) => (None, None),
        ProgramStatus::RustError(_) => (current.program_schema, None),
        ProgramStatus::SystemError(_) => (current.program_schema, None),
    };

    // Perform query
    let stmt = txn
        .prepare_cached(
            "UPDATE pipeline
             SET program_status = $1,
                 program_status_since = extract(epoch from now()),
                 program_error = $2,
                 program_schema = $3,
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
                &final_program_schema.as_ref().map(|v| v.to_yaml()),
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
pub(crate) async fn set_desired_deployment_status(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: &str,
    desired_status: PipelineStatus,
) -> Result<(), DBError> {
    assert!(
        desired_status == PipelineStatus::Running
            || desired_status == PipelineStatus::Paused
            || desired_status == PipelineStatus::Shutdown
    );

    // The pipeline must be fully compiled to change deployment desired status
    let current = get_pipeline(txn, tenant_id, pipeline_name).await?;

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
    assert!(current.program_schema.is_some());
    assert!(current.program_binary_url.is_some());

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
            &[&desired_status.to_string(), &tenant_id.0, &current.id.0],
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
    deployment_status: PipelineStatus,
    new_deployment_error: Option<ErrorResponse>,
    new_deployment_config: Option<PipelineConfig>,
    new_deployment_location: Option<String>,
) -> Result<ExtendedPipelineDescr<String>, DBError> {
    let current = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    // The pipeline must be fully compiled to change deployment status
    if current.program_status != ProgramStatus::Success {
        return Err(DBError::ProgramNotYetCompiled);
    }

    // Check that the transition from the current status to the new status is permitted
    let valid_transition = matches!(
        (current.deployment_status, deployment_status),
        (PipelineStatus::Shutdown, PipelineStatus::Provisioning)
            | (PipelineStatus::Provisioning, PipelineStatus::Initializing)
            | (PipelineStatus::Provisioning, PipelineStatus::Shutdown)
            | (PipelineStatus::Provisioning, PipelineStatus::Failed)
            | (PipelineStatus::Initializing, PipelineStatus::Paused)
            | (PipelineStatus::Initializing, PipelineStatus::Shutdown)
            | (PipelineStatus::Initializing, PipelineStatus::Failed)
            | (PipelineStatus::Running, PipelineStatus::Paused)
            | (PipelineStatus::Running, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Running, PipelineStatus::Failed)
            | (PipelineStatus::Paused, PipelineStatus::Running)
            | (PipelineStatus::Paused, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Paused, PipelineStatus::Failed)
            | (PipelineStatus::ShuttingDown, PipelineStatus::Shutdown)
            | (PipelineStatus::ShuttingDown, PipelineStatus::Failed)
    );
    if !valid_transition {
        return Err(DBError::InvalidDeploymentStatusTransition {
            current: current.deployment_status,
            transition_to: deployment_status,
        });
    }

    // Determine the final values of the additional fields using the current default.
    // Note that None becomes NULL as there is no coalescing in the query.

    // Deployment configuration is set when becoming...
    // - Failed: a value
    // - Otherwise: current value
    let final_deployment_error = if deployment_status == PipelineStatus::Failed {
        assert!(new_deployment_error.is_some());
        new_deployment_error
    } else {
        assert!(new_deployment_error.is_none());
        assert!(current.deployment_error.is_none());
        current.deployment_error
    };

    // Deployment configuration is set when becoming...
    // - Provisioning: a value
    // - Shutdown: NULL
    // - Otherwise: current value
    let final_deployment_config = if deployment_status == PipelineStatus::Provisioning {
        new_deployment_config
    } else if deployment_status == PipelineStatus::Shutdown {
        None
    } else {
        current.deployment_config
    };

    // Deployment location is set when becoming...
    // - Initializing: a value
    // - Shutdown: NULL
    // - Otherwise: current value
    let final_deployment_location = if deployment_status == PipelineStatus::Initializing {
        assert!(new_deployment_location.is_some());
        new_deployment_location
    } else if deployment_status == PipelineStatus::Shutdown {
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
                     deployment_status_since = extract(epoch from now()),
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
                &deployment_status.to_string(),
                &final_deployment_error.map(|v| v.to_yaml()),
                &final_deployment_config.map(|v| v.to_yaml()),
                &final_deployment_location,
                &tenant_id.0,
                &pipeline_id.0,
            ],
        )
        .await?;
    if rows_affected > 0 {
        get_pipeline_by_id(txn, tenant_id, pipeline_id).await // TODO: no longer needed?
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
             ORDER BY p.created_at
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
) -> Result<Vec<(TenantId, ExtendedPipelineDescr<String>)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id,p. tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_schema, p.program_binary_url,
                    p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
                    p.deployment_error, p.deployment_config, p.deployment_location
             FROM pipeline AS p
             ORDER BY p.created_at
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

/// Retrieves the pipeline whose program status has been Pending for the longest and whose program code is non-empty.
pub(crate) async fn get_next_pipeline_program_to_compile(
    txn: &Transaction<'_>,
) -> Result<Option<(TenantId, ExtendedPipelineDescr<String>)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT p.id, p.tenant_id, p.name, p.description, p.created_at, p.version, p.runtime_config,
                    p.program_code, p.program_config, p.program_version, p.program_status,
                    p.program_status_since, p.program_error, p.program_schema, p.program_binary_url,
                    p.deployment_status, p.deployment_status_since, p.deployment_desired_status,
                    p.deployment_error, p.deployment_config, p.deployment_location
             FROM pipeline AS p
             WHERE p.program_status = 'pending'
                   AND program_status_since = (
                       SELECT MIN(p2.program_status_since)
                       FROM pipeline AS p2
                       WHERE p2.program_status = 'pending' AND p2.program_code != ''
                   )
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
/// It is in use if the pipeline still exists AND the program version matches the one provided.
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
