use crate::common_error::CommonError;
use crate::compiler::util::{
    cleanup_specific_directories, create_new_file, create_new_file_with_content, read_file_content,
    recreate_dir, CleanupDecision,
};
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::common::Version;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::{
    generate_program_info, ProgramConfig, ProgramInfo, SqlCompilerMessage,
};
use crate::db::types::tenant::TenantId;
use feldera_types::program_schema::ProgramSchema;
use log::{debug, error, info, trace};
use std::path::Path;
use std::time::Instant;
use std::{process::Stdio, sync::Arc};
use tokio::{
    fs,
    process::Command,
    sync::Mutex,
    time::{sleep, Duration},
};
use uuid::Uuid;

/// The frequency at which the compiler polls the database for new SQL compilation requests.
/// It balances resource consumption due to polling and a fast SQL compilation response.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Minimum frequency at which the compiler polls the database.
/// This minimum is a preventative measure to avoid the SQL compiler
/// from ever polling the database in an uninterrupted loop.
const POLL_INTERVAL_MINIMUM: Duration = Duration::from_millis(25);

/// The poll frequency when an unexpected database error occurred.
/// This is set relatively long to not flood the logs when
/// for instance the database becomes temporarily unreachable.
const POLL_ERROR_INTERVAL: Duration = Duration::from_secs(30);

/// The frequency at which during SQL compilation it is checked whether
/// the process has finished and as well whether it needs to be canceled
/// if the program is outdated.
const COMPILATION_CHECK_INTERVAL: Duration = Duration::from_millis(250);

/// The frequency at which the deletion of SQL compilation directories of deleted pipelines occurs.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// SQL compilation task that wakes up periodically.
/// Sleeps inbetween cycles which affects the response time of SQL compilation.
/// Note that the logic in this task assumes only one is run at a time.
/// This task cannot fail, and any internal errors are caught and written to log if need-be.
pub async fn sql_compiler_task(
    common_config: CommonConfig,
    config: CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) {
    let mut last_cleanup: Option<Instant> = None;
    loop {
        let mut unexpected_error = false;

        // Clean up
        if last_cleanup.is_none() || last_cleanup.is_some_and(|ts| ts.elapsed() >= CLEANUP_INTERVAL)
        {
            if let Err(e) = cleanup_sql_compilation(&config, db.clone()).await {
                match e {
                    SqlCompilationCleanupError::DBError(e) => {
                        error!("SQL compilation cleanup failed: database error occurred: {e}");
                    }
                    SqlCompilationCleanupError::CommonError(e) => {
                        error!("SQL compilation cleanup failed: filesystem error occurred: {e}");
                    }
                }
                unexpected_error = true;
            }
            last_cleanup = Some(Instant::now());
        }

        // Compile
        let result = attempt_end_to_end_sql_compilation(&common_config, &config, db.clone()).await;
        if let Err(e) = &result {
            match e {
                DBError::UnknownPipeline { pipeline_id } => {
                    debug!("SQL compilation canceled: pipeline {pipeline_id} no longer exists");
                }
                DBError::OutdatedProgramVersion {
                    outdated_version,
                    latest_version,
                } => {
                    debug!("SQL compilation canceled: pipeline program version ({outdated_version}) is outdated by latest ({latest_version})");
                }
                e => {
                    unexpected_error = true;
                    error!("SQL compilation canceled: unexpected database error occurred: {e}");
                }
            }
        }

        // Wait
        if unexpected_error {
            // Unexpected error occurred
            sleep(POLL_ERROR_INTERVAL).await;
        } else if result.is_ok_and(|found| !found) {
            // No pipeline was found to attempt to be compiled
            sleep(POLL_INTERVAL).await;
        } else {
            // A pipeline was attempted to be compiled or an expected
            // database error occurred (e.g., no longer exists, outdated)
            sleep(POLL_INTERVAL_MINIMUM).await;
        }
    }
}

/// Performs end-to-end SQL compilation:
/// 1. Reset in the database any pipeline with `program_status` of `CompilingSql` back to
///    `Pending` if they are of the current `platform_version`. Any pipeline with
///    `program_status` of `Pending` or `CompilingSql` with a non-current `platform_version`
///    will have it updated to current and its `program_status` set back to `Pending`
///    (if not already).
/// 2. Queries the database for a pipeline which has `program_status` of `Pending` the longest
/// 3. Updates pipeline database `program_status` to `CompilingSql`
/// 4. Performs SQL compilation on `program_code`, configured with `program_config`
/// 5. Upon completion, the compilation status is set to `SqlCompiled` with the `program_info`
///    containing the output of the SQL compiler (inputs, outputs, `main.rs`, `stubs.rs`, etc.)
///
/// Note that this function assumes it runs in isolation, and as such at the beginning resets
/// any lingering pipelines that have `CompilingSql` status to `Pending`. This recovers from
/// if the compiler was interrupted (e.g., it was unexpectedly terminated) or a database
/// operation failed.
///
/// Returns with `Ok(true)` if there was an attempt to compile the SQL of a pipeline.
/// It does not necessarily mean the compilation was a success.
/// Returns with `Ok(false)` if no pipeline is found for which to compile SQL.
/// Returns with `Err(...)` if a database operation fails, e.g., due to:
/// - The pipeline no longer exists
/// - The pipeline program is detected to be updated (it became outdated)
/// - The database cannot be reached
async fn attempt_end_to_end_sql_compilation(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<bool, DBError> {
    trace!("Performing SQL compilation...");

    // (1) Reset any pipeline which is `CompilingSql` back to `Pending`
    db.lock()
        .await
        .clear_ongoing_sql_compilation(&common_config.platform_version)
        .await?;

    // (2) Find pipeline which needs SQL compilation
    let Some((tenant_id, pipeline)) = db
        .lock()
        .await
        .get_next_sql_compilation(&common_config.platform_version)
        .await?
    else {
        trace!("No pipeline found which needs SQL compilation");
        return Ok(false);
    };
    info!(
        "SQL compilation started: pipeline {} (program version: {})",
        pipeline.id, pipeline.program_version
    );

    // (3) Update database that SQL compilation is ongoing
    db.lock()
        .await
        .transit_program_status_to_compiling_sql(tenant_id, pipeline.id, pipeline.program_version)
        .await?;

    // (4) Perform SQL compilation
    let compilation_result = perform_sql_compilation(
        common_config,
        config,
        Some(db.clone()),
        tenant_id,
        pipeline.id,
        &pipeline.platform_version,
        pipeline.program_version,
        &pipeline.program_config,
        &pipeline.program_code,
    )
    .await;

    // (5) Update database that SQL compilation is finished
    match compilation_result {
        Ok((program_info, duration)) => {
            info!(
                "SQL compilation success: pipeline {} (program version: {}) (took {:.2}s)",
                pipeline.id,
                pipeline.program_version,
                duration.as_secs_f64()
            );
            db.lock()
                .await
                .transit_program_status_to_sql_compiled(
                    tenant_id,
                    pipeline.id,
                    pipeline.program_version,
                    &program_info,
                )
                .await?;
        }
        Err(e) => match e {
            SqlCompilationError::Outdated => {
                debug!(
                    "SQL compilation canceled: pipeline {} (program version: {}) is outdated",
                    pipeline.id, pipeline.program_version,
                );
            }
            SqlCompilationError::TerminatedBySignal => {
                error!(
                    "SQL compilation interrupted: pipeline {} (program version: {}) compilation process was terminated by a signal",
                    pipeline.id, pipeline.program_version,
                );
            }
            SqlCompilationError::SqlError(sql_compiler_messages) => {
                db.lock()
                    .await
                    .transit_program_status_to_sql_error(
                        tenant_id,
                        pipeline.id,
                        pipeline.program_version,
                        sql_compiler_messages.clone(),
                    )
                    .await?;
                info!(
                    "SQL compilation failed: pipeline {} (program version: {}) due to SQL errors",
                    pipeline.id, pipeline.program_version
                );
            }
            SqlCompilationError::SystemError(internal_system_error) => {
                db.lock()
                    .await
                    .transit_program_status_to_system_error(
                        tenant_id,
                        pipeline.id,
                        pipeline.program_version,
                        &internal_system_error,
                    )
                    .await?;
                error!("SQL compilation failed: pipeline {} (program version: {}) due to system error:\n{}", pipeline.id, pipeline.program_version, internal_system_error);
            }
        },
    }
    Ok(true)
}

/// SQL compilation possible error outcomes.
pub enum SqlCompilationError {
    /// In the meanwhile the pipeline was already updated, as such the
    /// SQL compilation is outdated and no longer useful.
    Outdated,
    /// The SQL compilation process was terminated by a signal.
    /// This can happen for instance when the compiler server is terminated by signal,
    /// and processes started by it are first terminated before itself. The signal is likely not
    /// related to the program itself inherently being unable to compile, nor the compiler
    /// server reaching an inconsistent state. As such, retrying is the desired
    /// behavior rather than declaring failure to compile the specific program.
    TerminatedBySignal,
    /// Identifiable issue with the SQL (e.g., syntax error, connector error)
    SqlError(Vec<SqlCompilerMessage>),
    /// General system problem occurred (e.g., I/O error)
    SystemError(String),
}

/// Common errors are system errors during SQL compilation.
impl From<CommonError> for SqlCompilationError {
    fn from(value: CommonError) -> Self {
        SqlCompilationError::SystemError(value.to_string())
    }
}

/// Performs the SQL compilation:
/// - Prepares a working directory for input and output
/// - Call the SQL-to-DBSP compiler executable via a process
/// - Returns the outcome from the output
#[allow(clippy::too_many_arguments)]
pub async fn perform_sql_compilation(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    db: Option<Arc<Mutex<StoragePostgres>>>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    platform_version: &str,
    program_version: Version,
    _program_config: &ProgramConfig, // Might be used in the future to pass SQL compiler flags
    program_code: &str,
) -> Result<(ProgramInfo, Duration), SqlCompilationError> {
    let start = Instant::now();

    // These must always be the same, the SQL compiler should never pick up
    // a pipeline program which is not of its current platform version.
    if common_config.platform_version != platform_version {
        return Err(SqlCompilationError::SystemError(format!(
            "Platform version {platform_version} is not equal to current {}",
            common_config.platform_version
        )));
    }

    // Recreate working directory for the input/output of the SQL compiler
    let working_dir = config
        .working_dir()
        .join("sql-compilation")
        .join(format!("pipeline-{pipeline_id}"));
    recreate_dir(&working_dir)
        .await
        .map_err(|e| SqlCompilationError::SystemError(e.to_string()))?;

    // Write SQL code to file
    let input_sql_file_path = working_dir.join("program.sql");
    create_new_file_with_content(&input_sql_file_path, program_code).await?;

    // Create file where stdout will be written to
    let output_stdout_file_path = working_dir.join("stdout.log");
    let output_stdout_file = create_new_file(&output_stdout_file_path).await?;

    // Create file where stderr will be written to
    let output_stderr_file_path = working_dir.join("stderr.log");
    let output_stderr_file = create_new_file(&output_stderr_file_path).await?;

    // Outputs
    let output_json_schema_file_path = working_dir.join("schema.json");
    let output_rust_main_file_path = working_dir.join("main.rs");
    let output_rust_udf_stubs_file_path = working_dir.join("stubs.rs");

    // SQL compiler executable
    let sql_compiler_executable_file_path = Path::new(&config.sql_compiler_home)
        .join("SQL-compiler")
        .join("sql-to-dbsp");

    // Call executable with arguments
    //
    // In the future, it might be that flags can be passed to the SQL compiler through
    // the program_config field of the pipeline.
    let mut command = Command::new(sql_compiler_executable_file_path.clone());
    command
        .arg(input_sql_file_path.as_os_str())
        .arg("-js")
        .arg(output_json_schema_file_path.as_os_str())
        .arg("-o")
        .arg(output_rust_main_file_path.as_os_str())
        .arg("-i")
        .arg("-je")
        .arg("--alltables")
        .arg("--ignoreOrder")
        .arg("--unquotedCasing")
        .arg("lower")
        .stdin(Stdio::null())
        .stdout(Stdio::from(output_stdout_file.into_std().await))
        .stderr(Stdio::from(output_stderr_file.into_std().await));

    // Start process
    let mut process = command.spawn().map_err(|e| {
        SqlCompilationError::SystemError(
            CommonError::io_error(
                format!(
                    "running SQL compiler executable '{}'",
                    sql_compiler_executable_file_path.display()
                ),
                e,
            )
            .to_string(),
        )
    })?;

    // Wait for process to exit while regularly checking if the pipeline still exists
    // and has not had its program get updated
    let exit_status = loop {
        match process.try_wait() {
            Ok(exit_status) => match exit_status {
                None => {
                    if let Some(db) = db.clone() {
                        match db
                            .lock()
                            .await
                            .get_pipeline_by_id(tenant_id, pipeline_id)
                            .await
                        {
                            Ok(pipeline) => {
                                if pipeline.program_version != program_version {
                                    return Err(SqlCompilationError::Outdated);
                                }
                            }
                            Err(DBError::UnknownPipeline { .. }) => {
                                return Err(SqlCompilationError::Outdated);
                            }
                            Err(e) => {
                                error!("SQL compilation outdated check failed due to database error: {e}")
                                // As preemption check failing is not fatal, compilation will continue
                            }
                        }
                    }
                }
                Some(exit_status) => break exit_status,
            },
            Err(e) => {
                return Err(SqlCompilationError::SystemError(
                    CommonError::io_error("waiting for SQL compilation process".to_string(), e)
                        .to_string(),
                ));
            }
        }
        sleep(COMPILATION_CHECK_INTERVAL).await;
    };

    // Compilation is successful if the return exit code is present and zero
    if exit_status.success() {
        // Read schema.json
        let schema_str = read_file_content(&output_json_schema_file_path).await?;
        let schema: ProgramSchema = serde_json::from_str(&schema_str).map_err(|e| {
            SqlCompilationError::SystemError(
                CommonError::json_deserialization_error(
                    "schema.json from SQL compiler into ProgramSchema".to_string(),
                    e,
                )
                .to_string(),
            )
        })?;

        // Read main.rs
        let main_rust = read_file_content(&output_rust_main_file_path).await?;

        // Read stubs.rs
        let stubs = read_file_content(&output_rust_udf_stubs_file_path).await?;

        // Generate the program information
        match generate_program_info(schema, main_rust, stubs) {
            Ok(program_info) => Ok((program_info, start.elapsed())),
            Err(e) => {
                // The SQL compilation itself was successful, however the connectors JSON within the
                // WITH statement could not be deserialized into connectors
                let message = SqlCompilerMessage::new_from_connector_generation_error(e);
                Err(SqlCompilationError::SqlError(vec![message]))
            }
        }
    } else {
        match exit_status.code() {
            None => {
                // No exit status code present because the process was terminated by a signal
                Err(SqlCompilationError::TerminatedBySignal)
            }
            Some(exit_code) => {
                let stderr_str = fs::read_to_string(output_stderr_file_path.clone())
                    .await
                    .map_err(|e| {
                        SqlCompilationError::SystemError(
                            CommonError::io_error(
                                format!(
                                    "reading stderr file '{}'",
                                    output_stderr_file_path.display()
                                ),
                                e,
                            )
                            .to_string(),
                        )
                    })?;
                let messages: serde_json::Result<Vec<SqlCompilerMessage>> =
                    serde_json::from_str(&stderr_str);
                match messages {
                    Ok(messages) => {
                        Err(SqlCompilationError::SqlError(messages))
                    }
                    Err(e) => {
                        Err(SqlCompilationError::SystemError(
                            format!("SQL compiler process returned with non-zero exit status code ({exit_code}) and stderr which cannot be deserialized due to {e}:\n{stderr_str}")
                        ))
                    }
                }
            }
        }
    }
}

/// SQL compilation cleanup possible error outcomes.
enum SqlCompilationCleanupError {
    /// Database error occurred (e.g., lost connectivity).
    DBError(DBError),
    /// Filesystem problem occurred (e.g., I/O error)
    CommonError(CommonError),
}

impl From<DBError> for SqlCompilationCleanupError {
    fn from(value: DBError) -> Self {
        SqlCompilationCleanupError::DBError(value)
    }
}

impl From<CommonError> for SqlCompilationCleanupError {
    fn from(value: CommonError) -> Self {
        SqlCompilationCleanupError::CommonError(value)
    }
}

/// Cleans up the SQL compilation working directory by removing directories of
/// pipelines that no longer exist.
async fn cleanup_sql_compilation(
    config: &CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<(), SqlCompilationCleanupError> {
    trace!("Performing SQL cleanup...");

    // (1) Retrieve identifiers of all existing pipelines
    let existing_pipeline_ids: Vec<PipelineId> = db
        .lock()
        .await
        .list_pipeline_ids_across_all_tenants()
        .await?
        .iter()
        .map(|(_, pid)| *pid)
        .collect();

    // (2) Clean up directories of pipelines that are deleted
    let sql_pipelines_dir = config.working_dir().join("sql-compilation");
    if sql_pipelines_dir.is_dir() {
        cleanup_specific_directories(
            "SQL compilation directories",
            &sql_pipelines_dir,
            Arc::new(move |dirname: &str| {
                let spl: Vec<&str> = dirname.splitn(2, '-').collect();
                if spl.len() == 2 && spl[0] == "pipeline" {
                    if let Ok(uuid) = Uuid::parse_str(spl[1]) {
                        if existing_pipeline_ids.contains(&PipelineId(uuid)) {
                            CleanupDecision::Keep
                        } else {
                            CleanupDecision::Remove
                        }
                    } else {
                        // Also remove if it starts with "pipeline-" but is not followed by a valid UUID,
                        CleanupDecision::Remove
                    }
                } else {
                    CleanupDecision::Ignore
                }
            }),
            true,
        )
        .await?;
    }

    Ok(())
}
