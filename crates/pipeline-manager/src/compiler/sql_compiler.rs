use crate::common_error::CommonError;
use crate::compiler::util::{
    cleanup_specific_directories, cleanup_specific_files, crate_name_pipeline_base,
    crate_name_pipeline_globals, create_new_file, create_new_file_with_content,
    encode_dir_as_string, read_file_content, recreate_dir, CleanupDecision, ProcessGroupTerminator,
    UtilError,
};
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::{
    generate_program_info, RuntimeSelector, SqlCompilationInfo, SqlCompilerMessage,
};
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::validate_program_config;
use crate::db::types::version::Version;
use crate::error::source_error;
use crate::has_unstable_feature;
use feldera_ir::Dataflow;
use feldera_observability::ReqwestTracingExt;
use feldera_types::program_schema::ProgramSchema;
use futures_util::StreamExt;
use indoc::formatdoc;
use std::fs::Metadata;
use std::path::PathBuf;
use std::time::Instant;
use std::{process::Stdio, sync::Arc};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::{
    fs,
    process::Command,
    sync::Mutex,
    time::{sleep, Duration},
};
use tracing::{debug, error, info, trace, warn};
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
/// Sleeps inbetween ticks which affects the response time of SQL compilation.
/// This task cannot fail, and any internal errors are caught and written to log if need-be.
pub async fn sql_compiler_task(
    worker_id: usize,
    total_workers: usize,
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
                    SqlCompilationCleanupError::Database(e) => {
                        error!("SQL worker {worker_id}: compilation cleanup failed: database error occurred: {e}");
                    }
                    SqlCompilationCleanupError::Utility(e) => {
                        error!("SQL worker {worker_id}: compilation cleanup failed: filesystem operation error occurred: {e}");
                    }
                }
                unexpected_error = true;
            }
            last_cleanup = Some(Instant::now());
        }

        // Compile
        let result = attempt_end_to_end_sql_compilation(
            worker_id,
            total_workers,
            &common_config,
            &config,
            db.clone(),
        )
        .await;
        if let Err(e) = &result {
            match e {
                DBError::UnknownPipeline { pipeline_id } => {
                    debug!(
                        pipeline_id = %pipeline_id,
                        "SQL worker {worker_id}: compilation canceled: pipeline {pipeline_id} no longer exists"
                    );
                }
                DBError::OutdatedProgramVersion {
                    outdated_version,
                    latest_version,
                } => {
                    debug!("SQL worker {worker_id}: compilation canceled: pipeline program version ({outdated_version}) is outdated by latest ({latest_version})");
                }
                e => {
                    unexpected_error = true;
                    error!("SQL worker {worker_id}: compilation canceled: unexpected database error occurred: {e}");
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
pub(crate) async fn attempt_end_to_end_sql_compilation(
    worker_id: usize,
    total_workers: usize,
    common_config: &CommonConfig,
    config: &CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<bool, DBError> {
    trace!("SQL worker {worker_id}: Performing SQL compilation...");

    // (1) Reset any pipeline which is `CompilingSql` back to `Pending`
    db.lock()
        .await
        .clear_ongoing_sql_compilation_for_worker(
            &common_config.platform_version,
            worker_id,
            total_workers,
        )
        .await?;

    // (2) Find pipeline which needs SQL compilation
    let Some((tenant_id, pipeline)) = db
        .lock()
        .await
        .get_next_sql_compilation(&common_config.platform_version, worker_id, total_workers)
        .await?
    else {
        trace!("No pipeline found which needs SQL compilation");
        return Ok(false);
    };

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
        Some(pipeline.name.clone()),
        &pipeline.platform_version,
        pipeline.program_version,
        &pipeline.program_config,
        &pipeline.program_code,
    )
    .await;

    // (5) Update database that SQL compilation is finished
    match compilation_result {
        Ok((program_info, duration, compilation_info)) => {
            info!(
                pipeline_id = %pipeline.id,
                pipeline = %pipeline.name,
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
                    &compilation_info,
                    &program_info,
                )
                .await?;
        }
        Err(e) => match e {
            SqlCompilationError::NoLongerExists => {
                debug!(
                    pipeline_id = %pipeline.id,
                    pipeline = %pipeline.name,
                    "SQL compilation canceled: pipeline {} no longer exists",
                    pipeline.id,
                );
            }
            SqlCompilationError::Outdated => {
                debug!(
                    pipeline_id = %pipeline.id,
                    pipeline = %pipeline.name,
                    "SQL compilation canceled: pipeline {} (program version: {}) is outdated",
                    pipeline.id, pipeline.program_version,
                );
            }
            SqlCompilationError::TerminatedBySignal => {
                error!(
                    pipeline_id = %pipeline.id,
                    pipeline = %pipeline.name,
                    "SQL compilation interrupted: pipeline {} (program version: {}) compilation process was terminated by a signal",
                    pipeline.id, pipeline.program_version,
                );
            }
            SqlCompilationError::SqlError(compilation_info) => {
                db.lock()
                    .await
                    .transit_program_status_to_sql_error(
                        tenant_id,
                        pipeline.id,
                        pipeline.program_version,
                        &compilation_info,
                    )
                    .await?;
                info!(
                    pipeline_id = %pipeline.id,
                    pipeline = %pipeline.name,
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
                error!(
                    pipeline_id = %pipeline.id,
                    pipeline = %pipeline.name,
                    "SQL compilation failed: pipeline {} (program version: {}) due to system error:\n{}",
                    pipeline.id,
                    pipeline.program_version,
                    internal_system_error
                );
            }
        },
    }
    Ok(true)
}

/// SQL compilation possible error outcomes.
#[derive(Debug)]
pub enum SqlCompilationError {
    /// In the meanwhile the pipeline was deleted, as such the SQL
    /// compilation is no longer useful.
    NoLongerExists,
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
    SqlError(SqlCompilationInfo),
    /// General system problem occurred (e.g., I/O error)
    SystemError(String),
}

/// Common errors are system errors during SQL compilation.
impl From<CommonError> for SqlCompilationError {
    fn from(value: CommonError) -> Self {
        SqlCompilationError::SystemError(value.to_string())
    }
}

/// Utility errors are system errors during SQL compilation.
impl From<UtilError> for SqlCompilationError {
    fn from(value: UtilError) -> Self {
        SqlCompilationError::SystemError(value.to_string())
    }
}

/// Determines the path to the SQL compiler executable based on the runtime selector.
fn determine_sql_compiler_path(
    config: &CompilerConfig,
    runtime_selector: &RuntimeSelector,
) -> PathBuf {
    match runtime_selector {
        RuntimeSelector::Platform(_) => PathBuf::from(&config.sql_compiler_path),
        RuntimeSelector::Sha(sha) => config
            .working_dir()
            .join("sql-compilation")
            .join("jar-cache")
            .join(format!("sql2dbsp-jar-with-dependencies-{sha}.jar")),
        RuntimeSelector::Version(version) => config
            .working_dir()
            .join("sql-compilation")
            .join("jar-cache")
            .join(format!("sql2dbsp-jar-with-dependencies-{version}.jar")),
    }
}

async fn fetch_sql_compiler(
    config: &CompilerConfig,
    runtime_selector: &RuntimeSelector,
) -> Result<(), SqlCompilationError> {
    assert!(
        has_unstable_feature("runtime_version"),
        "This code-path is only enabled in unstable mode"
    );

    let jar_cache_dir = config
        .working_dir()
        .join("sql-compilation")
        .join("jar-cache");
    fs::create_dir_all(&jar_cache_dir).await.map_err(|e| SqlCompilationError::SystemError(format!(
        "Unable initialize JAR cache directory '{}': {}. If possible, fall-back to platform version by removing `runtime_version` in the program config.",
        jar_cache_dir.display(),
        e
    )))?;

    // Where the file will end up
    let jar_dest_final = determine_sql_compiler_path(config, runtime_selector);
    assert!(
        !jar_dest_final.exists(),
        "SQL compiler JAR file does not exist"
    );

    // e.g., sql2dbsp-jar-with-dependencies-$SHA.jar
    let jar_file_name = jar_dest_final.file_name().unwrap().to_str().unwrap();
    // The URL where we can download the JAR file from.
    let jar_cache_url = format!(
        "{base_url}{jar_file_name}",
        base_url = config.sql_compiler_cache_url
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| {
            SqlCompilationError::SystemError(format!(
                "Unable to initiate download of SQL-to-DBSP compiler: {} for selected runtime version '{}'. If possible, fall-back to platform version or change `runtime_version` in the program config.",
                e,
                runtime_selector
            ))
        })?;

    let response = client
        .get(&jar_cache_url)
        .with_sentry_tracing()
        .send()
        .await
        .map_err(|e| {
        SqlCompilationError::SystemError(format!(
            "Unable to fetch SQL-to-DBSP compiler at '{}': {}, source error: {}. If possible, fall-back to platform version or change `runtime_version` in the program config.",
            &jar_cache_url,
            e,
            source_error(&e)
        ))
    })?;

    // Make a temp file turn it into async file
    let tmp_jar_file = NamedTempFile::new_in(&jar_cache_dir).map_err(|e| {
        SqlCompilationError::SystemError(format!(
            "Unable to create temporary file in '{}' when downloading SQL-to-DBSP compiler: {}. If possible, fall-back to platform version or change `runtime_version` in the program config.",
            jar_cache_dir.display(),
            e
        ))
    })?;
    let (f, path) = tmp_jar_file.into_parts();
    let mut async_tmp_jar_file = fs::File::from_std(f);

    let response = response.error_for_status().map_err(|e| {
        SqlCompilationError::SystemError(format!(
            "Unable to download SQL-to-DBSP compiler from: {}. If possible, fall-back to platform version or change `runtime_version` in the program config.",
            e
        ))
    })?;

    let mut response_stream = response.bytes_stream();
    while let Some(chunk) = response_stream.next().await {
        let bytes = chunk.map_err(|e| SqlCompilationError::SystemError(format!(
            "Unable to read JAR from HTTP stream '{}': {}. If possible, fall-back to platform version or change `runtime_version` in the program config.",
            &jar_cache_url,
            e
        )))?;
        async_tmp_jar_file.write_all(&bytes).await.map_err(|e| {
            SqlCompilationError::SystemError(format!(
                "Unable to persist SQL-to-DBSP compiler at '{}': {}. If possible, fall-back to platform version or change `runtime_version` in the program config.",
                path.display(),
                e
            ))
        })?;
    }
    let tmp_jar_file = NamedTempFile::from_parts(async_tmp_jar_file.into_std(), path);

    // Rename to the JAR file that we'll use for compilation
    tmp_jar_file.persist(&jar_dest_final).map_err(|_e| {
        SqlCompilationError::SystemError(format!(
            "Unable to persist SQL-to-DBSP compiler at '{}'. If possible, fall-back to platform version or change `runtime_version` in the program config.",
            jar_dest_final.display(),
        ))
    })?.await;

    Ok(())
}

/// Performs the SQL compilation:
/// - Prepares a working directory for input and output
/// - Call the SQL-to-DBSP compiler executable via a process
/// - Returns the outcome from the output (namely, the [`ProgramInfo`] serialized as a JSON value)
#[allow(clippy::too_many_arguments)]
pub(crate) async fn perform_sql_compilation(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    db: Option<Arc<Mutex<StoragePostgres>>>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    pipeline_name: Option<String>,
    platform_version: &str,
    program_version: Version,
    program_config: &serde_json::Value,
    program_code: &str,
) -> Result<(serde_json::Value, Duration, SqlCompilationInfo), SqlCompilationError> {
    let start = Instant::now();

    // These must always be the same, the SQL compiler should never pick up
    // a pipeline program which is not of its current platform version.
    if common_config.platform_version != platform_version {
        return Err(SqlCompilationError::SystemError(format!(
            "Platform version {platform_version} is not equal to current {}",
            common_config.platform_version
        )));
    }

    // Program configuration
    // Might be used in the future to pass SQL compiler flags
    let program_config = validate_program_config(program_config, true).map_err(|error| {
        SqlCompilationError::SystemError(formatdoc! {"
                The program configuration:
                {program_config:#}

                ... is not valid due to: {error}.

                This indicates a backward-incompatible platform upgrade occurred.
                Update the 'program_config' field of the pipeline to resolve this.
            "})
    })?;

    let runtime_selector = program_config.runtime_version();
    assert!(has_unstable_feature("runtime_version") || runtime_selector.is_platform());
    let pipeline_name = pipeline_name.as_deref();
    info!(
        pipeline_id = %pipeline_id,
        pipeline = pipeline_name.unwrap_or(""),
        "SQL compilation started: pipeline {} (program version: {}{})",
        pipeline_id,
        program_version,
        if !runtime_selector.is_platform() {
            format!(", runtime version: {runtime_selector}")
        } else {
            "".to_string()
        }
    );

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
    let output_dataflow_file_path = working_dir.join("dataflow.json");
    let output_rust_directory_path = working_dir.join("rust");
    recreate_dir(&output_rust_directory_path)
        .await
        .map_err(|e| SqlCompilationError::SystemError(e.to_string()))?;
    let output_rust_udf_stubs_file_path = working_dir
        .join("rust")
        .join(crate_name_pipeline_globals(pipeline_id))
        .join("src")
        .join("stubs.rs");

    // SQL compiler executable
    let sql_compiler_executable_file_path = determine_sql_compiler_path(config, &runtime_selector);
    if has_unstable_feature("runtime_version") && !sql_compiler_executable_file_path.exists() {
        fetch_sql_compiler(config, &runtime_selector).await?;
        // Either executable exists now or we error'd out
        assert!(sql_compiler_executable_file_path.exists());
    }

    // Call executable with arguments
    //
    // In the future, it might be that flags can be passed to the SQL compiler through
    // the program_config field of the pipeline.
    let mut command = Command::new("java");
    command
        .arg("-jar")
        .arg(&sql_compiler_executable_file_path)
        .arg(input_sql_file_path.as_os_str())
        .arg("-js")
        .arg(output_json_schema_file_path.as_os_str())
        .arg("-o")
        .arg(output_rust_directory_path.as_os_str())
        .arg("--dataflow")
        .arg(output_dataflow_file_path.as_os_str())
        .arg("-i")
        .arg("-je")
        .arg("--alltables")
        .arg("--ignoreOrder")
        .arg("--crates") // Generate multiple crates instead of a single main.rs
        .arg(crate_name_pipeline_base(pipeline_id));
    #[cfg(feature = "feldera-enterprise")]
    command.arg("--enterprise");
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(output_stdout_file.into_std().await))
        .stderr(Stdio::from(output_stderr_file.into_std().await))
        // Setting it to zero sets the process group ID to the PID.
        // This is done to be able to kill any subprocesses that are spawned.
        .process_group(0);

    // Start process
    let mut process = command.spawn().map_err(|e| {
        SqlCompilationError::SystemError(
            CommonError::io_error(
                format!(
                    "running SQL compiler executable 'java -jar {}'",
                    sql_compiler_executable_file_path.display()
                ),
                e,
            )
            .to_string(),
        )
    })?;

    // Retrieve process group ID and create a terminator
    // which ends the group when going out of scope.
    let Some(process_group) = process.id() else {
        return Err(SqlCompilationError::SystemError(
            "unable to retrieve pid".to_string(),
        ));
    };
    let mut terminator = ProcessGroupTerminator::new("SQL compilation", process_group);

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
                            .get_pipeline_by_id_for_monitoring(tenant_id, pipeline_id)
                            .await
                        {
                            Ok(pipeline) => {
                                if pipeline.program_version != program_version {
                                    return Err(SqlCompilationError::Outdated);
                                }
                            }
                            Err(DBError::UnknownPipeline { .. }) => {
                                return Err(SqlCompilationError::NoLongerExists);
                            }
                            Err(e) => {
                                error!(
                                    pipeline_id = %pipeline_id,
                                    pipeline = pipeline_name.unwrap_or(""),
                                    "SQL compilation outdated check failed due to database error: {e}"
                                )
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

    // Once the process has exited, it is no longer needed to terminate its process group
    terminator.cancel();

    // Check presence of exit status code
    let Some(exit_code) = exit_status.code() else {
        // No exit status code present because the process was terminated by a signal
        return Err(SqlCompilationError::TerminatedBySignal);
    };

    // Extract the SQL compiler messages (includes warnings and errors)
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
    let messages: Vec<SqlCompilerMessage> = if stderr_str.is_empty() {
        vec![]
    } else {
        // TODO: the proper solution is to log this to a separate stream, this breaks in case
        // JVM ever logs a line containing `[` to stderr
        let find_json_start = stderr_str.find("[");
        match serde_json::from_str(&stderr_str[find_json_start.unwrap_or(0)..]) {
            Ok(messages) => messages,
            Err(e) => {
                if !exit_status.success() {
                    return Err(SqlCompilationError::SystemError(
                        format!("SQL compiler process returned with exit status code ({exit_code}) and stderr which cannot be deserialized due to {e}:\n{stderr_str}")
                    ));
                } else {
                    error!(
                        pipeline_id = %pipeline_id,
                        pipeline = pipeline_name.unwrap_or(""),
                        "Unable to parse SQL compiler response after successful compilation, warnings were not passed to client: {}",
                        stderr_str
                    );
                    vec![]
                }
            }
        }
    };
    let mut compilation_info = SqlCompilationInfo {
        exit_code,
        messages,
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

        // Read dataflow.json
        let dataflow_str = read_file_content(&output_dataflow_file_path).await?;
        let dataflow: Dataflow = serde_json::from_str(&dataflow_str).map_err(|e| {
            SqlCompilationError::SystemError(
                CommonError::json_deserialization_error(
                    "dataflow.json from SQL compiler into struct Dataflow".to_string(),
                    e,
                )
                .to_string(),
            )
        })?;

        // The base64-encoded gzipped tar archive of the Rust output directory
        let main_rust = encode_dir_as_string(&output_rust_directory_path)?;

        // Read stubs.rs
        let stubs = read_file_content(&output_rust_udf_stubs_file_path).await?;

        // Generate the program information
        match generate_program_info(schema, main_rust, stubs, Some(dataflow)) {
            Ok(program_info) => {
                let program_info = match serde_json::to_value(program_info) {
                    Ok(value) => value,
                    Err(error) => {
                        return Err(SqlCompilationError::SystemError(format!(
                            "Failed to serialize program information due to: {error}"
                        )));
                    }
                };
                Ok((program_info, start.elapsed(), compilation_info))
            }
            Err(e) => {
                // The SQL compilation itself was successful, however the connectors JSON within the
                // WITH statement could not be deserialized into connectors
                let message = SqlCompilerMessage::new_from_connector_generation_error(e);
                compilation_info.messages.push(message);
                Err(SqlCompilationError::SqlError(compilation_info))
            }
        }
    } else {
        Err(SqlCompilationError::SqlError(compilation_info))
    }
}

/// SQL compilation cleanup possible error outcomes.
#[derive(Debug)]
pub(crate) enum SqlCompilationCleanupError {
    /// Database error occurred (e.g., lost connectivity).
    Database(DBError),
    /// Utility problem occurred (e.g., I/O error)
    Utility(UtilError),
}

impl From<DBError> for SqlCompilationCleanupError {
    fn from(value: DBError) -> Self {
        SqlCompilationCleanupError::Database(value)
    }
}

impl From<UtilError> for SqlCompilationCleanupError {
    fn from(value: UtilError) -> Self {
        SqlCompilationCleanupError::Utility(value)
    }
}

/// Cleans up the SQL compilation working directory by removing directories of
/// pipelines that no longer exist.
pub(crate) async fn cleanup_sql_compilation(
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
                            CleanupDecision::Keep {
                                motivation: spl[1].to_string(),
                            }
                        } else {
                            CleanupDecision::Remove
                        }
                    } else {
                        // Also remove if it starts with "pipeline-" but is not followed by a valid UUID,
                        CleanupDecision::Remove
                    }
                } else if dirname == "jar-cache" {
                    CleanupDecision::Keep {
                        motivation: "JAR cache".to_string(),
                    }
                } else {
                    CleanupDecision::Ignore
                }
            }),
            true,
        )
        .await?;
    }

    // (3) Clean up JAR cache to make sure it does not grow unboundedly
    let jar_cache_dir = config
        .working_dir()
        .join("sql-compilation")
        .join("jar-cache");
    if jar_cache_dir.is_dir() {
        cleanup_specific_files(
            "SQL JAR cache",
            &jar_cache_dir,
            Arc::new(move |_jar_name: &str, metadata: Option<Metadata>| {
                // Get rid of JAR files that have not been accessed within the last week
                const MAX_AGE: Duration = Duration::from_secs(7*24*60*60);
                if let Some(metadata) = metadata {
                    match metadata.accessed() {
                        Ok(atime) => {
                            if let Ok(elapsed) = atime.elapsed() {
                                if elapsed < MAX_AGE {
                                    trace!("Keeping {_jar_name} because it was accessed within the last 7 days ({elapsed:?} ago)");
                                    CleanupDecision::Keep {
                                        motivation: "Accessed within the last week".to_string(),
                                    }
                                } else {
                                    CleanupDecision::Remove
                                }
                            } else {
                                warn!("Unable to determine access time for JAR file, your system clock may be set incorrectly.");
                                CleanupDecision::Ignore
                            }
                        }
                        Err(e) => {
                            debug!("Failed to get access time for JAR file: {:?}", e);
                            CleanupDecision::Ignore
                        }
                    }
                } else {
                    debug!("Failed to get metadata for JAR file");
                    CleanupDecision::Ignore
                }
            }),
            true,
            true,
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::auth::TenantRecord;
    use crate::compiler::test::{list_content_as_sorted_names, CompilerTest};
    use crate::compiler::util::{create_new_file, recreate_dir};
    use crate::db::types::program::ProgramStatus;
    use crate::db::types::utils::validate_program_info;
    use crate::db::types::version::Version;
    use feldera_types::config::TransportConfig;
    use feldera_types::program_schema::{SqlIdentifier, SqlType};
    use indoc::formatdoc;

    /// Tests the compilation of several of the most basic SQL programs succeeds.
    #[tokio::test]
    async fn basics() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        for program_code in [
            "",                            // Empty
            "CREATE TABLE t1 (val1 INT);", // One table
            "CREATE VIEW v1 AS SELECT 1;", // One view
            // One table and one view (unrelated)
            &formatdoc! {"
                CREATE TABLE t1 (val1 INT);
                CREATE VIEW v1 AS SELECT 1;
            "},
            // One table and one view (related)
            &formatdoc! {"
                CREATE TABLE t1 (val1 INT);
                CREATE VIEW v1 AS SELECT * FROM t1;
            "},
        ] {
            test.sql_compiler_tick().await;
            let pipeline_id = test
                .create_pipeline(tenant_id, "p1", "v0", program_code)
                .await;
            test.sql_compiler_tick().await;
            test.check_outcome_sql_compiled(tenant_id, pipeline_id, program_code)
                .await;
            test.delete_pipeline(tenant_id, pipeline_id, "p1").await;
            test.sql_compiler_tick().await;
            test.sql_compiler_check_is_empty().await;
            test.sql_compiler_tick().await;
        }
    }

    /// Tests the compilation a table and view with a large coverage of the SQL types from:
    /// https://docs.feldera.com/sql/types
    #[tokio::test]
    async fn type_coverage() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let program_code = &formatdoc! {r#"
            CREATE TYPE CUSTOM_TYPE AS (
                v1 INT, v2 VARBINARY
            );
            CREATE TABLE t_all (
                val_boolean BOOLEAN,
                val_tinyint TINYINT,
                val_smallint SMALLINT,
                val_integer INTEGER,
                val_bigint BIGINT,
                val_uuid UUID,
                val_decimal_p_s DECIMAL(2, 1),
                val_real REAL,
                val_double DOUBLE,
                val_varchar_n VARCHAR(3),
                val_char_n CHAR(4),
                val_varchar VARCHAR,
                val_binary_n BINARY(5),
                val_varbinary VARBINARY,
                val_time TIME,
                val_timestamp TIMESTAMP,
                val_date DATE,
                val_row ROW(l INT NULL, r VARCHAR),
                val_array INT ARRAY,
                val_map MAP<BIGINT, INT>,
                val_variant VARIANT,
                val_custom CUSTOM_TYPE
            );
            CREATE VIEW v_all AS SELECT * FROM t_all;
        "#};

        // Create and compile
        let pipeline_id = test
            .create_pipeline(tenant_id, "p1", "v0", program_code)
            .await;
        test.sql_compiler_tick().await;
        let pipeline_descr = test
            .check_outcome_sql_compiled(tenant_id, pipeline_id, program_code)
            .await;

        // Check the types of the table and view
        let program_info = validate_program_info(&pipeline_descr.program_info.unwrap()).unwrap();
        let table = program_info.schema.inputs.first().unwrap();
        assert_eq!(table.name, SqlIdentifier::new("t_all", false));
        let view = program_info.schema.outputs.get(1).unwrap();
        assert_eq!(view.name, SqlIdentifier::new("v_all", false));
        for relation in [table, view] {
            assert!(!relation.materialized);
            assert!(relation.properties.is_empty());
            assert_eq!(relation.fields.len(), 22);

            // BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, UUID
            assert_eq!(
                relation.field("val_boolean").unwrap().columntype.typ,
                SqlType::Boolean
            );
            assert_eq!(
                relation.field("val_tinyint").unwrap().columntype.typ,
                SqlType::TinyInt
            );
            assert_eq!(
                relation.field("val_smallint").unwrap().columntype.typ,
                SqlType::SmallInt
            );
            assert_eq!(
                relation.field("val_integer").unwrap().columntype.typ,
                SqlType::Int
            );
            assert_eq!(
                relation.field("val_bigint").unwrap().columntype.typ,
                SqlType::BigInt
            );
            assert_eq!(
                relation.field("val_uuid").unwrap().columntype.typ,
                SqlType::Uuid
            );
            // DECIMAL(p, s)
            let decimal_column_type = relation
                .field("val_decimal_p_s")
                .unwrap()
                .clone()
                .columntype;
            assert_eq!(decimal_column_type.typ, SqlType::Decimal);
            assert_eq!(decimal_column_type.precision, Some(2));
            assert_eq!(decimal_column_type.scale, Some(1));
            assert!(decimal_column_type.nullable);

            // REAL, DOUBLE
            assert_eq!(
                relation.field("val_real").unwrap().columntype.typ,
                SqlType::Real
            );
            assert_eq!(
                relation.field("val_double").unwrap().columntype.typ,
                SqlType::Double
            );

            // VARCHAR(n)
            let varchar_n_column_type = relation.field("val_varchar_n").unwrap().clone().columntype;
            assert_eq!(varchar_n_column_type.typ, SqlType::Varchar);
            assert_eq!(varchar_n_column_type.precision, Some(3));
            assert_eq!(varchar_n_column_type.scale, None);
            assert!(varchar_n_column_type.nullable);

            // CHAR(n)
            let char_n_column_type = relation.field("val_char_n").unwrap().clone().columntype;
            assert_eq!(char_n_column_type.typ, SqlType::Char);
            assert_eq!(char_n_column_type.precision, Some(4));
            assert_eq!(char_n_column_type.scale, None);
            assert!(char_n_column_type.nullable);

            // VARCHAR
            assert_eq!(
                relation.field("val_varchar").unwrap().columntype.typ,
                SqlType::Varchar
            );

            // BINARY(n)
            let binary_n_column_type = relation.field("val_binary_n").unwrap().clone().columntype;
            assert_eq!(binary_n_column_type.typ, SqlType::Binary);
            assert_eq!(binary_n_column_type.precision, Some(5));
            assert_eq!(binary_n_column_type.scale, None);
            assert!(binary_n_column_type.nullable);

            // VARBINARY, TIME, TIMESTAMP, DATE
            assert_eq!(
                relation.field("val_varbinary").unwrap().columntype.typ,
                SqlType::Varbinary
            );
            assert_eq!(
                relation.field("val_time").unwrap().columntype.typ,
                SqlType::Time
            );
            assert_eq!(
                relation.field("val_timestamp").unwrap().columntype.typ,
                SqlType::Timestamp
            );
            assert_eq!(
                relation.field("val_date").unwrap().columntype.typ,
                SqlType::Date
            );

            // ROW
            let row_column_type = relation.field("val_row").unwrap().clone().columntype;
            assert_eq!(row_column_type.typ, SqlType::Struct);
            let subfields = row_column_type.fields.unwrap();
            assert_eq!(subfields.len(), 2);
            assert_eq!(subfields[0].columntype.typ, SqlType::Int);
            assert!(subfields[0].columntype.nullable);
            assert_eq!(subfields[1].columntype.typ, SqlType::Varchar);
            assert!(!subfields[1].columntype.nullable);

            // ARRAY
            let array_column_type = relation.field("val_array").unwrap().clone().columntype;
            assert_eq!(array_column_type.typ, SqlType::Array);
            assert_eq!(array_column_type.component.unwrap().typ, SqlType::Int);

            // MAP
            let map_column_type = relation.field("val_map").unwrap().clone().columntype;
            assert_eq!(map_column_type.typ, SqlType::Map);
            assert_eq!(map_column_type.key.unwrap().typ, SqlType::BigInt);
            assert_eq!(map_column_type.value.unwrap().typ, SqlType::Int);

            // CUSTOM TYPE
            let custom_column_type = relation.field("val_custom").unwrap().clone().columntype;
            assert_eq!(custom_column_type.typ, SqlType::Struct);
            let subfields = custom_column_type.fields.unwrap();
            assert_eq!(subfields.len(), 2);
            assert_eq!(subfields[0].name.name(), "v1");
            assert_eq!(subfields[0].columntype.typ, SqlType::Int);
            assert!(subfields[0].columntype.nullable);
            assert_eq!(subfields[1].name.name(), "v2");
            assert_eq!(subfields[1].columntype.typ, SqlType::Varbinary);
            assert!(subfields[1].columntype.nullable);
        }

        // Clean up
        test.delete_pipeline(tenant_id, pipeline_id, "p1").await;
        test.sql_compiler_tick().await;
        test.sql_compiler_check_is_empty().await;
        test.sql_compiler_tick().await;
    }

    /// Tests whether tables/views are correctly marked as materialized when applicable.
    #[tokio::test]
    async fn materialized() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let program_code = &formatdoc! {r#"
            CREATE TABLE t1 (val INT);
            CREATE TABLE t2 (val INT) WITH ( 'materialized' = 'true' );
            CREATE TABLE t3 (val INT) WITH ( 'materialized' = 'false' );
            CREATE VIEW v1 AS SELECT * FROM t1;
            CREATE LOCAL VIEW v2 AS SELECT * FROM t1;
            CREATE MATERIALIZED VIEW v3 AS SELECT * FROM t1;
        "#};

        // Create and compile
        let pipeline_id = test
            .create_pipeline(tenant_id, "p1", "v0", program_code)
            .await;
        test.sql_compiler_tick().await;
        let pipeline_descr = test
            .check_outcome_sql_compiled(tenant_id, pipeline_id, program_code)
            .await;

        // Check materialized outcome
        let program_info = validate_program_info(&pipeline_descr.program_info.unwrap()).unwrap();
        assert_eq!(program_info.schema.inputs.len(), 3);
        for table in program_info.schema.inputs {
            match table.name.name().as_str() {
                "t1" => assert!(!table.materialized),
                "t2" => assert!(table.materialized),
                "t3" => assert!(!table.materialized),
                t => panic!("Unknown table: {t}"),
            }
        }
        assert_eq!(program_info.schema.outputs.len(), 3);
        for view in program_info.schema.outputs {
            match view.name.name().as_str() {
                "v1" => assert!(!view.materialized),
                // v2 is a LOCAL VIEW and should not be an output
                "v3" => assert!(view.materialized),
                "error_view" => assert!(!view.materialized),
                v => panic!("Unknown view: {v}"),
            }
        }
    }

    /// Tests whether compilation succeeds when an input connector is defined.
    #[tokio::test]
    async fn input_connector() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let program_code = &formatdoc! {r#"
            CREATE TABLE t1 (
                val INT
            ) WITH (
                'connectors' = '[{{
                    "name": "c1",
                    "transport": {{
                        "name": "datagen",
                        "config": {{
                            "plan": [{{
                                "rate": 1000,
                                "fields": {{
                                    "val": {{
                                        "range": [0, 1000],
                                        "strategy": "uniform"
                                    }}
                                }}
                            }}]
                        }}
                    }}
                }}]'
            )
        "#};

        // Compile
        let pipeline_id = test
            .create_pipeline(tenant_id, "p1", "v0", program_code)
            .await;
        test.sql_compiler_tick().await;

        // Check result
        let pipeline_descr = test
            .check_outcome_sql_compiled(tenant_id, pipeline_id, program_code)
            .await;
        let input_connectors = validate_program_info(&pipeline_descr.program_info.unwrap())
            .unwrap()
            .clone()
            .input_connectors;
        assert_eq!(input_connectors.len(), 1);
        let connector_config = input_connectors
            .get("t1.c1")
            .unwrap()
            .connector_config
            .clone();
        assert!(matches!(
            connector_config.transport,
            TransportConfig::Datagen(_)
        ));
    }

    /// Tests that SQL compiler recovers from an incorrect platform version.
    #[tokio::test]
    async fn recover_from_incorrect_platform_version() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let program_code = "";
        let pipeline_id = test
            .create_pipeline(tenant_id, "p1", "v1", program_code)
            .await;
        // Pipeline is detected to be of an incorrect platform version, which is updated, and compiled
        test.sql_compiler_tick().await;
        let pipeline_descr = test.get_pipeline(tenant_id, pipeline_id).await;
        assert_eq!(pipeline_descr.program_status, ProgramStatus::SqlCompiled);
        assert_eq!(pipeline_descr.program_version, Version(2));
        assert_eq!(pipeline_descr.platform_version, "v0");
    }

    /// Tests the compilation order which is generally first-come-first-serve.
    #[tokio::test]
    async fn compilation_order() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let program_code = "";
        let pipeline_id1 = test
            .create_pipeline(tenant_id, "p1", "v0", program_code)
            .await;
        let pipeline_id2 = test
            .create_pipeline(tenant_id, "p2", "v0", program_code)
            .await;
        test.sql_compiler_tick().await;
        assert_eq!(
            test.get_pipeline(tenant_id, pipeline_id1)
                .await
                .program_status,
            ProgramStatus::SqlCompiled
        );
        assert_eq!(
            test.get_pipeline(tenant_id, pipeline_id2)
                .await
                .program_status,
            ProgramStatus::Pending
        );
        let pipeline_id3 = test
            .create_pipeline(tenant_id, "p3", "v0", program_code)
            .await;
        test.sql_compiler_tick().await;
        assert_eq!(
            test.get_pipeline(tenant_id, pipeline_id2)
                .await
                .program_status,
            ProgramStatus::SqlCompiled
        );
        assert_eq!(
            test.get_pipeline(tenant_id, pipeline_id3)
                .await
                .program_status,
            ProgramStatus::Pending
        );
        test.sql_compiler_tick().await;
        assert_eq!(
            test.get_pipeline(tenant_id, pipeline_id3)
                .await
                .program_status,
            ProgramStatus::SqlCompiled
        );
    }

    /// Tests that compilation fails with invalid SQL.
    #[tokio::test]
    async fn invalid_sql() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let program_code = &formatdoc! {r#"
            This is not valid SQL.
        "#};
        let pipeline_id = test
            .create_pipeline(tenant_id, "p1", "v0", program_code)
            .await;
        test.sql_compiler_tick().await;
        let pipeline_descr = test.get_pipeline(tenant_id, pipeline_id).await;
        assert_eq!(pipeline_descr.program_status, ProgramStatus::SqlError);
        assert!(pipeline_descr
            .program_error
            .sql_compilation
            .is_some_and(|info| info.messages.len() == 1
                && info.messages[0].to_owned().error_type == "Error parsing SQL"));
    }

    /// Tests that compilation fails with an invalid connector.
    #[tokio::test]
    async fn invalid_connector() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let program_code = &formatdoc! {r#"
            CREATE TABLE t1 (
                val INT
            ) WITH (
                'connectors' = '["These are not valid connectors."]'
            )
        "#};
        let pipeline_id = test
            .create_pipeline(tenant_id, "p1", "v0", program_code)
            .await;
        test.sql_compiler_tick().await;
        let pipeline_descr = test.get_pipeline(tenant_id, pipeline_id).await;
        assert_eq!(pipeline_descr.program_status, ProgramStatus::SqlError);
        // First message is a warning about the connector missing a name
        // Second message is an error
        assert!(pipeline_descr
            .program_error
            .sql_compilation
            .is_some_and(|info| info.messages.len() == 2
                && info.messages[1].to_owned().error_type == "ConnectorGenerationError"));
    }

    /// Tests that the cleanup ignores files and directories that do not follow the pattern.
    #[tokio::test]
    async fn cleanup_ignore() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;

        // Compile two pipeline programs
        let pipeline_id1 = test.create_pipeline(tenant_id, "p1", "v0", "").await;
        let pipeline_id2 = test.create_pipeline(tenant_id, "p2", "v0", "").await;
        test.sql_compiler_tick().await;
        test.sql_compiler_tick().await;

        // Check directory content
        let content: Vec<String> = list_content_as_sorted_names(&test.sql_workdir).await;
        let mut expected = vec![
            format!("pipeline-{pipeline_id1}"),
            format!("pipeline-{pipeline_id2}"),
        ];
        expected.sort();
        assert_eq!(content, expected);

        // Create some other files and directories
        create_new_file(&test.sql_workdir.join("example.txt"))
            .await
            .unwrap();
        recreate_dir(&test.sql_workdir.join("example2"))
            .await
            .unwrap();
        recreate_dir(&test.sql_workdir.join("pipeline-does-not-exist"))
            .await
            .unwrap();

        // Delete pipeline 2
        test.delete_pipeline(tenant_id, pipeline_id2, "p2").await;
        test.sql_compiler_tick().await;

        // Check directory content afterward
        let content: Vec<String> = list_content_as_sorted_names(&test.sql_workdir).await;
        let mut expected = vec![
            "example.txt".to_string(),
            "example2".to_string(),
            format!("pipeline-{pipeline_id1}"),
        ];
        expected.sort();
        assert_eq!(content, expected);
    }
}
