use crate::common_error::CommonError;
use crate::compiler::rust_compiler::{FileDeliveryMode, FileUploadMetadata, deliver_program_info};
use crate::compiler::util::{
    CleanupDecision, ProcessGroupTerminator, UtilError, checksum_buffer,
    cleanup_specific_directories, cleanup_specific_files, crate_name_pipeline_base,
    crate_name_pipeline_globals, create_dir_if_not_exists, create_new_file,
    create_new_file_with_content, encode_dir_as_string, read_file_content, recreate_dir,
};
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::{
    RuntimeSelector, SqlCompilationInfo, SqlCompilerMessage, generate_program_info,
};
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::{validate_program_config, validate_program_info};
use crate::db::types::version::Version;
use crate::error::source_error;
use crate::has_unstable_feature;
use feldera_ir::Dataflow;
use futures_util::StreamExt;
use indoc::formatdoc;
use serde::{Deserialize, Serialize};
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
    time::{Duration, sleep},
};
use tracing::{debug, error, info, trace, warn};
use utoipa::ToSchema;
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
) -> Result<(), ()> {
    let mut last_cleanup: Option<Instant> = None;
    loop {
        let mut unexpected_error = false;

        // Clean up
        if last_cleanup.is_none() || last_cleanup.is_some_and(|ts| ts.elapsed() >= CLEANUP_INTERVAL)
        {
            if let Err(e) = cleanup_sql_compilation(&config, db.clone()).await {
                match e {
                    SqlCompilationCleanupError::Database(e) => {
                        error!(
                            "SQL worker {worker_id}: compilation cleanup failed: database error occurred: {e}"
                        );
                    }
                    SqlCompilationCleanupError::Utility(e) => {
                        error!(
                            "SQL worker {worker_id}: compilation cleanup failed: filesystem operation error occurred: {e}"
                        );
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
                        pipeline = "N/A",
                        "SQL worker {worker_id}: compilation canceled: pipeline no longer exists"
                    );
                }
                DBError::OutdatedProgramVersion {
                    outdated_version,
                    latest_version,
                } => {
                    debug!(
                        "SQL worker {worker_id}: compilation canceled: pipeline program version ({outdated_version}) is outdated by latest ({latest_version})"
                    );
                }
                e => {
                    unexpected_error = true;
                    error!(
                        "SQL worker {worker_id}: compilation canceled: unexpected database error occurred: {e}"
                    );
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
///    containing the output of the SQL compiler (inputs, outputs, `main.rs`, `stubs.rs`, etc.).
///    A Gen-2 program needs no Rust compilation, so its program info is delivered here and
///    it is set to `Success` directly, ready to run without entering the Rust compiler queue.
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
        SqlCompilationOutput::Full,
    )
    .await;

    // (5) Update database that SQL compilation is finished
    match compilation_result {
        Ok((program_info, duration, compilation_info)) => {
            info!(
                pipeline_id = %pipeline.id,
                pipeline = %pipeline.name,
                "SQL compilation success (program version: {}) (took {:.2}s)",
                pipeline.program_version,
                duration.as_secs_f64()
            );

            // The Gen-2 engine needs no Rust compilation: deliver its program info (which
            // carries the circuit IR) and mark the program ready to run, so it never
            // enters the serialized Rust compilation queue. Every other runtime moves
            // to SqlCompiled and waits for the Rust compiler.
            let is_gen2 = validate_program_config(&pipeline.program_config, true)
                .map(|config| config.runtime_version().is_gen2())
                .unwrap_or(false);
            if is_gen2 {
                match deliver_gen2_program_info(
                    common_config,
                    config,
                    pipeline.id,
                    pipeline.program_version,
                    &program_info,
                )
                .await
                {
                    Ok(program_info_integrity_checksum) => {
                        db.lock()
                            .await
                            .transit_program_status_to_success_no_binary(
                                tenant_id,
                                pipeline.id,
                                pipeline.program_version,
                                &compilation_info,
                                &program_info,
                                &program_info_integrity_checksum,
                            )
                            .await?;
                    }
                    Err(error) => {
                        error!(
                            pipeline_id = %pipeline.id,
                            pipeline = %pipeline.name,
                            "Gen-2 program info delivery failed (program version: {}): {error}",
                            pipeline.program_version
                        );
                        db.lock()
                            .await
                            .transit_program_status_to_system_error(
                                tenant_id,
                                pipeline.id,
                                pipeline.program_version,
                                &error,
                            )
                            .await?;
                    }
                }
            } else {
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
        }
        Err(e) => match e {
            SqlCompilationError::NoLongerExists => {
                debug!(
                    pipeline_id = %pipeline.id,
                    pipeline = %pipeline.name,
                    "SQL compilation canceled: pipeline no longer exists"
                );
            }
            SqlCompilationError::Outdated => {
                debug!(
                    pipeline_id = %pipeline.id,
                    pipeline = %pipeline.name,
                    "SQL compilation canceled: program version {} is outdated",
                    pipeline.program_version
                );
            }
            SqlCompilationError::TerminatedBySignal => {
                error!(
                    pipeline_id = %pipeline.id,
                    pipeline = %pipeline.name,
                    "SQL compilation interrupted: compilation process was terminated by a signal (program version: {})",
                    pipeline.program_version
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
                    "SQL compilation failed due to SQL errors (program version: {})",
                    pipeline.program_version
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
                    "SQL compilation failed due to system error (program version: {}): {internal_system_error}",
                    pipeline.program_version
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

/// How long a cached SQL compiler jar is retained after its last use.
pub(crate) const JAR_CACHE_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

/// Removes a cached SQL compiler jar that was not accessed within
/// [`JAR_CACHE_RETENTION`]; jars still read by compilations or validations
/// stay cached. Missing metadata or access times never remove.
pub(crate) fn decide_stale_jar(jar_name: &str, metadata: Option<Metadata>) -> CleanupDecision {
    let Some(metadata) = metadata else {
        debug!("Failed to get metadata for JAR file");
        return CleanupDecision::Ignore;
    };
    let atime = match metadata.accessed() {
        Ok(atime) => atime,
        Err(e) => {
            debug!("Failed to get access time for JAR file: {:?}", e);
            return CleanupDecision::Ignore;
        }
    };
    let Ok(elapsed) = atime.elapsed() else {
        warn!(
            "Unable to determine access time for JAR file, your system clock may be set incorrectly."
        );
        return CleanupDecision::Ignore;
    };
    if elapsed < JAR_CACHE_RETENTION {
        trace!(
            "Keeping {jar_name} because it was accessed within the retention window ({elapsed:?} ago)"
        );
        CleanupDecision::Keep {
            motivation: "Accessed within the retention window".to_string(),
        }
    } else {
        CleanupDecision::Remove
    }
}

/// Directory in which downloaded SQL compiler jars for non-platform runtime
/// versions are cached.
pub(crate) fn jar_cache_dir(config: &CompilerConfig) -> PathBuf {
    config
        .working_dir()
        .join("sql-compilation")
        .join("jar-cache")
}

/// Determines the path to the SQL compiler executable based on the runtime selector.
fn determine_sql_compiler_path(
    config: &CompilerConfig,
    runtime_selector: &RuntimeSelector,
) -> PathBuf {
    match runtime_selector {
        // The Gen-2 engine consumes the platform SQL compiler's program info; only the
        // Rust half of the compilation differs.
        RuntimeSelector::Platform(_) | RuntimeSelector::Gen2 => {
            PathBuf::from(&config.sql_compiler_path)
        }
        RuntimeSelector::Sha(sha) => {
            jar_cache_dir(config).join(format!("sql2dbsp-jar-with-dependencies-{sha}.jar"))
        }
        RuntimeSelector::Version(version) => {
            jar_cache_dir(config).join(format!("sql2dbsp-jar-with-dependencies-{version}.jar"))
        }
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

    let jar_cache_dir = jar_cache_dir(config);
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
        .build()
        .map_err(|e| {
            SqlCompilationError::SystemError(format!(
                "Unable to initiate download of SQL-to-DBSP compiler: {}, source error: {}, for selected runtime version '{}'. If possible, fall-back to platform version or change `runtime_version` in the program config.",
                e,
                source_error(&e),
                runtime_selector
            ))
        })?;

    let response = client
        .get(&jar_cache_url)
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
            "Unable to download SQL-to-DBSP compiler from: {}, source error: {}. If possible, fall-back to platform version or change `runtime_version` in the program config.",
            e,
            source_error(&e)
        ))
    })?;

    let mut response_stream = response.bytes_stream();

    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(10), response_stream.next()).await.map_err(|_e| {
            SqlCompilationError::SystemError(format!(
                "Timed out while waiting for the next HTTP response chunk when downloading the SQL-to-DBSP compiler from '{}'.
                This might be due to slow network, please retry compilation; alternatively, fall back to the platform compiler by removing or changing `runtime_version` in the program config.",
                &jar_cache_url,
            ))
        })?;

        match chunk {
            Some(chunk) => {
                let bytes = chunk.map_err(|e| SqlCompilationError::SystemError(format!(
                    "Unable to read JAR from HTTP stream '{}': {}, source error: {}. If possible, fall-back to platform version or change `runtime_version` in the program config.",
                    &jar_cache_url,
                    e,
                    source_error(&e)
                )))?;
                async_tmp_jar_file.write_all(&bytes).await.map_err(|e| {
                    SqlCompilationError::SystemError(format!(
                        "Unable to persist SQL-to-DBSP compiler at '{}': {}. If possible, fall-back to platform version or change `runtime_version` in the program config.",
                        path.display(),
                        e
                    ))
                })?;
            }
            None => break,
        }
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

/// Selects what SQL compilation produces.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SqlCompilationOutput {
    /// Full output including the generated Rust crates (for the Rust compiler).
    Full,
    /// IR only: schema, dataflow, and connectors. Skips packaging the generated
    /// Rust, which is not needed to compute pipeline diffs.
    IrOnly,
}

/// Root directory for ephemeral (IR-only) SQL compilations. It is wiped on
/// compiler startup so that any directory orphaned by a crash mid-compile is
/// reaped; `cleanup_sql_compilation` deliberately ignores it, as it only manages
/// `pipeline-<uuid>` directories.
pub(crate) fn ephemeral_compilation_dir(config: &CompilerConfig) -> PathBuf {
    config
        .working_dir()
        .join("sql-compilation")
        .join("ephemeral")
}

/// Delivers the Gen-2 program info artifact so the runner can fetch its circuit
/// IR. The Gen-2 engine builds no binary, so the artifact is named by its own integrity
/// checksum; the runner rebuilds the same name. Returns that checksum. The SQL
/// compiler completes a Gen-2 program without Rust compilation.
async fn deliver_gen2_program_info(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    pipeline_id: PipelineId,
    program_version: Version,
    program_info_value: &serde_json::Value,
) -> Result<String, String> {
    let program_info = validate_program_info(program_info_value)
        .map_err(|error| format!("Gen-2 program info is not valid: {error}"))?
        .to_pipeline_config_program_info();
    let program_info_str = serde_json::to_string(&program_info)
        .map_err(|e| format!("failed to serialize Gen-2 program info: {e}"))?;
    let program_info_integrity_checksum = checksum_buffer(program_info_str.as_bytes())
        .await
        .map_err(|e| format!("failed to checksum Gen-2 program info: {e:?}"))?;

    // The compiler's HTTP server serves program info artifacts from this directory,
    // so deliver here for the runner's program_info_url to resolve.
    let pipeline_binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries");
    create_dir_if_not_exists(&pipeline_binaries_dir)
        .await
        .map_err(|e| format!("failed to create pipeline-binaries directory: {e:?}"))?;

    let program_info_metadata = FileUploadMetadata {
        pipeline_id,
        program_version,
        source_checksum: program_info_integrity_checksum.clone(),
        integrity_checksum: program_info_integrity_checksum.clone(),
    };
    deliver_program_info(
        common_config,
        &FileDeliveryMode::from_config(config),
        &program_info_str,
        &program_info_metadata,
        &pipeline_binaries_dir,
    )
    .await
    .map_err(|e| format!("failed to deliver Gen-2 program info: {e:?}"))?;
    Ok(program_info_integrity_checksum)
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
    output: SqlCompilationOutput,
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
    let use_platform_compiler =
        program_config.use_platform_compiler && !runtime_selector.is_platform();
    assert!(has_unstable_feature("runtime_version") || runtime_selector.is_platform());
    let pipeline_name = pipeline_name.as_deref().unwrap_or("N/A");
    info!(
        pipeline_id = %pipeline_id,
        pipeline = pipeline_name,
        "SQL compilation started (program version: {}{}{})",
        program_version,
        if !runtime_selector.is_platform() {
            format!(", runtime version: {runtime_selector}")
        } else {
            "".to_string()
        },
        if use_platform_compiler {
            ", using platform compiler"
        } else {
            ""
        }
    );

    // Recreate working directory for the input/output of the SQL compiler.
    // IR-only (ephemeral) compiles run under a dedicated `ephemeral/` root (wiped
    // on compiler startup); see `ephemeral_compilation_dir`.
    let working_dir = match output {
        SqlCompilationOutput::Full => config
            .working_dir()
            .join("sql-compilation")
            .join(format!("pipeline-{pipeline_id}")),
        SqlCompilationOutput::IrOnly => {
            ephemeral_compilation_dir(config).join(pipeline_id.to_string())
        }
    };
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
    let output_jit_file_path = working_dir.join("jit.json");
    let output_rust_directory_path = working_dir.join("rust");
    // The Gen-2 engine runs the circuit from the JIT IR and skips the Rust half of
    // compilation, so the Rust output tree is only prepared for other runtimes.
    if !runtime_selector.is_gen2() {
        recreate_dir(&output_rust_directory_path.join("crates"))
            .await
            .map_err(|e| SqlCompilationError::SystemError(e.to_string()))?;
    }
    let output_rust_udf_stubs_file_path = working_dir
        .join("rust")
        .join("crates")
        .join(crate_name_pipeline_globals(pipeline_id))
        .join("src")
        .join("stubs.rs");

    // SQL compiler executable
    // When use_platform_compiler is set with a non-platform runtime version, use
    // the platform's SQL compiler JAR directly, bypassing the per-version download.
    let sql_compiler_executable_file_path = if use_platform_compiler {
        PathBuf::from(&config.sql_compiler_path)
    } else {
        determine_sql_compiler_path(config, &runtime_selector)
    };
    if !use_platform_compiler
        && has_unstable_feature("runtime_version")
        && !sql_compiler_executable_file_path.exists()
    {
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
        .arg("--dataflow")
        .arg(output_dataflow_file_path.as_os_str())
        .arg("-i")
        .arg("-je")
        .arg("--alltables")
        .arg("--ignoreOrder");
    if runtime_selector.is_gen2() {
        // The Gen-2 engine consumes the JIT circuit IR and skips Rust codegen. `--jit`
        // emits the circuit IR to stdout (it is incompatible with `--crates`),
        // captured into jit.json below.
        command.arg("--jit");
    } else {
        let runtime_crates_path = runtime_selector.runtime_sources(config);
        command
            .arg("-o")
            .arg(output_rust_directory_path.as_os_str())
            .arg("--runtime")
            .arg(runtime_crates_path)
            .arg("--crates") // Generate multiple crates instead of a single main.rs
            .arg(crate_name_pipeline_base(pipeline_id));
    }
    #[cfg(feature = "feldera-enterprise")]
    command.arg("--enterprise");
    // The Gen-2 engine captures the JIT circuit IR that `--jit` writes to stdout into
    // jit.json; every other runtime sends stdout to stdout.log.
    let stdout_sink = if runtime_selector.is_gen2() {
        create_new_file(&output_jit_file_path).await?
    } else {
        output_stdout_file
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_sink.into_std().await))
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
                                    pipeline = pipeline_name,
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
                    return Err(SqlCompilationError::SystemError(format!(
                        "SQL compiler process returned with exit status code ({exit_code}) and stderr which cannot be deserialized due to {e}:\n{stderr_str}"
                    )));
                } else {
                    error!(
                        pipeline_id = %pipeline_id,
                        pipeline = pipeline_name,
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
        let schema: serde_json::Value = serde_json::from_str(&schema_str).map_err(|e| {
            SqlCompilationError::SystemError(
                CommonError::json_deserialization_error(
                    "schema.json from SQL compiler".to_string(),
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

        // For IR-only compilation (used to compute diffs), skip packaging the
        // generated Rust: only the schema, dataflow, and connectors are needed.
        let (main_rust, stubs) = if runtime_selector.is_gen2() {
            // The Gen-2 engine skips Rust codegen, so there is no generated Rust to package.
            (String::new(), String::new())
        } else {
            match output {
                SqlCompilationOutput::Full => {
                    // The base64-encoded gzipped tar archive of the Rust output directory
                    let main_rust = encode_dir_as_string(&output_rust_directory_path)?;
                    // Read stubs.rs
                    let stubs = read_file_content(&output_rust_udf_stubs_file_path).await?;
                    (main_rust, stubs)
                }
                SqlCompilationOutput::IrOnly => (String::new(), String::new()),
            }
        };

        // For Gen-2, read back the JIT circuit IR the compiler emitted via
        // `--jit`.
        let circuit_ir = if runtime_selector.is_gen2() {
            let jit_str = read_file_content(&output_jit_file_path).await?;
            let jit: serde_json::Value = serde_json::from_str(&jit_str).map_err(|e| {
                SqlCompilationError::SystemError(
                    CommonError::json_deserialization_error(
                        "jit.json from SQL compiler".to_string(),
                        e,
                    )
                    .to_string(),
                )
            })?;
            Some(jit)
        } else {
            None
        };

        // Generate the program information
        match generate_program_info(schema, main_rust, stubs, Some(dataflow), circuit_ir) {
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

/// Request body for the compiler's `/validate_program` endpoint.
///
/// Carries a SQL program and its configuration to validate, optionally returning
/// the IR, without creating a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProgramValidationRequest {
    /// Program configuration as a JSON value (includes the runtime version).
    pub program_config: serde_json::Value,
    /// SQL program code to validate.
    pub program_code: String,
    /// Include the program IR (dataflow) in the response. When `false`, only the
    /// schema and connectors are returned.
    pub ir: bool,
}

/// Outcome of validating a SQL program.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) enum ValidateProgramResponse {
    /// Validation succeeded; `program_info` is the serialized `ProgramInfo` with
    /// the Rust artifacts omitted (and the dataflow omitted unless `ir` was set).
    Success { program_info: serde_json::Value },
    /// The SQL program failed to compile.
    SqlError { info: SqlCompilationInfo },
    /// A system error prevented validation (e.g., the runtime-specific SQL
    /// compiler could not be downloaded).
    SystemError { error: String },
}

/// Validate `program_code` by running the SQL compiler, without creating a
/// pipeline or building the Rust binary. Returns the derived schema and
/// connectors, and, when `ir` is `true`, the program IR (dataflow).
///
/// Runs the SQL compiler in a throwaway working directory using a synthetic
/// pipeline id and no database connection, then removes the directory. A
/// non-platform runtime version is downloaded on demand and requires the
/// `runtime_version` unstable feature.
pub(crate) async fn validate_program(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    program_config: &serde_json::Value,
    program_code: &str,
    ir: bool,
) -> ValidateProgramResponse {
    // Reject a custom runtime version when the feature is disabled instead of
    // silently falling back to the platform runtime, which would produce a
    // misleading result.
    match validate_program_config(program_config, true) {
        Ok(validated) => {
            if validated
                .runtime_version
                .as_ref()
                .is_some_and(|selector| !selector.is_platform())
                && !has_unstable_feature("runtime_version")
            {
                return ValidateProgramResponse::SystemError {
                    error: "A custom runtime version was requested, but this Feldera instance does not have the 'runtime_version' feature enabled.".to_string(),
                };
            }
        }
        Err(e) => {
            return ValidateProgramResponse::SystemError {
                error: format!("Invalid program configuration: {e}"),
            };
        }
    }

    let synthetic_id = PipelineId(Uuid::now_v7());
    let result = perform_sql_compilation(
        common_config,
        config,
        None,
        TenantId(Uuid::nil()),
        synthetic_id,
        None,
        &common_config.platform_version,
        Version(1),
        program_config,
        program_code,
        SqlCompilationOutput::IrOnly,
    )
    .await;

    // Remove the throwaway working directory (best effort). Any directory left
    // behind by a crash before this point is reaped when the ephemeral root is
    // wiped on the next compiler startup.
    let working_dir = ephemeral_compilation_dir(config).join(synthetic_id.to_string());
    let _ = fs::remove_dir_all(&working_dir).await;

    match result {
        Ok((mut program_info, _duration, _info)) => {
            if let Some(object) = program_info.as_object_mut() {
                // The generated Rust and UDF stubs are build artifacts, not part
                // of a validation result; always drop them.
                object.remove("main_rust");
                object.remove("udf_stubs");
                // The IR (dataflow) is only returned on request; most callers
                // just want to know whether the program is valid.
                if !ir {
                    object.insert("dataflow".to_string(), serde_json::Value::Null);
                }
            }
            ValidateProgramResponse::Success { program_info }
        }
        Err(SqlCompilationError::SqlError(info)) => ValidateProgramResponse::SqlError { info },
        Err(SqlCompilationError::SystemError(error)) => {
            ValidateProgramResponse::SystemError { error }
        }
        Err(SqlCompilationError::TerminatedBySignal) => ValidateProgramResponse::SystemError {
            error: "SQL compilation was terminated by a signal".to_string(),
        },
        Err(SqlCompilationError::NoLongerExists | SqlCompilationError::Outdated) => {
            ValidateProgramResponse::SystemError {
                error: "SQL compilation was unexpectedly preempted".to_string(),
            }
        }
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
            Arc::new(move |dirname: &str, _metadata: Option<Metadata>| {
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
            false,
        )
        .await?;
    }

    // (3) Clean up JAR cache to make sure it does not grow unboundedly
    let jar_cache_dir = jar_cache_dir(config);
    if jar_cache_dir.is_dir() {
        cleanup_specific_files(
            "SQL JAR cache",
            &jar_cache_dir,
            Arc::new(decide_stale_jar),
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

    /// A jar unaccessed past the retention window is removed, a recently
    /// accessed one is kept, and missing metadata never removes.
    #[test]
    fn stale_jar_decision() {
        use crate::compiler::sql_compiler::{JAR_CACHE_RETENTION, decide_stale_jar};
        use crate::compiler::util::CleanupDecision;
        let tempdir = tempfile::tempdir().unwrap();
        let jar_path = tempdir.path().join("a.jar");
        std::fs::write(&jar_path, b"jar").unwrap();
        let recent = std::fs::metadata(&jar_path).unwrap();
        assert!(matches!(
            decide_stale_jar("a.jar", Some(recent)),
            CleanupDecision::Keep { .. }
        ));
        let old_time = std::time::SystemTime::now() - JAR_CACHE_RETENTION - JAR_CACHE_RETENTION;
        let times = std::fs::FileTimes::new()
            .set_accessed(old_time)
            .set_modified(old_time);
        std::fs::File::options()
            .write(true)
            .open(&jar_path)
            .unwrap()
            .set_times(times)
            .unwrap();
        let old = std::fs::metadata(&jar_path).unwrap();
        assert_eq!(
            decide_stale_jar("a.jar", Some(old)),
            CleanupDecision::Remove
        );
        assert_eq!(decide_stale_jar("a.jar", None), CleanupDecision::Ignore);
    }

    use crate::compiler::test::{CompilerTest, list_content_as_sorted_names};
    use crate::compiler::util::{create_new_file, recreate_dir};
    use crate::db::types::program::ProgramStatus;
    use crate::db::types::utils::validate_program_info;
    use crate::db::types::version::Version;
    use feldera_types::config::TransportConfig;
    use feldera_types::program_schema::{
        ProgramSchema, ProgramSchemaPropertiesOnly, SqlIdentifier, SqlType,
    };
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
            test.check_outcome_sql_compiled(tenant_id, pipeline_id, program_code, true)
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
                val_timestampTz TIMESTAMP WITH TIME ZONE,
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
            .check_outcome_sql_compiled(tenant_id, pipeline_id, program_code, false)
            .await;

        // Check the types of the table and view
        let program_info = validate_program_info(&pipeline_descr.program_info.unwrap()).unwrap();

        let program_schema: ProgramSchema =
            serde_json::from_value(program_info.schema.clone()).unwrap();

        let table = program_schema.inputs.first().unwrap();
        assert_eq!(table.name, SqlIdentifier::new("t_all", false));
        let view = program_schema.outputs.get(1).unwrap();
        assert_eq!(view.name, SqlIdentifier::new("v_all", false));
        for relation in [table, view] {
            assert!(!relation.materialized);
            assert!(relation.properties.is_empty());
            assert_eq!(relation.fields.len(), 23);

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
                relation.field("val_timestampTz").unwrap().columntype.typ,
                SqlType::TimestampTz
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

        // Program schema only with properties
        let program_schema_properties_only: ProgramSchemaPropertiesOnly =
            serde_json::from_value(program_info.schema.clone()).unwrap();

        // Table
        let table_properties_only = program_schema_properties_only.inputs.first().unwrap();
        assert_eq!(table_properties_only.name, table.name);
        assert_eq!(table_properties_only.properties, table.properties);

        // View
        let view_properties_only = program_schema_properties_only.outputs.get(1).unwrap();
        assert_eq!(view_properties_only.name, view.name);
        assert_eq!(view_properties_only.properties, view.properties);

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
            .check_outcome_sql_compiled(tenant_id, pipeline_id, program_code, false)
            .await;

        // Check materialized outcome
        let program_info = validate_program_info(&pipeline_descr.program_info.unwrap()).unwrap();
        let program_schema: ProgramSchema =
            serde_json::from_value(program_info.schema.clone()).unwrap();
        assert_eq!(program_schema.inputs.len(), 3);
        for table in program_schema.inputs {
            match table.name.name().as_str() {
                "t1" => assert!(!table.materialized),
                "t2" => assert!(table.materialized),
                "t3" => assert!(!table.materialized),
                t => panic!("Unknown table: {t}"),
            }
        }
        assert_eq!(program_schema.outputs.len(), 3);
        for view in program_schema.outputs {
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
            .check_outcome_sql_compiled(tenant_id, pipeline_id, program_code, false)
            .await;
        let input_connectors = validate_program_info(&pipeline_descr.program_info.clone().unwrap())
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

        // Program schema only with properties: check properties
        let program_schema_properties_only: ProgramSchemaPropertiesOnly =
            serde_json::from_value(pipeline_descr.program_info.unwrap()["schema"].clone()).unwrap();
        let table_properties_only = program_schema_properties_only.inputs.first().unwrap();
        assert_eq!(table_properties_only.name, "t1");
        assert_eq!(table_properties_only.properties.len(), 1);
        assert!(table_properties_only.properties.contains_key("connectors"));
        assert!(
            table_properties_only.properties["connectors"]
                .value
                .contains("\"name\": \"c1\"")
        );
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
        assert!(
            pipeline_descr
                .program_error
                .sql_compilation
                .is_some_and(|info| info.messages.len() == 1
                    && info.messages[0].to_owned().error_type == "Error parsing SQL")
        );
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
        assert!(
            pipeline_descr
                .program_error
                .sql_compilation
                .is_some_and(|info| info.messages.len() == 2
                    && info.messages[1].to_owned().error_type == "ConnectorGenerationError")
        );
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
