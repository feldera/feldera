use crate::common_error::CommonError;
use crate::compiler::util::{
    cleanup_specific_directories, cleanup_specific_files, copy_file, create_new_file,
    create_new_file_with_content, read_file_content, read_file_content_bytes, recreate_dir,
    recreate_file_with_content, truncate_sha256_checksum, CleanupDecision,
};
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::{CompilationProfile, ProgramConfig};
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::{validate_program_config, validate_program_info};
use crate::db::types::version::Version;
use chrono::{DateTime, Utc};
use indoc::formatdoc;
use log::{debug, error, info, trace, warn};
use openssl::sha;
use openssl::sha::sha256;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::time::{Instant, SystemTime};
use std::{process::Stdio, sync::Arc};
use tokio::{
    fs,
    process::Command,
    sync::Mutex,
    time::{sleep, Duration},
};

/// The frequency at which the compiler polls the database for new Rust compilation requests.
/// It balances resource consumption due to polling and a fast Rust compilation response.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Minimum frequency at which the compiler polls the database.
/// This minimum is a preventative measure to avoid the Rust compiler
/// from ever polling the database in an uninterrupted loop.
const POLL_INTERVAL_MINIMUM: Duration = Duration::from_millis(25);

/// The poll frequency when an unexpected database error occurred.
/// This is set relatively long to not flood the logs when
/// for instance the database becomes temporarily unreachable.
const POLL_ERROR_INTERVAL: Duration = Duration::from_secs(30);

/// The frequency at which during Rust compilation it is checked whether
/// the process has finished and as well whether it needs to be canceled
/// if the program is outdated.
const COMPILATION_CHECK_INTERVAL: Duration = Duration::from_millis(250);

/// The frequency at which Rust cleanup is performed.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Minimum time between when the Rust cleanup has detected a project with a certain source checksum
/// to be no longer used until it is actually cleaned up. It is a minimum, as cleanup both happens
/// at an interval, and as well is interchanged with compilation which can take a significant amount
/// of time. Note that if the cleanup mechanism is unable to write its state file, the retention might
/// be shorter than this under specific circumstances (e.g., a checksum is cleaned up but not written
/// it's cleaned up, and subsequently a pipeline is created with the checksum and immediately deleted,
/// resulting in the old retention expiration timestamp being used when cleanup then occurs).
const CLEANUP_RETENTION: Duration = Duration::from_secs(900);

/// Rust compilation task that wakes up periodically.
/// Sleeps inbetween ticks which affects the response time of Rust compilation.
/// Note that the logic in this task assumes only one is run at a time.
/// This task cannot fail, and any internal errors are caught and written to log if need-be.
pub async fn rust_compiler_task(
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
            if let Err(e) = cleanup_rust_compilation(&config, db.clone()).await {
                match e {
                    RustCompilationCleanupError::DBError(e) => {
                        error!("Rust compilation cleanup failed: database error occurred: {e}");
                    }
                    RustCompilationCleanupError::CommonError(e) => {
                        error!("Rust compilation cleanup failed: filesystem error occurred: {e}");
                    }
                }
                unexpected_error = true;
            }
            last_cleanup = Some(Instant::now());
        }

        // Compile
        let result = attempt_end_to_end_rust_compilation(&common_config, &config, db.clone()).await;
        if let Err(e) = &result {
            match e {
                DBError::UnknownPipeline { pipeline_id } => {
                    debug!("Rust compilation canceled: pipeline {pipeline_id} no longer exists");
                }
                DBError::OutdatedProgramVersion {
                    outdated_version,
                    latest_version,
                } => {
                    debug!("Rust compilation canceled: pipeline program version ({outdated_version}) is outdated by latest ({latest_version})");
                }
                e => {
                    unexpected_error = true;
                    error!("Rust compilation canceled: unexpected database error occurred: {e}");
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

/// Performs Rust compilation:
/// 1. Reset in the database any pipeline with `program_status` of `CompilingRust` back to
///    `SqlCompiled` if they are of the current `platform_version`. Any pipeline with
///    `program_status` of `SqlCompiled` or `CompilingRust` with a non-current `platform_version`
///    will have it updated to current and its `program_status` set back to `Pending`.
/// 2. Queries the database for a pipeline which has `program_status` of `SqlCompiled` the longest
/// 3. Updates pipeline database `program_status` to `CompilingRust`
/// 4. Performs Rust compilation on `program_info.main`, `udf_rust` and `udf_toml`, configured
///    with `program_config`
/// 5. Upon completion, the compilation status is set to `Success` with the `program_binary_url`
///    containing the URL where the binary can be retrieved from
///
/// Note that this function assumes it runs in isolation, and as such at the beginning resets
/// any lingering pipelines that have `CompilingRust` status to `SqlCompiled`. This recovers from
/// if the compiler was interrupted (e.g., it was unexpectedly terminated) or a database
/// operation failed.
///
/// Returns with `Ok(true)` if there was an attempt to compile the Rust of a pipeline.
/// It does not necessarily mean the compilation was a success.
/// Returns with `Ok(false)` if no pipeline is found for which to compile Rust.
/// Returns with `Err(...)` if a database operation fails, e.g., due to:
/// - The pipeline no longer exists
/// - The pipeline program is detected to be updated (it became outdated)
/// - The database cannot be reached
async fn attempt_end_to_end_rust_compilation(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<bool, DBError> {
    trace!("Performing Rust compilation...");

    // (1) Reset any pipeline which is `CompilingRust` back to `SqlCompiled`
    db.lock()
        .await
        .clear_ongoing_rust_compilation(&common_config.platform_version)
        .await?;

    // (2) Find pipeline which needs Rust compilation
    let Some((tenant_id, pipeline)) = db
        .lock()
        .await
        .get_next_rust_compilation(&common_config.platform_version)
        .await?
    else {
        trace!("No pipeline found which needs Rust compilation");
        return Ok(false);
    };

    // (3) Update database that Rust compilation is ongoing
    db.lock()
        .await
        .transit_program_status_to_compiling_rust(tenant_id, pipeline.id, pipeline.program_version)
        .await?;

    // (4) Perform Rust compilation
    let compilation_result = perform_rust_compilation(
        common_config,
        config,
        Some(db.clone()),
        tenant_id,
        pipeline.id,
        &pipeline.platform_version,
        pipeline.program_version,
        &pipeline.program_config,
        &pipeline.program_info,
        &pipeline.udf_rust,
        &pipeline.udf_toml,
    )
    .await;

    // (5) Update database that Rust compilation is finished
    match compilation_result {
        Ok((program_binary_url, source_checksum, integrity_checksum, duration, cached)) => {
            info!(
                "Rust compilation success: pipeline {} (program version: {}) ({}; source checksum: {}; integrity checksum: {})",
                pipeline.id,
                pipeline.program_version,
                if cached {
                    "cached".to_string()
                } else {
                    format!("took {:.2}s", duration.as_secs_f64())
                },
                truncate_sha256_checksum(&source_checksum),
                truncate_sha256_checksum(&integrity_checksum),
            );
            db.lock()
                .await
                .transit_program_status_to_success(
                    tenant_id,
                    pipeline.id,
                    pipeline.program_version,
                    &source_checksum,
                    &integrity_checksum,
                    &program_binary_url,
                )
                .await?;
        }
        Err(e) => match e {
            RustCompilationError::NoLongerExists => {
                info!(
                    "Rust compilation canceled: pipeline {} no longer exists",
                    pipeline.id,
                );
            }
            RustCompilationError::Outdated => {
                info!(
                    "Rust compilation canceled: pipeline {} (program version: {}) is outdated",
                    pipeline.id, pipeline.program_version,
                );
            }
            RustCompilationError::TerminatedBySignal => {
                error!(
                    "Rust compilation interrupted: pipeline {} (program version: {}) compilation process was terminated by a signal",
                    pipeline.id, pipeline.program_version,
                );
            }
            RustCompilationError::RustError(rust_error) => {
                db.lock()
                    .await
                    .transit_program_status_to_rust_error(
                        tenant_id,
                        pipeline.id,
                        pipeline.program_version,
                        &rust_error,
                    )
                    .await?;
                info!(
                    "Rust compilation failed: pipeline {} (program version: {}) due to Rust error",
                    pipeline.id, pipeline.program_version
                );
            }
            RustCompilationError::SystemError(internal_system_error) => {
                db.lock()
                    .await
                    .transit_program_status_to_system_error(
                        tenant_id,
                        pipeline.id,
                        pipeline.program_version,
                        &internal_system_error,
                    )
                    .await?;
                error!("Rust compilation failed: pipeline {} (program version: {}) due to system error:\n{}", pipeline.id, pipeline.program_version, internal_system_error);
            }
        },
    }
    Ok(true)
}

/// Calculates sha256 checksum across the fields.
/// It is calculated using: `H(H(field1) || H(field2) || ...)`.
/// `H(x)` is the hashing function. `||` represents concatenation.
/// The order of the fields is always the same.
#[allow(clippy::too_many_arguments)]
fn calculate_source_checksum(
    platform_version: &str,
    profile: &CompilationProfile,
    config: &CompilerConfig,
    _program_config: &ProgramConfig, // Not used as fields are already included or irrelevant
    main_rust: &str,
    udf_stubs: &str,
    udf_rust: &str,
    udf_toml: &str,
) -> String {
    let mut hasher = sha::Sha256::new();
    for (name, data) in [
        // Not used because already included in profile:
        // - config.compilation_profile
        // - program_config.profile

        // Not used because irrelevant:
        // - config.precompile
        // - config.binary_ref_host (prior compilations are expected to remain valid)
        // - config.binary_ref_port (prior compilations are expected to remain valid)
        // - program_config.cache
        ("platform_version", platform_version.as_bytes()),
        ("profile", profile.to_string().as_bytes()),
        (
            "config.compiler_working_directory",
            config.compiler_working_directory.to_string().as_bytes(),
        ),
        (
            "config.sql_compiler_home",
            config.sql_compiler_home.to_string().as_bytes(),
        ),
        (
            "config.dbsp_override_path",
            config.dbsp_override_path.to_string().as_bytes(),
        ),
        ("program_info.main_rust", main_rust.as_bytes()),
        ("program_info.udf_stubs", udf_stubs.as_bytes()),
        ("udf_rust", udf_rust.as_bytes()),
        ("udf_toml", udf_toml.as_bytes()),
    ] {
        let checksum = sha256(data);
        hasher.update(&checksum);
        trace!(
            "Rust compilation: checksum of {name}: {}",
            hex::encode(checksum)
        );
    }
    hex::encode(hasher.finish())
}

/// Rust compilation possible error outcomes.
#[derive(Debug)]
pub enum RustCompilationError {
    /// In the meanwhile the pipeline was deleted, as such the Rust
    /// compilation is no longer useful.
    NoLongerExists,
    /// In the meanwhile the pipeline was already updated, as such the
    /// Rust compilation is outdated and no longer useful.
    Outdated,
    /// The Rust compilation process was terminated by a signal.
    /// This can happen for instance when the compiler server is terminated by signal,
    /// and processes started by it are first terminated before itself. The signal is likely not
    /// related to the program itself inherently being unable to compile, nor the compiler
    /// server reaching an inconsistent state. As such, retrying is the desired
    /// behavior rather than declaring failure to compile the specific program.
    TerminatedBySignal,
    /// Rust compilation was unable to be performed.
    /// This is either due to:
    /// - A check failed when setting up the working directory in which
    ///   the Rust compiler will be called
    /// - The Rust compiler call failed and returned an error
    ///   (e.g., a syntax error in the SQL-generated Rust or
    ///   the user-provided Rust implementation of UDFs).
    RustError(String),
    /// General system problem occurred (e.g., I/O error)
    SystemError(String),
}

/// Common errors are system errors during Rust compilation.
impl From<CommonError> for RustCompilationError {
    fn from(value: CommonError) -> Self {
        RustCompilationError::SystemError(value.to_string())
    }
}

/// Performs the Rust compilation:
/// - Calculate source checksum across fields for cache lookup
/// - Perform cache lookup using the source checksum:
///   - If a binary already exists for that source checksum, return that
///   - Otherwise, set up the working directory for the Rust compiler
///     and call it in order to generate the binary
///
/// Returns the program binary URL, source checksum, integrity checksum,
/// duration and whether it was cached.
#[allow(clippy::too_many_arguments)]
pub async fn perform_rust_compilation(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    db: Option<Arc<Mutex<StoragePostgres>>>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    platform_version: &str,
    program_version: Version,
    program_config: &serde_json::Value,
    program_info: &Option<serde_json::Value>,
    udf_rust: &str,
    udf_toml: &str,
) -> Result<(String, String, String, Duration, bool), RustCompilationError> {
    let start = Instant::now();

    // These must always be the same, the Rust compiler should never pick up
    // a pipeline program which is not of its current platform version.
    if common_config.platform_version != platform_version {
        return Err(RustCompilationError::SystemError(format!(
            "Platform version {platform_version} is not equal to current {}",
            common_config.platform_version
        )));
    }

    // Program configuration
    let program_config = validate_program_config(program_config, true).map_err(|error| {
        RustCompilationError::SystemError(formatdoc! {"
                The program configuration:
                {program_config:#}

                ... is not valid due to: {error}.

                This indicates a backward-incompatible platform upgrade occurred.
                Update the 'program_config' field of the pipeline to resolve this.
            "})
    })?;

    // Program info
    let program_info = match program_info {
        None => {
            return Err(RustCompilationError::SystemError(
                "Unable to Rust compile pipeline program because its program information is missing".to_string()
            ));
        }
        Some(program_info) => program_info.clone(),
    };
    let program_info = validate_program_info(&program_info).map_err(|error| {
        RustCompilationError::SystemError(formatdoc! {"
                The program information:
                {program_info:#}

                ... is not valid due to: {error}.

                This indicates a backward-incompatible platform upgrade occurred.
                Recompile the pipeline to resolve this.
            "})
    })?;
    let main_rust = program_info.main_rust.clone();
    let udf_stubs = program_info.udf_stubs.clone();

    // Compilation profile is the one specified by the program configuration,
    // and if it is not, defaults to the one provided in the compiler arguments.
    let profile = program_config
        .profile
        .clone()
        .unwrap_or(config.compilation_profile.clone());

    // Calculate source checksum across fields
    let source_checksum = calculate_source_checksum(
        platform_version,
        &profile,
        config,
        &program_config,
        &main_rust,
        &udf_stubs,
        udf_rust,
        udf_toml,
    );
    trace!("Rust compilation: calculated source checksum: {source_checksum}");

    // The compilation is cached if cache is enabled AND a binary exists with that source checksum
    let binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("binaries");
    let binary_file_path = binaries_dir.join(format!("project-{source_checksum}"));
    let is_cached = program_config.cache && binary_file_path.exists() && binary_file_path.is_file();
    if !is_cached {
        // No cached binary exists: perform compilation
        info!(
            "Rust compilation started: pipeline {} (program version: {}) with profile {} (source checksum: {})",
            pipeline_id, program_version, profile.to_string(), truncate_sha256_checksum(&source_checksum)
        );
        prepare_project(
            config,
            &source_checksum,
            &main_rust,
            &udf_stubs,
            udf_rust,
            udf_toml,
        )
        .await?;
        prepare_workspace(config, &source_checksum).await?;
        call_compiler(
            db,
            tenant_id,
            pipeline_id,
            program_version,
            config,
            &source_checksum,
            &profile,
        )
        .await?;
    }

    // Either the binary was already binaries directory (cached) or the compiler just generated it.
    // We will now copy the binary to a file dedicated to the pipeline, such that if there is any
    // case where there is a difference in compilation which is not detected by the source checksum,
    // it will not affect other (potentially already running) pipelines whose compilation had the
    // same source checksum.

    // Create pipeline-binaries directory if it does not yet exist
    let pipeline_binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries");
    if !pipeline_binaries_dir.is_dir() {
        recreate_dir(&pipeline_binaries_dir).await?;
    }

    // Calculate the integrity checksum over the binary
    let integrity_checksum =
        hex::encode(sha256(&read_file_content_bytes(&binary_file_path).await?));

    // Destination per-pipeline-program-version binary
    let target_file_path = pipeline_binaries_dir.join(format!(
        "pipeline_{pipeline_id}_v{program_version}_sc_{source_checksum}_ic_{integrity_checksum}"
    ));
    if target_file_path.exists() {
        info!(
            "Target per-pipeline binary file '{}' already exists: overriding it",
            target_file_path.display()
        );
    }

    // Copy file from binaries directory to pipeline-binaries directory
    copy_file(&binary_file_path, &target_file_path).await?;

    // URL where the program binary can be downloaded from
    let program_binary_url = format!(
        "http://{}:{}/binary/{}/{}/{}/{}",
        config.binary_ref_host,
        config.binary_ref_port,
        pipeline_id,
        program_version,
        source_checksum,
        integrity_checksum
    );

    Ok((
        program_binary_url,
        source_checksum,
        integrity_checksum,
        start.elapsed(),
        is_cached,
    ))
}

/// The `main` function which is injected in each generated pipeline crate
/// by appending it to `main.rs`. This main function calls the `circuit()`
/// function that is already generated by the SQL compiler in `main.rs`.
const MAIN_FUNCTION: &str = r#"
fn main() {
    dbsp_adapters::server::server_main(|cconfig| {
        circuit(cconfig)
            .map(|(dbsp, catalog)| {
                (
                    dbsp,
                    Box::new(catalog) as Box<dyn dbsp_adapters::CircuitCatalog>,
                )
            })
            .map_err(|e| dbsp_adapters::ControllerError::dbsp_error(e))
    })
    .unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });
}"#;

/// Prepares the project directory by writing the files required for compilation:
/// - `src/main.rs`: content is the SQL-generated Rust code (`program_info.main_rust`) appended
///    with a main function
/// - `src/udf.rs`: content is `udf_rust`
/// - `src/stubs.rs`: content is `program_info.udf_stubs`
/// - `Cargo.toml`: content is a template Cargo.toml modified corresponding to compiler
///   configuration and incorporating the `udf_toml`
async fn prepare_project(
    config: &CompilerConfig,
    source_checksum: &str,
    main_rust: &str,
    udf_stubs: &str,
    udf_rust: &str,
    udf_toml: &str,
) -> Result<(), RustCompilationError> {
    // Recreate Rust project directory
    let project_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("projects")
        .join(format!("project-{source_checksum}"));
    recreate_dir(&project_dir).await?;

    // src/
    let src_dir = project_dir.join("src");
    recreate_dir(&src_dir).await?;

    // src/main.rs
    let main_rs_file_path = src_dir.join("main.rs");
    let mut main_rs_content = main_rust.to_string();
    main_rs_content.push_str(MAIN_FUNCTION);
    create_new_file_with_content(&main_rs_file_path, &main_rs_content).await?;

    // src/stubs.rs
    let stubs_rs_file_path = src_dir.join("stubs.rs");
    create_new_file_with_content(&stubs_rs_file_path, udf_stubs).await?;

    // src/udf.rs
    let udf_rs_file_path = src_dir.join("udf.rs");
    create_new_file_with_content(&udf_rs_file_path, udf_rust).await?;

    // Cargo.toml: read template
    let template_toml_file_path = Path::new(&config.sql_compiler_home)
        .join("temp")
        .join("Cargo.toml");
    let mut project_toml = read_file_content(&template_toml_file_path.clone()).await?;

    // Cargo.toml: project name and make binary executable instead of library
    let project_name = format!("name = \"project-{source_checksum}\"");
    project_toml = project_toml
        .replace("name = \"temp\"", &project_name)
        .replace(
            "[lib]\npath = \"src/lib.rs\"",
            &format!("\n\n[[bin]]\n{project_name}\npath = \"src/main.rs\""),
        );

    // Cargo.toml: Feldera crate locations
    project_toml = project_toml
        .replace(
            "dbsp_adapters = { path = \"../../crates/adapters\", default-features = false }",
            {
                #[cfg(not(feature = "feldera-enterprise"))]
                { r#"dbsp_adapters = { path = "../../crates/adapters" }"# }
                #[cfg(feature = "feldera-enterprise")]
                { r#"dbsp_adapters = { path = "../../crates/adapters", features = ["feldera-enterprise"] }"# }
            }
        )
        .replace(
            "../../crates",
            &format!("{}/crates", &config.dbsp_override_path),
        )
        .replace(
            "../lib",
            &format!(
                "{}",
                Path::new(&config.sql_compiler_home).join("lib").display()
            ),
        );

    // Cargo.toml: add UDF dependencies
    project_toml = project_toml.replace(
        "[dependencies]",
        &formatdoc! {"
            [dependencies]
            # START: UDF dependencies
            {udf_toml}
            # END: UDF dependencies
        "},
    );

    // Cargo.toml: write to file
    let cargo_toml_file_path = project_dir.join("Cargo.toml");
    create_new_file_with_content(&cargo_toml_file_path, &project_toml).await?;

    Ok(())
}

/// Prepares the workspace directory for compiling a specific project by writing
/// the workspace-level `Cargo.toml`.
async fn prepare_workspace(
    config: &CompilerConfig,
    source_checksum: &str,
) -> Result<(), RustCompilationError> {
    // Ensure the rust-compilation directory exists in
    // which to create the workspace-level Cargo.toml
    let rust_compilation_dir = config.working_dir().join("rust-compilation");
    fs::create_dir_all(&rust_compilation_dir)
        .await
        .map_err(|e| {
            CommonError::io_error(
                format!("creating directory '{}'", rust_compilation_dir.display()),
                e,
            )
        })?;

    // Create the workspace-level Cargo.toml
    let cargo_toml = formatdoc! {r#"
        [workspace]
        members = [ "projects/project-{source_checksum}" ]
        resolver = "2"

        [patch.crates-io]
        datafusion = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}
        datafusion-common = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}
        datafusion-expr = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}
        datafusion-functions = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}
        datafusion-functions-aggregate = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}
        datafusion-physical-expr = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}
        datafusion-physical-plan = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}
        datafusion-proto = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}
        datafusion-sql = {{ git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }}

        [profile.unoptimized]
        inherits = "release"
        opt-level = 0
        lto = "off"
        codegen-units = 256

        [profile.optimized]
        inherits = "release"
    "#};
    let cargo_toml_file_path = config
        .working_dir()
        .join("rust-compilation")
        .join("Cargo.toml");
    recreate_file_with_content(&cargo_toml_file_path, &cargo_toml).await?;

    Ok(())
}

/// Calls the compiler on the project with the provided source checksum and using the compilation profile.
async fn call_compiler(
    db: Option<Arc<Mutex<StoragePostgres>>>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    program_version: Version,
    config: &CompilerConfig,
    source_checksum: &str,
    profile: &CompilationProfile,
) -> Result<(), RustCompilationError> {
    // Pre-existing project directory
    let project_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("projects")
        .join(format!("project-{source_checksum}"));
    if !project_dir.is_dir() {
        return Err(RustCompilationError::SystemError(format!(
            "Expected {} to be a directory and exist, but it is or does not",
            project_dir.display()
        )));
    }

    // Create binaries directory if it does not yet exist
    let binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("binaries");
    if !binaries_dir.is_dir() {
        recreate_dir(&binaries_dir).await?;
    }

    // Create file where stdout will be written to
    let stdout_file_path = project_dir.join("stdout.log");
    let stdout_file = create_new_file(&stdout_file_path).await?;

    // Create file where stderr will be written to
    let stderr_file_path = project_dir.join("stderr.log");
    let stderr_file = create_new_file(&stderr_file_path).await?;

    // Formulate command
    let mut command = Command::new("cargo");
    command
        // Set compiler stack size to 20MB (10x the default) to prevent
        // SIGSEGV when the compiler runs out of stack on large programs.
        .env("RUST_MIN_STACK", "20971520")
        .current_dir(config.working_dir().join("rust-compilation"))
        .arg("build")
        .arg("--workspace")
        .arg("--profile")
        .arg(profile.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file.into_std().await))
        .stderr(Stdio::from(stderr_file.into_std().await));

    // Start process
    let mut process = command.spawn().map_err(|e| {
        RustCompilationError::SystemError(
            CommonError::io_error("running 'cargo build'".to_string(), e).to_string(),
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
                            .get_pipeline_by_id_for_monitoring(tenant_id, pipeline_id)
                            .await
                        {
                            Ok(pipeline) => {
                                if pipeline.program_version != program_version {
                                    return Err(RustCompilationError::Outdated);
                                }
                            }
                            Err(DBError::UnknownPipeline { .. }) => {
                                return Err(RustCompilationError::NoLongerExists);
                            }
                            Err(e) => {
                                error!("Rust compilation outdated check failed due to database error: {e}")
                                // As preemption check failing is not fatal, compilation will continue
                            }
                        }
                    }
                }
                Some(exit_status) => break exit_status,
            },
            Err(e) => {
                return Err(RustCompilationError::SystemError(
                    CommonError::io_error("waiting for 'cargo build'".to_string(), e).to_string(),
                ));
            }
        }
        sleep(COMPILATION_CHECK_INTERVAL).await;
    };

    // Compilation is successful if the return exit code is present and zero
    if exit_status.success() {
        // Source file
        let source_file_path = config
            .working_dir()
            .join("rust-compilation")
            .join("target")
            .join(profile.to_target_folder())
            .join(format!("project-{source_checksum}"));
        if !source_file_path.is_file() {
            return Err(RustCompilationError::SystemError(format!(
                "Rust compilation was successful but no binary was generated at '{}'",
                source_file_path.display()
            )));
        }

        // Integrity checksum of the source file
        let source_file_integrity_checksum =
            hex::encode(sha256(&read_file_content_bytes(&source_file_path).await?));

        // Destination file
        let target_file_path = binaries_dir.join(format!("project-{source_checksum}"));
        if target_file_path.exists() {
            info!(
                "Target binary file '{}' already exists: overriding it",
                target_file_path.display()
            );
            let existing_target_file_integrity_checksum =
                hex::encode(sha256(&read_file_content_bytes(&target_file_path).await?));
            if source_file_integrity_checksum != existing_target_file_integrity_checksum {
                warn!(
                    "{}",
                    formatdoc! {"
                    \n
                    The newly generated binary which is about to override a binary from a prior
                    compilation has the same source checksum as the prior binary, however it has
                    a different integrity checksum:
                      > Source checksum............ {source_checksum}
                      > New integrity checksum..... {source_file_integrity_checksum}
                      > Prior integrity checksum... {existing_target_file_integrity_checksum}

                    This might be caused by the Cargo.lock having changed in the meanwhile,
                    or another change which was not covered by the source checksum.
                "}
                );
            }
        }

        // Copy binary from Cargo target profile directory to the binaries directory
        copy_file(&source_file_path, &target_file_path).await?;

        // Success
        Ok(())
    } else {
        match exit_status.code() {
            None => {
                // No exit status code present because the process was terminated by a signal
                Err(RustCompilationError::TerminatedBySignal)
            }
            Some(exit_code) => {
                let stdout_str =
                    fs::read_to_string(stdout_file_path.clone())
                        .await
                        .map_err(|e| {
                            RustCompilationError::SystemError(
                                CommonError::io_error(
                                    format!("reading file '{}'", stdout_file_path.display()),
                                    e,
                                )
                                .to_string(),
                            )
                        })?;
                let stderr_str =
                    fs::read_to_string(stderr_file_path.clone())
                        .await
                        .map_err(|e| {
                            RustCompilationError::SystemError(
                                CommonError::io_error(
                                    format!("reading file '{}'", stderr_file_path.display()),
                                    e,
                                )
                                .to_string(),
                            )
                        })?;
                let error_message = formatdoc! {"
                    Rust error: the Rust code generated based on the SQL (possibly combined with any user-provided UDF Rust and TOML code) failed to compile.
                    This should not happen (except if the error is in user-provided UDF code).
                    Please file a bug report with the example SQL and any UDF Rust/TOML that triggers it at:
                    https://github.com/feldera/feldera/issues
                "};
                Err(RustCompilationError::RustError(
                    format!(
                        "{error_message}\n\n\
                        The compilation task exited with status code {exit_code} and produced the following logs:\n\n\
                        stderr:\n\
                        {stderr_str}\n\n\
                        stdout:\n\
                        {stdout_str}"
                    )
                ))
            }
        }
    }
}

/// Rust compilation cleanup possible error outcomes.
enum RustCompilationCleanupError {
    /// Database error occurred (e.g., lost connectivity).
    DBError(DBError),
    /// Filesystem problem occurred (e.g., I/O error)
    CommonError(CommonError),
}

impl From<DBError> for RustCompilationCleanupError {
    fn from(value: DBError) -> Self {
        RustCompilationCleanupError::DBError(value)
    }
}

impl From<CommonError> for RustCompilationCleanupError {
    fn from(value: CommonError) -> Self {
        RustCompilationCleanupError::CommonError(value)
    }
}

/// Makes the cleanup decision based on the provided name of the file or directory.
fn decide_cleanup(
    name: &str,
    project_separator: char,
    remainder_separator: Option<(bool, char)>,
    deletion: &[String],
) -> CleanupDecision {
    // Name must start with "project<project_separator>" (e.g. "project-", "project_")
    // Remainder is for example: "project-0123456789.abcdef" -> "0123456789.abcdef"
    let Some(remainder) = name.strip_prefix(&format!("project{project_separator}")) else {
        return CleanupDecision::Ignore;
    };

    // The checksum needs to be retrieved from the remainder
    let checksum = match remainder_separator {
        None => remainder,
        // The remainder split can either be required or optional.
        // If it is required, it will not return the entire remainder as default
        // if it cannot split the remainder by the separator.
        Some((required, separator)) => {
            let remainder_spl: Vec<&str> = remainder.splitn(2, separator).collect();
            if remainder_spl.len() == 2 {
                remainder_spl[0]
            } else if required {
                return CleanupDecision::Ignore;
            } else {
                remainder
            }
        }
    };

    // Final decision whether to keep or not is determined whether it is marked for deletion
    if deletion.contains(&checksum.to_string()) {
        CleanupDecision::Remove
    } else {
        CleanupDecision::Keep {
            motivation: checksum.to_string(),
        }
    }
}

/// Cleans up the Rust compilation working directory by removing projects
/// and binaries of pipelines that no longer exist. It also removes files
/// and directories that are unexpected.
async fn cleanup_rust_compilation(
    config: &CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<(), RustCompilationCleanupError> {
    trace!("Performing Rust cleanup...");

    // Location of the cleanup state file
    let cleanup_state_file_path = config
        .working_dir()
        .join("rust-compilation")
        .join("cleanup_state.json");

    // Attempt to read cleanup state from file.
    // If it fails, an error is written to log and the cleanup state is wiped.
    // The wipe means that all retention expirations will be reset.
    let mut cleanup_state: BTreeMap<String, SystemTime> =
        match read_file_content(&cleanup_state_file_path).await {
            Ok(content) => serde_json::from_str(&content).unwrap_or_else(|e| {
                error!("Unable to deserialize cleanup state due to: {e}");
                BTreeMap::new()
            }),
            Err(e) => {
                if !cleanup_state_file_path.exists() {
                    debug!(
                        "Cleanup state file does not yet exist -- \
                        it will be created at the end of the first cleanup cycle"
                    );
                } else {
                    error!("Unable to read cleanup state file due to: {e}");
                }
                BTreeMap::new()
            }
        };

    // (1) Retrieve existing pipeline programs
    //     (pipeline_id, program_version, program_binary_source_checksum, program_binary_integrity_checksum)
    let existing_pipeline_programs = db
        .lock()
        .await
        .list_pipeline_programs_across_all_tenants()
        .await?;

    // For compilation-related cleanup, only the source checksum is needed
    let mut existing_source_checksums: Vec<String> = existing_pipeline_programs
        .iter()
        .map(|(_, _, source_checksum, _)| source_checksum.clone())
        .collect();
    existing_source_checksums.dedup();

    // Current timestamp at which the cleanup takes place
    let current_timestamp = SystemTime::now();

    // Remove any source checksum which exists again
    for checksum in &existing_source_checksums {
        if cleanup_state.remove(checksum).is_some() {
            debug!("Rust compilation cleanup: source checksum {checksum} exists again")
        }
    }

    // The ones that have been marked longer ago than the retention period will be deleted
    let mut deletion = HashSet::<String>::new();
    for (checksum, expiration) in cleanup_state.iter() {
        if current_timestamp
            .duration_since(*expiration)
            .is_ok_and(|duration| duration >= CLEANUP_RETENTION)
        {
            deletion.insert(checksum.clone());
            debug!("Rust compilation cleanup: retention for source checksum {} has expired -- marked for deletion", checksum);
        }
    }
    let deletion: Vec<String> = deletion.into_iter().collect();

    // All found source checksums during the decisions, which is used later to update the cleanup state
    let mut found = Vec::<String>::new();

    // Decision when named "project-<checksum>"
    let deletion_clone = deletion.clone();
    let decide_cleanup_dash_exact =
        Arc::new(move |name: &str| decide_cleanup(name, '-', None, &deletion_clone));

    // (2) Clean up binaries
    let binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("binaries");
    if binaries_dir.is_dir() {
        found.append(
            &mut cleanup_specific_files(
                "Rust compilation binaries",
                &binaries_dir,
                decide_cleanup_dash_exact.clone(),
                true,
            )
            .await?,
        );
    }

    // (3) Clean up project directories
    let projects_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("projects");
    if projects_dir.is_dir() {
        found.append(
            &mut cleanup_specific_directories(
                "Rust compilation projects",
                &projects_dir,
                decide_cleanup_dash_exact.clone(),
                true,
            )
            .await?,
        );
    }

    // (4) For each possible compilation profile, clean up their target artifacts
    for profile in [
        CompilationProfile::Dev,
        CompilationProfile::Unoptimized,
        CompilationProfile::Optimized,
    ] {
        let target_profile_folder = profile.to_target_folder();
        let target_subdir = config
            .working_dir()
            .join("rust-compilation")
            .join("target")
            .join(target_profile_folder);
        if target_subdir.is_dir() {
            // target/<folder>: "project-<source-checksum>.<other>" or else "project-<source-checksum>"
            let deletion_clone = deletion.clone();
            found.append(
                &mut cleanup_specific_files(
                    &format!("Rust compilation target {target_profile_folder}"),
                    &target_subdir,
                    Arc::new(move |name: &str| {
                        decide_cleanup(name, '-', Some((false, '.')), &deletion_clone)
                    }),
                    false,
                )
                .await?,
            );

            // target/<folder>/deps: "project_<source-checksum>-<other>"
            let deps_dir = target_subdir.join("deps");
            if deps_dir.is_dir() {
                let deletion_clone = deletion.clone();
                found.append(
                    &mut cleanup_specific_files(
                        &format!("Rust compilation target {target_profile_folder} deps"),
                        &deps_dir,
                        Arc::new(move |name: &str| {
                            decide_cleanup(name, '_', Some((true, '-')), &deletion_clone)
                        }),
                        false,
                    )
                    .await?,
                );
            }

            // target/<folder>/.fingerprint: "project-<source-checksum>-<other>"
            let fingerprint_dir = target_subdir.join(".fingerprint");
            if fingerprint_dir.is_dir() {
                let deletion_clone = deletion.clone();
                found.append(
                    &mut cleanup_specific_directories(
                        &format!("Rust compilation target {target_profile_folder} .fingerprint"),
                        &fingerprint_dir,
                        Arc::new(move |name: &str| {
                            decide_cleanup(name, '-', Some((true, '-')), &deletion_clone)
                        }),
                        false,
                    )
                    .await?,
                );
            }
        }
    }

    // (5) Clean up pipeline binaries
    //     These are not subject to the retention period.
    let pipeline_binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries");
    let valid_pipeline_binary_filenames: Vec<String> = existing_pipeline_programs.iter().map(
        |(pipeline_id, program_version, source_checksum, integrity_checksum)| {
        format!("pipeline_{pipeline_id}_v{program_version}_sc_{source_checksum}_ic_{integrity_checksum}")
    }).collect();
    if pipeline_binaries_dir.is_dir() {
        cleanup_specific_files(
            "Rust compilation pipeline binaries",
            &pipeline_binaries_dir,
            Arc::new(move |filename: &str| {
                if filename.starts_with("pipeline_") {
                    if valid_pipeline_binary_filenames.contains(&filename.to_string()) {
                        CleanupDecision::Keep {
                            motivation: filename.to_string(),
                        }
                    } else {
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

    // Any checksum that was found which:
    // (1) is not an existing source checksum
    // (2) AND is not already in the cleanup state
    // ... will be added to the cleanup state.
    let found = HashSet::<String>::from_iter(found.into_iter());
    for checksum in found.iter() {
        if !existing_source_checksums.contains(checksum) && !cleanup_state.contains_key(checksum) {
            let expiration_datetime: DateTime<Utc> = (current_timestamp + CLEANUP_RETENTION).into();
            debug!(
                "Rust compilation cleanup: source checksum {} will be deleted some time after retention expires (currently: {})",
                checksum,
                expiration_datetime.format("%Y-%m-%d %H:%M:%S")
            );
            cleanup_state.insert(checksum.clone(), current_timestamp);
        }
    }

    // Remove any checksum in the cleanup state which is no longer found
    for checksum in cleanup_state.clone().keys() {
        if !found.contains(checksum) {
            cleanup_state.remove(checksum);
        }
    }

    // Attempt to write cleanup state to file.
    // If it fails, an error is written to log and the cleanup state changes will be lost.
    // The changes being lost means that new retention expirations are not set, and old ones remain.
    match serde_json::to_string(&cleanup_state) {
        Ok(s) => match recreate_file_with_content(&cleanup_state_file_path, &s).await {
            Ok(()) => {}
            Err(e) => error!("Unable to write cleanup state to file due to: {e}"),
        },
        Err(e) => error!("Unable to serialize cleanup state due to: {e}"),
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::compiler::rust_compiler::{
        calculate_source_checksum, decide_cleanup, prepare_project,
    };
    use crate::compiler::rust_compiler::{prepare_workspace, MAIN_FUNCTION};
    use crate::compiler::test::{list_content_as_sorted_names, CompilerTest};
    use crate::compiler::util::{read_file_content, CleanupDecision};
    use crate::config::CompilerConfig;
    use crate::db::types::program::{CompilationProfile, ProgramConfig};
    use std::collections::HashSet;

    /// Tests the calculation of the source checksum.
    #[tokio::test]
    async fn source_checksum_calculation() {
        // Calling with different input yields a different checksum
        let config_1 = CompilerConfig {
            compiler_working_directory: "".to_string(),
            compilation_profile: CompilationProfile::Dev,
            sql_compiler_home: "".to_string(),
            dbsp_override_path: "".to_string(),
            precompile: false,
            binary_ref_host: "".to_string(),
            binary_ref_port: 0,
        };
        let config_2 = CompilerConfig {
            compiler_working_directory: "a".to_string(),
            compilation_profile: CompilationProfile::Dev,
            sql_compiler_home: "".to_string(),
            dbsp_override_path: "".to_string(),
            precompile: false,
            binary_ref_host: "".to_string(),
            binary_ref_port: 0,
        };
        let config_3 = CompilerConfig {
            compiler_working_directory: "".to_string(),
            compilation_profile: CompilationProfile::Dev,
            sql_compiler_home: "b".to_string(),
            dbsp_override_path: "".to_string(),
            precompile: false,
            binary_ref_host: "".to_string(),
            binary_ref_port: 0,
        };
        let config_4 = CompilerConfig {
            compiler_working_directory: "".to_string(),
            compilation_profile: CompilationProfile::Dev,
            sql_compiler_home: "".to_string(),
            dbsp_override_path: "c".to_string(),
            precompile: false,
            binary_ref_host: "".to_string(),
            binary_ref_port: 0,
        };
        let program_config_1 = ProgramConfig {
            profile: None,
            cache: false,
        };
        let mut seen = HashSet::<String>::new();
        let platform_versions = ["", "v1", "v2"];
        let profiles = [
            CompilationProfile::Dev,
            CompilationProfile::Optimized,
            CompilationProfile::Unoptimized,
        ];
        let configs = [
            config_1.clone(),
            config_2.clone(),
            config_3.clone(),
            config_4.clone(),
        ];
        let program_configs = [program_config_1.clone()];
        let content = ["", "a", "aa", "b", "c", "d", "e"];
        for (
            platform_version,
            profile,
            config,
            program_config,
            main_rust,
            udf_stubs,
            udf_rust,
            udf_toml,
        ) in itertools::iproduct!(
            platform_versions,
            profiles,
            configs,
            program_configs,
            content,
            content,
            content,
            content
        ) {
            let source_checksum = calculate_source_checksum(
                platform_version,
                &profile,
                &config,
                &program_config,
                main_rust,
                udf_stubs,
                udf_rust,
                udf_toml,
            );
            assert!(
                !seen.contains(&source_checksum),
                "checksum {source_checksum} has a duplicate"
            );
            seen.insert(source_checksum);
        }

        // Calling with the same input yields the same checksum
        let checksum1 = calculate_source_checksum(
            "v0",
            &CompilationProfile::Optimized,
            &config_1,
            &program_config_1,
            "main_rust",
            "udf_stubs",
            "udf_rust",
            "udf_toml",
        );
        let checksum2 = calculate_source_checksum(
            "v0",
            &CompilationProfile::Optimized,
            &config_1,
            &program_config_1,
            "main_rust",
            "udf_stubs",
            "udf_rust",
            "udf_toml",
        );
        assert_eq!(checksum1, checksum2);

        // Program configuration currently does not impact the checksum
        let checksum1 = calculate_source_checksum(
            "v0",
            &CompilationProfile::Optimized,
            &config_1,
            &ProgramConfig {
                profile: None,
                cache: false,
            },
            "main_rust",
            "udf_stubs",
            "udf_rust",
            "udf_toml",
        );
        let checksum2 = calculate_source_checksum(
            "v0",
            &CompilationProfile::Optimized,
            &config_1,
            &ProgramConfig {
                profile: None,
                cache: true,
            },
            "main_rust",
            "udf_stubs",
            "udf_rust",
            "udf_toml",
        );
        assert_eq!(checksum1, checksum2);
    }

    /// Tests the project preparation helper function.
    #[tokio::test]
    async fn project_preparation() {
        let test = CompilerTest::new().await;
        let project_dir = test
            .rust_workdir
            .join("projects")
            .join("project-abcdef123456");

        // Project directory does not exist initially
        assert!(!project_dir.is_dir());

        // Prepare project directory
        prepare_project(
            &test.compiler_config,
            "abcdef123456",
            "main_rust",
            "udf_stubs",
            "udf_rust",
            "udf_toml",
        )
        .await
        .unwrap();

        // Check project directory exists and its content
        assert!(project_dir.is_dir());
        let project_dir_content = list_content_as_sorted_names(&project_dir).await;
        assert_eq!(project_dir_content, vec!["Cargo.toml", "src"]);

        // Check src directory and its content
        let src_dir = project_dir.join("src");
        let src_dir_content = list_content_as_sorted_names(&src_dir).await;
        assert_eq!(src_dir_content, vec!["main.rs", "stubs.rs", "udf.rs"]);
        assert_eq!(
            read_file_content(&src_dir.join("main.rs")).await.unwrap(),
            format!("main_rust{MAIN_FUNCTION}")
        );
        assert_eq!(
            read_file_content(&src_dir.join("stubs.rs")).await.unwrap(),
            "udf_stubs"
        );
        assert_eq!(
            read_file_content(&src_dir.join("udf.rs")).await.unwrap(),
            "udf_rust"
        );

        // Check Cargo.toml
        assert!(read_file_content(&project_dir.join("Cargo.toml"))
            .await
            .unwrap()
            .contains("udf_toml"));
    }

    /// Tests the workspace preparation helper function.
    #[tokio::test]
    async fn workspace_preparation() {
        let test = CompilerTest::new().await;
        let project_toml_file = test.rust_workdir.join("Cargo.toml");

        // Project TOML does not exist initially
        assert!(!project_toml_file.is_file());

        // Prepare workspace directory
        prepare_workspace(&test.compiler_config, "abcdef123456")
            .await
            .unwrap();

        // Check project TOML exists and its content
        assert!(project_toml_file.is_file());
        assert!(read_file_content(&project_toml_file)
            .await
            .unwrap()
            .contains("members = [ \"projects/project-abcdef123456\" ]"));
    }

    /// Tests the cleanup decision helper function.
    #[tokio::test]
    #[rustfmt::skip]
    async fn cleanup_decision_helper() {
        for (i, (name, project_separator, remainder_separator, deletion, expected)) in [
            // No remainder separator
            ("example", '-', None, vec![], CleanupDecision::Ignore),
            ("example", '-', None, vec!["a".to_string()], CleanupDecision::Ignore),
            ("project-a", '-', None, vec![], CleanupDecision::Keep { motivation: "a".to_string() }),
            ("project-a", '-', None, vec!["a".to_string()], CleanupDecision::Remove),
            ("project-a_b", '-', None, vec![], CleanupDecision::Keep { motivation: "a_b".to_string() }),
            ("project-a_b", '-', None, vec!["a".to_string()], CleanupDecision::Keep { motivation: "a_b".to_string() }),
            ("project-a_b", '-', None, vec!["b".to_string()], CleanupDecision::Keep { motivation: "a_b".to_string() }),
            ("project-a_b", '-', None, vec!["a_b".to_string()], CleanupDecision::Remove),
            // Optional remainder separator
            ("example", '-', Some((false, '_')), vec![], CleanupDecision::Ignore),
            ("example", '-', Some((false, '_')), vec!["a".to_string()], CleanupDecision::Ignore),
            ("project-a", '-', Some((false, '_')), vec![], CleanupDecision::Keep { motivation: "a".to_string() }),
            ("project-a", '-', Some((false, '_')), vec!["a".to_string()], CleanupDecision::Remove),
            ("project-a_b", '-', Some((false, '_')), vec![], CleanupDecision::Keep { motivation: "a".to_string() }),
            ("project-a_b", '-', Some((false, '_')), vec!["a".to_string()], CleanupDecision::Remove),
            ("project-a_b", '-', Some((false, '_')), vec!["b".to_string()], CleanupDecision::Keep { motivation: "a".to_string() }),
            ("project-a_b", '-', Some((false, '_')), vec!["a_b".to_string()], CleanupDecision::Keep { motivation: "a".to_string() }),
            // Mandatory remainder separator
            ("example", '-', Some((true, '_')), vec![], CleanupDecision::Ignore),
            ("example", '-', Some((true, '_')), vec!["a".to_string()], CleanupDecision::Ignore),
            ("project-a", '-', Some((true, '_')), vec![], CleanupDecision::Ignore),
            ("project-a", '-', Some((true, '_')), vec!["a".to_string()], CleanupDecision::Ignore),
            ("project-a_b", '-', Some((true, '_')), vec![], CleanupDecision::Keep { motivation: "a".to_string() }),
            ("project-a_b", '-', Some((true, '_')), vec!["a".to_string()], CleanupDecision::Remove),
            ("project-a_b", '-', Some((true, '_')), vec!["b".to_string()], CleanupDecision::Keep { motivation: "a".to_string() }),
            ("project-a_b", '-', Some((true, '_')), vec!["a_b".to_string()], CleanupDecision::Keep { motivation: "a".to_string() }),
        ].iter().enumerate() {
            assert_eq!(
                decide_cleanup(name, *project_separator, *remainder_separator, deletion),
                *expected,
                "test case {i} (zero-based index) fails"
            );
        }
    }
}
