use crate::compiler::util::{
    cleanup_specific_directories, cleanup_specific_files, copy_file, copy_file_if_checksum_differs,
    crate_name_pipeline_globals, crate_name_pipeline_main, create_dir_if_not_exists,
    create_new_file, create_new_file_with_content, decode_string_as_dir, read_file_content,
    read_file_content_bytes, recreate_dir, recreate_file_with_content, truncate_sha256_checksum,
    CleanupDecision, DirectoryContent, ProcessGroupTerminator, UtilError,
};
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::{CompilationProfile, RustCompilationInfo};
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
const CLEANUP_INTERVAL: Duration = Duration::from_secs(120);

/// Minimum time between when the Rust cleanup has detected a pipeline to be deleted until
/// its compilation artifacts are actually cleaned up. It is a minimum, as cleanup both
/// happens at an interval, and as well is interleaved with compilation which can take
/// a significant amount of time.
const CLEANUP_RETENTION: Duration = Duration::from_secs(3600);

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
                    RustCompilationCleanupError::Database(e) => {
                        error!("Rust compilation cleanup failed: database error occurred: {e}");
                    }
                    RustCompilationCleanupError::Utility(e) => {
                        error!("Rust compilation cleanup failed: utility error occurred: {e}");
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
    info!(
        "Rust compilation started: pipeline {} (program version: {})",
        pipeline.id, pipeline.program_version
    );

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
        Ok((
            program_binary_url,
            source_checksum,
            integrity_checksum,
            duration,
            compilation_info,
        )) => {
            info!(
                "Rust compilation success: pipeline {} (program version: {}) (took {:.2}s; source checksum: {}; integrity checksum: {})",
                pipeline.id,
                pipeline.program_version,
                duration.as_secs_f64(),
                truncate_sha256_checksum(&source_checksum),
                truncate_sha256_checksum(&integrity_checksum),
            );
            db.lock()
                .await
                .transit_program_status_to_success(
                    tenant_id,
                    pipeline.id,
                    pipeline.program_version,
                    &compilation_info,
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
            RustCompilationError::RustError(compilation_info) => {
                db.lock()
                    .await
                    .transit_program_status_to_rust_error(
                        tenant_id,
                        pipeline.id,
                        pipeline.program_version,
                        &compilation_info,
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
    main_rust: &str,
    udf_stubs: &str,
    udf_rust: &str,
    udf_toml: &str,
) -> String {
    let mut hasher = sha::Sha256::new();
    for (name, data) in [
        ("platform_version", platform_version.as_bytes()),
        ("profile", profile.to_string().as_bytes()),
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
    RustError(RustCompilationInfo),
    /// General system problem occurred (e.g., I/O error)
    SystemError(String),
}

/// Utility errors are system errors during Rust compilation.
impl From<UtilError> for RustCompilationError {
    fn from(value: UtilError) -> Self {
        RustCompilationError::SystemError(value.to_string())
    }
}

/// Performs the Rust compilation.
///
/// Returns the program binary URL, source checksum, integrity checksum,
/// duration, and compilation information.
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
) -> Result<(String, String, String, Duration, RustCompilationInfo), RustCompilationError> {
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
        &main_rust,
        &udf_stubs,
        udf_rust,
        udf_toml,
    );
    trace!("Rust compilation: calculated source checksum: {source_checksum}");

    // Prepare the workspace for the compilation of this specific pipeline
    prepare_workspace(config, pipeline_id, &main_rust, udf_rust, udf_toml).await?;

    // Perform the compilation in the workspace
    let (compilation_info, integrity_checksum) = call_compiler(
        db,
        tenant_id,
        pipeline_id,
        program_version,
        config,
        &source_checksum,
        &profile,
    )
    .await?;

    // URL where the program binary can be downloaded from
    let program_binary_url = format!(
        "http://{}:{}/binary/{}/{}/{}/{}",
        config.binary_ref_host,
        config.binary_ref_port,
        pipeline_id,
        program_version,
        source_checksum,
        integrity_checksum.clone()
    );

    Ok((
        program_binary_url,
        source_checksum,
        integrity_checksum.clone(),
        start.elapsed(),
        compilation_info,
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
}
"#;

/// Prepare the workspace for compilation of the specific pipeline.
/// This involves extracting the results of the prior SQL compilation and
/// configuring the workspace itself (e.g., its Cargo.toml and Cargo.lock).
async fn prepare_workspace(
    config: &CompilerConfig,
    pipeline_id: PipelineId,
    main_rust: &str,
    udf_rust: &str,
    udf_toml: &str,
) -> Result<(), RustCompilationError> {
    // Workspace directory
    let workspace_dir = config.working_dir().join("rust-compilation");
    create_dir_if_not_exists(&workspace_dir).await?;

    ////////////////////////////////
    // PHASE 1: EXTRACT AND MODIFY
    //
    // Extract the result from the SQL compilation, and make modifications:
    // (1) Set it to generate a binary instead of a library
    // (2) Set the user-provided UDF code and dependencies
    //
    // The modifications such that they are accounted for in the copy logic
    // in the second phase (in particular, checksums which determine whether
    // a copy needs to occur).

    // Extract the Rust generated by the SQL compilation
    let extract_dir = workspace_dir.join(".temp-extract");
    recreate_dir(&extract_dir).await?;
    decode_string_as_dir(main_rust, &extract_dir)?;

    // List all crates in extracted
    let mut list_crates = vec![];
    for (path, name, is_file) in DirectoryContent::new(&extract_dir).await?.content {
        if !is_file {
            list_crates.push((path, name));
        } else if name != "Cargo.toml" {
            return Err(RustCompilationError::SystemError(format!(
                "Unexpected directory entry: '{}'",
                path.to_string_lossy()
            )));
        }
    }
    let mut list_crate_names: Vec<String> =
        list_crates.iter().map(|(_, name)| name.clone()).collect();
    list_crate_names.sort();

    // Name of pipeline crates
    let pipeline_main_crate = crate_name_pipeline_main(pipeline_id);
    let pipeline_globals_crate = crate_name_pipeline_globals(pipeline_id);

    // Validate the crates
    if !list_crate_names.contains(&pipeline_main_crate) {
        return Err(RustCompilationError::SystemError(format!(
            "Missing required main crate: '{pipeline_main_crate}'"
        )));
    }
    if !list_crate_names.contains(&pipeline_globals_crate) {
        return Err(RustCompilationError::SystemError(format!(
            "Missing required globals crate: '{pipeline_globals_crate}'"
        )));
    }
    for crate_name in &list_crate_names {
        if !crate_name.starts_with("feldera_pipe_") {
            return Err(RustCompilationError::SystemError(format!(
                "Unknown crate '{}' not starting with 'feldera_pipe_'",
                crate_name
            )));
        }
    }

    // Pipeline main crate: src/lib.rs -> src/main.rs
    // ----------------------------------------------
    // Converted into a Rust file which specifies a main instead, the file is renamed.
    let lib_rs_path = extract_dir
        .join(&pipeline_main_crate)
        .join("src")
        .join("lib.rs");
    let main_rs_path = extract_dir
        .join(&pipeline_main_crate)
        .join("src")
        .join("main.rs");
    let mut main_rs_content = read_file_content(&lib_rs_path).await?;
    main_rs_content.push_str(MAIN_FUNCTION);
    #[cfg(feature = "feldera-enterprise")]
    main_rs_content.push_str("\nextern crate dbsp_enterprise;");
    create_new_file_with_content(&main_rs_path, &main_rs_content).await?;
    fs::remove_file(&lib_rs_path)
        .await
        .map_err(|e| UtilError::IoError(format!("removing file '{}'", lib_rs_path.display()), e))?;

    // Pipeline main crate: Cargo.toml
    // -------------------------------
    // By default, the main crate produces a library instead of a binary.
    // Rename the [lib] section to [[bin]], and changes the path therein.
    let cargo_toml_path = extract_dir.join(&pipeline_main_crate).join("Cargo.toml");
    let cargo_toml_content = read_file_content(&cargo_toml_path).await?.replace(
        "[lib]\npath = \"src/lib.rs\"",
        &format!("[[bin]]\nname = \"{pipeline_main_crate}\"\npath = \"src/main.rs\""),
    );
    recreate_file_with_content(&cargo_toml_path, &cargo_toml_content).await?;

    // Pipeline main crate: crates.json
    // --------------------------------
    // List all crates used for cleanup later.
    let crates_json_path = extract_dir.join(&pipeline_main_crate).join("crates.json");
    recreate_file_with_content(
        &crates_json_path,
        &serde_json::to_string_pretty(&list_crate_names).map_err(|e| {
            RustCompilationError::SystemError(format!("Unable to serialize crates list: {e}"))
        })?,
    )
    .await?;

    // Pipeline globals crate: src/udf.rs
    // ----------------------------------
    // Copy over the user-defined UDF code, overriding the pre-existing content.
    let udf_rs_path = extract_dir
        .join(&pipeline_globals_crate)
        .join("src")
        .join("udf.rs");
    recreate_file_with_content(&udf_rs_path, udf_rust).await?;

    // Pipeline globals crate: Cargo.toml
    // ----------------------------------
    // Copy over the user-defined UDF TOML dependencies.
    let udf_cargo_toml_path = extract_dir.join(&pipeline_globals_crate).join("Cargo.toml");
    let udf_cargo_toml_content = read_file_content(&udf_cargo_toml_path).await?.replace(
        "[dependencies]",
        &formatdoc! {"
            [dependencies]
            # START: UDF dependencies
            {udf_toml}
            # END: UDF dependencies
        "},
    );
    recreate_file_with_content(&udf_cargo_toml_path, &udf_cargo_toml_content).await?;

    ///////////////////////
    // PHASE 2: WORKSPACE
    //
    // (1) Copy over the modified extracted crates
    // (2) Generate the workspace Cargo.toml which targets the specific pipeline
    // (3) Copy over the Cargo.lock

    // Create the crates directory if it does not exist
    let crates_dir = workspace_dir.join("crates");
    create_dir_if_not_exists(&crates_dir).await?;

    // Copy over the content of the crates from the extracted archive
    for (source_crate_path, crate_name) in list_crates {
        let target_crate_path = crates_dir.join(&crate_name);
        create_dir_if_not_exists(&target_crate_path).await?;
        create_dir_if_not_exists(&target_crate_path.join("src")).await?;

        // Crate base: src/, Cargo.toml, crates.json
        let base_content = if crate_name.ends_with("_main") {
            vec![("src", false), ("Cargo.toml", true), ("crates.json", true)]
        } else {
            vec![("src", false), ("Cargo.toml", true)]
        };
        DirectoryContent::new(&source_crate_path)
            .await?
            .validate(&base_content)?;
        DirectoryContent::new(&target_crate_path)
            .await?
            .keep(
                &base_content,
                &vec![("stdout.log", true), ("stderr.log", true)],
            )
            .await?;

        // Crate base: copy files (src/ is the only directory)
        for (filename, is_file) in base_content {
            assert!(is_file || filename == "src");
            if is_file {
                copy_file_if_checksum_differs(
                    &source_crate_path.join(filename),
                    &target_crate_path.join(filename),
                )
                .await?;
            }
        }

        // Crate src/: main.rs, lib.rs, udf.rs, stubs.rs
        let src_content = if crate_name.ends_with("_main") {
            vec![("main.rs", true)]
        } else if crate_name.ends_with("_globals") {
            vec![("lib.rs", true), ("udf.rs", true), ("stubs.rs", true)]
        } else {
            vec![("lib.rs", true)]
        };
        DirectoryContent::new(&source_crate_path.join("src"))
            .await?
            .validate(&src_content)?;
        DirectoryContent::new(&target_crate_path.join("src"))
            .await?
            .keep(&src_content, &vec![])
            .await?;

        // Crate src/: copy content (should only be files)
        for (filename, is_file) in src_content {
            assert!(is_file);
            copy_file_if_checksum_differs(
                &source_crate_path.join("src").join(filename),
                &target_crate_path.join("src").join(filename),
            )
            .await?;
        }
    }

    // Workspace: Cargo.toml
    // ---------------------
    // Specifies the following:
    // - The pipeline crate to compile (changed per compilation)
    // - The possible profiles (same for all compilations)
    // - The dependencies used the other crates (same for all compilations)
    let cargo_toml = formatdoc! {r#"
        [workspace]
        members = [ "crates/{pipeline_main_crate}" ]
        resolver = "2"

        [profile.unoptimized]
        inherits = "release"
        opt-level = 0
        lto = "off"
        codegen-units = 256

        [profile.optimized]
        inherits = "release"

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

        [workspace.dependencies]
        paste = {{ version = "1.0.12" }}
        derive_more = {{ version = "0.99.17", features = ["add", "not", "from"] }}
        dbsp = {{ path = "{}/crates/dbsp", features = ["backend-mode"] }}
        dbsp_adapters = {{ path = "{}/crates/adapters" {}}}
        feldera-types = {{ path = "{}/crates/feldera-types" }}
        feldera-sqllib = {{ path = "{}/crates/sqllib" }}{}
        serde = {{ version = "1.0", features = ["derive"] }}
        compare = {{ version = "0.1.0" }}
        size-of = {{ version = "0.1.5", package = "feldera-size-of" }}
        serde_json = {{ version = "1.0.127", features = ["arbitrary_precision"] }}
        rkyv = {{ version = "0.7.45", default-features = false, features = ["std", "size_64"] }}
        tikv-jemallocator = {{ version = "0.6.0", features = ["profiling", "unprefixed_malloc_on_supported_platforms"] }}
    "#,
        config.dbsp_override_path, // Path: dbsp
        config.dbsp_override_path, // Path: dbsp_adapters
        if cfg!(feature = "feldera-enterprise") {
             // Enterprise features for: dbsp_adapters
             ", features = [\"feldera-enterprise\"] ".to_string()
        } else {
            "".to_string()
        },
        config.dbsp_override_path, // Path: feldera-types
        config.dbsp_override_path, // Path: feldera-sqllib
        if cfg!(feature = "feldera-enterprise") {
            // Enterprise crate: dbsp-enterprise
            format!("\ndbsp-enterprise = {{ path = \"{}/crates/dbsp-enterprise\" }}", config.dbsp_override_path)
        } else {
            "".to_string()
        }
    };
    let cargo_toml_file_path = workspace_dir.join("Cargo.toml");
    recreate_file_with_content(&cargo_toml_file_path, &cargo_toml).await?;

    // Workspace: Cargo.lock
    // ---------------------
    // Contains all the (indirect and direct) dependencies of the crates besides UDF.
    // The original is copied over each time such that the starting point is the same.
    let cargo_lock_source_path = Path::new(&config.compilation_cargo_lock_path);
    let cargo_lock_target_path = workspace_dir.join("Cargo.lock");
    copy_file(cargo_lock_source_path, &cargo_lock_target_path).await?;

    // Remove temporary extraction directory
    fs::remove_dir_all(&extract_dir).await.map_err(|e| {
        UtilError::IoError(format!("removing directory '{}'", extract_dir.display()), e)
    })?;

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
) -> Result<(RustCompilationInfo, String), RustCompilationError> {
    // Workspace directory
    let workspace_dir = config.working_dir().join("rust-compilation");
    if !workspace_dir.is_dir() {
        return Err(RustCompilationError::SystemError(format!(
            "Expected {} to be a directory and exist, but it is or does not",
            workspace_dir.display()
        )));
    }

    // Pipeline main crate directory
    let pipeline_main_crate_dir = workspace_dir
        .join("crates")
        .join(crate_name_pipeline_main(pipeline_id));
    if !pipeline_main_crate_dir.is_dir() {
        return Err(RustCompilationError::SystemError(format!(
            "Expected {} to be a directory and exist, but it is or does not",
            pipeline_main_crate_dir.display()
        )));
    }

    // Create pipeline-binaries directory if it does not yet exist
    let pipeline_binaries_dir = workspace_dir.join("pipeline-binaries");
    create_dir_if_not_exists(&pipeline_binaries_dir).await?;

    // Create file where stdout will be written to
    let stdout_file_path = pipeline_main_crate_dir.join("stdout.log");
    let stdout_file = create_new_file(&stdout_file_path).await?;

    // Create file where stderr will be written to
    let stderr_file_path = pipeline_main_crate_dir.join("stderr.log");
    let stderr_file = create_new_file(&stderr_file_path).await?;

    // By default, the command inherits all environment variables
    // from the originating process. This is fine when the compiler
    // server is run directly using its executable, but in the case
    // of being run from source using `cargo run`, this will result
    // in all kinds of cargo-related environment variables being set
    // such as `CARGO_PKG_VERSION`. This can cause unnecessary
    // recompilation for some dependency crates if they use
    // for example `cargo::rerun-if-env-changed=CARGO_PKG_VERSION`
    // in their build.rs, even if the actual `CARGO_PKG_VERSION`
    // they read does not change (as it is overwritten by cargo).
    //
    // Only a select number of environment variables are inherited
    // explicitly. All other inherited environment variables are cleared.
    let env_path = std::env::var_os("PATH").ok_or(RustCompilationError::SystemError(
        "The PATH environment variable is not set, which is needed to locate `cargo`".to_string(),
    ))?;
    let optional_env_rustflags = std::env::var_os("RUSTFLAGS");

    // Formulate command
    let mut command = Command::new("cargo");
    command.env_clear();
    command.env("PATH", env_path);
    if let Some(env_rustflags) = optional_env_rustflags {
        command.env("RUSTFLAGS", env_rustflags);
    }
    command
        // Set compiler stack size to 20MB (10x the default) to prevent
        // SIGSEGV when the compiler runs out of stack on large programs.
        .env("RUST_MIN_STACK", "20971520")
        .current_dir(&workspace_dir)
        .arg("build")
        .arg("--workspace")
        .arg("--profile")
        .arg(profile.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file.into_std().await))
        .stderr(Stdio::from(stderr_file.into_std().await))
        // Setting it to zero sets the process group ID to the PID.
        // This is done to be able to kill any subprocesses that are spawned.
        .process_group(0);

    // Start process
    let mut process = command.spawn().map_err(|e| {
        RustCompilationError::SystemError(
            UtilError::IoError("running 'cargo build'".to_string(), e).to_string(),
        )
    })?;

    // Retrieve process group ID and create a terminator
    // which ends the group when going out of scope.
    let Some(process_group) = process.id() else {
        return Err(RustCompilationError::SystemError(
            "unable to retrieve pid".to_string(),
        ));
    };
    let mut terminator = ProcessGroupTerminator::new("Rust compilation", process_group);

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
                    UtilError::IoError("waiting for 'cargo build'".to_string(), e).to_string(),
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
        return Err(RustCompilationError::TerminatedBySignal);
    };

    // Read stdout and stderr
    let stdout = read_file_content(&stdout_file_path).await?;
    let stderr = read_file_content(&stderr_file_path).await?;
    let compilation_info = RustCompilationInfo {
        exit_code,
        stdout,
        stderr,
    };

    // Compilation is successful if the return exit code is present and zero
    if exit_status.success() {
        // Source file
        let source_file_path = workspace_dir
            .join("target")
            .join(profile.to_target_folder())
            .join(crate_name_pipeline_main(pipeline_id));
        if !source_file_path.is_file() {
            return Err(RustCompilationError::SystemError(format!(
                "Rust compilation was successful but no binary was generated at '{}'",
                source_file_path.display()
            )));
        }

        // Integrity checksum of the source file
        let integrity_checksum =
            hex::encode(sha256(&read_file_content_bytes(&source_file_path).await?));

        // Destination file
        let target_file_path = pipeline_binaries_dir.join(format!(
            "pipeline_{pipeline_id}_v{program_version}_sc_{source_checksum}_ic_{integrity_checksum}"
        ));

        // Copy binary from Cargo target profile directory to the pipeline-binaries directory
        copy_file(&source_file_path, &target_file_path).await?;

        // Success
        Ok((compilation_info, integrity_checksum))
    } else {
        Err(RustCompilationError::RustError(compilation_info))
    }
}

/// Rust compilation cleanup possible error outcomes.
enum RustCompilationCleanupError {
    /// Database error occurred (e.g., lost connectivity).
    Database(DBError),
    /// Utility function problem occurred (e.g., I/O error)
    Utility(UtilError),
}

impl From<DBError> for RustCompilationCleanupError {
    fn from(value: DBError) -> Self {
        RustCompilationCleanupError::Database(value)
    }
}

impl From<UtilError> for RustCompilationCleanupError {
    fn from(value: UtilError) -> Self {
        RustCompilationCleanupError::Utility(value)
    }
}

/// Makes the cleanup decision based on the provided name of the file or directory.
fn decide_cleanup(
    name: &str,
    remainder_separator: Option<(bool, char)>,
    deletion: &[String],
) -> CleanupDecision {
    // Ignore anything which does not start with "feldera_pipe_" or "libfeldera_pipe_"
    if !name.starts_with("feldera_pipe_") && !name.starts_with("libfeldera_pipe_") {
        return CleanupDecision::Ignore;
    }

    // The checksum needs to be retrieved from the remainder
    let core_name = match remainder_separator {
        None => name,
        // The remainder split can either be required or optional.
        // If it is required, it will not return the entire remainder as default
        // if it cannot split the remainder by the separator.
        Some((required, separator)) => {
            let remainder_spl: Vec<&str> = name.splitn(2, separator).collect();
            if remainder_spl.len() == 2 {
                remainder_spl[0]
            } else if required {
                // If remainder is required for it to be considered,
                // it will be ignored as it doesn't match the criteria
                return CleanupDecision::Ignore;
            } else {
                name
            }
        }
    };

    // Final decision whether to keep or not is determined whether it is marked for deletion
    if deletion.contains(&core_name.to_string()) {
        CleanupDecision::Remove
    } else {
        CleanupDecision::Keep {
            motivation: core_name.to_string(),
        }
    }
}

/// Cleans up the Rust compilation working directory by removing binaries
/// and compilation artifacts of pipeline programs that no longer exist.
async fn cleanup_rust_compilation(
    config: &CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<(), RustCompilationCleanupError> {
    trace!("Performing Rust cleanup...");

    // Rust compilation directory
    let rust_compilation_dir = config.working_dir().join("rust-compilation");
    if !rust_compilation_dir.exists() {
        return Ok(());
    }

    ///////////////////////////////
    // PHASE 1: PIPELINE BINARIES
    // Only the latest version binaries of successfully compiled pipeline
    // programs are retained. Older version binaries are deleted.

    // Retrieve existing pipeline programs
    // (pipeline_id, program_version, program_binary_source_checksum, program_binary_integrity_checksum)
    let existing_pipeline_programs = db
        .lock()
        .await
        .list_pipeline_programs_across_all_tenants()
        .await?;

    // Clean up pipeline binaries
    // These are not subject to the retention period.
    let pipeline_binaries_dir = rust_compilation_dir.join("pipeline-binaries");
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

    ///////////////////////////////////
    // PHASE 2: COMPILATION ARTIFACTS
    // Remove the artifacts used during compilation itself, notably
    // the crates and the corresponding files in the target directories.

    // Retrieve all existing pipeline identifiers
    let existing_pipeline_ids: Vec<PipelineId> = db
        .lock()
        .await
        .list_pipeline_ids_across_all_tenants()
        .await?
        .into_iter()
        .map(|(_, pipeline_id)| pipeline_id)
        .collect();

    // Location of the cleanup state file
    let cleanup_state_file_path = rust_compilation_dir.join("target_cleanup.json");

    // Attempt to read cleanup state from file.
    // If it fails, an error is written to log and the cleanup state is wiped.
    // The wipe means that all retention expirations will be reset.
    // The cleanup state includes the artifact names that are planned
    // to be deleted: "<crate>" and "lib<crate>".
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

    // Determine all the crates and their artifact names that are currently in use
    let mut crates_in_use: HashSet<String> = HashSet::new();
    let mut artifacts_in_use: HashSet<String> = HashSet::new();
    let existing_main_crates: Vec<String> = existing_pipeline_ids
        .iter()
        .map(|pipeline_id| crate_name_pipeline_main(*pipeline_id))
        .collect();
    for main_crate_name in existing_main_crates {
        let path_crates_json = rust_compilation_dir
            .join("crates")
            .join(main_crate_name)
            .join("crates.json");
        // Inability to read the `crates.json` occurs if:
        // - The pipeline has not yet been Rust compiled
        // - The Rust compilation preparation was interrupted
        //   before it could be written fully
        //
        // In either case, the crates uniquely belonging to it will not
        // be marked as in-use and thus any of its artifacts will be marked
        // for deletion.
        let content_crates_json = read_file_content(&path_crates_json)
            .await
            .unwrap_or_else(|e| {
                trace!(
                    "Could not read '{}' due to: {e}",
                    path_crates_json.display()
                );
                "[]".to_string()
            });
        let crates_json: Vec<String> =
            serde_json::from_str(&content_crates_json).unwrap_or_else(|e| {
                warn!(
                    "Could not convert '{}' to JSON due to: {e}",
                    content_crates_json
                );
                vec![]
            });
        for crate_name in crates_json {
            crates_in_use.insert(crate_name.clone());
            artifacts_in_use.insert(crate_name.clone());
            artifacts_in_use.insert(format!("lib{crate_name}"));
        }
    }
    debug!(
        "Rust compilation cleanup: there are {} crates listed as in use (thus {} artifacts in use)",
        crates_in_use.len(),
        artifacts_in_use.len()
    );

    // Current timestamp at which the cleanup takes place
    let current_timestamp = SystemTime::now();

    // Remove any artifact which is in use again
    for artifact_name in &artifacts_in_use {
        if cleanup_state.remove(artifact_name).is_some() {
            trace!("Rust compilation cleanup: artifact '{artifact_name}' is in use again")
        }
    }

    // The ones that have been marked longer ago than the retention period will be deleted
    let mut deletion = HashSet::<String>::new();
    for (artifact_name, expiration) in cleanup_state.iter() {
        if current_timestamp
            .duration_since(*expiration)
            .is_ok_and(|duration| duration >= CLEANUP_RETENTION)
        {
            deletion.insert(artifact_name.clone());
            trace!("Rust compilation cleanup: retention for artifact '{}' has expired -- marked for deletion", artifact_name);
        }
    }
    let deletion: Vec<String> = deletion.into_iter().collect();

    // All found artifacts during the decisions, which is used later to update the cleanup state
    let mut found = Vec::<String>::new();

    // (1) Clean up crates directories
    let crates_dir = rust_compilation_dir.join("crates");
    if crates_dir.is_dir() {
        let deletion_clone = deletion.clone();
        found.append(
            &mut cleanup_specific_directories(
                "Rust compilation crates",
                &crates_dir,
                Arc::new(move |name: &str| decide_cleanup(name, None, &deletion_clone)),
                true,
            )
            .await?,
        );
    }

    // (2) For each possible compilation profile, clean up their target artifacts
    for profile in [
        CompilationProfile::Dev,
        CompilationProfile::Unoptimized,
        CompilationProfile::Optimized,
    ] {
        let target_profile_folder = profile.to_target_folder();
        let target_subdir = rust_compilation_dir
            .join("target")
            .join(target_profile_folder);
        if target_subdir.is_dir() {
            // target/<folder>: "<artifact name>.<other>" or else "<artifact name>"
            let deletion_clone = deletion.clone();
            found.append(
                &mut cleanup_specific_files(
                    &format!("Rust compilation target/{target_profile_folder}/"),
                    &target_subdir,
                    Arc::new(move |name: &str| {
                        decide_cleanup(name, Some((false, '.')), &deletion_clone)
                    }),
                    false,
                )
                .await?,
            );

            // target/<folder>/deps: "<artifact name>-<other>"
            let deps_dir = target_subdir.join("deps");
            if deps_dir.is_dir() {
                let deletion_clone = deletion.clone();
                found.append(
                    &mut cleanup_specific_files(
                        &format!("Rust compilation target/{target_profile_folder}/deps"),
                        &deps_dir,
                        Arc::new(move |name: &str| {
                            decide_cleanup(name, Some((true, '-')), &deletion_clone)
                        }),
                        false,
                    )
                    .await?,
                );
            }

            // target/<folder>/.fingerprint: "<artifact name>-<other>"
            let fingerprint_dir = target_subdir.join(".fingerprint");
            if fingerprint_dir.is_dir() {
                let deletion_clone = deletion.clone();
                found.append(
                    &mut cleanup_specific_directories(
                        &format!("Rust compilation target/{target_profile_folder}/.fingerprint"),
                        &fingerprint_dir,
                        Arc::new(move |name: &str| {
                            decide_cleanup(name, Some((true, '-')), &deletion_clone)
                        }),
                        false,
                    )
                    .await?,
                );
            }

            // target/<folder>/incremental: "<artifact name>-<other>"
            let incremental_dir = target_subdir.join("incremental");
            if incremental_dir.is_dir() {
                let deletion_clone = deletion.clone();
                found.append(
                    &mut cleanup_specific_directories(
                        &format!("Rust compilation target/{target_profile_folder}/incremental"),
                        &incremental_dir,
                        Arc::new(move |name: &str| {
                            decide_cleanup(name, Some((true, '-')), &deletion_clone)
                        }),
                        false,
                    )
                    .await?,
                );
            }
        }
    }

    // Any artifact name that was found which:
    // (1) is not an artifact in use
    // (2) AND is not already in the cleanup state
    // ... will be added to the cleanup state.
    let found = HashSet::<String>::from_iter(found.into_iter());
    for artifact_name in found.iter() {
        if !artifacts_in_use.contains(artifact_name) && !cleanup_state.contains_key(artifact_name) {
            let expiration_datetime: DateTime<Utc> = (current_timestamp + CLEANUP_RETENTION).into();
            trace!(
                "Rust compilation cleanup: artifact '{}' will be deleted some time after retention expires (currently: {})",
                artifact_name,
                expiration_datetime.format("%Y-%m-%d %H:%M:%S")
            );
            cleanup_state.insert(artifact_name.clone(), current_timestamp);
        }
    }

    // Remove any artifact in the cleanup state which is no longer found
    let mut artifacts_removed: u64 = 0;
    for artifact_name in cleanup_state.clone().keys() {
        if !found.contains(artifact_name) {
            artifacts_removed += 1;
            cleanup_state.remove(artifact_name);
        }
    }
    if artifacts_removed > 0 {
        info!(
            "Rust compilation cleanup: removed {artifacts_removed} artifacts; {} artifacts remain",
            found.len()
        );
    }

    // Attempt to write cleanup state to file.
    // If it fails, an error is written to log and the cleanup state changes will be lost.
    // The changes being lost means that new retention expirations are not set, and old ones remain.
    match serde_json::to_string_pretty(&cleanup_state) {
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
    use crate::auth::TenantRecord;
    use crate::compiler::rust_compiler::{calculate_source_checksum, decide_cleanup};
    use crate::compiler::rust_compiler::{prepare_workspace, MAIN_FUNCTION};
    use crate::compiler::test::{list_content_as_sorted_names, CompilerTest};
    use crate::compiler::util::{
        crate_name_pipeline_globals, crate_name_pipeline_main, read_file_content, CleanupDecision,
    };
    use crate::db::types::program::{CompilationProfile, ProgramStatus};
    use crate::db::types::utils::validate_program_info;
    use std::collections::HashSet;

    /// Tests the calculation of the source checksum based on the input.
    #[tokio::test]
    async fn source_checksum_calculation() {
        // Different input, different source checksum
        let mut seen = HashSet::<String>::new();
        let platform_versions = ["", "v1", "v2"];
        let profiles = [
            CompilationProfile::Dev,
            CompilationProfile::Optimized,
            CompilationProfile::Unoptimized,
        ];
        let content = ["", "a", "aa", "b", "c", "d", "e"];
        for (platform_version, profile, main_rust, udf_stubs, udf_rust, udf_toml) in itertools::iproduct!(
            platform_versions,
            profiles,
            content,
            content,
            content,
            content
        ) {
            let source_checksum = calculate_source_checksum(
                platform_version,
                &profile,
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

        // Same input, same source checksum
        let checksum1 = calculate_source_checksum(
            "v0",
            &CompilationProfile::Optimized,
            "main_rust",
            "udf_stubs",
            "udf_rust",
            "udf_toml",
        );
        let checksum2 = calculate_source_checksum(
            "v0",
            &CompilationProfile::Optimized,
            "main_rust",
            "udf_stubs",
            "udf_rust",
            "udf_toml",
        );
        assert_eq!(checksum1, checksum2);
    }

    /// Tests the workspace preparation helper function.
    #[tokio::test]
    async fn workspace_preparation() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;

        // Create a pipeline and SQL compile it
        let sql = "CREATE TABLE t1 (v1 INT);";
        let udf_rust = "abc";
        let udf_toml = "def";
        let pipeline_id = test
            .create_pipeline_with_udf(tenant_id, "p1", "v0", sql, udf_rust, udf_toml)
            .await;
        test.sql_compiler_tick().await;
        let pipeline_descr = test.get_pipeline(tenant_id, pipeline_id).await;
        assert_eq!(pipeline_descr.program_status, ProgramStatus::SqlCompiled);
        assert_eq!(pipeline_descr.program_code, sql);
        assert_eq!(pipeline_descr.udf_rust, udf_rust);
        assert_eq!(pipeline_descr.udf_toml, udf_toml);
        let program_info = validate_program_info(&pipeline_descr.program_info.unwrap()).unwrap();

        // Before, the rust-compilation directory will not exist
        assert!(!test.rust_workdir.exists());

        // Prepare the workspace directory using it
        prepare_workspace(
            &test.compiler_config,
            pipeline_id,
            &program_info.main_rust,
            &pipeline_descr.udf_rust,
            &pipeline_descr.udf_toml,
        )
        .await
        .unwrap();

        // Afterward, check for the presence of files/directories and their content

        // Main crate
        let main_crate_name = crate_name_pipeline_main(pipeline_id);
        let main_crate_path = test.rust_workdir.join("crates").join(&main_crate_name);
        assert!(main_crate_path.is_dir());
        assert_eq!(
            list_content_as_sorted_names(&main_crate_path).await,
            vec!["Cargo.toml", "crates.json", "src"]
        );
        assert_eq!(
            list_content_as_sorted_names(&main_crate_path.join("src")).await,
            vec!["main.rs"]
        );
        assert!(
            read_file_content(&main_crate_path.join("src").join("main.rs"))
                .await
                .unwrap()
                .contains(MAIN_FUNCTION)
        );

        // Globals crate
        let globals_crate_path = test
            .rust_workdir
            .join("crates")
            .join(crate_name_pipeline_globals(pipeline_id));
        assert!(globals_crate_path.is_dir());
        assert_eq!(
            list_content_as_sorted_names(&globals_crate_path).await,
            vec!["Cargo.toml", "src"]
        );
        assert_eq!(
            list_content_as_sorted_names(&globals_crate_path.join("src")).await,
            vec!["lib.rs", "stubs.rs", "udf.rs"]
        );
        assert_eq!(
            read_file_content(&globals_crate_path.join("src").join("stubs.rs"))
                .await
                .unwrap(),
            program_info.udf_stubs
        );
        assert_eq!(
            read_file_content(&globals_crate_path.join("src").join("udf.rs"))
                .await
                .unwrap(),
            pipeline_descr.udf_rust
        );
        assert!(read_file_content(&globals_crate_path.join("Cargo.toml"))
            .await
            .unwrap()
            .contains(&pipeline_descr.udf_toml));

        // Workspace-wide Cargo.toml
        let workspace_toml_file = test.rust_workdir.join("Cargo.toml");
        assert!(workspace_toml_file.is_file());
        assert!(read_file_content(&workspace_toml_file)
            .await
            .unwrap()
            .contains(&format!("members = [ \"crates/{main_crate_name}\" ]")));
    }

    /// Tests the cleanup decision helper function.
    #[tokio::test]
    #[rustfmt::skip]
    async fn cleanup_decision_helper() {
        for (i, (name, remainder_separator, deletion, expected)) in [
            // No remainder separator
            ("example", None, vec![], CleanupDecision::Ignore),
            ("feldera_pipe_a", None, vec![], CleanupDecision::Keep { motivation: "feldera_pipe_a".to_string() }),
            ("feldera_pipe_a-b", None, vec![], CleanupDecision::Keep { motivation: "feldera_pipe_a-b".to_string() }),
            ("feldera_pipe_a", None, vec!["feldera_pipe_a".to_string()], CleanupDecision::Remove),
            ("feldera_pipe_a-b", None, vec!["feldera_pipe_a".to_string()], CleanupDecision::Keep { motivation: "feldera_pipe_a-b".to_string() }),
            // Optional remainder separator
            ("example", Some((false, '-')), vec![], CleanupDecision::Ignore),
            ("feldera_pipe_a", Some((false, '-')), vec![], CleanupDecision::Keep { motivation: "feldera_pipe_a".to_string() }),
            ("feldera_pipe_a-b", Some((false, '-')), vec![], CleanupDecision::Keep { motivation: "feldera_pipe_a".to_string() }),
            ("feldera_pipe_a", Some((false, '-')), vec!["feldera_pipe_a".to_string()], CleanupDecision::Remove),
            ("feldera_pipe_a-b", Some((false, '-')), vec!["feldera_pipe_a".to_string()], CleanupDecision::Remove),
            // Mandatory remainder separator
            ("example", Some((true, '-')), vec![], CleanupDecision::Ignore),
            ("feldera_pipe_a", Some((true, '-')), vec![], CleanupDecision::Ignore),
            ("feldera_pipe_a-b", Some((true, '-')), vec![], CleanupDecision::Keep { motivation: "feldera_pipe_a".to_string() }),
            ("feldera_pipe_a", Some((true, '-')), vec!["feldera_pipe_a".to_string()], CleanupDecision::Ignore),
            ("feldera_pipe_a-b", Some((true, '-')), vec!["feldera_pipe_a".to_string()], CleanupDecision::Remove),
        ].iter().enumerate() {
            assert_eq!(
                decide_cleanup(name, *remainder_separator, deletion),
                *expected,
                "test case {i} (zero-based index) fails"
            );
        }
    }
}
