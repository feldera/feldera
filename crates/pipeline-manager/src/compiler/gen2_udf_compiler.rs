//! Rust compilation of a Gen-2 program, which builds only its Rust UDFs.
//!
//! Gen-2 runs Rust UDFs from a native shared library it loads into its process. The
//! `udf_rust` of a pipeline is a `cdylib` crate built against the `feldera-udf` SDK.

use crate::compiler::rust_compiler::{
    FileDeliveryMode, FileUploadMetadata, RustCompilationCleanupError, RustCompilationError,
    RustCompilationResult, cargo_command_with_minimal_env, checksum_fields,
    clear_target_dir_if_disk_full, deliver_binary, run_cargo_build,
};
use crate::compiler::sql_compiler::{deliver_gen2_program_info, gen2_has_rust_udfs};
use crate::compiler::util::{
    CleanupDecision, DecisionFn, DirectoryContent, checksum_file, cleanup_specific_directories,
    cleanup_specific_files, crate_name_pipeline_udf, create_dir_if_not_exists,
    write_file_if_content_differs,
};
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::{
    CompilationProfile, ProgramInfo, RuntimeSelector, RustCompilationInfo,
};
use crate::db::types::tenant::TenantId;
use crate::db::types::version::Version;
use crate::has_unstable_feature;
use feldera_types::config::{PipelineConfigProgramInfo, UdfLibrary};
use indoc::formatdoc;
use std::collections::HashSet;
use std::env::consts::{DLL_PREFIX, DLL_SUFFIX};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

/// The name a UDF crate's source has in panic locations and compiler diagnostics.
const UDF_SOURCE_NAME: &str = "udf.rs";

/// Name of the Gen-2 UDF workspace cleanup in the log.
const CLEANUP_NAME: &str = "Gen-2 UDF compilation";

/// Every compilation profile, whose target folders the cleanup sweeps.
const COMPILATION_PROFILES: [CompilationProfile; 4] = [
    CompilationProfile::Dev,
    CompilationProfile::Unoptimized,
    CompilationProfile::Optimized,
    CompilationProfile::OptimizedSymbols,
];

/// Workspace in which the UDF crates of Gen-2 pipelines are compiled.
pub(super) fn gen2_udf_workspace_dir(config: &CompilerConfig) -> PathBuf {
    config.working_dir().join("gen2-udf-compilation")
}

/// Compiles the Rust UDFs of a Gen-2 program into a shared library, and delivers the library
/// and the program info that references it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn perform_gen2_udf_compilation(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    db: Option<Arc<Mutex<StoragePostgres>>>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    pipeline_name_for_log: &str,
    program_version: Version,
    program_info: &ProgramInfo,
    profile: &CompilationProfile,
    udf_rust: &str,
    udf_toml: &str,
) -> Result<RustCompilationResult, RustCompilationError> {
    let start = Instant::now();

    // The SQL compiler completes a Gen-2 program without Rust UDFs, so reaching here
    // without them means the claim predicate is wrong.
    if !gen2_has_rust_udfs(udf_rust) {
        return Err(RustCompilationError::SystemError(
            "A Gen-2 program without Rust UDFs is completed by the SQL compiler and must not \
             reach Rust compilation"
                .to_string(),
        ));
    }
    let sdk_path = config.gen2_udf_sdk_path.as_deref();
    let (sdk_source, sdk_version) = match sdk_path {
        Some(sdk_path) => (
            format!("path:{sdk_path}"),
            gen2_udf_sdk_version(config, sdk_path).await?,
        ),
        None => (
            "crates.io".to_string(),
            config.gen2_udf_sdk_release().to_string(),
        ),
    };
    let source_checksum = calculate_gen2_udf_source_checksum(
        &common_config.platform_version,
        profile,
        &sdk_source,
        &sdk_version,
        udf_rust,
        udf_toml,
    );
    let crate_dir =
        prepare_gen2_udf_workspace(config, pipeline_id, sdk_path, udf_rust, udf_toml).await?;
    let rustc_result = build_udf_library(
        config,
        &crate_dir,
        profile,
        db,
        tenant_id,
        pipeline_id,
        pipeline_name_for_log,
        program_version,
    )
    .await?;
    let (udf_library, library_size) = deliver_udf_library(
        common_config,
        config,
        pipeline_id,
        program_version,
        &source_checksum,
        &udf_library_path(config, pipeline_id, profile),
    )
    .await?;
    let program_info = reference_udf_library(
        program_info.to_pipeline_config_program_info(),
        udf_library.clone(),
    )?;
    let program_info_integrity_checksum = deliver_gen2_program_info(
        common_config,
        config,
        pipeline_id,
        program_version,
        &program_info,
    )
    .await
    .map_err(RustCompilationError::SystemError)?;

    Ok(RustCompilationResult {
        source_checksum,
        binary_integrity_checksum: udf_library.integrity_checksum,
        binary_size: library_size,
        program_info_integrity_checksum,
        profile: profile.clone(),
        duration: start.elapsed(),
        rustc_result,
    })
}

/// Calculate Gen2 UDF checksum.
fn calculate_gen2_udf_source_checksum(
    platform_version: &str,
    profile: &CompilationProfile,
    sdk_source: &str,
    sdk_version: &str,
    udf_rust: &str,
    udf_toml: &str,
) -> String {
    let profile = profile.to_string();
    checksum_fields(&[
        ("platform_version", platform_version.as_bytes()),
        ("runtime_version", RuntimeSelector::Gen2(None).as_bytes()),
        ("profile", profile.as_bytes()),
        ("gen2_udf_sdk_source", sdk_source.as_bytes()),
        ("gen2_udf_sdk_version", sdk_version.as_bytes()),
        ("udf_rust", udf_rust.as_bytes()),
        ("udf_toml", udf_toml.as_bytes()),
    ])
}

/// Version of the UDF SDK crate in `sdk_path`, as declared in its `Cargo.toml`.
async fn gen2_udf_sdk_version(
    config: &CompilerConfig,
    sdk_path: &str,
) -> Result<String, RustCompilationError> {
    let manifest_path = Path::new(sdk_path).join("Cargo.toml");
    // Every cargo command of a UDF build runs in the UDF workspace.
    let workspace_dir = gen2_udf_workspace_dir(config);
    create_dir_if_not_exists(&workspace_dir).await?;
    let output = cargo_command_with_minimal_env()?
        .current_dir(&workspace_dir)
        .arg("metadata")
        .arg("--format-version=1")
        .arg("--no-deps")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .output()
        .await
        .map_err(|e| {
            RustCompilationError::SystemError(format!(
                "Unable to run 'cargo metadata' on the Gen-2 UDF SDK '{}': {e}",
                manifest_path.display()
            ))
        })?;
    if !output.status.success() {
        return Err(RustCompilationError::SystemError(format!(
            "'cargo metadata' failed on the Gen-2 UDF SDK '{}': {}",
            manifest_path.display(),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|e| {
        RustCompilationError::SystemError(format!(
            "'cargo metadata' printed invalid JSON for the Gen-2 UDF SDK: {e}"
        ))
    })?;
    // A workspace lists all its members; the SDK is the one with this manifest.
    metadata["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|package| {
            package["manifest_path"]
                .as_str()
                .is_some_and(|path| Path::new(path) == manifest_path)
        })
        .and_then(|package| package["version"].as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            RustCompilationError::SystemError(format!(
                "'cargo metadata' lists no package version for the Gen-2 UDF SDK '{}'",
                manifest_path.display()
            ))
        })
}

/// Writes the workspace manifest, the crate manifest and the crate source. Files are written
/// only when their content changes, so that cargo rebuilds only what did. Returns the crate
/// directory.
async fn prepare_gen2_udf_workspace(
    config: &CompilerConfig,
    pipeline_id: PipelineId,
    sdk_path: Option<&str>,
    udf_rust: &str,
    udf_toml: &str,
) -> Result<PathBuf, RustCompilationError> {
    let workspace_dir = gen2_udf_workspace_dir(config);
    let crate_name = crate_name_pipeline_udf(pipeline_id);
    let crate_dir = workspace_dir.join("crates").join(&crate_name);
    create_dir_if_not_exists(&crate_dir.join("src")).await?;

    // The logs of the previous build are removed, as the build creates them anew.
    DirectoryContent::new(&crate_dir)
        .await?
        .keep(
            &vec![("Cargo.toml", true), ("src", false)],
            &vec![("stdout.log", true), ("stderr.log", true)],
        )
        .await?;
    DirectoryContent::new(&crate_dir.join("src"))
        .await?
        .keep(&vec![("lib.rs", true)], &vec![])
        .await?;

    write_file_if_content_differs(
        &workspace_dir.join("Cargo.toml"),
        &udf_workspace_manifest(&crate_name),
    )
    .await?;
    write_file_if_content_differs(
        &crate_dir.join("Cargo.toml"),
        &udf_crate_manifest(
            &crate_name,
            sdk_path,
            config.gen2_udf_sdk_release(),
            udf_toml,
        ),
    )
    .await?;
    write_file_if_content_differs(&crate_dir.join("src").join("lib.rs"), udf_rust).await?;
    Ok(crate_dir)
}

/// Manifest of the workspace, which builds one UDF crate at a time. Cargo reads profiles
/// only from the workspace root, so the compilation profiles are defined here.
fn udf_workspace_manifest(crate_name: &str) -> String {
    formatdoc! {r#"
        # Generated by the Feldera compiler server
        [workspace]
        members = ["crates/{crate_name}"]
        resolver = "3"

        [profile.release]
        # Skip LTO to avoid rebuilding dependencies every time.
        lto = false
        # Catch panics for error reporting.
        panic = "unwind"

        [profile.unoptimized]
        inherits = "release"
        opt-level = 0
        codegen-units = 256

        [profile.optimized]
        inherits = "release"

        [profile.optimized_symbols]
        inherits = "release"
        debug = "line-tables-only"
    "#}
}

/// Manifest of the UDF crate of a pipeline. It depends on the local SDK at `sdk_path`, or else
/// on the SDK release `sdk_version`. The user's `udf_toml` extends its dependencies, between
/// the same markers as in the crates of other pipelines.
fn udf_crate_manifest(
    crate_name: &str,
    sdk_path: Option<&str>,
    sdk_version: &str,
    udf_toml: &str,
) -> String {
    let sdk_dependency = match sdk_path {
        Some(sdk_path) => format!("{{ path = {} }}", toml_basic_string(sdk_path)),
        None => format!("\"={sdk_version}\""),
    };
    formatdoc! {r#"
        # Generated by the Feldera compiler server
        [package]
        name = "{crate_name}"
        version = "0.1.0"
        edition = "2024"
        publish = false

        [lib]
        crate-type = ["cdylib"]

        [dependencies]
        feldera-udf = {sdk_dependency}
        # START: UDF dependencies
        {udf_toml}
        # END: UDF dependencies
    "#}
}

/// `value` quoted as a TOML basic string, escaping what would otherwise end the string.
fn toml_basic_string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            character if character.is_control() => {
                quoted.push_str(&format!("\\u{:04X}", u32::from(character)))
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

/// Builds the UDF crate in `crate_dir` into a shared library.
#[allow(clippy::too_many_arguments)]
async fn build_udf_library(
    config: &CompilerConfig,
    crate_dir: &Path,
    profile: &CompilationProfile,
    db: Option<Arc<Mutex<StoragePostgres>>>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    pipeline_name_for_log: &str,
    program_version: Version,
) -> Result<RustCompilationInfo, RustCompilationError> {
    let mut command = cargo_command_with_minimal_env()?;
    if let Some(env_rustflags) = std::env::var_os("RUSTFLAGS") {
        command.env("RUSTFLAGS", env_rustflags);
    }
    // To preserve panic locations and rustc's diagnostics, we use `cargo rustc` and remap the
    // UDF crate.
    let crate_name = crate_name_pipeline_udf(pipeline_id);
    command
        .current_dir(gen2_udf_workspace_dir(config))
        .arg("rustc")
        .arg("--package")
        .arg(&crate_name)
        .arg("--lib")
        .arg("--profile")
        .arg(profile.to_string())
        .arg("--message-format=json")
        .arg("--")
        .arg(format!(
            "--remap-path-prefix=crates/{crate_name}/src/lib.rs={UDF_SOURCE_NAME}"
        ));
    run_cargo_build(
        command,
        &crate_dir.join("stdout.log"),
        &crate_dir.join("stderr.log"),
        db,
        tenant_id,
        pipeline_id,
        pipeline_name_for_log,
        program_version,
    )
    .await
}

/// The file name cargo gives the shared library of crate `crate_name`. The UDF crate builds for
/// the host, so the host's naming applies: `lib<crate>.so` on Linux, `lib<crate>.dylib` on macOS.
fn library_file_name(crate_name: &str) -> String {
    format!("{DLL_PREFIX}{crate_name}{DLL_SUFFIX}")
}

/// Where cargo puts the UDF library of the pipeline built with `profile`.
fn udf_library_path(
    config: &CompilerConfig,
    pipeline_id: PipelineId,
    profile: &CompilationProfile,
) -> PathBuf {
    gen2_udf_workspace_dir(config)
        .join("target")
        .join(profile.to_target_folder())
        .join(library_file_name(&crate_name_pipeline_udf(pipeline_id)))
}

/// Delivers the UDF library at `library_path` the way a pipeline binary is delivered. Returns
/// its checksums and size in bytes.
async fn deliver_udf_library(
    common_config: &CommonConfig,
    config: &CompilerConfig,
    pipeline_id: PipelineId,
    program_version: Version,
    source_checksum: &str,
    library_path: &Path,
) -> Result<(UdfLibrary, usize), RustCompilationError> {
    if !library_path.is_file() {
        return Err(RustCompilationError::SystemError(format!(
            "Rust compilation was successful but no UDF library was generated at '{}'",
            library_path.display()
        )));
    }
    let (library_size, integrity_checksum) = checksum_file(library_path).await?;
    let pipeline_binaries_dir = config
        .working_dir()
        .join("rust-compilation")
        .join("pipeline-binaries");
    create_dir_if_not_exists(&pipeline_binaries_dir).await?;
    let metadata = FileUploadMetadata {
        pipeline_id,
        program_version,
        source_checksum: source_checksum.to_string(),
        integrity_checksum: integrity_checksum.clone(),
    };
    deliver_binary(
        common_config,
        &FileDeliveryMode::from_config(config),
        library_path,
        &metadata,
        &pipeline_binaries_dir,
    )
    .await?;
    let udf_library = UdfLibrary {
        source_checksum: source_checksum.to_string(),
        integrity_checksum,
    };
    Ok((udf_library, library_size))
}

/// Points the program IR at the delivered UDF library.
fn reference_udf_library(
    mut program_info: PipelineConfigProgramInfo,
    udf_library: UdfLibrary,
) -> Result<PipelineConfigProgramInfo, RustCompilationError> {
    let Some(program_ir) = program_info.program_ir.as_mut() else {
        return Err(RustCompilationError::SystemError(
            "The program information of a Gen-2 program carries no program IR to reference \
             the UDF library from. Recompile the pipeline to resolve this."
                .to_string(),
        ));
    };
    program_ir.udf_library = Some(udf_library);
    Ok(program_info)
}

/// Removes the UDF crates and build outputs of pipelines that no longer exist, and the
/// whole target directory when the disk is nearly full, as for the Rust compilation.
pub(super) async fn cleanup_gen2_udf_compilation(
    config: &CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<(), RustCompilationCleanupError> {
    let workspace_dir = gen2_udf_workspace_dir(config);
    if !workspace_dir.is_dir() {
        return Ok(());
    }
    if has_unstable_feature("rust_compiler_full_cleanup") {
        // The next build recreates whatever was removed.
        clear_target_dir_if_disk_full(&workspace_dir.join("target")).await;
    }
    let existing_crates: HashSet<String> = db
        .lock()
        .await
        .list_pipeline_ids_across_all_tenants()
        .await?
        .into_iter()
        .map(|(_, pipeline_id)| crate_name_pipeline_udf(pipeline_id))
        .collect();
    let decide: DecisionFn = Arc::new(move |name: &str, _metadata: Option<std::fs::Metadata>| {
        decide_gen2_udf_cleanup(name, &existing_crates)
    });

    let mut dirs = vec![workspace_dir.join("crates")];
    for profile in &COMPILATION_PROFILES {
        let profile_dir = workspace_dir
            .join("target")
            .join(profile.to_target_folder());
        dirs.extend([
            profile_dir.join("deps"),
            profile_dir.join(".fingerprint"),
            profile_dir.join("incremental"),
            profile_dir,
        ]);
    }
    for dir in dirs {
        if !dir.is_dir() {
            continue;
        }
        cleanup_specific_directories(CLEANUP_NAME, &dir, decide.clone(), false, false).await?;
        cleanup_specific_files(CLEANUP_NAME, &dir, decide.clone(), false, false).await?;
    }
    Ok(())
}

/// Removes an entry of a UDF crate whose pipeline no longer exists. Every entry of a UDF
/// crate starts with the crate name (the library with `lib` before it), followed by `.` and
/// an extension or `-` and a hash; entries of other crates are ignored.
fn decide_gen2_udf_cleanup(name: &str, existing_crates: &HashSet<String>) -> CleanupDecision {
    let name = name.strip_prefix("lib").unwrap_or(name);
    let crate_name = name.split(['.', '-']).next().unwrap_or(name);
    if !crate_name.starts_with("feldera_pipe_") || !crate_name.ends_with("_udf") {
        return CleanupDecision::Ignore;
    }
    if existing_crates.contains(crate_name) {
        CleanupDecision::Keep {
            motivation: crate_name.to_string(),
        }
    } else {
        CleanupDecision::Remove
    }
}

#[cfg(test)]
mod test {
    use super::{
        calculate_gen2_udf_source_checksum, cleanup_gen2_udf_compilation, decide_gen2_udf_cleanup,
        deliver_udf_library, gen2_udf_sdk_version, gen2_udf_workspace_dir, library_file_name,
        prepare_gen2_udf_workspace, reference_udf_library, toml_basic_string, udf_crate_manifest,
    };
    use crate::auth::TenantRecord;
    use crate::compiler::test::{CompilerTest, list_content_as_sorted_names};
    use crate::compiler::util::{
        CleanupDecision, crate_name_pipeline_udf, pipeline_binary_filename, read_file_content,
        sha256,
    };
    use crate::config::CompilerConfig;
    use crate::db::types::pipeline::PipelineId;
    use crate::db::types::program::CompilationProfile;
    use crate::db::types::version::Version;
    use feldera_types::config::{PipelineConfigProgramInfo, ProgramIr, UdfLibrary};
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime};
    use uuid::Uuid;

    fn config_in(working_dir: &Path) -> CompilerConfig {
        CompilerConfig {
            compiler_working_directory: working_dir.to_string_lossy().to_string(),
            compilation_profile: CompilationProfile::Optimized,
            sql_compiler_path: String::new(),
            sql_compiler_cache_url: String::new(),
            compilation_cargo_lock_path: String::new(),
            dbsp_override_path: String::new(),
            binary_upload_endpoint: None,
            binary_upload_timeout_secs: 600,
            binary_upload_max_retries: 3,
            binary_upload_retry_delay_ms: 1000,
            gen2_udf_sdk_path: None,
            gen2_udf_sdk_version: None,
            precompile: false,
        }
    }

    fn modified_time(path: &Path) -> SystemTime {
        std::fs::metadata(path).unwrap().modified().unwrap()
    }

    /// The workspace builds the pipeline's UDF crate alone, against the SDK, with the user's
    /// code and dependencies verbatim, and rewrites only the files whose content changes.
    #[tokio::test]
    async fn udf_workspace_holds_the_pipeline_crate() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = config_in(tempdir.path());
        let pipeline_id = PipelineId(Uuid::now_v7());
        let crate_name = crate_name_pipeline_udf(pipeline_id);
        let udf_rust = "use feldera_udf::prelude::*;\n";
        let udf_toml = "chrono = \"0.4\"\nserde = \"1\"";
        let prepare = |udf_rust: &'static str| {
            prepare_gen2_udf_workspace(&config, pipeline_id, Some("/opt/sdk"), udf_rust, udf_toml)
        };

        let crate_dir = prepare(udf_rust).await.unwrap();
        let workspace_dir = gen2_udf_workspace_dir(&config);
        assert_eq!(crate_dir, workspace_dir.join("crates").join(&crate_name));
        let workspace_manifest_path = workspace_dir.join("Cargo.toml");
        let crate_manifest_path = crate_dir.join("Cargo.toml");
        let lib_rs_path = crate_dir.join("src").join("lib.rs");

        let workspace_manifest = read_file_content(&workspace_manifest_path).await.unwrap();
        for line in [
            format!("members = [\"crates/{crate_name}\"]"),
            "panic = \"unwind\"".to_string(),
            "[profile.unoptimized]".to_string(),
            "[profile.optimized]".to_string(),
            "[profile.optimized_symbols]".to_string(),
        ] {
            assert!(workspace_manifest.contains(&line), "{workspace_manifest}");
        }
        let crate_manifest = read_file_content(&crate_manifest_path).await.unwrap();
        for line in [
            format!("name = \"{crate_name}\""),
            "edition = \"2024\"".to_string(),
            "crate-type = [\"cdylib\"]".to_string(),
            "feldera-udf = { path = \"/opt/sdk\" }".to_string(),
            format!("# START: UDF dependencies\n{udf_toml}\n# END: UDF dependencies"),
        ] {
            assert!(crate_manifest.contains(&line), "{crate_manifest}");
        }
        assert_eq!(read_file_content(&lib_rs_path).await.unwrap(), udf_rust);

        // Preparing the same program again rewrites nothing and removes the build logs.
        let long_ago = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        for path in [&workspace_manifest_path, &crate_manifest_path, &lib_rs_path] {
            let file = std::fs::File::options().write(true).open(path).unwrap();
            file.set_modified(long_ago).unwrap();
        }
        std::fs::write(crate_dir.join("stdout.log"), "").unwrap();
        std::fs::write(crate_dir.join("stderr.log"), "").unwrap();
        prepare(udf_rust).await.unwrap();
        for path in [&workspace_manifest_path, &crate_manifest_path, &lib_rs_path] {
            assert_eq!(
                modified_time(path),
                long_ago,
                "{} is rewritten",
                path.display()
            );
        }
        assert_eq!(
            list_content_as_sorted_names(&crate_dir).await,
            vec!["Cargo.toml", "src"]
        );

        // Changing the code rewrites the code alone.
        prepare("pub fn f() {}").await.unwrap();
        assert_eq!(
            read_file_content(&lib_rs_path).await.unwrap(),
            "pub fn f() {}"
        );
        assert_ne!(modified_time(&lib_rs_path), long_ago);
        assert_eq!(modified_time(&crate_manifest_path), long_ago);
    }

    /// Without a local SDK, a UDF crate depends on the configured crates.io release.
    #[test]
    fn udf_crate_depends_on_the_sdk_release_by_default() {
        let manifest = udf_crate_manifest("udf", None, "0.2.3", "");
        assert!(manifest.contains("feldera-udf = \"=0.2.3\""), "{manifest}");
    }

    /// The SDK path lands in the crate manifest as a TOML string, whatever it contains.
    #[test]
    fn sdk_path_is_quoted_as_a_toml_string() {
        assert_eq!(toml_basic_string("/opt/sdk"), r#""/opt/sdk""#);
        assert_eq!(toml_basic_string(r#"/a "b"\c"#), r#""/a \"b\"\\c""#);
        assert_eq!(toml_basic_string("/a\tb\n"), r#""/a\u0009b\u000A""#);
    }

    /// The source checksum is stable for the same inputs and changes with each of them.
    #[test]
    fn udf_source_checksum_tracks_every_input() {
        let checksum = calculate_gen2_udf_source_checksum;
        let optimized = &CompilationProfile::Optimized;
        let registry = "crates.io";
        let base = checksum("v0", optimized, registry, "0.1.0", "fn f() {}", "");
        assert_eq!(
            base,
            checksum("v0", optimized, registry, "0.1.0", "fn f() {}", "")
        );
        let variants = [
            base.clone(),
            checksum("v1", optimized, registry, "0.1.0", "fn f() {}", ""),
            checksum(
                "v0",
                &CompilationProfile::Dev,
                registry,
                "0.1.0",
                "fn f() {}",
                "",
            ),
            // A local SDK at the same version as the release.
            checksum("v0", optimized, "path:/opt/sdk", "0.1.0", "fn f() {}", ""),
            checksum("v0", optimized, registry, "0.2.0", "fn f() {}", ""),
            checksum("v0", optimized, registry, "0.1.0", "fn g() {}", ""),
            checksum(
                "v0",
                optimized,
                registry,
                "0.1.0",
                "fn f() {}",
                "chrono = \"0.4\"",
            ),
            // The same bytes split differently between neighboring fields.
            checksum("v0", optimized, registry, "0.1.0fn", " f() {}", ""),
        ];
        assert_eq!(
            variants.iter().collect::<HashSet<_>>().len(),
            variants.len()
        );
    }

    /// The SDK version is the one its manifest declares, read from the UDF workspace.
    #[tokio::test]
    async fn sdk_version_is_read_from_its_manifest() {
        let working_dir = tempfile::tempdir().unwrap();
        let config = config_in(working_dir.path());
        let sdk_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(sdk_dir.path().join("src")).unwrap();
        std::fs::write(sdk_dir.path().join("src").join("lib.rs"), "").unwrap();
        std::fs::write(
            sdk_dir.path().join("Cargo.toml"),
            "[package]\nname = \"feldera-udf\"\nversion = \"1.2.3\"\nedition = \"2024\"\n",
        )
        .unwrap();
        let sdk_path = std::fs::canonicalize(sdk_dir.path()).unwrap();
        assert_eq!(
            gen2_udf_sdk_version(&config, &sdk_path.to_string_lossy())
                .await
                .unwrap(),
            "1.2.3"
        );
        assert!(gen2_udf_workspace_dir(&config).is_dir());
        assert!(
            gen2_udf_sdk_version(&config, "/nonexistent-sdk")
                .await
                .is_err()
        );
    }

    /// The library is found under the name rustc gives a `cdylib` on this host.
    #[test]
    fn library_file_name_matches_rustc() {
        let crate_name = crate_name_pipeline_udf(PipelineId(Uuid::now_v7()));
        let output = std::process::Command::new("rustc")
            .args(["--crate-type", "cdylib", "--crate-name", &crate_name])
            .args(["--print", "file-names", "-"])
            .stdin(std::process::Stdio::null())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let rustc_name = String::from_utf8(output.stdout).unwrap();
        assert_eq!(rustc_name.trim(), library_file_name(&crate_name));
    }

    /// The library is stored under the name of a pipeline binary with its checksums.
    #[tokio::test]
    async fn udf_library_is_delivered_like_a_pipeline_binary() {
        let test = CompilerTest::new().await;
        let pipeline_id = PipelineId(Uuid::now_v7());
        let library_dir = tempfile::tempdir().unwrap();
        let library_path = library_dir.path().join("libudf.so");
        let library = b"\x7fELF\x02\x01\x01\0".as_slice();
        std::fs::write(&library_path, library).unwrap();
        let source_checksum = "5".repeat(64);

        let (udf_library, library_size) = deliver_udf_library(
            &test.common_config,
            &test.compiler_config,
            pipeline_id,
            Version(3),
            &source_checksum,
            &library_path,
        )
        .await
        .unwrap();
        assert_eq!(library_size, library.len());
        assert_eq!(
            udf_library,
            UdfLibrary {
                source_checksum: source_checksum.clone(),
                integrity_checksum: hex::encode(sha256(library)),
            }
        );
        let delivered = test
            .rust_workdir
            .join("pipeline-binaries")
            .join(pipeline_binary_filename(
                &pipeline_id,
                Version(3),
                &udf_library.source_checksum,
                &udf_library.integrity_checksum,
            ));
        assert_eq!(std::fs::read(delivered).unwrap(), library);

        let missing = library_dir.path().join("missing.so");
        let result = deliver_udf_library(
            &test.common_config,
            &test.compiler_config,
            pipeline_id,
            Version(3),
            &source_checksum,
            &missing,
        )
        .await;
        assert!(result.is_err());
    }

    /// The program IR references the library; a program info without IR is refused.
    #[test]
    fn udf_library_is_referenced_from_the_program_ir() {
        let udf_library = UdfLibrary {
            source_checksum: "50a1ce".to_string(),
            integrity_checksum: "ab0d1e".to_string(),
        };
        let program_info = PipelineConfigProgramInfo {
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            program_ir: Some(ProgramIr {
                mir: HashMap::new(),
                program_schema: json!({}),
                circuit_ir: Some(json!([])),
                udf_library: None,
            }),
        };
        let referenced = reference_udf_library(program_info.clone(), udf_library.clone()).unwrap();
        assert_eq!(
            referenced.program_ir.unwrap().udf_library,
            Some(udf_library.clone())
        );

        let without_ir = PipelineConfigProgramInfo {
            program_ir: None,
            ..program_info
        };
        assert!(reference_udf_library(without_ir, udf_library).is_err());
    }

    /// The build outputs of a deleted pipeline's UDF crate are removed, those of existing
    /// pipelines are kept, and those of the shared dependencies are left alone.
    #[test]
    fn udf_cleanup_decision() {
        let existing = crate_name_pipeline_udf(PipelineId(Uuid::now_v7()));
        let deleted = crate_name_pipeline_udf(PipelineId(Uuid::now_v7()));
        let existing_crates = HashSet::from([existing.clone()]);
        for (prefix, suffix) in [
            ("", ""),
            ("lib", ".so"),
            ("", ".d"),
            ("lib", ".d"),
            ("", "-0123456789abcdef"),
            ("", ".udf.0a1b-cgu.00.rcgu.o"),
        ] {
            assert_eq!(
                decide_gen2_udf_cleanup(&format!("{prefix}{existing}{suffix}"), &existing_crates),
                CleanupDecision::Keep {
                    motivation: existing.clone()
                },
                "{prefix}{suffix}"
            );
            assert_eq!(
                decide_gen2_udf_cleanup(&format!("{prefix}{deleted}{suffix}"), &existing_crates),
                CleanupDecision::Remove,
                "{prefix}{suffix}"
            );
        }
        for shared in [
            "arrow_array-0123456789abcdef",
            "libarrow_array-0123456789abcdef.rlib",
            ".cargo-lock",
            "deps",
            "feldera_pipe_pipeline_0_main",
        ] {
            assert_eq!(
                decide_gen2_udf_cleanup(shared, &existing_crates),
                CleanupDecision::Ignore,
                "{shared}"
            );
        }
    }

    /// The gen2 cleanup works as expected.
    #[tokio::test]
    async fn udf_cleanup_sweeps_the_workspace() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let existing_id = test.create_pipeline(tenant_id, "p1", "v0", "").await;
        let workspace_dir = gen2_udf_workspace_dir(&test.compiler_config);
        let target_dir = workspace_dir.join("target");
        // (entry, is_directory) for what builds of the crate leave behind.
        let entries_of = |crate_name: &str| -> Vec<(PathBuf, bool)> {
            let hashed = format!("{crate_name}-0123456789abcdef");
            let mut entries = vec![(workspace_dir.join("crates").join(crate_name), true)];
            for profile_dir in ["debug", "optimized"].map(|folder| target_dir.join(folder)) {
                entries.extend([
                    (profile_dir.join(library_file_name(crate_name)), false),
                    (
                        profile_dir.join("deps").join(format!("{crate_name}.d")),
                        false,
                    ),
                    (profile_dir.join(".fingerprint").join(&hashed), true),
                    (profile_dir.join("incremental").join(&hashed), true),
                ]);
            }
            entries
        };
        let shared = (
            target_dir
                .join("optimized")
                .join("deps")
                .join("libarrow_array-0a1b.rlib"),
            false,
        );
        let existing = entries_of(&crate_name_pipeline_udf(existing_id));
        let deleted = entries_of(&crate_name_pipeline_udf(PipelineId(Uuid::now_v7())));
        for (path, is_directory) in existing.iter().chain(&deleted).chain([&shared]) {
            if *is_directory {
                std::fs::create_dir_all(path).unwrap();
            } else {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, "").unwrap();
            }
        }

        if let Err(error) =
            cleanup_gen2_udf_compilation(&test.compiler_config, test.db.clone()).await
        {
            panic!("cleanup failed: {error}");
        }
        for (path, _) in existing.iter().chain([&shared]) {
            assert!(path.exists(), "{} is kept", path.display());
        }
        for (path, _) in &deleted {
            assert!(!path.exists(), "{} is removed", path.display());
        }
    }
}
