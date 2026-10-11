//! Rust compilation of a Gen-2 program, which builds only its Rust UDFs.
//!
//! The Gen-2 engine runs the circuit from the IR the SQL compiler emits, and runs Rust UDFs
//! from a WebAssembly module. The `udf_rust` of a pipeline becomes a `cdylib` crate built
//! against the `feldera-udf` SDK for `wasm32-wasip1`, in a workspace all UDF crates share so
//! that their common dependencies (the SDK and arrow) are built once. The module is embedded,
//! base64-encoded, in the program info, which remains the only artifact a Gen-2 pipeline
//! delivers.

use crate::compiler::rust_compiler::{
    RustCompilationCleanupError, RustCompilationError, RustCompilationResult,
    cargo_command_with_minimal_env, checksum_fields, run_cargo_build,
};
use crate::compiler::sql_compiler::{
    GEN2_UDF_SDK_NOT_CONFIGURED, deliver_gen2_program_info, gen2_has_rust_udfs,
};
use crate::compiler::util::{
    CleanupDecision, DecisionFn, DirectoryContent, cleanup_specific_directories,
    cleanup_specific_files, crate_name_pipeline_udf, create_dir_if_not_exists,
    read_file_content_bytes, sha256, write_file_if_content_differs,
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
use base64::prelude::{BASE64_STANDARD, Engine};
use feldera_types::config::PipelineConfigProgramInfo;
use indoc::formatdoc;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

/// Target triple of the WebAssembly modules the Gen-2 engine loads.
const UDF_WASM_TARGET: &str = "wasm32-wasip1";

/// Name of the Gen-2 UDF workspace cleanup in the log.
const CLEANUP_NAME: &str = "Gen-2 UDF compilation";

/// Workspace in which the UDF crates of Gen-2 pipelines are compiled. It is kept apart from
/// the Rust compilation workspace: UDF crates share no dependency with pipeline crates and
/// build for another target.
pub(super) fn gen2_udf_workspace_dir(config: &CompilerConfig) -> PathBuf {
    config.working_dir().join("gen2-udf-compilation")
}

/// Compiles the Rust UDFs of a Gen-2 program into a WebAssembly module, and delivers the
/// program info with the module embedded.
///
/// The binary checksums of the result describe the module, which is delivered as part of
/// the program info only. That artifact is named by its own integrity checksum, as for a
/// Gen-2 program without UDFs, which is the name the runner and the cleanup rebuild.
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
    // The SQL compiler refuses Rust UDFs while the SDK is unset, so it can only be missing
    // here if the compiler server restarted without it since.
    let Some(sdk_path) = config.gen2_udf_sdk_path.as_deref() else {
        return Err(RustCompilationError::SystemError(
            GEN2_UDF_SDK_NOT_CONFIGURED.to_string(),
        ));
    };

    let source_checksum = calculate_gen2_udf_source_checksum(
        &common_config.platform_version,
        sdk_path,
        udf_rust,
        udf_toml,
    );
    let crate_dir =
        prepare_gen2_udf_workspace(config, pipeline_id, sdk_path, udf_rust, udf_toml).await?;
    let rustc_result = build_udf_module(
        config,
        &crate_dir,
        db,
        tenant_id,
        pipeline_id,
        pipeline_name_for_log,
        program_version,
    )
    .await?;
    let udf_module = read_udf_module(config, pipeline_id).await?;
    let program_info =
        embed_udf_module(program_info.to_pipeline_config_program_info(), &udf_module)?;
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
        binary_integrity_checksum: hex::encode(sha256(&udf_module)),
        binary_size: udf_module.len(),
        program_info_integrity_checksum,
        // Only logged: the module always builds optimized, with the release profile.
        profile: CompilationProfile::Optimized,
        duration: start.elapsed(),
        rustc_result,
    })
}

/// Checksum of the inputs that determine the UDF module. The module always builds with the
/// release profile, so no profile enters it. The SDK enters by its location only: its
/// sources are not hashed, so an SDK replaced in place keeps the checksum.
fn calculate_gen2_udf_source_checksum(
    platform_version: &str,
    sdk_path: &str,
    udf_rust: &str,
    udf_toml: &str,
) -> String {
    checksum_fields(&[
        ("platform_version", platform_version.as_bytes()),
        ("runtime_version", RuntimeSelector::Gen2.as_bytes()),
        ("gen2_udf_sdk_path", sdk_path.as_bytes()),
        ("udf_rust", udf_rust.as_bytes()),
        ("udf_toml", udf_toml.as_bytes()),
    ])
}

/// Writes the workspace manifest, which names the UDF crate of the pipeline as its only
/// member, and that crate. Files are written only when their content changes, so that cargo
/// rebuilds only what did. Returns the crate directory.
async fn prepare_gen2_udf_workspace(
    config: &CompilerConfig,
    pipeline_id: PipelineId,
    sdk_path: &str,
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
        &udf_crate_manifest(&crate_name, sdk_path, udf_toml),
    )
    .await?;
    write_file_if_content_differs(&crate_dir.join("src").join("lib.rs"), udf_rust).await?;
    Ok(crate_dir)
}

/// Manifest of the workspace, which builds one UDF crate at a time. Cargo reads profiles
/// only from the workspace root, so the release profile of the modules is set here.
fn udf_workspace_manifest(crate_name: &str) -> String {
    formatdoc! {r#"
        # Generated by the Feldera compiler server
        [workspace]
        members = ["crates/{crate_name}"]
        resolver = "3"

        [profile.release]
        opt-level = 3
        # The UDF crate is rebuilt on every program change, while the SDK and arrow are
        # built once into the shared target directory. LTO would re-optimize all of them
        # at every rebuild, so the module keeps per-crate optimization and parallel codegen.
        lto = false
        codegen-units = 16
        panic = "abort"
        # Shrinks the module, which travels base64-encoded inside the program info.
        strip = "debuginfo"
    "#}
}

/// Manifest of the UDF crate of a pipeline. The user's `udf_toml` extends its dependencies,
/// between the same markers as in the crates of other pipelines.
fn udf_crate_manifest(crate_name: &str, sdk_path: &str, udf_toml: &str) -> String {
    let sdk_path = toml_basic_string(sdk_path);
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
        feldera-udf = {{ path = {sdk_path} }}
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

/// Builds the UDF crate in `crate_dir` for the WebAssembly target, keeping the output of
/// cargo in the crate directory.
async fn build_udf_module(
    config: &CompilerConfig,
    crate_dir: &Path,
    db: Option<Arc<Mutex<StoragePostgres>>>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    pipeline_name_for_log: &str,
    program_version: Version,
) -> Result<RustCompilationInfo, RustCompilationError> {
    // `RUSTFLAGS` is not passed on: it is set for native builds, and native linker flags
    // such as `-fuse-ld=mold` fail the WebAssembly link.
    let mut command = cargo_command_with_minimal_env()?;
    command
        .current_dir(gen2_udf_workspace_dir(config))
        .arg("build")
        .arg("--workspace")
        .arg("--release")
        .arg("--target")
        .arg(UDF_WASM_TARGET);
    let result = run_cargo_build(
        command,
        &crate_dir.join("stdout.log"),
        &crate_dir.join("stderr.log"),
        db,
        tenant_id,
        pipeline_id,
        pipeline_name_for_log,
        program_version,
    )
    .await;
    report_missing_wasm_target(result)
}

/// Turns a build that failed for want of the WebAssembly target into a system error, since
/// the compiler server is what needs fixing, not the program.
fn report_missing_wasm_target(
    result: Result<RustCompilationInfo, RustCompilationError>,
) -> Result<RustCompilationInfo, RustCompilationError> {
    match result {
        Err(RustCompilationError::RustError(info))
            if info.stderr.contains("target may not be installed") =>
        {
            Err(RustCompilationError::SystemError(format!(
                "The compiler server cannot build Rust UDFs for the Gen-2 engine because the \
                 '{UDF_WASM_TARGET}' Rust target is not installed. Install it with \
                 'rustup target add {UDF_WASM_TARGET}'.\n\n{}",
                info.stderr
            )))
        }
        result => result,
    }
}

/// Reads the module that cargo built from the UDF crate of the pipeline.
async fn read_udf_module(
    config: &CompilerConfig,
    pipeline_id: PipelineId,
) -> Result<Vec<u8>, RustCompilationError> {
    let module_path = gen2_udf_workspace_dir(config)
        .join("target")
        .join(UDF_WASM_TARGET)
        .join("release")
        .join(format!("{}.wasm", crate_name_pipeline_udf(pipeline_id)));
    if !module_path.is_file() {
        return Err(RustCompilationError::SystemError(format!(
            "Rust compilation was successful but no UDF module was generated at '{}'",
            module_path.display()
        )));
    }
    Ok(read_file_content_bytes(&module_path).await?)
}

/// Attaches the UDF module to the program IR, from which the Gen-2 engine loads it.
fn embed_udf_module(
    mut program_info: PipelineConfigProgramInfo,
    udf_module: &[u8],
) -> Result<PipelineConfigProgramInfo, RustCompilationError> {
    let Some(program_ir) = program_info.program_ir.as_mut() else {
        return Err(RustCompilationError::SystemError(
            "The program information of a Gen-2 program carries no program IR to attach the \
             UDF module to. Recompile the pipeline to resolve this."
                .to_string(),
        ));
    };
    program_ir.udf_wasm = Some(BASE64_STANDARD.encode(udf_module));
    Ok(program_info)
}

/// Removes the UDF crates and build outputs of pipelines that no longer exist. The
/// dependencies all UDF crates share stay in the target directory.
pub(super) async fn cleanup_gen2_udf_compilation(
    config: &CompilerConfig,
    db: Arc<Mutex<StoragePostgres>>,
) -> Result<(), RustCompilationCleanupError> {
    let workspace_dir = gen2_udf_workspace_dir(config);
    if !workspace_dir.is_dir() {
        return Ok(());
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

    let release_dir = workspace_dir
        .join("target")
        .join(UDF_WASM_TARGET)
        .join("release");
    for dir in [
        workspace_dir.join("crates"),
        release_dir.join("deps"),
        release_dir.join(".fingerprint"),
        release_dir.join("incremental"),
        release_dir,
    ] {
        if !dir.is_dir() {
            continue;
        }
        cleanup_specific_directories(CLEANUP_NAME, &dir, decide.clone(), false, false).await?;
        cleanup_specific_files(CLEANUP_NAME, &dir, decide.clone(), false, false).await?;
    }
    Ok(())
}

/// Removes an entry of a UDF crate whose pipeline no longer exists. Every entry of a UDF
/// crate starts with the crate name, followed by `.` and an extension or `-` and a hash;
/// entries of other crates are ignored.
fn decide_gen2_udf_cleanup(name: &str, existing_crates: &HashSet<String>) -> CleanupDecision {
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
        UDF_WASM_TARGET, calculate_gen2_udf_source_checksum, cleanup_gen2_udf_compilation,
        decide_gen2_udf_cleanup, embed_udf_module, gen2_udf_workspace_dir,
        prepare_gen2_udf_workspace, report_missing_wasm_target, toml_basic_string,
    };
    use crate::auth::TenantRecord;
    use crate::compiler::rust_compiler::RustCompilationError;
    use crate::compiler::test::{CompilerTest, list_content_as_sorted_names};
    use crate::compiler::util::{
        CleanupDecision, crate_name_pipeline_udf, program_info_filename, read_file_content, sha256,
    };
    use crate::config::CompilerConfig;
    use crate::db::types::pipeline::PipelineId;
    use crate::db::types::program::RustCompilationInfo;
    use crate::db::types::program::{CompilationProfile, ProgramStatus};
    use base64::prelude::{BASE64_STANDARD, Engine};
    use feldera_types::config::{PipelineConfigProgramInfo, ProgramIr};
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
            prepare_gen2_udf_workspace(&config, pipeline_id, "/opt/sdk", udf_rust, udf_toml)
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
            "[profile.release]".to_string(),
            "panic = \"abort\"".to_string(),
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
        let base = checksum("v0", "/sdk", "fn f() {}", "");
        assert_eq!(base, checksum("v0", "/sdk", "fn f() {}", ""));
        let variants = [
            base.clone(),
            checksum("v1", "/sdk", "fn f() {}", ""),
            checksum("v0", "/sdk2", "fn f() {}", ""),
            checksum("v0", "/sdk", "fn g() {}", ""),
            checksum("v0", "/sdk", "fn f() {}", "chrono = \"0.4\""),
            // The same bytes split differently between neighboring fields.
            checksum("v0", "/sdkfn", " f() {}", ""),
        ];
        assert_eq!(
            variants.iter().collect::<HashSet<_>>().len(),
            variants.len()
        );
    }

    /// The module travels base64-encoded in the program IR; a program info without IR has
    /// nowhere to carry it and is refused.
    #[test]
    fn udf_module_is_attached_to_the_program_ir() {
        let udf_module = b"\0asm\x01\0\0\0";
        let program_info = PipelineConfigProgramInfo {
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            program_ir: Some(ProgramIr {
                mir: HashMap::new(),
                program_schema: json!({}),
                circuit_ir: Some(json!([])),
                udf_wasm: None,
            }),
        };
        let embedded = embed_udf_module(program_info.clone(), udf_module).unwrap();
        let udf_wasm = embedded.program_ir.unwrap().udf_wasm.unwrap();
        assert_eq!(BASE64_STANDARD.decode(udf_wasm).unwrap(), udf_module);

        let without_ir = PipelineConfigProgramInfo {
            program_ir: None,
            ..program_info
        };
        assert!(embed_udf_module(without_ir, udf_module).is_err());
    }

    /// A toolchain without the WebAssembly target is reported as a system error that says how
    /// to install it, while the program's own compilation errors stay Rust errors.
    #[test]
    fn missing_wasm_target_is_a_system_error() {
        // Captured from cargo with a toolchain that lacks the target.
        let missing_target = RustCompilationInfo {
            exit_code: 101,
            stdout: String::new(),
            stderr: indoc::indoc! {"
                error[E0463]: can't find crate for `std`
                  |
                  = note: the `wasm32-wasip1` target may not be installed
                  = help: consider downloading the target with `rustup target add wasm32-wasip1`
            "}
            .to_string(),
        };
        let Err(RustCompilationError::SystemError(message)) =
            report_missing_wasm_target(Err(RustCompilationError::RustError(missing_target)))
        else {
            panic!("a missing target is the compiler server's to fix");
        };
        assert!(
            message.contains("rustup target add wasm32-wasip1"),
            "{message}"
        );

        let program_error = RustCompilationInfo {
            exit_code: 101,
            stdout: String::new(),
            stderr: "error: expected one of `!` or `::`, found `Jones`".to_string(),
        };
        assert!(matches!(
            report_missing_wasm_target(Err(RustCompilationError::RustError(program_error))),
            Err(RustCompilationError::RustError(_))
        ));
    }

    /// The build outputs of a deleted pipeline's UDF crate are removed, those of existing
    /// pipelines are kept, and those of the shared dependencies are left alone.
    #[test]
    fn udf_cleanup_decision() {
        let existing = crate_name_pipeline_udf(PipelineId(Uuid::now_v7()));
        let deleted = crate_name_pipeline_udf(PipelineId(Uuid::now_v7()));
        let existing_crates = HashSet::from([existing.clone()]);
        for suffix in [
            "",
            ".wasm",
            ".d",
            "-0123456789abcdef",
            ".udf.0a1b-cgu.00.rcgu.o",
        ] {
            assert_eq!(
                decide_gen2_udf_cleanup(&format!("{existing}{suffix}"), &existing_crates),
                CleanupDecision::Keep {
                    motivation: existing.clone()
                },
                "{suffix}"
            );
            assert_eq!(
                decide_gen2_udf_cleanup(&format!("{deleted}{suffix}"), &existing_crates),
                CleanupDecision::Remove,
                "{suffix}"
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

    /// The cleanup reaches the crates and every target directory a UDF build writes to.
    #[tokio::test]
    async fn udf_cleanup_sweeps_the_workspace() {
        let test = CompilerTest::new().await;
        let tenant_id = TenantRecord::default().id;
        let existing_id = test.create_pipeline(tenant_id, "p1", "v0", "").await;
        let workspace_dir = gen2_udf_workspace_dir(&test.compiler_config);
        let release_dir = workspace_dir
            .join("target")
            .join(UDF_WASM_TARGET)
            .join("release");
        // (entry, is_directory) for what a build of the crate leaves behind.
        let entries_of = |crate_name: &str| -> Vec<(PathBuf, bool)> {
            let hashed = format!("{crate_name}-0123456789abcdef");
            vec![
                (workspace_dir.join("crates").join(crate_name), true),
                (release_dir.join(format!("{crate_name}.wasm")), false),
                (
                    release_dir.join("deps").join(format!("{crate_name}.d")),
                    false,
                ),
                (release_dir.join(".fingerprint").join(&hashed), true),
                (release_dir.join("incremental").join(&hashed), true),
            ]
        };
        let shared = (
            release_dir.join("deps").join("libarrow_array-0a1b.rlib"),
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

    /// A Gen-2 program's Rust UDFs compile into a module that its delivered program info
    /// carries, and a UDF that does not compile fails with rustc's diagnostics. Runs cargo,
    /// so only where `GEN2_UDF_SDK_PATH` names the SDK.
    #[tokio::test]
    async fn gen2_rust_udfs_compile_to_a_wasm_module() {
        let Some(sdk_path) = std::env::var_os("GEN2_UDF_SDK_PATH") else {
            println!("Skipping: GEN2_UDF_SDK_PATH is not set to the feldera-udf SDK directory");
            return;
        };
        crate::enable_test_unstable_features();
        let mut test = CompilerTest::new().await;
        let sdk_path = std::fs::canonicalize(sdk_path).unwrap();
        test.compiler_config.gen2_udf_sdk_path = Some(sdk_path.to_string_lossy().to_string());
        let tenant_id = TenantRecord::default().id;
        let gen2 = json!({ "runtime_version": "gen2" });
        let program_code = indoc::indoc! {"
            CREATE FUNCTION double_value(x DOUBLE) RETURNS DOUBLE;
            CREATE TABLE t (x DOUBLE);
            CREATE VIEW v AS SELECT double_value(x) AS y FROM t;
        "};
        let udf_rust = indoc::indoc! {"
            use feldera_udf::prelude::*;

            #[feldera_udf]
            pub fn double_value(input: &Float64Array) -> Float64Array {
                input.iter().map(|x| x.map(|v| v * 2.0)).collect()
            }
        "};

        let pipeline_id = test
            .create_pipeline_with_program_config(
                tenant_id,
                "p1",
                "v0",
                program_code,
                udf_rust,
                "",
                gen2.clone(),
            )
            .await;
        test.sql_compiler_tick().await;
        test.rust_compiler_tick().await;
        let pipeline = test.get_pipeline(tenant_id, pipeline_id).await;
        assert_eq!(
            pipeline.program_status,
            ProgramStatus::Success,
            "{:?}",
            pipeline.program_error
        );
        let rust_compilation = pipeline.program_error.rust_compilation.unwrap();
        assert_eq!(rust_compilation.exit_code, 0, "the cargo output is kept");

        // The runner fetches the program info by its integrity checksum on both halves.
        let program_info_checksum = pipeline.program_info_integrity_checksum.unwrap();
        let delivered = read_file_content(&test.rust_workdir.join("pipeline-binaries").join(
            program_info_filename(
                &pipeline_id,
                pipeline.program_version,
                &program_info_checksum,
                &program_info_checksum,
            ),
        ))
        .await
        .unwrap();
        let program_info: PipelineConfigProgramInfo = serde_json::from_str(&delivered).unwrap();
        let udf_wasm = program_info.program_ir.unwrap().udf_wasm.unwrap();
        let udf_module = BASE64_STANDARD.decode(udf_wasm).unwrap();
        assert!(udf_module.starts_with(b"\0asm"), "a WebAssembly module");
        assert_eq!(
            pipeline.program_binary_integrity_checksum,
            Some(hex::encode(sha256(&udf_module)))
        );

        let broken_id = test
            .create_pipeline_with_program_config(
                tenant_id,
                "p2",
                "v0",
                program_code,
                "Davy Jones",
                "",
                gen2,
            )
            .await;
        test.sql_compiler_tick().await;
        test.rust_compiler_tick().await;
        let broken = test.get_pipeline(tenant_id, broken_id).await;
        assert_eq!(broken.program_status, ProgramStatus::RustError);
        let stderr = broken.program_error.rust_compilation.unwrap().stderr;
        assert!(stderr.contains("Davy Jones"), "{stderr}");
    }
}
