use crate::auth::TenantId;
use crate::config::{CompilationProfile, CompilerConfig};
use crate::db::storage::Storage;
use crate::db::{DBError, ProgramDescr, ProgramId, ProjectDB, Version};
use crate::error::ManagerError;
use crate::probe::Probe;
use actix_files::NamedFile;
use actix_web::{get, web, HttpRequest, HttpServer, Responder};
use futures_util::join;
use log::warn;
use log::{debug, error, info, trace};
use once_cell::sync::Lazy;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::{Registry, Unit};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Instant;
use std::{
    process::{ExitStatus, Stdio},
    sync::Arc,
};
use tokio::fs::DirEntry;
use tokio::io::AsyncWriteExt;
use tokio::{
    fs,
    fs::{File, OpenOptions},
    process::{Child, Command},
    select, spawn,
    sync::Mutex,
    time::{sleep, Duration},
};
use utoipa::ToSchema;
use uuid::Uuid;

/// The frequency with which the compiler polls the project database
/// for new compilation requests.
const COMPILER_POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// The frequency with which the GC task checks for no longer used binaries
/// against the database and removes the orphans.
///
/// # TODO
/// Artifcially low limit at the moment since this is new code. Increase in the
/// future.
const GC_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// A SQL compiler error.
///
/// The SQL compiler returns a list of errors in the following JSON format if
/// it's invoked with the `-je` option.
///
/// ```ignore
///  [ {
/// "startLineNumber" : 14,
/// "startColumn" : 13,
/// "endLineNumber" : 14,
/// "endColumn" : 13,
/// "warning" : false,
/// "errorType" : "Error parsing SQL",
/// "message" : "Encountered \"<EOF>\" at line 14, column 13."
/// } ]
/// ```
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "camelCase")]
pub(crate) struct SqlCompilerMessage {
    start_line_number: usize,
    start_column: usize,
    end_line_number: usize,
    end_column: usize,
    warning: bool,
    error_type: String,
    message: String,
}

/// Program compilation status.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) enum ProgramStatus {
    /// Compilation request received from the user; program has been placed
    /// in the queue.
    Pending,
    /// Compilation of SQL -> Rust in progress.
    CompilingSql,
    /// Compiling Rust -> executable in progress.
    CompilingRust,
    /// Compilation succeeded.
    #[cfg_attr(test, proptest(weight = 2))]
    Success,
    /// SQL compiler returned an error.
    SqlError(Vec<SqlCompilerMessage>),
    /// Rust compiler returned an error.
    RustError(String),
    /// System/OS returned an error when trying to invoke commands.
    SystemError(String),
}

impl ProgramStatus {
    /// Return true if program has been successfully compiled
    pub(crate) fn is_compiled(&self) -> bool {
        *self == ProgramStatus::Success
    }

    /// Return true if the program has failed to compile (for any reason).
    pub(crate) fn has_failed_to_compile(&self) -> bool {
        matches!(
            self,
            ProgramStatus::SqlError(_)
                | ProgramStatus::RustError(_)
                | ProgramStatus::SystemError(_)
        )
    }

    /// Return true if program is currently compiling.
    pub(crate) fn is_compiling(&self) -> bool {
        *self == ProgramStatus::CompilingRust || *self == ProgramStatus::CompilingSql
    }
}

/// Program configuration.
#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize, Deserialize, ToSchema)]
pub struct ProgramConfig {
    /// Compilation profile.
    /// If none is specified, the compiler default compilation profile is used.
    pub profile: Option<CompilationProfile>,
}

static METRICS: Lazy<CompilerMetrics> = Lazy::new(init_metrics);

/// Compiler metrics to track the number of invocations and the latency, broken
/// down by label, which is a pair of phase (SQL vs Rust) and status
/// (success/error).
pub struct CompilerMetrics {
    invocations: Family<MetricLabel, Counter>,
    latency: Family<MetricLabel, Histogram>,
}

/// We break down metrics in this file by the compiler phase and exit status
/// These types need to implement EncodeLabelSet to allow the registry to render
/// them for metrics scrape endpoints
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct MetricLabel {
    stage: StageType,
    status: Status,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum StageType {
    Sql,
    Rust,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum Status {
    Success,
    Error,
}

fn init_metrics() -> CompilerMetrics {
    CompilerMetrics {
        invocations: Family::<MetricLabel, Counter>::default(),
        latency: Family::<MetricLabel, Histogram>::new_with_constructor(|| {
            // These are latency buckets for measuring SQL and rust compilation times.
            let buckets = [1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 400.0];
            Histogram::new(buckets.into_iter())
        }),
    }
}

pub fn register_metrics(registry: &mut Registry) {
    registry.register(
        "stage_invocations",
        "Number of compiler invocations by stage (SQL vs Rust) and status (success vs error)",
        METRICS.invocations.clone(),
    );
    registry.register_with_unit(
        "latency",
        "Compilation latency by stage (SQL vs Rust) and status (success vs error)",
        Unit::Seconds,
        METRICS.latency.clone(),
    );
}

fn record(stage: StageType, status: Status, elapsed: f64) {
    let label = MetricLabel { stage, status };
    METRICS.invocations.get_or_create(&label).inc();
    METRICS.latency.get_or_create(&label).observe(elapsed);
}

pub struct Compiler {}

/// The `main` function injected in each generated pipeline
/// crate.
const MAIN_FUNCTION: &str = r#"
fn main() {
    dbsp_adapters::server::server_main(|cconfig| {
        circuit(cconfig)
            .map(|(dbsp, catalog)| {
                (
                    Box::new(dbsp) as Box<dyn dbsp_adapters::DbspCircuitHandle>,
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

// Simple endpoint to serve compiled binaries
#[get("/binary/{program_id}/{version}")]
async fn index(
    state: web::Data<CompilerConfig>,
    req: HttpRequest,
) -> Result<impl Responder, ManagerError> {
    let program_id = match req.match_info().get("program_id") {
        None => Err(ManagerError::MissingUrlEncodedParam {
            param: "program_id",
        }),
        Some(id) => match id.parse::<Uuid>() {
            Err(e) => Err(ManagerError::InvalidUuidParam {
                value: id.to_string(),
                error: e.to_string(),
            }),
            Ok(uuid) => Ok(uuid),
        },
    }?;
    let program_id = ProgramId(program_id);
    let version = match req.match_info().get("version") {
        None => Err(ManagerError::MissingUrlEncodedParam { param: "version" }),
        Some(version) => match version.parse::<i64>() {
            Err(_) => Err(ManagerError::MissingUrlEncodedParam { param: "version" }),
            Ok(version) => Ok(version),
        },
    }?;
    let version = Version(version);
    let path = state.versioned_executable(program_id, version);
    Ok(NamedFile::open_async(path).await)
}

/// Health check endpoint
#[get("/healthz")]
async fn healthz(probe: web::Data<Arc<Mutex<Probe>>>) -> Result<impl Responder, ManagerError> {
    probe.lock().await.status_as_http_response()
}

impl Compiler {
    pub async fn run(
        config: &CompilerConfig,
        db: Arc<Mutex<ProjectDB>>,
    ) -> Result<(), ManagerError> {
        Self::create_working_directory(config).await?;
        let compiler_task = spawn(Self::compiler_task(config.clone(), db.clone()));
        let probe = web::Data::new(Probe::new(db.clone()).await);
        let gc_task = spawn(Self::gc_task(config.clone(), db));
        let config_copy = web::Data::new(config.clone());
        let port = config.binary_ref_port;
        let http = spawn(
            HttpServer::new(move || {
                actix_web::App::new()
                    .app_data(config_copy.clone())
                    .app_data(probe.clone())
                    .service(index)
                    .service(healthz)
            })
            .bind(("0.0.0.0", port))
            .unwrap()
            .run(),
        );
        let r = join!(compiler_task, gc_task, http);
        r.0.unwrap()?;

        Ok(())
    }

    async fn create_working_directory(config: &CompilerConfig) -> Result<(), ManagerError> {
        fs::create_dir_all(&config.workspace_dir())
            .await
            .map_err(|e| {
                ManagerError::io_error(
                    format!(
                        "creating Rust workspace directory '{}'",
                        config.workspace_dir().display()
                    ),
                    e,
                )
            })
    }

    /// Determines the compilation profile to use provided the compiler
    /// and program configuration. Returns the compilation profile of the
    /// program if it is specified. If none is specified by the program,
    /// returns the default compilation profile in the compiler configuration.
    fn pick_compilation_profile(
        compiler_config: &CompilerConfig,
        program_profile: &Option<CompilationProfile>,
    ) -> CompilationProfile {
        program_profile
            .clone()
            .unwrap_or(compiler_config.compilation_profile.clone())
    }

    /// Download and compile all crates needed by the Rust code generated
    /// by the SQL compiler.
    ///
    /// This is useful to prepare the working directory, so that the first
    /// compilation job completes quickly.  Also creates the `Cargo.lock`
    /// file, making sure that subsequent `cargo` runs do not access the
    /// network.
    pub async fn precompile_dependencies(config: &CompilerConfig) -> Result<(), ManagerError> {
        let program_id = ProgramId(Uuid::nil());

        Self::create_working_directory(config).await?;

        // Create workspace-level Cargo.toml
        Compiler::write_workspace_toml(config, program_id).await?;

        // Create dummy package.
        let rust_file_path = config.rust_program_path(program_id);
        let rust_src_dir = rust_file_path.parent().unwrap();
        fs::create_dir_all(rust_src_dir).await.map_err(|e| {
            ManagerError::io_error(
                format!(
                    "creating Rust source directory '{}'",
                    rust_src_dir.display()
                ),
                e,
            )
        })?;

        Compiler::write_project_toml(config, program_id).await?;

        fs::write(&rust_file_path, "fn main() {}")
            .await
            .map_err(|e| {
                ManagerError::io_error(format!("writing '{}'", rust_file_path.display()), e)
            })?;

        // `cargo build`.
        let mut cargo_process =
            Compiler::run_cargo_build(config, program_id, &Some(CompilationProfile::Optimized))
                .await?;
        let exit_status = cargo_process
            .wait()
            .await
            .map_err(|e| ManagerError::io_error("waiting for 'cargo build'".to_string(), e))?;

        if !exit_status.success() {
            let stdout = fs::read_to_string(config.compiler_stdout_path(program_id))
                .await
                .map_err(|e| {
                    ManagerError::io_error(
                        format!(
                            "reading '{}'",
                            config.compiler_stdout_path(program_id).display()
                        ),
                        e,
                    )
                })?;
            let stderr = fs::read_to_string(config.compiler_stderr_path(program_id))
                .await
                .map_err(|e| {
                    ManagerError::io_error(
                        format!(
                            "reading '{}'",
                            config.compiler_stderr_path(program_id).display()
                        ),
                        e,
                    )
                })?;
            return Err(ManagerError::RustCompilerError {
                error: format!(
                    "Failed to precompile Rust dependencies\nstdout:\n{stdout}\nstderr:\n{stderr}"
                ),
            });
        }

        Ok(())
    }

    async fn run_cargo_build(
        config: &CompilerConfig,
        program_id: ProgramId,
        program_profile: &Option<CompilationProfile>,
    ) -> Result<Child, ManagerError> {
        let err_file = File::create(&config.compiler_stderr_path(program_id))
            .await
            .map_err(|e| {
                ManagerError::io_error(
                    format!(
                        "creating '{}'",
                        config.compiler_stderr_path(program_id).display()
                    ),
                    e,
                )
            })?;
        let out_file = File::create(&config.compiler_stdout_path(program_id))
            .await
            .map_err(|e| {
                ManagerError::io_error(
                    format!(
                        "creating '{}'",
                        config.compiler_stdout_path(program_id).display()
                    ),
                    e,
                )
            })?;

        let mut command = Command::new("cargo");
        let profile = Compiler::pick_compilation_profile(config, program_profile);
        info!("Compiling Rust for program {program_id} with profile {profile}");
        command
            .current_dir(&config.workspace_dir())
            .arg("build")
            .arg("--workspace")
            .arg("--profile")
            .arg(profile.to_string())
            .stdin(Stdio::null())
            .stderr(Stdio::from(err_file.into_std().await))
            .stdout(Stdio::from(out_file.into_std().await));

        command
            .spawn()
            .map_err(|e| ManagerError::io_error("starting 'cargo'".to_string(), e))
    }

    async fn version_binary(
        config: &CompilerConfig,
        db: &ProjectDB,
        program: &ProgramDescr,
    ) -> Result<(), ManagerError> {
        let program_id = program.program_id;
        let version = program.version;
        let profile = Compiler::pick_compilation_profile(config, &program.config.profile);
        info!(
            "Preserve binary {:?} as {:?}",
            config.target_executable(program_id, &profile),
            config.versioned_executable(program_id, version)
        );

        // Save the file locally and record a path to it as a "file://" scheme URL in the DB.
        // This requires any entity accessing it to have access to the same filesystem
        let source = config.target_executable(program_id, &profile);
        let destination = config.versioned_executable(program_id, version);
        fs::copy(&source, &destination).await.map_err(|e| {
            ManagerError::io_error(
                format!(
                    "copying '{}' to '{}'",
                    source.display(),
                    destination.display()
                ),
                e,
            )
        })?;

        db.create_compiled_binary_ref(
            program_id,
            version,
            format!(
                "http://{}:{}/binary/{program_id}/{version}",
                config.binary_ref_host, config.binary_ref_port
            ),
        )
        .await?;
        Ok(())
    }

    /// Generate workspace-level `Cargo.toml`.
    async fn write_workspace_toml(
        config: &CompilerConfig,
        program_id: ProgramId,
    ) -> Result<(), ManagerError> {
        let workspace_toml_code = format!(
            r#"
[workspace]
members = [ "{}" ]
resolver = "2"

[patch.crates-io]
rkyv = {{ git = "https://github.com/gz/rkyv.git", rev = "3d3fd86" }}
rust_decimal = {{ git = "https://github.com/gz/rust-decimal.git", rev = "ea85fdf" }}
size-of = {{ git = "https://github.com/gz/size-of.git", rev = "3ec40db" }}

[profile.unoptimized]
inherits = "release"
opt-level = 0
lto = "off"
codegen-units = 256

[profile.optimized]
inherits = "release"
"#,
            CompilerConfig::crate_name(program_id),
        );
        let toml_path = config.workspace_toml_path();
        fs::write(&toml_path, workspace_toml_code)
            .await
            .map_err(|e| ManagerError::io_error(format!("writing '{}'", toml_path.display()), e))?;

        Ok(())
    }

    /// Generate project-level `Cargo.toml`.
    async fn write_project_toml(
        config: &CompilerConfig,
        program_id: ProgramId,
    ) -> Result<(), ManagerError> {
        let template_path = config.project_toml_template_path();
        let template_toml = fs::read_to_string(&template_path).await.map_err(|e| {
            ManagerError::io_error(format!("reading template '{}'", template_path.display()), e)
        })?;
        let program_name = format!("name = \"{}\"", CompilerConfig::crate_name(program_id));
        let mut project_toml_code = template_toml
            .replace("name = \"temp\"", &program_name)
            .replace(
                "[lib]\npath = \"src/lib.rs\"",
                &format!("\n\n[[bin]]\n{program_name}\npath = \"src/main.rs\""),
            );
        let p = &config.dbsp_override_path;
        project_toml_code = project_toml_code
            .replace(
                "dbsp_adapters = { path = \"../../crates/adapters\", default-features = false }",
                "dbsp_adapters = { path = \"../../crates/adapters\" }",
            )
            .replace("../../crates", &format!("{p}/crates"))
            .replace("../lib", &format!("{}", config.sql_lib_path().display()));
        debug!("TOML:\n{project_toml_code}");

        let toml_path = config.project_toml_path(program_id);
        fs::write(&toml_path, project_toml_code)
            .await
            .map_err(|e| ManagerError::io_error(format!("writing '{}'", toml_path.display()), e))?;

        Ok(())
    }

    async fn gc_task(
        config: CompilerConfig,
        db: Arc<Mutex<ProjectDB>>,
    ) -> Result<(), ManagerError> {
        Self::do_gc_task(config, db).await.map_err(|e| {
            error!("gc task failed; error: '{e}'");
            e
        })
    }

    async fn binary_path_to_parts(path: &DirEntry) -> Option<(ProgramId, Version)> {
        let file_name = path.file_name();
        let file_name = file_name.to_str().unwrap();
        if file_name.starts_with("project") {
            let parts: Vec<&str> = file_name.split('_').collect();
            if parts.len() != 3
                || parts.first() != Some(&"project")
                || parts.get(2).map(|p| p.len()) <= Some(1)
            {
                error!("Invalid binary file found: {}", file_name);
                return None;
            }
            if let (Ok(program_uuid), Ok(program_version)) =
                // parse file name with the following format:
                // project_{uuid}_v{version}
                (Uuid::parse_str(parts[1]), parts[2][1..].parse::<i64>())
            {
                return Some((ProgramId(program_uuid), Version(program_version)));
            }
        }
        None
    }

    /// A task that wakes up periodically and removes stale binaries.
    ///
    /// Helps to keep the binaries directory clean and not run out of space if
    /// this runs for a very long time.
    ///
    /// Note that this task handles all errors internally and does not propagate
    /// them up so it can run forever and never aborts.
    async fn do_gc_task(
        config: CompilerConfig,
        db: Arc<Mutex<ProjectDB>>,
    ) -> Result<(), ManagerError> {
        loop {
            sleep(GC_POLL_INTERVAL).await;
            let read_dir = fs::read_dir(config.binaries_dir()).await;
            match read_dir {
                Ok(mut paths) => loop {
                    let entry = paths.next_entry().await;
                    match entry {
                        Ok(Some(path)) => {
                            let maybe_parts = Self::binary_path_to_parts(&path).await;
                            match maybe_parts {
                                Some((program_uuid, program_version)) => {
                                    let db = db.lock().await;
                                    if let Ok(is_used) = db
                                        .is_program_version_in_use(
                                            program_uuid.0,
                                            program_version.0,
                                        )
                                        .await
                                    {
                                        if !is_used {
                                            warn!("About to remove binary file '{:?}' that is no longer in use by any program", path.file_name());
                                            db.delete_compiled_binary_ref(
                                                program_uuid,
                                                program_version,
                                            )
                                            .await?;
                                            drop(db);
                                            let r = fs::remove_file(path.path()).await;
                                            if let Err(e) = r {
                                                error!(
                                                    "GC task failed to remove file '{:?}': {}",
                                                    path.file_name(),
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                                None => {
                                    warn!(
                                        "GC task found invalid file in {:?}: {:?}",
                                        config.binaries_dir(),
                                        path.file_name()
                                    );
                                }
                            }
                        }
                        // We are done with the directory
                        Ok(None) => break,
                        Err(e) => {
                            warn!("GC task unable to read an entry: {}", e);
                            // Not clear from docs if an error during iteration
                            // is recoverable and we could just `continue;` here
                            break;
                        }
                    }
                },
                Err(e) => {
                    error!("GC task couldn't read binaries directory: {}", e)
                }
            }
        }
    }

    async fn compiler_task(
        config: CompilerConfig,
        db: Arc<Mutex<ProjectDB>>,
    ) -> Result<(), ManagerError> {
        Self::do_compiler_task(config, db).await.map_err(|e| {
            error!("compiler task failed; error: '{e}'");
            e
        })
    }

    /// Invoked at startup so the compiler service can align its
    /// local state (versioned executables) with the Programs API state.
    /// It recovers from partially progressed compilation jobs.
    async fn reconcile_local_state(
        config: &CompilerConfig,
        db: &Arc<Mutex<ProjectDB>>,
    ) -> Result<(), DBError> {
        info!("Reconciling local state with API state");
        let mut map: HashSet<(Uuid, i64)> = HashSet::new();
        let read_dir = fs::read_dir(config.binaries_dir()).await;
        match read_dir {
            Ok(mut paths) => loop {
                let entry = paths.next_entry().await;
                match entry {
                    Ok(Some(path)) => {
                        let maybe_parts = Self::binary_path_to_parts(&path).await;
                        match maybe_parts {
                            Some((program_uuid, program_version)) => {
                                map.insert((program_uuid.0, program_version.0));
                            }
                            None => {
                                warn!(
                                    "Local state reconciler found invalid file in {:?}: {:?}",
                                    config.binaries_dir(),
                                    path.file_name()
                                );
                            }
                        }
                    }
                    // We are done with the directory
                    Ok(None) => break,
                    Err(e) => {
                        warn!("Local state reconciler was unable to read an entry: {}", e);
                        // Not clear from docs if an error during iteration
                        // is recoverable and we could just `continue;` here
                        break;
                    }
                }
            },
            Err(e) => {
                warn!(
                    "Local state reconciler could not read binaries directory: {}",
                    e
                )
            }
        }

        let db = db.lock().await;
        let programs = db.all_programs().await?;
        for (tenant_id, program) in programs {
            // We have some artifact but the program status has not been updated to Success.
            // This could indicate a failure between when we began writing the versioned
            // executable to before we could update the program status. This
            // means, the best solution is to start over with the compilation
            if program.status == ProgramStatus::CompilingRust
                && map.contains(&(program.program_id.0, program.version.0))
            {
                let path = config.versioned_executable(program.program_id, program.version);
                info!("File {:?} exists, but the program status is CompilingRust. Removing the file to start compilation again.", path.file_name());
                db.delete_compiled_binary_ref(program.program_id, program.version)
                    .await?;
                let r = fs::remove_file(path.clone()).await;
                if let Err(e) = r {
                    error!(
                        "Reconcile task failed to remove file '{:?}': {}",
                        path.file_name(),
                        e
                    );
                }
                db.set_program_status_guarded(
                    tenant_id,
                    program.program_id,
                    program.version,
                    ProgramStatus::Pending,
                )
                .await?;
            }
            // If the program was supposed to be further in the compilation chain, but
            // we don't have the binary artifact available locally, we need to queue the program
            // for compilation again. TODO: this behavior will change when the compiler uploads
            // artifacts remotely and not on its local filesystem.
            else if (program.status.is_compiling() || program.status == ProgramStatus::Success)
                && !map.contains(&(program.program_id.0, program.version.0))
            {
                info!(
                    "Program {} does not have a local artifact despite being in the {:?} state. Removing binary references to the program and re-queuing it for compilation.",
                    program.program_id, program.status
                );
                db.set_program_status_guarded(
                    tenant_id,
                    program.program_id,
                    program.version,
                    ProgramStatus::Pending,
                )
                .await?;
                db.delete_compiled_binary_ref(program.program_id, program.version)
                    .await?;
            }
        }
        Ok(())
    }

    async fn do_compiler_task(
        /* command_receiver: Receiver<CompilerCommand>, */
        config: CompilerConfig,
        db: Arc<Mutex<ProjectDB>>,
    ) -> Result<(), ManagerError> {
        Self::reconcile_local_state(&config, &db).await?;
        loop {
            let res = Self::compiler_task_inner(&config, &db).await;
            // Look for benign errors that the compiler can run into, in which case,
            // we resume working.
            #[allow(clippy::collapsible_match)]
            if let Err(ManagerError::DBError { ref db_error }) = res {
                if let DBError::OutdatedProgramVersion { latest_version: _ } = db_error {
                    warn!("Compiler encountered an OutdatedProgramVersion. Retrying.");
                    continue;
                }
            }
            res?
        }
    }

    async fn compiler_task_inner(
        config: &CompilerConfig,
        db: &Arc<Mutex<ProjectDB>>,
    ) -> Result<(), ManagerError> {
        let mut job: Option<CompilationJob> = None;
        loop {
            select! {
                // Wake up every `COMPILER_POLL_INTERVAL` to check
                // if we need to abort ongoing compilation.
                _ = sleep(COMPILER_POLL_INTERVAL) => {
                    let mut cancel = false;
                    if let Some(job) = &job {
                        // Program was deleted, updated or the user changed its status
                        // to cancelled -- abort compilation.
                        let descr = db.lock().await.get_program_by_id(job.tenant_id, job.program.program_id, false).await;
                        match descr {
                            Ok(descr) => {
                                if descr.version != job.program.version || !descr.status.is_compiling() {
                                    cancel = true;
                                }
                            },
                            Err(DBError::UnknownProgram { .. }) => {
                                cancel = true
                            }
                            Err(e) => {return Err(e.into())}
                        }
                    }
                    if cancel {
                        job.unwrap().cancel().await;
                        job = None;
                    }
                }
                // Compilation job finished - start the next stage of the compilation
                // (i.e. run the Rust compiler after SQL) or update program status in the
                // database.
                Some(exit_status) = async {
                    if let Some(job) = &mut job {
                        Some(job.wait().await)
                    } else {
                        None
                    }
                }, if job.is_some() => {
                    let tenant_id = job.as_ref().unwrap().tenant_id;
                    let program_id = job.as_ref().unwrap().program.program_id;
                    let version = job.as_ref().unwrap().program.version;
                    let elapsed = job.as_ref().unwrap().stage_start_time.elapsed().as_secs_f64();
                    let db = db.lock().await;

                    match exit_status {
                        Ok(status) if status.success() && job.as_ref().unwrap().is_sql() => {
                            record(StageType::Sql, Status::Success, elapsed);
                            // Read the schema so we can store it in the DB.
                            // We trust the compiler that it put the file
                            // there if it succeeded.
                            let schema_path = config.schema_path(program_id);
                            let schema_json = fs::read_to_string(&schema_path).await
                                .map_err(|e| {
                                    ManagerError::io_error(format!("reading '{}'", schema_path.display()), e)
                                })?;

                            let schema = serde_json::from_str(&schema_json)
                                .map_err(|e| { ManagerError::invalid_program_schema(e.to_string()) })?;
                            // SQL compiler succeeded -- start the Rust job and update the
                            // program schema.
                            db.update_program(
                                tenant_id,
                                program_id,
                                &None,
                                &None,
                                &None,
                                &Some(ProgramStatus::CompilingRust),
                                &Some(schema),
                                &None,
                                Some(version),
                                None
                            ).await?;

                            info!("Invoking rust compiler for program {program_id} version {version} (tenant {tenant_id}). This will take a while.");
                            debug!("Set ProgramStatus::CompilingRust '{program_id}', version '{version}'");
                            job = Some(CompilationJob::rust(config, job.as_ref().unwrap()).await?);
                        }
                        Ok(status) if status.success() && job.as_ref().unwrap().is_rust() => {
                            Self::version_binary(config, &db, &job.as_ref().unwrap().program).await?;
                            // Rust compiler succeeded -- declare victory.
                            db.set_program_status_guarded(tenant_id, program_id, version, ProgramStatus::Success).await?;
                            info!("Successfully invoked rust compiler for program {program_id} version {version} (tenant {tenant_id}).");
                            debug!("Set ProgramStatus::Success '{program_id}', version '{version}'");
                            record(StageType::Rust, Status::Success, elapsed);
                            job = None;
                        }
                        Ok(status) => {
                            // Compilation failed - update program status with the compiler
                            // error message.
                            let output = job.as_ref().unwrap().error_output(config).await?;
                            let status = if job.as_ref().unwrap().is_rust() {
                                ProgramStatus::RustError(format!("{output}\nexit code: {status}"))
                            } else if let Ok(messages) = serde_json::from_str(&output) {
                                // If we can parse the SqlCompilerMessages
                                // as JSON, we assume the compiler worked:
                                record(StageType::Sql, Status::Error, elapsed);
                                ProgramStatus::SqlError(messages)
                            } else {
                                // Otherwise something unexpected happened
                                // and we return a system error:
                                record(StageType::Sql, Status::Error, elapsed);
                                ProgramStatus::SystemError(format!("{output}\nexit code: {status}"))
                            };
                            db.set_program_status_guarded(tenant_id, program_id, version, status).await?;
                            job = None;
                        }
                        Err(e) => {
                            let status = if job.unwrap().is_rust() {
                                record(StageType::Rust, Status::Error, elapsed);
                                ProgramStatus::SystemError(format!("I/O error with rustc: {e}"))
                            } else {
                                record(StageType::Sql, Status::Error, elapsed);
                                ProgramStatus::SystemError(format!("I/O error with sql-to-dbsp: {e}"))
                            };
                            db.set_program_status_guarded(tenant_id, program_id, version, status).await?;
                            job = None;
                        }
                    }
                }
            }
            // Pick the next program from the queue.
            if job.is_none() {
                let program = {
                    let db = db.lock().await;
                    if let Some((tenant_id, program_id, version)) = db.next_job().await? {
                        trace!("Next program in the queue: '{program_id}', version '{version}'");
                        let program = db.get_program_by_id(tenant_id, program_id, true).await?;
                        Some((tenant_id, program))
                    } else {
                        None
                    }
                };

                if let Some((tenant_id, program)) = program {
                    job = Some(CompilationJob::sql(tenant_id, config, &program).await?);
                    db.lock()
                        .await
                        .set_program_status_guarded(
                            tenant_id,
                            program.program_id,
                            program.version,
                            ProgramStatus::CompilingSql,
                        )
                        .await?;
                }
            }
        }
    }
}

#[derive(Eq, PartialEq)]
enum Stage {
    Sql,
    Rust,
}

struct CompilationJob {
    stage: Stage,
    tenant_id: TenantId,
    compiler_process: Child,
    stage_start_time: Instant,
    program: ProgramDescr,
}

impl CompilationJob {
    fn is_sql(&self) -> bool {
        self.stage == Stage::Sql
    }

    fn is_rust(&self) -> bool {
        self.stage == Stage::Rust
    }

    /// Run SQL-to-DBSP compiler.
    async fn sql(
        tenant_id: TenantId,
        config: &CompilerConfig,
        program: &ProgramDescr,
    ) -> Result<Self, ManagerError> {
        let code = program
            .code
            .as_ref()
            .expect("Invariant: SQL compiler cannot be invoked on programs without code");
        let program_id = program.program_id;
        let version = program.version;
        debug!("Running SQL compiler on program '{program_id}', version '{version}'");

        // Create project directory.
        let sql_file_path = config.sql_file_path(program_id);
        let project_directory = sql_file_path.parent().unwrap();
        fs::create_dir_all(&project_directory).await.map_err(|e| {
            ManagerError::io_error(
                format!(
                    "creating project directory '{}'",
                    project_directory.display()
                ),
                e,
            )
        })?;

        // Write SQL code to file.
        fs::write(&sql_file_path, code).await.map_err(|e| {
            ManagerError::io_error(format!("writing '{}'", sql_file_path.display()), e)
        })?;

        let rust_file_path = config.rust_program_path(program_id);
        let rust_source_dir = rust_file_path.parent().unwrap();
        fs::create_dir_all(&rust_source_dir).await.map_err(|e| {
            ManagerError::io_error(format!("creating '{}'", rust_source_dir.display()), e)
        })?;

        let stderr_path = config.compiler_stderr_path(program_id);
        let err_file = File::create(&stderr_path).await.map_err(|e| {
            ManagerError::io_error(format!("creating error log '{}'", stderr_path.display()), e)
        })?;

        // `main.rs` file.
        let rust_file = File::create(&rust_file_path).await.map_err(|e| {
            ManagerError::io_error(
                format!("failed to create '{}'", rust_file_path.display()),
                e,
            )
        })?;

        // Run compiler, direct output to `main.rs`.
        let schema_path = config.schema_path(program_id);
        let compiler_process = Command::new(config.sql_compiler_path())
            .arg("-js")
            .arg(schema_path)
            .arg(sql_file_path.as_os_str())
            .arg("-i")
            .arg("-je")
            .arg("--alltables")
            .arg("--outputsAreSets")
            .arg("--ignoreOrder")
            .arg("--unquotedCasing")
            .arg("lower")
            .stdin(Stdio::null())
            .stderr(Stdio::from(err_file.into_std().await))
            .stdout(Stdio::from(rust_file.into_std().await))
            .spawn()
            .map_err(|e| {
                ManagerError::io_error(
                    format!("starting SQL compiler '{}'", sql_file_path.display()),
                    e,
                )
            })?;

        Ok(Self {
            tenant_id,
            stage: Stage::Sql,
            program: program.clone(),
            compiler_process,
            stage_start_time: Instant::now(),
        })
    }

    // Run `cargo` on the generated Rust workspace.
    async fn rust(config: &CompilerConfig, job: &Self) -> Result<Self, ManagerError> {
        let program_id = job.program.program_id;
        let version = job.program.version;
        let tenant_id = job.tenant_id;
        debug!("Running Rust compiler on program '{program_id}', version '{version}'");

        let rust_path = config.rust_program_path(program_id);
        let mut main_rs = OpenOptions::new()
            .append(true)
            .open(&rust_path)
            .await
            .map_err(|e| ManagerError::io_error(format!("opening '{}'", rust_path.display()), e))?;

        main_rs
            .write_all(MAIN_FUNCTION.as_bytes())
            .await
            .map_err(|e| ManagerError::io_error(format!("writing '{}'", rust_path.display()), e))?;
        drop(main_rs);

        // Write `project/Cargo.toml`.
        Compiler::write_project_toml(config, program_id).await?;

        // Write workspace `Cargo.toml`.  The workspace contains SQL libs and the
        // generated project crate.
        Compiler::write_workspace_toml(config, program_id).await?;

        // Run cargo, direct stdout and stderr to the same file.
        let compiler_process =
            Compiler::run_cargo_build(config, program_id, &job.program.config.profile).await?;

        Ok(Self {
            tenant_id,
            stage: Stage::Rust,
            program: job.program.clone(),
            compiler_process,
            stage_start_time: Instant::now(),
        })
    }

    /// Async-wait for the compiler to terminate.
    async fn wait(&mut self) -> Result<ExitStatus, ManagerError> {
        let exit_status = self.compiler_process.wait().await.map_err(|e| {
            ManagerError::io_error("waiting for the compiler process".to_string(), e)
        })?;
        Ok(exit_status)
        // doesn't update status
    }

    /// Read error output of (Rust or SQL) compiler.
    async fn error_output(&self, config: &CompilerConfig) -> Result<String, ManagerError> {
        let output = match self.stage {
            Stage::Sql => {
                let stderr_path = config.compiler_stderr_path(self.program.program_id);
                fs::read_to_string(&stderr_path).await.map_err(|e| {
                    ManagerError::io_error(format!("reading '{}'", stderr_path.display()), e)
                })?
            }
            Stage::Rust => {
                let stdout_path = config.compiler_stdout_path(self.program.program_id);
                let stdout = fs::read_to_string(&stdout_path).await.map_err(|e| {
                    ManagerError::io_error(format!("reading '{}'", stdout_path.display()), e)
                })?;
                let stderr_path = config.compiler_stderr_path(self.program.program_id);
                let stderr = fs::read_to_string(&stderr_path).await.map_err(|e| {
                    ManagerError::io_error(format!("reading '{}'", stderr_path.display()), e)
                })?;
                format!("stdout:\n{stdout}\nstderr:\n{stderr}")
            }
        };

        Ok(output)
    }

    /// Kill (Rust or SQL) compiler process.
    async fn cancel(&mut self) {
        let _ = self.compiler_process.kill().await;
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, sync::Arc};

    use tempfile::TempDir;
    use tokio::{fs, sync::Mutex};
    use uuid::Uuid;

    use crate::compiler::Compiler;
    use crate::{
        auth::TenantRecord,
        compiler::ProgramStatus,
        config::{CompilationProfile, CompilerConfig},
        db::{storage::Storage, ProgramId, ProjectDB, Version},
    };

    async fn create_program(db: &Arc<Mutex<ProjectDB>>, pname: &str) -> (ProgramId, Version) {
        let tenant_id = TenantRecord::default().id;
        db.lock()
            .await
            .new_program(
                tenant_id,
                Uuid::now_v7(),
                pname,
                "program desc",
                "ignored",
                &super::ProgramConfig {
                    profile: Some(CompilationProfile::Unoptimized),
                },
                None,
            )
            .await
            .unwrap()
    }

    async fn check_program_status_pending(db: &Arc<Mutex<ProjectDB>>, pname: &str) {
        let tenant_id = TenantRecord::default().id;
        let programdesc = db
            .lock()
            .await
            .get_program_by_name(tenant_id, pname, false, None)
            .await
            .unwrap();
        assert_eq!(ProgramStatus::Pending, programdesc.status);
    }

    #[tokio::test]
    async fn test_compiler_reconcile_no_local_binary() {
        let tid = TenantRecord::default().id;
        let tmp_dir = TempDir::new().unwrap();
        let workdir = tmp_dir.path().to_str().unwrap();
        let conf = CompilerConfig {
            sql_compiler_home: "".to_owned(),
            dbsp_override_path: "../../".to_owned(),
            compilation_profile: crate::config::CompilationProfile::Unoptimized,
            precompile: false,
            compiler_working_directory: workdir.to_owned(),
            binary_ref_host: "127.0.0.1".to_string(),
            binary_ref_port: 9090,
        };

        let (db, _temp) = crate::db::test::setup_pg().await;
        let db = Arc::new(Mutex::new(db));

        // Empty binaries folder, no programs in API
        super::Compiler::reconcile_local_state(&conf, &db)
            .await
            .unwrap();

        // Create uncompiled program
        let (pid, vid) = create_program(&db, "p1").await;
        super::Compiler::reconcile_local_state(&conf, &db)
            .await
            .unwrap();

        // Now set the program to be queued for compilation
        db.lock()
            .await
            .set_program_status_guarded(tid, pid, vid, super::ProgramStatus::Pending)
            .await
            .unwrap();
        super::Compiler::reconcile_local_state(&conf, &db)
            .await
            .unwrap();
        check_program_status_pending(&db, "p1").await;

        // Now try all "compiling" states set the program to be compiled
        for state in vec![
            super::ProgramStatus::CompilingSql,
            super::ProgramStatus::CompilingRust,
            super::ProgramStatus::Success,
        ] {
            if state == super::ProgramStatus::Success {
                db.lock()
                    .await
                    .create_compiled_binary_ref(pid, vid, "dummy".to_string())
                    .await
                    .unwrap();
            }
            db.lock()
                .await
                .set_program_status_guarded(tid, pid, vid, state)
                .await
                .unwrap();
            super::Compiler::reconcile_local_state(&conf, &db)
                .await
                .unwrap();
            check_program_status_pending(&db, "p1").await;
        }
    }

    #[tokio::test]
    async fn test_compiler_reconcile_after_upgrade() {
        let tid = TenantRecord::default().id;
        let tmp_dir = TempDir::new().unwrap();
        let workdir = tmp_dir.path().to_str().unwrap();
        let conf = CompilerConfig {
            sql_compiler_home: "".to_owned(),
            dbsp_override_path: "../../".to_owned(),
            compilation_profile: crate::config::CompilationProfile::Unoptimized,
            precompile: false,
            compiler_working_directory: workdir.to_owned(),
            binary_ref_host: "127.0.0.1".to_string(),
            binary_ref_port: 9090,
        };

        let (db, _temp) = crate::db::test::setup_pg().await;
        let db = Arc::new(Mutex::new(db));

        // Create successfully compiled program
        let (pid, vid) = create_program(&db, "p1").await;

        db.lock()
            .await
            .set_program_status_guarded(tid, pid, vid, super::ProgramStatus::Success)
            .await
            .unwrap();

        db.lock()
            .await
            .create_compiled_binary_ref(pid, vid, "dummy".to_string())
            .await
            .unwrap();

        // Start without any local filesystem state
        super::Compiler::reconcile_local_state(&conf, &db)
            .await
            .unwrap();

        // Attempt to create a new binary ref to simulate a successful compilation
        db.lock()
            .await
            .create_compiled_binary_ref(pid, vid, "dummy1".to_string())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_compiler_with_local_binaries() {
        let tid = TenantRecord::default().id;
        let tmp_dir = TempDir::new().unwrap();
        let workdir = tmp_dir.path().to_str().unwrap();
        let conf = CompilerConfig {
            sql_compiler_home: "".to_owned(),
            dbsp_override_path: "../../".to_owned(),
            compilation_profile: crate::config::CompilationProfile::Unoptimized,
            precompile: false,
            compiler_working_directory: workdir.to_owned(),
            binary_ref_host: "127.0.0.1".to_string(),
            binary_ref_port: 9090,
        };

        let (db, _temp) = crate::db::test::setup_pg().await;
        let db = Arc::new(Mutex::new(db));

        let (pid1, v1) = create_program(&db, "p1").await;
        let (pid2, v2) = create_program(&db, "p2").await;
        for state in vec![
            super::ProgramStatus::Pending,
            super::ProgramStatus::CompilingSql,
        ] {
            db.lock()
                .await
                .set_program_status_guarded(tid, pid1, v1, state.clone())
                .await
                .unwrap();
            db.lock()
                .await
                .set_program_status_guarded(tid, pid2, v2, state)
                .await
                .unwrap();
            super::Compiler::reconcile_local_state(&conf, &db)
                .await
                .unwrap();
            check_program_status_pending(&db, "p1").await;
            check_program_status_pending(&db, "p2").await;
        }

        // Simulate compiled artifacts
        fs::create_dir(conf.binaries_dir()).await.unwrap();
        let path1 = conf.versioned_executable(pid1, v1);
        let path2 = conf.versioned_executable(pid2, v2);
        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();

        db.lock()
            .await
            .set_program_status_guarded(tid, pid1, v1, ProgramStatus::CompilingRust)
            .await
            .unwrap();
        db.lock()
            .await
            .set_program_status_guarded(tid, pid2, v2, ProgramStatus::CompilingRust)
            .await
            .unwrap();
        super::Compiler::reconcile_local_state(&conf, &db)
            .await
            .unwrap();
        check_program_status_pending(&db, "p1").await;
        check_program_status_pending(&db, "p2").await;
        assert!(!path1.exists());
        assert!(!path2.exists());
    }

    #[tokio::test]
    async fn test_determine_compilation_profile_to_use() {
        for compiler_compilation_profile in [
            CompilationProfile::Dev,
            CompilationProfile::Unoptimized,
            CompilationProfile::Optimized,
        ] {
            let compiler_config = CompilerConfig {
                compiler_working_directory: "".to_string(),
                compilation_profile: compiler_compilation_profile.clone(),
                sql_compiler_home: "".to_string(),
                dbsp_override_path: "".to_string(),
                precompile: false,
                binary_ref_host: "".to_string(),
                binary_ref_port: 0,
            };

            // Program profile if specified is used
            for program_profile in [
                Some(CompilationProfile::Dev),
                Some(CompilationProfile::Unoptimized),
                Some(CompilationProfile::Optimized),
            ] {
                assert_eq!(
                    Compiler::pick_compilation_profile(&compiler_config, &program_profile),
                    program_profile.unwrap()
                );
            }

            // If not specified, compiler default compilation profile is used
            assert_eq!(
                Compiler::pick_compilation_profile(&compiler_config, &None),
                compiler_compilation_profile
            );
        }
    }
}
