use crate::auth::TenantId;
use crate::db::storage::Storage;
use crate::{ManagerConfig, ProgramId, ProjectDB, Version};
use anyhow::{bail, Error as AnyError, Result as AnyResult};
use log::{debug, error, trace};
use serde::{Deserialize, Serialize};
use std::{
    process::{ExitStatus, Stdio},
    sync::Arc,
};
use tokio::{
    fs,
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    process::{Child, Command},
    select, spawn,
    sync::Mutex,
    task::JoinHandle,
    time::{sleep, Duration},
};
use utoipa::ToSchema;
use uuid::Uuid;

/// The frequency with which the compiler polls the project database
/// for new compilation requests.
const COMPILER_POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// A SQL compiler error.
///
/// The SQL compiler returns a list of errors in the following JSON format if
/// it's invoked with the `-je` option.
///
/// ```no_run
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
#[derive(Debug, Serialize, Eq, PartialEq, ToSchema, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) enum ProgramStatus {
    /// Initial state: program has been created or modified, but the user
    /// hasn't yet started compiling the program.
    None,
    /// Compilation request received from the user; program has been placed
    /// in the queue.
    Pending,
    /// Compilation of SQL -> Rust in progress.
    CompilingSql,
    /// Compiling Rust -> executable in progress
    CompilingRust,
    /// Compilation succeeded.
    Success,
    /// SQL compiler returned an error.
    SqlError(Vec<SqlCompilerMessage>),
    /// Rust compiler returned an error.
    RustError(String),
    /// System/OS returned an error when trying to invoke commands.
    SystemError(String),
}

impl ProgramStatus {
    /// Return true if program is currently compiling.
    pub(crate) fn is_compiling(&self) -> bool {
        *self == ProgramStatus::CompilingRust || *self == ProgramStatus::CompilingSql
    }
}

pub struct Compiler {
    compiler_task: JoinHandle<AnyResult<()>>,
}

impl Drop for Compiler {
    fn drop(&mut self) {
        self.compiler_task.abort();
    }
}

/// The `main` function injected in each generated pipeline
/// crate.
const MAIN_FUNCTION: &str = r#"
fn main() {
    dbsp_adapters::server::server_main(&circuit).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });
}"#;

impl Compiler {
    pub(crate) async fn new(config: &ManagerConfig, db: Arc<Mutex<ProjectDB>>) -> AnyResult<Self> {
        Self::create_working_directory(config).await?;
        let compiler_task = spawn(Self::compiler_task(config.clone(), db));
        Ok(Self { compiler_task })
    }

    async fn create_working_directory(config: &ManagerConfig) -> AnyResult<()> {
        fs::create_dir_all(&config.workspace_dir())
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to create Rust workspace directory '{}': {e}",
                    config.workspace_dir().display()
                ))
            })
    }

    /// Download and compile all crates needed by the Rust code generated
    /// by the SQL compiler.
    ///
    /// This is useful to prepare the working directory, so that the first
    /// compilation job completes quickly.  Also creates the `Cargo.lock`
    /// file, making sure that subsequent `cargo` runs do not access the
    /// network.
    pub(crate) async fn precompile_dependencies(config: &ManagerConfig) -> AnyResult<()> {
        let program_id = ProgramId(Uuid::nil());

        Self::create_working_directory(config).await?;

        // Create workspace-level Cargo.toml
        Compiler::write_workspace_toml(config, program_id).await?;

        // Create dummy package.
        let rust_file_path = config.rust_program_path(program_id);
        fs::create_dir_all(rust_file_path.parent().unwrap()).await?;

        Compiler::write_project_toml(config, program_id).await?;

        fs::write(&rust_file_path, "fn main() {}")
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to write '{}': '{e}'",
                    rust_file_path.display()
                ))
            })?;

        // `cargo build`.
        let mut cargo_process = Compiler::run_cargo_build(config, program_id).await?;
        let exit_status = cargo_process.wait().await?;

        if !exit_status.success() {
            let stdout = fs::read_to_string(config.compiler_stdout_path(program_id)).await?;
            let stderr = fs::read_to_string(config.compiler_stderr_path(program_id)).await?;
            bail!("Failed to precompile Rust dependencies\nstdout:\n{stdout}\nstderr:\n{stderr}");
        }

        Ok(())
    }

    async fn run_cargo_build(config: &ManagerConfig, program_id: ProgramId) -> AnyResult<Child> {
        let err_file = File::create(&config.compiler_stderr_path(program_id))
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to create '{}': '{e}'",
                    config.compiler_stderr_path(program_id).display()
                ))
            })?;
        let out_file = File::create(&config.compiler_stdout_path(program_id))
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to create '{}': '{e}'",
                    config.compiler_stdout_path(program_id).display()
                ))
            })?;

        let mut command = Command::new("cargo");

        command
            .current_dir(&config.workspace_dir())
            .arg("build")
            .arg("--workspace")
            .stdin(Stdio::null())
            .stderr(Stdio::from(err_file.into_std().await))
            .stdout(Stdio::from(out_file.into_std().await));

        if !config.debug {
            command.arg("--release");
        }

        command
            .spawn()
            .map_err(|e| AnyError::msg(format!("failed to start 'cargo': '{e}'")))
    }

    /// Generate workspace-level `Cargo.toml`.
    async fn write_workspace_toml(config: &ManagerConfig, program_id: ProgramId) -> AnyResult<()> {
        let workspace_toml_code = format!(
            "[workspace]\nmembers = [ \"{}\" ]\n",
            ManagerConfig::crate_name(program_id),
        );
        fs::write(&config.workspace_toml_path(), workspace_toml_code)
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to write '{}': '{e}'",
                    config.workspace_toml_path().display()
                ))
            })?;

        Ok(())
    }

    /// Generate project-level `Cargo.toml`.
    async fn write_project_toml(config: &ManagerConfig, program_id: ProgramId) -> AnyResult<()> {
        let template_toml = fs::read_to_string(&config.project_toml_template_path())
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to read template '{}': '{e}'",
                    config.project_toml_template_path().display()
                ))
            })?;
        let program_name = format!("name = \"{}\"", ManagerConfig::crate_name(program_id));
        let mut project_toml_code = template_toml
            .replace("name = \"temp\"", &program_name)
            .replace(", default-features = false", "")
            .replace(
                "[lib]\npath = \"src/lib.rs\"",
                &format!("\n\n[[bin]]\n{program_name}\npath = \"src/main.rs\""),
            );
        if let Some(p) = &config.dbsp_override_path {
            project_toml_code = project_toml_code
                .replace("../../crates", &format!("{p}/crates"))
                .replace("../lib", &format!("{}", config.sql_lib_path().display()));
        };
        debug!("TOML:\n{project_toml_code}");

        fs::write(&config.project_toml_path(program_id), project_toml_code)
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to write '{}': '{e}'",
                    config.project_toml_path(program_id).display()
                ))
            })?;

        Ok(())
    }

    async fn compiler_task(config: ManagerConfig, db: Arc<Mutex<ProjectDB>>) -> AnyResult<()> {
        Self::do_compiler_task(config, db).await.map_err(|e| {
            error!("compiler task failed; error: '{e}'");
            e
        })
    }

    async fn do_compiler_task(
        /* command_receiver: Receiver<CompilerCommand>, */ config: ManagerConfig,
        db: Arc<Mutex<ProjectDB>>,
    ) -> AnyResult<()> {
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
                        let descr = db.lock().await.get_program_if_exists(job.tenant_id, job.program_id).await?;
                        if let Some(descr) = descr {
                            if descr.version != job.version || !descr.status.is_compiling() {
                                cancel = true;
                            }
                        } else {
                            cancel = true;
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
                    let program_id = job.as_ref().unwrap().program_id;
                    let version = job.as_ref().unwrap().version;
                    let db = db.lock().await;

                    match exit_status {
                        Ok(status) if status.success() && job.as_ref().unwrap().is_sql() => {
                            // SQL compiler succeeded -- start the Rust job.
                            db.set_program_status_guarded(
                                tenant_id,
                                program_id,
                                version,
                                ProgramStatus::CompilingRust,
                            ).await?;

                            // Read the schema so we can store it in the DB.
                            //
                            // - We trust the compiler that it put the file
                            // there if it succeeded.
                            // - We hold the db lock so we are executing this
                            // update in the same transaction as the program
                            // status above.
                            let schema_json = fs::read_to_string(config.schema_path(program_id)).await?;
                            db.set_program_schema(tenant_id, program_id, schema_json).await?;

                            debug!("Set ProgramStatus::CompilingRust '{program_id}', version '{version}'");
                            job = Some(CompilationJob::rust(tenant_id, &config, program_id, version).await?);
                        }
                        Ok(status) if status.success() && job.as_ref().unwrap().is_rust() => {
                            // Rust compiler succeeded -- declare victory.
                            db.set_program_status_guarded(tenant_id, program_id, version, ProgramStatus::Success).await?;
                            debug!("Set ProgramStatus::Success '{program_id}', version '{version}'");
                            job = None;
                        }
                        Ok(status) => {
                            // Compilation failed - update program status with the compiler
                            // error message.
                            let output = job.as_ref().unwrap().error_output(&config).await?;
                            let status = if job.as_ref().unwrap().is_rust() {
                                ProgramStatus::RustError(format!("{output}\nexit code: {status}"))
                            } else if let Ok(messages) = serde_json::from_str(&output) {
                                    // If we can parse the SqlCompilerMessages
                                    // as JSON, we assume the compiler worked:
                                    ProgramStatus::SqlError(messages)
                            } else {
                                    // Otherwise something unexpected happened
                                    // and we return a system error:
                                    ProgramStatus::SystemError(format!("{output}\nexit code: {status}"))
                            };
                            db.set_program_status_guarded(tenant_id, program_id, version, status).await?;
                            job = None;
                        }
                        Err(e) => {
                            let status = if job.unwrap().is_rust() {
                                ProgramStatus::SystemError(format!("I/O error with rustc: {e}"))
                            } else {
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
                        let (_version, code) = db.program_code(tenant_id, program_id).await?;
                        Some((tenant_id, program_id, version, code))
                    } else {
                        None
                    }
                };

                if let Some((tenant_id, program_id, version, code)) = program {
                    job = Some(
                        CompilationJob::sql(tenant_id, &config, &code, program_id, version).await?,
                    );
                    db.lock()
                        .await
                        .set_program_status_guarded(
                            tenant_id,
                            program_id,
                            version,
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
    program_id: ProgramId,
    version: Version,
    compiler_process: Child,
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
        config: &ManagerConfig,
        code: &str,
        program_id: ProgramId,
        version: Version,
    ) -> AnyResult<Self> {
        debug!("Running SQL compiler on program '{program_id}', version '{version}'");

        // Create project directory.
        let sql_file_path = config.sql_file_path(program_id);
        let project_directory = sql_file_path.parent().unwrap();
        fs::create_dir_all(&project_directory).await.map_err(|e| {
            AnyError::msg(format!(
                "failed to create project directory '{}': '{e}'",
                project_directory.display()
            ))
        })?;

        // Write SQL code to file.
        fs::write(&sql_file_path, code).await?;

        let rust_file_path = config.rust_program_path(program_id);
        fs::create_dir_all(rust_file_path.parent().unwrap()).await?;

        let stderr_path = config.compiler_stderr_path(program_id);
        let err_file = File::create(&stderr_path).await.map_err(|e| {
            AnyError::msg(format!(
                "failed to create error log '{}': '{e}'",
                stderr_path.display()
            ))
        })?;

        // `main.rs` file.
        let rust_file = File::create(&rust_file_path).await.map_err(|e| {
            AnyError::msg(format!(
                "failed to create '{}': '{e}'",
                rust_file_path.display()
            ))
        })?;

        // Run compiler, direct output to `main.rs`.
        let schema_path = config.schema_path(program_id);
        let compiler_process = Command::new(config.sql_compiler_path())
            .arg("-js")
            .arg(schema_path)
            .arg(sql_file_path.as_os_str())
            .arg("-i")
            .arg("-je")
            .stdin(Stdio::null())
            .stderr(Stdio::from(err_file.into_std().await))
            .stdout(Stdio::from(rust_file.into_std().await))
            .spawn()
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to start SQL compiler '{}': '{e}'",
                    sql_file_path.display()
                ))
            })?;

        Ok(Self {
            tenant_id,
            stage: Stage::Sql,
            program_id,
            version,
            compiler_process,
        })
    }

    // Run `cargo` on the generated Rust workspace.
    async fn rust(
        tenant_id: TenantId,
        config: &ManagerConfig,
        program_id: ProgramId,
        version: Version,
    ) -> AnyResult<Self> {
        debug!("Running Rust compiler on program '{program_id}', version '{version}'");

        let mut main_rs = OpenOptions::new()
            .append(true)
            .open(&config.rust_program_path(program_id))
            .await?;
        main_rs.write_all(MAIN_FUNCTION.as_bytes()).await?;
        drop(main_rs);

        // Write `project/Cargo.toml`.
        Compiler::write_project_toml(config, program_id).await?;

        // Write workspace `Cargo.toml`.  The workspace contains SQL libs and the
        // generated project crate.
        Compiler::write_workspace_toml(config, program_id).await?;

        // Run cargo, direct stdout and stderr to the same file.
        let compiler_process = Compiler::run_cargo_build(config, program_id).await?;

        Ok(Self {
            tenant_id,
            stage: Stage::Rust,
            program_id,
            version,
            compiler_process,
        })
    }

    /// Async-wait for the compiler to terminate.
    async fn wait(&mut self) -> AnyResult<ExitStatus> {
        let exit_status = self.compiler_process.wait().await?;
        Ok(exit_status)
        // doesn't update status
    }

    /// Read error output of (Rust or SQL) compiler.
    async fn error_output(&self, config: &ManagerConfig) -> AnyResult<String> {
        let output = match self.stage {
            Stage::Sql => fs::read_to_string(config.compiler_stderr_path(self.program_id)).await?,
            Stage::Rust => {
                let stdout =
                    fs::read_to_string(config.compiler_stdout_path(self.program_id)).await?;
                let stderr =
                    fs::read_to_string(config.compiler_stderr_path(self.program_id)).await?;
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
