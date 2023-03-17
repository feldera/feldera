use crate::{ManagerConfig, ProjectDB, ProjectId, Version};
use anyhow::{Error as AnyError, Result as AnyResult};
use fs_extra::{dir, dir::CopyOptions};
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
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema)]
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

/// Project compilation status.
#[derive(Debug, Serialize, Eq, PartialEq, ToSchema)]
pub(crate) enum ProjectStatus {
    /// Initial state: project has been created or modified, but the user
    /// hasn't yet started compiling the project.
    None,
    /// Compilation request received from the user; project has been placed
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

impl ProjectStatus {
    /// Return true if project is currently compiling.
    pub(crate) fn is_compiling(&self) -> bool {
        *self == ProjectStatus::CompilingRust || *self == ProjectStatus::CompilingSql
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
        fs::create_dir_all(&config.workspace_dir())
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to create Rust workspace directory '{}': {e}",
                    config.workspace_dir().display()
                ))
            })?;

        // Copy SQL libraries to the workspace.  We do this instead of
        // just referring to them as external dependencies, so that we can
        // use the cargo `[patch]` mechanism to overwrite their `dbsp` crate
        // dependencies.
        let mut copy_options = CopyOptions::new();
        copy_options.overwrite = true;
        copy_options.copy_inside = true;
        dir::copy(config.sql_lib_path(), config.workspace_dir(), &copy_options)?;

        let compiler_task = spawn(Self::compiler_task(config.clone(), db));
        Ok(Self { compiler_task })
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
                        // Project was deleted, updated or the user changed its status
                        // to cancelled -- abort compilation.
                        let descr = db.lock().await.get_project_if_exists(job.project_id).await?;
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
                // (i.e. run the Rust compiler after SQL) or update project status in the
                // database.
                Some(exit_status) = async {
                    if let Some(job) = &mut job {
                        Some(job.wait().await)
                    } else {
                        None
                    }
                }, if job.is_some() => {
                    let project_id = job.as_ref().unwrap().project_id;
                    let version = job.as_ref().unwrap().version;
                    let db = db.lock().await;

                    match exit_status {
                        Ok(status) if status.success() && job.as_ref().unwrap().is_sql() => {
                            // SQL compiler succeeded -- start the Rust job.
                            db.set_project_status_guarded(
                                project_id,
                                version,
                                ProjectStatus::CompilingRust,
                            ).await?;

                            // Read the schema so we can store it in the DB.
                            //
                            // - We trust the compiler that it put the file
                            // there if it succeeded.
                            // - We hold the db lock so we are executing this
                            // update in the same transaction as the project
                            // status above.
                            let schema_json = fs::read_to_string(config.schema_path(project_id)).await?;
                            db.set_project_schema(project_id, schema_json).await?;

                            debug!("Set ProjectStatus::CompilingRust '{project_id}', version '{version}'");
                            job = Some(CompilationJob::rust(&config, project_id, version).await?);
                        }
                        Ok(status) if status.success() && job.as_ref().unwrap().is_rust() => {
                            // Rust compiler succeeded -- declare victory.
                            db.set_project_status_guarded(project_id, version, ProjectStatus::Success).await?;
                            debug!("Set ProjectStatus::Success '{project_id}', version '{version}'");
                            job = None;
                        }
                        Ok(status) => {
                            // Compilation failed - update project status with the compiler
                            // error message.
                            let output = job.as_ref().unwrap().error_output(&config).await?;
                            let status = if job.as_ref().unwrap().is_rust() {
                                ProjectStatus::RustError(format!("{output}\nexit code: {status}"))
                            } else if let Ok(messages) = serde_json::from_str(&output) {
                                    // If we can parse the SqlCompilerMessages
                                    // as JSON, we assume the compiler worked:
                                    ProjectStatus::SqlError(messages)
                            } else {
                                    // Otherwise something unexpected happened
                                    // and we return a system error:
                                    ProjectStatus::SystemError(format!("{output}\nexit code: {status}"))
                            };
                            db.set_project_status_guarded(project_id, version, status).await?;
                            job = None;
                        }
                        Err(e) => {
                            let status = if job.unwrap().is_rust() {
                                ProjectStatus::SystemError(format!("I/O error with rustc: {e}"))
                            } else {
                                ProjectStatus::SystemError(format!("I/O error with sql-to-dbsp: {e}"))
                            };
                            db.set_project_status_guarded(project_id, version, status).await?;
                            job = None;
                        }
                    }
                }
            }
            // Pick the next project from the queue.
            if job.is_none() {
                let project = {
                    let db = db.lock().await;
                    if let Some((project_id, version)) = db.next_job().await? {
                        trace!("Next project in the queue: '{project_id}', version '{version}'");
                        let (_version, code) = db.project_code(project_id).await?;
                        Some((project_id, version, code))
                    } else {
                        None
                    }
                };

                if let Some((project_id, version, code)) = project {
                    job = Some(CompilationJob::sql(&config, &code, project_id, version).await?);
                    db.lock()
                        .await
                        .set_project_status_guarded(
                            project_id,
                            version,
                            ProjectStatus::CompilingSql,
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
    project_id: ProjectId,
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
        config: &ManagerConfig,
        code: &str,
        project_id: ProjectId,
        version: Version,
    ) -> AnyResult<Self> {
        debug!("Running SQL compiler on project '{project_id}', version '{version}'");

        // Create project directory.
        let sql_file_path = config.sql_file_path(project_id);
        let project_directory = sql_file_path.parent().unwrap();
        fs::create_dir_all(&project_directory).await.map_err(|e| {
            AnyError::msg(format!(
                "failed to create project directory '{}': '{e}'",
                project_directory.display()
            ))
        })?;

        // Write SQL code to file.
        fs::write(&sql_file_path, code).await?;

        let rust_file_path = config.rust_program_path(project_id);
        fs::create_dir_all(rust_file_path.parent().unwrap()).await?;

        let stderr_path = config.compiler_stderr_path(project_id);
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
        let schema_path = config.schema_path(project_id);
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
            stage: Stage::Sql,
            project_id,
            version,
            compiler_process,
        })
    }

    // Run `cargo` on the generated Rust workspace.
    async fn rust(
        config: &ManagerConfig,
        project_id: ProjectId,
        version: Version,
    ) -> AnyResult<Self> {
        debug!("Running Rust compiler on project '{project_id}', version '{version}'");

        let mut main_rs = OpenOptions::new()
            .append(true)
            .open(&config.rust_program_path(project_id))
            .await?;
        main_rs.write_all(MAIN_FUNCTION.as_bytes()).await?;
        drop(main_rs);

        // Write `project/Cargo.toml`.
        let template_toml = fs::read_to_string(&config.project_toml_template_path())
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to read template '{}': '{e}'",
                    config.project_toml_template_path().display()
                ))
            })?;
        let project_name = format!("name = \"{}\"", ManagerConfig::crate_name(project_id));
        let project_toml_code = template_toml
            .replace("name = \"temp\"", &project_name)
            .replace(", default-features = false", "")
            .replace(
                "[lib]\npath = \"src/lib.rs\"",
                &format!("\n\n[[bin]]\n{project_name}\npath = \"src/main.rs\""),
            );

        fs::write(&config.project_toml_path(project_id), project_toml_code)
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to write '{}': '{e}'",
                    config.project_toml_path(project_id).display()
                ))
            })?;

        // Write workspace `Cargo.toml`.  The workspace contains SQL libs and the
        // generated project crate.
        let mut workspace_toml_code = format!(
            "[workspace]\nmembers = [ \"lib/*\", \"{}\"]\n",
            ManagerConfig::crate_name(project_id),
        );

        // Generate the `[patch]` section to point to the local DBSP source tree.
        if let Some(dbsp_override_path) = &config.dbsp_override_path {
            let patch = format!(
                "[patch.'https://github.com/vmware/database-stream-processor']\n\
                dbsp = {{ path = \"{dbsp_override_path}/crates/dbsp\" }}\n\
                dbsp_adapters = {{ path = \"{dbsp_override_path}/crates/adapters\" }}"
            );
            workspace_toml_code.push_str(&patch);
        }

        fs::write(&config.workspace_toml_path(), workspace_toml_code)
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to write '{}': '{e}'",
                    config.workspace_toml_path().display()
                ))
            })?;

        let err_file = File::create(&config.compiler_stderr_path(project_id))
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to create '{}': '{e}'",
                    config.compiler_stderr_path(project_id).display()
                ))
            })?;
        let out_file = File::create(&config.compiler_stdout_path(project_id))
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to create '{}': '{e}'",
                    config.compiler_stdout_path(project_id).display()
                ))
            })?;

        // Run cargo, direct stdout and stderr to the same file.
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

        let compiler_process = command
            .spawn()
            .map_err(|e| AnyError::msg(format!("failed to start 'cargo': '{e}'")))?;

        Ok(Self {
            stage: Stage::Rust,
            project_id,
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
            Stage::Sql => fs::read_to_string(config.compiler_stderr_path(self.project_id)).await?,
            Stage::Rust => {
                let stdout =
                    fs::read_to_string(config.compiler_stdout_path(self.project_id)).await?;
                let stderr =
                    fs::read_to_string(config.compiler_stderr_path(self.project_id)).await?;
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
