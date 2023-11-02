use crate::config::CompilerConfig;
/// A local runner that watches for pipeline objects in the API
/// and instantiates them locally as processes.
use crate::db_notifier::{DbNotification, Operation};
use crate::pipeline_automata::{fetch_binary_ref, PipelineAutomaton};
use crate::pipeline_automata::{PipelineExecutionDesc, PipelineExecutor};
use crate::{
    api::ManagerError,
    config::LocalRunnerConfig,
    db::{PipelineId, ProjectDB},
    runner::RunnerError,
};
use async_trait::async_trait;
use log::trace;
use std::{collections::BTreeMap, process::Stdio, sync::Arc};
use tokio::sync::Notify;
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all},
    process::{Child, Command},
    spawn,
    sync::Mutex,
};

/// Limit on the chunk of stderr included in error messages.
static MAX_STDERR_LEN: usize = 102_400;

/// A handle to the pipeline process that kills the pipeline
/// on `drop`.
pub struct ProcessRunner {
    pipeline_id: PipelineId,
    pipeline_process: Option<Child>,
    config: Arc<LocalRunnerConfig>,
    compiler_config: Arc<CompilerConfig>,
}

impl Drop for ProcessRunner {
    fn drop(&mut self) {
        // This shouldn't normally happen, as the runner should shutdown the
        // pipeline before destroying the automaton, but we make sure that the
        // pipeline process is still killed on error.  We use `start_kill`
        // to avoid blocking in `drop`.
        self.pipeline_process.as_mut().map(|p| p.start_kill());
    }
}

impl ProcessRunner {
    async fn kill_pipeline(&mut self) {
        if let Some(mut p) = self.pipeline_process.take() {
            let _ = p.kill().await;
            let _ = p.wait().await;
        }
    }

    async fn start_binary(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError> {
        let pipeline_id = ped.pipeline_id;
        let program_id = ped.program_id;
        let version = ped.version;
        let config_file_path = self.config.config_file_path(pipeline_id);

        let fetched_executable = fetch_binary_ref(
            &self.config,
            &ped.binary_ref,
            pipeline_id,
            program_id,
            version,
        )
        .await?;

        // Run executable, set current directory to pipeline directory, pass metadata
        // file and config as arguments.
        let pipeline_process = Command::new(fetched_executable)
            .current_dir(self.config.pipeline_dir(pipeline_id))
            .arg("--config-file")
            .arg(&config_file_path)
            .stdin(Stdio::null())
            .spawn()
            .map_err(|e| RunnerError::PipelineStartupError {
                pipeline_id,
                error: e.to_string(),
            })?;
        self.pipeline_process = Some(pipeline_process);
        Ok(())
    }

    async fn start_jit(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError> {
        let pipeline_id = ped.pipeline_id;
        let pipeline_dir = self.config.pipeline_dir(pipeline_id);
        let config_file_path = self.config.config_file_path(pipeline_id);

        // Write SQL code to a file.
        let sql_file_path = self.config.sql_file_path(pipeline_id);
        fs::write(&sql_file_path, &ped.code).await.map_err(|e| {
            ManagerError::io_error(format!("writing SQL file '{}'", sql_file_path.display()), e)
        })?;

        let schema_file_path = self.config.schema_file_path(pipeline_id);
        let ir_file_path = self.config.jit_ir_file_path(pipeline_id);
        let ir_file = fs::File::create(&ir_file_path).await.map_err(|e| {
            ManagerError::io_error(
                format!("creating JIT IR file '{}'", ir_file_path.display()),
                e,
            )
        })?;

        // Run SQL compiler to generate IR.
        let output = Command::new(self.compiler_config.sql_compiler_path())
            .current_dir(&pipeline_dir)
            .arg("-js")
            .arg(&schema_file_path)
            .arg("-i")
            .arg("-j")
            .arg("--alltables")
            .arg("--outputsAreSets")
            .arg("--ignoreOrder")
            .arg(&sql_file_path)
            .stdout(ir_file.into_std().await)
            // Can't use `.output()` here, which redirects output to a pipe instead of the file.
            .spawn()
            .map_err(|e| ManagerError::io_error("running SQL compiler".to_string(), e))?
            .wait_with_output()
            .await
            .map_err(|e| {
                ManagerError::io_error("waitinf for the SQL compiler process".to_string(), e)
            })?;

        if !output.status.success() {
            let mut stderr = String::from_utf8_lossy(&output.stderr).to_string();
            stderr.truncate(MAX_STDERR_LEN);
            return Err(ManagerError::from(RunnerError::SqlToJitCompilerError {
                pipeline_id,
                status: output.status.code(),
                stderr,
            }));
        }

        // Run pipeline.
        let pipeline_process = Command::new(self.config.jit_pipeline_runner_path().unwrap())
            .current_dir(&pipeline_dir)
            .arg("--ir")
            .arg(&ir_file_path)
            .arg("--schema")
            .arg(&schema_file_path)
            .arg("--config-file")
            .arg(&config_file_path)
            .spawn()
            .map_err(|e| RunnerError::PipelineStartupError {
                pipeline_id,
                error: e.to_string(),
            })?;
        self.pipeline_process = Some(pipeline_process);
        Ok(())
    }
}

#[async_trait]
impl PipelineExecutor for ProcessRunner {
    async fn start(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError> {
        let pipeline_id = ped.pipeline_id;

        log::debug!("Pipeline config is '{:?}'", ped.config);

        if ped.jit_mode && !self.config.jit_support_enabled() {
            Err(RunnerError::JitSupportDisabled)?;
        }

        // Create pipeline directory (delete old directory if exists); write metadata
        // and config files to it.
        let pipeline_dir = self.config.pipeline_dir(pipeline_id);

        let _ = remove_dir_all(&pipeline_dir).await;
        create_dir_all(&pipeline_dir).await.map_err(|e| {
            ManagerError::io_error(
                format!("creating pipeline directory '{}'", pipeline_dir.display()),
                e,
            )
        })?;
        let config_file_path = self.config.config_file_path(pipeline_id);
        let expanded_config = serde_yaml::to_string(&ped.config).unwrap();
        fs::write(&config_file_path, &expanded_config)
            .await
            .map_err(|e| {
                ManagerError::io_error(
                    format!("writing config file '{}'", config_file_path.display()),
                    e,
                )
            })?;

        if ped.jit_mode {
            self.start_jit(ped).await
        } else {
            self.start_binary(ped).await
        }
    }

    async fn get_location(&mut self) -> Result<Option<String>, ManagerError> {
        let port_file_path = self.config.port_file_path(self.pipeline_id);
        let host = &self.config.pipeline_host;

        match fs::read_to_string(port_file_path).await {
            Ok(port) => {
                let parse = port.trim().parse::<u16>();
                match parse {
                    Ok(port) => Ok(Some(format!("{host}:{port}"))),
                    Err(e) => Err(ManagerError::from(RunnerError::PortFileParseError {
                        pipeline_id: self.pipeline_id,
                        error: e.to_string(),
                    })),
                }
            }
            Err(_) => Ok(None),
        }
    }

    async fn check_if_shutdown(&mut self) -> bool {
        self.pipeline_process
            .as_mut()
            .map(|p| p.try_wait().is_ok())
            .unwrap_or(true)
    }

    async fn shutdown(&mut self) -> Result<(), ManagerError> {
        self.kill_pipeline().await;
        match remove_dir_all(self.config.pipeline_dir(self.pipeline_id)).await {
            Ok(_) => (),
            Err(e) => {
                log::warn!(
                    "Failed to delete pipeline directory for pipeline {}: {}",
                    self.pipeline_id,
                    e
                );
            }
        }
        Ok(())
    }
}

/// Starts a runner that executes pipelines locally
///
/// # Starting a pipeline
///
/// Starting a pipeline amounts to running the compiled executable with
/// selected config, and monitoring the pipeline log file for either
/// "Started HTTP server on port XXXXX" or "Failed to create server
/// [detailed error message]".  In the former case, the port number is
/// recorded in the database.  In the latter case, the error message is
/// returned to the client.
///
/// # Shutting down a pipeline
///
/// To shutdown the pipeline, the runner sends a `/shutdown` HTTP request to the
/// pipeline.  This request is asynchronous: the pipeline may continue running
/// for a few seconds after the request succeeds.
pub async fn run(
    db: Arc<Mutex<ProjectDB>>,
    config: &LocalRunnerConfig,
    compiler_config: &CompilerConfig,
) {
    let runner_task = spawn(reconcile(
        db,
        Arc::new(config.clone()),
        Arc::new(compiler_config.clone()),
    ));
    runner_task.await.unwrap().unwrap();
}

async fn reconcile(
    db: Arc<Mutex<ProjectDB>>,
    config: Arc<LocalRunnerConfig>,
    compiler_config: Arc<CompilerConfig>,
) -> Result<(), ManagerError> {
    let pipelines: Mutex<BTreeMap<PipelineId, Arc<Notify>>> = Mutex::new(BTreeMap::new());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(crate::db_notifier::listen(db.clone(), tx));
    loop {
        trace!("Waiting for notification");
        if let Some(DbNotification::Pipeline(op, tenant_id, pipeline_id)) = rx.recv().await {
            trace!("Received DbNotification {op:?} {tenant_id} {pipeline_id}");
            match op {
                Operation::Add | Operation::Update => {
                    pipelines
                        .lock()
                        .await
                        .entry(pipeline_id)
                        .or_insert_with(|| {
                            let notifier = Arc::new(Notify::new());
                            let pipeline_handle = ProcessRunner {
                                pipeline_id,
                                pipeline_process: None,
                                config: config.clone(),
                                compiler_config: compiler_config.clone(),
                            };
                            spawn(
                                PipelineAutomaton::new(
                                    pipeline_id,
                                    tenant_id,
                                    db.clone(),
                                    notifier.clone(),
                                    pipeline_handle,
                                )
                                .run(),
                            );
                            notifier
                        })
                        .notify_one();
                }
                Operation::Delete => {
                    if let Some(n) = pipelines.lock().await.remove(&pipeline_id) {
                        // Notify the automaton so it shuts down
                        n.notify_one();
                    }
                }
            };
        }
    }
}
