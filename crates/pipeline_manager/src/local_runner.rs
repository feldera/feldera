//! A local runner that watches for pipeline objects in the API
//! and instantiates them locally as processes.

use crate::db::PipelineRevision;
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
use tokio::process::{Child, Command};
use tokio::sync::Notify;
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all},
    spawn,
    sync::Mutex,
};

/// A handle to the pipeline process that kills the pipeline
/// on `drop`.
pub struct ProcessRunner {
    pipeline_id: PipelineId,
    pipeline_process: Option<Child>,
    config: Arc<LocalRunnerConfig>,
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
}

#[async_trait]
impl PipelineExecutor for ProcessRunner {
    /// Convert a PipelineRevision into a PipelineExecutionDesc.
    async fn to_execution_desc(
        &self,
        mut pr: PipelineRevision,
        binary_ref: String,
    ) -> Result<PipelineExecutionDesc, ManagerError> {
        let pipeline_dir = self.config.pipeline_dir(pr.pipeline.pipeline_id);
        let pipeline_data_dir = pipeline_dir.join("data");
        pr.config.storage_location = if pr.config.global.storage {
            Some(pipeline_data_dir.to_string_lossy().into())
        } else {
            None
        };

        Ok(PipelineExecutionDesc {
            pipeline_id: pr.pipeline.pipeline_id,
            pipeline_name: pr.pipeline.name,
            program_id: pr.program.program_id,
            version: pr.program.version,
            config: pr.config,
            binary_ref,
        })
    }

    async fn start(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError> {
        let pipeline_id = ped.pipeline_id;
        let program_id = ped.program_id;
        let version = ped.version;

        log::debug!("Pipeline config is '{:?}'", ped.config);

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
        if let Some(ref pipeline_data_dir) = ped.config.storage_location {
            create_dir_all(&pipeline_data_dir).await.map_err(|e| {
                ManagerError::io_error(
                    format!("creating pipeline data directory '{}'", pipeline_data_dir),
                    e,
                )
            })?;
        }

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
pub async fn run(db: Arc<Mutex<ProjectDB>>, config: &LocalRunnerConfig) {
    let runner_task = spawn(reconcile(db, Arc::new(config.clone())));
    runner_task.await.unwrap().unwrap();
}

async fn reconcile(
    db: Arc<Mutex<ProjectDB>>,
    config: Arc<LocalRunnerConfig>,
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
