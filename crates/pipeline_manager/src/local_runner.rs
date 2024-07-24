//! A local runner that watches for pipeline objects in the API
//! and instantiates them locally as processes.

use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::{ExtendedPipelineDescr, PipelineId};
use crate::db_notifier::{DbNotification, Operation};
use crate::error::ManagerError;
use crate::pipeline_automata::{fetch_binary_ref, PipelineAutomaton};
use crate::pipeline_automata::{PipelineExecutionDesc, PipelineExecutor};
use crate::{config::LocalRunnerConfig, runner::RunnerError};
use async_trait::async_trait;
use log::trace;
use pipeline_types::config::{generate_pipeline_config, StorageCacheConfig, StorageConfig};
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
        pipeline: &ExtendedPipelineDescr<String>,
    ) -> Result<PipelineExecutionDesc, ManagerError> {
        let mut deployment_config = generate_pipeline_config(
            pipeline.id.0,
            &pipeline.runtime_config,
            &pipeline.program_schema.clone().unwrap(), // TODO: unwrap
        )
        .map_err(|e| RunnerError::PipelineConfigurationGenerationFailed { error: e })?;

        // The deployment configuration must be modified to fill in the details for storage,
        // which varies for each pipeline executor
        let pipeline_dir = self.config.pipeline_dir(pipeline.id);
        let pipeline_data_dir = pipeline_dir.join("data");
        deployment_config.storage_config = if deployment_config.global.storage {
            Some(StorageConfig {
                path: pipeline_data_dir.to_string_lossy().into(),
                cache: StorageCacheConfig::default(),
            })
        } else {
            None
        };

        Ok(PipelineExecutionDesc {
            pipeline_id: pipeline.id,
            pipeline_name: pipeline.name.clone(),
            program_version: pipeline.program_version,
            program_binary_url: pipeline.program_binary_url.clone().unwrap(), // TODO: unwrap
            deployment_config,
        })
    }

    async fn start(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError> {
        let pipeline_id = ped.pipeline_id;
        let program_version = ped.program_version;

        log::debug!("Pipeline config is '{:?}'", ped.deployment_config);

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
        if let Some(pipeline_data_dir) = ped
            .deployment_config
            .storage_config
            .as_ref()
            .map(|storage| &storage.path)
        {
            create_dir_all(&pipeline_data_dir).await.map_err(|e| {
                ManagerError::io_error(
                    format!("creating pipeline data directory '{}'", pipeline_data_dir),
                    e,
                )
            })?;
        }

        let config_file_path = self.config.config_file_path(pipeline_id);
        let expanded_config = serde_yaml::to_string(&ped.deployment_config).unwrap();
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
            &ped.program_binary_url,
            pipeline_id,
            program_version,
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
pub async fn run(db: Arc<Mutex<StoragePostgres>>, config: &LocalRunnerConfig) {
    let runner_task = spawn(reconcile(db, Arc::new(config.clone())));
    runner_task.await.unwrap().unwrap();
}

async fn reconcile(
    db: Arc<Mutex<StoragePostgres>>,
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
