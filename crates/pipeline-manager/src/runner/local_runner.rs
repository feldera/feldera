//! A local runner that watches for pipeline objects in the API
//! and instantiates them locally as processes.

use crate::config::LocalRunnerConfig;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::common::Version;
use crate::db::types::pipeline::{ExtendedPipelineDescr, PipelineId};
use crate::db::types::program::generate_pipeline_config;
use crate::db_notifier::{DbNotification, Operation};
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::pipeline_automata::{
    PipelineAutomaton, PipelineExecutionDesc, PipelineExecutor,
};
use async_trait::async_trait;
use feldera_types::config::{StorageCacheConfig, StorageConfig};
use log::{debug, info, trace};
use std::time::Duration;
use std::{collections::BTreeMap, process::Stdio, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::sync::Notify;
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all},
    spawn,
    sync::Mutex,
};

// Provisioning is over once the pipeline port file has been detected.
const PROVISIONING_TIMEOUT: Duration = Duration::from_millis(10_000);
const PROVISIONING_POLL_PERIOD: Duration = Duration::from_millis(300);

// Shutdown is over once the process has exited.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10_000);
const SHUTDOWN_POLL_PERIOD: Duration = Duration::from_millis(300);

/// Retrieve the binary executable using its URL.
pub async fn fetch_binary_ref(
    config: &LocalRunnerConfig,
    binary_ref: &str,
    pipeline_id: PipelineId,
    program_version: Version,
) -> Result<String, ManagerError> {
    let parsed =
        url::Url::parse(binary_ref).expect("Can only be invoked with valid URLs created by us");
    match parsed.scheme() {
        // A file scheme assumes the binary is available locally where
        // the runner is located.
        "file" => {
            let exists = fs::try_exists(parsed.path()).await;
            match exists {
                Ok(true) => Ok(parsed.path().to_string()),
                Ok(false) => Err(RunnerError::BinaryFetchError {
                    pipeline_id,
                    error: format!(
                        "Binary required by pipeline {pipeline_id} does not exist at URL {}",
                        parsed.path()
                    ),
                }.into()),
                Err(e) => Err(RunnerError::BinaryFetchError {
                    pipeline_id,
                    error: format!(
                        "Accessing URL {} for binary required by pipeline {pipeline_id} returned an error: {}",
                        parsed.path(),
                        e
                    ),
                }.into()),
            }
        }
        // Access a file over HTTP/HTTPS
        // TODO: implement retries
        "http" | "https" => {
            let resp = reqwest::get(binary_ref).await;
            match resp {
                Ok(resp) => {
                    let resp = resp.bytes().await.expect("Binary reference should be accessible as bytes");
                    let resp_ref = resp.as_ref();
                    let path = config.binary_file_path(pipeline_id, program_version);
                    let mut file = tokio::fs::File::options()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .read(true)
                        .mode(0o760)
                        .open(path.clone())
                        .await
                        .map_err(|e|
                            ManagerError::io_error(
                                format!("File creation failed ({:?}) while saving {pipeline_id} binary fetched from '{}'", path, parsed.path()),
                                e,
                            )
                        )?;
                    file.write_all(resp_ref).await.map_err(|e|
                        ManagerError::io_error(
                            format!("File write failed ({:?}) while saving binary file for {pipeline_id} fetched from '{}'", path, parsed.path()),
                            e,
                        )
                    )?;
                    file.flush().await.map_err(|e|
                        ManagerError::io_error(
                            format!("File flush() failed ({:?}) while saving binary file for {pipeline_id} fetched from '{}'", path, parsed.path()),
                            e,
                        )
                    )?;
                    Ok(path.into_os_string().into_string().expect("Path should be valid Unicode"))
                }
                Err(e) => {
                    Err(RunnerError::BinaryFetchError {
                        pipeline_id,
                        error: format!(
                            "Fetching URL {} for binary required by pipeline {pipeline_id} returned an error: {}",
                            parsed.path(), e
                        ),
                    }.into())
                }
            }
        }
        _ => todo!("Unsupported URL scheme for binary ref"),
    }
}

/// A runner handle to run a pipeline using a local process.
/// * Start: spawn a process
/// * Retrieve its location (port)
/// * Shutdown: kill the spawned process
///
/// In addition, it attempts to non-blocking kill the spawned process upon
/// `drop` (which occurs when the owning automaton exits its run loop).
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
    /// Converts a pipeline descriptor into a self-contained execution descriptor.
    async fn to_execution_desc(
        &self,
        pipeline: &ExtendedPipelineDescr,
    ) -> Result<PipelineExecutionDesc, ManagerError> {
        // Handle optional fields
        let (inputs, outputs) = match pipeline.program_info.clone() {
            None => {
                return Err(ManagerError::from(
                    RunnerError::PipelineMissingProgramInfo {
                        pipeline_name: pipeline.name.clone(),
                        pipeline_id: pipeline.id,
                    },
                ))
            }
            Some(program_info) => (
                program_info.input_connectors,
                program_info.output_connectors,
            ),
        };
        let program_binary_url = match pipeline.program_binary_url.clone() {
            None => {
                return Err(ManagerError::from(
                    RunnerError::PipelineMissingProgramBinaryUrl {
                        pipeline_name: pipeline.name.clone(),
                        pipeline_id: pipeline.id,
                    },
                ))
            }
            Some(program_binary_url) => program_binary_url,
        };

        let mut deployment_config =
            generate_pipeline_config(pipeline.id, &pipeline.runtime_config, &inputs, &outputs);

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
            program_binary_url,
            deployment_config,
        })
    }

    /// Starting is done by creating a data directory, followed by writing a configuration file
    /// to it and copying over the binary executable. Finally, a process running the
    /// binary executable is launched.
    async fn start(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError> {
        let pipeline_id = ped.pipeline_id;
        let program_version = ped.program_version;

        debug!("Pipeline config is '{:?}'", ped.deployment_config);

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
        let expanded_config = serde_yaml::to_string(&ped.deployment_config).unwrap(); // TODO: unwrap
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

    /// Pipeline location is determined by finding and reading its port file.
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

    /// Shutdown by killing the process and removing the pipeline directory.
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

    /// A pipeline is shutdown when the process has exited.
    async fn check_if_shutdown(&mut self) -> bool {
        self.pipeline_process
            .as_mut()
            .map(|p| p.try_wait().is_ok())
            .unwrap_or(true)
    }
}

/// Starts a runner that executes pipelines locally using processes.
///
/// # Starting a pipeline
///
/// Starting a pipeline amounts to running the compiled executable with
/// selected config, and monitoring the pipeline log file for either
/// "Started HTTP server on port XXXXX" or "Failed to create server
/// [detailed error message]". In the former case, the port number is
/// recorded in the database. In the latter case, the error message is
/// returned to the client.
///
/// # Shutting down a pipeline
///
/// To shutdown the pipeline, the runner sends a `/shutdown` HTTP request to the
/// pipeline. This request is asynchronous: the pipeline may continue running
/// for a few seconds after the request succeeds.
pub async fn run(db: Arc<Mutex<StoragePostgres>>, config: &LocalRunnerConfig) {
    let runner_task = spawn(reconcile(db, Arc::new(config.clone())));
    runner_task.await.unwrap().unwrap(); // TODO: unwrap
}

/// Continuous reconciliation loop between what is stored about the pipelines in the database and
/// the local process runner managing their deployment as processes.
async fn reconcile(
    db: Arc<Mutex<StoragePostgres>>,
    config: Arc<LocalRunnerConfig>,
) -> Result<(), ManagerError> {
    // Mapping of the present pipelines to a notifier to the pipeline automaton
    let pipelines: Mutex<BTreeMap<PipelineId, Arc<Notify>>> = Mutex::new(BTreeMap::new());

    // Channel between the listener which listens to any database-triggered notifications
    // regarding whether pipelines should be added, updated or deleted
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    spawn(crate::db_notifier::listen(db.clone(), tx));

    // Continuously wait and act on pipeline operation notifications
    info!("Local runner is ready");
    loop {
        trace!("Waiting for pipeline operation notification from database...");
        if let Some(DbNotification::Pipeline(op, tenant_id, pipeline_id)) = rx.recv().await {
            debug!("Received pipeline operation notification: operation={op:?} tenant_id={tenant_id} pipeline_id={pipeline_id}");
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
                                    PROVISIONING_TIMEOUT,
                                    PROVISIONING_POLL_PERIOD,
                                    SHUTDOWN_TIMEOUT,
                                    SHUTDOWN_POLL_PERIOD,
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
