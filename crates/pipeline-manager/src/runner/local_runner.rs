//! A local runner that watches for pipeline objects in the API
//! and instantiates them locally as processes.

use crate::config::LocalRunnerConfig;
use crate::db::types::common::Version;
use crate::db::types::pipeline::{ExtendedPipelineDescr, PipelineId};
use crate::db::types::program::generate_pipeline_config;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::logs_buffer::LogsBuffer;
use crate::runner::pipeline_executor::{LogMessage, PipelineExecutionDesc, PipelineExecutor};
use async_trait::async_trait;
use feldera_types::config::{StorageCacheConfig, StorageConfig};
use log::{debug, error};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all},
    select, spawn,
};

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
///
/// The log follow requests are replied to by a background thread:
/// - If the pipeline has not yet started, it sends back rejections
/// - If it has started, the thread adds new followers and catches them up,
///   and sends the latest log lines as they come in to existing followers
#[allow(clippy::type_complexity)]
pub struct LocalRunner {
    pipeline_id: PipelineId,
    config: LocalRunnerConfig,
    process: Option<Child>,
    log_terminate_sender_and_join_handle: Option<(
        oneshot::Sender<()>,
        JoinHandle<mpsc::Receiver<mpsc::Sender<LogMessage>>>,
    )>,
}

impl Drop for LocalRunner {
    fn drop(&mut self) {
        // This shouldn't normally happen, as the runner should shut down the
        // pipeline before destroying the automaton, but we make sure that the
        // pipeline process is still killed on error.  We use `start_kill`
        // to avoid blocking in `drop`.
        if let Some(p) = &mut self.process {
            let _ = p.start_kill();
        }

        // Terminate the log thread as best as possible by sending termination message
        // and aborting the task associated to the join handle
        if let Some((terminate_sender, join_handle)) =
            self.log_terminate_sender_and_join_handle.take()
        {
            let _ = terminate_sender.send(());
            join_handle.abort()
        }
    }
}

impl LocalRunner {
    async fn kill_pipeline(&mut self) {
        if let Some(mut p) = self.process.take() {
            let _ = p.kill().await;
            let _ = p.wait().await;
        }
    }

    /// Sets up a thread which listens to log follow requests and new incoming lines
    /// from stdout/stderr. New followers are caught up and existing followers receive
    /// the new stdout/stderr lines as they come in.
    ///
    /// Returns a sender to invoke termination of the thread and the corresponding join handle.
    fn setup_log_operational(
        pipeline_id: PipelineId,
        stdout: ChildStdout,
        stderr: ChildStderr,
        mut log_follow_request_receiver: mpsc::Receiver<mpsc::Sender<LogMessage>>,
    ) -> (
        oneshot::Sender<()>,
        JoinHandle<mpsc::Receiver<mpsc::Sender<LogMessage>>>,
    ) {
        let (terminate_sender, mut terminate_receiver) = oneshot::channel::<()>();
        let join_handle = spawn(async move {
            // Buffered line readers such that we can already segment it into lines,
            // such that stdout and stderr can be interleaved without making it unreadable
            let mut lines_stdout = BufReader::new(stdout).lines();
            let mut lines_stderr = BufReader::new(stderr).lines();

            // Buffer with the latest lines
            let mut logs = LogsBuffer::new(
                Self::LOGS_BUFFER_LIMIT_BYTE,
                Self::LOGS_BUFFER_LIMIT_NUM_LINES,
            );

            // All parties interested in receiving the logs
            let mut log_followers: Vec<mpsc::Sender<LogMessage>> = Vec::new();

            // Continues running until the logging is sent a termination message,
            // or an I/O or send error is encountered
            let mut stdout_finished = false;
            let mut stderr_finished = false;
            loop {
                select! {
                    // Terminate
                    _ = &mut terminate_receiver => {
                        Self::end_log_of_followers(&mut log_followers, LogMessage::End("LOG STREAM END: pipeline is being shutdown".to_string())).await;
                        debug!("Terminating logging by request");
                        break;
                    }
                    // New follower
                    follower = log_follow_request_receiver.recv() => {
                        if let Some(follower) = follower {
                            Self::catch_up_and_add_follower(&mut logs, &mut log_followers, follower).await;
                        } else {
                            // The automata cannot recover from this as this is the only method to receive log followers from the runner.
                            // Operational logging should not be active when the pipeline is shutdown, and only if the pipeline is
                            // shutdown should it be deleted (which would cause this). As such, this is an error.
                            error!("Log request channel closed during operational logging. Logs for this pipeline will no longer work.");
                            break;
                        }
                    }
                    // New stdout line
                    line = lines_stdout.next_line(), if !stdout_finished => {
                        match line {
                            Ok(line) => match line {
                                None => {
                                    stdout_finished = true;
                                }
                                Some(line) => {
                                    println!("{line}"); // Also print it to manager's stdout
                                    Self::process_log_line_with_followers(&mut logs, &mut log_followers, line).await;
                                }
                            },
                            Err(e) => {
                                let explanation = format!("stdout experienced I/O error. Logs for this pipeline until restarted will no longer work. Error: {e}");
                                Self::end_log_of_followers(&mut log_followers, LogMessage::End(format!("LOG STREAM END: {explanation}"))).await;
                                error!("Logs of pipeline {pipeline_id} ended incorrectly: {explanation}");
                                break;
                            }
                        }
                    }
                    // New stderr line
                    line = lines_stderr.next_line(), if !stderr_finished => {
                        match line {
                            Ok(line) => match line {
                                None => {
                                    stderr_finished = true;
                                }
                                Some(line) => {
                                    eprintln!("{line}"); // Also print it to manager's stderr
                                    Self::process_log_line_with_followers(&mut logs, &mut log_followers, line).await;
                                }
                            },
                            Err(e) => {
                                let explanation = format!("stderr experienced I/O error. Logs for this pipeline until restarted will no longer work. Error: {e}");
                                Self::end_log_of_followers(&mut log_followers, LogMessage::End(format!("LOG STREAM END: {explanation}"))).await;
                                error!("Logs of pipeline {pipeline_id} ended incorrectly: {explanation}");
                                break;
                            }
                        }
                    }
                }
            }

            // The end of the scope of this loop will cause the Senders of the
            // followers to be dropped and disconnected, which will notify the
            // Receivers on the other end

            // Return the log follow receiver such that it can be re-used when
            // the pipeline is restarted
            log_follow_request_receiver
        });
        debug!("Logging is operational for pipeline {}", pipeline_id);
        (terminate_sender, join_handle)
    }
}

#[async_trait]
impl PipelineExecutor for LocalRunner {
    type Config = LocalRunnerConfig;

    // Provisioning is over once the pipeline port file has been detected.
    const PROVISIONING_TIMEOUT: Duration = Duration::from_millis(10_000);
    const PROVISIONING_POLL_PERIOD: Duration = Duration::from_millis(300);

    // Shutdown is over once the process has exited.
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10_000);
    const SHUTDOWN_POLL_PERIOD: Duration = Duration::from_millis(300);

    /// Creation steps:
    /// - Spawn a log rejection thread
    /// - Construct object
    fn new(
        pipeline_id: PipelineId,
        config: Self::Config,
        log_follow_request_receiver: mpsc::Receiver<mpsc::Sender<LogMessage>>,
    ) -> Self {
        let (log_reject_terminate_sender, log_reject_join_handle) =
            Self::setup_log_rejection(pipeline_id, log_follow_request_receiver);
        Self {
            pipeline_id,
            config,
            process: None,
            log_terminate_sender_and_join_handle: Some((
                log_reject_terminate_sender,
                log_reject_join_handle,
            )),
        }
    }

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

    /// At initialization of the runner, we cannot reconnect with an existing running
    /// pipeline process because local processes are terminated upon pipeline manager exit.
    /// For this reason, it is not possible to re-acquire the stdout/stderr and as such there is
    /// nothing to implement for this local runner function.
    async fn init(&mut self, _was_started: bool) {
        // Nothing to implement
    }

    /// Starting consists of the following steps:
    /// 1. Create pipeline directory
    /// 2. Write configuration file
    /// 3. Fetch and copy over the binary executable
    /// 4. Launch process running the binary executable
    /// 5. Setup log following
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
        let mut process = Command::new(fetched_executable)
            .current_dir(self.config.pipeline_dir(pipeline_id))
            .arg("--config-file")
            .arg(&config_file_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| RunnerError::PipelineStartupError {
                pipeline_id,
                error: e.to_string(),
            })?;

        // Setup log following
        let stdout = process
            .stdout
            .take()
            .expect("stdout could not be taken out");
        let stderr = process
            .stderr
            .take()
            .expect("stderr could not be taken out");
        let (current_log_terminate_sender, current_log_join_handle) = self
            .log_terminate_sender_and_join_handle
            .take()
            .expect("Log terminate sender and join handle are not present");
        let log_follow_request_receiver =
            Self::terminate_log_thread(current_log_terminate_sender, current_log_join_handle).await;
        let (new_log_terminate_sender, new_log_join_handle) = Self::setup_log_operational(
            self.pipeline_id,
            stdout,
            stderr,
            log_follow_request_receiver,
        );

        // Store state
        self.process = Some(process);
        self.log_terminate_sender_and_join_handle =
            Some((new_log_terminate_sender, new_log_join_handle));

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

    /// Shutdown steps:
    /// - Kill the process
    /// - Remove the pipeline directory
    /// - Shutdown operational logging thread and start the rejection thread
    async fn shutdown(&mut self) -> Result<(), ManagerError> {
        // Kill pipeline process
        self.kill_pipeline().await;

        // Remove the pipeline directory
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

        // Terminate the operational logging thread and start the rejection one
        let (current_log_terminate_sender, current_log_join_handle) = self
            .log_terminate_sender_and_join_handle
            .take()
            .expect("Log terminate sender and join handle are not present");
        let log_follow_request_receiver =
            Self::terminate_log_thread(current_log_terminate_sender, current_log_join_handle).await;
        let (new_log_terminate_sender, new_log_join_handle) =
            Self::setup_log_rejection(self.pipeline_id, log_follow_request_receiver);
        self.log_terminate_sender_and_join_handle =
            Some((new_log_terminate_sender, new_log_join_handle));

        Ok(())
    }

    /// A pipeline is shutdown when the process has exited.
    async fn check_if_shutdown(&mut self) -> bool {
        self.process
            .as_mut()
            .map(|p| p.try_wait().is_ok())
            .unwrap_or(true)
    }
}
