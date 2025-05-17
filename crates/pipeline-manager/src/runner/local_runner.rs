//! A local runner that watches for pipeline objects in the API
//! and instantiates them locally as processes.

use crate::common_error::CommonError;
use crate::config::LocalRunnerConfig;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::logs_buffer::LogsBuffer;
use crate::runner::pipeline_executor::{LogMessage, PipelineExecutor, LOGS_END_MESSAGE};
use async_trait::async_trait;
use feldera_types::config::{PipelineConfig, StorageCacheConfig, StorageConfig};
use log::{debug, error, Level};
use reqwest::StatusCode;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::fs::{remove_dir_all, remove_file};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::mpsc::channel;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::{fs, fs::create_dir_all, select, spawn};

/// Retrieve the binary executable using its URL.
pub async fn fetch_binary_ref(
    config: &LocalRunnerConfig,
    client: &reqwest::Client,
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
                Ok(false) => Err(RunnerError::RunnerDeploymentProvisionError {
                    error: format!(
                        "Binary required by pipeline {pipeline_id} does not exist at URL {}",
                        parsed.path()
                    ),
                }.into()),
                Err(e) => Err(RunnerError::RunnerDeploymentProvisionError {
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
            let resp = client.get(binary_ref).send().await;
            match resp {
                Ok(resp) => {
                    if resp.status() != StatusCode::OK {
                        return Err(RunnerError::RunnerDeploymentProvisionError {
                            error: format!(
                                "response status of retrieving {} is {} instead of expected {}",
                                binary_ref, resp.status(), StatusCode::OK
                            ),
                        }.into());
                    }
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
                            ManagerError::from(CommonError::io_error(
                                format!("File creation failed ({:?}) while saving {pipeline_id} binary fetched from '{}'", path, parsed.path()),
                                e,
                            ))
                        )?;
                    file.write_all(resp_ref).await.map_err(|e|
                        ManagerError::from(CommonError::io_error(
                            format!("File write failed ({:?}) while saving binary file for {pipeline_id} fetched from '{}'", path, parsed.path()),
                            e,
                        ))
                    )?;
                    file.flush().await.map_err(|e|
                        ManagerError::from(CommonError::io_error(
                            format!("File flush() failed ({:?}) while saving binary file for {pipeline_id} fetched from '{}'", path, parsed.path()),
                            e,
                        ))
                    )?;
                    Ok(path.into_os_string().into_string().expect("Path should be valid Unicode"))
                }
                Err(e) => {
                    Err(RunnerError::RunnerDeploymentProvisionError {
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
    client: reqwest::Client,
    process: Option<Child>,
    log_deployment_check_sender: mpsc::Sender<String>,
    log_terminate_sender_and_join_handle: Option<(
        oneshot::Sender<()>,
        JoinHandle<(
            mpsc::Receiver<mpsc::Sender<LogMessage>>,
            mpsc::Receiver<String>,
        )>,
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
    /// Number of deployment check messages that can be buffered before dropping instead of sending.
    const CHANNEL_DEPLOYMENT_CHECK_BUFFER_SIZE: usize = 10000;

    /// Sets up a thread which listens to log follow requests and new incoming lines
    /// from stdout/stderr. New followers are caught up and existing followers receive
    /// the new stdout/stderr lines as they come in.
    ///
    /// Returns a sender to invoke termination of the thread and the corresponding join handle.
    #[allow(clippy::type_complexity)]
    fn setup_log_operational(
        pipeline_id: PipelineId,
        stdout: ChildStdout,
        stderr: ChildStderr,
        mut log_follow_request_receiver: mpsc::Receiver<mpsc::Sender<LogMessage>>,
        mut log_deployment_check_receiver: mpsc::Receiver<String>,
    ) -> (
        oneshot::Sender<()>,
        JoinHandle<(
            mpsc::Receiver<mpsc::Sender<LogMessage>>,
            mpsc::Receiver<String>,
        )>,
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
                        Self::end_log_of_followers(&mut log_followers, LogMessage::End(
                            Self::format_control_plane_log_line(Level::Info, LOGS_END_MESSAGE)
                        )).await;
                        debug!("Logs ended: pipeline {pipeline_id} -- terminating logging by request");
                        break;
                    }
                    // New follower
                    follower = log_follow_request_receiver.recv() => {
                        if let Some(follower) = follower {
                            Self::catch_up_and_add_follower(&mut logs, &mut log_followers, follower).await;
                        } else {
                            // Fatal: this should only occur upon pipeline deletion, which should not be while it is not shutdown (operational).
                            error!("Logs failed: {pipeline_id} -- follow request channel itself closed while operational. Logs will no longer work.");
                            break;
                        }
                    }
                    // New stdout line
                    line = lines_stdout.next_line(), if !stdout_finished => {
                        match line {
                            Ok(line) => match line {
                                None => {
                                    stdout_finished = true;
                                    Self::process_log_line_with_followers(
                                        &mut logs,
                                        &mut log_followers,
                                        Self::format_control_plane_log_line(Level::Info, "Process stdout ended")
                                    ).await;
                                    debug!("Logs stdout ended: pipeline {pipeline_id}");
                                }
                                Some(line) => {
                                    println!("{line}"); // Also print it to manager's stdout
                                    Self::process_log_line_with_followers(&mut logs, &mut log_followers, line).await;
                                }
                            },
                            Err(e) => {
                                stdout_finished = true;
                                let message = format!("Process stdout encountered I/O error: {e}");
                                Self::process_log_line_with_followers(
                                    &mut logs,
                                    &mut log_followers,
                                    Self::format_control_plane_log_line(Level::Error, &message)
                                ).await;
                                error!("Logs stdout ended (unexpected): pipeline {pipeline_id}: {message}");
                            }
                        }
                    }
                    // New stderr line
                    line = lines_stderr.next_line(), if !stderr_finished => {
                        match line {
                            Ok(line) => match line {
                                None => {
                                    stderr_finished = true;
                                     Self::process_log_line_with_followers(
                                        &mut logs,
                                        &mut log_followers,
                                        Self::format_control_plane_log_line(Level::Info, "Process stderr ended")
                                    ).await;
                                    debug!("Logs stderr ended: pipeline {pipeline_id}");
                                }
                                Some(line) => {
                                    eprintln!("{line}"); // Also print it to manager's stderr
                                    Self::process_log_line_with_followers(&mut logs, &mut log_followers, line).await;
                                }
                            },
                            Err(e) => {
                                stderr_finished = true;
                                let message = format!("Process stderr encountered I/O error: {e}");
                                Self::process_log_line_with_followers(
                                    &mut logs,
                                    &mut log_followers,
                                    Self::format_control_plane_log_line(Level::Error, &message)
                                ).await;
                                error!("Logs stderr ended (unexpected): pipeline {pipeline_id}: {message}");
                            }
                        }
                    }
                    // New deployment check
                    message = log_deployment_check_receiver.recv() => {
                        if let Some(message) = message {
                            Self::process_log_line_with_followers(
                                &mut logs,
                                &mut log_followers,
                                Self::format_control_plane_log_line(Level::Error, &message)
                            ).await;
                        } else {
                            // Fatal: this should only occur upon pipeline deletion, which should not be while it is not shutdown (operational).
                            error!("Logs failed: {pipeline_id} -- deployment check channel itself closed while operational. Logs will no longer work.");
                            break;
                        }
                    }
                }
            }

            // The end of the scope of this loop will cause the Senders of the
            // followers to be dropped and disconnected, which will notify the
            // Receivers on the other end.

            // Return the log follow receiver such that it can be re-used when
            // the pipeline is restarted.
            (log_follow_request_receiver, log_deployment_check_receiver)
        });
        debug!("Logging is operational for pipeline {}", pipeline_id);
        (terminate_sender, join_handle)
    }

    /// Checks the process and working directory of the deployment.
    /// Returns a fatal error if it is encountered.
    async fn inner_check(&mut self) -> Result<(), String> {
        // Pipeline process status must be checkable and not have exited
        let process_check = if let Some(p) = &mut self.process {
            match p.try_wait() {
                Ok(status) => {
                    if let Some(status) = status {
                        // If there is an exit status, the process has exited
                        Err(format!("Pipeline process exited prematurely with {status}"))
                    } else {
                        Ok(())
                    }
                }
                Err(e) => {
                    // Unable to check if it has an exit status, which indicates something went wrong
                    Err(format!(
                        "Pipeline process status could not be checked due to: {e}"
                    ))
                }
            }
        } else {
            // No process handle
            Err("The pipeline-manager and this pipeline were previously terminated. Shutdown and start the pipeline again.".to_string())
        };

        // Pipeline working directory must exist
        let pipeline_dir = &self.config.pipeline_dir(self.pipeline_id);
        let working_dir_check = if Path::new(pipeline_dir).is_dir() {
            Ok(())
        } else {
            Err(format!(
                "Working directory '{}' no longer exists.",
                pipeline_dir.to_string_lossy()
            ))
        };

        // Collect errors
        let mut errors = vec![];
        if let Err(e) = &working_dir_check {
            errors.push(e.clone());
        }
        if let Err(e) = &process_check {
            errors.push(e.clone());
        }

        // Write to both runner and pipeline log
        for error in &errors {
            error!("Deployment of pipeline {}: {}", self.pipeline_id, error);
            if let Err(e) = self.log_deployment_check_sender.try_send(error.clone()) {
                error!("Unable to send deployment check to the pipeline log due to: {e}");
            }
        }

        // Result
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("\n"))
        }
    }
}

#[async_trait]
impl PipelineExecutor for LocalRunner {
    type Config = LocalRunnerConfig;

    // Provisioning is over once the process is spawned and the port file created by it is detected.
    const DEFAULT_PROVISIONING_TIMEOUT: Duration = Duration::from_millis(20_000);

    /// Creation steps:
    /// - Spawn a log rejection thread
    /// - Construct object
    fn new(
        pipeline_id: PipelineId,
        config: Self::Config,
        client: reqwest::Client,
        log_follow_request_receiver: mpsc::Receiver<mpsc::Sender<LogMessage>>,
    ) -> Self {
        let (log_deployment_check_sender, log_deployment_check_receiver) =
            channel::<String>(Self::CHANNEL_DEPLOYMENT_CHECK_BUFFER_SIZE);
        let (log_reject_terminate_sender, log_reject_join_handle) = Self::setup_log_rejection(
            pipeline_id,
            log_follow_request_receiver,
            log_deployment_check_receiver,
        );
        Self {
            pipeline_id,
            config,
            client,
            process: None,
            log_deployment_check_sender,
            log_terminate_sender_and_join_handle: Some((
                log_reject_terminate_sender,
                log_reject_join_handle,
            )),
        }
    }

    /// It is not possible to re-acquire the stdout/stderr as the process
    /// is killed upon runner exit and the handle is lost. As such, there
    /// is nothing to implement for this local runner function.
    async fn init(&mut self, _was_provisioned: bool) {
        // Nothing to implement
    }

    /// Storage is located at:
    /// `<working-directory>/pipeline-<pipeline_id>/storage`
    async fn generate_storage_config(&self) -> StorageConfig {
        let pipeline_dir = self.config.pipeline_dir(self.pipeline_id);
        let pipeline_storage_dir = pipeline_dir.join("storage");
        StorageConfig {
            path: pipeline_storage_dir.to_string_lossy().into(),
            cache: StorageCacheConfig::default(),
        }
    }

    /// Provisions the process deployment.
    /// - Creates pipeline working directory
    /// - Creates pipeline storage directory
    /// - Writes config.yaml to pipeline working directory
    /// - Retrieve and writes executable to pipeline working directory
    /// - Runs executable as process in pipeline working directory
    /// - Switches to operational logging following the stdout/stderr of the process
    async fn provision(
        &mut self,
        deployment_config: &PipelineConfig,
        program_binary_url: &str,
        program_version: Version,
        suspend_info: Option<serde_json::Value>,
    ) -> Result<(), ManagerError> {
        // (Re-)create pipeline working directory (which will contain storage directory)
        let pipeline_dir = self.config.pipeline_dir(self.pipeline_id);
        if suspend_info.is_none() {
            let _ = remove_dir_all(&pipeline_dir).await;
        }
        create_dir_all(&pipeline_dir).await.map_err(|e| {
            ManagerError::from(CommonError::io_error(
                format!(
                    "create pipeline working directory '{}'",
                    pipeline_dir.display()
                ),
                e,
            ))
        })?;

        // (Re-)create pipeline storage directory
        if let Some(storage_config) = &deployment_config.storage_config {
            if suspend_info.is_none() {
                let _ = remove_dir_all(&storage_config.path).await;
            }
            create_dir_all(&storage_config.path).await.map_err(|e| {
                ManagerError::from(CommonError::io_error(
                    format!(
                        "create pipeline storage directory '{}'",
                        storage_config.path
                    ),
                    e,
                ))
            })?;
        }

        // Write config.yaml
        let config_file_path = self.config.config_file_path(self.pipeline_id);
        let expanded_config = serde_yaml::to_string(&deployment_config)
            .expect("Deployment configuration serialization failed");
        fs::write(&config_file_path, &expanded_config)
            .await
            .map_err(|e| {
                ManagerError::from(CommonError::io_error(
                    format!("write config file '{}'", config_file_path.display()),
                    e,
                ))
            })?;

        // Delete port file (which will only exist if we are restarting from a
        // checkpoint).
        let _ = remove_file(&self.config.port_file_path(self.pipeline_id)).await;

        // Retrieve and store executable in pipeline working directory
        let fetched_executable = fetch_binary_ref(
            &self.config,
            &self.client,
            program_binary_url,
            self.pipeline_id,
            program_version,
        )
        .await?;

        // Run executable:
        // - Current directory: pipeline working directory
        // - Configuration file: path to config.yaml
        // - Stdout/stderr are piped to follow logs
        let mut process = Command::new(fetched_executable)
            .current_dir(pipeline_dir)
            .arg("--config-file")
            .arg(&config_file_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| RunnerError::RunnerDeploymentProvisionError {
                error: format!("unable to spawn process due to: {e}"),
            })?;

        // Switch to operational logging
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
        let (log_follow_request_receiver, log_deployment_check_receiver) =
            Self::terminate_log_thread(current_log_terminate_sender, current_log_join_handle).await;
        let (new_log_terminate_sender, new_log_join_handle) = Self::setup_log_operational(
            self.pipeline_id,
            stdout,
            stderr,
            log_follow_request_receiver,
            log_deployment_check_receiver,
        );

        // Store state
        self.process = Some(process);
        self.log_terminate_sender_and_join_handle =
            Some((new_log_terminate_sender, new_log_join_handle));

        Ok(())
    }

    /// Process deployment provisioning is completed when the port file is found and read.
    async fn is_provisioned(&mut self) -> Result<Option<String>, ManagerError> {
        if let Err(error) = self.inner_check().await {
            return Err(ManagerError::from(
                RunnerError::RunnerDeploymentCheckError { error },
            ));
        }

        let port_file_path = self.config.port_file_path(self.pipeline_id);
        match fs::read_to_string(port_file_path).await {
            Ok(port) => {
                let parse = port.trim().parse::<u16>();
                match parse {
                    Ok(port) => {
                        let host = &self.config.pipeline_host;
                        Ok(Some(format!("{host}:{port}")))
                    }
                    Err(e) => Err(ManagerError::from(
                        RunnerError::RunnerDeploymentProvisionError {
                            error: format!("unable to parse port file due to: {e}"),
                        },
                    )),
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Checks the process deployment is healthy.
    /// - Pipeline working directory must exist
    /// - Process status must be checkable and not be exited
    async fn check(&mut self) -> Result<(), ManagerError> {
        if let Err(error) = self.inner_check().await {
            Err(ManagerError::from(
                RunnerError::RunnerDeploymentCheckError { error },
            ))
        } else {
            Ok(())
        }
    }

    async fn suspend_compute(&mut self) -> Result<(), ManagerError> {
        // Kill pipeline process
        if let Some(mut p) = self.process.take() {
            let _ = p.kill().await;
            let _ = p.wait().await;
        }

        // Switch to rejection logging
        let (current_log_terminate_sender, current_log_join_handle) = self
            .log_terminate_sender_and_join_handle
            .take()
            .expect("Log terminate sender and join handle are not present");
        let (log_follow_request_receiver, log_deployment_check_receiver) =
            Self::terminate_log_thread(current_log_terminate_sender, current_log_join_handle).await;
        let (new_log_terminate_sender, new_log_join_handle) = Self::setup_log_rejection(
            self.pipeline_id,
            log_follow_request_receiver,
            log_deployment_check_receiver,
        );
        self.log_terminate_sender_and_join_handle =
            Some((new_log_terminate_sender, new_log_join_handle));
        Ok(())
    }

    async fn is_compute_suspended(&mut self) -> Result<bool, ManagerError> {
        Ok(true) // TODO
    }

    /// Shuts down the process deployment.
    /// 1. Kill process
    /// 2. Remove pipeline working directory
    /// 3. Switch to rejection logging
    async fn shutdown(&mut self) -> Result<(), ManagerError> {
        // Kill pipeline process
        if let Some(mut p) = self.process.take() {
            let _ = p.kill().await;
            let _ = p.wait().await;
        }

        // Remove the pipeline working directory
        if self.config.pipeline_dir(self.pipeline_id).exists() {
            match remove_dir_all(self.config.pipeline_dir(self.pipeline_id)).await {
                Ok(_) => (),
                Err(e) => {
                    log::warn!(
                        "Runner failed to delete pipeline working directory for pipeline {}: {}",
                        self.pipeline_id,
                        e
                    );
                }
            }
        }

        // Switch to rejection logging
        let (current_log_terminate_sender, current_log_join_handle) = self
            .log_terminate_sender_and_join_handle
            .take()
            .expect("Log terminate sender and join handle are not present");
        let (log_follow_request_receiver, log_deployment_check_receiver) =
            Self::terminate_log_thread(current_log_terminate_sender, current_log_join_handle).await;
        let (new_log_terminate_sender, new_log_join_handle) = Self::setup_log_rejection(
            self.pipeline_id,
            log_follow_request_receiver,
            log_deployment_check_receiver,
        );
        self.log_terminate_sender_and_join_handle =
            Some((new_log_terminate_sender, new_log_join_handle));

        Ok(())
    }
}
