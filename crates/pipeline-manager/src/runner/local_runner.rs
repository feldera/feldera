//! A local runner that watches for pipeline objects in the API
//! and instantiates them locally as processes.

use crate::common_error::CommonError;
use crate::config::{CommonConfig, LocalRunnerConfig};
use crate::db::types::pipeline::{
    bootstrap_policy_to_string, runtime_desired_status_to_string, PipelineId,
};
use crate::db::types::version::Version;
use crate::error::{source_error, ManagerError};
use crate::runner::error::RunnerError;
use crate::runner::pipeline_executor::PipelineExecutor;
use crate::runner::pipeline_logs::{LogMessage, LogsSender};
use async_trait::async_trait;
use feldera_observability::ReqwestTracingExt;
use feldera_types::config::{
    PipelineConfig, PipelineConfigProgramInfo, StorageCacheConfig, StorageConfig,
};
use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus};
use reqwest::StatusCode;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::fs::{remove_dir_all, remove_file};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::{fs, fs::create_dir_all, select, spawn};
use tracing::{error, warn, Level};
use uuid::Uuid;

/// How many times to attempt to retrieve the pipeline binary.
const BINARY_RETRIEVAL_ATTEMPTS: u64 = 5;

/// Interval between binary retrieval attempts.
const BINARY_RETRIEVAL_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// A runner handle to run a pipeline using a local process.
/// During provisioning, it spawns a process, retrieves its location (port)
/// and follows the logs of the process stdout and stderr. Upon stop, it kills
/// the spawned process and terminates the logging.
///
/// In addition, it attempts to non-blocking kill the spawned process upon
/// `drop` (which occurs when the owning automaton exits its run loop).
#[allow(clippy::type_complexity)]
pub struct LocalRunner {
    pipeline_id: PipelineId,
    common_config: CommonConfig,
    config: LocalRunnerConfig,
    client: reqwest::Client,
    process: Option<Child>,
    logs_sender: LogsSender,
    logs_thread_terminate_sender_and_join_handle: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl Drop for LocalRunner {
    fn drop(&mut self) {
        // This shouldn't normally happen, as the runner should stop the
        // pipeline before destroying the automaton, but we make sure that the
        // pipeline process is still killed on error.  We use `start_kill`
        // to avoid blocking in `drop`.
        if let Some(p) = &mut self.process {
            let _ = p.start_kill();
        }

        // Terminate the logs thread as best as possible by sending termination message
        // and aborting the task associated to the join handle
        if let Some((terminate_sender, join_handle)) =
            self.logs_thread_terminate_sender_and_join_handle.take()
        {
            let _ = terminate_sender.send(());
            join_handle.abort()
        }
    }
}

impl LocalRunner {
    /// Spawns a thread which follows the stdout and stderr of the process.
    /// The thread can be terminated by the return termination sender and join handle.
    fn start_logs_thread_for_stdout_stderr(
        pipeline_id: PipelineId,
        stdout: ChildStdout,
        stderr: ChildStderr,
        mut logs_sender: LogsSender,
    ) -> (oneshot::Sender<()>, JoinHandle<()>) {
        let (terminate_sender, mut terminate_receiver) = oneshot::channel::<()>();
        let join_handle = spawn(async move {
            // Buffered line readers such that we can already segment it into lines,
            // such that stdout and stderr can be interleaved without making it unreadable
            let mut lines_stdout = BufReader::new(stdout).lines();
            let mut lines_stderr = BufReader::new(stderr).lines();

            // Continues running until the logging is sent a termination message,
            // or an I/O or send error is encountered
            let mut stdout_finished = false;
            let mut stderr_finished = false;
            loop {
                select! {
                    // Termination request
                    _ = &mut terminate_receiver => {
                        break;
                    }

                    // stdout
                    line = lines_stdout.next_line(), if !stdout_finished => {
                        match line {
                            Ok(line) => match line {
                                None => {
                                    stdout_finished = true;
                                }
                                Some(line) => {
                                    println!("{line}"); // Print to manager stdout
                                    logs_sender
                                        .send(LogMessage::new_from_pipeline(&line))
                                        .await;
                                }
                            },
                            Err(e) => {
                                stdout_finished = true;
                                let line = format!("stdout encountered I/O error: {e}");
                                logs_sender.send(LogMessage::new_from_control_plane(
                                    module_path!(),
                                    "runner",
                                    pipeline_id.to_string(),
                                    pipeline_id.to_string(),
                                    Level::ERROR,
                                    &line,
                                )).await;
                                error!(
                                    pipeline_id = %pipeline_id,
                                    "Logs of pipeline {pipeline_id}: {line}"
                                );
                            }
                        }
                    }

                    // stderr
                    line = lines_stderr.next_line(), if !stderr_finished => {
                        match line {
                            Ok(line) => match line {
                                None => {
                                    stderr_finished = true;
                                }
                                Some(line) => {
                                    eprintln!("{line}"); // Print to manager stderr
                                    logs_sender
                                        .send(LogMessage::new_from_pipeline(&line))
                                        .await;
                                }
                            },
                            Err(e) => {
                                stderr_finished = true;
                                let line = format!("stderr encountered I/O error: {e}");
                                logs_sender.send(LogMessage::new_from_control_plane(
                                    module_path!(),
                                    "runner",
                                    pipeline_id.to_string(),
                                    pipeline_id.to_string(),
                                    Level::ERROR,
                                    &line,
                                )).await;
                                error!(
                                    pipeline_id = %pipeline_id,
                                    "Logs of pipeline {pipeline_id}: {line}"
                                );
                            }
                        }
                    }
                }
            }
        });
        (terminate_sender, join_handle)
    }

    /// Retrieves the binary executable or program info file of the pipeline from the compiler
    /// server and stores it in the local runner working directory for that pipeline
    /// located at `target_file_path`.
    ///
    /// Attempts to retrieve several times. Attempts are only made again if a sending error
    /// occurred, not when a response was returned.
    ///
    /// # Arguments
    ///
    /// * `file_url` - The URL of the file to retrieve.
    /// * `description` - The description of the file to retrieve (e.g. "binary", "program info").
    /// * `target_file_path` - The path to store the file.
    /// * `mode` - File access mode for the created file.
    async fn retrieve_pipeline_file(
        &self,
        file_url: &str,
        description: &str,
        target_file_path: &Path,
        mode: u32,
    ) -> Result<(), ManagerError> {
        // URL validation
        let parsed = url::Url::parse(file_url).map_err(|e| {
            ManagerError::from(RunnerError::RunnerProvisionError {
                error: format!(
                    "pipeline {description} retrieval failed: invalid URL '{file_url}' due to: {e}"
                ),
            })
        })?;

        // Scheme must match either HTTP or HTTPS depending on configuration
        let scheme = parsed.scheme();
        if !self.common_config.enable_https && scheme != "http" {
            return Err(RunnerError::RunnerProvisionError {
                error: format!("pipeline {description} retrieval failed: URL '{file_url}' has scheme '{scheme}' whereas 'http' is expected"),
            }.into());
        }
        if self.common_config.enable_https && parsed.scheme() != "https" {
            return Err(RunnerError::RunnerProvisionError {
                error: format!("pipeline {description} retrieval failed: URL '{file_url}' has scheme '{scheme}' whereas 'https' is expected"),
            }.into());
        }

        // Perform request
        let mut attempt = 1;
        loop {
            match self.client.get(file_url).with_sentry_tracing().send().await {
                Ok(response) => {
                    // Check status code
                    if response.status() != StatusCode::OK {
                        return Err(RunnerError::RunnerProvisionError {
                            error: format!(
                                "pipeline {description} retrieval failed: GET '{file_url}': expected response status code {} but got {}",
                                StatusCode::OK, response.status(),
                            ),
                        }.into());
                    }

                    // Parse response as bytes
                    let body = response.bytes().await.map_err(|_e| {
                        ManagerError::from(RunnerError::RunnerProvisionError {
                            error: format!("pipeline {description} retrieval failed: GET '{file_url}': could not convert response body into bytes")
                        })
                    })?;

                    // Create, open, write and flush file
                    let mut file = fs::File::options()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .read(true)
                        .mode(mode)
                        .open(target_file_path)
                        .await
                        .map_err(|e| {
                            ManagerError::from(RunnerError::RunnerProvisionError {
                                error: format!(
                                    "pipeline {description} retrieval failed: unable to create file '{}': {e}",
                                    target_file_path.display()
                                ),
                            })
                        })?;
                    file.write_all(&body).await.map_err(|e| {
                        ManagerError::from(RunnerError::RunnerProvisionError {
                            error: format!(
                                "pipeline {description} retrieval failed: unable to write file '{}': {e}",
                                target_file_path.display()
                            ),
                        })
                    })?;
                    file.flush().await.map_err(|e| {
                        ManagerError::from(RunnerError::RunnerProvisionError {
                            error: format!(
                                "pipeline {description} retrieval failed: unable to flush file '{}': {e}",
                                target_file_path.display()
                            ),
                        })
                    })?;

                    return Ok(());
                }
                Err(e) => {
                    let error = format!(
                        "pipeline {description} retrieval failed (attempt {attempt} / {BINARY_RETRIEVAL_ATTEMPTS}): GET '{file_url}': unable to get response: {e}, source: {}",
                        source_error(&e)
                    );
                    error!("{error}");
                    if attempt >= BINARY_RETRIEVAL_ATTEMPTS {
                        return Err(RunnerError::RunnerProvisionError { error }.into());
                    }
                }
            }
            sleep(BINARY_RETRIEVAL_RETRY_INTERVAL).await;
            attempt += 1;
        }
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
            Err("The pipeline-manager and this pipeline were previously terminated. Start the pipeline again.".to_string())
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

        // Write to both runner and pipeline logs
        for error in &errors {
            error!(
                "Resources error for pipeline {}: {}",
                self.pipeline_id, error
            );
            self.logs_sender
                .send(LogMessage::new_from_control_plane(
                    module_path!(),
                    "runner",
                    self.pipeline_id.to_string(),
                    self.pipeline_id.to_string(),
                    Level::ERROR,
                    &format!("Resources error: {error}"),
                ))
                .await;
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

    fn new(
        pipeline_id: PipelineId,
        common_config: CommonConfig,
        config: Self::Config,
        client: reqwest::Client,
        logs_sender: LogsSender,
    ) -> Self {
        Self {
            pipeline_id,
            common_config,
            config,
            client,
            process: None,
            logs_sender,
            logs_thread_terminate_sender_and_join_handle: None,
        }
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
        deployment_initial: RuntimeDesiredStatus,
        bootstrap_policy: Option<BootstrapPolicy>,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
        _program_info: &serde_json::Value,
        program_binary_url: &str,
        program_info_url: Option<&str>,
        program_version: Version,
        _suspend_info: Option<serde_json::Value>,
    ) -> Result<(), ManagerError> {
        // (Re-)create pipeline working directory (which will contain storage directory)
        let pipeline_dir = self.config.pipeline_dir(self.pipeline_id);
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

        // If program_info_url is provided, combine information in deployment_config and program info.
        // TODO: This implementation ensures backward compatibility with older pipelines.
        // Going forward, we should be able to pass program info and deployment config files
        // as separate arguments to the pipeline instead of merging them into one JSON file.
        let mut deployment_config = deployment_config.clone();
        if let Some(program_info_url) = program_info_url {
            // Retrieve and store executable in pipeline working directory
            let program_info_file_path = self.config.program_info_file_path(self.pipeline_id);

            self.retrieve_pipeline_file(
                program_info_url,
                "program info",
                &program_info_file_path,
                0o660, // User: rw, Group: rw, Others: /
            )
            .await?;

            // Read and parse the program info file
            let program_info_contents =
                fs::read_to_string(&program_info_file_path)
                    .await
                    .map_err(|e| {
                        ManagerError::from(CommonError::io_error(
                            format!(
                                "read program info file '{}'",
                                program_info_file_path.display()
                            ),
                            e,
                        ))
                    })?;

            let program_info: PipelineConfigProgramInfo =
                serde_json::from_str(&program_info_contents).map_err(|e| {
                    ManagerError::from(RunnerError::RunnerProvisionError {
                        error: format!(
                            "failed to parse program info file '{}': {e}",
                            program_info_file_path.display()
                        ),
                    })
                })?;

            // Merge program info into deployment_config
            deployment_config.inputs = program_info.inputs;
            deployment_config.outputs = program_info.outputs;
            deployment_config.program_ir = program_info.program_ir;
        }

        // Write config as YAML and JSON
        //
        // Newer pipelines will read the JSON, older ones will read the YAML.
        let json_config = serde_json::to_string_pretty(&deployment_config)
            .expect("JSON config serialization failed");
        let yaml_config =
            serde_yaml::to_string(&deployment_config).expect("YAML config serialization failed");
        for (extension, expanded_config) in [("json", json_config), ("yaml", yaml_config)] {
            let config_file_path = self.config.config_file_path(self.pipeline_id, extension);
            fs::write(&config_file_path, &expanded_config)
                .await
                .map_err(|e| {
                    ManagerError::from(CommonError::io_error(
                        format!("write config file '{}'", config_file_path.display()),
                        e,
                    ))
                })?;
        }

        // Delete port file (which will only exist if we are restarting from a
        // checkpoint).
        let _ = remove_file(&self.config.port_file_path(self.pipeline_id)).await;

        // Retrieve and store executable in pipeline working directory
        let binary_file_path = self
            .config
            .binary_file_path(self.pipeline_id, program_version);
        self.retrieve_pipeline_file(
            program_binary_url,
            "binary",
            &binary_file_path,
            0o760, // User: rwx, Group: rw, Others: /
        )
        .await?;

        // Run executable:
        // - Current directory: pipeline working directory
        // - Configuration file: path to config.yaml
        // - Stdout/stderr are piped to follow logs
        let mut command = Command::new(binary_file_path);
        command
            .env(
                "TOKIO_WORKER_THREADS",
                deployment_config
                    .global
                    .io_workers
                    .unwrap_or(deployment_config.global.workers as u64)
                    .to_string(),
            )
            .current_dir(pipeline_dir)
            .arg("--config-file")
            .arg(self.config.config_file_path(self.pipeline_id, "yaml"))
            .arg("--bind-address")
            .arg(&self.common_config.bind_address)
            .arg("--initial")
            .arg(runtime_desired_status_to_string(deployment_initial))
            .arg("--deployment-id")
            .arg(deployment_id.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(bootstrap_policy) = bootstrap_policy {
            command
                .arg("--bootstrap-policy")
                .arg(bootstrap_policy_to_string(bootstrap_policy));
        }

        if let Some((https_tls_cert_path, https_tls_key_path)) = self.common_config.https_config() {
            command
                .arg("--enable-https")
                .arg("--https-tls-cert-path")
                .arg(https_tls_cert_path)
                .arg("--https-tls-key-path")
                .arg(https_tls_key_path);
        }
        let mut process = command
            .spawn()
            .map_err(|e| RunnerError::RunnerProvisionError {
                error: format!("unable to spawn process due to: {e}"),
            })?;

        // Spawn a thread which follows the process stdout and stderr
        let stdout = process
            .stdout
            .take()
            .expect("stdout could not be taken out");
        let stderr = process
            .stderr
            .take()
            .expect("stderr could not be taken out");
        let logs_thread_terminate_sender_and_join_handle =
            Self::start_logs_thread_for_stdout_stderr(
                self.pipeline_id,
                stdout,
                stderr,
                self.logs_sender.clone(),
            );

        // Finished
        self.process = Some(process);
        self.logs_thread_terminate_sender_and_join_handle =
            Some(logs_thread_terminate_sender_and_join_handle);
        Ok(())
    }

    /// Process deployment provisioning is completed when the port file is found and read.
    async fn is_provisioned(&mut self) -> Result<Option<String>, ManagerError> {
        if let Err(error) = self.inner_check().await {
            return Err(ManagerError::from(RunnerError::RunnerCheckError { error }));
        }

        let port_file_path = self.config.port_file_path(self.pipeline_id);
        match fs::read_to_string(port_file_path).await {
            Ok(port) => {
                let parse = port.trim().parse::<u16>();
                match parse {
                    Ok(port) => {
                        // Pipelines run on the same host as the runner
                        let host = &self.common_config.runner_host;
                        Ok(Some(format!("{host}:{port}")))
                    }
                    Err(e) => Err(ManagerError::from(RunnerError::RunnerProvisionError {
                        error: format!("unable to parse port file due to: {e}"),
                    })),
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Checks the process and working directory is healthy.
    /// - Pipeline working directory must exist
    /// - Process status must be checkable and not be exited
    async fn check(&mut self) -> Result<(), ManagerError> {
        if let Err(error) = self.inner_check().await {
            Err(ManagerError::from(RunnerError::RunnerCheckError { error }))
        } else {
            Ok(())
        }
    }

    /// Kills the pipeline process and terminates the thread which follows its stdout and stderr.
    async fn stop(&mut self) -> Result<(), ManagerError> {
        // Kill pipeline process
        if let Some(mut p) = self.process.take() {
            let _ = p.kill().await;
            let _ = p.wait().await;
        }

        // Terminate the thread which follows the process stdout and stderr
        if let Some((terminate_sender, join_handle)) =
            self.logs_thread_terminate_sender_and_join_handle.take()
        {
            let _ = terminate_sender.send(());
            join_handle.abort();
            let _ = join_handle.await;
        }
        Ok(())
    }

    /// Removes the pipeline working directory.
    async fn clear(&mut self) -> Result<(), ManagerError> {
        if self.config.pipeline_dir(self.pipeline_id).exists() {
            match remove_dir_all(self.config.pipeline_dir(self.pipeline_id)).await {
                Ok(_) => (),
                Err(e) => {
                    warn!(
                        "Failed to remove working directory for pipeline {}: {}",
                        self.pipeline_id, e
                    );
                }
            }
        }
        Ok(())
    }
}
