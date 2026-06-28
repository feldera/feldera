//! A local runner that watches for pipeline objects in the API
//! and instantiates them locally as processes.

use crate::common_error::CommonError;
use crate::config::{CommonConfig, LocalRunnerConfig};
use crate::db::types::pipeline::{
    bootstrap_policy_to_string, runtime_desired_status_to_string, PipelineId,
};
use crate::db::types::version::Version;
use crate::error::{source_error, ManagerError};
use crate::pipeline_env::validate_pipeline_env;
use crate::runner::error::RunnerError;
use crate::runner::pipeline_executor::{PipelineExecutor, ProvisionStatus};
use crate::runner::pipeline_logs::{LogMessage, LogsSender};
use async_trait::async_trait;
use feldera_observability::ReqwestTracingExt;
use feldera_types::config::{
    PipelineConfig, PipelineConfigProgramInfo, StorageCacheConfig, StorageConfig,
};
use feldera_types::runtime_status::{BootstrapConfig, RuntimeDesiredStatus};
use reqwest::StatusCode;
use serde_json::json;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::fs::{remove_dir_all, remove_file};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio::{fs, fs::create_dir_all, select, spawn};
use tokio_stream::StreamExt;
use tracing::{error, info, warn, Level};
use uuid::Uuid;

/// How many times to attempt to retrieve the pipeline binary.
const BINARY_RETRIEVAL_ATTEMPTS: u64 = 5;

/// Interval between binary retrieval attempts.
const BINARY_RETRIEVAL_RETRY_INTERVAL: Duration = Duration::from_millis(500);

// The ports below are entirely internal to a multihost deployment (the
// coordinator and pipelines just need to agree on them), so they are chosen in
// a high, uncommon range to avoid colliding with a service that binds the same
// port on the wildcard address (e.g. MinIO on 9000) — such a listener would
// otherwise capture our `127.100.<ordinal>.1:<port>` binds.

/// HTTP port each pipeline host of a multihost deployment binds (on its own
/// loopback IP).  Shared across hosts because their IPs differ.
const MULTIHOST_PIPELINE_HTTP_PORT: u16 = 28080;

/// Inter-host data-exchange port.  Shared across hosts (IPs differ); passed to
/// the coordinator via `--pipeline-exchange-port`.
const MULTIHOST_EXCHANGE_PORT: u16 = 29000;

/// HTTP port the coordinator binds (on its own loopback IP).  This is the
/// endpoint the rest of the manager talks to, exactly like ordinal-0 in
/// Kubernetes.
const MULTIHOST_COORDINATOR_PORT: u16 = 28080;

/// Exit code by which a coordinator or pipeline process requests a coordinated
/// restart (see `coord` `wait_for_coordination` and `pipeline_run.sh`).  The
/// supervisor re-spawns the process rather than treating this as a failure.
const COORDINATED_RESTART_EXIT_CODE: i32 = 55;

/// Maximum number of hosts for a single-container multihost deployment.  The
/// host ordinal occupies the third octet of the loopback address
/// `127.100.<ordinal>.1`; `255` is reserved for the coordinator.
const MAX_MULTIHOST_HOSTS: usize = 254;

/// Second octet of the loopback addresses used by a multihost deployment.
///
/// The members bind addresses of the form `127.<this>.<ordinal>.1`.  This octet
/// is deliberately non-zero (so member HTTP servers never collide with the
/// api-server/compiler/runner, which bind `127.0.0.1`) and fixed (so the small,
/// finite set of addresses can be pre-aliased once on macOS, where — unlike
/// Linux — `127.0.0.0/8` addresses other than `127.0.0.1` are not bound to the
/// loopback interface by default).
///
/// Because the addresses do not depend on the pipeline id, only one multihost
/// pipeline can run on a single host at a time; this is sufficient for local
/// development and matches the single-container target.  The addresses are
/// constant, so they are trivially stable across runner restarts (as
/// [`PipelineExecutor::provision`] requires).
const MULTIHOST_LOOPBACK_OCTET: u8 = 100;

/// Loopback IP that host `ordinal` of a multihost deployment binds.
///
/// The ordinal occupies the third octet (not the last, which would force a
/// `.0` address for ordinal 0) so that each host has a distinct IP and the
/// shared exchange port therefore does not collide.
fn multihost_host_ip(ordinal: usize) -> Ipv4Addr {
    Ipv4Addr::new(127, MULTIHOST_LOOPBACK_OCTET, ordinal as u8, 1)
}

/// Loopback IP the coordinator of a multihost deployment binds.  Uses third
/// octet `255`, distinct from any host ordinal.
fn multihost_coordinator_ip() -> Ipv4Addr {
    Ipv4Addr::new(127, MULTIHOST_LOOPBACK_OCTET, 255, 1)
}

/// `--host-template` for the coordinator: it substitutes `#` with each host
/// ordinal to obtain that host's address (`127.<octet>.<ordinal>.1`).
fn multihost_host_template() -> String {
    format!("127.{MULTIHOST_LOOPBACK_OCTET}.#.1")
}

/// How to spawn one member process (a host pipeline or the coordinator) of a
/// multihost deployment.  Retained so the supervisor can re-spawn the process
/// on a coordinated restart (exit code 55).
struct MemberSpec {
    /// Display name used as a log prefix (e.g. `host-0`, `coordinator`).
    name: String,
    /// Executable to run.
    program: PathBuf,
    /// Working directory (its own per-member subdirectory).
    cwd: PathBuf,
    /// Command-line arguments.
    args: Vec<OsString>,
    /// Extra environment variables.
    envs: Vec<(String, String)>,
}

impl MemberSpec {
    fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command
            .current_dir(&self.cwd)
            .args(&self.args)
            .envs(self.envs.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }
}

/// Records the first fatal error observed across the member processes of a
/// multihost deployment, so `check()`/`is_provisioned()` can surface it.
fn record_multihost_error(health: &Arc<Mutex<Option<String>>>, message: String) {
    let mut health = health.lock().unwrap();
    if health.is_none() {
        error!("{message}");
        *health = Some(message);
    }
}

/// Supervises one member process: follows its stdout/stderr (prefixed with the
/// member name), re-spawns it on a coordinated restart (exit code 55), records
/// a fatal error on any other unexpected exit, and kills it when `cancel` fires.
///
/// This mirrors the supervision loop in `pipeline_run.sh` but for a single
/// container: the Kubernetes StatefulSet that would re-create a crashed pod is
/// replaced by re-spawning here.
async fn supervise_member(
    spec: MemberSpec,
    pipeline_id: PipelineId,
    mut logs_sender: LogsSender,
    health: Arc<Mutex<Option<String>>>,
    mut cancel: watch::Receiver<bool>,
) {
    loop {
        if *cancel.borrow() {
            return;
        }
        let mut child = match spec.command().spawn() {
            Ok(child) => child,
            Err(e) => {
                record_multihost_error(&health, format!("{}: unable to spawn process: {e}", spec.name));
                return;
            }
        };
        let mut stdout = BufReader::new(child.stdout.take().expect("stdout could not be taken")).lines();
        let mut stderr = BufReader::new(child.stderr.take().expect("stderr could not be taken")).lines();
        let mut stdout_done = false;
        let mut stderr_done = false;

        // Run the process to completion, following its output, until it exits
        // or a cancellation is requested.
        let mut cancelled = false;
        let exit = loop {
            select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        cancelled = true;
                        break None;
                    }
                }
                line = stdout.next_line(), if !stdout_done => match line {
                    Ok(Some(line)) => {
                        println!("{} | {line}", spec.name);
                        logs_sender.send(LogMessage::new_from_pipeline(&line)).await;
                    }
                    Ok(None) => stdout_done = true,
                    Err(_) => stdout_done = true,
                },
                line = stderr.next_line(), if !stderr_done => match line {
                    Ok(Some(line)) => {
                        eprintln!("{} | {line}", spec.name);
                        logs_sender.send(LogMessage::new_from_pipeline(&line)).await;
                    }
                    Ok(None) => stderr_done = true,
                    Err(_) => stderr_done = true,
                },
                status = child.wait() => break Some(status),
            }
        };

        if cancelled {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return;
        }

        match exit.expect("exit status is set unless cancelled") {
            Ok(status) => match status.code() {
                Some(0) => {
                    info!("{}: process exited normally", spec.name);
                    return;
                }
                Some(code) if code == COORDINATED_RESTART_EXIT_CODE => {
                    let message = format!("{} requested a coordinated restart (exit {code}); restarting it", spec.name);
                    info!("{message}");
                    logs_sender
                        .send(LogMessage::new_from_control_plane(
                            module_path!(),
                            "runner",
                            String::new(),
                            pipeline_id.to_string(),
                            Level::INFO,
                            &message,
                        ))
                        .await;
                    // Re-spawn from the same spec.
                    continue;
                }
                other => {
                    record_multihost_error(
                        &health,
                        format!("{} exited unexpectedly with {status} (code {other:?})", spec.name),
                    );
                    return;
                }
            },
            Err(e) => {
                record_multihost_error(&health, format!("{}: failed to await process: {e}", spec.name));
                return;
            }
        }
    }
}

/// State of a provisioned multihost deployment: the coordinator endpoint that
/// the rest of the manager talks to, plus handles to stop the member processes.
struct MultihostDeployment {
    /// `host:port` of the coordinator (the deployment location reported upward).
    location: String,
    /// Number of host pipeline processes (excludes the coordinator).
    n_hosts: usize,
    /// Set to `true` to request all member supervisors to kill their processes.
    cancel: watch::Sender<bool>,
    /// First fatal error observed across the member processes, if any.
    health: Arc<Mutex<Option<String>>>,
    /// One supervisor task per member process (hosts + coordinator).
    supervisors: Vec<JoinHandle<()>>,
}

impl MultihostDeployment {
    fn error(&self) -> Option<String> {
        self.health.lock().unwrap().clone()
    }
}

/// A runner handle to run a pipeline using a local process.
/// During provisioning, it spawns a process, retrieves its location (port)
/// and follows the logs of the process stdout and stderr. Upon stop, it kills
/// the spawned process and terminates the logging.
///
/// For multihost pipelines (`runtime_config.hosts > 1`) it instead spawns one
/// pipeline process per host plus a coordinator process, each on its own
/// loopback IP, and reports the coordinator as the deployment location.
///
/// In addition, it attempts to non-blocking kill the spawned process(es) upon
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
    /// Set instead of `process` when a multihost pipeline is provisioned.
    multihost: Option<MultihostDeployment>,
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

        // Request the multihost member supervisors to kill their processes. The
        // supervisor tasks are detached, so they keep running after this handle
        // is dropped and react to the cancellation by killing their children.
        if let Some(multihost) = &self.multihost {
            let _ = multihost.cancel.send(true);
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
                                    String::new(),
                                    pipeline_id.to_string(),
                                    Level::ERROR,
                                    &line,
                                )).await;
                                error!(
                                    pipeline_id = %pipeline_id,
                                    pipeline = "N/A",
                                    "Logs of pipeline: {line}"
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
                                    String::new(),
                                    pipeline_id.to_string(),
                                    Level::ERROR,
                                    &line,
                                )).await;
                                error!(
                                    pipeline_id = %pipeline_id,
                                    pipeline = "N/A",
                                    "Logs of pipeline: {line}"
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

                    // Response body as a stream of bytes
                    let mut body = response.bytes_stream();

                    // Create and open the file
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

                    // Write the file in streaming fashion
                    while let Some(result_bytes) = body.next().await {
                        let bytes = result_bytes.map_err(|e| ManagerError::from(RunnerError::RunnerProvisionError {
                            error: format!("pipeline {description} retrieval failed: GET '{file_url}': could not convert response body chunk into bytes due to: {e}")
                        }))?;
                        file.write_all(&bytes).await.map_err(|e| {
                            ManagerError::from(RunnerError::RunnerProvisionError {
                                error: format!(
                                    "pipeline {description} retrieval failed: unable to write file '{}': {e}",
                                    target_file_path.display()
                                ),
                            })
                        })?;
                    }

                    // Flush the file to be sure everything is written to disk
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
    async fn inner_check(&mut self) -> Result<serde_json::Value, String> {
        // Pipeline process status must be checkable and not have exited
        let process_check = if let Some(p) = &mut self.process {
            match p.try_wait() {
                Ok(status) => {
                    if let Some(status) = status {
                        // If there is an exit status, the process has exited
                        Err(format!("Pipeline process exited prematurely with {status}"))
                    } else {
                        Ok("has not exited".to_string())
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
            Ok("exists".to_string())
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
                pipeline_id = %self.pipeline_id,
                pipeline = "N/A",
                "Resources error: {error}"
            );
            self.logs_sender
                .send(LogMessage::new_from_control_plane(
                    module_path!(),
                    "runner",
                    String::new(),
                    self.pipeline_id.to_string(),
                    Level::ERROR,
                    &format!("Resources error: {error}"),
                ))
                .await;
        }

        // Result
        if errors.is_empty() {
            Ok(json!({
                "process": process_check.unwrap_or("".to_string()),
                "working_dir": working_dir_check.unwrap_or("".to_string()),
            }))
        } else {
            Err(errors.join("\n"))
        }
    }

    /// Writes a member process's config file (`config.json`) into its working
    /// directory, with the storage directory rewritten to live under that
    /// directory so the members sharing this container do not collide.
    async fn write_member_config(
        &self,
        member_dir: &Path,
        base_config: &PipelineConfig,
    ) -> Result<PathBuf, ManagerError> {
        let mut config = base_config.clone();
        if let Some(storage_config) = &mut config.storage_config {
            let storage_dir = member_dir.join("storage");
            create_dir_all(&storage_dir).await.map_err(|e| {
                ManagerError::from(CommonError::io_error(
                    format!(
                        "create member storage directory '{}'",
                        storage_dir.display()
                    ),
                    e,
                ))
            })?;
            storage_config.path = storage_dir.to_string_lossy().into_owned();
        }

        // Both the pipeline (adapters) and the coordinator read `config.json`.
        let config_file_path = self.config.member_config_file_path(member_dir, "json");
        let json_config =
            serde_json::to_string_pretty(&config).expect("JSON config serialization failed");
        fs::write(&config_file_path, &json_config)
            .await
            .map_err(|e| {
                ManagerError::from(CommonError::io_error(
                    format!("write config file '{}'", config_file_path.display()),
                    e,
                ))
            })?;
        Ok(config_file_path)
    }

    /// Provisions a multihost pipeline as multiple local processes: one pipeline
    /// process per host (each `--initial coordination`, on its own loopback IP)
    /// plus one `feldera-coordinator` process. The coordinator is the endpoint
    /// the rest of the manager talks to, mirroring how it talks to ordinal-0:80
    /// in Kubernetes.
    #[allow(clippy::too_many_arguments)]
    async fn provision_multihost(
        &mut self,
        deployment_initial: RuntimeDesiredStatus,
        bootstrap_config: Option<BootstrapConfig>,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
        program_binary_url: &str,
        program_info_url: &str,
        program_version: Version,
    ) -> Result<(), ManagerError> {
        // The coordinator binary must be configured.
        let coordinator_binary = self.config.coordinator_binary.clone().ok_or_else(|| {
            ManagerError::from(RunnerError::RunnerProvisionError {
                error: "multihost pipelines require the local runner to be configured with \
                    --coordinator-binary (path to the feldera-coordinator executable); build it \
                    with `cargo build -p feldera-coordinator`"
                    .to_string(),
            })
        })?;

        // HTTPS for multihost is not yet supported by the local runner (the
        // coordinator dials bare loopback IPs, which would need IP-SAN certs).
        if self.common_config.https_config().is_some() {
            return Err(RunnerError::RunnerProvisionError {
                error: "the local runner does not support multihost pipelines over HTTPS yet; \
                    disable HTTPS or reduce hosts to 1"
                    .to_string(),
            }
            .into());
        }

        // Number of hosts, clamped to [1, workers] to match the coordinator
        // (`Pipelines::new`), so we never spawn more hosts than the coordinator
        // will use.
        let requested_hosts = deployment_config
            .multihost
            .as_ref()
            .map(|m| m.hosts)
            .unwrap_or(1);
        let workers = (deployment_config.global.workers as usize).max(1);
        let n_hosts = requested_hosts.clamp(1, workers);
        if n_hosts > MAX_MULTIHOST_HOSTS {
            return Err(RunnerError::RunnerProvisionError {
                error: format!(
                    "the local runner supports at most {MAX_MULTIHOST_HOSTS} hosts for a \
                    multihost pipeline (requested {requested_hosts})"
                ),
            }
            .into());
        }
        if requested_hosts > workers {
            warn!(
                pipeline_id = %self.pipeline_id,
                "multihost pipeline requested {requested_hosts} hosts but only has {workers} workers; \
                 using {n_hosts} hosts"
            );
        }

        // (Re-)create the pipeline working directory.
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

        // Retrieve the program info and merge it into the deployment config
        // (shared by every member process).
        let program_info_file_path = self.config.program_info_file_path(self.pipeline_id);
        self.retrieve_pipeline_file(program_info_url, "program info", &program_info_file_path, 0o660)
            .await?;
        let program_info_contents = fs::read_to_string(&program_info_file_path)
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
        let program_info: PipelineConfigProgramInfo = serde_json::from_str(&program_info_contents)
            .map_err(|e| {
                ManagerError::from(RunnerError::RunnerProvisionError {
                    error: format!(
                        "failed to parse program info file '{}': {e}",
                        program_info_file_path.display()
                    ),
                })
            })?;

        let mut base_config = deployment_config.clone();
        base_config.inputs = program_info.inputs;
        base_config.outputs = program_info.outputs;
        base_config.program_ir = program_info.program_ir;

        // Retrieve the pipeline binary once; every host process runs the same
        // executable.
        let binary_file_path = self
            .config
            .binary_file_path(self.pipeline_id, program_version);
        self.retrieve_pipeline_file(program_binary_url, "binary", &binary_file_path, 0o760)
            .await?;

        let host_template = multihost_host_template();
        let tokio_worker_threads = base_config
            .global
            .io_workers
            .unwrap_or(base_config.global.workers as u64)
            .to_string();
        let pipeline_env: Vec<(String, String)> = base_config
            .global
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Build the spec for each member process.
        let mut specs: Vec<MemberSpec> = Vec::with_capacity(n_hosts + 1);

        // Host pipeline processes.
        for ordinal in 0..n_hosts {
            let host_dir = self.config.host_dir(self.pipeline_id, ordinal);
            create_dir_all(&host_dir).await.map_err(|e| {
                ManagerError::from(CommonError::io_error(
                    format!("create host working directory '{}'", host_dir.display()),
                    e,
                ))
            })?;
            let config_file_path = self.write_member_config(&host_dir, &base_config).await?;
            let bind_ip = multihost_host_ip(ordinal);

            let mut args: Vec<OsString> = vec![
                "--config-file".into(),
                config_file_path.into_os_string(),
                "--bind-address".into(),
                bind_ip.to_string().into(),
                "--default-port".into(),
                MULTIHOST_PIPELINE_HTTP_PORT.to_string().into(),
                "--host-id".into(),
                ordinal.to_string().into(),
                "--initial".into(),
                runtime_desired_status_to_string(RuntimeDesiredStatus::Coordination).into(),
                "--deployment-id".into(),
                deployment_id.to_string().into(),
            ];
            if let Some(bootstrap_config) = bootstrap_config {
                args.push("--bootstrap-policy".into());
                args.push(bootstrap_policy_to_string(bootstrap_config.active_bootstrap_policy()).into());
                if bootstrap_config.silent_bootstrap {
                    args.push("--silent-bootstrap".into());
                }
            }

            let mut envs = pipeline_env.clone();
            envs.push(("TOKIO_WORKER_THREADS".to_string(), tokio_worker_threads.clone()));

            specs.push(MemberSpec {
                name: format!("host-{ordinal}"),
                program: binary_file_path.clone(),
                cwd: host_dir,
                args,
                envs,
            });
        }

        // Coordinator process.
        {
            let coordinator_dir = self.config.coordinator_dir(self.pipeline_id);
            create_dir_all(&coordinator_dir).await.map_err(|e| {
                ManagerError::from(CommonError::io_error(
                    format!(
                        "create coordinator working directory '{}'",
                        coordinator_dir.display()
                    ),
                    e,
                ))
            })?;
            let config_file_path = self
                .write_member_config(&coordinator_dir, &base_config)
                .await?;
            let coordinator_ip = multihost_coordinator_ip();

            let mut args: Vec<OsString> = vec![
                "--config-file".into(),
                config_file_path.into_os_string(),
                "--bind-address".into(),
                coordinator_ip.to_string().into(),
                "--default-port".into(),
                MULTIHOST_COORDINATOR_PORT.to_string().into(),
                "--host-template".into(),
                host_template.into(),
                "--pipeline-http-port".into(),
                MULTIHOST_PIPELINE_HTTP_PORT.to_string().into(),
                "--pipeline-exchange-port".into(),
                MULTIHOST_EXCHANGE_PORT.to_string().into(),
                "--initial".into(),
                runtime_desired_status_to_string(deployment_initial).into(),
                "--deployment-id".into(),
                deployment_id.to_string().into(),
            ];
            if let Some(bootstrap_config) = bootstrap_config {
                args.push("--bootstrap-policy".into());
                args.push(bootstrap_policy_to_string(bootstrap_config.active_bootstrap_policy()).into());
            }

            specs.push(MemberSpec {
                name: "coordinator".to_string(),
                program: PathBuf::from(&coordinator_binary),
                cwd: coordinator_dir,
                args,
                envs: pipeline_env.clone(),
            });
        }

        // Spawn a supervisor task for each member process.
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let health = Arc::new(Mutex::new(None));
        let mut supervisors = Vec::with_capacity(specs.len());
        for spec in specs {
            supervisors.push(spawn(supervise_member(
                spec,
                self.pipeline_id,
                self.logs_sender.clone(),
                health.clone(),
                cancel_rx.clone(),
            )));
        }

        let location = format!("{}:{}", multihost_coordinator_ip(), MULTIHOST_COORDINATOR_PORT);
        self.multihost = Some(MultihostDeployment {
            location,
            n_hosts,
            cancel: cancel_tx,
            health,
            supervisors,
        });
        Ok(())
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
            multihost: None,
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

    /// The local runner should always be able to manage local processes and the pipeline working
    /// directory irrespective of configuration, hence no checks are performed here.
    async fn can_provision(
        &self,
        _deployment_config: &PipelineConfig,
        _runtime_config: &serde_json::Value,
    ) -> Result<(), ManagerError> {
        Ok(())
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
        bootstrap_config: Option<BootstrapConfig>,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
        _program_info: &serde_json::Value,
        program_binary_url: &str,
        program_info_url: &str,
        program_version: Version,
        _runtime_config: &serde_json::Value,
    ) -> Result<(), ManagerError> {
        if let Err(e) = validate_pipeline_env(&deployment_config.global.env) {
            return Err(RunnerError::RunnerProvisionError { error: e }.into());
        }

        // Multihost pipelines: spawn one pipeline process per host plus a
        // coordinator process, each on its own loopback IP. See
        // `provision_multihost`.
        if deployment_config.multihost.is_some() {
            return self
                .provision_multihost(
                    deployment_initial,
                    bootstrap_config,
                    deployment_id,
                    deployment_config,
                    program_binary_url,
                    program_info_url,
                    program_version,
                )
                .await;
        }

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

        // Retrieve and store program info in pipeline working directory
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

        let program_info: PipelineConfigProgramInfo = serde_json::from_str(&program_info_contents)
            .map_err(|e| {
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

        let mut retries = 0..3;
        let mut process = loop {
            // Run executable:
            // - Current directory: pipeline working directory
            // - Configuration file: path to config.yaml
            // - Stdout/stderr are piped to follow logs
            let mut command = Command::new(&binary_file_path);
            command
                .env(
                    "TOKIO_WORKER_THREADS",
                    deployment_config
                        .global
                        .io_workers
                        .unwrap_or(deployment_config.global.workers as u64)
                        .to_string(),
                )
                .envs(
                    deployment_config
                        .global
                        .env
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_str())),
                )
                .current_dir(&pipeline_dir)
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

            if let Some(bootstrap_config) = bootstrap_config {
                command
                    .arg("--bootstrap-policy")
                    .arg(bootstrap_policy_to_string(
                        bootstrap_config.active_bootstrap_policy(),
                    ));
                if bootstrap_config.silent_bootstrap {
                    command.arg("--silent-bootstrap");
                }
            }

            if let Some((https_tls_cert_path, https_tls_key_path)) =
                self.common_config.https_config()
            {
                command
                    .arg("--enable-https")
                    .arg("--https-tls-cert-path")
                    .arg(https_tls_cert_path)
                    .arg("--https-tls-key-path")
                    .arg(https_tls_key_path);
            }
            match command.spawn() {
                Ok(process) => break process,
                Err(e) if e.kind() == ErrorKind::ExecutableFileBusy && retries.next().is_some() => {
                    // This race appears very occasionally in testing.  It might
                    // be that tokio in some cases keeps an `Arc<File>` for the
                    // underlying file in some background task after we've
                    // finished retrieving the binary.
                    warn!("pipeline executable is busy, retrying in 1 second...");
                    sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    return Err(RunnerError::RunnerProvisionError {
                        error: format!("unable to spawn process due to: {e}"),
                    }
                    .into())
                }
            }
        };

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
    async fn is_provisioned(
        &mut self,
        _runtime_config: &serde_json::Value,
    ) -> Result<ProvisionStatus, ManagerError> {
        // Multihost: provisioning is complete once the coordinator's HTTP
        // endpoint accepts connections; the deployment location is the
        // coordinator. A fatal error from any member process fails provisioning.
        if let Some(multihost) = &self.multihost {
            let details = json!({
                "hosts": multihost.n_hosts,
                "coordinator": multihost.location,
            });
            if let Some(error) = multihost.error() {
                return Err(RunnerError::RunnerProvisionError { error }.into());
            }
            let reachable = timeout(
                Duration::from_secs(1),
                TcpStream::connect(multihost.location.clone()),
            )
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false);
            return Ok(if reachable {
                ProvisionStatus::Provisioned {
                    location: multihost.location.clone(),
                    details,
                }
            } else {
                ProvisionStatus::Ongoing { details }
            });
        }

        let details = self
            .inner_check()
            .await
            .map_err(|error| ManagerError::from(RunnerError::RunnerCheckError { error }))?;

        let port_file_path = self.config.port_file_path(self.pipeline_id);
        match fs::read_to_string(port_file_path).await {
            Ok(port) => {
                let parse = port.trim().parse::<u16>();
                match parse {
                    Ok(port) => {
                        // Pipelines run on the same host as the runner
                        let host = &self.common_config.runner_host;
                        Ok(ProvisionStatus::Provisioned {
                            location: format!("{host}:{port}"),
                            details,
                        })
                    }
                    Err(e) => Err(ManagerError::from(RunnerError::RunnerProvisionError {
                        error: format!("unable to parse port file due to: {e}"),
                    })),
                }
            }
            Err(_) => Ok(ProvisionStatus::Ongoing { details }),
        }
    }

    /// Checks the process and working directory is healthy.
    /// - Pipeline working directory must exist
    /// - Process status must be checkable and not be exited
    async fn check(
        &mut self,
        _runtime_config: &serde_json::Value,
    ) -> Result<serde_json::Value, ManagerError> {
        // Multihost: healthy as long as no member process has fatally exited.
        // (Coordinated restarts, exit code 55, are handled transparently by the
        // member supervisors and do not surface here.)
        if let Some(multihost) = &self.multihost {
            if let Some(error) = multihost.error() {
                error!(
                    pipeline_id = %self.pipeline_id,
                    pipeline = "N/A",
                    "Resources error: {error}"
                );
                self.logs_sender
                    .send(LogMessage::new_from_control_plane(
                        module_path!(),
                        "runner",
                        String::new(),
                        self.pipeline_id.to_string(),
                        Level::ERROR,
                        &format!("Resources error: {error}"),
                    ))
                    .await;
                return Err(RunnerError::RunnerCheckError { error }.into());
            }
            return Ok(json!({
                "hosts": multihost.n_hosts,
                "coordinator": multihost.location,
            }));
        }

        self.inner_check()
            .await
            .map_err(|error| ManagerError::from(RunnerError::RunnerCheckError { error }))
    }

    /// Kills the pipeline process and terminates the thread which follows its stdout and stderr.
    async fn stop(&mut self, _runtime_config: &serde_json::Value) -> Result<(), ManagerError> {
        // Multihost: signal every member supervisor to kill its process and
        // wait for them to finish, so this blocks until all processes are gone.
        if let Some(multihost) = self.multihost.take() {
            let _ = multihost.cancel.send(true);
            for supervisor in multihost.supervisors {
                let _ = supervisor.await;
            }
            return Ok(());
        }

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
    async fn clear(&mut self, _runtime_config: &serde_json::Value) -> Result<(), ManagerError> {
        if self.config.pipeline_dir(self.pipeline_id).exists() {
            match remove_dir_all(self.config.pipeline_dir(self.pipeline_id)).await {
                Ok(_) => (),
                Err(e) => {
                    warn!(
                        pipeline_id = %self.pipeline_id,
                        pipeline = "N/A",
                        "Failed to remove working directory: {e}"
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod multihost_tests {
    use super::{
        multihost_coordinator_ip, multihost_host_ip, multihost_host_template,
        MAX_MULTIHOST_HOSTS, MULTIHOST_LOOPBACK_OCTET,
    };
    use std::net::Ipv4Addr;

    /// The coordinator resolves each host's address by substituting `#` in the
    /// host template with the ordinal (this is exactly what `coord`'s
    /// `Args::hostname` does). That resolved address MUST equal the address the
    /// host process actually binds, otherwise the coordinator would dial the
    /// wrong IP and the data-exchange addresses would be inconsistent.
    #[test]
    fn host_template_resolves_to_host_ip() {
        let template = multihost_host_template();
        for ordinal in 0..MAX_MULTIHOST_HOSTS {
            let resolved = template.replace('#', &ordinal.to_string());
            let parsed: Ipv4Addr = resolved
                .parse()
                .unwrap_or_else(|_| panic!("template resolved to invalid IP: {resolved}"));
            assert_eq!(parsed, multihost_host_ip(ordinal), "ordinal {ordinal}");
        }
    }

    /// Every member (hosts + coordinator) must bind a distinct address so that
    /// the shared HTTP and exchange ports do not collide.
    #[test]
    fn member_addresses_are_distinct() {
        let mut seen = std::collections::HashSet::new();
        for ordinal in 0..MAX_MULTIHOST_HOSTS {
            assert!(
                seen.insert(multihost_host_ip(ordinal)),
                "duplicate host address for ordinal {ordinal}"
            );
        }
        assert!(
            seen.insert(multihost_coordinator_ip()),
            "coordinator address collides with a host address"
        );
    }

    /// No member may bind `127.0.0.1`: that is where the api-server, compiler,
    /// and runner bind, and a host on `127.0.0.1:8080` would collide with the
    /// api-server's default port.
    #[test]
    fn members_avoid_localhost() {
        let localhost = Ipv4Addr::new(127, 0, 0, 1);
        assert_ne!(multihost_coordinator_ip(), localhost);
        for ordinal in 0..MAX_MULTIHOST_HOSTS {
            let ip = multihost_host_ip(ordinal);
            assert_ne!(ip, localhost, "ordinal {ordinal} binds localhost");
            // All members share the fixed second octet, which is non-zero.
            assert_eq!(ip.octets()[1], MULTIHOST_LOOPBACK_OCTET);
            assert_ne!(MULTIHOST_LOOPBACK_OCTET, 0);
            // Last octet is never 0 (a `.0` host address).
            assert_ne!(ip.octets()[3], 0);
        }
    }
}
