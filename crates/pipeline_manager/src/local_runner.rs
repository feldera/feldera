/// A local runner that watches for pipeline objects in the API
/// and instantiates them locally as processes.
use crate::db_notifier::{DbNotification, Operation};
use crate::runner::RunnerApi;
use crate::{
    api::ManagerError,
    auth::TenantId,
    config::LocalRunnerConfig,
    db::{
        storage::Storage, DBError, PipelineId, PipelineRevision, PipelineRuntimeState,
        PipelineStatus, ProjectDB,
    },
    runner::RunnerError,
};
use actix_web::http::{Method, StatusCode};
use chrono::{DateTime, Utc};
use dbsp_adapters::ErrorResponse;
use log::{error, info, trace};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::{
    collections::BTreeMap,
    process::Stdio,
    process::{Child, Command},
    sync::Arc,
};
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all},
    spawn,
    sync::Mutex,
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

/// A handle to the pipeline process that kills the pipeline
/// on `drop`.
struct PipelineHandle {
    pipeline_process: Child,
}

impl Drop for PipelineHandle {
    fn drop(&mut self) {
        let _ = self.pipeline_process.kill();
    }
}

/// Pipeline automaton monitors the runtime state of a single pipeline
/// and continually reconciles desired and actual state.
///
/// The automaton runs as a separate tokio task.
struct PipelineAutomaton {
    pipeline_id: PipelineId,
    tenant_id: TenantId,
    pipeline_process: Option<PipelineHandle>,
    config: Arc<LocalRunnerConfig>,
    db: Arc<Mutex<ProjectDB>>,
    notifier: Arc<Notify>,
}

impl PipelineAutomaton {
    /// The frequency of polling the pipeline during normal operation
    /// when we don't normally expect its state to change.
    const DEFAULT_PIPELINE_POLL_PERIOD: Duration = Duration::from_millis(10_000);

    /// Max time to wait for the pipeline process to initialize its
    /// HTTP server.
    const PROVISIONING_TIMEOUT: Duration = Duration::from_millis(10_000);

    /// How often to check for the pipeline port file during the
    /// provisioning phase.
    const PROVISIONING_POLL_PERIOD: Duration = Duration::from_millis(300);

    /// Max time to wait for the pipeline to initialize all connectors.
    const INITIALIZATION_TIMEOUT: Duration = Duration::from_millis(20_000);

    /// How often to poll for the pipeline initialization status.
    const INITIALIZATION_POLL_PERIOD: Duration = Duration::from_millis(300);

    /// Max time to wait for the pipeline process to exit.
    // TODO: It seems that Actix often takes a while to shutdown.  This
    // is something to investigate.
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10_000);

    /// How often to poll for the pipeline process to exit.
    const SHUTDOWN_POLL_PERIOD: Duration = Duration::from_millis(300);

    fn new(
        pipeline_id: PipelineId,
        tenant_id: TenantId,
        config: &Arc<LocalRunnerConfig>,
        db: Arc<Mutex<ProjectDB>>,
        notifier: Arc<Notify>,
    ) -> Self {
        Self {
            pipeline_id,
            tenant_id,
            pipeline_process: None,
            config: config.clone(),
            db,
            notifier,
        }
    }

    /// Runs until the pipeline is deleted or an unexpected error occurs.
    async fn run(self) -> Result<(), ManagerError> {
        let pipeline_id = self.pipeline_id;

        self.do_run().await.map_err(|e| {
            error!(
                "Pipeline automaton '{}' terminated with error: '{e}'",
                pipeline_id
            );
            e
        })
    }

    async fn do_run(mut self) -> Result<(), ManagerError> {
        let mut poll_timeout = Self::DEFAULT_PIPELINE_POLL_PERIOD;

        loop {
            // Wait until the timeout expires or we get notified that
            // the desired state of the pipelime has changed.
            let _ = timeout(poll_timeout, self.notifier.notified()).await;
            poll_timeout = Self::DEFAULT_PIPELINE_POLL_PERIOD;

            // TODO: use transactional API when ready to avoid races with a parallel
            // `/deploy /shutdown` sequence.

            // txn = db.transaction();
            let db = self.db.lock().await;
            let result = db
                .get_pipeline_runtime_state(self.tenant_id, self.pipeline_id)
                .await;
            if let Err(e) = result {
                match e {
                    DBError::UnknownPipeline { pipeline_id } => {
                        // Pipeline deletions should not lead to errors in the logs.
                        info!("Pipeline {pipeline_id} does not exist. Shutting down pipeline automaton.");
                        return Ok(());
                    }
                    _ => return Err(e.into()),
                }
            }
            let mut pipeline = result.unwrap();

            // Handle deployment request.
            if pipeline.current_status == PipelineStatus::Shutdown
                && pipeline.desired_status != PipelineStatus::Shutdown
            {
                self.update_pipeline_status(&mut pipeline, PipelineStatus::Provisioning, None)
                    .await;
                let revision = db
                    .get_last_committed_pipeline_revision(self.tenant_id, self.pipeline_id)
                    .await?;
                db.update_pipeline_runtime_state(self.tenant_id, self.pipeline_id, &pipeline)
                    .await?;
                // txn.commit();
                drop(db);

                match self.start(revision).await {
                    Ok(child) => {
                        self.pipeline_process = Some(child);
                    }
                    Err(e) => {
                        self.force_kill_pipeline(&mut pipeline, Some(e)).await?;
                    }
                }
                poll_timeout = Self::PROVISIONING_POLL_PERIOD;
                continue;
            } else {
                // txn.commit();
                drop(db);
            }

            match (pipeline.current_status, pipeline.desired_status) {
                // We're waiting for the pipeline's HTTP server to come online.
                // Poll its port file.  On success, go to `Initializing` state.
                (PipelineStatus::Provisioning, PipelineStatus::Running)
                | (PipelineStatus::Provisioning, PipelineStatus::Paused) => {
                    match self.read_pipeline_port_file().await {
                        Ok(Some(port)) => {
                            self.update_pipeline_status(
                                &mut pipeline,
                                PipelineStatus::Initializing,
                                None,
                            )
                            .await;
                            pipeline.set_location(port.to_string());
                            pipeline.set_created();
                            self.update_pipeline_runtime_state(&pipeline).await?;
                            poll_timeout = Self::INITIALIZATION_POLL_PERIOD;
                        }
                        Ok(None) => {
                            if Self::timeout_expired(
                                pipeline.status_since,
                                Self::PROVISIONING_TIMEOUT,
                            ) {
                                self.force_kill_pipeline(
                                    &mut pipeline,
                                    Some(RunnerError::PipelineProvisioningTimeout {
                                        pipeline_id: self.pipeline_id,
                                        timeout: Self::PROVISIONING_TIMEOUT,
                                    }),
                                )
                                .await?;
                            } else {
                                poll_timeout = Self::PROVISIONING_POLL_PERIOD;
                            }
                        }
                        Err(e) => {
                            self.force_kill_pipeline(&mut pipeline, Some(e)).await?;
                        }
                    }
                }
                // User cancels the pipeline while it's still provisioning.
                (PipelineStatus::Provisioning, PipelineStatus::Shutdown) => {
                    self.force_kill_pipeline(&mut pipeline, <Option<RunnerError>>::None)
                        .await?;
                }
                // We're waiting for the pipeline to initialize.
                // Poll the pipeline's status.  Kill the pipeline on timeout or error.
                // On success, go to the `PAUSED` state.
                (PipelineStatus::Initializing, PipelineStatus::Running)
                | (PipelineStatus::Initializing, PipelineStatus::Paused) => {
                    match pipeline_http_request_json_response(
                        self.pipeline_id,
                        Method::GET,
                        "stats",
                        &pipeline.location,
                    )
                    .await
                    {
                        Err(e) => {
                            self.force_kill_pipeline(&mut pipeline, Some(e)).await?;
                        }
                        Ok((status, body)) => {
                            if status.is_success() {
                                self.update_pipeline_status(
                                    &mut pipeline,
                                    PipelineStatus::Paused,
                                    None,
                                )
                                .await;
                                self.update_pipeline_runtime_state(&pipeline).await?;
                            } else if status == StatusCode::SERVICE_UNAVAILABLE {
                                if Self::timeout_expired(
                                    pipeline.status_since,
                                    Self::INITIALIZATION_TIMEOUT,
                                ) {
                                    self.force_kill_pipeline(
                                        &mut pipeline,
                                        Some(RunnerError::PipelineInitializationTimeout {
                                            pipeline_id: self.pipeline_id,
                                            timeout: Self::INITIALIZATION_TIMEOUT,
                                        }),
                                    )
                                    .await?;
                                } else {
                                    poll_timeout = Self::INITIALIZATION_POLL_PERIOD;
                                }
                            } else {
                                self.force_kill_pipeline_on_error(&mut pipeline, status, &body)
                                    .await?;
                            }
                        }
                    }
                }
                // User cancels the pipeline while it is still initalizing.
                (PipelineStatus::Initializing, PipelineStatus::Shutdown) => {
                    self.force_kill_pipeline(&mut pipeline, <Option<RunnerError>>::None)
                        .await?;
                }
                // Unpause the pipeline.
                (PipelineStatus::Paused, PipelineStatus::Running) => {
                    match pipeline_http_request_json_response(
                        self.pipeline_id,
                        Method::GET,
                        "start",
                        &pipeline.location,
                    )
                    .await
                    {
                        Err(e) => {
                            self.force_kill_pipeline(&mut pipeline, Some(e)).await?;
                        }
                        Ok((status, body)) => {
                            if status.is_success() {
                                self.update_pipeline_status(
                                    &mut pipeline,
                                    PipelineStatus::Running,
                                    None,
                                )
                                .await;
                                self.update_pipeline_runtime_state(&pipeline).await?;
                            } else {
                                self.force_kill_pipeline_on_error(&mut pipeline, status, &body)
                                    .await?;
                            }
                        }
                    }
                }
                // Pause the pipeline.
                (PipelineStatus::Running, PipelineStatus::Paused) => {
                    match pipeline_http_request_json_response(
                        self.pipeline_id,
                        Method::GET,
                        "pause",
                        &pipeline.location,
                    )
                    .await
                    {
                        Err(e) => {
                            self.force_kill_pipeline(&mut pipeline, Some(e)).await?;
                        }
                        Ok((status, body)) => {
                            if status.is_success() {
                                self.update_pipeline_status(
                                    &mut pipeline,
                                    PipelineStatus::Paused,
                                    None,
                                )
                                .await;
                                self.update_pipeline_runtime_state(&pipeline).await?;
                            } else {
                                self.force_kill_pipeline_on_error(&mut pipeline, status, &body)
                                    .await?;
                            }
                        }
                    }
                }
                // Shutdown request.  Forward to the pipeline; on success go to the `ShuttingDown`
                // state.
                (PipelineStatus::Running, PipelineStatus::Shutdown)
                | (PipelineStatus::Paused, PipelineStatus::Shutdown) => {
                    match pipeline_http_request_json_response(
                        self.pipeline_id,
                        Method::GET,
                        "shutdown",
                        &pipeline.location,
                    )
                    .await
                    {
                        Err(e) => {
                            self.force_kill_pipeline(&mut pipeline, Some(e)).await?;
                        }
                        Ok((status, body)) => {
                            if status.is_success() {
                                self.update_pipeline_status(
                                    &mut pipeline,
                                    PipelineStatus::ShuttingDown,
                                    None,
                                )
                                .await;
                                self.update_pipeline_runtime_state(&pipeline).await?;
                                poll_timeout = Self::SHUTDOWN_POLL_PERIOD;
                            } else {
                                self.force_kill_pipeline_on_error(&mut pipeline, status, &body)
                                    .await?;
                            }
                        }
                    }
                }
                // Graceful shutdown in progress.  Wait for the pipeline process to self-terminate.
                // Force-kill the pipeline after a timeout.
                (PipelineStatus::ShuttingDown, _) => {
                    if self
                        .pipeline_process
                        .as_mut()
                        .map(|p| p.pipeline_process.try_wait().is_ok())
                        .unwrap_or(true)
                    {
                        self.pipeline_process = None;
                        self.update_pipeline_status(&mut pipeline, PipelineStatus::Shutdown, None)
                            .await;
                        self.update_pipeline_runtime_state(&pipeline).await?;
                    } else if Self::timeout_expired(pipeline.status_since, Self::SHUTDOWN_TIMEOUT) {
                        self.force_kill_pipeline(
                            &mut pipeline,
                            Some(RunnerError::PipelineShutdownTimeout {
                                pipeline_id: self.pipeline_id,
                                timeout: Self::SHUTDOWN_TIMEOUT,
                            }),
                        )
                        .await?;
                    } else {
                        poll_timeout = Self::SHUTDOWN_POLL_PERIOD;
                    }
                }
                // Steady-state operation.  Periodically poll the pipeline.
                (PipelineStatus::Running, _) | (PipelineStatus::Paused, _) => {
                    match pipeline_http_request_json_response(
                        self.pipeline_id,
                        Method::GET,
                        "stats",
                        &pipeline.location,
                    )
                    .await
                    {
                        Err(e) => {
                            // Cannot reach the pipeline.
                            self.force_kill_pipeline(&mut pipeline, Some(e)).await?;
                        }
                        Ok((status, body)) => {
                            if !status.is_success() {
                                // Pipeline responds with an error, meaning that the pipeline
                                // HTTP server is still running, but the pipeline itself failed --
                                // save it out of its misery.
                                self.force_kill_pipeline_on_error(&mut pipeline, status, &body)
                                    .await?;
                            } else {
                                let global_metrics = if let Some(metrics) =
                                    body.get("global_metrics")
                                {
                                    metrics
                                } else {
                                    Err(RunnerError::HttpForwardError {
                                        pipeline_id: self.pipeline_id,
                                        error: format!("Pipeline status descriptor doesn't contain 'global_metrics' field: '{body}'")
                                    })?
                                };
                                let state = if let Some(state) = global_metrics.get("state") {
                                    state
                                } else {
                                    Err(RunnerError::HttpForwardError {
                                        pipeline_id: self.pipeline_id,
                                        error: format!("Pipeline status descriptor doesn't contain 'global_metrics.state' field: '{body}'")
                                    })?
                                };
                                let state = if let Some(state) = state.as_str() {
                                    state
                                } else {
                                    Err(RunnerError::HttpForwardError {
                                        pipeline_id: self.pipeline_id,
                                        error: format!("Pipeline status descriptor contains invalid 'global_metrics.state' field: '{body}'")
                                    })?
                                };

                                if state == "Paused"
                                    && pipeline.current_status != PipelineStatus::Paused
                                {
                                    self.update_pipeline_status(
                                        &mut pipeline,
                                        PipelineStatus::Paused,
                                        None,
                                    )
                                    .await;
                                    self.update_pipeline_runtime_state(&pipeline).await?;
                                } else if state == "Running"
                                    && pipeline.current_status != PipelineStatus::Running
                                {
                                    self.update_pipeline_status(
                                        &mut pipeline,
                                        PipelineStatus::Running,
                                        None,
                                    )
                                    .await;
                                    self.update_pipeline_runtime_state(&pipeline).await?;
                                } else if state != "Paused" && state != "Running" {
                                    self.force_kill_pipeline(&mut pipeline, Some(RunnerError::HttpForwardError {
                                        pipeline_id: self.pipeline_id,
                                        error: format!("Pipeline reported unexpected status '{state}', expected 'Paused' or 'Running'")
                                    })).await?;
                                }
                            }
                        }
                    }
                }
                // User acknowledges pipeline failure by invoking the `/shutdown` endpoint.
                // Move to the `Shutdown` state so that the pipeline can be started again.
                (PipelineStatus::Failed, PipelineStatus::Shutdown) => {
                    let error = pipeline.error.clone();
                    self.update_pipeline_status(&mut pipeline, PipelineStatus::Shutdown, error)
                        .await;
                    self.update_pipeline_runtime_state(&pipeline).await?;
                }
                (PipelineStatus::Failed | PipelineStatus::Shutdown, _) => {}
                _ => {
                    error!(
                        "Unexpected current/desired pipeline status combination {:?}/{:?}",
                        pipeline.current_status, pipeline.desired_status
                    )
                }
            }
        }
    }

    async fn update_pipeline_status(
        &self,
        pipeline: &mut PipelineRuntimeState,
        status: PipelineStatus,
        error: Option<ErrorResponse>,
    ) {
        pipeline.set_current_status(status, error.clone());
        if status == PipelineStatus::Shutdown {
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
        }
    }

    async fn update_pipeline_runtime_state(
        &self,
        state: &PipelineRuntimeState,
    ) -> Result<(), DBError> {
        self.db
            .lock()
            .await
            .update_pipeline_runtime_state(self.tenant_id, self.pipeline_id, state)
            .await
    }

    // We store timestamps in the DB and retrieve them as Utc times;
    // hence we cannot use the normal `Instant::elapsed` API for timeouts.
    fn timeout_expired(since: DateTime<Utc>, timeout: Duration) -> bool {
        Utc::now().timestamp_millis() - since.timestamp_millis() > timeout.as_millis() as i64
    }

    /// Force-kill the pipeline process, set its error description.
    ///
    /// If the desired state of the pipeline is `Paused` or `Running`,
    /// places pipeline in the `Failed` state to avoid instant automatic
    /// restart.  Otherwise (the desired state was `Shutdown`), set the
    /// state of the pipeline to `Shutdown`.
    async fn force_kill_pipeline<E>(
        &mut self,
        pipeline: &mut PipelineRuntimeState,
        error: Option<E>,
    ) -> Result<(), DBError>
    where
        ErrorResponse: for<'a> From<&'a E>,
    {
        self.pipeline_process = None;

        if pipeline.desired_status == PipelineStatus::Shutdown {
            self.update_pipeline_status(
                pipeline,
                PipelineStatus::Shutdown,
                error.map(|e| ErrorResponse::from(&e)),
            )
            .await;
        } else {
            self.update_pipeline_status(
                pipeline,
                PipelineStatus::Failed,
                error.map(|e| ErrorResponse::from(&e)),
            )
            .await;
        }
        self.update_pipeline_runtime_state(pipeline).await
    }

    /// Same as `force_kill_pipeline`, but deserializes `ErrorResponse` from
    /// a JSON error returned by the pipeline.
    async fn force_kill_pipeline_on_error(
        &mut self,
        pipeline: &mut PipelineRuntimeState,
        status: StatusCode,
        error: &JsonValue,
    ) -> Result<(), DBError> {
        self.pipeline_process = None;
        let error = Self::error_response_from_json(self.pipeline_id, status, error);

        if pipeline.desired_status == PipelineStatus::Shutdown {
            self.update_pipeline_status(pipeline, PipelineStatus::Shutdown, Some(error))
                .await;
        } else {
            self.update_pipeline_status(pipeline, PipelineStatus::Failed, Some(error))
                .await;
        }
        self.update_pipeline_runtime_state(pipeline).await
    }

    async fn read_pipeline_port_file(&self) -> Result<Option<u16>, ManagerError> {
        let port_file_path = self.config.port_file_path(self.pipeline_id);

        match fs::read_to_string(port_file_path).await {
            Ok(port) => {
                let parse = port.trim().parse::<u16>();
                match parse {
                    Ok(port) => Ok(Some(port)),
                    Err(e) => Err(ManagerError::from(RunnerError::PortFileParseError {
                        pipeline_id: self.pipeline_id,
                        error: e.to_string(),
                    })),
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Start the pipeline process.
    async fn start(&self, pr: PipelineRevision) -> Result<PipelineHandle, ManagerError> {
        let pipeline_id = pr.pipeline.pipeline_id;
        let program_id = pr.pipeline.program_id.unwrap();

        log::debug!("Pipeline config is '{:?}'", pr.config);

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
        let expanded_config = serde_yaml::to_string(&pr.config).unwrap();
        fs::write(&config_file_path, &expanded_config)
            .await
            .map_err(|e| {
                ManagerError::io_error(
                    format!("writing config file '{}'", config_file_path.display()),
                    e,
                )
            })?;
        let metadata_file_path = self.config.metadata_file_path(pipeline_id);
        fs::write(&metadata_file_path, serde_json::to_string(&pr).unwrap())
            .await
            .map_err(|e| {
                ManagerError::io_error(
                    format!("writing metadata file '{}'", metadata_file_path.display()),
                    e,
                )
            })?;

        // Locate project executable.
        let executable_ref = self
            .db
            .lock()
            .await
            .get_compiled_binary_ref(program_id, pr.program.version)
            .await?;
        if executable_ref.is_none() {
            return Err(RunnerError::BinaryFetchError {
                pipeline_id,
                error: format!("Did not receieve a compiled binary URL for {pipeline_id}"),
            }
            .into());
        }
        let fetched_executable = binary_ref_to_url(&executable_ref.unwrap(), pipeline_id).await?;

        // Run executable, set current directory to pipeline directory, pass metadata
        // file and config as arguments.
        let pipeline_process = Command::new(fetched_executable)
            .current_dir(self.config.pipeline_dir(pipeline_id))
            .arg("--config-file")
            .arg(&config_file_path)
            .arg("--metadata-file")
            .arg(&metadata_file_path)
            .stdin(Stdio::null())
            .spawn()
            .map_err(|e| RunnerError::PipelineStartupError {
                pipeline_id,
                error: e.to_string(),
            })?;

        Ok(PipelineHandle { pipeline_process })
    }

    /// Parse `ErrorResponse` from JSON. On error, builds an `ErrorResponse`
    /// with the originaln JSON content.
    fn error_response_from_json(
        pipeline_id: PipelineId,
        status: StatusCode,
        json: &JsonValue,
    ) -> ErrorResponse {
        ErrorResponse::deserialize(json).unwrap_or_else(|_| {
            ErrorResponse::from(&RunnerError::HttpForwardError {
                pipeline_id,
                error: format!("Pipeline returned HTTP status {status}, response body:{json:#}"),
            })
        })
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
                            spawn(
                                PipelineAutomaton::new(
                                    pipeline_id,
                                    tenant_id,
                                    &config,
                                    db.clone(),
                                    notifier.clone(),
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

pub async fn binary_ref_to_url(
    binary_ref: &str,
    pipeline_id: PipelineId,
) -> Result<String, RunnerError> {
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
                        "fileref {} for binary required by pipeline {pipeline_id} does not exist",
                        parsed.path()
                    ),
                }),
                Err(e) => Err(RunnerError::BinaryFetchError {
                    pipeline_id,
                    error: format!(
                        "Check fileref {} for binary required by pipeline {pipeline_id} returned an error: {}",
                        parsed.path(),
                        e
                    ),
                }),
            }
        }
        _ => todo!("Unsupported URL scheme for binary ref"),
    }
}

/// Send HTTP request to pipeline and parse response as a JSON object.
async fn pipeline_http_request_json_response(
    pipeline_id: PipelineId,
    method: Method,
    endpoint: &str,
    port: &str,
) -> Result<(StatusCode, JsonValue), RunnerError> {
    let response = RunnerApi::pipeline_http_request(pipeline_id, method, endpoint, port).await?;
    let status = response.status();

    let value = response
        .json::<JsonValue>()
        .await
        .map_err(|e| RunnerError::HttpForwardError {
            pipeline_id,
            error: e.to_string(),
        })?;

    Ok((status, value))
}
