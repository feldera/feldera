use crate::{
    auth::TenantId,
    config::LocalRunnerConfig,
    db::{
        storage::Storage, DBError, PipelineId, PipelineRevision, PipelineRuntimeState,
        PipelineStatus, ProjectDB,
    },
    pipeline_manager::ManagerError,
};
use actix_web::{
    body::BoxBody,
    http::{Method, StatusCode},
    web::Payload,
    HttpRequest, HttpResponse, HttpResponseBuilder, ResponseError,
};
use chrono::{DateTime, Utc};
use dbsp_adapters::{DetailedError, ErrorResponse};
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    error::Error as StdError,
    fmt,
    fmt::Display,
    process::Stdio,
    process::{Child, Command},
    sync::Arc,
};
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all},
    spawn,
    sync::Mutex,
    time::{sleep, Duration},
};
use tokio::{sync::Notify, time::timeout};
use uuid::Uuid;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RunnerError {
    PipelineShutdown {
        pipeline_id: PipelineId,
    },
    HttpForwardError {
        pipeline_id: PipelineId,
        error: String,
    },
    PortFileParseError {
        pipeline_id: PipelineId,
        error: String,
    },
    PipelineProvisioningTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    PipelineInitializationTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    PipelineShutdownTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    PipelineStartupError {
        pipeline_id: PipelineId,
        // TODO: This should be IOError, so we can serialize the error code
        // similar to `DBSPError::IO`.
        error: String,
    },
    IllegalPipelineStateTransition {
        pipeline_id: PipelineId,
        error: String,
        current_status: PipelineStatus,
        desired_status: PipelineStatus,
        requested_status: Option<PipelineStatus>,
    },
    BinaryFetchError {
        pipeline_id: PipelineId,
        error: String,
    },
}

impl DetailedError for RunnerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PipelineShutdown { .. } => Cow::from("PipelineShutdown"),
            Self::HttpForwardError { .. } => Cow::from("HttpForwardError"),
            Self::PortFileParseError { .. } => Cow::from("PortFileParseError"),
            Self::PipelineProvisioningTimeout { .. } => Cow::from("PipelineProvisioningTimeout"),
            Self::PipelineInitializationTimeout { .. } => {
                Cow::from("PipelineInitializationTimeout")
            }
            Self::PipelineShutdownTimeout { .. } => Cow::from("PipelineShutdownTimeout"),
            Self::PipelineStartupError { .. } => Cow::from("PipelineStartupError"),
            Self::IllegalPipelineStateTransition { .. } => {
                Cow::from("IllegalPipelineStateTransition")
            }
            Self::BinaryFetchError { .. } => Cow::from("BinaryFetchError"),
        }
    }
}

impl Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PipelineShutdown { pipeline_id } => {
                write!(f, "Pipeline '{pipeline_id}' is not currently running.")
            }
            Self::HttpForwardError { pipeline_id, error } => {
                write!(
                    f,
                    "Error forwarding HTTP request to pipeline '{pipeline_id}': '{error}'"
                )
            }
            Self::PipelineProvisioningTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(
                    f,
                    "Waiting for pipeline '{pipeline_id}' to start timed out after {timeout:?}"
                )
            }
            Self::PipelineInitializationTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(f, "Waiting for pipeline '{pipeline_id}' initialization timed out after {timeout:?}")
            }
            Self::PipelineShutdownTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(
                    f,
                    "Waiting for pipeline '{pipeline_id}' to shutdown timed out after {timeout:?}"
                )
            }
            Self::PortFileParseError { pipeline_id, error } => {
                write!(
                    f,
                    "Could not parse port for pipeline '{pipeline_id}' from port file: '{error}'"
                )
            }
            Self::PipelineStartupError { pipeline_id, error } => {
                write!(f, "Failed to start pipeline '{pipeline_id}': '{error}'")
            }
            Self::IllegalPipelineStateTransition { error, .. } => {
                write!(
                    f,
                    "Action is not applicable in the current state of the pipeline: {error}"
                )
            }
            Self::BinaryFetchError { pipeline_id, error } => {
                write!(f, "Failed to fetch binary executable for running pipeline '{pipeline_id}': '{error}'")
            }
        }
    }
}

impl StdError for RunnerError {}

impl ResponseError for RunnerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::PipelineShutdown { .. } => StatusCode::NOT_FOUND,
            Self::HttpForwardError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PortFileParseError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineProvisioningTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineInitializationTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineShutdownTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineStartupError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::IllegalPipelineStateTransition { .. } => StatusCode::BAD_REQUEST,
            Self::BinaryFetchError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}

/// A runner component responsible for running and interacting with
/// pipelines at runtime.
pub struct RunnerInterface {
    db: Arc<Mutex<ProjectDB>>,
}

impl RunnerInterface {
    /// Create a local runner.
    pub fn new(db: Arc<Mutex<ProjectDB>>) -> Self {
        Self { db }
    }

    /// Initiate pipeline shutdown.
    ///
    /// Sets desired pipeline state to [`PipelineStatus::Paused`].
    pub(crate) async fn shutdown_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), ManagerError> {
        self.set_desired_status(tenant_id, pipeline_id, PipelineStatus::Shutdown)
            .await?;
        Ok(())
    }

    /// Delete the pipeline.
    ///
    /// Botht the desired and the actual states of the pipeline must be equal
    /// to [`PipelineStatus::Shutdown`] or [`PipelineStatus::Failed`].
    pub(crate) async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), ManagerError> {
        // Make sure that the pipeline is in a `Shutdown` state.

        // TODO: this function should run in a transaction to avoid conflicts with
        // another manager instance.
        let db = self.db.lock().await;

        let pipeline_state = db
            .get_pipeline_runtime_state(tenant_id, pipeline_id)
            .await?;
        Self::validate_desired_state_request(pipeline_id, &pipeline_state, None)?;

        db.delete_pipeline(tenant_id, pipeline_id).await?;

        // No need to do anything else since the pipeline was in the `Shutdown` state.
        // The pipeline tokio task will self-destruct when it polls pipeline
        // state and discovers it has been deleted.

        Ok(())
    }

    /// Set the desired state of the pipeline to [`PipelineStatus::Paused`].
    ///
    /// If the pipeline is currently in the `Shutdown` state, will validate
    /// and commit the pipeline before running it.
    pub(crate) async fn pause_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), ManagerError> {
        self.set_desired_status(tenant_id, pipeline_id, PipelineStatus::Paused)
            .await?;

        Ok(())
    }

    /// Set the desired state of the pipeline to [`PipelineStatus::Running`].
    ///
    /// If the pipeline is currently in the `Shutdown` state, will validate
    /// and commit the pipeline before running it.
    pub(crate) async fn start_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), ManagerError> {
        self.set_desired_status(tenant_id, pipeline_id, PipelineStatus::Running)
            .await?;

        Ok(())
    }

    /// Check the `request` is a valid new desired state given the current runtime state
    /// of the pipeline.  `request` value of `None` represents the request to delete the
    /// pipeline.
    fn validate_desired_state_request(
        pipeline_id: PipelineId,
        pipeline_state: &PipelineRuntimeState,
        request: Option<PipelineStatus>,
    ) -> Result<(), ManagerError> {
        match request {
            None => {
                if pipeline_state.current_status != PipelineStatus::Shutdown
                    || pipeline_state.desired_status != PipelineStatus::Shutdown
                {
                    Err(RunnerError::IllegalPipelineStateTransition {
                        pipeline_id,
                        error: "Cannot delete a running pipeline. Shutdown the pipeline first by invoking the '/shutdown' endpoint.".to_string(),
                        current_status: pipeline_state.current_status,
                        desired_status: pipeline_state.desired_status,
                        requested_status: None,
                    })?
                };
            }
            Some(new_desired_status) => {
                if new_desired_status == PipelineStatus::Paused
                    || new_desired_status == PipelineStatus::Running
                {
                    // Refuse to restart a pipeline that has not completed shutting down.
                    if pipeline_state.desired_status == PipelineStatus::Shutdown
                        && pipeline_state.current_status != PipelineStatus::Shutdown
                    {
                        Err(RunnerError::IllegalPipelineStateTransition {
                            pipeline_id,
                            error: "Cannot restart the pipeline while it is shutting down. Wait for the shutdown to complete before starting a new instance of the pipeline.".to_string(),
                            current_status: pipeline_state.current_status,
                            desired_status: pipeline_state.desired_status,
                            requested_status: Some(new_desired_status),
                        })?;
                    };

                    // Refuse to restart failed pipeline until it's in the shutdown state.
                    if pipeline_state.desired_status != PipelineStatus::Shutdown
                        && (pipeline_state.current_status == PipelineStatus::ShuttingDown
                            || pipeline_state.current_status == PipelineStatus::Failed)
                    {
                        Err(RunnerError::IllegalPipelineStateTransition {
                            pipeline_id,
                            error: "Cannot restart a failed pipeline. Clear the error state first by invoking the '/shutdown' endpoint.".to_string(),
                            current_status: pipeline_state.current_status,
                            desired_status: pipeline_state.desired_status,
                            requested_status: Some(new_desired_status),
                        })?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn set_desired_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        new_desired_status: PipelineStatus,
    ) -> Result<(), ManagerError> {
        // TODO: this function should run in a transaction to avoid conflicts with
        // another manager instance.

        let db = self.db.lock().await;
        let pipeline_state = db
            .get_pipeline_runtime_state(tenant_id, pipeline_id)
            .await?;

        Self::validate_desired_state_request(
            pipeline_id,
            &pipeline_state,
            Some(new_desired_status),
        )?;

        // When starting a previously shutdown pipeline, commit its config first.
        if pipeline_state.current_status == PipelineStatus::Shutdown
            && new_desired_status != PipelineStatus::Shutdown
        {
            Self::commit_revision(&db, tenant_id, pipeline_id).await?;
        }

        db.set_pipeline_desired_status(tenant_id, pipeline_id, new_desired_status)
            .await?;
        Ok(())
    }

    /// Retrieve the last revision for a pipeline.
    ///
    /// Tries to create a new revision if this pipeline never had a revision
    /// created before.
    async fn commit_revision(
        db: &ProjectDB,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), ManagerError> {
        // Make sure we create a revision by updating to latest config state
        match db
            .create_pipeline_revision(Uuid::now_v7(), tenant_id, pipeline_id)
            .await
        {
            Ok(_revision) => Ok(()),
            Err(DBError::RevisionNotChanged) => Ok(()),
            Err(e) => Err(e)?,
        }
    }

    /// Forward HTTP request to the pipeline.
    pub(crate) async fn forward_to_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
    ) -> Result<HttpResponse, ManagerError> {
        let pipeline_state = self
            .db
            .lock()
            .await
            .get_pipeline_runtime_state(tenant_id, pipeline_id)
            .await?;

        match pipeline_state.current_status {
            PipelineStatus::Shutdown | PipelineStatus::Failed | PipelineStatus::Provisioning => {
                Err(RunnerError::PipelineShutdown { pipeline_id })?
            }
            _ => {}
        }

        Self::do_forward_to_pipeline(pipeline_id, method, endpoint, &pipeline_state.location).await
    }

    /// Forward HTTP request to pipeline.  Assumes that the pipeline is running.
    /// Takes pipeline port as an argument instead of reading it from the database.
    async fn do_forward_to_pipeline(
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
        port: &str,
    ) -> Result<HttpResponse, ManagerError> {
        let response = Self::pipeline_http_request(pipeline_id, method, endpoint, port).await?;
        let status = response.status();

        let mut response_builder = HttpResponse::build(status);
        // Remove `Connection` as per
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Connection#Directives
        for (header_name, header_value) in response
            .headers()
            .iter()
            .filter(|(h, _)| *h != "connection")
        {
            response_builder.insert_header((header_name.clone(), header_value.clone()));
        }

        let response_body = response
            .bytes()
            .await
            .map_err(|e| RunnerError::HttpForwardError {
                pipeline_id,
                error: e.to_string(),
            })?;

        Ok(response_builder.body(response_body))
    }

    /// Send HTTP request to pipeline.
    async fn pipeline_http_request(
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
        port: &str,
    ) -> Result<reqwest::Response, RunnerError> {
        let client = reqwest::Client::new();
        client
            .request(method, &format!("http://localhost:{port}/{endpoint}",))
            .send()
            .await
            .map_err(|e| RunnerError::HttpForwardError {
                pipeline_id,
                error: e.to_string(),
            })
    }

    /// Send HTTP request to pipeline and parse response as a JSON object.
    async fn pipeline_http_request_json_response(
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
        port: &str,
    ) -> Result<(StatusCode, JsonValue), RunnerError> {
        let response = Self::pipeline_http_request(pipeline_id, method, endpoint, port).await?;
        let status = response.status();

        let value =
            response
                .json::<JsonValue>()
                .await
                .map_err(|e| RunnerError::HttpForwardError {
                    pipeline_id,
                    error: e.to_string(),
                })?;

        Ok((status, value))
    }

    pub(crate) async fn forward_to_pipeline_as_stream(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        endpoint: &str,
        req: HttpRequest,
        body: Payload,
    ) -> Result<HttpResponse, ManagerError> {
        let pipeline_state = self
            .db
            .lock()
            .await
            .get_pipeline_runtime_state(tenant_id, pipeline_id)
            .await?;

        match pipeline_state.current_status {
            PipelineStatus::Shutdown | PipelineStatus::Failed | PipelineStatus::Provisioning => {
                Err(RunnerError::PipelineShutdown { pipeline_id })?
            }
            _ => {}
        }
        let port = pipeline_state.location;

        // TODO: it might be better to have ?name={}, otherwise we have to
        // restrict name format
        let url = format!("http://localhost:{port}/{endpoint}?{}", req.query_string());

        let client = awc::Client::new();

        let mut request = client.request(req.method().clone(), url);

        for header in req
            .headers()
            .into_iter()
            .filter(|(h, _)| *h != "connection")
        {
            request = request.append_header(header);
        }

        let response =
            request
                .send_stream(body)
                .await
                .map_err(|e| RunnerError::HttpForwardError {
                    pipeline_id,
                    error: e.to_string(),
                })?;

        let mut builder = HttpResponseBuilder::new(response.status());
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        Ok(builder.streaming(response))
    }
}

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
            let mut pipeline = db
                .get_pipeline_runtime_state(self.tenant_id, self.pipeline_id)
                .await?;

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
                    match RunnerInterface::pipeline_http_request_json_response(
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
                    match RunnerInterface::pipeline_http_request_json_response(
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
                    match RunnerInterface::pipeline_http_request_json_response(
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
                    match RunnerInterface::pipeline_http_request_json_response(
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
                    match RunnerInterface::pipeline_http_request_json_response(
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

                                if state == "Paused" {
                                    self.update_pipeline_status(
                                        &mut pipeline,
                                        PipelineStatus::Paused,
                                        None,
                                    )
                                    .await;
                                    self.update_pipeline_runtime_state(&pipeline).await?;
                                } else if state == "Running" {
                                    self.update_pipeline_status(
                                        &mut pipeline,
                                        PipelineStatus::Running,
                                        None,
                                    )
                                    .await;
                                    self.update_pipeline_runtime_state(&pipeline).await?;
                                } else {
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
        let fetched_executable = binary_ref_to_url(&executable_ref, pipeline_id).await?;

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

/// A runner that executes pipelines locally
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
pub struct LocalRunner {}

impl LocalRunner {
    pub async fn run(db: Arc<Mutex<ProjectDB>>, config: &LocalRunnerConfig) {
        let runner_task = spawn(Self::reconcile(db, Arc::new(config.clone())));
        runner_task.await.unwrap().unwrap();
    }

    async fn reconcile(
        db: Arc<Mutex<ProjectDB>>,
        config: Arc<LocalRunnerConfig>,
    ) -> Result<(), ManagerError> {
        let pipelines: Mutex<BTreeMap<PipelineId, Arc<Notify>>> = Mutex::new(BTreeMap::new());
        loop {
            sleep(Duration::from_millis(10)).await;
            for entry in db.lock().await.all_pipelines().await?.iter() {
                let tenant_id = entry.0;
                let pipeline_id = entry.1;
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
