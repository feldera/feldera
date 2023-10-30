//! This module contains helpers to build pipeline runners.
use crate::db::{ProgramId, Version};
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
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{error, info};
use pipeline_types::config::PipelineConfig;
use pipeline_types::error::ErrorResponse;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::{fs, sync::Mutex, time::Duration};
use tokio::{sync::Notify, time::timeout};

/// Trait to be implemented by any pipeline runner. The PipelineAutomaton
/// invokes these methods per pipeline.
#[async_trait]
pub trait PipelineExecutor {
    /// Starts a new pipeline (e.g., brings up a process that runs the pipeline
    /// binary)
    async fn start(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError>;

    /// Return the hostname:port over which the pipeline's HTTP server should be
    /// reachable Ok(None) indicates that the pipeline is still initializing
    async fn get_location(&mut self) -> Result<Option<String>, ManagerError>;

    /// Returns whether the pipeline has been shutdown
    async fn check_if_shutdown(&mut self) -> bool;

    /// Initiates pipeline shutdown (e.g., send a SIGTERM successfully to the
    /// process)
    async fn shutdown(&mut self) -> Result<(), ManagerError>;
}

/// Pipeline automaton monitors the runtime state of a single pipeline
/// and continually reconciles desired and actual state.
///
/// The automaton runs as a separate tokio task.
pub struct PipelineAutomaton<T>
where
    T: PipelineExecutor,
{
    pipeline_id: PipelineId,
    tenant_id: TenantId,
    pipeline_handle: T,
    db: Arc<Mutex<ProjectDB>>,
    notifier: Arc<Notify>,
}

/// A description of a pipeline to execute
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PipelineExecutionDesc {
    pub pipeline_id: PipelineId,
    pub pipeline_name: String,
    pub program_id: ProgramId,
    pub version: Version,
    pub config: PipelineConfig,
    pub jit_mode: bool,
    pub code: String,
    pub binary_ref: String,
}

fn to_execution_desc(pr: PipelineRevision, binary_ref: String) -> PipelineExecutionDesc {
    PipelineExecutionDesc {
        pipeline_id: pr.pipeline.pipeline_id,
        pipeline_name: pr.pipeline.name,
        program_id: pr.program.program_id,
        version: pr.program.version,
        config: pr.config,
        jit_mode: pr.program.jit_mode,
        // PipelineRevision always contains program code.
        code: pr.program.code.as_ref().unwrap().clone(),
        binary_ref,
    }
}

impl<T: PipelineExecutor> PipelineAutomaton<T> {
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
    const INITIALIZATION_TIMEOUT: Duration = Duration::from_millis(60_000);

    /// How often to poll for the pipeline initialization status.
    const INITIALIZATION_POLL_PERIOD: Duration = Duration::from_millis(300);

    /// Max time to wait for the pipeline process to exit.
    // TODO: It seems that Actix often takes a while to shutdown.  This
    // is something to investigate.
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10_000);

    /// How often to poll for the pipeline process to exit.
    const SHUTDOWN_POLL_PERIOD: Duration = Duration::from_millis(300);

    /// Create a new PipelineAutomaton for a given pipeline
    pub fn new(
        pipeline_id: PipelineId,
        tenant_id: TenantId,
        db: Arc<Mutex<ProjectDB>>,
        notifier: Arc<Notify>,
        pipeline_handle: T,
    ) -> Self {
        Self {
            pipeline_id,
            tenant_id,
            pipeline_handle,
            db,
            notifier,
        }
    }

    /// Runs until the pipeline is deleted or an unexpected error occurs.
    pub async fn run(self) -> Result<(), ManagerError> {
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
                // Locate project executable.
                let pipeline_id = self.pipeline_id;
                let executable_ref = if !revision.program.jit_mode {
                    db.get_compiled_binary_ref(
                        revision.program.program_id,
                        revision.program.version,
                    )
                    .await?
                    .ok_or_else(|| RunnerError::BinaryFetchError {
                        pipeline_id,
                        error: format!("Did not receieve a compiled binary URL for {pipeline_id}"),
                    })?
                } else {
                    String::new()
                };
                drop(db);
                let execution_desc = to_execution_desc(revision, executable_ref);

                match self.pipeline_handle.start(execution_desc).await {
                    Ok(_) => {
                        info!(
                            "Pipeline {} started (Tenant {})",
                            self.pipeline_id, self.tenant_id
                        );
                    }
                    Err(e) => {
                        self.mark_pipeline_as_failed(&mut pipeline, Some(e)).await?;
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
                    match self.pipeline_handle.get_location().await {
                        Ok(Some(location)) => {
                            self.update_pipeline_status(
                                &mut pipeline,
                                PipelineStatus::Initializing,
                                None,
                            )
                            .await;
                            pipeline.set_location(location);
                            pipeline.set_created();
                            self.update_pipeline_runtime_state(&pipeline).await?;
                            poll_timeout = Self::INITIALIZATION_POLL_PERIOD;
                        }
                        Ok(None) => {
                            if Self::timeout_expired(
                                pipeline.status_since,
                                Self::PROVISIONING_TIMEOUT,
                            ) {
                                self.mark_pipeline_as_failed(
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
                            self.mark_pipeline_as_failed(&mut pipeline, Some(e)).await?;
                        }
                    }
                }
                // User cancels the pipeline while it's still provisioning.
                (PipelineStatus::Provisioning, PipelineStatus::Shutdown) => {
                    self.mark_pipeline_as_failed(&mut pipeline, <Option<RunnerError>>::None)
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
                            info!("Could not connect to pipeline {e:?}");
                            // self.mark_pipeline_as_failed(&mut pipeline, Some(e)).await?;
                            if Self::timeout_expired(
                                pipeline.status_since,
                                Self::INITIALIZATION_TIMEOUT,
                            ) {
                                self.mark_pipeline_as_failed(
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
                                    self.mark_pipeline_as_failed(
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
                                self.mark_pipeline_as_failed_on_error(&mut pipeline, status, &body)
                                    .await?;
                            }
                        }
                    }
                }
                // User cancels the pipeline while it is still initalizing.
                (PipelineStatus::Initializing, PipelineStatus::Shutdown) => {
                    self.mark_pipeline_as_failed(&mut pipeline, <Option<RunnerError>>::None)
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
                            self.mark_pipeline_as_failed(&mut pipeline, Some(e)).await?;
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
                                self.mark_pipeline_as_failed_on_error(&mut pipeline, status, &body)
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
                            self.mark_pipeline_as_failed(&mut pipeline, Some(e)).await?;
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
                                self.mark_pipeline_as_failed_on_error(&mut pipeline, status, &body)
                                    .await?;
                            }
                        }
                    }
                }
                // Issue a pipeline shutdown.
                (PipelineStatus::Running, PipelineStatus::Shutdown)
                | (PipelineStatus::Paused, PipelineStatus::Shutdown) => {
                    match self.pipeline_handle.shutdown().await {
                        Ok(_) => {
                            self.update_pipeline_status(
                                &mut pipeline,
                                PipelineStatus::ShuttingDown,
                                None,
                            )
                            .await;
                            self.update_pipeline_runtime_state(&pipeline).await?;
                            poll_timeout = Self::SHUTDOWN_POLL_PERIOD;
                        }
                        Err(e) => {
                            self.mark_pipeline_as_failed(
                                &mut pipeline,
                                Some(RunnerError::PipelineShutdownError {
                                    pipeline_id: self.pipeline_id,
                                    error: e.to_string(),
                                }),
                            )
                            .await?;
                        }
                    }
                }
                // Shutdown in progress. Wait for the pipeline process to terminate.
                (PipelineStatus::ShuttingDown, _) => {
                    if self.pipeline_handle.check_if_shutdown().await {
                        self.update_pipeline_status(&mut pipeline, PipelineStatus::Shutdown, None)
                            .await;
                        self.update_pipeline_runtime_state(&pipeline).await?;
                    } else if Self::timeout_expired(pipeline.status_since, Self::SHUTDOWN_TIMEOUT) {
                        self.mark_pipeline_as_failed(
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
                // User acknowledges pipeline failure by invoking the `/shutdown` endpoint.
                // Move to the `Shutdown` state so that the pipeline can be started again.
                (PipelineStatus::Failed, PipelineStatus::Shutdown) => {
                    let error = pipeline.error.clone();
                    let _ = self.pipeline_handle.shutdown().await;
                    self.update_pipeline_status(&mut pipeline, PipelineStatus::Shutdown, error)
                        .await;
                    self.update_pipeline_runtime_state(&pipeline).await?;
                }
                // Steady-state operation.  Periodically poll the pipeline.
                (PipelineStatus::Running, _)
                | (PipelineStatus::Paused, _)
                | (PipelineStatus::Failed, _) => {
                    self.probe(&mut pipeline).await?;
                }
                (PipelineStatus::Shutdown, _) => {}
                _ => {
                    error!(
                        "Unexpected current/desired pipeline status combination {:?}/{:?}",
                        pipeline.current_status, pipeline.desired_status
                    )
                }
            }
        }
    }

    async fn probe(&mut self, pipeline: &mut PipelineRuntimeState) -> Result<(), ManagerError> {
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
                self.mark_pipeline_as_failed(pipeline, Some(e)).await?;
            }
            Ok((status, body)) => {
                if !status.is_success() {
                    // Pipeline responds with an error, meaning that the pipeline
                    // HTTP server is still running, but the pipeline itself failed --
                    // save it out of its misery.
                    self.mark_pipeline_as_failed_on_error(pipeline, status, &body)
                        .await?;
                } else {
                    let global_metrics = if let Some(metrics) = body.get("global_metrics") {
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

                    if state == "Paused" && pipeline.current_status != PipelineStatus::Paused {
                        self.update_pipeline_status(pipeline, PipelineStatus::Paused, None)
                            .await;
                        self.update_pipeline_runtime_state(pipeline).await?;
                    } else if state == "Running"
                        && pipeline.current_status != PipelineStatus::Running
                    {
                        self.update_pipeline_status(pipeline, PipelineStatus::Running, None)
                            .await;
                        self.update_pipeline_runtime_state(pipeline).await?;
                    } else if state != "Paused" && state != "Running" {
                        self.mark_pipeline_as_failed(pipeline, Some(RunnerError::HttpForwardError {
                                        pipeline_id: self.pipeline_id,
                                        error: format!("Pipeline reported unexpected status '{state}', expected 'Paused' or 'Running'")
                                    })).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn update_pipeline_status(
        &self,
        pipeline: &mut PipelineRuntimeState,
        status: PipelineStatus,
        error: Option<ErrorResponse>,
    ) {
        pipeline.set_current_status(status, error);
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
    async fn mark_pipeline_as_failed<E>(
        &mut self,
        pipeline: &mut PipelineRuntimeState,
        error: Option<E>,
    ) -> Result<(), DBError>
    where
        ErrorResponse: for<'a> From<&'a E>,
    {
        self.update_pipeline_status(
            pipeline,
            PipelineStatus::Failed,
            error.map(|e| ErrorResponse::from(&e)),
        )
        .await;
        self.update_pipeline_runtime_state(pipeline).await
    }

    /// Same as `mark_pipeline_as_failed`, but deserializes `ErrorResponse` from
    /// a JSON error returned by the pipeline.
    async fn mark_pipeline_as_failed_on_error(
        &mut self,
        pipeline: &mut PipelineRuntimeState,
        status: StatusCode,
        error: &JsonValue,
    ) -> Result<(), DBError> {
        let error = Self::error_response_from_json(self.pipeline_id, status, error);
        self.update_pipeline_status(pipeline, PipelineStatus::Failed, Some(error))
            .await;
        self.update_pipeline_runtime_state(pipeline).await
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

pub async fn fetch_binary_ref(
    config: &LocalRunnerConfig,
    binary_ref: &str,
    pipeline_id: PipelineId,
    program_id: ProgramId,
    version: Version,
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
                    let path = config.binary_file_path(pipeline_id, program_id, version);
                    let mut file = tokio::fs::File::options()
                        .create(true)
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
