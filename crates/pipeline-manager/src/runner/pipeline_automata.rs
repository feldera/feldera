use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDesiredStatus, PipelineId, PipelineStatus,
};
use crate::db::types::program::{generate_pipeline_config, ProgramStatus};
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::interaction::RunnerInteraction;
use crate::runner::pipeline_executor::PipelineExecutor;
use actix_web::http::{Method, StatusCode};
use chrono::{DateTime, Utc};
use feldera_types::config::PipelineConfig;
use feldera_types::error::ErrorResponse;
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::{sync::Mutex, time::Duration};
use tokio::{sync::Notify, time::timeout};

/// Timeout for an HTTP request of the automata to a pipeline.
const PIPELINE_AUTOMATA_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Sends HTTP request from the automata to pipeline and parses response as a JSON object.
async fn automaton_http_request_to_pipeline_json(
    pipeline_id: PipelineId,
    method: Method,
    endpoint: &str,
    location: &str,
) -> Result<(StatusCode, serde_json::Value), ManagerError> {
    RunnerInteraction::http_request_to_pipeline_json(
        pipeline_id,
        None,
        location,
        method,
        endpoint,
        "",
        Some(PIPELINE_AUTOMATA_HTTP_REQUEST_TIMEOUT),
    )
    .await
}

/// Utility type for the pipeline automaton to describe state changes.
#[derive(Debug, PartialEq)]
enum State {
    TransitionToProvisioning { deployment_config: PipelineConfig },
    TransitionToInitializing { deployment_location: String },
    TransitionToPaused,
    TransitionToRunning,
    TransitionToUnavailable,
    TransitionToFailed { error: ErrorResponse },
    TransitionToShuttingDown,
    TransitionToShutdown,
    Unchanged,
}

/// Outcome of a status check for a pipeline by polling its `/stats` endpoint.
enum StatusCheckResult {
    Paused,
    Running,
    /// Unable to be reached or responded to not yet be ready.
    Unavailable,
    /// Failed to parse response or a runtime error was returned.
    Error(ErrorResponse),
}

/// Pipeline automaton monitors the runtime state of a single pipeline
/// and continually reconciles actual with desired state.
///
/// The automaton runs as a separate tokio task.
pub struct PipelineAutomaton<T>
where
    T: PipelineExecutor,
{
    platform_version: String,
    pipeline_id: PipelineId,
    tenant_id: TenantId,
    pipeline_handle: T,
    db: Arc<Mutex<StoragePostgres>>,
    notifier: Arc<Notify>,

    /// Whether the first run cycle still has to be done.
    /// In the first run cycle, the pipeline handle's initialization is called.
    first_run_cycle: bool,

    /// Whether provision() has been called in the `Provisioning` stage.
    provision_called: bool,

    /// Maximum time to wait for the pipeline resources to be provisioned.
    /// This can differ significantly between the type of runner.
    provisioning_timeout: Duration,

    /// How often to poll during provisioning.
    provisioning_poll_period: Duration,

    /// How often to poll during shutting down.
    shutdown_poll_period: Duration,
}

impl<T: PipelineExecutor> PipelineAutomaton<T> {
    /// The frequency of polling the pipeline during normal operation
    /// when we don't normally expect its state to change.
    const DEFAULT_PIPELINE_POLL_PERIOD: Duration = Duration::from_millis(2_500);

    /// Maximum time to wait for the pipeline to initialize its connectors and web server.
    const INITIALIZATION_TIMEOUT: Duration = Duration::from_millis(60_000);

    /// How often to poll the pipeline during initialization.
    const INITIALIZATION_POLL_PERIOD: Duration = Duration::from_millis(250);

    /// Creates a new automaton for a given pipeline.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        platform_version: &str,
        pipeline_id: PipelineId,
        tenant_id: TenantId,
        db: Arc<Mutex<StoragePostgres>>,
        notifier: Arc<Notify>,
        pipeline_handle: T,
        provisioning_timeout: Duration,
        provisioning_poll_period: Duration,
        shutdown_poll_period: Duration,
    ) -> Self {
        Self {
            platform_version: platform_version.to_string(),
            pipeline_id,
            tenant_id,
            pipeline_handle,
            db,
            notifier,
            first_run_cycle: true,
            provision_called: false,
            provisioning_timeout,
            provisioning_poll_period,
            shutdown_poll_period,
        }
    }

    /// Runs until the pipeline is deleted or an unexpected error occurs.
    pub async fn run(mut self) -> Result<(), ManagerError> {
        let pipeline_id = self.pipeline_id;
        let mut poll_timeout = Self::DEFAULT_PIPELINE_POLL_PERIOD;
        loop {
            // Wait until the timeout expires, or we get notified that the
            // pipeline has been updated
            let _ = timeout(poll_timeout, self.notifier.notified()).await;
            match self.do_run().await {
                Ok(new_poll_timeout) => {
                    poll_timeout = new_poll_timeout;
                }
                Err(e) => {
                    // Only database errors can bubble up here. They are always fatal
                    // to the automaton because the database itself is used to update
                    // the pipeline status and communicate failures to the user.
                    //
                    // TODO: as a consequence, if the database is temporarily unreachable,
                    //       the pipeline automatons will terminate. It is not possible to
                    //       recreate a pipeline automaton currently except by restarting
                    //       the runner. There could be a retry strategy here for the database,
                    //       where it not immediately terminates but instead waits in hopes
                    //       of the database returning.
                    match &e {
                        // Pipeline deletions should not lead to errors in the logs.
                        DBError::UnknownPipeline { pipeline_id } => {
                            info!("Pipeline {pipeline_id} no longer exists: shutting down its automaton");
                        }
                        _ => {
                            error!("Pipeline automaton {pipeline_id} terminated with database error: {e}")
                        }
                    };

                    // By leaving the run loop, the automata will consume itself.
                    // As such, the pipeline_handle it owns will be dropped,
                    // which in turn will shut down by itself as a consequence.
                    return Err(ManagerError::from(e));
                }
            }
        }
    }

    /// Executes one run cycle.
    async fn do_run(&mut self) -> Result<Duration, DBError> {
        // Retrieve the current pipeline including its statuses
        let pipeline = &self
            .db
            .lock()
            .await
            .get_pipeline_by_id(self.tenant_id, self.pipeline_id)
            .await?;

        // Runner initialization is called on the first run cycle
        if self.first_run_cycle {
            self.pipeline_handle
                .init(
                    pipeline.deployment_status != PipelineStatus::Shutdown
                        && pipeline.deployment_status != PipelineStatus::Provisioning,
                )
                .await;
            self.first_run_cycle = false;
        }

        // Determine transition
        let transition: State = match (
            pipeline.deployment_status,
            pipeline.deployment_desired_status,
        ) {
            // Shutdown
            (PipelineStatus::Shutdown, PipelineDesiredStatus::Shutdown) => State::Unchanged,
            (
                PipelineStatus::Shutdown,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => self.transit_shutdown_to_paused_or_running(pipeline).await?,

            // Provisioning
            (PipelineStatus::Provisioning, PipelineDesiredStatus::Shutdown) => {
                State::TransitionToShuttingDown
            }
            (
                PipelineStatus::Provisioning,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => {
                self.transit_provisioning_to_paused_or_running(pipeline)
                    .await
            }

            // Initializing
            (PipelineStatus::Initializing, PipelineDesiredStatus::Shutdown) => {
                State::TransitionToShuttingDown
            }
            (
                PipelineStatus::Initializing,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => {
                self.transit_initializing_to_paused_or_running(pipeline)
                    .await
            }

            // Paused
            (PipelineStatus::Paused, PipelineDesiredStatus::Shutdown) => {
                State::TransitionToShuttingDown
            }
            (PipelineStatus::Paused, PipelineDesiredStatus::Paused) => {
                self.probe_initialized_pipeline(pipeline).await
            }
            (PipelineStatus::Paused, PipelineDesiredStatus::Running) => {
                self.perform_action_initialized_pipeline(pipeline, true)
                    .await
            }

            // Running
            (PipelineStatus::Running, PipelineDesiredStatus::Shutdown) => {
                State::TransitionToShuttingDown
            }
            (PipelineStatus::Running, PipelineDesiredStatus::Paused) => {
                self.perform_action_initialized_pipeline(pipeline, false)
                    .await
            }
            (PipelineStatus::Running, PipelineDesiredStatus::Running) => {
                self.probe_initialized_pipeline(pipeline).await
            }

            // Unavailable
            (PipelineStatus::Unavailable, PipelineDesiredStatus::Shutdown) => {
                State::TransitionToShuttingDown
            }
            (
                PipelineStatus::Unavailable,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => self.probe_initialized_pipeline(pipeline).await,

            // Failed
            (PipelineStatus::Failed, PipelineDesiredStatus::Shutdown) => {
                State::TransitionToShuttingDown
            }
            (PipelineStatus::Failed, PipelineDesiredStatus::Paused) => State::Unchanged,
            (PipelineStatus::Failed, PipelineDesiredStatus::Running) => State::Unchanged,

            // Shutting down
            (PipelineStatus::ShuttingDown, _) => {
                self.transit_shutting_down_to_shutdown(pipeline).await
            }
        };

        // Store the transition in the database
        if transition != State::Unchanged {
            debug!(
                "Performing database operation to store {transition:?} for pipeline {}...",
                pipeline.id
            );
        }
        let new_status = match transition {
            State::TransitionToProvisioning { deployment_config } => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_provisioning(
                        self.tenant_id,
                        pipeline.id,
                        deployment_config,
                    )
                    .await?;
                PipelineStatus::Provisioning
            }
            State::TransitionToInitializing {
                deployment_location,
            } => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_initializing(
                        self.tenant_id,
                        pipeline.id,
                        &deployment_location,
                    )
                    .await?;
                PipelineStatus::Initializing
            }
            State::TransitionToPaused => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_paused(self.tenant_id, pipeline.id)
                    .await?;
                PipelineStatus::Paused
            }
            State::TransitionToRunning => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_running(self.tenant_id, pipeline.id)
                    .await?;
                PipelineStatus::Running
            }
            State::TransitionToUnavailable => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_unavailable(self.tenant_id, pipeline.id)
                    .await?;
                PipelineStatus::Unavailable
            }
            State::TransitionToFailed { error } => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_failed(self.tenant_id, pipeline.id, &error)
                    .await?;
                PipelineStatus::Failed
            }
            State::TransitionToShuttingDown => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_shutting_down(self.tenant_id, pipeline.id)
                    .await?;
                PipelineStatus::ShuttingDown
            }
            State::TransitionToShutdown => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_shutdown(self.tenant_id, pipeline.id)
                    .await?;
                PipelineStatus::Shutdown
            }
            State::Unchanged => pipeline.deployment_status,
        };
        if pipeline.deployment_status != new_status {
            info!(
                "Transition: {} -> {} (desired: {}) for pipeline {}",
                pipeline.deployment_status,
                new_status,
                pipeline.deployment_desired_status,
                pipeline.id
            );
        }

        let poll_timeout = match pipeline.deployment_status {
            PipelineStatus::Shutdown => self.shutdown_poll_period,
            PipelineStatus::Provisioning => self.provisioning_poll_period,
            PipelineStatus::Initializing => Self::INITIALIZATION_POLL_PERIOD,
            PipelineStatus::Paused => Self::DEFAULT_PIPELINE_POLL_PERIOD,
            PipelineStatus::Running => Self::DEFAULT_PIPELINE_POLL_PERIOD,
            PipelineStatus::Unavailable => Self::DEFAULT_PIPELINE_POLL_PERIOD,
            PipelineStatus::ShuttingDown => self.shutdown_poll_period,
            PipelineStatus::Failed => Self::DEFAULT_PIPELINE_POLL_PERIOD,
        };
        Ok(poll_timeout)
    }

    /// Whether the time between now and the since timestamp has exceeded the timeout.
    fn has_timeout_expired(since: DateTime<Utc>, timeout: Duration) -> bool {
        Utc::now().timestamp_millis() - since.timestamp_millis() > timeout.as_millis() as i64
    }

    /// Parses `ErrorResponse` from JSON.
    /// Upon error, builds an `ErrorResponse` with the original JSON content.
    fn error_response_from_json(
        pipeline_id: PipelineId,
        status: StatusCode,
        json: &serde_json::Value,
    ) -> ErrorResponse {
        serde_json::from_value(json.clone()).unwrap_or_else(|_| {
            ErrorResponse::from(&RunnerError::PipelineEndpointInvalidResponse {
                pipeline_id,
                error: format!("Pipeline {pipeline_id} returned HTTP response which cannot be deserialized. Status code: {status}; body: {json:#}"),
            })
        })
    }

    /// Retrieves the deployment location from the descriptor.
    /// The location is expected to be there.
    /// Returns an error if the location is missing.
    fn get_required_deployment_location(
        pipeline: &ExtendedPipelineDescr,
    ) -> Result<String, RunnerError> {
        match pipeline.deployment_location.clone() {
            None => Err(RunnerError::PipelineMissingDeploymentLocation {
                pipeline_id: pipeline.id,
                pipeline_name: pipeline.name.clone(),
            }),
            Some(location) => Ok(location),
        }
    }

    /// Checks the pipeline status by attempting to poll its `/stats` endpoint.
    /// An error result is only returned if the response could not be parsed or
    /// contained an error.
    async fn check_pipeline_status(
        pipeline_id: PipelineId,
        deployment_location: String,
    ) -> StatusCheckResult {
        match automaton_http_request_to_pipeline_json(
            pipeline_id,
            Method::GET,
            "stats",
            &deployment_location,
        )
        .await
        {
            Ok((status, body)) => {
                // Able to reach the pipeline web server and get a response
                if status == StatusCode::OK {
                    // Only fatal errors are if the state cannot be retrieved or is not Paused/Running
                    let Some(global_metrics) = body.get("global_metrics") else {
                        return StatusCheckResult::Error(ErrorResponse::from_error_nolog(
                            &RunnerError::PipelineEndpointInvalidResponse {
                                pipeline_id,
                                error: format!(
                                    "Missing 'global_metrics' field in /stats response: {body}"
                                ),
                            },
                        ));
                    };
                    let Some(serde_json::Value::String(state)) = global_metrics.get("state") else {
                        return StatusCheckResult::Error(ErrorResponse::from_error_nolog(&RunnerError::PipelineEndpointInvalidResponse {
                            pipeline_id,
                            error: format!("Missing or non-string type of 'global_metrics.state' field in /stats response: {body}")
                        }));
                    };
                    if state == "Paused" {
                        StatusCheckResult::Paused
                    } else if state == "Running" {
                        StatusCheckResult::Running
                    } else {
                        // Notably: "Terminated"
                        return StatusCheckResult::Error(ErrorResponse::from_error_nolog(&RunnerError::PipelineEndpointInvalidResponse {
                            pipeline_id,
                            error: format!("Pipeline is not in Running or Paused state, but in {state} state")
                        }));
                    }
                } else if status == StatusCode::SERVICE_UNAVAILABLE {
                    // Pipeline HTTP server is running but indicates it is not yet available
                    warn!(
                        "Pipeline {} responds to status check it is not (yet) ready",
                        pipeline_id
                    );
                    StatusCheckResult::Unavailable
                } else {
                    // All other status codes indicate a fatal error
                    // The HTTP server is still running, but the pipeline itself failed
                    error!("Error response to status check for pipeline {}. Status: {status}. Body: {body}", pipeline_id);
                    StatusCheckResult::Error(Self::error_response_from_json(
                        pipeline_id,
                        status,
                        &body,
                    ))
                }
            }
            Err(e) => {
                debug!(
                    "Unable to reach pipeline {} for status check due to: {e}",
                    pipeline_id
                );
                StatusCheckResult::Unavailable
            }
        }
    }

    /// Transition from shutdown to paused or running state.
    async fn transit_shutdown_to_paused_or_running(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> Result<State, DBError> {
        // If the pipeline program errored during compilation, immediately transition to `Failed`
        if matches!(
            pipeline.program_status,
            ProgramStatus::SqlError(_)
                | ProgramStatus::RustError(_)
                | ProgramStatus::SystemError(_)
        ) {
            return Ok(State::TransitionToFailed {
                error: ErrorResponse::from_error_nolog(&DBError::StartFailedDueToFailedCompilation),
            });
        }

        // The runner is unable to run a pipeline program compiled under an outdated platform.
        // As such, it requests the compiler to recompile it again by setting the program_status back to `Pending`.
        // The runner is able to do this as it got ownership of the pipeline when the user set the desired deployment status to `Running`/`Paused`.
        // It does not do the platform version bump by itself, because it is the compiler's responsibility
        // to generate only binaries that are of the current platform version.
        if self.platform_version != pipeline.platform_version
            && pipeline.program_status == ProgramStatus::Success
        {
            info!("Runner re-initiates program compilation of pipeline {} because its platform version ({}) is outdated by current ({})", pipeline.id, pipeline.platform_version, self.platform_version);
            self.db
                .lock()
                .await
                .transit_program_status_to_pending(
                    self.tenant_id,
                    pipeline.id,
                    pipeline.program_version,
                )
                .await?;
            return Ok(State::Unchanged);
        }

        // Early start: await ongoing compilation
        if pipeline.program_status == ProgramStatus::Pending
            || pipeline.program_status == ProgramStatus::CompilingSql
            || pipeline.program_status == ProgramStatus::SqlCompiled
            || pipeline.program_status == ProgramStatus::CompilingRust
        {
            return Ok(State::Unchanged);
        }

        // All other program statuses (errors and ongoing) have already been checked for,
        // as such it must be successfully compiled at this point, and as well for the
        // current platform. The program status will not change anymore till the pipeline
        // is shutdown, because the runner will not do so and is the only one who can initiate
        // recompilation if the pipeline is not fully shutdown and the program status is success.
        assert_eq!(pipeline.program_status, ProgramStatus::Success);
        assert_eq!(self.platform_version, pipeline.platform_version);

        // Input and output connectors
        let (inputs, outputs) = match pipeline.program_info.clone() {
            None => {
                return Ok(State::TransitionToFailed {
                    error: ErrorResponse::from_error_nolog(
                        &RunnerError::PipelineMissingProgramInfo {
                            pipeline_name: pipeline.name.clone(),
                            pipeline_id: pipeline.id,
                        },
                    ),
                });
            }
            Some(program_info) => (
                program_info.input_connectors,
                program_info.output_connectors,
            ),
        };

        // Deployment configuration
        let mut deployment_config =
            generate_pipeline_config(pipeline.id, &pipeline.runtime_config, &inputs, &outputs);
        deployment_config.storage_config = if deployment_config.global.storage {
            Some(self.pipeline_handle.generate_storage_config().await)
        } else {
            None
        };

        Ok(State::TransitionToProvisioning { deployment_config })
    }

    /// Start and await the pipeline provisioning.
    async fn transit_provisioning_to_paused_or_running(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        // Deployment configuration and program binary URL are expected to be set
        let deployment_config = match pipeline.deployment_config.clone() {
            None => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from_error_nolog(
                        &RunnerError::PipelineMissingDeploymentConfig {
                            pipeline_id: pipeline.id,
                            pipeline_name: pipeline.name.clone(),
                        },
                    ),
                }
            }
            Some(deployment_config) => deployment_config,
        };
        let program_binary_url = match pipeline.program_binary_url.clone() {
            None => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from_error_nolog(
                        &RunnerError::PipelineMissingProgramBinaryUrl {
                            pipeline_id: pipeline.id,
                            pipeline_name: pipeline.name.clone(),
                        },
                    ),
                }
            }
            Some(program_binary_url) => program_binary_url,
        };

        // Call `provision()` once and wait for provisioning to finish.
        // The call is idempotent, as such it can be called again if the
        // runner unexpectedly restarts and the `provision_called` boolean
        // is reset.
        if !self.provision_called {
            match self
                .pipeline_handle
                .provision(
                    &deployment_config,
                    &program_binary_url,
                    pipeline.program_version,
                )
                .await
            {
                Ok(()) => {
                    self.provision_called = true;
                    info!(
                        "Provisioning pipeline {} (tenant: {})",
                        self.pipeline_id, self.tenant_id
                    );
                    State::Unchanged
                }
                Err(e) => State::TransitionToFailed {
                    error: ErrorResponse::from_error_nolog(&e),
                },
            }
        } else {
            match self.pipeline_handle.is_provisioned().await {
                Ok(Some(location)) => State::TransitionToInitializing {
                    deployment_location: location,
                },
                Ok(None) => {
                    debug!(
                        "Pipeline provisioning: pipeline {} is not yet provisioned",
                        pipeline.id
                    );
                    if Self::has_timeout_expired(
                        pipeline.deployment_status_since,
                        self.provisioning_timeout,
                    ) {
                        error!(
                            "Pipeline provisioning: timed out for pipeline {}",
                            pipeline.id
                        );
                        State::TransitionToFailed {
                            error: RunnerError::PipelineProvisioningTimeout {
                                pipeline_id: self.pipeline_id,
                                timeout: self.provisioning_timeout,
                            }
                            .into(),
                        }
                    } else {
                        State::Unchanged
                    }
                }
                Err(e) => {
                    error!(
                        "Pipeline provisioning: error occurred for pipeline {}: {e}",
                        pipeline.id
                    );
                    State::TransitionToFailed {
                        error: ErrorResponse::from_error_nolog(&e),
                    }
                }
            }
        }
    }

    /// Awaiting the initialization to finish.
    /// The pipeline web server is polled at an interval.
    /// The pipeline transitions to failure upon timeout or error.
    async fn transit_initializing_to_paused_or_running(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        // Check deployment when initialized
        if let Err(e) = self.pipeline_handle.check().await {
            return State::TransitionToFailed {
                error: ErrorResponse::from_error_nolog(&e),
            };
        }

        // Probe pipeline
        let deployment_location = match Self::get_required_deployment_location(pipeline) {
            Ok(deployment_location) => deployment_location,
            Err(e) => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from_error_nolog(&e),
                };
            }
        };
        match Self::check_pipeline_status(pipeline.id, deployment_location).await {
            StatusCheckResult::Paused => State::TransitionToPaused,
            StatusCheckResult::Running => {
                // After initialization, it should not become running automatically
                State::TransitionToFailed {
                    error: RunnerError::PipelineAfterInitializationBecameRunning {
                        pipeline_id: self.pipeline_id,
                    }
                    .into(),
                }
            }
            StatusCheckResult::Unavailable => {
                debug!(
                    "Pipeline initialization: could not (yet) connect to pipeline {}",
                    pipeline.id
                );
                if Self::has_timeout_expired(
                    pipeline.deployment_status_since,
                    Self::INITIALIZATION_TIMEOUT,
                ) {
                    error!(
                        "Pipeline initialization: timed out for pipeline {}",
                        pipeline.id
                    );
                    State::TransitionToFailed {
                        error: RunnerError::PipelineInitializingTimeout {
                            pipeline_id: self.pipeline_id,
                            timeout: Self::INITIALIZATION_TIMEOUT,
                        }
                        .into(),
                    }
                } else {
                    State::Unchanged
                }
            }
            StatusCheckResult::Error(error) => State::TransitionToFailed { error },
        }
    }

    /// Transit the pipeline from paused to running or running to paused by issuing an HTTP request.
    /// Issues `/start` if `is_start` is true, and `/pause` if it is false.
    /// The action request result determines which state transition needs to occur.
    async fn perform_action_initialized_pipeline(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
        is_start: bool,
    ) -> State {
        let deployment_location = match Self::get_required_deployment_location(pipeline) {
            Ok(deployment_location) => deployment_location,
            Err(e) => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from_error_nolog(&e),
                };
            }
        };

        // Check deployment when initialized
        if let Err(e) = self.pipeline_handle.check().await {
            return State::TransitionToFailed {
                error: ErrorResponse::from_error_nolog(&e),
            };
        }

        // Issue request to the /start or /pause endpoint
        let action = if is_start { "start" } else { "pause" };
        match automaton_http_request_to_pipeline_json(
            self.pipeline_id,
            Method::GET,
            action,
            &deployment_location,
        )
        .await
        {
            Ok((status, body)) => {
                if status == StatusCode::OK {
                    if is_start {
                        State::TransitionToRunning
                    } else {
                        State::TransitionToPaused
                    }
                } else if status == StatusCode::SERVICE_UNAVAILABLE {
                    warn!("Unable to perform action '{action}' on pipeline {} because pipeline indicated it is not (yet) ready", pipeline.id);
                    State::TransitionToUnavailable
                } else {
                    error!("Error response to action '{action}' on pipeline {}. Status: {status}. Body: {body}", pipeline.id);
                    State::TransitionToFailed {
                        error: Self::error_response_from_json(self.pipeline_id, status, &body),
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Unable to reach pipeline {} to perform action '{action}' due to: {e}",
                    pipeline.id
                );
                State::TransitionToUnavailable
            }
        }
    }

    /// Probes a pipeline which is past the Initializing status, hence is
    /// either Paused, Running or Unavailable. The probe result determines
    /// whether and, if so, which state transition needs to occur.
    async fn probe_initialized_pipeline(&mut self, pipeline: &ExtendedPipelineDescr) -> State {
        // Check deployment when initialized
        if let Err(e) = self.pipeline_handle.check().await {
            return State::TransitionToFailed {
                error: ErrorResponse::from_error_nolog(&e),
            };
        }

        // Perform probe
        let deployment_location = match Self::get_required_deployment_location(pipeline) {
            Ok(deployment_location) => deployment_location,
            Err(e) => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from_error_nolog(&e),
                };
            }
        };
        match Self::check_pipeline_status(pipeline.id, deployment_location).await {
            StatusCheckResult::Paused => {
                if pipeline.deployment_status == PipelineStatus::Paused {
                    State::Unchanged
                } else {
                    // Possible mismatch: pipeline reports Paused, database reports Running
                    //
                    // It is possible for the pipeline endpoint /pause to have been called,
                    // and the automaton being terminated before the database has stored the
                    // new status. If then API endpoint /v0/pipelines/{name}/start is called
                    // before the automaton starts up, this case will occur. In that case, we
                    // transition to paused such that the automaton tries again to start.
                    State::TransitionToPaused
                }
            }
            StatusCheckResult::Running => {
                if pipeline.deployment_status == PipelineStatus::Running {
                    State::Unchanged
                } else {
                    // The same possible mismatch as above can occur but the other way around
                    State::TransitionToRunning
                }
            }
            StatusCheckResult::Unavailable => {
                if pipeline.deployment_status == PipelineStatus::Unavailable {
                    State::Unchanged
                } else {
                    State::TransitionToUnavailable
                }
            }
            StatusCheckResult::Error(error) => State::TransitionToFailed { error },
        }
    }

    /// Shuts the pipeline down by terminating and deleting underlying runtime resources.
    /// The runner should always be able to do this, irrespective of the state the
    /// pipeline or its underlying runtime resources are in.
    async fn transit_shutting_down_to_shutdown(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        if let Err(e) = self.pipeline_handle.shutdown().await {
            error!("Pipeline {} could not be shutdown: {e}", pipeline.id);
            State::Unchanged
        } else {
            self.provision_called = false;
            State::TransitionToShutdown
        }
    }
}

#[cfg(test)]
mod test {
    use crate::auth::TenantRecord;
    use crate::db::storage::Storage;
    use crate::db::storage_postgres::StoragePostgres;
    use crate::db::types::common::Version;
    use crate::db::types::pipeline::{PipelineDescr, PipelineId, PipelineStatus};
    use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramInfo};
    use crate::error::ManagerError;
    use crate::logging;
    use crate::runner::pipeline_automata::PipelineAutomaton;
    use crate::runner::pipeline_executor::{LogMessage, PipelineExecutor};
    use async_trait::async_trait;
    use feldera_types::config::{PipelineConfig, RuntimeConfig, StorageConfig};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::{Receiver, Sender};
    use tokio::sync::{Mutex, Notify};
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct MockPipeline {
        uri: String,
    }

    #[async_trait]
    impl PipelineExecutor for MockPipeline {
        type Config = ();
        const PROVISIONING_TIMEOUT: Duration = Duration::from_millis(1);
        const PROVISIONING_POLL_PERIOD: Duration = Duration::from_millis(1);
        const SHUTDOWN_POLL_PERIOD: Duration = Duration::from_millis(1);

        fn new(
            _pipeline_id: PipelineId,
            _config: Self::Config,
            _follow_request_receiver: Receiver<Sender<LogMessage>>,
        ) -> Self {
            todo!()
        }

        async fn generate_storage_config(&self) -> StorageConfig {
            StorageConfig {
                path: "".to_string(),
                cache: Default::default(),
            }
        }

        async fn init(&mut self, _was_provisioned: bool) {
            // Nothing to implement
        }

        async fn provision(
            &mut self,
            _: &PipelineConfig,
            _: &str,
            _: Version,
        ) -> Result<(), ManagerError> {
            Ok(())
        }

        async fn is_provisioned(&self) -> Result<Option<String>, ManagerError> {
            Ok(Some(self.uri.clone()))
        }

        async fn check(&mut self) -> Result<(), ManagerError> {
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), ManagerError> {
            Ok(())
        }
    }

    struct AutomatonTest {
        db: Arc<Mutex<StoragePostgres>>,
        automaton: PipelineAutomaton<MockPipeline>,
    }

    impl AutomatonTest {
        async fn set_desired_state(&self, status: PipelineStatus) {
            let automaton = &self.automaton;
            let pipeline = self
                .db
                .lock()
                .await
                .get_pipeline_by_id(automaton.tenant_id, automaton.pipeline_id)
                .await
                .unwrap();
            match status {
                PipelineStatus::Shutdown => {
                    self.db
                        .lock()
                        .await
                        .set_deployment_desired_status_shutdown(automaton.tenant_id, &pipeline.name)
                        .await
                        .unwrap();
                }
                PipelineStatus::Paused => {
                    self.db
                        .lock()
                        .await
                        .set_deployment_desired_status_paused(automaton.tenant_id, &pipeline.name)
                        .await
                        .unwrap();
                }
                PipelineStatus::Running => {
                    self.db
                        .lock()
                        .await
                        .set_deployment_desired_status_running(automaton.tenant_id, &pipeline.name)
                        .await
                        .unwrap();
                }
                _ => panic!("Invalid desired status"),
            }
        }

        async fn check_current_state(&self, status: PipelineStatus) {
            let automaton = &self.automaton;
            let pipeline = self
                .db
                .lock()
                .await
                .get_pipeline_by_id(automaton.tenant_id, automaton.pipeline_id)
                .await
                .unwrap();
            assert_eq!(
                status, pipeline.deployment_status,
                "Status does not match; deployment_error: {:?}",
                pipeline.deployment_error
            );
        }

        async fn tick(&mut self) {
            self.automaton.do_run().await.unwrap();
        }
    }

    async fn setup(db: Arc<Mutex<StoragePostgres>>, uri: String) -> AutomatonTest {
        // Create a pipeline and a corresponding automaton
        let tenant_id = TenantRecord::default().id;
        let pipeline_id = PipelineId(Uuid::now_v7());
        let _ = db
            .lock()
            .await
            .new_pipeline(
                tenant_id,
                pipeline_id.0,
                "v0",
                PipelineDescr {
                    name: "example1".to_string(),
                    description: "Description of example1".to_string(),
                    runtime_config: RuntimeConfig::from_yaml(""),
                    program_code: "CREATE TABLE example1 ( col1 INT );".to_string(),
                    udf_rust: "".to_string(),
                    udf_toml: "".to_string(),
                    program_config: ProgramConfig {
                        profile: Some(CompilationProfile::Unoptimized),
                        cache: false,
                    },
                },
            )
            .await
            .unwrap();

        // Transition the pipeline program to success
        db.lock()
            .await
            .transit_program_status_to_compiling_sql(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_sql_compiled(
                tenant_id,
                pipeline_id,
                Version(1),
                &ProgramInfo::default(),
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_compiling_rust(tenant_id, pipeline_id, Version(1))
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_success(
                tenant_id,
                pipeline_id,
                Version(1),
                "not-used-program-binary-source-checksum",
                "not-used-program-binary-integrity-checksum",
                "not-used-program-binary-url",
            )
            .await
            .unwrap();

        // Construct the automaton
        let notifier = Arc::new(Notify::new());
        let automaton = PipelineAutomaton::new(
            "v0",
            pipeline_id,
            tenant_id,
            db.clone(),
            notifier.clone(),
            MockPipeline { uri },
            Duration::from_secs(10),
            Duration::from_millis(300),
            Duration::from_millis(300),
        );
        AutomatonTest {
            db: db.clone(),
            automaton,
        }
    }

    async fn mock_endpoint(
        server: &mut MockServer,
        endpoint: &str,
        code: u16,
        json_body: serde_json::Value,
    ) {
        server.reset().await;
        let template = ResponseTemplate::new(code).set_body_json(json_body);
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(template)
            .mount(server)
            .await;
    }

    #[cfg(feature = "pg-embed")]
    type SetupResult = (MockServer, tempfile::TempDir, AutomatonTest);
    #[cfg(not(feature = "pg-embed"))]
    type SetupResult = (MockServer, tokio_postgres::Config, AutomatonTest);

    async fn setup_complete() -> SetupResult {
        logging::init_logging("foo".into());
        let (db, temp_dir) = crate::db::test::setup_pg().await;
        let db = Arc::new(Mutex::new(db));
        // Start a background HTTP server on a random local port
        let mock_server = MockServer::start().await;
        let addr = mock_server.address().to_string();
        (mock_server, temp_dir, setup(db.clone(), addr).await)
    }

    #[tokio::test]
    async fn start_paused() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineStatus::Paused).await;
        test.check_current_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        mock_endpoint(
            &mut server,
            "/stats",
            200,
            json!({ "global_metrics": { "state": "Paused" } }),
        )
        .await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.set_desired_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::ShuttingDown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Shutdown).await;
    }

    #[tokio::test]
    async fn start_running() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineStatus::Running).await;
        test.check_current_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        mock_endpoint(
            &mut server,
            "/stats",
            200,
            json!({ "global_metrics": { "state": "Paused" } }),
        )
        .await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        mock_endpoint(&mut server, "/start", 200, json!({})).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Running).await;
        mock_endpoint(
            &mut server,
            "/stats",
            200,
            json!({ "global_metrics": { "state": "Running" } }),
        )
        .await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Running).await;
        test.set_desired_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::ShuttingDown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Shutdown).await;
    }

    #[tokio::test]
    async fn start_paused_then_running() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineStatus::Paused).await;
        test.check_current_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        mock_endpoint(
            &mut server,
            "/stats",
            200,
            json!({ "global_metrics": { "state": "Paused" } }),
        )
        .await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.set_desired_state(PipelineStatus::Running).await;
        mock_endpoint(&mut server, "/start", 200, json!({})).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Running).await;
        mock_endpoint(
            &mut server,
            "/stats",
            200,
            json!({ "global_metrics": { "state": "Running" } }),
        )
        .await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Running).await;
        test.set_desired_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::ShuttingDown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Shutdown).await;
    }

    #[tokio::test]
    async fn shutdown_provisioning() {
        let (_mock_server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineStatus::Paused).await;
        test.check_current_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.set_desired_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::ShuttingDown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Shutdown).await;
    }

    #[tokio::test]
    async fn shutdown_initializing() {
        let (_mock_server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineStatus::Paused).await;
        test.check_current_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        test.set_desired_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::ShuttingDown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Shutdown).await;
    }
}
