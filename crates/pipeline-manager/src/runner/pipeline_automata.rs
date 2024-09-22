use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::common::Version;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDesiredStatus, PipelineId, PipelineStatus,
};
use crate::db::types::program::generate_pipeline_config;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::interaction::RunnerInteraction;
use actix_web::http::{Method, StatusCode};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use feldera_types::config::PipelineConfig;
use feldera_types::error::ErrorResponse;
use log::{debug, error, info};
use std::sync::Arc;
use tokio::{sync::Mutex, time::Duration};
use tokio::{sync::Notify, time::timeout};

/// Timeout for a HTTP request of the automata to a pipeline.
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

/// A description of a pipeline to execute.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PipelineExecutionDesc {
    pub pipeline_id: PipelineId,
    pub pipeline_name: String,
    pub program_version: Version,
    pub program_binary_url: String,
    pub deployment_config: PipelineConfig,
}

/// Trait to be implemented by any pipeline runner.
/// The `PipelineAutomaton` invokes these methods per pipeline.
#[async_trait]
pub trait PipelineExecutor: Sync + Send {
    /// Converts an extended pipeline descriptor retrieved from the database
    /// into a execution descriptor which has no optional fields.
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

        let deployment_config =
            generate_pipeline_config(pipeline.id, &pipeline.runtime_config, &inputs, &outputs);
        Ok(PipelineExecutionDesc {
            pipeline_id: pipeline.id,
            pipeline_name: pipeline.name.clone(),
            program_version: pipeline.program_version,
            program_binary_url,
            deployment_config,
        })
    }

    /// Starts a new pipeline
    /// (e.g., brings up a process that runs the pipeline binary)
    async fn start(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError>;

    /// Returns the hostname:port over which the pipeline's HTTP server should be
    /// reachable. `Ok(None)` indicates that the pipeline is still initializing.
    async fn get_location(&mut self) -> Result<Option<String>, ManagerError>;

    /// Initiates pipeline shutdown
    /// (e.g., send a SIGTERM successfully to the process).
    async fn shutdown(&mut self) -> Result<(), ManagerError>;

    /// Returns whether the pipeline has been shutdown.
    async fn check_if_shutdown(&mut self) -> bool;
}

/// Utility type for the pipeline automaton to describe state changes.
enum State {
    TransitionToShutdown,
    TransitionToProvisioning { deployment_config: PipelineConfig },
    TransitionToInitializing { deployment_location: String },
    TransitionToPaused,
    TransitionToRunning,
    TransitionToShuttingDown,
    TransitionToFailed { error: ErrorResponse },
    Unchanged,
}

/// Pipeline automaton monitors the runtime state of a single pipeline
/// and continually reconciles actual with desired state.
///
/// The automaton runs as a separate tokio task.
pub struct PipelineAutomaton<T>
where
    T: PipelineExecutor,
{
    pipeline_id: PipelineId,
    tenant_id: TenantId,
    pipeline_handle: T,
    db: Arc<Mutex<StoragePostgres>>,
    notifier: Arc<Notify>,

    /// Maximum time to wait for the pipeline instance to be provisioned.
    /// This can differ significantly between the type of runner.
    provisioning_timeout: Duration,

    /// How often to poll during provisioning.
    provisioning_poll_period: Duration,

    /// Maximum time to wait for the pipeline instance to shutdown.
    /// This can differ significantly between the type of runner.
    shutdown_timeout: Duration,

    /// How often to poll during shutting down.
    shutdown_poll_period: Duration,
}

impl<T: PipelineExecutor> PipelineAutomaton<T> {
    /// The frequency of polling the pipeline during normal operation
    /// when we don't normally expect its state to change.
    const DEFAULT_PIPELINE_POLL_PERIOD: Duration = Duration::from_millis(10_000);

    /// Maximum time to wait for the pipeline to initialize its connectors and web server.
    const INITIALIZATION_TIMEOUT: Duration = Duration::from_millis(60_000);

    /// How often to poll the pipeline during initialization.
    const INITIALIZATION_POLL_PERIOD: Duration = Duration::from_millis(300);

    /// Creates a new automaton for a given pipeline.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline_id: PipelineId,
        tenant_id: TenantId,
        db: Arc<Mutex<StoragePostgres>>,
        notifier: Arc<Notify>,
        pipeline_handle: T,
        provisioning_timeout: Duration,
        provisioning_poll_period: Duration,
        shutdown_timeout: Duration,
        shutdown_poll_period: Duration,
    ) -> Self {
        Self {
            pipeline_id,
            tenant_id,
            pipeline_handle,
            db,
            notifier,
            provisioning_timeout,
            provisioning_poll_period,
            shutdown_timeout,
            shutdown_poll_period,
        }
    }

    /// Runs until the pipeline is deleted or an unexpected error occurs.
    pub async fn run(mut self) -> Result<(), ManagerError> {
        let pipeline_id = self.pipeline_id;
        let mut poll_timeout = Self::DEFAULT_PIPELINE_POLL_PERIOD;
        loop {
            // Wait until the timeout expires or we get notified that the
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
                    // which in turn will shutdown by itself as a consequence.
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
            ) => self.transit_shutdown_to_paused_or_running(pipeline).await,

            // Provisioning
            (PipelineStatus::Provisioning, PipelineDesiredStatus::Shutdown) => {
                self.transit_provisioning_initializing_paused_running_to_shutdown(pipeline)
                    .await
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
                self.transit_provisioning_initializing_paused_running_to_shutdown(pipeline)
                    .await
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
                self.transit_provisioning_initializing_paused_running_to_shutdown(pipeline)
                    .await
            }
            (PipelineStatus::Paused, PipelineDesiredStatus::Paused) => {
                self.probe_paused_or_running(pipeline).await
            }
            (PipelineStatus::Paused, PipelineDesiredStatus::Running) => {
                self.transit_paused_to_running(pipeline).await
            }

            // Running
            (PipelineStatus::Running, PipelineDesiredStatus::Shutdown) => {
                self.transit_provisioning_initializing_paused_running_to_shutdown(pipeline)
                    .await
            }
            (PipelineStatus::Running, PipelineDesiredStatus::Paused) => {
                self.transit_running_to_paused(pipeline).await
            }
            (PipelineStatus::Running, PipelineDesiredStatus::Running) => {
                self.probe_paused_or_running(pipeline).await
            }

            // Shutting down
            (PipelineStatus::ShuttingDown, PipelineDesiredStatus::Shutdown) => {
                self.transit_shutting_down_to_shutdown(pipeline).await
            }
            (PipelineStatus::ShuttingDown, PipelineDesiredStatus::Paused) => State::Unchanged,
            (PipelineStatus::ShuttingDown, PipelineDesiredStatus::Running) => State::Unchanged,

            // Failed
            (PipelineStatus::Failed, PipelineDesiredStatus::Shutdown) => {
                self.transit_failed_to_shutdown().await
            }
            (PipelineStatus::Failed, PipelineDesiredStatus::Paused) => State::Unchanged,
            (PipelineStatus::Failed, PipelineDesiredStatus::Running) => State::Unchanged,
        };

        // Store the transition in the database
        let new_status = match transition {
            State::TransitionToShutdown => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_shutdown(self.tenant_id, pipeline.id)
                    .await?;
                PipelineStatus::Shutdown
            }
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
            State::TransitionToShuttingDown => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_shutting_down(self.tenant_id, pipeline.id)
                    .await?;
                PipelineStatus::ShuttingDown
            }
            State::TransitionToFailed { error } => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_failed(self.tenant_id, pipeline.id, &error)
                    .await?;
                PipelineStatus::Failed
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

    /// Transition from shutdown to paused or running state.
    async fn transit_shutdown_to_paused_or_running(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        let execution_desc = match self.pipeline_handle.to_execution_desc(pipeline).await {
            Ok(execution_desc) => execution_desc,
            Err(e) => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from(&e),
                }
            }
        };

        // This requires start() to be idempotent. If the pipeline instance crashes after
        // start is called but before the state machine is correctly updated in the
        // database, then on restart, start will be called again
        match self.pipeline_handle.start(execution_desc.clone()).await {
            Ok(()) => {
                info!(
                    "Started pipeline {} (tenant: {})",
                    self.pipeline_id, self.tenant_id
                );
                State::TransitionToProvisioning {
                    deployment_config: execution_desc.deployment_config.clone(),
                }
            }
            Err(e) => State::TransitionToFailed {
                error: ErrorResponse::from(&e),
            },
        }
    }

    /// Awaiting the pipeline to be provisioned. Continuously poll for a location,
    /// which is the indicator that the pipeline has been provisioned somewhere.
    async fn transit_provisioning_to_paused_or_running(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        match self.pipeline_handle.get_location().await {
            Ok(Some(location)) => State::TransitionToInitializing {
                deployment_location: location,
            },
            Ok(None) => {
                if Self::has_timeout_expired(
                    pipeline.deployment_status_since,
                    self.provisioning_timeout,
                ) {
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
            Err(e) => State::TransitionToFailed {
                error: ErrorResponse::from(&e),
            },
        }
    }

    /// Awaiting for the initialization to finish.
    /// The pipeline web server is polled at an interval.
    /// The pipeline transitions to failure upon timeout or error.
    async fn transit_initializing_to_paused_or_running(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        let location = match pipeline.deployment_location.clone() {
            None => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from(RunnerError::PipelineMissingDeploymentLocation {
                        pipeline_id: pipeline.id,
                        pipeline_name: pipeline.name.clone(),
                    }),
                };
            }
            Some(location) => location,
        };

        match automaton_http_request_to_pipeline_json(
            self.pipeline_id,
            Method::GET,
            "stats",
            &location,
        )
        .await
        {
            Ok((status, body)) => {
                if status == StatusCode::OK {
                    State::TransitionToPaused
                } else if status == StatusCode::SERVICE_UNAVAILABLE {
                    if Self::has_timeout_expired(
                        pipeline.deployment_status_since,
                        Self::INITIALIZATION_TIMEOUT,
                    ) {
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
                } else {
                    State::TransitionToFailed {
                        error: Self::error_response_from_json(self.pipeline_id, status, &body),
                    }
                }
            }
            Err(e) => {
                debug!(
                    "Pipeline initialization: could not (yet) connect to pipeline {}: {}",
                    pipeline.id, e
                );
                if Self::has_timeout_expired(
                    pipeline.deployment_status_since,
                    Self::INITIALIZATION_TIMEOUT,
                ) {
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
        }
    }

    /// Transit the pipeline from paused to running by issuing a HTTP request.
    async fn transit_paused_to_running(&mut self, pipeline: &ExtendedPipelineDescr) -> State {
        let location = match pipeline.deployment_location.clone() {
            None => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from(RunnerError::PipelineMissingDeploymentLocation {
                        pipeline_id: pipeline.id,
                        pipeline_name: pipeline.name.clone(),
                    }),
                };
            }
            Some(location) => location,
        };
        match automaton_http_request_to_pipeline_json(
            self.pipeline_id,
            Method::GET,
            "start",
            &location,
        )
        .await
        {
            Ok((status, body)) => {
                if status == StatusCode::OK {
                    State::TransitionToRunning
                } else {
                    State::TransitionToFailed {
                        error: Self::error_response_from_json(self.pipeline_id, status, &body),
                    }
                }
            }
            Err(e) => State::TransitionToFailed {
                error: ErrorResponse::from_error_nolog(&RunnerError::PipelineUnreachable {
                    original_error: e.to_string(),
                }),
            },
        }
    }

    /// Transit the pipeline from running to paused by issuing a HTTP request.
    async fn transit_running_to_paused(&mut self, pipeline: &ExtendedPipelineDescr) -> State {
        let location = match pipeline.deployment_location.clone() {
            None => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from(RunnerError::PipelineMissingDeploymentLocation {
                        pipeline_id: pipeline.id,
                        pipeline_name: pipeline.name.clone(),
                    }),
                };
            }
            Some(location) => location,
        };

        match automaton_http_request_to_pipeline_json(
            self.pipeline_id,
            Method::GET,
            "pause",
            &location,
        )
        .await
        {
            Ok((status, body)) => {
                if status.is_success() {
                    State::TransitionToPaused
                } else {
                    State::TransitionToFailed {
                        error: Self::error_response_from_json(self.pipeline_id, status, &body),
                    }
                }
            }
            Err(e) => State::TransitionToFailed {
                error: ErrorResponse::from_error_nolog(&RunnerError::PipelineUnreachable {
                    original_error: e.to_string(),
                }),
            },
        }
    }

    /// Transit the pipeline from provisioning, initializing, paused or running towards shutdown.
    async fn transit_provisioning_initializing_paused_running_to_shutdown(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        match self.pipeline_handle.shutdown().await {
            Ok(()) => State::TransitionToShuttingDown,
            Err(e) => State::TransitionToFailed {
                error: RunnerError::PipelineShutdownError {
                    pipeline_id: pipeline.id,
                    error: e.to_string(),
                }
                .into(),
            },
        }
    }

    /// Periodically poll a paused or running pipeline.
    /// Steady-state operation.
    async fn probe_paused_or_running(&mut self, pipeline: &ExtendedPipelineDescr) -> State {
        let location = match pipeline.deployment_location.clone() {
            None => {
                return State::TransitionToFailed {
                    error: ErrorResponse::from(RunnerError::PipelineMissingDeploymentLocation {
                        pipeline_id: pipeline.id,
                        pipeline_name: pipeline.name.clone(),
                    }),
                };
            }
            Some(location) => location,
        };

        match automaton_http_request_to_pipeline_json(
            self.pipeline_id,
            Method::GET,
            "stats",
            &location,
        )
        .await
        {
            Ok((status, body)) => {
                if !status.is_success() {
                    // Pipeline responds with an error, meaning that the pipeline
                    // HTTP server is still running, but the pipeline itself failed.
                    State::TransitionToFailed {
                        error: Self::error_response_from_json(self.pipeline_id, status, &body),
                    }
                } else {
                    // The pipeline response must have a field global_metrics.state which the
                    // pipeline uses to communicate its current status
                    let global_metrics = if let Some(metrics) = body.get("global_metrics") {
                        metrics
                    } else {
                        return State::TransitionToFailed {
                            error: ErrorResponse::from(RunnerError::PipelineEndpointInvalidResponse {
                                pipeline_id: self.pipeline_id,
                                error: format!("Pipeline status descriptor doesn't contain 'global_metrics' field: '{body}'")
                            })
                        };
                    };
                    let json_state = if let Some(state) = global_metrics.get("state") {
                        state
                    } else {
                        return State::TransitionToFailed {
                            error: ErrorResponse::from(RunnerError::PipelineEndpointInvalidResponse {
                                pipeline_id: self.pipeline_id,
                                error: format!("Pipeline status descriptor doesn't contain 'global_metrics.state' field: '{body}'")
                            })
                        };
                    };
                    let state = if let Some(state) = json_state.as_str() {
                        state
                    } else {
                        return State::TransitionToFailed {
                            error: ErrorResponse::from(RunnerError::PipelineEndpointInvalidResponse {
                                pipeline_id: self.pipeline_id,
                                error: format!("Pipeline status descriptor contains invalid 'global_metrics.state' field: '{body}'")
                            })
                        };
                    };

                    // Check that the found state matches what is known to the runner
                    if state == "Paused" && pipeline.deployment_status == PipelineStatus::Running {
                        // Mismatch: pipeline reports Paused, database reports Running
                        //
                        // It is possible for the pipeline endpoint /pause to have been called,
                        // and the automaton being terminated before the database has stored the
                        // new status. If then API endpoint /v0/pipelines/{name}/start is called
                        // before the automaton starts up, this case will occur. In that case, we
                        // transition to paused such that the automaton tries again to start.
                        State::TransitionToPaused
                    } else if state == "Running"
                        && pipeline.deployment_status == PipelineStatus::Paused
                    {
                        // The same mismatch reasoning as above but the other way around
                        State::TransitionToRunning
                    } else if state != "Paused" && state != "Running" {
                        State::TransitionToFailed {
                            error: RunnerError::PipelineEndpointInvalidResponse {
                                pipeline_id: self.pipeline_id,
                                error: format!("Pipeline reported unexpected status '{state}', expected 'Paused' or 'Running'")
                            }.into()}
                    } else {
                        State::Unchanged
                    }
                }
            }
            Err(e) => {
                // Cannot reach the pipeline.
                error!(
                    "Failed to probe {} pipeline which thus will become failed (pipeline: {})",
                    pipeline.deployment_status, pipeline.id
                );
                State::TransitionToFailed {
                    error: ErrorResponse::from_error_nolog(&RunnerError::PipelineUnreachable {
                        original_error: e.to_string(),
                    }),
                }
            }
        }
    }

    /// Shutdown is in progress. Wait for the pipeline instance to terminate with a timeout.
    async fn transit_shutting_down_to_shutdown(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        if self.pipeline_handle.check_if_shutdown().await {
            State::TransitionToShutdown
        } else if Self::has_timeout_expired(pipeline.deployment_status_since, self.shutdown_timeout)
        {
            State::TransitionToFailed {
                error: RunnerError::PipelineShutdownTimeout {
                    pipeline_id: self.pipeline_id,
                    timeout: self.shutdown_timeout,
                }
                .into(),
            }
        } else {
            State::Unchanged
        }
    }

    /// User acknowledges pipeline failure by invoking the `/shutdown` endpoint.
    /// Make a last effort to shutdown the pipeline, and transition to `Shutdown` state
    /// such that the pipeline can be started again.
    async fn transit_failed_to_shutdown(&mut self) -> State {
        // TODO: the runner should be authoritative on deciding whether a pipeline
        //       is sufficiently shutdown
        if let Err(e) = self.pipeline_handle.shutdown().await {
            error!(
                "Pipeline shutdown: shutdown operation from Failed status was not successful: {e}"
            );
        }
        State::TransitionToShutdown
    }
}

#[cfg(test)]
mod test {
    use super::{PipelineExecutionDesc, PipelineExecutor};
    use crate::auth::TenantRecord;
    use crate::db::storage::Storage;
    use crate::db::storage_postgres::StoragePostgres;
    use crate::db::types::common::Version;
    use crate::db::types::pipeline::{PipelineDescr, PipelineId, PipelineStatus};
    use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramInfo};
    use crate::error::ManagerError;
    use crate::logging;
    use crate::runner::pipeline_automata::PipelineAutomaton;
    use async_trait::async_trait;
    use feldera_types::config::RuntimeConfig;
    use feldera_types::program_schema::ProgramSchema;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Mutex, Notify};
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct MockPipeline {
        uri: String,
    }

    #[async_trait]
    impl PipelineExecutor for MockPipeline {
        async fn start(&mut self, _ped: PipelineExecutionDesc) -> Result<(), ManagerError> {
            Ok(())
        }

        async fn get_location(&mut self) -> Result<Option<String>, ManagerError> {
            Ok(Some(self.uri.clone()))
        }

        async fn shutdown(&mut self) -> Result<(), ManagerError> {
            Ok(())
        }

        async fn check_if_shutdown(&mut self) -> bool {
            true
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
                PipelineDescr {
                    name: "example1".to_string(),
                    description: "Description of example1".to_string(),
                    runtime_config: RuntimeConfig::from_yaml(""),
                    program_code: "CREATE TABLE example1 ( col1 INT );".to_string(),
                    program_config: ProgramConfig {
                        profile: Some(CompilationProfile::Unoptimized),
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
            .transit_program_status_to_compiling_rust(
                tenant_id,
                pipeline_id,
                Version(1),
                &ProgramInfo {
                    schema: ProgramSchema {
                        inputs: vec![],
                        outputs: vec![],
                    },
                    input_connectors: Default::default(),
                    output_connectors: Default::default(),
                },
            )
            .await
            .unwrap();
        db.lock()
            .await
            .transit_program_status_to_success(
                tenant_id,
                pipeline_id,
                Version(1),
                "not-used-program-binary-url",
            )
            .await
            .unwrap();

        // Construct the automaton
        let notifier = Arc::new(Notify::new());
        let automaton = PipelineAutomaton::new(
            pipeline_id,
            tenant_id,
            db.clone(),
            notifier.clone(),
            MockPipeline { uri },
            Duration::from_secs(10),
            Duration::from_millis(300),
            Duration::from_secs(10),
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
        test.tick().await;
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
        test.tick().await;
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
        test.tick().await;
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
        test.tick().await;
        test.check_current_state(PipelineStatus::Initializing).await;
        test.set_desired_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::ShuttingDown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Shutdown).await;
    }
}
