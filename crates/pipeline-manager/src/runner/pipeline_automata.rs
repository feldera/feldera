use crate::config::CommonConfig;
use crate::db::error::DBError;
use crate::db::storage::{ExtendedPipelineDescrRunner, Storage};
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDesiredStatus, PipelineId,
    PipelineStatus,
};
use crate::db::types::program::{generate_pipeline_config, ProgramStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::{
    validate_deployment_config, validate_program_info, validate_runtime_config,
};
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::interaction::{format_pipeline_url, format_timeout_error_message};
use crate::runner::pipeline_executor::PipelineExecutor;
use crate::runner::pipeline_logs::{start_thread_pipeline_logs, LogMessage, LogsSender};
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
use log::{debug, error, info, warn, Level};
use reqwest::{Method, StatusCode};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::{sync::Mutex, time::Duration};
use tokio::{sync::Notify, time::timeout};

/// State change action that needs to be undertaken.
#[derive(Debug, PartialEq)]
enum State {
    TransitionToProvisioning {
        deployment_config: serde_json::Value,
    },
    TransitionToInitializing {
        deployment_location: String,
    },
    TransitionToPaused,
    TransitionToRunning,
    TransitionToUnavailable,
    TransitionToSuspending,
    TransitionToStopping {
        error: Option<ErrorResponse>,
        suspend_info: Option<serde_json::Value>,
    },
    TransitionToStopped,
    StorageTransitionToCleared,
    Unchanged,
}

/// Outcome of a status check for a pipeline by polling its `/status` endpoint.
enum StatusCheckResult {
    /// Pipeline responded it is `Paused` (status code: OK).
    Paused,
    /// Pipeline responded it is `Running` (status code: OK).
    Running,
    /// Pipeline responded it is `Initializing` (status code: SERVICE_UNAVAILABLE).
    Initializing,
    /// The pipeline could not be reached, as such there is no response.
    Unavailable,
    /// The pipeline responded, but the response could not be parsed or was a runtime error.
    Error(ErrorResponse),
}

impl<T: PipelineExecutor> Drop for PipelineAutomaton<T> {
    fn drop(&mut self) {
        if let Some((terminate_sender, join_handle)) =
            self.logs_thread_terminate_sender_and_join_handle.take()
        {
            let _ = terminate_sender.send(());
            join_handle.abort();
        }
    }
}

/// Pipeline automaton monitors the runtime state of a single pipeline and continually reconciles
/// actual with desired state. The automaton runs as a separate tokio task.
pub struct PipelineAutomaton<T>
where
    T: PipelineExecutor,
{
    platform_version: String,
    common_config: CommonConfig,
    pipeline_id: PipelineId,
    tenant_id: TenantId,
    pipeline_handle: T,
    db: Arc<Mutex<StoragePostgres>>,
    notifier: Arc<Notify>,

    /// HTTP client which is reused.
    client: reqwest::Client,

    /// Set when provision() is called in the `Provisioning` stage.
    /// Content is the provisioning timeout in seconds.
    provision_called: Option<u64>,

    /// Maximum time to wait for the pipeline resources to be provisioned.
    /// This can differ significantly between the type of runner.
    default_provisioning_timeout: Duration,

    /// Counter for how many database errors were encountered in succession.
    database_error_counter: u64,

    /// Sender to the pipeline logs.
    logs_sender: LogsSender,

    /// Terminate sender and join handle for the logs thread.
    logs_thread_terminate_sender_and_join_handle: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl<T: PipelineExecutor> PipelineAutomaton<T> {
    /// While stopped, database notifications should trigger when the user sets
    /// the desired status, which will preempt the waiting.
    const POLL_PERIOD_STOPPED: Duration = Duration::from_millis(2_500);

    /// During provisioning, there is regular polling to check whether the pipeline
    /// resources have become available. Usually nothing will change in the database,
    /// which means no notifications will occur in this phase: as such, this poll
    /// period is frequent.
    const POLL_PERIOD_PROVISIONING: Duration = Duration::from_millis(500);

    /// During initialization, there is regular polling to check whether the pipeline
    /// process has come up. Usually nothing will change in the database, which means no
    /// notifications will occur in this phase: as such, this poll period is frequent.
    const POLL_PERIOD_INITIALIZING: Duration = Duration::from_millis(250);

    /// While operational, polling should happen regularly to check the pipeline
    /// is still reachable. Generally this is the case, and changes are usually
    /// caused by the user changing the desired state, thus triggering a database
    /// notification which will preempt the waiting.
    const POLL_PERIOD_RUNNING_PAUSED_UNAVAILABLE: Duration = Duration::from_millis(2_500);

    /// The suspend call of the pipeline is done synchronously, as such this period
    /// is for when to retry if it failed.
    const POLL_PERIOD_SUSPENDING: Duration = Duration::from_millis(1_000);

    /// The stop operation is done synchronously, as such this period is
    /// for when to retry if it failed.
    const POLL_PERIOD_STOPPING: Duration = Duration::from_millis(1_000);

    /// Timeout for an HTTP request to the pipeline `/status`.
    const HTTP_REQUEST_STATUS_TIMEOUT: Duration = Duration::from_secs(5);

    /// Timeout for an HTTP request to the pipeline `/start` or `/pause`.
    const HTTP_REQUEST_ACTION_TIMEOUT: Duration = Duration::from_secs(5);

    /// Timeout for an HTTP request to the pipeline `/suspend`.
    const HTTP_REQUEST_SUSPEND_TIMEOUT: Duration = Duration::from_secs(600);

    /// Creates a new automaton for a given pipeline.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        common_config: CommonConfig,
        pipeline_id: PipelineId,
        tenant_id: TenantId,
        db: Arc<Mutex<StoragePostgres>>,
        notifier: Arc<Notify>,
        client: reqwest::Client,
        pipeline_handle: T,
        default_provisioning_timeout: Duration,
        follow_request_receiver: mpsc::Receiver<mpsc::Sender<String>>,
        logs_sender: LogsSender,
        logs_receiver: mpsc::Receiver<LogMessage>,
    ) -> Self {
        // Start the thread which composes the pipeline logs and serves them
        let logs_thread_terminate_sender_and_join_handle =
            start_thread_pipeline_logs(follow_request_receiver, logs_receiver);

        Self {
            platform_version: common_config.platform_version.clone(),
            common_config,
            pipeline_id,
            tenant_id,
            pipeline_handle,
            db,
            notifier,
            client,
            provision_called: None,
            default_provisioning_timeout,
            logs_sender,
            logs_thread_terminate_sender_and_join_handle: Some(
                logs_thread_terminate_sender_and_join_handle,
            ),
            database_error_counter: 0,
        }
    }

    /// Runs until the pipeline is deleted or an unexpected error occurs.
    pub async fn run(mut self) -> Result<(), ManagerError> {
        let pipeline_id = self.pipeline_id;
        debug!("Automaton started: pipeline {pipeline_id}");
        let mut poll_timeout = Duration::from_secs(0);
        loop {
            // Wait until the timeout expires, or we get notified that the
            // pipeline has been updated
            let _ = timeout(poll_timeout, self.notifier.notified()).await;
            match self.do_run().await {
                Ok(new_poll_timeout) => {
                    poll_timeout = new_poll_timeout;
                    self.database_error_counter = 0; // Cycle succeeded
                }
                Err(e) => {
                    // Only database errors can bubble up here. The database itself
                    // is used to update the pipeline status and communication failures
                    // to the user, as such it does not have a way to communicate the
                    // database errors here. There are two categories of database errors:
                    // not being able to reach the database (e.g., due to temporary outage),
                    // or a database operation is expected to succeed but is not.
                    //
                    // As we cannot communicate the errors, instead we log the error and
                    // backoff before trying again. In the case of a temporary outage,
                    // it should again be reachable eventually. In the case of a failing
                    // database operation, it might be that the user can change the operation
                    // by setting a different desired state (e.g., shutting it down).
                    // In the latter case, it needs to still be investigated why a database
                    // operation did not work, and the relevant operation fixed.
                    match &e {
                        // Pipeline deletions should not lead to errors in the logs.
                        DBError::UnknownPipeline { pipeline_id } => {
                            info!("Automaton ended: pipeline {pipeline_id}");

                            // By leaving the run loop, the automaton will consume itself.
                            // As such, the pipeline_handle it owns will be dropped,
                            // which in turn will terminate itself as a consequence.
                            return Err(ManagerError::from(e));
                        }
                        e => {
                            let backoff_timeout = match self.database_error_counter {
                                0 => Duration::from_secs(5),
                                1..=5 => Duration::from_secs(10),
                                6..=10 => Duration::from_secs(30),
                                11..=15 => Duration::from_secs(60),
                                16..=20 => Duration::from_secs(120),
                                21..=25 => Duration::from_secs(300),
                                26..=30 => Duration::from_secs(600),
                                _ => Duration::from_secs(1200),
                            };
                            self.database_error_counter += 1;
                            error!(
                                "Automaton of pipeline {pipeline_id} encountered a database error, retrying in {} seconds (retry no. {})... Error was:\n{e}",
                                backoff_timeout.as_secs(),
                                self.database_error_counter
                            );
                            poll_timeout = backoff_timeout;
                        }
                    };
                }
            }
        }
    }

    /// Executes one run cycle.
    async fn do_run(&mut self) -> Result<Duration, DBError> {
        // Depending on the upcoming transition, it either retrieves the smaller monitoring descriptor,
        // or the larger complete descriptor. It should only get the complete descriptor if:
        // - current=`Stopped`
        //   AND desired=`Paused`/`Running`
        //   AND (program_status=`Success` AND platform_version=self.platform_version)
        // - current=`Provisioning`
        //   AND desired=`Paused`/`Running`
        //   AND `provision()` is not yet called
        //
        // The complete descriptor can be converted into the monitoring one, which is done to avoid
        // checking which one was returned in the general flow.
        let pipeline_monitoring_or_complete = &self
            .db
            .lock()
            .await
            .get_pipeline_by_id_for_runner(
                self.tenant_id,
                self.pipeline_id,
                &self.platform_version,
                self.provision_called.is_some(),
            )
            .await?;
        let pipeline = &pipeline_monitoring_or_complete.only_monitoring();

        // Determine transition
        let transition: State = match (
            pipeline.storage_status,
            pipeline.deployment_status,
            pipeline.deployment_desired_status,
        ) {
            // Stopped
            (
                StorageStatus::Cleared | StorageStatus::InUse,
                PipelineStatus::Stopped,
                PipelineDesiredStatus::Stopped,
            ) => State::Unchanged,
            (StorageStatus::Clearing, PipelineStatus::Stopped, PipelineDesiredStatus::Stopped) => {
                self.transit_storage_clearing_to_cleared(pipeline).await
            }
            (
                StorageStatus::Cleared | StorageStatus::InUse,
                PipelineStatus::Stopped,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => {
                self.transit_stopped_towards_paused_or_running(pipeline_monitoring_or_complete)
                    .await?
            }

            // Provisioning
            (
                StorageStatus::InUse,
                PipelineStatus::Provisioning,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => {
                self.transit_provisioning_towards_paused_or_running(pipeline_monitoring_or_complete)
                    .await
            }

            // Initializing
            (
                StorageStatus::InUse,
                PipelineStatus::Initializing,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => self.probe_pipeline_for_status(pipeline).await,

            // Paused
            (StorageStatus::InUse, PipelineStatus::Paused, PipelineDesiredStatus::Paused) => {
                self.probe_pipeline_for_status(pipeline).await
            }
            (StorageStatus::InUse, PipelineStatus::Paused, PipelineDesiredStatus::Running) => {
                self.perform_action_on_pipeline(pipeline, true).await
            }
            (StorageStatus::InUse, PipelineStatus::Paused, PipelineDesiredStatus::Suspended) => {
                State::TransitionToSuspending
            }

            // Running
            (StorageStatus::InUse, PipelineStatus::Running, PipelineDesiredStatus::Paused) => {
                self.perform_action_on_pipeline(pipeline, false).await
            }
            (StorageStatus::InUse, PipelineStatus::Running, PipelineDesiredStatus::Running) => {
                self.probe_pipeline_for_status(pipeline).await
            }
            (StorageStatus::InUse, PipelineStatus::Running, PipelineDesiredStatus::Suspended) => {
                State::TransitionToSuspending
            }

            // Unavailable
            (
                StorageStatus::InUse,
                PipelineStatus::Unavailable,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => self.probe_pipeline_for_status(pipeline).await,
            (
                StorageStatus::InUse,
                PipelineStatus::Unavailable,
                PipelineDesiredStatus::Suspended,
            ) => State::TransitionToSuspending,

            // Suspending
            (
                StorageStatus::InUse,
                PipelineStatus::Suspending,
                PipelineDesiredStatus::Suspended,
            ) => self.transit_suspending_towards_suspended(pipeline).await,

            // Stopping: as a fail-safe, all storage and desired pipeline statuses are viable
            (_, PipelineStatus::Stopping, _) => {
                self.transit_stopping_towards_stopped(pipeline).await
            }

            // Any statuses except Stopping will transition to Stopping when going towards Stopped
            (
                StorageStatus::InUse,
                PipelineStatus::Provisioning
                | PipelineStatus::Initializing
                | PipelineStatus::Paused
                | PipelineStatus::Running
                | PipelineStatus::Unavailable
                | PipelineStatus::Suspending,
                PipelineDesiredStatus::Stopped,
            ) => State::TransitionToStopping {
                error: None,
                suspend_info: None,
            },

            // All other combinations should not occur, which are explicitly listed here.
            // As a fail-safe, they transition towards `Stopped`.
            (_, PipelineStatus::Stopped, PipelineDesiredStatus::Suspended)
            | (_, PipelineStatus::Provisioning, PipelineDesiredStatus::Suspended)
            | (_, PipelineStatus::Initializing, PipelineDesiredStatus::Suspended)
            | (
                StorageStatus::Clearing,
                PipelineStatus::Stopped,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            )
            | (StorageStatus::Cleared | StorageStatus::Clearing, PipelineStatus::Provisioning, _)
            | (StorageStatus::Cleared | StorageStatus::Clearing, PipelineStatus::Initializing, _)
            | (StorageStatus::Cleared | StorageStatus::Clearing, PipelineStatus::Paused, _)
            | (StorageStatus::Cleared | StorageStatus::Clearing, PipelineStatus::Running, _)
            | (StorageStatus::Cleared | StorageStatus::Clearing, PipelineStatus::Unavailable, _)
            | (StorageStatus::Cleared | StorageStatus::Clearing, PipelineStatus::Suspending, _)
            | (
                StorageStatus::InUse,
                PipelineStatus::Suspending,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
            ) => State::TransitionToStopping {
                error: Some(ErrorResponse::from(
                    &RunnerError::AutomatonImpossibleDesiredStatus {
                        current_status: pipeline.deployment_status,
                        desired_status: pipeline.deployment_desired_status,
                    },
                )),
                suspend_info: None,
            },
        };

        // Store the transition in the database
        let version_guard = pipeline.version;
        let new_status = match &transition {
            State::TransitionToProvisioning { deployment_config } => {
                match self
                    .db
                    .lock()
                    .await
                    .transit_deployment_status_to_provisioning(
                        self.tenant_id,
                        pipeline.id,
                        version_guard,
                        deployment_config.clone(),
                    )
                    .await
                {
                    Ok(_) => PipelineStatus::Provisioning,
                    Err(e) => match e {
                        DBError::OutdatedPipelineVersion {
                            outdated_version,
                            latest_version,
                        } => {
                            // This can happen in the following concurrency scenario:
                            // (1) Automaton is (current: Stopped, desired: Stopped)
                            // (2) User issues /start on pipeline (v1)
                            // (3) Automaton picks up (current: Stopped, desired: Running) and
                            //     generates the deployment_config for v1, but has not yet stored
                            //     it in the database
                            // (4) User issues /stop on pipeline, makes an edit to for example
                            //     the runtime_config (making it v2), and issues /start on the
                            //     pipeline again
                            // (5) Only now the automaton gets to store the transition in the
                            //     database, which would have the deployment_config of v1 whereas
                            //     the current on which /start was called is v2
                            //
                            // The solution is to retry again the next cycle, in which a new
                            // deployment_config will be generated which corresponds to v2.
                            //
                            // For all other transitions, the version guard should always match,
                            // and as such will cause a database error to bubble up if it does not.
                            debug!(
                                "Pipeline automaton {}: version initially intended to be started ({}) is outdated by latest ({})",
                                self.pipeline_id, outdated_version, latest_version
                            );
                            assert_eq!(pipeline.deployment_status, PipelineStatus::Stopped);
                            PipelineStatus::Stopped
                        }
                        e => {
                            return Err(e);
                        }
                    },
                }
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
                        version_guard,
                        deployment_location,
                    )
                    .await?;
                PipelineStatus::Initializing
            }
            State::TransitionToPaused => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_paused(self.tenant_id, pipeline.id, version_guard)
                    .await?;
                PipelineStatus::Paused
            }
            State::TransitionToRunning => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_running(
                        self.tenant_id,
                        pipeline.id,
                        version_guard,
                    )
                    .await?;
                PipelineStatus::Running
            }
            State::TransitionToUnavailable => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_unavailable(
                        self.tenant_id,
                        pipeline.id,
                        version_guard,
                    )
                    .await?;
                PipelineStatus::Unavailable
            }
            State::TransitionToSuspending => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_suspending(
                        self.tenant_id,
                        pipeline.id,
                        version_guard,
                    )
                    .await?;
                PipelineStatus::Suspending
            }
            State::TransitionToStopping {
                error,
                suspend_info,
            } => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_stopping(
                        self.tenant_id,
                        pipeline.id,
                        version_guard,
                        error.clone(),
                        suspend_info.clone(),
                    )
                    .await?;
                PipelineStatus::Stopping
            }
            State::TransitionToStopped => {
                self.db
                    .lock()
                    .await
                    .transit_deployment_status_to_stopped(
                        self.tenant_id,
                        pipeline.id,
                        version_guard,
                    )
                    .await?;
                PipelineStatus::Stopped
            }
            State::StorageTransitionToCleared => {
                self.db
                    .lock()
                    .await
                    .transit_storage_status_to_cleared(self.tenant_id, pipeline.id)
                    .await?;
                pipeline.deployment_status
            }
            State::Unchanged => pipeline.deployment_status,
        };
        if transition != State::Unchanged {
            if transition == State::StorageTransitionToCleared {
                info!(
                    "Storage transition: {} -> {} for pipeline {}",
                    pipeline.storage_status,
                    StorageStatus::Cleared,
                    pipeline.id
                );
                self.logs_sender
                    .send(LogMessage::new_from_control_plane(
                        Level::Info,
                        "Storage has been cleared",
                    ))
                    .await;
            } else {
                info!(
                    "Transition: {} -> {} (desired: {}) for pipeline {}",
                    pipeline.deployment_status,
                    new_status,
                    pipeline.deployment_desired_status,
                    pipeline.id
                );
                self.logs_sender
                    .send(LogMessage::new_from_control_plane(
                        Level::Info,
                        &format!(
                            "Status transition: {} -> {}",
                            pipeline.deployment_status, new_status
                        ),
                    ))
                    .await;
            }
        }

        // Determine the poll timeout based on the pipeline status it has become.
        // This timeout can be preempted by a database notification.
        let poll_timeout = match &transition {
            State::TransitionToProvisioning { .. } => Self::POLL_PERIOD_PROVISIONING,
            State::TransitionToInitializing { .. } => Self::POLL_PERIOD_INITIALIZING,
            State::TransitionToPaused
            | State::TransitionToRunning
            | State::TransitionToUnavailable => Self::POLL_PERIOD_RUNNING_PAUSED_UNAVAILABLE,
            State::TransitionToSuspending => Self::POLL_PERIOD_SUSPENDING,
            State::TransitionToStopping { .. } => Self::POLL_PERIOD_STOPPING,
            State::TransitionToStopped => Self::POLL_PERIOD_STOPPED,
            State::StorageTransitionToCleared | State::Unchanged => {
                match pipeline.deployment_status {
                    PipelineStatus::Stopped => Self::POLL_PERIOD_STOPPED,
                    PipelineStatus::Provisioning => Self::POLL_PERIOD_PROVISIONING,
                    PipelineStatus::Initializing => Self::POLL_PERIOD_INITIALIZING,
                    PipelineStatus::Paused => Self::POLL_PERIOD_RUNNING_PAUSED_UNAVAILABLE,
                    PipelineStatus::Running => Self::POLL_PERIOD_RUNNING_PAUSED_UNAVAILABLE,
                    PipelineStatus::Unavailable => Self::POLL_PERIOD_RUNNING_PAUSED_UNAVAILABLE,
                    PipelineStatus::Suspending => Self::POLL_PERIOD_SUSPENDING,
                    PipelineStatus::Stopping => Self::POLL_PERIOD_STOPPING,
                }
            }
        };
        Ok(poll_timeout)
    }

    /// Whether the time between now and the since timestamp has exceeded the timeout.
    fn has_timeout_expired(since: DateTime<Utc>, timeout: Duration) -> bool {
        Utc::now().timestamp_millis() - since.timestamp_millis() > timeout.as_millis() as i64
    }

    /// Sends HTTP request from the automaton to the pipeline and parses response as a JSON object.
    /// The automaton uses a `reqwest::Client` rather than an `awc::Client` because the latter does
    /// not support being sent across async worker threads.
    async fn http_request_pipeline_json(
        &self,
        method: Method,
        location: &str,
        endpoint: &str,
        timeout: Duration,
    ) -> Result<(StatusCode, serde_json::Value), ManagerError> {
        let url = format_pipeline_url(
            if self.common_config.enable_https {
                "https"
            } else {
                "http"
            },
            location,
            endpoint,
            "",
        );
        let response = self
            .client
            .request(method, &url)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    RunnerError::PipelineInteractionUnreachable {
                        error: format_timeout_error_message(timeout, e),
                    }
                } else {
                    RunnerError::PipelineInteractionUnreachable {
                        error: format!("unable to send request due to: {e}"),
                    }
                }
            })?;
        let status = response.status();
        let value = response.json::<serde_json::Value>().await.map_err(|e| {
            RunnerError::PipelineInteractionInvalidResponse {
                error: format!("unable to deserialize as JSON due to: {e}"),
            }
        })?;
        Ok((status, value))
    }

    /// Parses `ErrorResponse` from JSON.
    /// Upon error, builds an `ErrorResponse` with the original JSON content.
    fn error_response_from_json(
        pipeline_id: PipelineId,
        status: StatusCode,
        json: &serde_json::Value,
    ) -> ErrorResponse {
        serde_json::from_value(json.clone()).unwrap_or_else(|_| {
            ErrorResponse::from(&RunnerError::PipelineInteractionInvalidResponse {
                error: format!("Pipeline {pipeline_id} returned HTTP response which cannot be deserialized. Status code: {status}; body: {json:#}"),
            })
        })
    }

    /// Retrieves the deployment location from the descriptor.
    /// The location is expected to be there.
    /// Returns an error if the location is missing.
    fn get_required_deployment_location(
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> Result<String, RunnerError> {
        match pipeline.deployment_location.clone() {
            None => Err(RunnerError::AutomatonMissingDeploymentLocation),
            Some(location) => Ok(location),
        }
    }

    /// Checks the pipeline status by attempting to poll its `/status` endpoint.
    /// An error result is only returned if the response could not be parsed or
    /// contained an error.
    async fn check_pipeline_status(
        &self,
        pipeline_id: PipelineId,
        deployment_location: &str,
    ) -> StatusCheckResult {
        match self
            .http_request_pipeline_json(
                Method::GET,
                deployment_location,
                "status",
                Self::HTTP_REQUEST_STATUS_TIMEOUT,
            )
            .await
        {
            Ok((http_status, http_body)) => {
                // Able to reach the pipeline HTTP(S) server and get a response
                if http_status == StatusCode::OK {
                    // Fatal error: cannot deserialize status
                    let Some(pipeline_status) = http_body.as_str() else {
                        return StatusCheckResult::Error(ErrorResponse::from_error_nolog(
                            &RunnerError::PipelineInteractionInvalidResponse {
                                error: format!("Body of /status response: {http_body}"),
                            },
                        ));
                    };

                    // Fatal error: if it is not Paused/Running
                    if pipeline_status == "Paused" {
                        StatusCheckResult::Paused
                    } else if pipeline_status == "Running" {
                        StatusCheckResult::Running
                    } else {
                        // Notably: "Terminated"
                        return StatusCheckResult::Error(ErrorResponse::from_error_nolog(
                            &RunnerError::PipelineInteractionInvalidResponse {
                                error: format!(
                                    "Pipeline status is not Paused or Running, but is: {pipeline_status}"
                                ),
                            },
                        ));
                    }
                } else if http_status == StatusCode::SERVICE_UNAVAILABLE {
                    // Check if pipeline returned SERVICE_UNAVAILABLE because it is Initializing
                    if http_body["error_code"]
                        .as_str()
                        .is_some_and(|s| s == "Initializing")
                    {
                        StatusCheckResult::Initializing
                    } else {
                        // Pipeline HTTP server is running but indicates it is not yet available
                        warn!(
                            "Pipeline {pipeline_id} responds to status check it is not (yet) ready"
                        );
                        StatusCheckResult::Unavailable
                    }
                } else {
                    // All other status codes indicate a fatal error
                    // The HTTP server is still running, but the pipeline itself failed
                    error!("Error response to status check for pipeline {pipeline_id}. Status code: {http_status}. Body: {http_body}");
                    StatusCheckResult::Error(Self::error_response_from_json(
                        pipeline_id,
                        http_status,
                        &http_body,
                    ))
                }
            }
            Err(e) => {
                debug!("Unable to reach pipeline {pipeline_id} for status check due to: {e}");
                StatusCheckResult::Unavailable
            }
        }
    }

    /// Transits from `Stopped` towards `Paused` or `Running`.
    async fn transit_stopped_towards_paused_or_running(
        &mut self,
        pipeline_monitoring_or_complete: &ExtendedPipelineDescrRunner,
    ) -> Result<State, DBError> {
        let pipeline = pipeline_monitoring_or_complete.only_monitoring();
        if pipeline.program_status == ProgramStatus::Success
            && pipeline.platform_version == self.platform_version
        {
            if let ExtendedPipelineDescrRunner::Complete(pipeline) = pipeline_monitoring_or_complete
            {
                self.transit_stopped_towards_paused_or_running_phase_ready(pipeline)
                    .await
            } else {
                panic!(
                    "For the transit of Stopped towards Running/Paused \
                    (program successfully compiled at current platform version), \
                    the complete pipeline descriptor should have been retrieved"
                );
            }
        } else {
            self.transit_stopped_towards_paused_or_running_early_start(&pipeline)
                .await
        }
    }

    /// Transits from `Stopped` towards `Paused` or `Running`
    /// when it has not yet successfully compiled at the current platform version.
    async fn transit_stopped_towards_paused_or_running_early_start(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> Result<State, DBError> {
        assert!(
            pipeline.program_status != ProgramStatus::Success
                || self.platform_version != pipeline.platform_version,
            "Expected to be true: {:?} != {:?} || {} != {}",
            pipeline.program_status,
            ProgramStatus::Success,
            self.platform_version,
            pipeline.platform_version
        );

        // If the pipeline program errored during compilation, immediately transition to `Failed`
        match &pipeline.program_status {
            ProgramStatus::SqlError => {
                return Ok(State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &DBError::StartFailedDueToFailedCompilation {
                            compiler_error:
                                "SQL error occurred (see `program_error` for more information)"
                                    .to_string(),
                        },
                    )),
                    suspend_info: None,
                });
            }
            ProgramStatus::RustError => {
                return Ok(State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &DBError::StartFailedDueToFailedCompilation {
                            compiler_error:
                                "Rust error occurred (see `program_error` for more information)"
                                    .to_string(),
                        },
                    )),
                    suspend_info: None,
                });
            }
            ProgramStatus::SystemError => {
                return Ok(State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &DBError::StartFailedDueToFailedCompilation {
                            compiler_error:
                                "System error occurred (see `program_error` for more information)"
                                    .to_string(),
                        },
                    )),
                    suspend_info: None,
                });
            }
            _ => {}
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
        }

        Ok(State::Unchanged)
    }

    /// Transits from `Stopped` towards `Paused` or `Running`
    /// when it has successfully compiled at the current platform version.
    async fn transit_stopped_towards_paused_or_running_phase_ready(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> Result<State, DBError> {
        assert_eq!(pipeline.program_status, ProgramStatus::Success);
        assert_eq!(self.platform_version, pipeline.platform_version);

        // Required runtime_config
        let runtime_config = match validate_runtime_config(&pipeline.runtime_config, true) {
            Ok(runtime_config) => runtime_config,
            Err(e) => {
                return Ok(State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonInvalidRuntimeConfig {
                            value: pipeline.runtime_config.clone(),
                            error: e,
                        },
                    )),
                    suspend_info: None,
                });
            }
        };

        // Input and output connectors from required program_info
        let (inputs, outputs) = match &pipeline.program_info {
            None => {
                return Ok(State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonMissingProgramInfo,
                    )),
                    suspend_info: None,
                });
            }
            Some(program_info) => {
                let program_info = match validate_program_info(program_info) {
                    Ok(program_info) => program_info,
                    Err(e) => {
                        return Ok(State::TransitionToStopping {
                            error: Some(ErrorResponse::from_error_nolog(
                                &RunnerError::AutomatonInvalidProgramInfo {
                                    value: program_info.clone(),
                                    error: e,
                                },
                            )),
                            suspend_info: None,
                        });
                    }
                };
                (
                    program_info.input_connectors,
                    program_info.output_connectors,
                )
            }
        };

        // Deployment configuration
        let mut deployment_config =
            generate_pipeline_config(pipeline.id, &runtime_config, &inputs, &outputs);
        deployment_config.storage_config =
            Some(self.pipeline_handle.generate_storage_config().await);
        let deployment_config = match serde_json::to_value(&deployment_config) {
            Ok(deployment_config) => deployment_config,
            Err(error) => {
                return Ok(State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonFailedToSerializeDeploymentConfig {
                            error: error.to_string(),
                        },
                    )),
                    suspend_info: None,
                });
            }
        };

        Ok(State::TransitionToProvisioning { deployment_config })
    }

    /// Transits from `Provisioning` towards `Paused` or `Running`.
    async fn transit_provisioning_towards_paused_or_running(
        &mut self,
        pipeline_monitoring_or_complete: &ExtendedPipelineDescrRunner,
    ) -> State {
        if self.provision_called.is_none() {
            if let ExtendedPipelineDescrRunner::Complete(pipeline) = pipeline_monitoring_or_complete
            {
                self.transit_provisioning_towards_paused_or_running_phase_call(pipeline)
                    .await
            } else {
                panic!(
                    "For the transit of Provisioning towards Paused/Running (provision not yet called), \
                    the complete pipeline descriptor should have been retrieved"
                );
            }
        } else {
            self.transit_provisioning_towards_paused_or_running_phase_await(
                &pipeline_monitoring_or_complete.only_monitoring(),
            )
            .await
        }
    }

    /// Transits from `Provisioning` towards `Paused` or `Running`
    /// when it has not yet called `provision()` (which is idempotent).
    async fn transit_provisioning_towards_paused_or_running_phase_call(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        assert!(self.provision_called.is_none());

        // The runner is only able to provision a pipeline of the current platform version.
        // If in the meanwhile (e.g., due to runner restart during upgrade) the platform
        // version has changed, provisioning will fail.
        if pipeline.platform_version != self.platform_version {
            return State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(
                    &RunnerError::AutomatonCannotProvisionDifferentPlatformVersion {
                        pipeline_platform_version: pipeline.platform_version.clone(),
                        runner_platform_version: self.platform_version.clone(),
                    },
                )),
                suspend_info: None,
            };
        }

        // Deployment configuration and program binary URL are expected to be set
        let deployment_config = match &pipeline.deployment_config {
            None => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonMissingDeploymentConfig,
                    )),
                    suspend_info: None,
                }
            }
            Some(deployment_config) => match validate_deployment_config(deployment_config) {
                Ok(deployment_config) => deployment_config,
                Err(e) => {
                    return State::TransitionToStopping {
                        error: Some(ErrorResponse::from_error_nolog(
                            &RunnerError::AutomatonInvalidDeploymentConfig {
                                value: deployment_config.clone(),
                                error: e,
                            },
                        )),
                        suspend_info: None,
                    };
                }
            },
        };

        // URL where the program binary can be downloaded from
        let program_binary_url = format!(
            "{}://{}:{}/binary/{}/{}/{}/{}",
            if self.common_config.enable_https {
                "https"
            } else {
                "http"
            },
            self.common_config.compiler_host,
            self.common_config.compiler_port,
            self.pipeline_id,
            pipeline.program_version,
            if let Some(source_checksum) = pipeline.program_binary_source_checksum.as_ref() {
                source_checksum
            } else {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonCannotConstructProgramBinaryUrl {
                            error: "source checksum is missing".to_string(),
                        },
                    )),
                    suspend_info: None,
                };
            },
            if let Some(integrity_checksum) = pipeline.program_binary_integrity_checksum.as_ref() {
                integrity_checksum
            } else {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonCannotConstructProgramBinaryUrl {
                            error: "integrity checksum is missing".to_string(),
                        },
                    )),
                    suspend_info: None,
                };
            },
        );

        match self
            .pipeline_handle
            .provision(
                &deployment_config,
                &program_binary_url,
                pipeline.program_version,
                pipeline.suspend_info.clone(),
            )
            .await
        {
            Ok(()) => {
                self.provision_called = Some(
                    deployment_config
                        .global
                        .provisioning_timeout_secs
                        .unwrap_or(self.default_provisioning_timeout.as_secs()),
                );
                info!(
                    "Provisioning pipeline {} (tenant: {})",
                    self.pipeline_id, self.tenant_id
                );
                State::Unchanged
            }
            Err(e) => State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(&e)),
                suspend_info: None,
            },
        }
    }

    /// Transits from `Provisioning` towards `Paused` or `Running`
    /// when it has called `provision()` and is now awaiting
    /// `is_provisioned()` to return success in time.
    async fn transit_provisioning_towards_paused_or_running_phase_await(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> State {
        assert!(self.provision_called.is_some());
        let provisioning_timeout = Duration::from_secs(
            self.provision_called
                .expect("Provision must have been called"),
        );
        match self.pipeline_handle.is_provisioned().await {
            Ok(Some(location)) => {
                match self
                    .check_pipeline_status(self.pipeline_id, &location)
                    .await
                {
                    StatusCheckResult::Unavailable
                        if Self::has_timeout_expired(
                            pipeline.deployment_status_since,
                            provisioning_timeout,
                        ) =>
                    {
                        error!(
                            "Pipeline {} was provisioned, but didn't respond to status check in {} seconds - terminating the pipeline",
                            pipeline.id,
                            provisioning_timeout.as_secs()
                        );
                        State::TransitionToStopping {
                            error: Some(
                                RunnerError::AutomatonProvisioningTimeout {
                                    timeout: provisioning_timeout,
                                }
                                .into(),
                            ),
                            suspend_info: None,
                        }
                    }
                    StatusCheckResult::Unavailable => {
                        warn!(
                            "Pipeline {} is provisioned, but is not yet reachable, retrying...",
                            pipeline.id
                        );
                        State::Unchanged
                    }
                    // Only valid transitions from Provisioning is to Initializing or Stopping
                    // so even when we see it is running, we are transitioning to Initializing
                    // first which would then probe pipeline and move to actual state.
                    StatusCheckResult::Initializing
                    | StatusCheckResult::Running
                    | StatusCheckResult::Paused => State::TransitionToInitializing {
                        deployment_location: location,
                    },
                    StatusCheckResult::Error(error) => State::TransitionToStopping {
                        error: Some(error),
                        suspend_info: None,
                    },
                }
            }

            Ok(None) => {
                debug!(
                    "Pipeline provisioning: pipeline {} is not yet provisioned",
                    pipeline.id
                );
                if Self::has_timeout_expired(pipeline.deployment_status_since, provisioning_timeout)
                {
                    error!(
                        "Pipeline provisioning: timed out for pipeline {}",
                        pipeline.id
                    );
                    State::TransitionToStopping {
                        error: Some(
                            RunnerError::AutomatonProvisioningTimeout {
                                timeout: provisioning_timeout,
                            }
                            .into(),
                        ),
                        suspend_info: None,
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
                State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(&e)),
                    suspend_info: None,
                }
            }
        }
    }

    /// Transits from `Paused` or `Running` towards the other one.
    /// Can go to other states based on pipelines current state
    /// It issues a request to the pipeline HTTP server `/start` or `/pause` HTTP endpoint.
    async fn perform_action_on_pipeline(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
        is_start: bool,
    ) -> State {
        let deployment_location = match Self::get_required_deployment_location(pipeline) {
            Ok(deployment_location) => deployment_location,
            Err(e) => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(&e)),
                    suspend_info: None,
                };
            }
        };

        // Check deployment when initialized
        if let Err(e) = self.pipeline_handle.check().await {
            return State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(&e)),
                suspend_info: None,
            };
        }

        // Issue request to the /start or /pause endpoint
        let action = if is_start { "start" } else { "pause" };
        match self
            .http_request_pipeline_json(
                Method::GET,
                &deployment_location,
                action,
                Self::HTTP_REQUEST_ACTION_TIMEOUT,
            )
            .await
        {
            Ok((status, body)) => {
                match status {
                    StatusCode::OK if is_start => State::TransitionToRunning,
                    StatusCode::OK => State::TransitionToPaused,
                    // Check if pipeline returned SERVICE_UNAVAILABLE because it is Initializing
                    StatusCode::SERVICE_UNAVAILABLE
                        if body["error_code"]
                            .as_str()
                            .is_some_and(|s| s == "Initializing") =>
                    {
                        warn!("Unable to perform action '{action}' on pipeline {} because pipeline indicated it is not (yet) ready", pipeline.id);
                        State::TransitionToInitializing {
                            deployment_location,
                        }
                    }
                    StatusCode::SERVICE_UNAVAILABLE => {
                        warn!("Unable to perform action '{action}' on pipeline {} because pipeline is unreachable", pipeline.id);
                        State::TransitionToUnavailable
                    }
                    _ => {
                        error!("Error response to action '{action}' on pipeline {}. Status: {status}. Body: {body}", pipeline.id);
                        State::TransitionToStopping {
                            error: Some(Self::error_response_from_json(
                                self.pipeline_id,
                                status,
                                &body,
                            )),
                            suspend_info: None,
                        }
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

    /// Transits between `Initializing`, `Paused`, `Running` and `Unavailable` depending
    /// on what the pipeline HTTP `/status` endpoint reports.
    async fn probe_pipeline_for_status(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> State {
        // Check deployment when initialized
        if let Err(e) = self.pipeline_handle.check().await {
            return State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(&e)),
                suspend_info: None,
            };
        }

        // Perform probe
        let deployment_location = match Self::get_required_deployment_location(pipeline) {
            Ok(deployment_location) => deployment_location,
            Err(e) => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(&e)),
                    suspend_info: None,
                }
            }
        };
        match self
            .check_pipeline_status(pipeline.id, &deployment_location)
            .await
        {
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
            StatusCheckResult::Initializing => match pipeline.deployment_status {
                PipelineStatus::Initializing => State::Unchanged,
                _ => State::TransitionToInitializing {
                    deployment_location,
                },
            },
            StatusCheckResult::Unavailable => match pipeline.deployment_status {
                PipelineStatus::Unavailable => State::Unchanged,
                _ => State::TransitionToUnavailable,
            },
            StatusCheckResult::Error(error) => State::TransitionToStopping {
                error: Some(error),
                suspend_info: None,
            },
        }
    }

    /// Transits from `Suspending` towards `Suspended`.
    ///
    /// It calls the idempotent pipeline `/suspend` HTTP endpoint, in order to get it to suspend
    /// its circuit to storage. It does the following based on the outcome:
    /// - If it cannot be reached, it will try again later
    /// - If it gets back OK, it will transition to `Stopping` with `suspend_info` set
    /// - If it gets back SERVICE_UNAVAILABLE, it will try again later
    /// - If it gets back any other status code, it will transition to `Stopping` with `deployment_error` set
    async fn transit_suspending_towards_suspended(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> State {
        // Check deployment when suspending
        if let Err(e) = self.pipeline_handle.check().await {
            return State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(&e)),
                suspend_info: None,
            };
        }
        let deployment_location = match Self::get_required_deployment_location(pipeline) {
            Ok(deployment_location) => deployment_location,
            Err(e) => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(&e)),
                    suspend_info: None,
                };
            }
        };
        match self
            .http_request_pipeline_json(
                Method::POST,
                &deployment_location,
                "suspend",
                Self::HTTP_REQUEST_SUSPEND_TIMEOUT,
            )
            .await
        {
            Ok((status, body)) => {
                if status == StatusCode::OK {
                    // Pipeline has responded its circuit has been suspended to storage,
                    // as such we can now transition to suspending the compute resources
                    // themselves (which will terminate the pipeline in its entirety)
                    State::TransitionToStopping {
                        error: None,
                        suspend_info: Some(json!({})),
                    }
                } else if status == StatusCode::SERVICE_UNAVAILABLE {
                    // NOTE: we don't care about the error_code of response here,
                    // because we want to try it again irrespective of it.
                    warn!("Unable to suspend pipeline {} because pipeline indicated it is not (yet) ready", pipeline.id);
                    State::Unchanged
                } else {
                    error!("Suspend operation of pipeline {} returned an error. Status: {status}. Body: {body}", pipeline.id);
                    State::TransitionToStopping {
                        error: Some(Self::error_response_from_json(
                            self.pipeline_id,
                            status,
                            &body,
                        )),
                        suspend_info: None,
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Unable to suspend pipeline {} because it could not be reached due to: {e}",
                    pipeline.id
                );
                State::Unchanged
            }
        }
    }

    /// Transits from `Stopping` towards `Stopped`.
    ///
    /// Scales down to zero or deallocates the compute resources.
    ///
    /// The runner should always be able to eventually delete resources, as they are
    /// under its control. As such, it does not return an error upon failure to stop,
    /// but instead does not change the state such that it is retried.
    async fn transit_stopping_towards_stopped(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> State {
        if let Err(e) = self.pipeline_handle.stop().await {
            error!(
                "Pipeline {} could not be stopped (will retry): {e}",
                pipeline.id
            );
            State::Unchanged
        } else {
            self.provision_called = None;
            State::TransitionToStopped
        }
    }

    /// Transits storage status from `Clearing` towards `Cleared`.
    ///
    /// Scales down to zero or deallocates the compute resources.
    ///
    /// The runner should always be able to eventually delete resources, as they are
    /// under its control. As such, it does not return an error upon failure to stop,
    /// but instead does not change the state such that it is retried.
    async fn transit_storage_clearing_to_cleared(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> State {
        if let Err(e) = self.pipeline_handle.clear().await {
            error!(
                "Pipeline {} storage could not be cleared (will retry): {e}",
                pipeline.id
            );
            State::Unchanged
        } else {
            State::StorageTransitionToCleared
        }
    }
}

#[cfg(test)]
mod test {
    use crate::auth::TenantRecord;
    use crate::config::CommonConfig;
    use crate::db::storage::Storage;
    use crate::db::storage_postgres::StoragePostgres;
    use crate::db::types::pipeline::{
        PipelineDescr, PipelineDesiredStatus, PipelineId, PipelineStatus,
    };
    use crate::db::types::program::{ProgramInfo, RustCompilationInfo, SqlCompilationInfo};
    use crate::db::types::version::Version;
    use crate::error::ManagerError;
    use crate::logging;
    use crate::runner::main::MAXIMUM_OUTSTANDING_LOG_FOLLOW_REQUESTS;
    use crate::runner::pipeline_automata::PipelineAutomaton;
    use crate::runner::pipeline_executor::PipelineExecutor;
    use crate::runner::pipeline_logs::{LogMessage, LogsSender};
    use async_trait::async_trait;
    use feldera_types::config::{PipelineConfig, StorageConfig};
    use feldera_types::program_schema::ProgramSchema;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::{channel, Sender};
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
        const DEFAULT_PROVISIONING_TIMEOUT: Duration = Duration::from_millis(1);

        fn new(
            _pipeline_id: PipelineId,
            _common_config: CommonConfig,
            _config: Self::Config,
            _client: reqwest::Client,
            _logs_sender: LogsSender,
        ) -> Self {
            unimplemented!()
        }

        async fn generate_storage_config(&self) -> StorageConfig {
            StorageConfig {
                path: "".to_string(),
                cache: Default::default(),
            }
        }

        async fn provision(
            &mut self,
            _: &PipelineConfig,
            _: &str,
            _: Version,
            _: Option<serde_json::Value>,
        ) -> Result<(), ManagerError> {
            Ok(())
        }

        async fn is_provisioned(&mut self) -> Result<Option<String>, ManagerError> {
            Ok(Some(self.uri.clone()))
        }

        async fn check(&mut self) -> Result<(), ManagerError> {
            Ok(())
        }

        async fn stop(&mut self) -> Result<(), ManagerError> {
            Ok(())
        }

        async fn clear(&mut self) -> Result<(), ManagerError> {
            Ok(())
        }
    }

    struct AutomatonTest {
        db: Arc<Mutex<StoragePostgres>>,
        automaton: PipelineAutomaton<MockPipeline>,
        _follow_request_sender: Sender<Sender<String>>,
    }

    impl AutomatonTest {
        async fn set_desired_state(&self, status: PipelineDesiredStatus) {
            let automaton = &self.automaton;
            let pipeline = self
                .db
                .lock()
                .await
                .get_pipeline_by_id(automaton.tenant_id, automaton.pipeline_id)
                .await
                .unwrap();
            match status {
                PipelineDesiredStatus::Stopped => {
                    self.db
                        .lock()
                        .await
                        .set_deployment_desired_status_suspended_or_stopped(
                            automaton.tenant_id,
                            &pipeline.name,
                            true,
                        )
                        .await
                        .unwrap();
                }
                PipelineDesiredStatus::Suspended => {
                    self.db
                        .lock()
                        .await
                        .set_deployment_desired_status_suspended_or_stopped(
                            automaton.tenant_id,
                            &pipeline.name,
                            false,
                        )
                        .await
                        .unwrap();
                }
                PipelineDesiredStatus::Paused => {
                    self.db
                        .lock()
                        .await
                        .set_deployment_desired_status_paused(automaton.tenant_id, &pipeline.name)
                        .await
                        .unwrap();
                }
                PipelineDesiredStatus::Running => {
                    self.db
                        .lock()
                        .await
                        .set_deployment_desired_status_running(automaton.tenant_id, &pipeline.name)
                        .await
                        .unwrap();
                }
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
                    runtime_config: json!({}),
                    program_code: "CREATE TABLE example1 ( col1 INT );".to_string(),
                    udf_rust: "".to_string(),
                    udf_toml: "".to_string(),
                    program_config: json!({
                        "profile": "unoptimized",
                        "cache": false
                    }),
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
                &SqlCompilationInfo {
                    exit_code: 0,
                    messages: vec![],
                },
                &serde_json::to_value(ProgramInfo {
                    schema: ProgramSchema {
                        inputs: vec![],
                        outputs: vec![],
                    },
                    main_rust: "".to_string(),
                    udf_stubs: "".to_string(),
                    input_connectors: Default::default(),
                    output_connectors: Default::default(),
                    dataflow: serde_json::Value::Null,
                })
                .unwrap(),
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
                &RustCompilationInfo {
                    exit_code: 0,
                    stdout: "".to_string(),
                    stderr: "".to_string(),
                },
                "not-used-program-binary-source-checksum",
                "not-used-program-binary-integrity-checksum",
            )
            .await
            .unwrap();

        // Construct the automaton
        let (_follow_request_sender, follow_request_receiver) =
            channel::<Sender<String>>(MAXIMUM_OUTSTANDING_LOG_FOLLOW_REQUESTS);
        let (logs_sender, logs_receiver) =
            channel::<LogMessage>(MAXIMUM_OUTSTANDING_LOG_FOLLOW_REQUESTS);
        let logs_sender = LogsSender::new(logs_sender);
        let notifier = Arc::new(Notify::new());
        let client = reqwest::Client::new();
        let automaton = PipelineAutomaton::new(
            CommonConfig {
                platform_version: "v0".to_string(),
                bind_address: "127.0.0.1".to_string(),
                api_port: 8080,
                compiler_host: "127.0.0.1".to_string(),
                compiler_port: 8085,
                runner_host: "127.0.0.1".to_string(),
                runner_port: 8089,
                http_workers: 1,
                enable_https: false,
                https_tls_cert_path: None,
                https_tls_key_path: None,
            },
            pipeline_id,
            tenant_id,
            db.clone(),
            notifier.clone(),
            client,
            MockPipeline { uri },
            Duration::from_secs(1),
            follow_request_receiver,
            logs_sender,
            logs_receiver,
        );
        AutomatonTest {
            db: db.clone(),
            automaton,
            _follow_request_sender,
        }
    }

    async fn mock_endpoint(
        server: &mut MockServer,
        http_method: &str,
        endpoint: &str,
        code: u16,
        json_body: serde_json::Value,
    ) {
        server.reset().await;
        let template = ResponseTemplate::new(code).set_body_json(json_body);
        Mock::given(method(http_method))
            .and(path(endpoint))
            .respond_with(template)
            .mount(server)
            .await;
    }

    #[cfg(feature = "postgresql_embedded")]
    type SetupResult = (MockServer, tempfile::TempDir, AutomatonTest);
    #[cfg(not(feature = "postgresql_embedded"))]
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
        test.set_desired_state(PipelineDesiredStatus::Paused).await;
        test.check_current_state(PipelineStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        mock_endpoint(&mut server, "GET", "/status", 200, json!("Paused")).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.set_desired_state(PipelineDesiredStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopping).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopped).await;
    }

    #[tokio::test]
    async fn start_running() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineDesiredStatus::Running).await;
        test.check_current_state(PipelineStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        mock_endpoint(&mut server, "GET", "/status", 200, json!("Paused")).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        mock_endpoint(&mut server, "GET", "/start", 200, json!({})).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Running).await;
        mock_endpoint(&mut server, "GET", "/status", 200, json!("Running")).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Running).await;
        test.set_desired_state(PipelineDesiredStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopping).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopped).await;
    }

    #[tokio::test]
    async fn start_paused_then_running() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineDesiredStatus::Paused).await;
        test.check_current_state(PipelineStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        mock_endpoint(&mut server, "GET", "/status", 200, json!("Paused")).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.set_desired_state(PipelineDesiredStatus::Running).await;
        mock_endpoint(&mut server, "GET", "/start", 200, json!({})).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Running).await;
        mock_endpoint(&mut server, "GET", "/status", 200, json!("Running")).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Running).await;
        test.set_desired_state(PipelineDesiredStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopping).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopped).await;
    }

    #[tokio::test]
    async fn stop_provisioning() {
        let (_mock_server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineDesiredStatus::Paused).await;
        test.check_current_state(PipelineStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.set_desired_state(PipelineDesiredStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopping).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopped).await;
    }

    #[tokio::test]
    async fn stop_initializing() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineDesiredStatus::Paused).await;
        test.check_current_state(PipelineStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        mock_endpoint(&mut server, "GET", "/status", 200, json!("Paused")).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        test.set_desired_state(PipelineDesiredStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopping).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopped).await;
    }

    #[tokio::test]
    async fn suspend() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.set_desired_state(PipelineDesiredStatus::Paused).await;
        test.check_current_state(PipelineStatus::Stopped).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await; // provision()
        test.check_current_state(PipelineStatus::Provisioning).await;
        mock_endpoint(&mut server, "GET", "/status", 200, json!("Paused")).await;
        test.tick().await; // is_provisioned()
        test.check_current_state(PipelineStatus::Initializing).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
        test.set_desired_state(PipelineDesiredStatus::Suspended)
            .await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Suspending).await;
        mock_endpoint(
            &mut server,
            "POST",
            "/suspend",
            200,
            json!("Suspend successful"),
        )
        .await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopping).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Stopped).await;
    }
}
