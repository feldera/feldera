use crate::config::CommonConfig;
use crate::db::error::DBError;
use crate::db::storage::{ExtendedPipelineDescrRunner, Storage};
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::{
    runtime_desired_status_to_string, runtime_status_to_string, ExtendedPipelineDescr,
    ExtendedPipelineDescrMonitoring, PipelineId,
};
use crate::db::types::program::{generate_pipeline_config, ProgramStatus};
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::{
    validate_deployment_config, validate_program_info, validate_runtime_config,
};
use crate::error::source_error;
use crate::is_supported_runtime;
use crate::runner::error::RunnerError;
use crate::runner::interaction::{format_pipeline_url, format_timeout_error_message};
use crate::runner::pipeline_executor::PipelineExecutor;
use crate::runner::pipeline_logs::{start_thread_pipeline_logs, LogMessage, LogsSender};
use chrono::Utc;
use feldera_observability::ReqwestTracingExt;
use feldera_types::error::ErrorResponse;
use feldera_types::runtime_status::{ExtendedRuntimeStatus, RuntimeDesiredStatus, RuntimeStatus};
use reqwest::{Method, StatusCode};
use semver::Version;
use serde_json::json;
use std::sync::Arc;
use thiserror::Error as ThisError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::{sync::Mutex, time::Duration};
use tokio::{sync::Notify, time::timeout};
use tracing::{debug, error, info, warn, Level};
use uuid::Uuid;

/// State change action that needs to be undertaken.
#[derive(Debug, PartialEq)]
enum State {
    TransitionToProvisioning {
        deployment_id: Uuid,
        deployment_config: serde_json::Value,
    },
    TransitionToProvisioned {
        deployment_location: String,
        extended_runtime_status: ExtendedRuntimeStatus,
        /// `true` if it is the transition from `Provisioning` toward `Provisioned`,
        /// `false` otherwise (it is an update to the runtime status and remains `Provisioned`).
        is_initial_transition: bool,
    },
    TransitionToStopping {
        error: Option<ErrorResponse>,
        suspend_info: Option<serde_json::Value>,
    },
    TransitionToStopped,
    StorageTransitionToCleared,
    Unchanged,
}

#[derive(ThisError, Debug)]
enum InternalRequestError {
    /// Unable to get a response because the timeout occurred before a response was returned.
    #[error("{}", format_timeout_error_message(*timeout, error))]
    Timeout { timeout: Duration, error: String },
    /// Unable to get a response for a different reason than timeout (e.g., connection refused).
    #[error("{error}")]
    Unreachable { error: String },
    /// The response body could not be parsed (e.g., incorrect format).
    #[error("{error}")]
    InvalidResponseBody { error: String },
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
    /// Current platform version. The runner is only able to start pipelines that were compiled on
    /// this platform version.
    platform_version: String,

    /// Common configuration.
    common_config: CommonConfig,

    /// Identifier of the pipeline this runner manages.
    pipeline_id: PipelineId,

    /// Identifier of the tenant that owns the pipeline.
    tenant_id: TenantId,

    /// Handle to the pipeline executor.
    pipeline_handle: T,

    /// Database connection.
    db: Arc<Mutex<StoragePostgres>>,

    /// Notifier which notifies when the table row in the pipeline table corresponding to this
    /// pipeline was added, updated or deleted.
    notifier: Arc<Notify>,

    /// HTTP client which is reused.
    client: reqwest::Client,

    /// Set when the pipeline executor `provision()` is called in the `Provisioning` stage.
    /// Content is the provisioning timeout in seconds.
    provision_called: Option<u64>,

    /// Default maximum time to wait for the pipeline resources to be provisioned.
    /// This can differ significantly between the type of runner.
    default_provisioning_timeout: Duration,

    /// Start of the grace period of provisioned during which the pipeline does not become
    /// `Unavailable` because the pipeline cannot be reached.
    provisioned_grace_period_start: Option<Instant>,

    /// Counter for how many database errors were encountered in succession.
    database_error_counter: u64,

    /// Sender to the pipeline logs.
    logs_sender: LogsSender,

    /// Terminate sender and join handle for the logs thread.
    logs_thread_terminate_sender_and_join_handle: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl<T: PipelineExecutor> PipelineAutomaton<T> {
    /// Polling period while `Stopped`. The database notification that occurs when the user changes
    /// the desired status or storage status will likely preempt this.
    const POLL_PERIOD_STOPPED: Duration = Duration::from_millis(2_500);

    /// Polling period while `Provisioning`. It will not be preempted by a database notification
    /// usually unless the user decides to prematurely stop the pipeline.
    const POLL_PERIOD_PROVISIONING: Duration = Duration::from_millis(1_000);

    /// Polling period while `Provisioned`. During normal operation, changes will likely be caused
    /// by the user changing the desired status, and as such database notification will occur. If
    /// something goes wrong with the pipeline (e.g., it can't be reached anymore, or worse, it
    /// encounters a runtime or resource error), this poll period will impact the delay until this
    /// is discovered.
    const POLL_PERIOD_PROVISIONED: Duration = Duration::from_millis(2_500);

    /// Polling period while `Stopping`. The stop operation is done synchronously, as such this
    /// period is for how long to wait to retry if it failed.
    const POLL_PERIOD_STOPPING: Duration = Duration::from_millis(1_000);

    /// Timeout for an HTTP request to the pipeline `/status` endpoint.
    const PIPELINE_STATUS_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// When the pipeline becomes `Provisioned`, it will provide the first runtime status in
    /// advance (`Initializing`). It will take some time before the pipeline HTTP server actually
    /// becomes responsive and is able to provide its own runtime status. This is the grace period
    /// during which even if the status check comes back `Unavailable`, the runtime status remains
    /// `Initializing`. If it comes back one time something different, the grace period ends.
    const PROVISIONED_GRACE_PERIOD: Duration = Duration::from_secs(20);

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
        let logs_thread_terminate_sender_and_join_handle = start_thread_pipeline_logs(
            pipeline_id.to_string(),
            follow_request_receiver,
            logs_receiver,
        );

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
            provisioned_grace_period_start: None,
            logs_sender,
            logs_thread_terminate_sender_and_join_handle: Some(
                logs_thread_terminate_sender_and_join_handle,
            ),
            database_error_counter: 0,
        }
    }

    /// Runs until the pipeline is deleted or an unexpected error occurs.
    pub async fn run(mut self) {
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
                            return;
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
        // Depending on the upcoming transition, it either retrieves the smaller monitoring
        // descriptor, or the larger complete descriptor. It needs the complete descriptor when it
        // needs to construct the `deployment_config` (when transitioning to Provisioning) or
        // to access it (when calling `provision()` during the Provisioning phase).
        //
        // - Stopped but wanting to be provisioned and is compiled at correct platform version:
        //   * status = Stopped
        //   * AND desired_status = Provisioned
        //   * AND program_status = Success AND is_supported_runtime(self.platform_version, platform_version)
        //
        // - Provisioning but wanting to be provisioned:
        //   * status = Provisioning
        //   * AND desired_status = Provisioned
        //   * AND `provision()` is not yet called
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
            pipeline.deployment_resources_status,
            pipeline.deployment_resources_desired_status,
        ) {
            // Stopped
            (
                StorageStatus::Cleared | StorageStatus::InUse,
                ResourcesStatus::Stopped,
                ResourcesDesiredStatus::Stopped,
            ) => State::Unchanged,
            (
                StorageStatus::Clearing,
                ResourcesStatus::Stopped,
                ResourcesDesiredStatus::Stopped,
            ) => self.transit_storage_clearing_to_cleared(pipeline).await,
            (
                StorageStatus::Cleared | StorageStatus::InUse,
                ResourcesStatus::Stopped,
                ResourcesDesiredStatus::Provisioned,
            ) => {
                self.transit_stopped_towards_provisioned(pipeline_monitoring_or_complete)
                    .await?
            }

            // Provisioning
            (
                StorageStatus::InUse,
                ResourcesStatus::Provisioning,
                ResourcesDesiredStatus::Provisioned,
            ) => {
                self.transit_provisioning_towards_provisioned(pipeline_monitoring_or_complete)
                    .await
            }

            // Provisioned
            (
                StorageStatus::InUse,
                ResourcesStatus::Provisioned,
                ResourcesDesiredStatus::Provisioned,
            ) => {
                self.observe_provisioned_pipeline_and_enforce_runtime_status(pipeline)
                    .await
            }

            // Stopping: as a fail-safe, all storage and desired pipeline statuses are viable
            (_, ResourcesStatus::Stopping, _) => {
                self.transit_stopping_towards_stopped(pipeline).await
            }

            // Any statuses except Stopping will transition to Stopping when going towards Stopped
            (
                StorageStatus::InUse,
                ResourcesStatus::Provisioning | ResourcesStatus::Provisioned,
                ResourcesDesiredStatus::Stopped,
            ) => State::TransitionToStopping {
                error: None,
                suspend_info: None,
            },

            // All other combinations are explicitly listed here, and should not occur.
            // As a fail-safe, they transition towards `Stopped`.
            (
                StorageStatus::Clearing,
                ResourcesStatus::Stopped,
                ResourcesDesiredStatus::Provisioned,
            )
            | (
                StorageStatus::Cleared | StorageStatus::Clearing,
                ResourcesStatus::Provisioning,
                _,
            )
            | (StorageStatus::Cleared | StorageStatus::Clearing, ResourcesStatus::Provisioned, _) => {
                State::TransitionToStopping {
                    error: Some(ErrorResponse::from(
                        &RunnerError::AutomatonImpossibleDesiredStatus {
                            current_status: pipeline.deployment_resources_status,
                            desired_status: pipeline.deployment_resources_desired_status,
                        },
                    )),
                    suspend_info: None,
                }
            }
        };

        // Store the transition in the database
        let version_guard = pipeline.version;
        let (new_resources_status, new_runtime_status, new_runtime_desired_status) =
            match &transition {
                State::TransitionToProvisioning {
                    deployment_id,
                    deployment_config,
                } => {
                    match self
                        .db
                        .lock()
                        .await
                        .transit_deployment_resources_status_to_provisioning(
                            self.tenant_id,
                            pipeline.id,
                            version_guard,
                            *deployment_id,
                            deployment_config.clone(),
                        )
                        .await
                    {
                        Ok(_) => (ResourcesStatus::Provisioning, None, None),
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
                                info!(
                                "Pipeline automaton {}: version initially intended to be started ({}) is outdated by latest ({})",
                                self.pipeline_id, outdated_version, latest_version
                            );
                                if pipeline.deployment_resources_status != ResourcesStatus::Stopped
                                {
                                    error!("Outdated pipeline version occurred when transitioning from {} to Provisioning (not Stopped)", pipeline.deployment_resources_status);
                                }
                                (ResourcesStatus::Stopped, None, None)
                            }
                            e => {
                                return Err(e);
                            }
                        },
                    }
                }
                State::TransitionToProvisioned {
                    deployment_location,
                    extended_runtime_status,
                    is_initial_transition,
                } => {
                    self.db
                        .lock()
                        .await
                        .transit_deployment_resources_status_to_provisioned(
                            self.tenant_id,
                            pipeline.id,
                            version_guard,
                            deployment_location,
                            extended_runtime_status.clone(),
                        )
                        .await?;
                    if *is_initial_transition {
                        // Start the grace period if it is the initial transition to `Provisioned`
                        self.provisioned_grace_period_start = Some(Instant::now());
                    } else {
                        // Any update to the runtime status means that it has changed, as such we can
                        // end the grace period
                        self.provisioned_grace_period_start = None;
                    }
                    (
                        ResourcesStatus::Provisioned,
                        Some(extended_runtime_status.runtime_status),
                        Some(extended_runtime_status.runtime_desired_status),
                    )
                }
                State::TransitionToStopping {
                    error,
                    suspend_info,
                } => {
                    self.db
                        .lock()
                        .await
                        .transit_deployment_resources_status_to_stopping(
                            self.tenant_id,
                            pipeline.id,
                            version_guard,
                            error.clone(),
                            suspend_info.clone(),
                        )
                        .await?;
                    (ResourcesStatus::Stopping, None, None)
                }
                State::TransitionToStopped => {
                    self.db
                        .lock()
                        .await
                        .transit_deployment_resources_status_to_stopped(
                            self.tenant_id,
                            pipeline.id,
                            version_guard,
                        )
                        .await?;
                    (ResourcesStatus::Stopped, None, None)
                }
                State::StorageTransitionToCleared => {
                    self.db
                        .lock()
                        .await
                        .transit_storage_status_to_cleared(self.tenant_id, pipeline.id)
                        .await?;
                    (
                        pipeline.deployment_resources_status,
                        pipeline.deployment_runtime_status,
                        pipeline.deployment_runtime_desired_status,
                    )
                }
                State::Unchanged => (
                    pipeline.deployment_resources_status,
                    pipeline.deployment_runtime_status,
                    pipeline.deployment_runtime_desired_status,
                ),
            };

        // Log the transition that occurred
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
                        module_path!(),
                        pipeline.name.clone(),
                        pipeline.id.to_string(),
                        Level::INFO,
                        "Storage has been cleared",
                    ))
                    .await;
            } else {
                if pipeline.deployment_resources_status != new_resources_status {
                    let message = format!(
                        "Resources transition: {} -> {} (desired: {})",
                        pipeline.deployment_resources_status,
                        new_resources_status,
                        pipeline.deployment_resources_desired_status,
                    );
                    info!("{message} for pipeline {}", pipeline.id);
                    self.logs_sender
                        .send(LogMessage::new_from_control_plane(
                            module_path!(),
                            pipeline.name.clone(),
                            pipeline.id.to_string(),
                            Level::INFO,
                            &message,
                        ))
                        .await;
                };
                if pipeline.deployment_runtime_status != new_runtime_status
                    || pipeline.deployment_runtime_desired_status != new_runtime_desired_status
                {
                    let message = format!(
                        "Runtime transition: {} -> {} (desired: {})",
                        pipeline
                            .deployment_runtime_status
                            .map(runtime_status_to_string)
                            .unwrap_or("(none)".to_string()),
                        new_runtime_status
                            .map(runtime_status_to_string)
                            .unwrap_or("(none)".to_string()),
                        new_runtime_desired_status
                            .map(runtime_desired_status_to_string)
                            .unwrap_or("(none)".to_string())
                    );
                    info!("{message} for pipeline {}", pipeline.id);
                    self.logs_sender
                        .send(LogMessage::new_from_control_plane(
                            module_path!(),
                            pipeline.name.clone(),
                            pipeline.id.to_string(),
                            Level::INFO,
                            &message,
                        ))
                        .await;
                }
            }
        }

        // Determine the poll timeout based on the pipeline status it has stayed or become.
        // This timeout can be preempted by a database notification.
        let poll_timeout = match new_resources_status {
            ResourcesStatus::Stopped => Self::POLL_PERIOD_STOPPED,
            ResourcesStatus::Provisioning => Self::POLL_PERIOD_PROVISIONING,
            ResourcesStatus::Provisioned => Self::POLL_PERIOD_PROVISIONED,
            ResourcesStatus::Stopping => Self::POLL_PERIOD_STOPPING,
        };
        Ok(poll_timeout)
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
    ) -> Result<(StatusCode, serde_json::Value), InternalRequestError> {
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
            .with_sentry_tracing()
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    InternalRequestError::Timeout {
                        timeout,
                        error: e.to_string(),
                    }
                } else {
                    InternalRequestError::Unreachable {
                        error: format!(
                            "unable to send request due to: {e}, source: {}",
                            source_error(&e)
                        ),
                    }
                }
            })?;
        let status_code = response.status();
        match response.json::<serde_json::Value>().await {
            Ok(value) => Ok((status_code, value)),
            Err(e) => Err(InternalRequestError::InvalidResponseBody {
                error: format!("unable to deserialize response as JSON due to: {e}"),
            }),
        }
    }

    /// Transits from `Stopped` towards `Provisioned`.
    async fn transit_stopped_towards_provisioned(
        &mut self,
        pipeline_monitoring_or_complete: &ExtendedPipelineDescrRunner,
    ) -> Result<State, DBError> {
        let pipeline = pipeline_monitoring_or_complete.only_monitoring();
        if pipeline.program_status == ProgramStatus::Success
            && is_supported_runtime(&self.platform_version, &pipeline.platform_version)
        {
            if let ExtendedPipelineDescrRunner::Complete(pipeline) = pipeline_monitoring_or_complete
            {
                self.transit_stopped_towards_provisioned_phase_ready(pipeline)
                    .await
            } else {
                panic!(
                    "For the transit of Stopped towards Running/Paused \
                    (program successfully compiled at a compatible platform version), \
                    the complete pipeline descriptor should have been retrieved"
                );
            }
        } else {
            self.transit_stopped_towards_provisioned_early_start(&pipeline)
                .await
        }
    }

    /// Transits from `Stopped` towards `Provisioned` when it has not yet successfully compiled at
    /// the current platform version.
    async fn transit_stopped_towards_provisioned_early_start(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> Result<State, DBError> {
        assert!(
            pipeline.program_status != ProgramStatus::Success
                || !is_supported_runtime(&self.platform_version, &pipeline.platform_version),
            "Expected to be true: {:?} != {:?} || is_compatible_runtime({:?}, {:?})",
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

        // The runner is unable to run a pipeline program compiled under an incompatible platform.
        if !is_supported_runtime(&self.platform_version, &pipeline.platform_version)
            && pipeline.program_status == ProgramStatus::Success
        {
            info!("Runner cannot start pipeline {} because its runtime version ({}) is incompatible with current ({})", pipeline.id, pipeline.platform_version, self.platform_version);

            return Ok(State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(
                    &RunnerError::AutomatonCannotProvisionUnsupportedPlatformVersion {
                        runner_platform_version: self.platform_version.clone(),
                        pipeline_platform_version: pipeline.platform_version.clone(),
                    },
                )),
                suspend_info: None,
            });
        }

        Ok(State::Unchanged)
    }

    /// Transits from `Stopped` towards `Provisioned` when it has successfully compiled at a
    /// compatible platform version.
    async fn transit_stopped_towards_provisioned_phase_ready(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> Result<State, DBError> {
        assert_eq!(pipeline.program_status, ProgramStatus::Success);
        assert!(is_supported_runtime(
            &self.platform_version,
            &pipeline.platform_version
        ));

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
        let program_info = match &pipeline.program_info {
            None => {
                return Ok(State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonMissingProgramInfo,
                    )),
                    suspend_info: None,
                });
            }
            Some(program_info) => match validate_program_info(program_info) {
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
            },
        };

        // Deployment identifier
        let deployment_id = Uuid::now_v7();

        // Deployment configuration
        let mut deployment_config = generate_pipeline_config(
            pipeline.id,
            &pipeline.name,
            &runtime_config,
            if pipeline.program_info_integrity_checksum.is_none() {
                Some(&program_info)
            } else {
                None
            },
        );
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

        Ok(State::TransitionToProvisioning {
            deployment_id,
            deployment_config,
        })
    }

    /// Transits from `Provisioning` towards `Provisioned`.
    async fn transit_provisioning_towards_provisioned(
        &mut self,
        pipeline_monitoring_or_complete: &ExtendedPipelineDescrRunner,
    ) -> State {
        if self.provision_called.is_none() {
            if let ExtendedPipelineDescrRunner::Complete(pipeline) = pipeline_monitoring_or_complete
            {
                self.transit_provisioning_towards_provisioned_phase_call(pipeline)
                    .await
            } else {
                panic!(
                    "For the transit of Provisioning towards Provisioned (provision not yet called), \
                    the complete pipeline descriptor should have been retrieved"
                );
            }
        } else {
            self.transit_provisioning_towards_provisioned_phase_await(
                &pipeline_monitoring_or_complete.only_monitoring(),
            )
            .await
        }
    }

    /// Transits from `Provisioning` towards `Provisioned` when it has not yet called `provision()`
    /// (which is idempotent).
    async fn transit_provisioning_towards_provisioned_phase_call(
        &mut self,
        pipeline: &ExtendedPipelineDescr,
    ) -> State {
        assert!(self.provision_called.is_none());

        // The runner is only able to provision a pipeline of the current platform version.
        // If in the meanwhile (e.g., due to runner restart during upgrade) the platform
        // version has changed, provisioning will fail.
        if !is_supported_runtime(&self.platform_version, &pipeline.platform_version) {
            return State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(
                    &RunnerError::AutomatonCannotProvisionUnsupportedPlatformVersion {
                        pipeline_platform_version: pipeline.platform_version.clone(),
                        runner_platform_version: self.platform_version.clone(),
                    },
                )),
                suspend_info: None,
            };
        }

        // Deployment initial runtime desired status is expected
        let deployment_initial = match &pipeline.deployment_initial {
            None => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonMissingDeploymentInitial,
                    )),
                    suspend_info: None,
                }
            }
            Some(deployment_id) => *deployment_id,
        };

        // Deployment identifier is expected
        let deployment_id = match &pipeline.deployment_id {
            None => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonMissingDeploymentId,
                    )),
                    suspend_info: None,
                }
            }
            Some(deployment_id) => *deployment_id,
        };

        // Deployment configuration is expected
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

        let Some(source_checksum) = pipeline.program_binary_source_checksum.as_ref() else {
            return State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(
                    &RunnerError::AutomatonCannotConstructProgramBinaryUrl {
                        error: "source checksum is missing".to_string(),
                    },
                )),
                suspend_info: None,
            };
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
            source_checksum,
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

        let program_info_url = if let Some(program_info_integrity_checksum) =
            pipeline.program_info_integrity_checksum.as_ref()
        {
            Some(format!(
                "{}://{}:{}/program_info/{}/{}/{}/{}",
                if self.common_config.enable_https {
                    "https"
                } else {
                    "http"
                },
                self.common_config.compiler_host,
                self.common_config.compiler_port,
                self.pipeline_id,
                pipeline.program_version,
                source_checksum,
                program_info_integrity_checksum,
            ))
        } else {
            None
        };

        let bootstrap_policy =
            if Self::platform_version_requires_bootstrap_policy(&pipeline.platform_version) {
                Some(pipeline.bootstrap_policy.unwrap_or_default())
            } else {
                None
            };

        match self
            .pipeline_handle
            .provision(
                deployment_initial,
                bootstrap_policy,
                &deployment_id,
                &deployment_config,
                &program_binary_url,
                program_info_url.as_deref(),
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

    /// Older versions of Feldera do not support the `bootstrap_policy` parameter.
    /// Pipelines compiled with these versions will fail when starting with the `--bootstrap-policy` parameter.
    ///
    /// TODO: remove this check when v0.161.0 is no longer supported.
    fn platform_version_requires_bootstrap_policy(platform_version: &str) -> bool {
        let Ok(version) = Version::parse(platform_version) else {
            // If we cannot parse it, err on the side of caution and assume it requires the `bootstrap_policy` parameter.
            return true;
        };

        if version >= Version::parse("0.164.0").unwrap() {
            return true;
        }

        false
    }

    /// Transits from `Provisioning` towards `Provisioned` when it has called `provision()` and is
    /// now awaiting `is_provisioned()` to return success before the timeout.
    async fn transit_provisioning_towards_provisioned_phase_await(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> State {
        assert!(self.provision_called.is_some());
        let provisioning_timeout = Duration::from_secs(
            self.provision_called
                .expect("Provision must have been called"),
        );

        // Deployment initial runtime desired state is expected
        let deployment_initial = match &pipeline.deployment_initial {
            None => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonMissingDeploymentInitial,
                    )),
                    suspend_info: None,
                }
            }
            Some(deployment_id) => *deployment_id,
        };

        match self.pipeline_handle.is_provisioned().await {
            Ok(Some(deployment_location)) => State::TransitionToProvisioned {
                deployment_location,
                extended_runtime_status: ExtendedRuntimeStatus {
                    runtime_status: RuntimeStatus::Initializing,
                    runtime_status_details: json!(""),
                    runtime_desired_status: deployment_initial,
                },
                is_initial_transition: true,
            },

            Ok(None) => {
                debug!(
                    "Pipeline provisioning: pipeline {} is not yet provisioned",
                    pipeline.id
                );
                if Utc::now().timestamp_millis()
                    - pipeline
                        .deployment_resources_status_since
                        .timestamp_millis()
                    > provisioning_timeout.as_millis() as i64
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

    fn parse_status_result(
        &mut self,
        deployment_initial: RuntimeDesiredStatus,
        result: Result<(StatusCode, serde_json::Value), InternalRequestError>,
    ) -> Result<ExtendedRuntimeStatus, ErrorResponse> {
        let pipeline_id = self.pipeline_id;
        match result {
            Ok((status_code, body)) => {
                // Able to reach the pipeline HTTP(S) server and get a response
                if status_code == StatusCode::OK {
                    if let Some(response_str) = body.as_str() {
                        // Backward compatibility: a JSON string is returned with 200 OK
                        match response_str {
                            "Paused" => Ok(ExtendedRuntimeStatus {
                                runtime_status: RuntimeStatus::Paused,
                                runtime_status_details: json!(""),
                                runtime_desired_status: RuntimeDesiredStatus::Paused,
                            }),
                            "Running" => Ok(ExtendedRuntimeStatus {
                                runtime_status: RuntimeStatus::Running,
                                runtime_status_details: json!(""),
                                runtime_desired_status: RuntimeDesiredStatus::Running,
                            }),
                            "Initializing" => Ok(ExtendedRuntimeStatus { // Backward compatibility: in anticipation of recent change of 503 to 200
                                runtime_status: RuntimeStatus::Initializing,
                                runtime_status_details: json!(""),
                                runtime_desired_status: RuntimeDesiredStatus::Paused,
                            }),
                            "Terminated" => Err(ErrorResponse::from(&RunnerError::PipelineInteractionInvalidResponse {
                                pipeline_name: self.pipeline_id.to_string(),
                                error: "Pipeline responded that it is Terminated".to_string(),
                            })),
                            _ => Ok(ExtendedRuntimeStatus {
                                runtime_status: RuntimeStatus::Unavailable,
                                runtime_status_details: json!(format!("Pipeline status response (200 OK) is an unexpected JSON string: '{response_str}'")),
                                runtime_desired_status: RuntimeDesiredStatus::Unavailable,
                            }),
                        }
                    } else if body.is_object() {
                        // JSON-serialized RuntimeStatusResponse is returned with 200 OK
                        match serde_json::from_value::<ExtendedRuntimeStatus>(body) {
                            Ok(response) => Ok(response),
                            Err(e) => Ok(ExtendedRuntimeStatus {
                                runtime_status: RuntimeStatus::Unavailable,
                                runtime_status_details: json!(format!("Pipeline status response (200 OK) cannot be deserialized due to: {e}")),
                                runtime_desired_status: RuntimeDesiredStatus::Unavailable,
                            }),
                        }
                    } else {
                        // JSON response must be either a string or an object.
                        Ok(ExtendedRuntimeStatus {
                            runtime_status: RuntimeStatus::Unavailable,
                            runtime_status_details: json!(format!("Pipeline status response (200 OK) is not a string or an object:\n{body:#}")),
                            runtime_desired_status: RuntimeDesiredStatus::Unavailable,
                        })
                    }
                } else if status_code == StatusCode::SERVICE_UNAVAILABLE {
                    match serde_json::from_value::<ErrorResponse>(body.clone()) {
                        Ok(error_response) => {
                            match error_response.error_code.as_ref() {
                                "Initializing" => Ok(ExtendedRuntimeStatus { // For backward compatibility
                                    runtime_status: RuntimeStatus::Initializing,
                                    runtime_status_details: json!(""),
                                    runtime_desired_status: RuntimeDesiredStatus::Paused
                                }),
                                "Suspended" => Ok(ExtendedRuntimeStatus { // For backward compatibility
                                    runtime_status: RuntimeStatus::Suspended,
                                    runtime_status_details: json!(""),
                                    runtime_desired_status: RuntimeDesiredStatus::Suspended
                                }),
                                _ => {
                                    warn!(
                                        "Pipeline {pipeline_id} status is unavailable because the endpoint responded with 503 Service Unavailable:\n{error_response:?}"
                                    );
                                    Ok(ExtendedRuntimeStatus {
                                        runtime_status: RuntimeStatus::Unavailable,
                                        runtime_status_details: json!(format!("Pipeline status response (503 Service Unavailable) is an error:\n{error_response:?}")),
                                        runtime_desired_status: RuntimeDesiredStatus::Unavailable,
                                    })
                                },
                            }
                        }
                        Err(e) => Ok(ExtendedRuntimeStatus {
                            runtime_status: RuntimeStatus::Unavailable,
                            runtime_status_details: json!(format!("Pipeline status response (503 Service Unavailable) cannot be deserialized due to: {e}. Response was:\n{body:#}")),
                            runtime_desired_status: RuntimeDesiredStatus::Unavailable,
                        })
                    }
                } else {
                    // All other status codes indicate a fatal error, where the HTTP server is still
                    // running, but the circuit has failed
                    let error_response = serde_json::from_value(body.clone()).unwrap_or_else(|e| {
                        ErrorResponse::from(&RunnerError::PipelineInteractionInvalidResponse {
                            pipeline_name: self.pipeline_id.to_string(),
                            error: format!("Error response cannot be deserialized due to: {e}. Response was:\n{body:#}"),
                        })
                    });
                    error!("Pipeline {pipeline_id} has fatal runtime error and will be stopped. Error:\n{error_response:?}");
                    Err(error_response)
                }
            }
            Err(e) => {
                if self
                    .provisioned_grace_period_start
                    .is_some_and(|t| t.elapsed() <= Self::PROVISIONED_GRACE_PERIOD)
                {
                    Ok(ExtendedRuntimeStatus {
                        runtime_status: RuntimeStatus::Initializing,
                        runtime_status_details: json!(format!("Still in the grace period for initializing. Pipeline status endpoint cannot yet be reached due to: {e}")),
                        runtime_desired_status: deployment_initial,
                    })
                } else {
                    warn!(
                        "Pipeline {pipeline_id} status is unavailable because the endpoint could not be reached due to: {e}"
                    );
                    Ok(ExtendedRuntimeStatus {
                        runtime_status: RuntimeStatus::Unavailable,
                        runtime_status_details: json!(format!(
                            "Pipeline status endpoint could not be reached due to: {e}"
                        )),
                        runtime_desired_status: RuntimeDesiredStatus::Unavailable,
                    })
                }
            }
        }
    }

    /// While in `Provisioned` status, probe the pipeline for its latest runtime status.
    async fn observe_provisioned_pipeline_and_enforce_runtime_status(
        &mut self,
        pipeline: &ExtendedPipelineDescrMonitoring,
    ) -> State {
        // While provisioned, check the health of the resources
        if let Err(e) = self.pipeline_handle.check().await {
            return State::TransitionToStopping {
                error: Some(ErrorResponse::from_error_nolog(&e)),
                suspend_info: None,
            };
        }

        // Determine deployment location
        let deployment_location = match pipeline.deployment_location.as_ref() {
            Some(deployment_location) => deployment_location,
            None => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonMissingDeploymentLocation,
                    )),
                    suspend_info: None,
                }
            }
        };

        // Determine deployment initial
        let deployment_initial = match pipeline.deployment_initial.as_ref() {
            Some(deployment_initial) => *deployment_initial,
            None => {
                return State::TransitionToStopping {
                    error: Some(ErrorResponse::from_error_nolog(
                        &RunnerError::AutomatonMissingDeploymentInitial,
                    )),
                    suspend_info: None,
                }
            }
        };

        // Retrieve runtime status
        let retrieved_status = self.parse_status_result(
            deployment_initial,
            self.http_request_pipeline_json(
                Method::GET,
                deployment_location,
                "status",
                Self::PIPELINE_STATUS_HTTP_REQUEST_TIMEOUT,
            )
            .await,
        );
        match retrieved_status {
            Ok(extended_runtime_status) => {
                if let Some(prev_runtime_status) = pipeline.deployment_runtime_status {
                    if let Some(prev_runtime_desired_status) =
                        pipeline.deployment_runtime_desired_status
                    {
                        if prev_runtime_status == extended_runtime_status.runtime_status
                            && prev_runtime_desired_status
                                == extended_runtime_status.runtime_desired_status
                        {
                            return State::Unchanged;
                        }
                    }
                }
                if extended_runtime_status.runtime_status == RuntimeStatus::Suspended {
                    info!(
                        "Pipeline {} reported it has suspended the circuit and done its last checkpoint -- stopping it", self.pipeline_id
                    );
                    return State::TransitionToStopping {
                        error: None,
                        suspend_info: Some(json!({})),
                    };
                }
                State::TransitionToProvisioned {
                    deployment_location: deployment_location.to_owned(),
                    extended_runtime_status,
                    is_initial_transition: false,
                }
            }
            Err(error_response) => State::TransitionToStopping {
                error: Some(error_response),
                suspend_info: None,
            },
        }
    }

    /// Transits from `Stopping` towards `Stopped` by deprovisioning the compute resources.
    ///
    /// The runner should always be able to eventually delete compute resources, as they are under
    /// its control. As such, it does not return an error upon failure to stop, but instead does not
    /// change the state such that it is retried.
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

    /// Transits storage status from `Clearing` towards `Cleared` by deprovisioning the storage
    /// resources.
    ///
    /// The runner should always be able to eventually delete storage resources, as they are under
    /// its control. As such, it does not return an error upon failure to stop, but instead does not
    /// change the state such that it is retried.
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
    use crate::db::types::pipeline::{PipelineDescr, PipelineId};
    use crate::db::types::program::{ProgramInfo, RustCompilationInfo, SqlCompilationInfo};
    use crate::db::types::resources_status::ResourcesStatus;
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
    use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus, RuntimeStatus};
    use serde_json::json;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::{channel, Sender};
    use tokio::sync::{Mutex, Notify};
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{http, Mock, MockServer, ResponseTemplate};

    struct MockRunner {
        deployment_location: String,
    }

    #[async_trait]
    impl PipelineExecutor for MockRunner {
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
            _: RuntimeDesiredStatus,
            _: Option<BootstrapPolicy>,
            _: &Uuid,
            _: &PipelineConfig,
            _: &str,
            _: Option<&str>,
            _: Version,
            _: Option<serde_json::Value>,
        ) -> Result<(), ManagerError> {
            Ok(())
        }

        async fn is_provisioned(&mut self) -> Result<Option<String>, ManagerError> {
            Ok(Some(self.deployment_location.clone()))
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
        automaton: PipelineAutomaton<MockRunner>,
        _follow_request_sender: Sender<Sender<String>>,
    }

    impl AutomatonTest {
        async fn desire_start(&self, initial: RuntimeDesiredStatus) {
            let automaton = &self.automaton;
            let pipeline = self
                .db
                .lock()
                .await
                .get_pipeline_by_id(automaton.tenant_id, automaton.pipeline_id)
                .await
                .unwrap();
            self.db
                .lock()
                .await
                .set_deployment_resources_desired_status_provisioned(
                    automaton.tenant_id,
                    &pipeline.name,
                    initial,
                    BootstrapPolicy::default(),
                )
                .await
                .unwrap();
        }

        async fn desire_stopped(&self) {
            let automaton = &self.automaton;
            let pipeline = self
                .db
                .lock()
                .await
                .get_pipeline_by_id(automaton.tenant_id, automaton.pipeline_id)
                .await
                .unwrap();
            self.db
                .lock()
                .await
                .set_deployment_resources_desired_status_stopped(
                    automaton.tenant_id,
                    &pipeline.name,
                )
                .await
                .unwrap();
        }

        async fn resources_status(&self) -> ResourcesStatus {
            let automaton = &self.automaton;
            self.db
                .lock()
                .await
                .get_pipeline_by_id(automaton.tenant_id, automaton.pipeline_id)
                .await
                .unwrap()
                .deployment_resources_status
        }

        async fn runtime_status(&self) -> Option<RuntimeStatus> {
            let automaton = &self.automaton;
            self.db
                .lock()
                .await
                .get_pipeline_by_id(automaton.tenant_id, automaton.pipeline_id)
                .await
                .unwrap()
                .deployment_runtime_status
        }

        async fn tick(&mut self) {
            self.automaton.do_run().await.unwrap();
        }
    }

    async fn setup(db: Arc<Mutex<StoragePostgres>>, deployment_location: String) -> AutomatonTest {
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
                    dataflow: None,
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
                "not-used-program-info-integrity-checksum",
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
                unstable_features: None,
                enable_https: false,
                https_tls_cert_path: None,
                https_tls_key_path: None,
            },
            pipeline_id,
            tenant_id,
            db.clone(),
            notifier.clone(),
            client,
            MockRunner {
                deployment_location,
            },
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

    struct MockEndpoint {
        http_method: String,
        path: String,
        response_code: u16,
        response_body: serde_json::Value,
    }

    impl MockEndpoint {
        fn new(
            http_method: &str,
            path: &str,
            response_code: u16,
            response_body: serde_json::Value,
        ) -> Self {
            Self {
                http_method: http_method.to_string(),
                path: path.to_string(),
                response_code,
                response_body,
            }
        }
    }

    /// Mocks for the server the provided endpoints.
    /// Prior mocked endpoints are cleared.
    async fn mock_endpoints(server: &mut MockServer, endpoints: Vec<MockEndpoint>) {
        server.reset().await;
        for endpoint in endpoints {
            let template =
                ResponseTemplate::new(endpoint.response_code).set_body_json(endpoint.response_body);
            Mock::given(method(
                http::Method::from_str(&endpoint.http_method).unwrap(),
            ))
            .and(path(endpoint.path))
            .respond_with(template)
            .mount(server)
            .await;
        }
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
    #[rustfmt::skip]
    async fn starting() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.desire_start(RuntimeDesiredStatus::Paused).await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopped);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioning);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await; // provision()
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioning);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await; // is_provisioned()
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioned);
        assert_eq!(test.runtime_status().await, Some(RuntimeStatus::Initializing));
        mock_endpoints(&mut server, vec![MockEndpoint::new("GET", "/status", 200, json!("Initializing"))]).await;
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioned);
        assert_eq!(test.runtime_status().await, Some(RuntimeStatus::Initializing));
        mock_endpoints(&mut server, vec![MockEndpoint::new("GET", "/status", 200, json!("Paused"))]).await;
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioned);
        assert_eq!(test.runtime_status().await, Some(RuntimeStatus::Paused));
        mock_endpoints(&mut server, vec![MockEndpoint::new("GET", "/status", 200, json!("Running"))]).await;
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioned);
        assert_eq!(test.runtime_status().await, Some(RuntimeStatus::Running));
        test.desire_stopped().await;
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopping);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopped);
        assert_eq!(test.runtime_status().await, None);
    }

    #[tokio::test]
    #[rustfmt::skip]
    async fn stop_provisioning() {
        let (_server, _temp, mut test) = setup_complete().await;
        test.desire_start(RuntimeDesiredStatus::Paused).await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopped);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioning);
        assert_eq!(test.runtime_status().await, None);
        test.desire_stopped().await;
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopping);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopped);
        assert_eq!(test.runtime_status().await, None);
    }

    #[tokio::test]
    #[rustfmt::skip]
    async fn stop_initializing() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.desire_start(RuntimeDesiredStatus::Paused).await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopped);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioning);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await; // provision()
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioning);
        assert_eq!(test.runtime_status().await, None);
        mock_endpoints(&mut server, vec![MockEndpoint::new("GET", "/status", 200, json!("Initializing"))]).await;
        test.tick().await; // is_provisioned()
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioned);
        assert_eq!(test.runtime_status().await, Some(RuntimeStatus::Initializing));
        mock_endpoints(&mut server, vec![MockEndpoint::new("GET", "/status", 200, json!("Paused"))]).await;
        test.desire_stopped().await;
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopping);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopped);
        assert_eq!(test.runtime_status().await, None);
    }

    #[tokio::test]
    #[rustfmt::skip]
    async fn detecting_suspended() {
        let (mut server, _temp, mut test) = setup_complete().await;
        test.desire_start(RuntimeDesiredStatus::Paused).await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopped);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioning);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await; // provision()
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioning);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await; // is_provisioned()
        assert_eq!(test.resources_status().await, ResourcesStatus::Provisioned);
        assert_eq!(test.runtime_status().await, Some(RuntimeStatus::Initializing));
        mock_endpoints(&mut server, vec![MockEndpoint::new("GET", "/status", 200, json!({
            "runtime_status": "Suspended",
            "runtime_status_details": "",
            "runtime_desired_status": "Suspended",
        }))]).await;
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopping);
        assert_eq!(test.runtime_status().await, None);
        test.tick().await;
        assert_eq!(test.resources_status().await, ResourcesStatus::Stopped);
        assert_eq!(test.runtime_status().await, None);
    }
}
