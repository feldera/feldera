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
use log::{debug, error, info};
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
pub trait PipelineExecutor: Sync + Send {
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

    /// Convert a PipelineRevision into a PipelineExecutionDesc.
    async fn to_execution_desc(
        &self,
        pr: PipelineRevision,
        binary_ref: String,
    ) -> Result<PipelineExecutionDesc, ManagerError> {
        Ok(PipelineExecutionDesc {
            pipeline_id: pr.pipeline.pipeline_id,
            pipeline_name: pr.pipeline.name,
            program_id: pr.program.program_id,
            version: pr.program.version,
            config: pr.config,
            binary_ref,
        })
    }
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
    pub binary_ref: String,
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
    pub async fn run(mut self) -> Result<(), ManagerError> {
        let pipeline_id = self.pipeline_id;

        let mut poll_timeout = Self::DEFAULT_PIPELINE_POLL_PERIOD;
        loop {
            // Wait until the timeout expires or we get notified that
            // the desired state of the pipelime has changed.
            let _ = timeout(poll_timeout, self.notifier.notified()).await;
            poll_timeout = self.do_run().await.map_err(|e| {
                match e {
                    ManagerError::DBError { ref db_error } => {
                        // Pipeline deletions should not lead to errors in the logs.
                        if let DBError::UnknownPipeline { pipeline_id } = db_error {
                            info!("Pipeline {pipeline_id} no longer exists. Shutting down pipeline automaton.");
                        }
                    }
                    _ => {
                        error!(
                            "Pipeline automaton '{}' terminated with error: '{e}'",
                            pipeline_id
                        );
                    }
                }
                e
            })?;
        }
    }

    async fn do_run(&mut self) -> Result<Duration, ManagerError> {
        let mut poll_timeout = Self::DEFAULT_PIPELINE_POLL_PERIOD;
        let db = self.db.lock().await;
        let mut pipeline = db
            .get_pipeline_runtime_state_by_id(self.tenant_id, self.pipeline_id)
            .await?;
        drop(db);
        let transition: State = match (pipeline.current_status, pipeline.desired_status) {
            (PipelineStatus::Shutdown, PipelineStatus::Running)
            | (PipelineStatus::Shutdown, PipelineStatus::Paused) => {
                let db = self.db.lock().await;
                let revision = db
                    .get_pipeline_deployment(self.tenant_id, self.pipeline_id)
                    .await?;
                db.update_pipeline_runtime_state(self.tenant_id, self.pipeline_id, &pipeline)
                    .await?;
                // txn.commit();
                // Locate project executable.
                let executable_ref = db
                    .get_compiled_binary_ref(revision.program.program_id, revision.program.version)
                    .await?;
                let pipeline_id = self.pipeline_id;
                if executable_ref.is_none() {
                    return Err(RunnerError::BinaryFetchError {
                        pipeline_id,
                        error: format!("Did not receieve a compiled binary URL for {pipeline_id}"),
                    }
                    .into());
                }
                drop(db);
                let execution_desc = self
                    .pipeline_handle
                    .to_execution_desc(revision, executable_ref.unwrap())
                    .await?;

                poll_timeout = Self::PROVISIONING_POLL_PERIOD;
                // This requires start() to be idempotent. If the process crashes after start
                // is called but before the state machine is correctly updated in the DB, then
                // on restart, start will be called again
                match self.pipeline_handle.start(execution_desc).await {
                    Ok(_) => {
                        info!(
                            "Pipeline {} started (Tenant {})",
                            self.pipeline_id, self.tenant_id
                        );
                        State::Transition(PipelineStatus::Provisioning, None)
                    }
                    Err(e) => {
                        State::Transition(PipelineStatus::Failed, Some(ErrorResponse::from(&e)))
                    }
                }
            }
            // We're waiting for the pipeline's HTTP server to come online.
            // Poll its port file.  On success, go to `Initializing` state.
            (PipelineStatus::Provisioning, PipelineStatus::Running)
            | (PipelineStatus::Provisioning, PipelineStatus::Paused) => {
                match self.pipeline_handle.get_location().await {
                    Ok(Some(location)) => {
                        pipeline.set_location(location);
                        pipeline.set_created();
                        poll_timeout = Self::INITIALIZATION_POLL_PERIOD;
                        State::Transition(PipelineStatus::Initializing, None)
                    }
                    Ok(None) => {
                        if Self::timeout_expired(pipeline.status_since, Self::PROVISIONING_TIMEOUT)
                        {
                            State::Transition(
                                PipelineStatus::Failed,
                                Some(
                                    RunnerError::PipelineProvisioningTimeout {
                                        pipeline_id: self.pipeline_id,
                                        timeout: Self::PROVISIONING_TIMEOUT,
                                    }
                                    .into(),
                                ),
                            )
                        } else {
                            poll_timeout = Self::PROVISIONING_POLL_PERIOD;
                            State::Unchanged
                        }
                    }
                    Err(e) => {
                        State::Transition(PipelineStatus::Failed, Some(ErrorResponse::from(&e)))
                    }
                }
            }
            // User cancels the pipeline while it's still provisioning.
            (PipelineStatus::Provisioning, PipelineStatus::Shutdown) => {
                State::Transition(PipelineStatus::Failed, None)
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
                        if Self::timeout_expired(
                            pipeline.status_since,
                            Self::INITIALIZATION_TIMEOUT,
                        ) {
                            State::Transition(
                                PipelineStatus::Failed,
                                Some(
                                    RunnerError::PipelineInitializationTimeout {
                                        pipeline_id: self.pipeline_id,
                                        timeout: Self::INITIALIZATION_TIMEOUT,
                                    }
                                    .into(),
                                ),
                            )
                        } else {
                            poll_timeout = Self::INITIALIZATION_POLL_PERIOD;
                            State::Unchanged
                        }
                    }
                    Ok((status, body)) => {
                        if status.is_success() {
                            State::Transition(PipelineStatus::Paused, None)
                        } else if status == StatusCode::SERVICE_UNAVAILABLE {
                            if Self::timeout_expired(
                                pipeline.status_since,
                                Self::INITIALIZATION_TIMEOUT,
                            ) {
                                State::Transition(
                                    PipelineStatus::Failed,
                                    Some(
                                        RunnerError::PipelineInitializationTimeout {
                                            pipeline_id: self.pipeline_id,
                                            timeout: Self::INITIALIZATION_TIMEOUT,
                                        }
                                        .into(),
                                    ),
                                )
                            } else {
                                poll_timeout = Self::INITIALIZATION_POLL_PERIOD;
                                State::Unchanged
                            }
                        } else {
                            let error =
                                Self::error_response_from_json(self.pipeline_id, status, &body);
                            State::Transition(PipelineStatus::Failed, Some(error))
                        }
                    }
                }
            }
            // User cancels the pipeline while it is still initalizing.
            (PipelineStatus::Initializing, PipelineStatus::Shutdown) => {
                State::Transition(PipelineStatus::Failed, None)
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
                    Err(e) => State::Transition(PipelineStatus::Failed, Some(e.into())),
                    Ok((status, body)) => {
                        if status.is_success() {
                            State::Transition(PipelineStatus::Running, None)
                        } else {
                            let error =
                                Self::error_response_from_json(self.pipeline_id, status, &body);
                            State::Transition(PipelineStatus::Failed, Some(error))
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
                    Err(e) => State::Transition(PipelineStatus::Failed, Some(e.into())),
                    Ok((status, body)) => {
                        if status.is_success() {
                            State::Transition(PipelineStatus::Paused, None)
                        } else {
                            let error =
                                Self::error_response_from_json(self.pipeline_id, status, &body);
                            State::Transition(PipelineStatus::Failed, Some(error))
                        }
                    }
                }
            }
            // Issue a pipeline shutdown.
            (PipelineStatus::Running, PipelineStatus::Shutdown)
            | (PipelineStatus::Paused, PipelineStatus::Shutdown) => {
                match self.pipeline_handle.shutdown().await {
                    Ok(_) => {
                        poll_timeout = Self::SHUTDOWN_POLL_PERIOD;
                        State::Transition(PipelineStatus::ShuttingDown, None)
                    }
                    Err(e) => State::Transition(
                        PipelineStatus::Failed,
                        Some(
                            RunnerError::PipelineShutdownError {
                                pipeline_id: self.pipeline_id,
                                error: e.to_string(),
                            }
                            .into(),
                        ),
                    ),
                }
            }
            // Shutdown in progress. Wait for the pipeline process to terminate.
            (PipelineStatus::ShuttingDown, _) => {
                if self.pipeline_handle.check_if_shutdown().await {
                    State::Transition(PipelineStatus::Shutdown, None)
                } else if Self::timeout_expired(pipeline.status_since, Self::SHUTDOWN_TIMEOUT) {
                    State::Transition(
                        PipelineStatus::Failed,
                        Some(
                            RunnerError::PipelineShutdownTimeout {
                                pipeline_id: self.pipeline_id,
                                timeout: Self::SHUTDOWN_TIMEOUT,
                            }
                            .into(),
                        ),
                    )
                } else {
                    poll_timeout = Self::SHUTDOWN_POLL_PERIOD;
                    State::Unchanged
                }
            }
            // User acknowledges pipeline failure by invoking the `/shutdown` endpoint.
            // Move to the `Shutdown` state so that the pipeline can be started again.
            (PipelineStatus::Failed, PipelineStatus::Shutdown) => {
                let error = pipeline.error.clone();
                let _ = self.pipeline_handle.shutdown().await;
                State::Transition(PipelineStatus::Shutdown, error)
            }
            // Steady-state operation.  Periodically poll the pipeline.
            (PipelineStatus::Running, _)
            | (PipelineStatus::Paused, _)
            | (PipelineStatus::Failed, _) => self.probe(&mut pipeline).await?,
            (PipelineStatus::Shutdown, PipelineStatus::Shutdown) => State::Unchanged,
            _ => {
                error!(
                    "Unexpected current/desired pipeline status combination {:?}/{:?}",
                    pipeline.current_status, pipeline.desired_status
                );
                State::Unchanged
            }
        };
        if let State::Transition(new_status, error) = transition {
            debug!(
                "Pipeline {} current state is changing from {:?} to {:?} (desired: {:?})",
                self.pipeline_id, pipeline.current_status, new_status, pipeline.desired_status
            );
            pipeline.set_current_status(new_status, error);
            self.update_pipeline_runtime_state(&pipeline).await?;
        }
        Ok(poll_timeout)
    }

    async fn probe(&mut self, pipeline: &mut PipelineRuntimeState) -> Result<State, ManagerError> {
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
                if pipeline.current_status != PipelineStatus::Failed {
                    Ok(State::Transition(PipelineStatus::Failed, Some(e.into())))
                } else {
                    Ok(State::Unchanged)
                }
            }
            Ok((status, body)) => {
                if !status.is_success() {
                    if pipeline.current_status != PipelineStatus::Failed {
                        // Pipeline responds with an error, meaning that the pipeline
                        // HTTP server is still running, but the pipeline itself failed.
                        let error = Self::error_response_from_json(self.pipeline_id, status, &body);
                        Ok(State::Transition(PipelineStatus::Failed, Some(error)))
                    } else {
                        Ok(State::Unchanged)
                    }
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
                        Ok(State::Transition(PipelineStatus::Paused, None))
                    } else if state == "Running"
                        && pipeline.current_status != PipelineStatus::Running
                    {
                        Ok(State::Transition(PipelineStatus::Running, None))
                    } else if state != "Paused"
                        && state != "Running"
                        && pipeline.current_status != PipelineStatus::Failed
                    {
                        Ok(State::Transition(PipelineStatus::Failed, Some(RunnerError::HttpForwardError {
                                        pipeline_id: self.pipeline_id,
                                        error: format!("Pipeline reported unexpected status '{state}', expected 'Paused' or 'Running'")
                                    }.into())))
                    } else {
                        Ok(State::Unchanged)
                    }
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

/// Utility type for the pipeline automaton to describe state changes
enum State {
    Transition(PipelineStatus, Option<ErrorResponse>),
    Unchanged,
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
                        .truncate(true)
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use async_trait::async_trait;
    use pipeline_types::config::RuntimeConfig;
    use tokio::sync::{Mutex, Notify};
    use uuid::Uuid;

    use crate::compiler::ProgramConfig;
    use crate::config::CompilationProfile;
    use crate::db::storage::Storage;
    use crate::db::{PipelineId, PipelineStatus, ProjectDB};
    use crate::pipeline_automata::PipelineAutomaton;
    use crate::{api::ManagerError, auth::TenantRecord};

    use super::{PipelineExecutionDesc, PipelineExecutor};
    use crate::logging;
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

        async fn check_if_shutdown(&mut self) -> bool {
            true
        }

        async fn shutdown(&mut self) -> Result<(), ManagerError> {
            Ok(())
        }
    }

    struct AutomatonTest {
        conn: Arc<Mutex<ProjectDB>>,
        automaton: PipelineAutomaton<MockPipeline>,
    }

    impl AutomatonTest {
        async fn set_desired_state(&self, status: PipelineStatus) {
            let automaton = &self.automaton;
            self.conn
                .lock()
                .await
                .set_pipeline_desired_status(automaton.tenant_id, automaton.pipeline_id, status)
                .await
                .unwrap();
        }

        async fn check_current_state(&self, status: PipelineStatus) {
            let automaton = &self.automaton;
            let pipeline = self
                .conn
                .lock()
                .await
                .get_pipeline_runtime_state_by_id(automaton.tenant_id, automaton.pipeline_id)
                .await
                .unwrap();
            assert_eq!(status, pipeline.current_status);
        }

        async fn tick(&mut self) {
            self.automaton.do_run().await.unwrap();
        }
    }

    async fn setup(conn: Arc<Mutex<ProjectDB>>, uri: String) -> AutomatonTest {
        // Create some programs and pipelines before listening for changes
        let tenant_id = TenantRecord::default().id;
        let program_id = Uuid::now_v7();

        let (program_id, version) = conn
            .lock()
            .await
            .new_program(
                tenant_id,
                program_id,
                "test0",
                "program desc",
                "ignored",
                &ProgramConfig {
                    profile: Some(CompilationProfile::Unoptimized),
                },
                None,
            )
            .await
            .unwrap();
        let _ = conn
            .lock()
            .await
            .set_program_status_guarded(
                tenant_id,
                program_id,
                version,
                crate::api::ProgramStatus::Success,
            )
            .await
            .unwrap();
        let _ = conn
            .lock()
            .await
            .set_program_schema(
                tenant_id,
                program_id,
                pipeline_types::program_schema::ProgramSchema {
                    inputs: vec![],
                    outputs: vec![],
                },
            )
            .await
            .unwrap();
        let rc = RuntimeConfig::from_yaml("");
        let pipeline_id = Uuid::now_v7();
        let _ = conn
            .lock()
            .await
            .new_pipeline(
                tenant_id,
                pipeline_id,
                &Some("test0".to_string()),
                "pipeline-id",
                "2",
                &rc,
                &Some(vec![]),
                None,
            )
            .await
            .unwrap();
        let pipeline_id = PipelineId(pipeline_id);
        let _ = conn
            .lock()
            .await
            .create_pipeline_deployment(Uuid::now_v7(), tenant_id, pipeline_id)
            .await
            .unwrap();
        let _ = conn
            .lock()
            .await
            .create_compiled_binary_ref(program_id, version, "ignored".to_string())
            .await
            .unwrap();
        let notifier = Arc::new(Notify::new());
        let automaton = PipelineAutomaton::new(
            pipeline_id,
            tenant_id,
            conn.clone(),
            notifier.clone(),
            MockPipeline { uri },
        );
        AutomatonTest {
            conn: conn.clone(),
            automaton,
        }
    }

    #[tokio::test]
    async fn pipeline_start() {
        logging::init_logging("foo".into());
        let (conn, _temp) = crate::db::test::setup_pg().await;
        let conn = Arc::new(tokio::sync::Mutex::new(conn));
        // Start a background HTTP server on a random local port
        let mock_server = MockServer::start().await;
        let template = ResponseTemplate::new(200).set_body_json(r#"{}"#);

        // Simulate /stats responses.
        Mock::given(method("GET"))
            .and(path("/stats"))
            .respond_with(template)
            .mount(&mock_server)
            .await;

        let addr = mock_server.address().to_string();
        let mut test = setup(conn.clone(), addr).await;
        test.set_desired_state(PipelineStatus::Paused).await;
        test.check_current_state(PipelineStatus::Shutdown).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Provisioning).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Initializing).await;
        test.tick().await;
        test.check_current_state(PipelineStatus::Paused).await;
    }
}
