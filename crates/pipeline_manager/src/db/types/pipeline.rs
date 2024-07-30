use crate::db::error::DBError;
use crate::db::types::common::Version;
use crate::db::types::program::{ProgramConfig, ProgramInfo, ProgramStatus};
use chrono::{DateTime, Utc};
use pipeline_types::config::{PipelineConfig, RuntimeConfig};
use pipeline_types::error::ErrorResponse;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;
use uuid::Uuid;

/// Pipeline identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct PipelineId(
    #[cfg_attr(test, proptest(strategy = "crate::db::test::limited_uuid()"))] pub Uuid,
);
impl Display for PipelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Pipeline status.
///
/// This type represents the state of the pipeline tracked by the pipeline
/// runner and observed by the API client via the `GET /pipeline` endpoint.
///
/// ### The lifecycle of a pipeline
///
/// The following automaton captures the lifecycle of the pipeline.  Individual
/// states and transitions of the automaton are described below.
///
/// * In addition to the transitions shown in the diagram, all states have an
///   implicit "forced shutdown" transition to the `Shutdown` state.  This
///   transition is triggered when the pipeline runner is unable to communicate
///   with the pipeline and thereby forces a shutdown.
///
/// * States labeled with the hourglass symbol (⌛) are **timed** states.  The
///   automaton stays in timed state until the corresponding operation completes
///   or until the runner performs a forced shutdown of the pipeline after a
///   pre-defined timeout period.
///
/// * State transitions labeled with API endpoint names (`/deploy`, `/start`,
///   `/pause`, `/shutdown`) are triggered by invoking corresponding endpoint,
///   e.g., `POST /v0/pipelines/{pipeline_id}/start`.
///
/// ```text
///                  Shutdown◄────┐
///                     │         │
///              /deploy│         │
///                     │   ⌛ShuttingDown
///                     ▼         ▲
///             ⌛Provisioning    │
///                     │         │
///  Provisioned        │         │
///                     ▼         │/shutdown
///             ⌛Initializing    │
///                     │         │
///            ┌────────┴─────────┴─┐
///            │        ▼           │
///            │      Paused        │
///            │      │    ▲        │
///            │/start│    │/pause  │
///            │      ▼    │        │
///            │     Running        │
///            └──────────┬─────────┘
///                       │
///                       ▼
///                     Failed
/// ```
///
/// ### Desired and actual status
///
/// We use the desired state model to manage the lifecycle of a pipeline.
/// In this model, the pipeline has two status attributes associated with
/// it at runtime: the **desired** status, which represents what the user
/// would like the pipeline to do, and the **current** status, which
/// represents the actual state of the pipeline.  The pipeline runner
/// service continuously monitors both fields and steers the pipeline
/// towards the desired state specified by the user.
// Using rustdoc references in the following paragraph upsets `docusaurus`.
/// Only three of the states in the pipeline automaton above can be
/// used as desired statuses: `Paused`, `Running`, and `Shutdown`.
/// These statuses are selected by invoking REST endpoints shown
/// in the diagram.
///
/// The user can monitor the current state of the pipeline via the
/// `/status` endpoint, which returns an object of type `Pipeline`.
/// In a typical scenario, the user first sets
/// the desired state, e.g., by invoking the `/deploy` endpoint, and
/// then polls the `GET /pipeline` endpoint to monitor the actual status
/// of the pipeline until its `state.current_status` attribute changes
/// to "paused" indicating that the pipeline has been successfully
/// initialized, or "failed", indicating an error.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum PipelineStatus {
    /// Pipeline has not been started or has been shut down.
    ///
    /// The pipeline remains in this state until the user triggers
    /// a deployment by invoking the `/deploy` endpoint.
    Shutdown,

    /// The runner triggered a deployment of the pipeline and is
    /// waiting for the pipeline HTTP server to come up.
    ///
    /// In this state, the runner provisions a runtime for the pipeline
    /// (e.g., a Kubernetes pod or a local process), starts the pipeline
    /// within this runtime and waits for it to start accepting HTTP
    /// requests.
    ///
    /// The user is unable to communicate with the pipeline during this
    /// time.  The pipeline remains in this state until:
    ///
    /// 1. Its HTTP server is up and running; the pipeline transitions to the
    ///    [`Initializing`](`Self::Initializing`) state.
    /// 2. A pre-defined timeout has passed.  The runner performs forced
    ///    shutdown of the pipeline; returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    /// 3. The user cancels the pipeline by invoking the `/shutdown` endpoint.
    ///    The manager performs forced shutdown of the pipeline, returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    Provisioning,

    /// The pipeline is initializing its internal state and connectors.
    ///
    /// This state is part of the pipeline's deployment process.  In this state,
    /// the pipeline's HTTP server is up and running, but its query engine
    /// and input and output connectors are still initializing.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. Initialization completes successfully; the pipeline transitions to the
    ///    [`Paused`](`Self::Paused`) state.
    /// 2. Initialization fails; transitions to the [`Failed`](`Self::Failed`)
    ///    state.
    /// 3. A pre-defined timeout has passed.  The runner performs forced
    ///    shutdown of the pipeline; returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    /// 4. The user cancels the pipeline by invoking the `/shutdown` endpoint.
    ///    The manager performs forced shutdown of the pipeline, returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    Initializing,

    /// The pipeline is fully initialized, but data processing has been paused.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user starts the pipeline by invoking the `/start` endpoint. The
    ///    manager passes the request to the pipeline; transitions to the
    ///    [`Running`](`Self::Running`) state.
    /// 2. The user cancels the pipeline by invoking the `/shutdown` endpoint.
    ///    The manager passes the shutdown request to the pipeline to perform a
    ///    graceful shutdown; transitions to the
    ///    [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 3. An unexpected runtime error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Paused,

    /// The pipeline is processing data.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user pauses the pipeline by invoking the `/pause` endpoint. The
    ///    manager passes the request to the pipeline; transitions to the
    ///    [`Paused`](`Self::Paused`) state.
    /// 2. The user cancels the pipeline by invoking the `/shutdown` endpoint.
    ///    The runner passes the shutdown request to the pipeline to perform a
    ///    graceful shutdown; transitions to the
    ///    [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 3. An unexpected runtime error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Running,

    /// Graceful shutdown in progress.
    ///
    /// In this state, the pipeline finishes any ongoing data processing,
    /// produces final outputs, shuts down input/output connectors and
    /// terminates.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. Shutdown completes successfully; transitions to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    /// 2. A pre-defined timeout has passed.  The manager performs forced
    ///    shutdown of the pipeline; returns to the
    ///    [`Shutdown`](`Self::Shutdown`) state.
    ShuttingDown,

    /// The pipeline remains in this state until the users acknowledges the
    /// error by issuing a `/shutdown` request; transitions to the
    /// [`Shutdown`](`Self::Shutdown`) state.
    Failed,
}

impl TryFrom<String> for PipelineStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "shutdown" => Ok(Self::Shutdown),
            "provisioning" => Ok(Self::Provisioning),
            "initializing" => Ok(Self::Initializing),
            "paused" => Ok(Self::Paused),
            "running" => Ok(Self::Running),
            "failed" => Ok(Self::Failed),
            "shutting_down" => Ok(Self::ShuttingDown),
            _ => Err(DBError::invalid_pipeline_status(value)),
        }
    }
}

impl From<PipelineStatus> for &'static str {
    fn from(val: PipelineStatus) -> Self {
        match val {
            PipelineStatus::Shutdown => "shutdown",
            PipelineStatus::Provisioning => "provisioning",
            PipelineStatus::Initializing => "initializing",
            PipelineStatus::Paused => "paused",
            PipelineStatus::Running => "running",
            PipelineStatus::Failed => "failed",
            PipelineStatus::ShuttingDown => "shutting_down",
        }
    }
}

impl Display for PipelineStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status: &'static str = (*self).into();
        write!(f, "{status}")
    }
}

/// Validates the deployment status transition from current status to a new one.
pub fn validate_deployment_status_transition(
    current_status: &PipelineStatus,
    new_status: &PipelineStatus,
) -> Result<(), DBError> {
    if matches!(
        (current_status, new_status),
        (PipelineStatus::Shutdown, PipelineStatus::Provisioning)
            | (PipelineStatus::Provisioning, PipelineStatus::Initializing)
            | (PipelineStatus::Provisioning, PipelineStatus::Shutdown)
            | (PipelineStatus::Provisioning, PipelineStatus::Failed)
            | (PipelineStatus::Initializing, PipelineStatus::Paused)
            | (PipelineStatus::Initializing, PipelineStatus::Shutdown)
            | (PipelineStatus::Initializing, PipelineStatus::Failed)
            | (PipelineStatus::Running, PipelineStatus::Paused)
            | (PipelineStatus::Running, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Running, PipelineStatus::Failed)
            | (PipelineStatus::Paused, PipelineStatus::Running)
            | (PipelineStatus::Paused, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Paused, PipelineStatus::Failed)
            | (PipelineStatus::ShuttingDown, PipelineStatus::Shutdown)
            | (PipelineStatus::ShuttingDown, PipelineStatus::Failed)
            | (PipelineStatus::Shutdown, PipelineStatus::Failed)
            | (PipelineStatus::Failed, PipelineStatus::Shutdown)
    ) {
        Ok(())
    } else {
        Err(DBError::InvalidDeploymentStatusTransition {
            current: *current_status,
            transition_to: *new_status,
        })
    }
}

/// Validates the deployment desired status transition from current status to a new one.
pub fn validate_deployment_desired_status_transition(
    current_status: &PipelineStatus,
    current_desired_status: &PipelineStatus,
    new_desired_status: &PipelineStatus,
) -> Result<(), DBError> {
    // Check that the desired status can be set
    if *new_desired_status == PipelineStatus::Paused
        || *new_desired_status == PipelineStatus::Running
    {
        // Refuse to restart a pipeline that has not completed shutting down
        if *current_desired_status == PipelineStatus::Shutdown
            && *current_status != PipelineStatus::Shutdown
        {
            Err(DBError::IllegalPipelineStateTransition {
                hint: "Cannot restart the pipeline while it is shutting down. Wait for the shutdown to complete before starting the pipeline again.".to_string(),
                status: *current_status,
                desired_status: *current_desired_status,
                requested_desired_status: *new_desired_status,
            })?;
        };

        // Refuse to restart a pipeline which is failed or shutting down until it's in the shutdown state
        if *current_desired_status != PipelineStatus::Shutdown
            && (*current_status == PipelineStatus::ShuttingDown
                || *current_status == PipelineStatus::Failed)
        {
            Err(DBError::IllegalPipelineStateTransition {
                hint: "Cannot restart a pipeline which is failed or shutting down. If it is failed, clear the error state first by invoking the '/shutdown' endpoint.".to_string(),
                status: *current_status,
                desired_status: *current_desired_status,
                requested_desired_status: *new_desired_status,
            })?;
        }
    }
    Ok(())
}

/// Pipeline descriptor.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct PipelineDescr {
    /// Pipeline name.
    pub name: String,

    /// Pipeline description.
    pub description: String,

    /// Pipeline runtime configuration.
    pub runtime_config: RuntimeConfig,

    /// Program SQL code.
    pub program_code: String,

    /// Program compilation configuration.
    pub program_config: ProgramConfig,
}

/// Pipeline descriptor which besides the basic fields in direct regular control of the user
/// also has all additional fields generated and maintained by the back-end.
// TODO: add or derive proptest values
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct ExtendedPipelineDescr {
    /// Assigned globally unique pipeline identifier.
    pub id: PipelineId,

    /// Pipeline name.
    pub name: String,

    /// Pipeline description.
    pub description: String,

    /// Pipeline version, incremented every time name, description, config, program_code or
    /// program_config is/are modified.
    pub version: Version,

    /// Timestamp when the pipeline was originally created.
    pub created_at: DateTime<Utc>,

    /// Pipeline runtime configuration.
    pub runtime_config: RuntimeConfig,

    /// Program SQL code.
    pub program_code: String,

    /// Program compilation configuration.
    pub program_config: ProgramConfig,

    /// Program version, incremented every time program_code is modified.
    pub program_version: Version,

    /// Program compilation status.
    pub program_status: ProgramStatus,

    /// Timestamp when the current program status was set.
    pub program_status_since: DateTime<Utc>,

    /// Program information which includes schema, input connectors and output connectors.
    /// It is set once SQL compilation has been successfully completed
    /// (i.e., the `program_status` field reaches >= `ProgramStatus::CompilingRust`).
    pub program_info: Option<ProgramInfo>,

    /// URL where to download the program binary from.
    /// TODO: should this be in here or not?
    pub program_binary_url: Option<String>,

    /// Current status of the pipeline.
    pub deployment_status: PipelineStatus,

    /// Time when the pipeline was assigned its current status
    /// of the pipeline.
    pub deployment_status_since: DateTime<Utc>,

    /// Desired pipeline status, i.e., the status requested by the user.
    ///
    /// Possible values are:
    /// [`Shutdown`](`PipelineStatus::Shutdown`),
    /// [`Paused`](`PipelineStatus::Paused`), and
    /// [`Running`](`PipelineStatus::Running`).
    pub deployment_desired_status: PipelineStatus,

    /// Error that caused the pipeline to fail.
    ///
    /// This field is only used when the `deployment_status` of the pipeline
    /// is [`Failed`](`PipelineStatus::Failed`).
    /// When present, this field contains the error that caused
    /// the pipeline to terminate abnormally.
    pub deployment_error: Option<ErrorResponse>,

    // Pipeline configuration.
    pub deployment_config: Option<PipelineConfig>,

    /// Location where the pipeline can be reached at runtime.
    /// e.g., a TCP port number or a URI.
    pub deployment_location: Option<String>,
}

impl ExtendedPipelineDescr {
    /// Returns true if the pipeline is fully shutdown, which means both
    /// its current deployment status and desired status are `Shutdown`.
    pub(crate) fn is_fully_shutdown(&self) -> bool {
        self.deployment_status == PipelineStatus::Shutdown
            && self.deployment_desired_status == PipelineStatus::Shutdown
    }
}
