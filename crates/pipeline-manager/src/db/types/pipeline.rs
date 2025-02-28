use crate::db::error::DBError;
use crate::db::types::program::{ProgramError, ProgramStatus};
use crate::db::types::version::Version;
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
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
/// runner and observed by the API client via the `GET /v0/pipelines/{name}` endpoint.
///
/// ### The lifecycle of a pipeline
///
/// The following automaton captures the lifecycle of the pipeline.
/// Individual states and transitions of the automaton are described below.
///
/// * States labeled with the hourglass symbol (⌛) are **timed** states. The
///   automaton stays in timed state until the corresponding operation completes
///   or until it transitions to become failed after the pre-defined timeout
///   period expires.
///
/// * State transitions labeled with API endpoint names (`/start`, `/pause`,
///   `/shutdown`) are triggered by invoking corresponding endpoint,
///   e.g., `POST /v0/pipelines/{name}/start`. Note that these only express
///   desired state, and are applied asynchronously by the automata.
///
/// ```text
///                Shutdown◄────────────────────┐
///                    │                        │
///    /start or /pause│                    ShuttingDown ◄────── Failed
///                    │                        ▲                  ▲
///                    ▼              /shutdown │                  │
///             ⌛Provisioning ──────────────────┤        Shutdown, Provisioning,
///                    │                        │        Initializing, Paused,
///                    │                        │         Running, Unavailable
///                    ▼                        │    (all states except ShuttingDown
///             ⌛Initializing ──────────────────┤      can transition to Failed)
///                    │                        │
///          ┌─────────┼────────────────────────┴─┐
///          │         ▼                          │
///          │       Paused  ◄──────► Unavailable │
///          │       │    ▲                ▲      │
///          │ /start│    │/pause          │      │
///          │       ▼    │                │      │
///          │      Running ◄──────────────┘      │
///          └────────────────────────────────────┘
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
/// `GET /v0/pipelines/{name}` endpoint. In a typical scenario,
/// the user first sets the desired state, e.g., by invoking the
/// `/start` endpoint, and then polls the `GET /v0/pipelines/{name}`
/// endpoint to monitor the actual status of the pipeline until its
/// `deployment_status` attribute changes to `Running` indicating
/// that the pipeline has been successfully initialized and is
/// processing data, or `Failed`, indicating an error.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum PipelineStatus {
    /// Pipeline has not (yet) been started or has been shut down.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user triggers a deployment by invoking the `/start`
    ///    or `/pause` endpoint, after which it transitions to the
    ///    [`Provisioning`](`Self::Provisioning`) state.
    ///
    /// 2. An unexpected deployment error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Shutdown,

    /// The runner is provisioning the resources needed for the
    /// deployment of the pipeline (including storage).
    ///
    /// The deployment is performed asynchronously, as such the user
    /// is able to cancel. The pipeline remains in this state until:
    ///
    /// 1. Deployment check passed successfully, indicating all resources
    ///    were provisioned. It proceeds to transition to the
    ///    [`Initializing`](`Self::Initializing`) state.
    /// 2. A pre-defined timeout has passed; after which it
    ///    transitions to the [`Failed`](`Self::Failed`) state.
    /// 3. The user cancels the pipeline by invoking the `/shutdown` endpoint,
    ///    after which it transitions to the [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 4. An unexpected deployment error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Provisioning,

    /// The pipeline is initializing its internal state and connectors.
    ///
    /// In this state, the pipeline resources were provisioned, but the pipeline
    /// is not yet ready to be able to process data (e.g., its query engine and
    /// input and output connectors are still initializing).
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. Initialization completes successfully through a successful status check;
    ///    the pipeline transitions to the [`Paused`](`Self::Paused`) state.
    /// 2. A pre-defined timeout has passed; after which it
    ///    transitions to the [`Failed`](`Self::Failed`) state.
    /// 3. The user cancels the pipeline by invoking the `/shutdown` endpoint,
    ///    after which it transitions to the [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 4. An unexpected deployment or runtime error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Initializing,

    /// The pipeline was at least once initialized, and in the most recent status check
    /// reported its data processing is paused.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user starts the pipeline by invoking the `/start` endpoint.
    ///    The runner asynchronously passes the request to the pipeline;
    ///    transitions to the [`Running`](`Self::Running`) state.
    /// 2. The user cancels the pipeline by invoking the `/shutdown` endpoint,
    ///    after which it transitions to the [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 3. An unexpected deployment or runtime error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Paused,

    /// The pipeline was at least once initialized, and in the most recent status check
    /// reported to be processing data.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user pauses the pipeline by invoking the `/pause` endpoint.
    ///    The runner asynchronously passes the request to the pipeline;
    ///    transitions to the [`Paused`](`Self::Paused`) state.
    /// 2. The user cancels the pipeline by invoking the `/shutdown` endpoint,
    ///    after which it transitions to the [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 3. An unexpected deployment or runtime error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    Running,

    /// The pipeline was at least once initialized, but in the most recent status check either
    /// could not be reached or returned it is not yet ready.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. A status check succeeds, in which case it transitions to either
    ///    [`Paused`](`Self::Paused`) or [`Running`](`Self::Running`) state
    ///    depending on the check outcome.
    /// 2. The user cancels the pipeline by invoking the `/shutdown` endpoint,
    ///    after which it transitions to the [`ShuttingDown`](`Self::ShuttingDown`) state.
    /// 3. An unexpected deployment or runtime error renders the pipeline
    ///    [`Failed`](`Self::Failed`).
    ///
    /// Note that calls to `/start` and `/pause` express desired state and
    /// are applied asynchronously by the runner. While the pipeline is in
    /// this state, the runner will not try to reach out to start/pause
    /// until a status check has succeeded.
    Unavailable,

    /// A fatal error occurred for the pipeline.
    /// This can be caused by either the pipeline itself or its resources.
    /// The pipeline remains in this state until the user acknowledges the
    /// error by issuing a `/shutdown` request, after which it transitions
    /// to the [`ShuttingDown`](`Self::ShuttingDown`) state.
    Failed,

    /// Shutdown in progress.
    ///
    /// The pipeline resources are being terminated and deleted.
    /// The pipeline remains in this state until shutdown completes successfully,
    /// after which it transitions to the [`Shutdown`](`Self::Shutdown`) state.
    ///
    /// Shutdown (i.e., cleanup) should always be within the ability of the runner,
    /// as such it **cannot** transition to [`Failed`](`Self::Failed`) but instead
    /// can only print errors to logs.
    ShuttingDown,
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
            "unavailable" => Ok(Self::Unavailable),
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
            PipelineStatus::Unavailable => "unavailable",
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

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum PipelineDesiredStatus {
    Shutdown,
    Paused,
    Running,
}

impl TryFrom<String> for PipelineDesiredStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "shutdown" => Ok(Self::Shutdown),
            "paused" => Ok(Self::Paused),
            "running" => Ok(Self::Running),
            _ => Err(DBError::invalid_desired_pipeline_status(value)),
        }
    }
}

impl From<PipelineDesiredStatus> for &'static str {
    fn from(val: PipelineDesiredStatus) -> Self {
        match val {
            PipelineDesiredStatus::Shutdown => "shutdown",
            PipelineDesiredStatus::Paused => "paused",
            PipelineDesiredStatus::Running => "running",
        }
    }
}

impl Display for PipelineDesiredStatus {
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
            | (PipelineStatus::Shutdown, PipelineStatus::Failed)
            | (PipelineStatus::Provisioning, PipelineStatus::Initializing)
            | (PipelineStatus::Provisioning, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Provisioning, PipelineStatus::Failed)
            | (PipelineStatus::Initializing, PipelineStatus::Paused)
            | (PipelineStatus::Initializing, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Initializing, PipelineStatus::Failed)
            | (PipelineStatus::Running, PipelineStatus::Paused)
            | (PipelineStatus::Running, PipelineStatus::Unavailable)
            | (PipelineStatus::Running, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Running, PipelineStatus::Failed)
            | (PipelineStatus::Paused, PipelineStatus::Running)
            | (PipelineStatus::Paused, PipelineStatus::Unavailable)
            | (PipelineStatus::Paused, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Paused, PipelineStatus::Failed)
            | (PipelineStatus::Unavailable, PipelineStatus::Running)
            | (PipelineStatus::Unavailable, PipelineStatus::Paused)
            | (PipelineStatus::Unavailable, PipelineStatus::ShuttingDown)
            | (PipelineStatus::Unavailable, PipelineStatus::Failed)
            | (PipelineStatus::Failed, PipelineStatus::ShuttingDown)
            | (PipelineStatus::ShuttingDown, PipelineStatus::Shutdown)
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
    current_desired_status: &PipelineDesiredStatus,
    new_desired_status: &PipelineDesiredStatus,
) -> Result<(), DBError> {
    // Desired status of shutdown can always be set, as such only the cases
    // where desired is to become Paused/Running needs to be checked
    if *new_desired_status == PipelineDesiredStatus::Paused
        || *new_desired_status == PipelineDesiredStatus::Running
    {
        // Refuse to restart a pipeline that has not completed shutting down.
        // Occurs when the user calls /shutdown followed by either /start or /pause,
        // but not waiting inbetween for it to have actually become Shutdown status.
        if *current_desired_status == PipelineDesiredStatus::Shutdown
            && *current_status != PipelineStatus::Shutdown
        {
            Err(DBError::IllegalPipelineAction {
                hint: "Cannot restart the pipeline while it is shutting down. Wait for the shutdown to complete before starting the pipeline again.".to_string(),
                status: *current_status,
                desired_status: *current_desired_status,
                requested_desired_status: *new_desired_status,
            })?;
        };

        // Refuse to restart a pipeline which is failed.
        // Occurs when the user issues /start or /pause while in Failed status.
        // This check is not necessary for ShuttingDown status because it can only
        // get in that state when the desired status is Shutdown.
        if *current_desired_status != PipelineDesiredStatus::Shutdown
            && *current_status == PipelineStatus::Failed
        {
            Err(DBError::IllegalPipelineAction {
                hint: "Cannot restart a pipeline which is failed. Clear the failed error state first by invoking the '/shutdown' endpoint.".to_string(),
                status: *current_status,
                desired_status: *current_desired_status,
                requested_desired_status: *new_desired_status,
            })?;
        }
    }
    Ok(())
}

/// Pipeline descriptor.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PipelineDescr {
    /// Pipeline name.
    pub name: String,

    /// Pipeline description.
    pub description: String,

    /// Pipeline runtime configuration.
    pub runtime_config: serde_json::Value,

    /// Program SQL code.
    pub program_code: String,

    /// Rust code for UDFs.
    pub udf_rust: String,

    /// Rust dependencies in the TOML format.
    pub udf_toml: String,

    /// Program compilation configuration.
    pub program_config: serde_json::Value,
}

/// Pipeline descriptor which besides the basic fields in direct regular control of the user
/// also has all additional fields generated and maintained by the back-end.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ExtendedPipelineDescr {
    /// Assigned globally unique pipeline identifier.
    pub id: PipelineId,

    /// Pipeline name.
    pub name: String,

    /// Pipeline description.
    pub description: String,

    /// Timestamp when the pipeline was originally created.
    pub created_at: DateTime<Utc>,

    /// Pipeline version, incremented every time name, description, runtime_config, program_code,
    /// udf_rust, udf_toml, program_config or platform_version is/are modified.
    pub version: Version,

    /// Pipeline platform version.
    pub platform_version: String,

    /// Pipeline runtime configuration.
    pub runtime_config: serde_json::Value,

    /// Program SQL code.
    pub program_code: String,

    /// Rust code for UDFs.
    pub udf_rust: String,

    /// Rust dependencies in the TOML format.
    pub udf_toml: String,

    /// Program compilation configuration.
    pub program_config: serde_json::Value,

    /// Program version, incremented every time program_code, udf_rust,
    /// udf_toml, program_config or platform_version is/are modified.
    pub program_version: Version,

    /// Program compilation status.
    pub program_status: ProgramStatus,

    /// Timestamp when the current program status was set.
    pub program_status_since: DateTime<Utc>,

    /// Log, warning and error information about the program compilation.
    pub program_error: ProgramError,

    /// Program information which includes schema, input connectors and output connectors.
    /// It is set once SQL compilation has been successfully completed
    /// (i.e., the `program_status` field reaches >= `ProgramStatus::SqlCompiled`).
    pub program_info: Option<serde_json::Value>,

    /// Combined checksum of all the inputs that influenced Rust compilation to a binary.
    pub program_binary_source_checksum: Option<String>,

    /// Checksum of the binary file itself.
    pub program_binary_integrity_checksum: Option<String>,

    /// URL where to download the program binary from.
    pub program_binary_url: Option<String>,

    /// Current status of the pipeline.
    pub deployment_status: PipelineStatus,

    /// Time when the pipeline was assigned its current status
    /// of the pipeline.
    pub deployment_status_since: DateTime<Utc>,

    /// Desired pipeline status, i.e., the status requested by the user.
    pub deployment_desired_status: PipelineDesiredStatus,

    /// Error that caused the pipeline to fail.
    ///
    /// This field is only used when the `deployment_status` of the pipeline
    /// is [`Failed`](`PipelineStatus::Failed`).
    /// When present, this field contains the error that caused
    /// the pipeline to terminate abnormally.
    pub deployment_error: Option<ErrorResponse>,

    // Pipeline deployment configuration.
    pub deployment_config: Option<serde_json::Value>,

    /// Location where the pipeline can be reached at runtime
    /// (e.g., a TCP port number or a URI).
    pub deployment_location: Option<String>,

    /// Refresh version, incremented for the same fields as `version` but also including
    /// `program_info` as it contains information of interest to the user regarding the pipeline.
    /// It is a notification mechanism for users. If a user detects it changed while monitoring
    /// only the status fields, it should refresh fully (retrieve all fields).
    pub refresh_version: Version,
}

/// Pipeline descriptor which includes the fields relevant to system monitoring.
/// The advantage of this descriptor over the [`ExtendedPipelineDescr`] is that it
/// excludes fields which can be quite large (e.g., the generated Rust code stored
/// in `program_info` can become several MiB in size). This is particularly relevant
/// for monitoring in which the pipeline tuple is retrieved very frequently, which would
/// result in high CPU usage to retrieve large fields that are not of interest.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ExtendedPipelineDescrMonitoring {
    pub id: PipelineId,
    pub name: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub version: Version,
    pub platform_version: String,
    pub program_version: Version,
    pub program_status: ProgramStatus,
    pub program_status_since: DateTime<Utc>,
    pub program_error: ProgramError,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub deployment_location: Option<String>,
    pub refresh_version: Version,
}
