use crate::db::error::DBError;
use crate::db::types::program::{ProgramError, ProgramStatus};
use crate::db::types::storage::StorageStatus;
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
///   `/suspend`, `/stop`) are triggered by invoking corresponding endpoint,
///   e.g., `POST /v0/pipelines/{name}/start`. Note that these only express
///   desired state, and are applied asynchronously by the automata.
///
/// ```text
///                 Stopped ◄─────────── Stopping ◄───── All states can transition
///                    │                    ▲            to Stopping by either:
///   /start or /pause │                    │            (1) user calling /stop, or;
///                    ▼                    │            (2) pipeline encountering a fatal
///             ⌛Provisioning          Suspending            resource or runtime error,
///                    │                    ▲                having the system call /stop
///                    ▼                    │ /suspend       effectively
///             ⌛Initializing ──────────────┤
///                    │                    │
///          ┌─────────┼────────────────────┴─────┐
///          │         ▼                          │
///          │       Paused  ◄──────► Unavailable │
///          │        │   ▲                ▲      │
///          │ /start │   │  /pause        │      │
///          │        ▼   │                │      │
///          │       Running ◄─────────────┘      │
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
///
/// Only four of the states in the pipeline automaton above can be
/// used as desired statuses: `Paused`, `Running`, `Suspended` and
/// `Stopped`. These statuses are selected by invoking REST endpoints
/// shown in the diagram (respectively, `/pause`, `/start`, `/suspend`
/// and `/stop`).
///
/// The user can monitor the current state of the pipeline via the
/// `GET /v0/pipelines/{name}` endpoint. In a typical scenario,
/// the user first sets the desired state, e.g., by invoking the
/// `/start` endpoint, and then polls the `GET /v0/pipelines/{name}`
/// endpoint to monitor the actual status of the pipeline until its
/// `deployment_status` attribute changes to `Running` indicating
/// that the pipeline has been successfully initialized and is
/// processing data, or `Stopped` with `stop_by_error` being set.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum PipelineStatus {
    /// Pipeline has not (yet) been started or has been stopped either
    /// manually by the user or automatically by the system because
    /// a resource or runtime error was encountered.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user triggers a start by invoking the `/start` or `/pause` endpoint.
    ///    It transitions to `Provisioning`.
    ///
    /// 2. If applicable, early start fails (e.g., pipeline fails to compile).
    ///    It transitions to `Stopping`.
    Stopped,

    /// The compute and (optionally) storage resources needed for the
    /// running the pipeline are being provisioned.
    ///
    /// The provisioning is performed asynchronously, as such the user
    /// is able to cancel.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. Resources check passes, indicating all resources were provisioned.
    ///    It transitions to `Initializing`.
    ///
    /// 2. Resource provisioning fails or takes too long (timeout exceeded).
    ///    It transitions to `Stopping` with `stop_by_error` set.
    ///
    /// 3. The user stops the pipeline by invoking the `/stop` endpoint.
    ///    It transitions to `Stopping`.
    Provisioning,

    /// The pipeline is initializing its internal state and connectors.
    ///
    /// In this state, the pipeline resources were provisioned, but the pipeline
    /// is not yet ready to be able to process data (e.g., its query engine and
    /// input and output connectors are still initializing).
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. Initialization check passes, indicating pipeline is ready.
    ///    It transitions to `Paused` state.
    ///
    /// 2. Resource error or runtime error is encountered,
    ///    or initialization takes too long (timeout exceeded).
    ///    It transitions to `Stopping` with `stop_by_error` set.
    ///
    /// 3. The user suspends the pipeline by invoking the `/suspend` endpoint.
    ///    It transitions to `Suspending`.
    ///
    /// 4. The user stops the pipeline by invoking the `/stop` endpoint.
    ///    It transitions to `Stopping`.
    Initializing,

    /// The pipeline was at least once initialized, and in the most recent status check
    /// reported its data processing is paused.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user starts the pipeline by invoking the `/start` endpoint.
    ///    It asynchronously passes the request to the pipeline, and upon
    ///    success transitions to `Running`.
    ///
    /// 2. Resource error or runtime error is encountered.
    ///    It transitions to `Stopping` with `stop_by_error` set.
    ///
    /// 3. The user suspends the pipeline by invoking the `/suspend` endpoint.
    ///    It transitions to `Suspending`.
    ///
    /// 4. The user stops the pipeline by invoking the `/stop` endpoint.
    ///    It transitions to `Stopping`.
    Paused,

    /// The pipeline was at least once initialized, and in the most recent status check
    /// reported to be processing data.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. The user pauses the pipeline by invoking the `/pause` endpoint.
    ///    It asynchronously passes the request to the pipeline, and upon
    ///    transitions to `Paused`.
    ///
    /// 2. Resource error or runtime error is encountered.
    ///    It transitions to `Stopping` with `stop_by_error` set.
    ///
    /// 3. The user suspends the pipeline by invoking the `/suspend` endpoint.
    ///    It transitions to `Suspending`.
    ///
    /// 4. The user stops the pipeline by invoking the `/stop` endpoint.
    ///    It transitions to `Stopping`.
    Running,

    /// The pipeline was at least once initialized, but in the most recent status check either
    /// could not be reached or returned it is not yet ready.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. A status check succeeds, in which case it transitions to either
    ///    `Paused` or `Running` depending on the check outcome.
    ///
    /// 2. Resource error or runtime error is encountered.
    ///    It transitions to `Stopping` with `stop_by_error` set.
    ///
    /// 3. The user suspends the pipeline by invoking the `/suspend` endpoint.
    ///    It transitions to `Suspending`.
    ///
    /// 4. The user stops the pipeline by invoking the `/stop` endpoint.
    ///    It transitions to `Stopping`.
    ///
    /// Note that calls to `/start` and `/pause` express desired state and
    /// are applied asynchronously. While the pipeline is in this state,
    /// the runner will not try to reach out to start/pause until a status
    /// check has succeeded.
    Unavailable,

    /// The pipeline is being requested to suspend the circuit to storage.
    ///
    /// The pipeline remains in this state until:
    ///
    /// 1. A suspend request succeeds, in which case it transitions to
    ///    `Stopping` with `suspend_info` set.
    ///
    /// 2. Resource error or runtime error is encountered.
    ///    It transitions to `Stopping` with `stop_by_error` set.
    Suspending,

    /// The compute resources of the pipeline are being scaled down to zero.
    ///
    /// The pipeline remains in this state until the compute resources are deallocated,
    /// after which it transitions to `Stopped`.
    Stopping,
}

impl TryFrom<String> for PipelineStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "stopped" => Ok(Self::Stopped),
            "provisioning" => Ok(Self::Provisioning),
            "initializing" => Ok(Self::Initializing),
            "paused" => Ok(Self::Paused),
            "running" => Ok(Self::Running),
            "unavailable" => Ok(Self::Unavailable),
            "suspending" => Ok(Self::Suspending),
            "stopping" => Ok(Self::Stopping),
            _ => Err(DBError::invalid_pipeline_status(value)),
        }
    }
}

impl From<PipelineStatus> for &'static str {
    fn from(val: PipelineStatus) -> Self {
        match val {
            PipelineStatus::Stopped => "stopped",
            PipelineStatus::Provisioning => "provisioning",
            PipelineStatus::Initializing => "initializing",
            PipelineStatus::Paused => "paused",
            PipelineStatus::Running => "running",
            PipelineStatus::Unavailable => "unavailable",
            PipelineStatus::Suspending => "suspending",
            PipelineStatus::Stopping => "stopping",
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
    Stopped,
    Paused,
    Running,
    Suspended,
}

impl TryFrom<String> for PipelineDesiredStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "stopped" => Ok(Self::Stopped),
            "paused" => Ok(Self::Paused),
            "running" => Ok(Self::Running),
            "suspended" => Ok(Self::Suspended),
            _ => Err(DBError::invalid_desired_pipeline_status(value)),
        }
    }
}

impl From<PipelineDesiredStatus> for &'static str {
    fn from(val: PipelineDesiredStatus) -> Self {
        match val {
            PipelineDesiredStatus::Stopped => "stopped",
            PipelineDesiredStatus::Paused => "paused",
            PipelineDesiredStatus::Running => "running",
            PipelineDesiredStatus::Suspended => "suspended",
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
#[rustfmt::skip]
pub fn validate_deployment_status_transition(
    storage_status: StorageStatus,
    current_status: PipelineStatus,
    new_status: PipelineStatus,
) -> Result<(), DBError> {
    if matches!(
        (storage_status, current_status, new_status),
        (StorageStatus::Unbound | StorageStatus::Bound,  PipelineStatus::Stopped, PipelineStatus::Provisioning)
        | (StorageStatus::Unbound | StorageStatus::Bound, PipelineStatus::Stopped, PipelineStatus::Stopping)
        | (StorageStatus::Bound,PipelineStatus::Provisioning, PipelineStatus::Initializing)
        | (StorageStatus::Bound, PipelineStatus::Provisioning, PipelineStatus::Stopping)
        | (StorageStatus::Bound, PipelineStatus::Initializing, PipelineStatus::Paused)
        | (StorageStatus::Bound, PipelineStatus::Initializing, PipelineStatus::Suspending)
        | (StorageStatus::Bound, PipelineStatus::Initializing, PipelineStatus::Stopping)
        | (StorageStatus::Bound, PipelineStatus::Running, PipelineStatus::Paused)
        | (StorageStatus::Bound, PipelineStatus::Running, PipelineStatus::Unavailable)
        | (StorageStatus::Bound, PipelineStatus::Running, PipelineStatus::Suspending)
        | (StorageStatus::Bound, PipelineStatus::Running, PipelineStatus::Stopping)
        | (StorageStatus::Bound, PipelineStatus::Paused, PipelineStatus::Running)
        | (StorageStatus::Bound, PipelineStatus::Paused, PipelineStatus::Unavailable)
        | (StorageStatus::Bound, PipelineStatus::Paused, PipelineStatus::Suspending)
        | (StorageStatus::Bound, PipelineStatus::Paused, PipelineStatus::Stopping)
        | (StorageStatus::Bound, PipelineStatus::Unavailable, PipelineStatus::Running)
        | (StorageStatus::Bound, PipelineStatus::Unavailable, PipelineStatus::Paused)
        | (StorageStatus::Bound, PipelineStatus::Unavailable, PipelineStatus::Suspending)
        | (StorageStatus::Bound, PipelineStatus::Unavailable, PipelineStatus::Stopping)
        | (StorageStatus::Bound, PipelineStatus::Suspending, PipelineStatus::Stopping)
        | (StorageStatus::Unbound | StorageStatus::Bound, PipelineStatus::Stopping, PipelineStatus::Stopped)
    ) {
        Ok(())
    } else {
        Err(DBError::InvalidDeploymentStatusTransition {
            storage_status,
            current_status,
            new_status,
        })
    }
}

/// Validates the deployment desired status transition from current status to a new one.
pub fn validate_deployment_desired_status_transition(
    pipeline_status: PipelineStatus,
    current_desired_status: PipelineDesiredStatus,
    new_desired_status: PipelineDesiredStatus,
) -> Result<(), DBError> {
    match new_desired_status {
        PipelineDesiredStatus::Stopped => {
            // It's always possible to stop a pipeline
            Ok(())
        }
        PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running => {
            if current_desired_status == PipelineDesiredStatus::Stopped
                && pipeline_status != PipelineStatus::Stopped
            {
                // After calling `/stop`, it must first become `Stopped`
                return Err(DBError::IllegalPipelineAction {
                    pipeline_status,
                    current_desired_status,
                    new_desired_status,
                    hint: "Cannot restart the pipeline while it is stopping. Wait until it is stopped before starting the pipeline again.".to_string(),
                });
            };

            if current_desired_status == PipelineDesiredStatus::Suspended {
                // After calling `/suspend`, it must first become `Stopped`
                return Err(DBError::IllegalPipelineAction {
                    pipeline_status,
                    current_desired_status,
                    new_desired_status,
                    hint: "Cannot resume the pipeline while it is suspending. Wait until is stopped after the suspend before resuming the pipeline.".to_string(),
                });
            };

            Ok(())
        }
        PipelineDesiredStatus::Suspended => {
            match current_desired_status {
                PipelineDesiredStatus::Stopped => Err(DBError::IllegalPipelineAction {
                    pipeline_status,
                    current_desired_status,
                    new_desired_status,
                    hint: "Cannot suspend a pipeline which is stopping.".to_string(),
                }),
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running => {
                    if matches!(
                        pipeline_status,
                        PipelineStatus::Running
                            | PipelineStatus::Paused
                            | PipelineStatus::Unavailable
                    ) {
                        // Calling `/suspend` is only possible if the pipeline is Running, Paused or Unavailable.
                        Ok(())
                    } else {
                        // In other cases, it is not possible. Sometimes it will become possible eventually
                        // (e.g., `Provisioning` should eventually generally transition to these states).
                        Err(DBError::IllegalPipelineAction {
                            pipeline_status,
                            current_desired_status,
                            new_desired_status,
                            hint: "Cannot suspend a pipeline which is not initializing, running, paused or unavailable."
                                .to_string(),
                        })
                    }
                }
                PipelineDesiredStatus::Suspended => Ok(()),
            }
        }
    }
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

    /// Resource or runtime error that caused the pipeline to stop unexpectedly.
    ///
    /// Can only be set when [`Stopping`](`PipelineStatus::Stopping`)
    /// or [`Stopped`](`PipelineStatus::Stopped`).
    pub deployment_error: Option<ErrorResponse>,

    // Pipeline deployment configuration.
    pub deployment_config: Option<serde_json::Value>,

    /// Location where the pipeline can be reached at runtime
    /// (e.g., a TCP port number or a URI).
    pub deployment_location: Option<String>,

    /// Refresh version, incremented for the same fields as `version` but also including
    /// `program_info` and `program_error` as it contains information of interest to the user
    /// regarding the pipeline. It is a notification mechanism for users. If a user detects
    /// it changed while monitoring only the status fields, it should refresh fully (retrieve
    /// all fields).
    pub refresh_version: Version,

    /// Information about the current suspended state.
    ///
    /// Can only be set when [`Stopping`](`PipelineStatus::Stopping`)
    /// or [`Stopped`](`PipelineStatus::Stopped`).
    pub suspend_info: Option<serde_json::Value>,

    /// Storage status.
    pub storage_status: StorageStatus,
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
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub deployment_location: Option<String>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
}
