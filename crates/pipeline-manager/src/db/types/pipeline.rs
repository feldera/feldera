use crate::db::error::DBError;
use crate::db::types::program::{ProgramError, ProgramStatus};
use crate::db::types::resources_status::{
    validate_resources_desired_status_transition, validate_resources_status_transition,
    ResourcesDesiredStatus, ResourcesStatus,
};
use crate::db::types::storage::StorageStatus;
use crate::db::types::version::Version;
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
use feldera_types::runtime_status::{RuntimeDesiredStatus, RuntimeStatus};
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
/// This type represents the status of the pipeline tracked by the pipeline runner and observed by
/// the API client via the `GET /v0/pipelines/{name}` endpoint.
///
/// ### The lifecycle of a pipeline
///
/// The following automaton captures the lifecycle of the pipeline.
/// Individual statuses and transitions of the automaton are described below.
///
/// * Statuses labeled with the hourglass symbol (⌛) are **timed** statuses. The automaton stays in
///   timed status until the corresponding operation completes or until it transitions to become
///   failed after the pre-defined timeout  period expires.
///
/// * Some transitions can be initiated by calling an API endpoint (e.g., `/start`). These
///   endpoints only express desired state, and are applied asynchronously by the automaton.
///
/// ```text
///          /start /pause /standby (early start failed)
///          ┌───────────────────┐
///          │                   ▼
///       Stopped ◄────────── Stopping
///   /start │                   ▲
///   /pause │                   │ /stop
/// /standby │                   │ OR: timeout (from Provisioning)
///          ▼                   │ OR: fatal runtime or resource error
///    ⌛Provisioning ────────────│
///          │                   │
///          │                   │
///          ▼                   │
///      ┌───────────────────────┴─────┐
///      │ Initializing, Unavailable,  │
///      │   Standby, Bootstrapping,   │
///      │ Replaying, Paused, Running, │
///      │    Suspended, Terminated    │
///      └─────────────────────────────┘
///  Runtime statuses, can be changed by calling
///     /start, /pause, /standby, and /stop
/// ```
///
/// ### Desired and actual status
///
/// We use the desired state model to manage the lifecycle of a pipeline. In this model, the
/// pipeline has two status attributes associated with it: the **desired** status, which represents
/// what the user would like the pipeline to do, and the **current** status, which represents the
/// actual (last observed) status of the pipeline. The pipeline runner service continuously monitors
/// the desired status field to decide where to steer the pipeline towards.
///
/// There are five desired statuses:
/// - `Standby` (set by invoking `/standby`)
/// - `Running` (set by invoking `/start`)
/// - `Paused` (set by invoking `/pause`)
/// - `Suspended` (set by invoking `/stop?force=false`)
/// - `Stopped` (set by invoking `/stop?force=true`)
///
/// Of these, `Suspended` is a "virtual" desired status. Once the runner has successfully suspended,
/// it will change the desired status to `Stopped`. Not all endpoints can be called at all times.
///
/// The user can monitor the current status of the pipeline via the `GET /v0/pipelines/{name}`
/// endpoint. In a typical scenario, the user first sets the desired status, e.g., by invoking the
/// `/start` endpoint, and then polls the `GET /v0/pipelines/{name}` endpoint to monitor the actual
/// status of the pipeline until its `deployment_status` attribute changes to `Running` indicating
/// that the pipeline has been successfully provisioned, or `Stopped` with `deployment_error` being
/// set.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum PipelineStatus {
    /// Pipeline has not (yet) been started or has been stopped either manually by the user or
    /// automatically by the system because a resource or runtime error was encountered.
    ///
    /// No compute resources are provisioned, but there might still be storage resources
    /// provisioned. They can be deprovisioned using `/clear`.
    ///
    /// The pipeline remains in this status until:
    ///
    /// 1. The user triggers a start by invoking the `/start` endpoint.
    ///    It transitions to `Provisioning`.
    ///
    /// 2. If applicable, early start fails (e.g., pipeline fails to compile).
    ///    It transitions to `Stopping`.
    Stopped,

    /// The compute and (optionally) storage resources needed for the running the pipeline process
    /// are being provisioned.
    ///
    /// The provisioning is performed asynchronously, as such the user is able to cancel.
    ///
    /// The pipeline remains in this status until:
    ///
    /// 1. Resources check passes, indicating all resources were provisioned.
    ///    It transitions to `Provisioned`.
    ///
    /// 2. Resource provisioning fails or takes too long (timeout exceeded).
    ///    It transitions to `Stopping` with `deployment_error` set.
    ///
    /// 3. The user stops the pipeline by invoking the `/stop` endpoint.
    ///    It transitions to `Stopping`.
    Provisioning,

    /// The pipeline was at least once initialized, but in the most recent status check either
    /// could not be reached or returned it is not yet ready.
    ///
    /// The pipeline remains in this status until:
    /// - It is available again
    /// - Resource or runtime error
    /// - User calls `/stop`
    Unavailable,

    /// The pipeline constantly pulling the latest checkpoint to S3 but not processing any inputs.
    ///
    /// The pipeline remains in this status until:
    /// - Resource or runtime error
    /// - User calls `/start` or `/pause`
    /// - User calls `/stop`
    Standby,

    /// The input and output connectors are establishing connections to their data sources and sinks
    /// respectively.
    ///
    /// The pipeline remains in this status until:
    /// - Initialization finishes
    /// - Resource or runtime error
    /// - User calls `/stop`
    Initializing,

    /// The pipeline was modified since the last time it was started, and as such it is currently
    /// computing modified views.
    ///
    /// The pipeline remains in this status until:
    /// - Bootstrapping finishes
    /// - Resource or runtime error
    /// - User calls `/stop`
    Bootstrapping,

    /// Input records that were stored in the journal but were not yet processed, are being
    /// processed first.
    ///
    /// The pipeline remains in this status until:
    /// - Replaying finishes
    /// - Resource or runtime error
    /// - User calls `/stop`
    Replaying,

    /// The input connectors are paused.
    ///
    /// The pipeline remains in this status until:
    /// - Resource or runtime error
    /// - User calls `/start`
    /// - User calls `/stop`
    Paused,

    /// The input connectors are running.
    ///
    /// The pipeline remains in this status until:
    /// - Resource or runtime error
    /// - User calls `/pause`
    /// - User calls `/stop`
    Running,

    /// The compute resources of the pipeline are being deprovisioned.
    ///
    /// The pipeline remains in this status until the compute resources are deprovisioned, after
    /// which it transitions to `Stopped`.
    Stopping,
}

impl PipelineStatus {
    pub fn as_resources_status(&self) -> ResourcesStatus {
        match self {
            PipelineStatus::Stopped => ResourcesStatus::Stopped,
            PipelineStatus::Provisioning => ResourcesStatus::Provisioning,
            PipelineStatus::Stopping => ResourcesStatus::Stopping,
            _ => ResourcesStatus::Provisioned,
        }
    }

    pub fn as_runtime_status(&self) -> Option<RuntimeStatus> {
        match self {
            PipelineStatus::Stopped => None,
            PipelineStatus::Provisioning => None,
            PipelineStatus::Unavailable => Some(RuntimeStatus::Unavailable),
            PipelineStatus::Standby => Some(RuntimeStatus::Standby),
            PipelineStatus::Initializing => Some(RuntimeStatus::Initializing),
            PipelineStatus::Bootstrapping => Some(RuntimeStatus::Bootstrapping),
            PipelineStatus::Replaying => Some(RuntimeStatus::Replaying),
            PipelineStatus::Paused => Some(RuntimeStatus::Paused),
            PipelineStatus::Running => Some(RuntimeStatus::Running),
            PipelineStatus::Stopping => None,
        }
    }
}

impl TryFrom<String> for PipelineStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "stopped" => Ok(Self::Stopped),
            "provisioning" => Ok(Self::Provisioning),
            "standby" => Ok(Self::Standby),
            "initializing" => Ok(Self::Initializing),
            "bootstrapping" => Ok(Self::Bootstrapping),
            "replaying" => Ok(Self::Replaying),
            "paused" => Ok(Self::Paused),
            "running" => Ok(Self::Running),
            "unavailable" => Ok(Self::Unavailable),
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
            PipelineStatus::Standby => "standby",
            PipelineStatus::Initializing => "initializing",
            PipelineStatus::Bootstrapping => "bootstrapping",
            PipelineStatus::Replaying => "replaying",
            PipelineStatus::Paused => "paused",
            PipelineStatus::Running => "running",
            PipelineStatus::Unavailable => "unavailable",
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
    Standby,
    Paused,
    Running,
    Suspended,
}

impl PipelineDesiredStatus {
    pub fn as_resources_desired_status(&self) -> ResourcesDesiredStatus {
        match self {
            PipelineDesiredStatus::Stopped => ResourcesDesiredStatus::Stopped,
            PipelineDesiredStatus::Standby => ResourcesDesiredStatus::Provisioned,
            PipelineDesiredStatus::Paused => ResourcesDesiredStatus::Provisioned,
            PipelineDesiredStatus::Running => ResourcesDesiredStatus::Provisioned,
            PipelineDesiredStatus::Suspended => ResourcesDesiredStatus::Provisioned,
        }
    }

    pub fn as_runtime_desired_status(&self) -> Option<RuntimeDesiredStatus> {
        match self {
            PipelineDesiredStatus::Stopped => None,
            PipelineDesiredStatus::Standby => Some(RuntimeDesiredStatus::Standby),
            PipelineDesiredStatus::Paused => Some(RuntimeDesiredStatus::Paused),
            PipelineDesiredStatus::Running => Some(RuntimeDesiredStatus::Running),
            PipelineDesiredStatus::Suspended => Some(RuntimeDesiredStatus::Suspended),
        }
    }
}

impl TryFrom<String> for PipelineDesiredStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "stopped" => Ok(Self::Stopped),
            "standby" => Ok(Self::Standby),
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
            PipelineDesiredStatus::Standby => "standby",
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
    // Check that the underlying resources status transition is valid
    validate_resources_status_transition(storage_status, current_status, new_status)?;

    // All other (runtime) transitions are currently allowed, as they are strictly observed.
    Ok(())
}

/// Validates the deployment desired status transition from current status to a new one.
pub fn validate_deployment_desired_status_transition(
    pipeline_status: PipelineStatus,
    current_desired_status: PipelineDesiredStatus,
    new_desired_status: PipelineDesiredStatus,
) -> Result<(), DBError> {
    // Check that the underlying resources desired status transition is valid
    validate_resources_desired_status_transition(
        pipeline_status,
        current_desired_status,
        new_desired_status,
    )?;

    // What remains are validating whether runtime status transitions can be performed
    match new_desired_status {
        PipelineDesiredStatus::Stopped => {
            // It's always possible to stop a pipeline
            Ok(())
        }
        PipelineDesiredStatus::Standby => {
            if current_desired_status != PipelineDesiredStatus::Stopped
                && current_desired_status != PipelineDesiredStatus::Standby
            {
                return Err(DBError::IllegalPipelineAction {
                    pipeline_status,
                    current_desired_status,
                    new_desired_status,
                    hint: "Cannot start into or transition into standby mode if it has already been set to become running or paused.".to_string(),
                });
            };
            Ok(())
        }
        PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running => {
            if current_desired_status == PipelineDesiredStatus::Suspended {
                // After calling `/stop?force=false`, it must first become `Stopped`
                return Err(DBError::IllegalPipelineAction {
                    pipeline_status,
                    current_desired_status,
                    new_desired_status,
                    hint: "Cannot set a pipeline to become running or paused while it is suspending. Wait until it is stopped after the suspend.".to_string(),
                });
            };
            Ok(())
        }
        PipelineDesiredStatus::Suspended => {
            if current_desired_status == PipelineDesiredStatus::Standby {
                return Err(DBError::IllegalPipelineAction {
                    pipeline_status,
                    current_desired_status,
                    new_desired_status,
                    hint: "Cannot suspend a pipeline which is standby.".to_string(),
                });
            }
            Ok(())
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

impl PipelineDescr {
    #[cfg(test)]
    pub(crate) fn test_descr() -> Self {
        use serde_json::json;
        Self {
            name: "test_pipeline".to_string(),
            description: "Test pipeline".to_string(),
            runtime_config: json!({}),
            program_code: "CREATE TABLE test (col1 INT);".to_string(),
            udf_rust: "".to_string(),
            udf_toml: "".to_string(),
            program_config: json!({
                "profile": "unoptimized",
                "cache": false
            }),
        }
    }
}

/// Pipeline descriptor which besides the basic fields in direct regular control of the user
/// also has all additional fields generated and maintained by the back-end.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
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

    /// Current status of the pipeline.
    pub deployment_status: PipelineStatus,

    /// Timestamp at which the `deployment_status` last changed.
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

    /// Identifier of the current deployment.
    pub deployment_id: Option<Uuid>,
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
    pub deployment_id: Option<Uuid>,
}
