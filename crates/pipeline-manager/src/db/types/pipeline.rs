use crate::db::error::DBError;
use crate::db::types::program::{ProgramError, ProgramStatus};
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::version::Version;
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus, RuntimeStatus};
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

/// Converts the `RuntimeStatus` to its string representation.
/// This is used by the database operations.
pub fn runtime_status_to_string(runtime_status: RuntimeStatus) -> String {
    match runtime_status {
        RuntimeStatus::Unavailable => "unavailable",
        RuntimeStatus::Standby => "standby",
        RuntimeStatus::AwaitingApproval => "awaiting_approval",
        RuntimeStatus::Initializing => "initializing",
        RuntimeStatus::Bootstrapping => "bootstrapping",
        RuntimeStatus::Replaying => "replaying",
        RuntimeStatus::Paused => "paused",
        RuntimeStatus::Running => "running",
        RuntimeStatus::Suspended => "suspended",
    }
    .to_string()
}

/// Parses the string as a `RuntimeStatus`.
/// This is used by the database operations.
/// Upon failure returns a database error.
pub fn parse_string_as_runtime_status(s: String) -> Result<RuntimeStatus, DBError> {
    match s.as_str() {
        "unavailable" => Ok(RuntimeStatus::Unavailable),
        "standby" => Ok(RuntimeStatus::Standby),
        "awaiting_approval" => Ok(RuntimeStatus::AwaitingApproval),
        "initializing" => Ok(RuntimeStatus::Initializing),
        "bootstrapping" => Ok(RuntimeStatus::Bootstrapping),
        "replaying" => Ok(RuntimeStatus::Replaying),
        "paused" => Ok(RuntimeStatus::Paused),
        "running" => Ok(RuntimeStatus::Running),
        "suspended" => Ok(RuntimeStatus::Suspended),
        _ => Err(DBError::InvalidRuntimeStatus(s)),
    }
}

/// Converts the `RuntimeDesiredStatus` to its string representation.
/// This is used by the database operations.
pub fn runtime_desired_status_to_string(runtime_desired_status: RuntimeDesiredStatus) -> String {
    match runtime_desired_status {
        RuntimeDesiredStatus::Unavailable => "unavailable",
        RuntimeDesiredStatus::Standby => "standby",
        RuntimeDesiredStatus::Paused => "paused",
        RuntimeDesiredStatus::Running => "running",
        RuntimeDesiredStatus::Suspended => "suspended",
    }
    .to_string()
}

/// Parses the string as a `RuntimeDesiredStatus`.
/// This is used by the database operations. Upon failure returns a database error.
pub fn parse_string_as_runtime_desired_status(s: String) -> Result<RuntimeDesiredStatus, DBError> {
    match s.as_str() {
        "unavailable" => Ok(RuntimeDesiredStatus::Unavailable),
        "standby" => Ok(RuntimeDesiredStatus::Standby),
        "paused" => Ok(RuntimeDesiredStatus::Paused),
        "running" => Ok(RuntimeDesiredStatus::Running),
        "suspended" => Ok(RuntimeDesiredStatus::Suspended),
        _ => Err(DBError::InvalidRuntimeDesiredStatus(s)),
    }
}

pub fn bootstrap_policy_to_string(bootstrap: BootstrapPolicy) -> String {
    match bootstrap {
        BootstrapPolicy::Allow => "allow",
        BootstrapPolicy::Reject => "reject",
        BootstrapPolicy::AwaitApproval => "await_approval",
    }
    .to_string()
}

pub fn parse_string_as_bootstrap_policy(s: String) -> Result<BootstrapPolicy, DBError> {
    match s.as_str() {
        "allow" => Ok(BootstrapPolicy::Allow),
        "reject" => Ok(BootstrapPolicy::Reject),
        "await_approval" => Ok(BootstrapPolicy::AwaitApproval),
        _ => Err(DBError::InvalidBootstrap(s)),
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

    /// Resource or runtime error that caused the pipeline to stop unexpectedly.
    ///
    /// Can only be set when `Stopping` or `Stopped`.
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
    /// Can only be set when `Stopping` or `Stopped`.
    pub suspend_info: Option<serde_json::Value>,

    /// Storage status.
    pub storage_status: StorageStatus,

    /// Identifier of the current deployment.
    pub deployment_id: Option<Uuid>,

    /// Initial runtime desired status of the current deployment.
    pub deployment_initial: Option<RuntimeDesiredStatus>,

    pub bootstrap_policy: Option<BootstrapPolicy>,

    /// Resources status of the current deployment.
    pub deployment_resources_status: ResourcesStatus,

    /// Timestamp when the `deployment_resources_status` last changed.
    pub deployment_resources_status_since: DateTime<Utc>,

    /// Resources desired status of the current deployment.
    pub deployment_resources_desired_status: ResourcesDesiredStatus,

    /// Timestamp when the `deployment_resources_desired_status` last changed.
    pub deployment_resources_desired_status_since: DateTime<Utc>,

    /// Observed runtime status of the current deployment.
    pub deployment_runtime_status: Option<RuntimeStatus>,

    pub deployment_runtime_status_details: Option<serde_json::Value>,

    /// Timestamp when the `deployment_runtime_status` observation last changed.
    pub deployment_runtime_status_since: Option<DateTime<Utc>>,

    /// Observed runtime desired status of the current deployment.
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,

    /// Timestamp when the `deployment_runtime_desired_status` last changed.
    pub deployment_runtime_desired_status_since: Option<DateTime<Utc>>,
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
    pub program_config: serde_json::Value,
    pub program_version: Version,
    pub program_status: ProgramStatus,
    pub program_status_since: DateTime<Utc>,
    pub deployment_error: Option<ErrorResponse>,
    pub deployment_location: Option<String>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
    pub deployment_id: Option<Uuid>,
    pub deployment_initial: Option<RuntimeDesiredStatus>,
    pub deployment_resources_status: ResourcesStatus,
    pub deployment_resources_status_since: DateTime<Utc>,
    pub deployment_resources_desired_status: ResourcesDesiredStatus,
    pub deployment_resources_desired_status_since: DateTime<Utc>,
    pub deployment_runtime_status: Option<RuntimeStatus>,
    pub deployment_runtime_status_details: Option<serde_json::Value>,
    pub deployment_runtime_status_since: Option<DateTime<Utc>>,
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    pub bootstrap_policy: Option<BootstrapPolicy>,
    pub deployment_runtime_desired_status_since: Option<DateTime<Utc>>,
}
