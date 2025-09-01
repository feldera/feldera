use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use chrono::{DateTime, Utc};
use feldera_types::runtime_status::{RuntimeDesiredStatus, RuntimeStatus};
use log::error;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum CombinedStatus {
    /// See `ResourcesStatus::Stopped`.
    Stopped,
    /// See `ResourcesStatus::Provisioning`.
    Provisioning,
    /// See `RuntimeStatus::Unavailable`.
    Unavailable,
    /// See `RuntimeStatus::Standby`.
    Standby,
    /// See `RuntimeStatus::Initializing`.
    Initializing,
    /// See `RuntimeStatus::Bootstrapping`.
    Bootstrapping,
    /// See `RuntimeStatus::Replaying`.
    Replaying,
    /// See `RuntimeStatus::Paused`.
    Paused,
    /// See `RuntimeStatus::Running`.
    Running,
    /// See `RuntimeStatus::Suspended`.
    Suspended,
    /// See `ResourcesStatus::Stopping`.
    Stopping,
}

impl CombinedStatus {
    pub fn new(resources_status: ResourcesStatus, runtime_status: Option<RuntimeStatus>) -> Self {
        match resources_status {
            ResourcesStatus::Stopped => Self::Stopped,
            ResourcesStatus::Provisioning => Self::Provisioning,
            ResourcesStatus::Provisioned => {
                if let Some(runtime_status) = runtime_status {
                    match runtime_status {
                        RuntimeStatus::Unavailable => Self::Unavailable,
                        RuntimeStatus::Standby => Self::Standby,
                        RuntimeStatus::Initializing => Self::Initializing,
                        RuntimeStatus::Bootstrapping => Self::Bootstrapping,
                        RuntimeStatus::Replaying => Self::Replaying,
                        RuntimeStatus::Paused => Self::Paused,
                        RuntimeStatus::Running => Self::Running,
                        RuntimeStatus::Suspended => Self::Suspended,
                    }
                } else {
                    error!("Generating combined status encountered unexpected scenario: resource status is Provisioned but runtime status is None -- falling back to Unavailable");
                    Self::Unavailable
                }
            }
            ResourcesStatus::Stopping => Self::Stopping,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum CombinedDesiredStatus {
    /// See `ResourcesDesiredStatus::Stopped`.
    Stopped,
    /// See `RuntimeDesiredStatus::Unavailable`.
    Unavailable,
    /// See `RuntimeDesiredStatus::Standby`.
    Standby,
    /// See `RuntimeDesiredStatus::Paused`.
    Paused,
    /// See `RuntimeDesiredStatus::Running`.
    Running,
    /// See `RuntimeDesiredStatus::Suspended`.
    Suspended,
}

impl CombinedDesiredStatus {
    pub fn new(
        resources_desired_status: ResourcesDesiredStatus,
        initial: Option<RuntimeDesiredStatus>,
        runtime_desired_status: Option<RuntimeDesiredStatus>,
    ) -> Self {
        match resources_desired_status {
            ResourcesDesiredStatus::Stopped => Self::Stopped,
            ResourcesDesiredStatus::Provisioned => {
                if let Some(runtime_desired_status) = runtime_desired_status {
                    match runtime_desired_status {
                        RuntimeDesiredStatus::Unavailable => Self::Unavailable,
                        RuntimeDesiredStatus::Standby => Self::Standby,
                        RuntimeDesiredStatus::Paused => Self::Paused,
                        RuntimeDesiredStatus::Running => Self::Running,
                        RuntimeDesiredStatus::Suspended => Self::Suspended,
                    }
                } else if let Some(initial) = initial {
                    match initial {
                        RuntimeDesiredStatus::Unavailable => Self::Unavailable,
                        RuntimeDesiredStatus::Standby => Self::Standby,
                        RuntimeDesiredStatus::Paused => Self::Paused,
                        RuntimeDesiredStatus::Running => Self::Running,
                        RuntimeDesiredStatus::Suspended => Self::Suspended,
                    }
                } else {
                    error!("Generating combined desired status encountered unexpected scenario: resource desired status is Provisioned but initial and current runtime desired status is None -- falling back to Unavailable");
                    Self::Unavailable
                }
            }
        }
    }
}

/// Combines the resources (desired) status since timestamp (which always is set) with the runtime
/// (desired) status since (which is not always set).
pub fn combine_since(
    resources_since: DateTime<Utc>,
    runtime_since: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    if let Some(runtime_status_since) = runtime_since {
        std::cmp::max(resources_since, runtime_status_since)
    } else {
        resources_since
    }
}
