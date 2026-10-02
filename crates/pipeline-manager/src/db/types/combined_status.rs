use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use chrono::{DateTime, Utc};
use feldera_types::runtime_status::{RuntimeDesiredStatus, RuntimeStatus};
use serde::{Deserialize, Serialize};
use tracing::error;
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum CombinedStatus {
    /// See `ResourcesStatus::Stopped`.
    Stopped,
    /// See `ResourcesStatus::Provisioning`.
    Provisioning,
    /// See `RuntimeStatus::Unavailable`.
    Unavailable,
    /// See `RuntimeStatus::Coordination`.
    Coordination,
    /// See `RuntimeStatus::Standby`.
    Standby,
    /// See `RuntimeStatus::AwaitingApproval`.
    AwaitingApproval,
    /// See `RuntimeStatus::Initializing`.
    Initializing,
    /// See `RuntimeStatus::Bootstrapping`.
    Bootstrapping,
    /// See `RuntimeStatus::ConcurrentBootstrapping`.
    ConcurrentBootstrapping,
    /// See `RuntimeStatus::Synchronizing`.
    Synchronizing,
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
    pub const ALL: [Self; 15] = [
        Self::Stopped,
        Self::Provisioning,
        Self::Unavailable,
        Self::Coordination,
        Self::Standby,
        Self::AwaitingApproval,
        Self::Initializing,
        Self::Bootstrapping,
        Self::ConcurrentBootstrapping,
        Self::Synchronizing,
        Self::Replaying,
        Self::Paused,
        Self::Running,
        Self::Suspended,
        Self::Stopping,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stopped => "Stopped",
            Self::Provisioning => "Provisioning",
            Self::Unavailable => "Unavailable",
            Self::Coordination => "Coordination",
            Self::Standby => "Standby",
            Self::AwaitingApproval => "AwaitingApproval",
            Self::Initializing => "Initializing",
            Self::Bootstrapping => "Bootstrapping",
            Self::ConcurrentBootstrapping => "ConcurrentBootstrapping",
            Self::Synchronizing => "Synchronizing",
            Self::Replaying => "Replaying",
            Self::Paused => "Paused",
            Self::Running => "Running",
            Self::Suspended => "Suspended",
            Self::Stopping => "Stopping",
        }
    }

    pub fn new(resources_status: ResourcesStatus, runtime_status: Option<RuntimeStatus>) -> Self {
        match resources_status {
            ResourcesStatus::Stopped => Self::Stopped,
            ResourcesStatus::Provisioning => Self::Provisioning,
            ResourcesStatus::Provisioned => {
                if let Some(runtime_status) = runtime_status {
                    match runtime_status {
                        RuntimeStatus::Unavailable => Self::Unavailable,
                        RuntimeStatus::Coordination => Self::Coordination,
                        RuntimeStatus::AwaitingApproval => Self::AwaitingApproval,
                        RuntimeStatus::Standby => Self::Standby,
                        RuntimeStatus::Initializing => Self::Initializing,
                        RuntimeStatus::Bootstrapping => Self::Bootstrapping,
                        RuntimeStatus::ConcurrentBootstrapping => Self::ConcurrentBootstrapping,
                        RuntimeStatus::Synchronizing => Self::Synchronizing,
                        RuntimeStatus::Replaying => Self::Replaying,
                        RuntimeStatus::Paused => Self::Paused,
                        RuntimeStatus::Running => Self::Running,
                        RuntimeStatus::Suspended => Self::Suspended,
                    }
                } else {
                    error!(
                        "Generating combined status encountered unexpected scenario: resource status is Provisioned but runtime status is None -- falling back to Unavailable"
                    );
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
                        RuntimeDesiredStatus::Coordination => todo!(),
                        RuntimeDesiredStatus::Standby => Self::Standby,
                        RuntimeDesiredStatus::Paused => Self::Paused,
                        RuntimeDesiredStatus::Running => Self::Running,
                        RuntimeDesiredStatus::Suspended => Self::Suspended,
                    }
                } else if let Some(initial) = initial {
                    match initial {
                        RuntimeDesiredStatus::Unavailable => Self::Unavailable,
                        RuntimeDesiredStatus::Coordination => todo!(),
                        RuntimeDesiredStatus::Standby => Self::Standby,
                        RuntimeDesiredStatus::Paused => Self::Paused,
                        RuntimeDesiredStatus::Running => Self::Running,
                        RuntimeDesiredStatus::Suspended => Self::Suspended,
                    }
                } else {
                    error!(
                        "Generating combined desired status encountered unexpected scenario: resource desired status is Provisioned but initial and current runtime desired status is None -- falling back to Unavailable"
                    );
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

#[cfg(test)]
mod test {
    use super::CombinedStatus;
    use utoipa::ToSchema;

    /// Returns the variant names that the OpenAPI schema declares for `T`.
    fn schema_variants<'a, T: ToSchema<'a>>() -> Vec<String> {
        serde_json::to_value(T::schema().1).unwrap()["enum"]
            .as_array()
            .unwrap()
            .iter()
            .map(|variant| variant.as_str().unwrap().to_string())
            .collect()
    }

    /// `as_str` is an exhaustive match, so a new variant forces an update there,
    /// but nothing forces one in `ALL`.  A variant missing from `ALL` reports no
    /// series at all, which reads as every status being 0.
    #[test]
    fn all_lists_every_variant_in_order() {
        let listed: Vec<String> = CombinedStatus::ALL
            .iter()
            .map(|status| status.as_str().to_string())
            .collect();
        assert_eq!(listed, schema_variants::<CombinedStatus>());
    }

    #[test]
    fn as_str_matches_the_api_representation() {
        for status in CombinedStatus::ALL {
            assert_eq!(serde_json::to_value(status).unwrap(), status.as_str());
        }
    }
}
