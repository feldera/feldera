use crate::db::error::DBError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum MonitorStatus {
    /// The service has not yet had any health check pass and is likely still getting ready.
    /// Once this status is changed to `Unhealthy` or `Healthy`, it will never transition back.
    InitialUnhealthy,
    /// The service is experiencing issues.
    Unhealthy,
    /// The service is not experiencing any issue.
    Healthy,
}

impl TryFrom<String> for MonitorStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "initial_unhealthy" => Ok(Self::InitialUnhealthy),
            "unhealthy" => Ok(Self::Unhealthy),
            "healthy" => Ok(Self::Healthy),
            _ => Err(DBError::InvalidMonitorStatus(value)),
        }
    }
}

impl From<MonitorStatus> for &'static str {
    fn from(value: MonitorStatus) -> Self {
        match value {
            MonitorStatus::InitialUnhealthy => "initial_unhealthy",
            MonitorStatus::Unhealthy => "unhealthy",
            MonitorStatus::Healthy => "healthy",
        }
    }
}

impl Display for MonitorStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status: &'static str = (*self).into();
        write!(f, "{status}")
    }
}

/// Cluster monitor event identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ClusterMonitorEventId(
    #[cfg_attr(test, proptest(strategy = "crate::db::test::limited_uuid()"))] pub Uuid,
);
impl Display for ClusterMonitorEventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Internally used to propagate across the details about the new monitor event.
#[derive(Clone, Debug)]
pub(crate) struct NewClusterMonitorEvent {
    pub api_status: MonitorStatus,
    pub api_self_info: String,
    pub api_resources_info: String,
    pub compiler_status: MonitorStatus,
    pub compiler_self_info: String,
    pub compiler_resources_info: String,
    pub runner_status: MonitorStatus,
    pub runner_self_info: String,
    pub runner_resources_info: String,
}

/// Brief cluster monitoring event with only the identifier, timestamp and health conclusions.
#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct ClusterMonitorEvent {
    /// Identifier of the event, which can be used to fetch the extended event.
    pub id: ClusterMonitorEventId,

    /// Timestamp at which the event was recorded in the database. Because collecting the data for
    /// the health checks can take time, this timestamp is approximate.
    pub recorded_at: DateTime<Utc>,

    /// Collective API server(s) status.
    pub api_status: MonitorStatus,

    /// Collective compiler server(s) status.
    pub compiler_status: MonitorStatus,

    /// Runner status.
    pub runner_status: MonitorStatus,
}

/// Extended cluster monitoring event with full details.
#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct ExtendedClusterMonitorEvent {
    /// Identifier of the event, which can be used to fetch the extended event.
    pub id: ClusterMonitorEventId,

    /// Timestamp at which the event was recorded in the database. Because collecting the data for
    /// the health checks can take time, this timestamp is approximate.
    pub recorded_at: DateTime<Utc>,

    /// Collective API server(s) status.
    pub api_status: MonitorStatus,

    /// Human-readable API server(s) status report.
    pub api_self_info: String,

    /// Human-readable API server(s) status report of the resources backing the API server(s)
    /// -- in particular, the Kubernetes objects.
    pub api_resources_info: String,

    /// Collective compiler server(s) status.
    pub compiler_status: MonitorStatus,

    /// Human-readable compiler server(s) status report.
    pub compiler_self_info: String,

    /// Human-readable API server(s) status report of the resources backing the compiler server(s)
    /// -- in particular, the Kubernetes objects.
    pub compiler_resources_info: String,

    /// Runner status.
    pub runner_status: MonitorStatus,

    /// Human-readable runner status report.
    pub runner_self_info: String,

    /// Human-readable API server(s) status report of the resources backing the runner
    /// -- in particular, the Kubernetes objects.
    pub runner_resources_info: String,
}
