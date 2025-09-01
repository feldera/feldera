use crate::db::error::DBError;
use crate::db::types::storage::StorageStatus;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;

/// Pipeline resources status.
///
/// ```text
///          /start (early start failed)
///          ┌───────────────────┐
///          │                   ▼
///       Stopped ◄────────── Stopping
///   /start │                   ▲
///          │                   │ /stop?force=true
///          │                   │ OR: timeout (from Provisioning)
///          ▼                   │ OR: fatal runtime or resource error
///    ⌛Provisioning ────────────│ OR: runtime status is Suspended
///          │                   │
///          │                   │
///          ▼                   │
///     Provisioned ─────────────┘
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
/// There are two desired statuses:
/// - `Provisioned` (set by invoking `/start`)
/// - `Stopped` (set by invoking `/stop?force=true`)
///
/// The user can monitor the current status of the pipeline via the `GET /v0/pipelines/{name}`
/// endpoint. In a typical scenario, the user first sets the desired status, e.g., by invoking the
/// `/start` endpoint, and then polls the `GET /v0/pipelines/{name}` endpoint to monitor the actual
/// status of the pipeline until its `deployment_resources_status` attribute changes to
/// `Provisioned` indicating that the pipeline has been successfully provisioned, or `Stopped` with
/// `deployment_error` being set.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ResourcesStatus {
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

    /// The pipeline resources have been provisioned. The compute resources are actively keeping the
    /// pipeline process alive.
    ///
    /// The pipeline remains in this status until:
    ///
    /// 1. Resource error or runtime error is encountered.
    ///    It transitions to `Stopping` with `deployment_error` set.
    ///
    /// 2. The runtime status is observed to be `Suspended` (caused by `/stop?force=false`).
    ///    It transitions to `Stopping` with `suspend_info` set.
    ///
    /// 3. The user stops the pipeline by invoking the `/stop?force=true` endpoint.
    ///    It transitions to `Stopping`.
    Provisioned,

    /// The compute resources of the pipeline are being deprovisioned.
    ///
    /// The pipeline remains in this status until the compute resources are deprovisioned, after
    /// which it transitions to `Stopped`.
    Stopping,
}

impl TryFrom<String> for ResourcesStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "stopped" => Ok(Self::Stopped),
            "provisioning" => Ok(Self::Provisioning),
            "provisioned" => Ok(Self::Provisioned),
            "stopping" => Ok(Self::Stopping),
            _ => Err(DBError::InvalidResourcesStatus(value)),
        }
    }
}

impl From<ResourcesStatus> for &'static str {
    fn from(value: ResourcesStatus) -> Self {
        match value {
            ResourcesStatus::Stopped => "stopped",
            ResourcesStatus::Provisioning => "provisioning",
            ResourcesStatus::Provisioned => "provisioned",
            ResourcesStatus::Stopping => "stopping",
        }
    }
}

impl Display for ResourcesStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status: &'static str = (*self).into();
        write!(f, "{status}")
    }
}

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ResourcesDesiredStatus {
    Stopped,
    Provisioned,
}

impl TryFrom<String> for ResourcesDesiredStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "stopped" => Ok(Self::Stopped),
            "provisioned" => Ok(Self::Provisioned),
            _ => Err(DBError::InvalidResourcesDesiredStatus(value)),
        }
    }
}

impl From<ResourcesDesiredStatus> for &'static str {
    fn from(value: ResourcesDesiredStatus) -> Self {
        match value {
            ResourcesDesiredStatus::Stopped => "stopped",
            ResourcesDesiredStatus::Provisioned => "provisioned",
        }
    }
}

impl Display for ResourcesDesiredStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status: &'static str = (*self).into();
        write!(f, "{status}")
    }
}

/// Validates the resources status transition from current status to a new one.
#[rustfmt::skip]
pub fn validate_resources_status_transition(
    storage_status: StorageStatus,
    current_status: ResourcesStatus,
    new_status: ResourcesStatus,
) -> Result<(), DBError> {
    // Check rules on transitioning resources status
    if matches!(
        (storage_status, current_status, new_status),
        (StorageStatus::Cleared | StorageStatus::InUse,  ResourcesStatus::Stopped, ResourcesStatus::Provisioning)
        | (StorageStatus::Cleared | StorageStatus::InUse, ResourcesStatus::Stopped, ResourcesStatus::Stopping)
        | (StorageStatus::InUse, ResourcesStatus::Provisioning, ResourcesStatus::Provisioned)
        | (StorageStatus::InUse, ResourcesStatus::Provisioned, ResourcesStatus::Provisioned)
        | (StorageStatus::InUse, ResourcesStatus::Provisioning, ResourcesStatus::Stopping)
        | (StorageStatus::InUse, ResourcesStatus::Provisioned, ResourcesStatus::Stopping)
        | (StorageStatus::Cleared | StorageStatus::InUse, ResourcesStatus::Stopping, ResourcesStatus::Stopped)
    ) {
        Ok(())
    } else {
        Err(DBError::InvalidResourcesStatusTransition {
            storage_status,
            current_status,
            new_status,
        })
    }
}

/// Validates the resources desired status transition from current status to a new one.
pub fn validate_resources_desired_status_transition(
    status: ResourcesStatus,
    current_desired_status: ResourcesDesiredStatus,
    new_desired_status: ResourcesDesiredStatus,
) -> Result<(), DBError> {
    // Check rules on changing desired resources status
    match new_desired_status {
        ResourcesDesiredStatus::Stopped => {
            // It's always possible to stop a pipeline
            Ok(())
        }
        ResourcesDesiredStatus::Provisioned => {
            if current_desired_status == ResourcesDesiredStatus::Stopped
                && status != ResourcesStatus::Stopped
            {
                // If the desired status is right now `Stopped`, but we are not yet Stopped
                return Err(DBError::IllegalPipelineAction {
                    status,
                    current_desired_status,
                    new_desired_status,
                    hint: "Cannot restart the pipeline while it is stopping. Wait until it is stopped before starting the pipeline again.".to_string(),
                });
            };
            Ok(())
        }
    }
}
