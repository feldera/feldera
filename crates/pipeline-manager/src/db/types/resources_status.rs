use crate::db::error::DBError;
use crate::db::types::pipeline::{PipelineDesiredStatus, PipelineStatus};
use crate::db::types::storage::StorageStatus;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Pipeline resources status.
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
///     Provisioned ─────────────┘
/// ```
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ResourcesStatus {
    /// See [PipelineStatus::Stopped].
    Stopped,

    /// See [PipelineStatus::Provisioning].
    Provisioning,

    /// The pipeline resources have been provisioned. The compute resources are actively keeping the
    /// pipeline process alive.
    ///
    /// The pipeline remains in this status until:
    ///
    /// 1. Resource error or runtime error is encountered.
    ///    It transitions to `Stopping` with `deployment_error` set.
    ///
    /// 2. The user stops the pipeline by invoking the `/stop` endpoint.
    ///    It transitions to `Stopping`.
    Provisioned,

    /// See [PipelineStatus::Stopping].
    Stopping,
}

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ResourcesDesiredStatus {
    Stopped,
    Provisioned,
}

/// Validates the resources status transition from current status to a new one.
#[rustfmt::skip]
pub fn validate_resources_status_transition(
    storage_status: StorageStatus,
    current_status: PipelineStatus,
    new_status: PipelineStatus,
) -> Result<(), DBError> {
    // Convert pipeline status which includes runtime statuses to their base resources status
    let current_resources_status = current_status.as_resources_status();
    let new_resources_status = new_status.as_resources_status();
    
    // Check rules on transitioning resources status
    if matches!(
        (storage_status, current_resources_status, new_resources_status),
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
        Err(DBError::InvalidDeploymentStatusTransition {
            storage_status,
            current_status,
            new_status,
        })
    }
}

/// Validates the resources desired status transition from current status to a new one.
pub fn validate_resources_desired_status_transition(
    pipeline_status: PipelineStatus,
    current_desired_status: PipelineDesiredStatus,
    new_desired_status: PipelineDesiredStatus,
) -> Result<(), DBError> {
    // Convert pipeline status which includes runtime statuses to their base resources status
    let resources_status = pipeline_status.as_resources_status();
    let current_resources_desired_status = current_desired_status.as_resources_desired_status();
    let new_resources_desired_status = new_desired_status.as_resources_desired_status();

    // Check rules on changing desired resources status
    match new_resources_desired_status {
        ResourcesDesiredStatus::Stopped => {
            // It's always possible to stop a pipeline
            Ok(())
        }
        ResourcesDesiredStatus::Provisioned => {
            if current_resources_desired_status == ResourcesDesiredStatus::Stopped
                && resources_status != ResourcesStatus::Stopped
            {
                // If the desired status is right now `Stopped`, but we are not yet Stopped
                return Err(DBError::IllegalPipelineAction {
                    pipeline_status,
                    current_desired_status,
                    new_desired_status,
                    hint: "Cannot restart the pipeline while it is stopping. Wait until it is stopped before starting the pipeline again.".to_string(),
                });
            };
            Ok(())
        }
    }
}
