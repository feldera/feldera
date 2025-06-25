use crate::db::error::DBError;
use crate::db::types::pipeline::PipelineStatus;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;

/// Storage status.
///
/// The storage status can only transition when the pipeline status is `Stopped`.
///
/// ```text
///           Unbound ───┐
///              ▲       │
///      /unbind │       │
///              │       │
///          Unbinding   │
///              ▲       │
///              │       │
///            Bound ◄───┘
/// ```
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum StorageStatus {
    /// The pipeline has not been started before, or the user has unbound storage.
    Unbound,

    /// The pipeline was (attempted to be) started before by transitioning from
    /// `Stopped` to `Provisioning`, causing the storage status to become `Bound`.
    ///
    /// Being `Bound` restricts what edits can be made when the pipeline is `Stopped`.
    ///
    /// It remains in this state until the user invokes `/unbind`, which transitions
    /// it to `Unbinding`.
    Bound,

    /// The pipeline is in the process of becoming unbound from the storage resources.
    ///
    /// No work is performed in this unbinding process unless the storage resources
    /// are marked to be deleted upon unbinding, in which case deletion of storage
    /// resources will occur before being able to transition to `Unbound`. Otherwise,
    /// there are no operations that need to succeed to transition to `Unbound`.
    ///
    /// If the storage resources are not marked to be deleted upon unbinding, it is
    /// the responsibility of the user to manage and delete them.
    Unbinding,
}

impl TryFrom<String> for StorageStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "unbound" => Ok(Self::Unbound),
            "bound" => Ok(Self::Bound),
            "unbinding" => Ok(Self::Unbinding),
            _ => Err(DBError::invalid_storage_status(value)),
        }
    }
}

impl From<StorageStatus> for &'static str {
    fn from(val: StorageStatus) -> Self {
        match val {
            StorageStatus::Unbound => "unbound",
            StorageStatus::Bound => "bound",
            StorageStatus::Unbinding => "unbinding",
        }
    }
}

impl Display for StorageStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status: &'static str = (*self).into();
        write!(f, "{status}")
    }
}

/// Validates the storage status transition from current status to a new one.
pub fn validate_storage_status_transition(
    pipeline_status: PipelineStatus,
    current_status: StorageStatus,
    new_status: StorageStatus,
) -> Result<(), DBError> {
    if pipeline_status != PipelineStatus::Stopped {
        return Err(DBError::StorageStatusImmutableUnlessStopped {
            pipeline_status,
            current_status,
            new_status,
        });
    }
    if matches!(
        (current_status, new_status),
        (StorageStatus::Unbound, StorageStatus::Bound)
        | (StorageStatus::Bound, StorageStatus::Bound) // Pipeline starts with already bound storage
        | (StorageStatus::Bound, StorageStatus::Unbinding)
        | (StorageStatus::Unbinding, StorageStatus::Unbinding) // User calls multiple times
        | (StorageStatus::Unbinding, StorageStatus::Unbound)
    ) {
        Ok(())
    } else {
        Err(DBError::InvalidStorageStatusTransition {
            current_status,
            new_status,
        })
    }
}
