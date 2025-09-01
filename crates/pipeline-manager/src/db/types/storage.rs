use crate::db::error::DBError;
use crate::db::types::resources_status::ResourcesStatus;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;

/// Storage status.
///
/// The storage status can only transition when the resources status is `Stopped`.
///
/// ```text
///           Cleared ───┐
///              ▲       │
///       /clear │       │
///              │       │
///           Clearing   │
///              ▲       │
///              │       │
///            InUse ◄───┘
/// ```
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum StorageStatus {
    /// The pipeline has not been started before, or the user has cleared storage.
    Cleared,

    /// The pipeline was (attempted to be) started before by transitioning from
    /// `Stopped` to `Provisioning`, causing the storage status to become `InUse`.
    ///
    /// Being `InUse` restricts what edits can be made when the pipeline is `Stopped`.
    ///
    /// It remains in this status until the user invokes `/clear`, which transitions
    /// it to `Clearing`.
    InUse,

    /// The pipeline is in the process of becoming cleared from the storage resources.
    ///
    /// No work is performed in this clearing process unless the storage resources
    /// are marked to be deleted upon clearing, in which case deletion of storage
    /// resources will occur before being able to transition to `Cleared`. Otherwise,
    /// there are no operations that need to succeed to transition to `Cleared`.
    ///
    /// If the storage resources are not marked to be deleted upon clearing, it is
    /// the responsibility of the user to manage and delete them.
    Clearing,
}

impl TryFrom<String> for StorageStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "cleared" => Ok(Self::Cleared),
            "in_use" => Ok(Self::InUse),
            "clearing" => Ok(Self::Clearing),
            _ => Err(DBError::invalid_storage_status(value)),
        }
    }
}

impl From<StorageStatus> for &'static str {
    fn from(val: StorageStatus) -> Self {
        match val {
            StorageStatus::Cleared => "cleared",
            StorageStatus::InUse => "in_use",
            StorageStatus::Clearing => "clearing",
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
    resources_status: ResourcesStatus,
    current_status: StorageStatus,
    new_status: StorageStatus,
) -> Result<(), DBError> {
    if resources_status != ResourcesStatus::Stopped {
        return Err(DBError::StorageStatusImmutableUnlessStopped {
            resources_status,
            current_status,
            new_status,
        });
    }
    if matches!(
        (current_status, new_status),
        (StorageStatus::Cleared, StorageStatus::InUse)
        | (StorageStatus::InUse, StorageStatus::InUse) // Pipeline starts with already in use storage
        | (StorageStatus::InUse, StorageStatus::Clearing)
        | (StorageStatus::Clearing, StorageStatus::Clearing) // User calls multiple times
        | (StorageStatus::Clearing, StorageStatus::Cleared)
    ) {
        Ok(())
    } else {
        Err(DBError::InvalidStorageStatusTransition {
            current_status,
            new_status,
        })
    }
}
