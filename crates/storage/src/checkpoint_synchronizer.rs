use std::sync::Arc;
use std::{error::Error, fmt::Display};

use feldera_types::{checkpoint::CheckpointMetadata, config::SyncConfig};

use crate::error::StorageError;
use crate::StorageBackend;

pub trait CheckpointSynchronizer: Sync {
    fn push(
        &self,
        checkpoint: &CheckpointMetadata,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
    ) -> Result<(), SyncError>;

    fn pull(
        &self,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
    ) -> Result<(), SyncError>;
}

#[derive(Debug)]
pub enum SyncError {
    Io(std::io::Error),
    Serde(serde_json::Error),
    RcloneExitCode(String),
    Storage(String),
}

impl Error for SyncError {}

impl Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Io(error) => write!(f, "synchronizer IO err: {error}",),
            SyncError::Serde(error) => write!(
                f,
                "synchronizer: error parsing checkpoint metadata: {error}",
            ),
            SyncError::RcloneExitCode(error) => write!(f, "synchronizer: rclone: '{error}'"),
            SyncError::Storage(error) => write!(f, "synchronizer: storage error: '{error}'"),
        }
    }
}

impl From<std::io::Error> for SyncError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for SyncError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serde(value)
    }
}

impl From<StorageError> for SyncError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value.to_string())
    }
}

inventory::collect!(&'static dyn CheckpointSynchronizer);
