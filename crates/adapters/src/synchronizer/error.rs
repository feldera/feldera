use std::{error::Error, fmt::Display};

use dbsp::storage::backend::StorageError;
use feldera_adapterlib::errors::journal::ControllerError;

#[derive(Debug)]
pub(super) enum SyncError {
    Io(std::io::Error),
    Serde(serde_json::Error),
    Controller(Box<ControllerError>),
}

impl From<ControllerError> for SyncError {
    fn from(value: ControllerError) -> Self {
        Self::Controller(Box::new(value))
    }
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
            SyncError::Controller(controller_error) => write!(f, "{controller_error}"),
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

impl From<SyncError> for ControllerError {
    fn from(val: SyncError) -> Self {
        match val {
            SyncError::Io(error) => {
                ControllerError::storage_error(error.to_string(), StorageError::StdIo(error.kind()))
            }
            SyncError::Serde(error) => ControllerError::CheckpointParseError {
                error: error.to_string(),
            },
            SyncError::Controller(controller_error) => *controller_error,
        }
    }
}
