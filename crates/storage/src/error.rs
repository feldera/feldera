use feldera_types::config::StorageBackendConfig;
use object_store::Error as ObjectStoreError;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use std::io::ErrorKind;
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

/// An error that can occur when using the storage backend.
#[derive(Clone, Error, Debug)]
pub enum StorageError {
    /// I/O error.
    #[error("{0}")]
    StdIo(ErrorKind),

    /// A process already locked the provided storage directory.
    ///
    /// If this is not expected, please remove the lock file manually, after verifying
    /// that the process with the given PID no longer exists.
    #[error("A process with PID {0} is already using the storage directory {1:?}.")]
    StorageLocked(u32, PathBuf),

    /// Unknown checkpoint specified in configuration.
    #[error("Couldn't find the specified checkpoint ({0:?}).")]
    CheckpointNotFound(Uuid),

    /// Cannot perform operation because storage is not enabled.
    #[error("Cannot perform operation because storage is not enabled.")]
    StorageDisabled,
    /// Error while creating a bloom filter.
    #[error("Failed to serialize/deserialize bloom filter.")]
    BloomFilter,

    /// Path is not valid in storage.
    ///
    /// Storage paths may not be absolute, may not start with a drive letter (on
    /// Windows), and may not contain `.` or `..` components.
    #[error("Path is not valid in storage: {}", .0.display())]
    InvalidPath(PathBuf),

    /// Unable to parse URL.
    #[error("Unable to parse URL {0:?}")]
    InvalidURL(String),

    /// Error accessing object store.
    #[error("Error accessing object store: {message}")]
    ObjectStore { kind: ErrorKind, message: String },

    /// The requested storage backend is not available.
    #[error("The requested storage backend ({0:?}) is not available in the open-source version of feldera"
    )]
    BackendNotSupported(StorageBackendConfig),
}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::StdIo(value.kind())
    }
}

impl From<ObjectStoreError> for StorageError {
    fn from(value: ObjectStoreError) -> Self {
        let kind = match value {
            ObjectStoreError::NotFound { .. } => ErrorKind::NotFound,
            ObjectStoreError::NotSupported { .. } => ErrorKind::Unsupported,
            ObjectStoreError::AlreadyExists { .. } => ErrorKind::AlreadyExists,
            ObjectStoreError::NotImplemented => ErrorKind::Unsupported,
            ObjectStoreError::PermissionDenied { .. }
            | ObjectStoreError::Unauthenticated { .. } => ErrorKind::PermissionDenied,
            ObjectStoreError::InvalidPath { .. } => {
                // Should be `ErrorKind::InvalidFilename` (once stabilized).
                ErrorKind::Other
            }
            ObjectStoreError::Generic { .. }
            | ObjectStoreError::JoinError { .. }
            | ObjectStoreError::Precondition { .. }
            | ObjectStoreError::NotModified { .. }
            | ObjectStoreError::UnknownConfigurationKey { .. }
            | _ => ErrorKind::Other,
        };
        Self::ObjectStore {
            kind,
            message: value.to_string(),
        }
    }
}

impl Serialize for StorageError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::StdIo(error) => {
                let mut ser = serializer.serialize_struct("IOError", 1)?;
                ser.serialize_field("kind", &error.to_string())?;
                ser.end()
            }
            error => error.serialize(serializer),
        }
    }
}

impl StorageError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            StorageError::StdIo(kind) => *kind,
            StorageError::StorageLocked(..) => ErrorKind::ResourceBusy,
            StorageError::CheckpointNotFound(_) => ErrorKind::NotFound,
            StorageError::StorageDisabled => ErrorKind::Other,
            StorageError::BloomFilter => ErrorKind::Other,
            StorageError::InvalidPath(_) => ErrorKind::Other,
            StorageError::InvalidURL(_) => ErrorKind::Other,
            StorageError::ObjectStore { kind, .. } => *kind,
            StorageError::BackendNotSupported(_) => ErrorKind::Other,
        }
    }
}
