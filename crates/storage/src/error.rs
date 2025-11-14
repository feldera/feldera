use feldera_types::config::StorageBackendConfig;
use object_store::Error as ObjectStoreError;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use std::io::ErrorKind;
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

/// An error that can occur when using the storage backend.
#[derive(Clone, Error, Debug, Serialize)]
pub enum StorageError {
    /// I/O error.
    #[error("{}: {operation} failed: {kind}", path.as_ref().map_or("(unknown file)", |path| path.as_str()))]
    #[serde(serialize_with = "serialize_io_error")]
    StdIo {
        kind: ErrorKind,
        operation: &'static str,
        path: Option<String>,
    },

    /// A process already locked the provided storage directory.
    ///
    /// If this is not expected, please remove the lock file manually, after verifying
    /// that the process with the given PID no longer exists.
    #[error("A process with PID {0} is already using the storage directory {1:?}.")]
    StorageLocked(u32, PathBuf),

    /// Unknown checkpoint specified in configuration.
    #[error("Couldn't find the specified checkpoint ({0:?}).")]
    CheckpointNotFound(Uuid),

    /// The operator wasn't assigned a persistent ID when the circuit was constructed.
    #[error("Internal error: operator {0} has not been assigned a persistent id.")]
    NoPersistentId(String),

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
    #[serde(serialize_with = "serialize_object_store_error")]
    ObjectStore { kind: ErrorKind, message: String },

    /// The requested storage backend is not available.
    #[error("The requested storage backend ({0:?}) is not available in the open-source version of feldera"
    )]
    BackendNotSupported(Box<StorageBackendConfig>),

    /// The requested storage backend ({backend}) cannot be configured with {}.
    #[error("The requested storage backend ({backend}) cannot be configured with {config:?}.")]
    InvalidBackendConfig {
        backend: String,
        config: Box<StorageBackendConfig>,
    },

    #[error("Error deserializing JSON: {0}")]
    JsonError(String),
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

fn serialize_io_error<S>(
    kind: &ErrorKind,
    operation: &str,
    path: &Option<String>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("IOError", 3)?;
    ser.serialize_field("kind", &kind.to_string())?;
    ser.serialize_field("operation", operation)?;
    ser.serialize_field("path", &path)?;
    ser.end()
}

fn serialize_object_store_error<S>(
    kind: &ErrorKind,
    message: &String,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("ObjectStoreError", 2)?;
    ser.serialize_field("kind", &kind.to_string())?;
    ser.serialize_field("message", message)?;
    ser.end()
}

impl StorageError {
    pub fn stdio(kind: ErrorKind, operation: &'static str, path: impl ToString) -> Self {
        Self::StdIo {
            kind,
            operation,
            path: Some(path.to_string()),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        match self {
            StorageError::StdIo { kind, .. } => *kind,
            StorageError::StorageLocked(..) => ErrorKind::ResourceBusy,
            StorageError::NoPersistentId(_) => ErrorKind::Other,
            StorageError::CheckpointNotFound(_) => ErrorKind::NotFound,
            StorageError::StorageDisabled => ErrorKind::Other,
            StorageError::BloomFilter => ErrorKind::Other,
            StorageError::InvalidPath(_) => ErrorKind::Other,
            StorageError::InvalidURL(_) => ErrorKind::Other,
            StorageError::ObjectStore { kind, .. } => *kind,
            StorageError::BackendNotSupported(_) => ErrorKind::Other,
            StorageError::InvalidBackendConfig { .. } => ErrorKind::Other,
            StorageError::JsonError(_) => ErrorKind::Other,
        }
    }

    pub fn ignore_notfound<T>(result: Result<T, Self>) -> Result<(), Self> {
        match result {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}
