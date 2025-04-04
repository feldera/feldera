use crate::transport::Step;
use dbsp::storage::backend::{StorageError, StoragePath};
use serde::{Serialize, Serializer};
use std::backtrace::Backtrace;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io::ErrorKind;

pub use crate::errors::controller::ControllerError;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum StepError {
    /// Storage error.
    StorageError {
        path: String,
        error: StorageError,
        #[serde(serialize_with = "serialize_as_string")]
        backtrace: Backtrace,
    },

    EncodeError {
        path: String,
        #[serde(serialize_with = "serialize_as_string")]
        error: rmp_serde::encode::Error,
    },

    DecodeError {
        path: String,
        #[serde(serialize_with = "serialize_as_string")]
        error: rmp_serde::decode::Error,
    },

    WrongStep {
        path: String,
        expected: Step,
        found: Step,
    },
}

impl StepError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::StorageError { error, .. } => error.kind(),
            Self::EncodeError { .. } | Self::DecodeError { .. } | Self::WrongStep { .. } => {
                ErrorKind::Other
            }
        }
    }
}

fn serialize_as_string<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ToString,
{
    serializer.serialize_str(&value.to_string())
}

impl Display for StepError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            StepError::EncodeError { path, error } => {
                write!(f, "{path}: error writing step ({error})")
            }
            StepError::DecodeError { path, error } => {
                write!(f, "{path}: error parsing step ({error})")
            }
            StepError::StorageError { path, error, .. } => write!(f, "{path}: {error}"),
            StepError::WrongStep {
                path,
                expected,
                found,
            } => write!(
                f,
                "{path}: file should contain  step {expected}, but read step {found}"
            ),
        }
    }
}

impl StepError {
    pub fn storage_error(path: &StoragePath, error: StorageError) -> StepError {
        StepError::StorageError {
            path: path.as_ref().into(),
            error,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<StepError> for ControllerError {
    fn from(value: StepError) -> Self {
        ControllerError::StepError(value)
    }
}
