use crate::transport::Step;
use dbsp::storage::backend::StorageError;
use serde::{Serialize, Serializer};
use std::backtrace::Backtrace;
use std::io::ErrorKind;
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    path::{Path, PathBuf},
};

pub use crate::errors::controller::ControllerError;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum StepError {
    /// Storage error.
    StorageError {
        path: PathBuf,
        error: StorageError,
        #[serde(serialize_with = "serialize_as_string")]
        backtrace: Backtrace,
    },

    EncodeError {
        path: PathBuf,
        #[serde(serialize_with = "serialize_as_string")]
        error: rmp_serde::encode::Error,
    },

    DecodeError {
        path: PathBuf,
        #[serde(serialize_with = "serialize_as_string")]
        error: rmp_serde::decode::Error,
    },

    WrongStep {
        path: PathBuf,
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
                write!(f, "{}: error writing step ({error})", path.display())
            }
            StepError::DecodeError { path, error } => {
                write!(f, "{}: error parsing step ({error})", path.display())
            }
            StepError::StorageError { path, error, .. } => write!(f, "{}: {error}", path.display()),
            StepError::WrongStep {
                path,
                expected,
                found,
            } => write!(
                f,
                "{}: file should contain  step {expected}, but read step {found}",
                path.display()
            ),
        }
    }
}

impl StepError {
    pub fn storage_error(path: &Path, error: StorageError) -> StepError {
        StepError::StorageError {
            path: path.to_path_buf(),
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
