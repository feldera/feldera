use crate::transport::Step;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::backtrace::Backtrace;
use std::io::ErrorKind;
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
    path::{Path, PathBuf},
};

pub use crate::errors::controller::ControllerError;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum StepError {
    /// I/O error.
    #[serde(serialize_with = "serialize_io_error")]
    IoError {
        path: PathBuf,
        io_error: IoError,
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
            Self::IoError { io_error, .. } => io_error.kind(),
            Self::EncodeError { .. } | Self::DecodeError { .. } | Self::WrongStep { .. } => {
                ErrorKind::Other
            }
        }
    }
}

fn serialize_io_error<S>(
    path: &PathBuf,
    io_error: &IoError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("IoError", 4)?;
    ser.serialize_field("path", path)?;
    ser.serialize_field("kind", &io_error.kind().to_string())?;
    ser.serialize_field("os_error", &io_error.raw_os_error())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
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
            StepError::IoError { path, io_error, .. } => {
                write!(f, "I/O error on {}: {io_error}", path.display())
            }
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
    pub fn io_error(path: &Path, io_error: IoError) -> StepError {
        StepError::IoError {
            path: path.to_path_buf(),
            io_error,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<StepError> for ControllerError {
    fn from(value: StepError) -> Self {
        ControllerError::StepError(value)
    }
}
