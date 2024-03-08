use crate::{storage::backend::StorageError, RuntimeError, SchedulerError};
use anyhow::Error as AnyError;
use log::Level;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    io::Error as IOError,
};

pub trait DetailedError: StdError + Serialize {
    fn error_code(&self) -> Cow<'static, str>;
    fn log_level(&self) -> Level {
        Level::Error
    }
}

#[derive(Debug)]
pub enum Error {
    Scheduler(SchedulerError),
    Runtime(RuntimeError),
    IO(IOError),
    Constructor(AnyError),
    Storage(StorageError),
}

impl DetailedError for Error {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::Scheduler(error) => Cow::from(format!("SchedulerError.{}", error.error_code())),
            Self::Runtime(error) => Cow::from(format!("RuntimeError.{}", error.error_code())),
            Self::IO(_) => Cow::from("IOError"),
            Self::Constructor(_) => Cow::from("CircuitConstructorError"),
            Self::Storage(_) => Cow::from("StorageError"),
        }
    }
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Scheduler(error) => error.serialize(serializer),
            Self::Runtime(error) => error.serialize(serializer),
            Self::IO(error) => {
                let mut ser = serializer.serialize_struct("IOError", 2)?;
                ser.serialize_field("kind", &error.kind().to_string())?;
                ser.serialize_field("os_error", &error.raw_os_error())?;
                ser.end()
            }
            Self::Constructor(_) => serializer
                .serialize_struct("CircuitConstructorError", 0)?
                .end(),
            Self::Storage(error) => error.serialize(serializer),
        }
    }
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::Scheduler(error) => {
                write!(f, "scheduler error: {error}")
            }
            Self::Runtime(error) => {
                write!(f, "runtime error: {error}")
            }
            Self::IO(error) => {
                write!(f, "IO error: {error}")
            }
            Self::Constructor(error) => {
                write!(f, "circuit construction error: {error}")
            }
            Self::Storage(error) => {
                write!(f, "storage error: {error}")
            }
        }
    }
}

impl From<IOError> for Error {
    fn from(error: IOError) -> Self {
        Self::IO(error)
    }
}

impl From<SchedulerError> for Error {
    fn from(error: SchedulerError) -> Self {
        Self::Scheduler(error)
    }
}

impl From<RuntimeError> for Error {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
