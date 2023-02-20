use crate::{RuntimeError, SchedulerError};
use std::{
    fmt::{Display, Error as FmtError, Formatter},
    io::Error as IOError,
};

#[derive(Debug)]
pub enum Error {
    Scheduler(SchedulerError),
    Runtime(RuntimeError),
    IO(IOError),
    Custom(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::Scheduler(error) => {
                write!(f, "scheduler error: '{error}'")
            }
            Self::Runtime(error) => {
                write!(f, "runtime error: '{error}'")
            }
            Self::IO(error) => {
                write!(f, "IO error: '{error}'")
            }
            Self::Custom(error) => f.write_str(error),
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

impl From<String> for Error {
    fn from(error: String) -> Self {
        Self::Custom(error)
    }
}
