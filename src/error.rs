use crate::{RuntimeError, SchedulerError};
use std::io::Error as IOError;

#[derive(Debug)]
pub enum Error {
    Scheduler(SchedulerError),
    Runtime(RuntimeError),
    IO(IOError),
    Custom(String),
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
