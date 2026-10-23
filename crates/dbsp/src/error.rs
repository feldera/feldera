use crate::{
    RuntimeError, SchedulerError, operator::dynamic::balance::BalancerError,
    storage::backend::StorageError,
};
use anyhow::Error as AnyError;
use serde::{Serialize, Serializer, ser::SerializeStruct};
use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    io::Error as IOError,
};

pub use feldera_types::error::DetailedError;
use feldera_types::error::EngineError;

#[derive(Debug)]
pub enum Error {
    Scheduler(SchedulerError),
    Runtime(RuntimeError),
    IO(IOError),
    Constructor(AnyError),
    Storage(StorageError),
    Balancer(BalancerError),
}

impl DetailedError for Error {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::Scheduler(error) => Cow::from(format!("SchedulerError.{}", error.error_code())),
            Self::Runtime(error) => Cow::from(format!("RuntimeError.{}", error.error_code())),
            Self::IO(_) => Cow::from("IOError"),
            Self::Constructor(_) => Cow::from("CircuitConstructorError"),
            Self::Storage(_) => Cow::from("StorageError"),
            Self::Balancer(_) => Cow::from("BalancerError"),
        }
    }
}

impl EngineError for Error {
    fn error_code(&self) -> Cow<'static, str> {
        DetailedError::error_code(self)
    }

    fn io_error_kind(&self) -> Option<std::io::ErrorKind> {
        match self {
            Self::IO(error) => Some(error.kind()),
            _ => None,
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
            Self::Balancer(error) => error.serialize(serializer),
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
            Self::Balancer(error) => {
                write!(f, "balancer error: {error}")
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

impl From<BalancerError> for Error {
    fn from(error: BalancerError) -> Self {
        Self::Balancer(error)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error as IOError, ErrorKind};

    use feldera_types::error::EngineError;

    use super::Error;

    /// An engine error behind a pointer reports and serializes as the error itself.
    #[test]
    fn a_boxed_engine_error_reports_like_the_error() {
        let error = Error::IO(IOError::from(ErrorKind::NotFound));
        let expected = serde_json::to_value(&error).unwrap();
        let boxed: Box<dyn EngineError> = Box::new(error);
        assert_eq!(boxed.error_code(), "IOError");
        assert_eq!(boxed.io_error_kind(), Some(ErrorKind::NotFound));
        assert_eq!(serde_json::to_value(&boxed).unwrap(), expected);

        let boxed: Box<dyn EngineError> =
            Box::new(Error::Constructor(anyhow::anyhow!("no circuit")));
        assert_eq!(boxed.error_code(), "CircuitConstructorError");
        assert_eq!(boxed.io_error_kind(), None);
    }
}
