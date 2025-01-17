use thiserror::Error;

/// Error that may be produced by a function at runtime
#[derive(Error, Debug)]
pub enum SqlRuntimeError {
    #[error("{0}")]
    CustomError(String),
}

impl SqlRuntimeError {
    /// Create a Boxed SqlRuntimeError from a message string
    pub fn from_string(message: String) -> Box<Self> {
        Box::new(SqlRuntimeError::CustomError(message))
    }

    /// Create a Boxed SqlRuntimeError from a message slice
    pub fn from_strng(message: &str) -> Box<Self> {
        Box::new(SqlRuntimeError::CustomError(message.to_string()))
    }
}

pub type SqlResult<T> = Result<T, Box<SqlRuntimeError>>;

#[doc(hidden)]
// Convert a SqlResult<T> into a SqlResult<Option<T>>
pub(crate) fn r2o<T>(result: SqlResult<T>) -> SqlResult<Option<T>> {
    match result {
        Err(e) => Err(e),
        Ok(value) => Ok(Some(value)),
    }
}
