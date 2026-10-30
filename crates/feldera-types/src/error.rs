use crate::secret_redaction::secret_redactor;
use actix_web::ResponseError;
use actix_web::http::StatusCode;
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{borrow::Cow, error::Error as StdError};
use tracing::Level;
use utoipa::ToSchema;
pub const MAX_REPORTED_PARSE_ERRORS: usize = 1_000;

/// Information returned by REST API endpoints on error.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ErrorResponse {
    /// Human-readable error message.
    #[schema(example = "Explanation of the error that occurred.")]
    pub message: String,
    /// Error code is a string that specifies this error type.
    #[schema(example = "CodeSpecifyingErrorType")]
    pub error_code: Cow<'static, str>,
    /// Detailed error metadata.
    /// The contents of this field is determined by `error_code`.
    #[schema(value_type = Value)]
    pub details: JsonValue,
}

/// Error trait which internal errors must implement such that it
/// can be transformed to a complete JSON error response.
///
/// An error that is also an HTTP response, an actix `ResponseError`, converts to an
/// [`ErrorResponse`] that is logged by its status code.
pub trait DetailedError: StdError + Serialize {
    /// Identifying name of the error.
    fn error_code(&self) -> Cow<'static, str>;

    /// The level to log the error at.
    fn log_level(&self) -> Level {
        Level::ERROR
    }
}

/// An error from the engine that runs a pipeline's circuit, where the engine's own error type
/// is unknown: inside the pipeline controller's errors. It reports what a [`DetailedError`]
/// reports, and it serializes behind a pointer.
pub trait EngineError: StdError + erased_serde::Serialize + Send + Sync + 'static {
    /// Identifying name of the error, as [`DetailedError::error_code`] gives it.
    fn error_code(&self) -> Cow<'static, str>;

    /// The kind of the I/O error that caused this error, if an I/O error did.
    fn io_error_kind(&self) -> Option<std::io::ErrorKind> {
        None
    }
}

erased_serde::serialize_trait_object!(EngineError);

impl<E> From<&E> for ErrorResponse
where
    E: DetailedError + ResponseError,
{
    /// Transform the detailed error to a complete JSON error response.
    /// - The message is retrieved using `to_string()` (available due to trait `StdError`)
    /// - The status code determines the level of the log statement during this function
    ///   (available due to trait `ResponseError`)
    /// - The details are retrieved by serializing to JSON (available due to trait `Serialize`)
    fn from(error: &E) -> ErrorResponse {
        Self::from_error(error)
    }
}

impl ErrorResponse {
    pub fn from_error<E>(error: &E) -> Self
    where
        E: DetailedError + ResponseError,
    {
        // Transform the error into a response
        let response = Self::from_error_nolog(error);

        // Log based on the status code
        if error.status_code().is_success() {
            // The status code should not be successful for an error response
            error!(
                "[HTTP error (caused by implementation)] expected error but got success status code {} {}: {}",
                error.status_code(),
                response.error_code,
                response.message
            );
        } else if error.status_code().is_client_error() {
            // Client-caused error responses
            info!(
                "[HTTP error (caused by client)] {} {}: {}",
                error.status_code(),
                response.error_code,
                response.message
            );
        } else if error.status_code() == StatusCode::SERVICE_UNAVAILABLE {
            info!(
                "[HTTP error] {} {}: {}",
                error.status_code(),
                response.error_code,
                response.message
            );
        } else {
            // All other status responses should not occur in the implementation,
            // and thus are logged as errors. Many of the implementation errors are
            // represented as Internal Server Errors (500).
            error!(
                "[HTTP error (caused by implementation)] {} {}: {}",
                error.status_code(),
                response.error_code,
                response.message
            );
        }

        // Print error backtrace if available for an internal server error
        if error.status_code() == StatusCode::INTERNAL_SERVER_ERROR
            && let Some(backtrace) = response
                .details
                .get("backtrace")
                .and_then(JsonValue::as_str)
        {
            error!("Error backtrace:\n{backtrace}");
        }

        response
    }

    pub fn from_error_nolog<E>(error: &E) -> Self
    where
        E: DetailedError,
    {
        // A resolved secret value that the error copied shows as its reference.
        let redactor = secret_redactor();
        let message = redactor.redact(&error.to_string()).into_owned();
        let error_code = error.error_code();
        let mut details = serde_json::to_value(error).unwrap_or_else(|e| {
            JsonValue::String(format!("Failed to serialize error. Details: '{e}'"))
        });
        redactor.redact_json(&mut details);

        Self {
            message,
            error_code,
            details,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DetailedError, ErrorResponse};
    use crate::secret_redaction::secret_redactor;
    use serde::Serialize;
    use serde_json::json;
    use std::borrow::Cow;
    use thiserror::Error as ThisError;

    #[derive(Debug, Serialize, ThisError)]
    #[error("connection to {uri} refused")]
    struct ConnectError {
        uri: String,
    }

    impl DetailedError for ConnectError {
        fn error_code(&self) -> Cow<'static, str> {
            Cow::from("ConnectError")
        }
    }

    /// The message and the details of a response show a resolved secret as
    /// its reference.
    #[test]
    fn response_shows_the_secret_reference_not_the_value() {
        let uri = "postgres://feldera:response-test-secret@db:5432/app";
        secret_redactor().register("${secret:kubernetes:pg/uri}", uri);

        let response = ErrorResponse::from_error_nolog(&ConnectError {
            uri: uri.to_string(),
        });

        assert_eq!(
            response.message,
            "connection to ${secret:kubernetes:pg/uri} refused"
        );
        assert_eq!(
            response.details,
            json!({"uri": "${secret:kubernetes:pg/uri}"})
        );
    }
}
