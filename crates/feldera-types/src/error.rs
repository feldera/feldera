use actix_web::http::StatusCode;
use actix_web::ResponseError;
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{borrow::Cow, error::Error as StdError};
use utoipa::ToSchema;
pub const MAX_REPORTED_PARSE_ERRORS: usize = 1_000;

/// Information returned by REST API endpoints on error.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ErrorResponse {
    /// Human-readable error message.
    #[schema(example = "Unknown input format 'xml'.")]
    pub message: String,
    /// Error code is a string that specifies this error type.
    #[schema(example = "UnknownInputFormat")]
    pub error_code: Cow<'static, str>,
    /// Detailed error metadata.
    /// The contents of this field is determined by `error_code`.
    #[schema(value_type=Object)]
    pub details: JsonValue,
}

/// Error trait which internal errors must implement such that it
/// can be transformed to a complete JSON error response.
pub trait DetailedError: StdError + ResponseError + Serialize {
    /// Identifying name of the error.
    fn error_code(&self) -> Cow<'static, str>;
}

impl<E> From<&E> for ErrorResponse
where
    E: DetailedError,
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
        E: DetailedError,
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
        if error.status_code() == StatusCode::INTERNAL_SERVER_ERROR {
            if let Some(backtrace) = response
                .details
                .get("backtrace")
                .and_then(JsonValue::as_str)
            {
                error!("Error backtrace:\n{backtrace}");
            }
        }

        response
    }

    pub fn from_error_nolog<E>(error: &E) -> Self
    where
        E: DetailedError,
    {
        let message = error.to_string();
        let error_code = error.error_code();
        let details = serde_json::to_value(error).unwrap_or_else(|e| {
            JsonValue::String(format!("Failed to serialize error. Details: '{e}'"))
        });

        Self {
            message,
            error_code,
            details,
        }
    }
}
