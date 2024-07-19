use anyhow::Error as AnyError;
use log::{error, warn, Level};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
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

impl<E> From<&E> for ErrorResponse
where
    E: DetailedError,
{
    fn from(error: &E) -> ErrorResponse {
        Self::from_error(error)
    }
}

impl ErrorResponse {
    pub fn from_anyerror(error: &AnyError) -> Self {
        let message = error.to_string();
        let error_code = Cow::from("UnknownError");

        error!("[HTTP error response] {error_code}: {message}");
        warn!("Backtrace: {:#?}", error.backtrace());

        Self {
            message,
            error_code,
            details: json!(null),
        }
    }

    pub fn from_error<E>(error: &E) -> Self
    where
        E: DetailedError,
    {
        let result = Self::from_error_nolog(error);

        log::log!(
            error.log_level(),
            "[HTTP error response] {}: {}",
            result.error_code,
            result.message
        );
        // Uncomment this when all pipeline manager errors implement `ResponseError`
        // if error.status_code() == StatusCode::INTERNAL_SERVER_ERROR {
        if let Some(backtrace) = result.details.get("backtrace").and_then(JsonValue::as_str) {
            error!("Error backtrace:\n{backtrace}");
        }
        // }

        result
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

    pub fn from_yaml(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }
}

pub trait DetailedError: StdError + Serialize {
    fn error_code(&self) -> Cow<'static, str>;
    fn log_level(&self) -> Level {
        Level::Error
    }
}
