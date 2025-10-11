//! Error type returned by HTTP endpoints of the pipeline runner server.
//!
//! [`PipelineError`] is the top-level error type returned by HTTP endpoints of
//! the pipeline runner server.  Other error types defined in by the controller,
//! by various transport and format adapters, and in the `dbsp` crate are
//! returned as variants of this type.
//!
//! ## Error hierarchy
//!
//! At a high level, the error hierarchy looks like this:
//!
//! * [`PipelineError`], in this module, include errors related to interacting
//!   with the client via HTTP endpoints (e.g., invalid API arguments).  In some
//!   variants, it embeds:
//!
//!    * [`controller::ControllerError`](ControllerError), for errors that arise
//!      from operating a streaming pipeline consisting of input adapters, output adapters,
//!      and a DBSP circuit, via the controller API.  In some variants, it embeds:
//!
//!      * [`dbsp::Error`], which indicates invalid pipeline or endpoint
//!        configuration.
//!      * [`ConfigError`](feldera_adapterlib::errors::controller::ConfigError)
//!      * [`ControllerError::InputTransportError`]<br>
//!        [`ControllerError::OutputTransportError`]<br>
//!        [`ControllerError::ParseError`]<br>
//!        [`ControllerError::EncodeError`]<br> Errors that transport and format
//!        adapters return as dynamically typed [`anyhow::Error`] instances.
//!
//! ## [`struct ErrorResponse`](`ErrorResponse`)
//!
//! This type represents the body of an HTTP error response returned by all
//! endpoints in this crate and in the manager crate.  An instance of
//! [`ErrorResponse`] can be generated from any type that implements `trait
//! DetailedError`, including all types in our error hierarchy.
//!
//! ## Implementing `trait IntoResponse`
//!
//! Finally, we implement the `axum` `IntoResponse` trait for
//! [`PipelineError`], which allows [`PipelineError`] to be returned as an error
//! type by HTTP endpoints.

use crate::{dyn_event, ControllerError, ParseError};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use feldera_types::error::DetailedError;
use anyhow::Error as AnyError;
use datafusion::error::DataFusionError;
use feldera_types::runtime_status::RuntimeDesiredStatus;
use parquet::errors::ParquetError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    sync::Arc,
};
use tracing::{error, warn, Level};
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

        // Log the error at ERROR level since we don't have log_level method
        error!(
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
}

/// Top-level error type returned by the pipeline server.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum PipelineError {
    Initializing,
    Terminating,
    InitializationError {
        error: Arc<ControllerError>,
    },
    PrometheusError {
        error: String,
    },
    MissingUrlEncodedParam {
        param: &'static str,
    },
    InvalidParam {
        error: String,
    },
    ApiConnectionLimit,
    ControllerError {
        // Fold `ControllerError` directly into `PipelineError` to simplify
        // the error hierarchy from the user's perspective.
        #[serde(flatten)]
        error: Arc<ControllerError>,
    },
    ParseErrors {
        num_errors: u64,
        errors: Vec<ParseError>,
    },
    HeapProfilerError {
        error: String,
    },
    AdHocQueryError {
        error: String,
        #[serde(skip)]
        df: Option<Box<DataFusionError>>,
    },
    Suspended,
    InvalidActivateStatus(RuntimeDesiredStatus),
    InvalidActivateStatusString(String),
    InvalidTransition(&'static str, RuntimeDesiredStatus),
}

impl From<ControllerError> for PipelineError {
    fn from(error: ControllerError) -> Self {
        Self::ControllerError {
            error: Arc::new(error),
        }
    }
}

impl From<Arc<ControllerError>> for PipelineError {
    fn from(error: Arc<ControllerError>) -> Self {
        Self::ControllerError { error }
    }
}

impl From<DataFusionError> for PipelineError {
    fn from(error: DataFusionError) -> Self {
        Self::AdHocQueryError {
            // Until https://github.com/apache/datafusion/issues/14080 is fixed, we'll
            // remove the `DataFusionError::External` strings of the error message
            //
            // Tracking issue: https://github.com/feldera/feldera/issues/3215
            error: error.to_string().replace("External error: ", ""),
            df: Some(Box::new(error)),
        }
    }
}

impl From<ParquetError> for PipelineError {
    fn from(error: ParquetError) -> Self {
        Self::AdHocQueryError {
            error: error.to_string(),
            df: None,
        }
    }
}

impl StdError for PipelineError {}

impl Display for PipelineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::Initializing => {
                f.write_str("Operation failed because the pipeline has not finished initializing.")
            }
            Self::Terminating => {
                f.write_str("Operation failed because the pipeline is shutting down.")
            }
            Self::InitializationError{error} => {
                write!(f, "Operation failed because the pipeline failed to initialize. Error details: '{error}'.")
            }
            Self::PrometheusError{error} => {
                write!(f, "Error retrieving Prometheus metrics: '{error}'.")
            }
            Self::MissingUrlEncodedParam { param } => {
                write!(f, "Missing URL-encoded parameter '{param}'.")
            }
            Self::InvalidParam { error } => {
                write!(f, "Invalid parameter: {error}.")
            }
            Self::ApiConnectionLimit => {
                f.write_str("The API connections limit has been exceded. Close some of the existing connections before opening new ones.")
            }
            Self::ControllerError{ error } => {
                error.fmt(f)
            }
            Self::ParseErrors{ num_errors, errors } => {
                if *num_errors > errors.len() as u64 {
                    write!(f, "Errors parsing input data (reporting {} out of {} total errors):", errors.len(), num_errors)?;
                    for error in errors.iter() {
                        write!(f, "\n    {error}")?;
                    }
                    Ok(())
                } else {
                    write!(f, "Errors parsing input data ({} errors):", errors.len())?;
                    for error in errors.iter() {
                        write!(f, "\n    {error}")?;
                    }
                    Ok(())
                }
            }
            Self::HeapProfilerError {error} => {
                write!(f, "Heap profiler error: {error}.")
            }
            Self::AdHocQueryError {error, df: _} => {
                write!(f, "Error during query processing: {error}.")
            }
            Self::Suspended => {
                write!(f, "Operation failed because the pipeline has been suspended.")
            }
            Self::InvalidActivateStatus(status) => {
                write!(
                    f,
                    "Invalid activation status {status:?} (only running and paused are valid)"
                )
            }
            Self::InvalidActivateStatusString(status) => {
                write!(
                    f,
                    "Invalid activation status ?initial={status} (only running and paused are valid)"
                )
            }
            Self::InvalidTransition(transition, status) => {
                write!(f, "Cannot execute {transition} transition starting from {status:?}")
            }
        }
    }
}


// Implement `IntoResponse`, so that `PipelineError` can be returned as error
// type by HTTP endpoint handlers.
impl IntoResponse for PipelineError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            Self::Initializing => StatusCode::SERVICE_UNAVAILABLE,
            Self::Terminating => StatusCode::GONE,
            Self::InitializationError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PrometheusError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::MissingUrlEncodedParam { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidParam { .. } => StatusCode::BAD_REQUEST,
            Self::ApiConnectionLimit => StatusCode::TOO_MANY_REQUESTS,
            Self::ParseErrors { .. } => StatusCode::BAD_REQUEST,
            Self::HeapProfilerError { .. } => StatusCode::BAD_REQUEST,
            Self::ControllerError { error } => error.status_code(),
            Self::AdHocQueryError { .. } => StatusCode::BAD_REQUEST,
            Self::Suspended => StatusCode::SERVICE_UNAVAILABLE,
            Self::InvalidActivateStatus(_) => StatusCode::BAD_REQUEST,
            Self::InvalidActivateStatusString(_) => StatusCode::BAD_REQUEST,
            Self::InvalidTransition(_, _) => StatusCode::BAD_REQUEST,
        };

        (status_code, Json(ErrorResponse::from_error(&self))).into_response()
    }
}

impl feldera_types::error::DetailedError for PipelineError {
    fn error_code(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::Initializing => std::borrow::Cow::Borrowed("initializing"),
            Self::Terminating => std::borrow::Cow::Borrowed("terminating"),
            Self::InitializationError { .. } => std::borrow::Cow::Borrowed("initialization_error"),
            Self::PrometheusError { .. } => std::borrow::Cow::Borrowed("prometheus_error"),
            Self::MissingUrlEncodedParam { .. } => std::borrow::Cow::Borrowed("missing_url_encoded_param"),
            Self::InvalidParam { .. } => std::borrow::Cow::Borrowed("invalid_param"),
            Self::ApiConnectionLimit => std::borrow::Cow::Borrowed("api_connection_limit"),
            Self::ParseErrors { .. } => std::borrow::Cow::Borrowed("parse_errors"),
            Self::HeapProfilerError { .. } => std::borrow::Cow::Borrowed("heap_profiler_error"),
            Self::ControllerError { .. } => std::borrow::Cow::Borrowed("controller_error"),
            Self::AdHocQueryError { .. } => std::borrow::Cow::Borrowed("adhoc_query_error"),
            Self::Suspended => std::borrow::Cow::Borrowed("suspended"),
            Self::InvalidActivateStatus(_) => std::borrow::Cow::Borrowed("invalid_activate_status"),
            Self::InvalidActivateStatusString(_) => std::borrow::Cow::Borrowed("invalid_activate_status_string"),
            Self::InvalidTransition(_, _) => std::borrow::Cow::Borrowed("invalid_transition"),
        }
    }

    fn status_code(&self) -> StatusCode {
        match &self {
            Self::Initializing => StatusCode::SERVICE_UNAVAILABLE,
            Self::Terminating => StatusCode::GONE,
            Self::InitializationError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PrometheusError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::MissingUrlEncodedParam { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidParam { .. } => StatusCode::BAD_REQUEST,
            Self::ApiConnectionLimit => StatusCode::TOO_MANY_REQUESTS,
            Self::ParseErrors { .. } => StatusCode::BAD_REQUEST,
            Self::HeapProfilerError { .. } => StatusCode::BAD_REQUEST,
            Self::ControllerError { error } => error.status_code(),
            Self::AdHocQueryError { .. } => StatusCode::BAD_REQUEST,
            Self::Suspended => StatusCode::SERVICE_UNAVAILABLE,
            Self::InvalidActivateStatus(_) => StatusCode::BAD_REQUEST,
            Self::InvalidActivateStatusString(_) => StatusCode::BAD_REQUEST,
            Self::InvalidTransition(_, _) => StatusCode::BAD_REQUEST,
        }
    }

}

impl PipelineError {
    pub fn parse_errors<'a, I: IntoIterator<Item = &'a ParseError>>(
        num_errors: usize,
        errors: I,
    ) -> Self {
        Self::ParseErrors {
            num_errors: num_errors as u64,
            errors: errors.into_iter().cloned().collect(),
        }
    }
}
