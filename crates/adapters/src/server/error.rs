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
//!    from operating a streaming pipeline consisting of input adapters, output
//!    adapters, and a DBSP circuit, via the controller API.  In some variants,
//!    it embeds:
//!
//!      * [`dbsp::Error`], which indicates invalid pipeline or endpoint
//!        configuration.
//!      * [`controller::ConfigError`](ConfigError)
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
//! ## Implementing `trait ResponseError`
//!
//! Finally, we implement the `actix-web` `ResponseError` trait for
//! [`PipelineError`], which allows [`PipelineError`] to be returned as an error
//! type by HTTP endpoints.

use crate::{ConfigError, ControllerError, ParseError};
use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use anyhow::Error as AnyError;
use dbsp::{operator::sample::MAX_QUANTILES, DetailedError};
use log::{error, log, warn, Level};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    sync::Arc,
};
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

        log!(
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
    ApiConnectionLimit,
    TableSnapshotNotImplemented,
    QuantileStreamingNotSupported,
    NumQuantilesOutOfRange {
        quantiles: u32,
    },
    QuantilesNotSupported,
    MissingNeighborhoodSpec,
    InvalidNeighborhoodSpec {
        spec: JsonValue,
        parse_error: String,
    },
    NeighborhoodNotSupported,
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
}

impl From<ControllerError> for PipelineError {
    fn from(error: ControllerError) -> Self {
        Self::ControllerError {
            error: Arc::new(error),
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
            Self::ApiConnectionLimit => {
                f.write_str("The API connections limit has been exceded. Close some of the existing connections before opening new ones.")
            }
            Self::QuantileStreamingNotSupported => {
                f.write_str("Continuous monitoring is not supported for quantiles. Use '?mode=snapshot' to retrieve a single set of quantiles.")
            }
            Self::QuantilesNotSupported => {
                f.write_str("Quantiles queries are not supported for this table.")
            }
            Self::TableSnapshotNotImplemented => {
                f.write_str("Taking a snapshot of a table or view is not yet supported.")
            }
            Self::MissingNeighborhoodSpec => {
                f.write_str(r#"Neighborhood request must specify neighborhood in the body of the request: '{"anchor": ..., "before": 100, "after": 100}'."#)
            }
            Self::NumQuantilesOutOfRange{quantiles} => {
                write!(f, "The requested number of quantiles, {quantiles}, is beyond the allowed range 1 to {MAX_QUANTILES}.")
            }
            Self::InvalidNeighborhoodSpec{spec, parse_error} => {
                write!(f, "Unable to parse neighborhood descriptor '{spec}'. Error returned by the parser: '{parse_error}'.")
            }
            Self::NeighborhoodNotSupported => {
                f.write_str("Neighborhood queries are not supported for this table.")
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
        }
    }
}

impl DetailedError for PipelineError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::Initializing => Cow::from("Initializing"),
            Self::Terminating => Cow::from("Terminating"),
            Self::InitializationError { .. } => Cow::from("InitializationError"),
            Self::PrometheusError { .. } => Cow::from("PrometheusError"),
            Self::MissingUrlEncodedParam { .. } => Cow::from("MissingUrlEncodedParam"),
            Self::ApiConnectionLimit => Cow::from("ApiConnectionLimit"),
            Self::QuantileStreamingNotSupported => Cow::from("QuantileStreamingNotSupported"),
            Self::QuantilesNotSupported => Cow::from("QuantilesNotSupported"),
            Self::TableSnapshotNotImplemented => Cow::from("TableSnapshotNotImplemented"),
            Self::MissingNeighborhoodSpec => Cow::from("MissingNeighborhoodSpec"),
            Self::NeighborhoodNotSupported => Cow::from("NeighborhoodNotSupported"),
            Self::NumQuantilesOutOfRange { .. } => Cow::from("NumQuantilesOutOfRange"),
            Self::InvalidNeighborhoodSpec { .. } => Cow::from("InvalidNeighborhoodSpec"),
            Self::ParseErrors { .. } => Cow::from("ParseErrors"),
            Self::ControllerError { error } => error.error_code(),
            Self::HeapProfilerError { .. } => Cow::from("HeapProfilerError"),
        }
    }

    fn log_level(&self) -> Level {
        match self {
            Self::Initializing => Level::Info,
            Self::Terminating => Level::Info,
            Self::ControllerError { error } => error.log_level(),
            _ => Level::Error,
        }
    }
}

impl ResponseError for ControllerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Config {
                config_error: ConfigError::UnknownInputStream { .. },
            } => StatusCode::NOT_FOUND,
            Self::Config {
                config_error: ConfigError::UnknownOutputStream { .. },
            } => StatusCode::NOT_FOUND,
            Self::Config { .. } => StatusCode::BAD_REQUEST,
            Self::ParseError { .. } => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}

// Implement `ResponseError`, so that `PipelineError` can be returned as error
// type by HTTP endpoint handlers.
impl ResponseError for PipelineError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Initializing => StatusCode::SERVICE_UNAVAILABLE,
            Self::Terminating => StatusCode::SERVICE_UNAVAILABLE,
            Self::InitializationError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PrometheusError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::MissingUrlEncodedParam { .. } => StatusCode::BAD_REQUEST,
            Self::ApiConnectionLimit => StatusCode::TOO_MANY_REQUESTS,
            Self::QuantileStreamingNotSupported => StatusCode::METHOD_NOT_ALLOWED,
            Self::QuantilesNotSupported => StatusCode::METHOD_NOT_ALLOWED,
            Self::TableSnapshotNotImplemented => StatusCode::NOT_IMPLEMENTED,
            Self::MissingNeighborhoodSpec => StatusCode::BAD_REQUEST,
            Self::NeighborhoodNotSupported => StatusCode::METHOD_NOT_ALLOWED,
            Self::NumQuantilesOutOfRange { .. } => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::InvalidNeighborhoodSpec { .. } => StatusCode::BAD_REQUEST,
            Self::ParseErrors { .. } => StatusCode::BAD_REQUEST,
            Self::HeapProfilerError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ControllerError { error } => error.status_code(),
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
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
