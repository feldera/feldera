use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use std::{borrow::Cow, error::Error as StdError, fmt, fmt::Display};

/// The [`ApiError`] encompasses API-related errors, which primarily will show up
/// in the API server, but are also used by the endpoints of the HTTP servers
/// of the runner and compiler.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ApiError {
    // This is unlikely, possibly impossible, to happen as the endpoint will
    // very likely already not be matched if it misses a URL-encoded parameter.
    MissingUrlEncodedParam { param: &'static str },
    InvalidUuidParam { value: String, error: String },
    InvalidNameParam { value: String, error: String },
    InvalidChecksumParam { value: String, error: String },
    InvalidVersionParam { value: String, error: String },
    InvalidPipelineAction { action: String },
    InvalidConnectorAction { action: String },
}

impl DetailedError for ApiError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::MissingUrlEncodedParam { .. } => Cow::from("MissingUrlEncodedParam"),
            Self::InvalidUuidParam { .. } => Cow::from("InvalidUuidParam"),
            Self::InvalidNameParam { .. } => Cow::from("InvalidNameParam"),
            Self::InvalidChecksumParam { .. } => Cow::from("InvalidChecksumParam"),
            Self::InvalidVersionParam { .. } => Cow::from("InvalidVersionParam"),
            Self::InvalidPipelineAction { .. } => Cow::from("InvalidPipelineAction"),
            Self::InvalidConnectorAction { .. } => Cow::from("InvalidConnectorAction"),
        }
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingUrlEncodedParam { param } => {
                write!(f, "Missing URL-encoded parameter '{param}'")
            }
            Self::InvalidUuidParam { value, error } => {
                write!(f, "Invalid UUID string '{value}': {error}")
            }
            Self::InvalidNameParam { value, error } => {
                write!(f, "Invalid name string '{value}': {error}")
            }
            Self::InvalidChecksumParam { value, error } => {
                write!(f, "Invalid checksum string '{value}': {error}")
            }
            Self::InvalidVersionParam { value, error } => {
                write!(f, "Invalid version string '{value}': {error}")
            }
            Self::InvalidPipelineAction { action } => {
                write!(f, "Invalid pipeline action '{action}'; valid actions are: 'start', 'pause', or 'shutdown'")
            }
            Self::InvalidConnectorAction { action } => {
                write!(
                    f,
                    "Invalid connector action '{action}'; valid actions are: 'start' or 'pause'"
                )
            }
        }
    }
}

impl From<ApiError> for ErrorResponse {
    fn from(val: ApiError) -> Self {
        ErrorResponse::from(&val)
    }
}

impl StdError for ApiError {}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::MissingUrlEncodedParam { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidUuidParam { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidNameParam { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidChecksumParam { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidVersionParam { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidPipelineAction { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidConnectorAction { .. } => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
