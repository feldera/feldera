use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use std::time::Duration;
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
    UnsupportedPipelineAction { action: String, reason: String },
    InvalidConnectorAction { action: String },
    UnableToConnect { reason: String },
    LockTimeout { value: String, timeout: Duration },
    UnableToCreateSupportBundle { reason: String },
    UnableToFetchCircuitProfile { reason: String },
    ProgramInfoMissesDataflow { pipeline_name: String },
    InvalidProgramInfo { error: String },
    ProgramNotCompiled { pipeline_name: String },
    CompilerConfigNotAvailable,
    ConnectorManifestBuildFailed { error: String },
    ConnectorsConfigInvalidShape { line: u32, reason: String },
    /// `PUT /v0/connectors/connectors.toml` issued without an `If-Match`
    /// header. Maps to `428 Precondition Required` (RFC 6585).
    ConnectorsIfMatchRequired,
    /// `If-Match: *` is not accepted; the client must send the
    /// `content_hash` from the most recent GET. Maps to `412
    /// Precondition Failed`.
    ConnectorsIfMatchWildcardRejected,
}

impl DetailedError for ApiError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::MissingUrlEncodedParam { .. } => Cow::from("MissingUrlEncodedParam"),
            Self::InvalidUuidParam { .. } => Cow::from("InvalidUuidParam"),
            Self::InvalidNameParam { .. } => Cow::from("InvalidNameParam"),
            Self::InvalidChecksumParam { .. } => Cow::from("InvalidChecksumParam"),
            Self::InvalidVersionParam { .. } => Cow::from("InvalidVersionParam"),
            Self::UnsupportedPipelineAction { .. } => Cow::from("UnsupportedPipelineAction"),
            Self::InvalidConnectorAction { .. } => Cow::from("InvalidConnectorAction"),
            Self::UnableToConnect { .. } => Cow::from("UnableToConnect"),
            Self::LockTimeout { .. } => Cow::from("LockTimeout"),
            Self::UnableToCreateSupportBundle { .. } => Cow::from("UnableToCreateSupportBundle"),
            Self::UnableToFetchCircuitProfile { .. } => Cow::from("UnableToFetchCircuitProfile"),
            Self::ProgramInfoMissesDataflow { .. } => Cow::from("ProgramInfoMissesDataflow"),
            Self::InvalidProgramInfo { .. } => Cow::from("InvalidProgramInfo"),
            Self::ProgramNotCompiled { .. } => Cow::from("ProgramNotCompiled"),
            Self::CompilerConfigNotAvailable => Cow::from("CompilerConfigNotAvailable"),
            Self::ConnectorManifestBuildFailed { .. } => Cow::from("ConnectorManifestBuildFailed"),
            Self::ConnectorsConfigInvalidShape { .. } => Cow::from("ConnectorsConfigInvalidShape"),
            Self::ConnectorsIfMatchRequired => Cow::from("ConnectorsIfMatchRequired"),
            Self::ConnectorsIfMatchWildcardRejected => Cow::from("ConnectorsIfMatchWildcardRejected"),
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
            Self::UnsupportedPipelineAction { action, reason } => {
                write!(f, "Unsupported pipeline action '{action}': {reason}")
            }
            Self::InvalidConnectorAction { action } => {
                write!(
                    f,
                    "Invalid connector action '{action}'; valid actions are: 'start' or 'pause'"
                )
            }
            Self::UnableToConnect { reason } => {
                write!(f, "Error forwarding connection to pipeline: {reason}")
            }
            Self::LockTimeout { value, timeout } => {
                write!(
                    f,
                    "It took longer than {}s to acquire the lock to read the {value}",
                    timeout.as_secs_f64()
                )
            }
            Self::UnableToCreateSupportBundle { reason } => {
                write!(f, "Unable to create support bundle: {reason}")
            }
            Self::UnableToFetchCircuitProfile { reason } => {
                write!(f, "Unable to fetch circuit profile: {reason}")
            }
            Self::ProgramInfoMissesDataflow { pipeline_name } => {
                write!(
                    f,
                    "Dataflow graph is missing from pipeline '{pipeline_name}'. The pipeline may have been compiled before dataflow graphs were introduced."
                )
            }
            Self::InvalidProgramInfo { error } => {
                write!(f, "Invalid program info: {error}")
            }
            Self::ProgramNotCompiled { pipeline_name } => {
                write!(
                    f,
                    "Pipeline '{pipeline_name}' has not been compiled yet. Please compile the pipeline first."
                )
            }
            Self::CompilerConfigNotAvailable => {
                write!(f, "Compiler configuration is not available on this server.")
            }
            Self::ConnectorManifestBuildFailed { error } => {
                write!(f, "Failed to build the connector manifest: {error}")
            }
            Self::ConnectorsConfigInvalidShape { line, reason } => {
                write!(
                    f,
                    "connectors.toml line {line}: {reason}. \
                     Each non-blank, non-comment line must be `<key> = <cargo-dep-spec>`."
                )
            }
            Self::ConnectorsIfMatchRequired => {
                write!(
                    f,
                    "If-Match header is required for connectors.toml edits. \
                     Use the ETag value from a recent GET."
                )
            }
            Self::ConnectorsIfMatchWildcardRejected => {
                write!(
                    f,
                    "If-Match: * is not accepted for connectors.toml edits. \
                     Send the specific content_hash from a recent GET."
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
            Self::UnsupportedPipelineAction { .. } => StatusCode::METHOD_NOT_ALLOWED,
            Self::InvalidConnectorAction { .. } => StatusCode::BAD_REQUEST,
            Self::UnableToConnect { .. } => StatusCode::BAD_REQUEST,
            Self::LockTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::UnableToCreateSupportBundle { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::UnableToFetchCircuitProfile { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ProgramInfoMissesDataflow { .. } => StatusCode::NOT_FOUND,
            Self::InvalidProgramInfo { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ProgramNotCompiled { .. } => StatusCode::NOT_FOUND,
            Self::CompilerConfigNotAvailable => StatusCode::NOT_FOUND,
            Self::ConnectorManifestBuildFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ConnectorsConfigInvalidShape { .. } => StatusCode::BAD_REQUEST,
            Self::ConnectorsIfMatchRequired => StatusCode::PRECONDITION_REQUIRED,
            Self::ConnectorsIfMatchWildcardRejected => StatusCode::PRECONDITION_FAILED,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
