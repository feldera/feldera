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
    InvalidBootstrapConfig { reason: String },
    InvalidConnectorAction { action: String },
    UnableToConnect { reason: String },
    LockTimeout { value: String, timeout: Duration },
    UnableToCreateSupportBundle { reason: String },
    InvalidSupportBundleParameter { reason: String },
    UnableToFetchCircuitProfile { reason: String },
    ProgramInfoMissesDataflow { pipeline_name: String },
    InvalidProgramInfo { error: String },
    ProgramNotCompiled { pipeline_name: String },
    CompilerUnavailable { reason: String },
    CompilerTimeout { timeout_secs: u64 },
    InvalidRuntimeVersion { error: String },
    InvalidNewProgramSql { error: String },
    NewProgramCompilationFailed { error: String },
    BootstrapNotAllowed { error: String },
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
            Self::InvalidBootstrapConfig { .. } => Cow::from("InvalidBootstrapConfig"),
            Self::InvalidConnectorAction { .. } => Cow::from("InvalidConnectorAction"),
            Self::UnableToConnect { .. } => Cow::from("UnableToConnect"),
            Self::LockTimeout { .. } => Cow::from("LockTimeout"),
            Self::UnableToCreateSupportBundle { .. } => Cow::from("UnableToCreateSupportBundle"),
            Self::InvalidSupportBundleParameter { .. } => {
                Cow::from("InvalidSupportBundleParameter")
            }
            Self::UnableToFetchCircuitProfile { .. } => Cow::from("UnableToFetchCircuitProfile"),
            Self::ProgramInfoMissesDataflow { .. } => Cow::from("ProgramInfoMissesDataflow"),
            Self::InvalidProgramInfo { .. } => Cow::from("InvalidProgramInfo"),
            Self::ProgramNotCompiled { .. } => Cow::from("ProgramNotCompiled"),
            Self::CompilerUnavailable { .. } => Cow::from("CompilerUnavailable"),
            Self::CompilerTimeout { .. } => Cow::from("CompilerTimeout"),
            Self::InvalidRuntimeVersion { .. } => Cow::from("InvalidRuntimeVersion"),
            Self::InvalidNewProgramSql { .. } => Cow::from("InvalidNewProgramSql"),
            Self::NewProgramCompilationFailed { .. } => Cow::from("NewProgramCompilationFailed"),
            Self::BootstrapNotAllowed { .. } => Cow::from("BootstrapNotAllowed"),
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
            Self::InvalidBootstrapConfig { reason } => {
                write!(f, "Invalid bootstrap configuration: {reason}")
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
            Self::InvalidSupportBundleParameter { reason } => {
                write!(f, "Invalid support bundle parameter: {reason}")
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
            Self::CompilerUnavailable { reason } => {
                write!(f, "The compiler service is unavailable: {reason}")
            }
            Self::CompilerTimeout { timeout_secs } => {
                write!(
                    f,
                    "The compiler did not respond within the configured {timeout_secs}s timeout. If the program is large and needs longer to compile, increase the 'sql_compilation_timeout_secs' configuration setting (or the FELDERA_SQL_COMPILATION_TIMEOUT_SECS environment variable)."
                )
            }
            Self::InvalidRuntimeVersion { error } => {
                write!(f, "Invalid runtime version: {error}")
            }
            Self::InvalidNewProgramSql { error } => {
                write!(
                    f,
                    "The proposed new SQL program has compilation errors: {error}"
                )
            }
            Self::NewProgramCompilationFailed { error } => {
                write!(
                    f,
                    "The proposed new program could not be compiled because of an internal error (for example, the compiler service failed or a runtime version could not be downloaded): {error}"
                )
            }
            Self::BootstrapNotAllowed { error } => {
                write!(f, "The requested change cannot be bootstrapped: {error}")
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
            Self::InvalidBootstrapConfig { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidConnectorAction { .. } => StatusCode::BAD_REQUEST,
            Self::UnableToConnect { .. } => StatusCode::BAD_REQUEST,
            Self::LockTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::UnableToCreateSupportBundle { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidSupportBundleParameter { .. } => StatusCode::BAD_REQUEST,
            Self::UnableToFetchCircuitProfile { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ProgramInfoMissesDataflow { .. } => StatusCode::NOT_FOUND,
            Self::InvalidProgramInfo { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ProgramNotCompiled { .. } => StatusCode::NOT_FOUND,
            Self::CompilerUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::CompilerTimeout { .. } => StatusCode::GATEWAY_TIMEOUT,
            Self::InvalidRuntimeVersion { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidNewProgramSql { .. } => StatusCode::BAD_REQUEST,
            Self::NewProgramCompilationFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BootstrapNotAllowed { .. } => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
