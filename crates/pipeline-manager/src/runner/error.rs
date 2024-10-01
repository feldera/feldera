use crate::db::types::pipeline::PipelineId;
use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use std::{borrow::Cow, error::Error as StdError, fmt, fmt::Display, time::Duration};

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RunnerError {
    // Pipeline information missing
    PipelineMissingDeploymentLocation {
        pipeline_id: PipelineId,
        pipeline_name: String,
    },
    PipelineMissingProgramInfo {
        pipeline_id: PipelineId,
        pipeline_name: String,
    },
    PipelineMissingProgramBinaryUrl {
        pipeline_id: PipelineId,
        pipeline_name: String,
    },
    // Runner web server interaction
    RunnerEndpointSendError {
        url: String,
        error: String,
    },
    RunnerUnreachable {
        original_error: String,
    },
    LogFollowRequestChannelFull,
    LogFollowRequestChannelClosed,
    // Pipeline web server interaction
    PipelineNotRunningOrPaused {
        pipeline_id: PipelineId,
        pipeline_name: String,
    },
    PipelineEndpointSendError {
        pipeline_id: PipelineId,
        pipeline_name: Option<String>,
        url: String,
        error: String,
    },
    PipelineEndpointResponseBodyError {
        pipeline_id: PipelineId,
        pipeline_name: Option<String>,
        url: String,
        error: String,
    },
    PipelineEndpointResponseJsonParseError {
        pipeline_id: PipelineId,
        pipeline_name: Option<String>,
        url: String,
        error: String,
    },
    PipelineEndpointInvalidResponse {
        pipeline_id: PipelineId,
        error: String,
    },
    // Automaton
    PipelineProvisioningTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    PipelineInitializingTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    PipelineShutdownTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    // Runner
    PipelineStartupError {
        pipeline_id: PipelineId,
        // TODO: This should be IOError, so we can serialize the error code
        // similar to `DbspError::IO`.
        error: String,
    },
    PipelineShutdownError {
        pipeline_id: PipelineId,
        // TODO: This should be IOError, so we can serialize the error code
        // similar to `DbspError::IO`.
        error: String,
    },
    PortFileParseError {
        pipeline_id: PipelineId,
        error: String,
    },
    BinaryFetchError {
        pipeline_id: PipelineId,
        error: String,
    },
    PipelineUnreachable {
        original_error: String,
    },
}

impl DetailedError for RunnerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PipelineMissingDeploymentLocation { .. } => {
                Cow::from("PipelineMissingDeploymentLocation")
            }
            Self::PipelineMissingProgramInfo { .. } => Cow::from("PipelineMissingProgramInfo"),
            Self::PipelineMissingProgramBinaryUrl { .. } => {
                Cow::from("PipelineMissingProgramBinaryUrl")
            }
            Self::RunnerEndpointSendError { .. } => Cow::from("RunnerEndpointSendError"),
            Self::RunnerUnreachable { .. } => Cow::from("RunnerUnreachable"),
            Self::LogFollowRequestChannelFull { .. } => Cow::from("LogFollowRequestChannelFull"),
            Self::LogFollowRequestChannelClosed { .. } => {
                Cow::from("LogFollowRequestChannelClosed")
            }
            Self::PipelineNotRunningOrPaused { .. } => Cow::from("PipelineNotRunningOrPaused"),
            Self::PipelineEndpointSendError { .. } => Cow::from("PipelineEndpointSendError"),
            Self::PipelineEndpointResponseBodyError { .. } => {
                Cow::from("PipelineEndpointResponseBodyError")
            }
            Self::PipelineEndpointResponseJsonParseError { .. } => {
                Cow::from("PipelineEndpointResponseJsonParseError")
            }
            Self::PipelineEndpointInvalidResponse { .. } => {
                Cow::from("PipelineEndpointInvalidResponse")
            }
            Self::PipelineProvisioningTimeout { .. } => Cow::from("PipelineProvisioningTimeout"),
            Self::PipelineInitializingTimeout { .. } => Cow::from("PipelineInitializingTimeout"),
            Self::PipelineShutdownTimeout { .. } => Cow::from("PipelineShutdownTimeout"),
            Self::PipelineStartupError { .. } => Cow::from("PipelineStartupError"),
            Self::PipelineShutdownError { .. } => Cow::from("PipelineShutdownError"),
            Self::PortFileParseError { .. } => Cow::from("PortFileParseError"),
            Self::BinaryFetchError { .. } => Cow::from("BinaryFetchError"),
            Self::PipelineUnreachable { .. } => Cow::from("PipelineUnreachable"),
        }
    }
}

impl Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PipelineMissingDeploymentLocation {
                pipeline_id,
                pipeline_name,
            } => {
                write!(
                    f,
                    "Pipeline {pipeline_name} ({pipeline_id}) is missing its deployment location"
                )
            }
            Self::PipelineMissingProgramInfo {
                pipeline_id,
                pipeline_name,
            } => {
                write!(
                    f,
                    "Pipeline {pipeline_name} ({pipeline_id}) is missing its program info"
                )
            }
            Self::PipelineMissingProgramBinaryUrl {
                pipeline_id,
                pipeline_name,
            } => {
                write!(
                    f,
                    "Pipeline {pipeline_name} ({pipeline_id}) is missing its program binary URL"
                )
            }
            Self::RunnerEndpointSendError {
                url,
                error,
            } => {
                write!(
                    f,
                    "Sending request to URL {url} of runner failed: {error}"
                )
            }
            Self::RunnerUnreachable { original_error } => {
                write!(f, "Runner is unreachable. This indicates that the runner either is still starting up or has crashed unexpectedly (original error: {original_error}).")
            }
            Self::LogFollowRequestChannelFull => {
                write!(f, "Log follow request channel is full. This indicates that the runner logging is overwhelmed.")
            }
            Self::LogFollowRequestChannelClosed => {
                write!(f, "Log follow request channel is closed. This indicates that the runner crashed unexpectedly.")
            }
            Self::PipelineNotRunningOrPaused {
                pipeline_id,
                pipeline_name,
            } => {
                write!(
                    f,
                    "Pipeline {pipeline_name} ({pipeline_id}) is not currently running or paused."
                )
            }
            Self::PipelineEndpointSendError {
                pipeline_id,
                pipeline_name,
                url,
                error,
            } => {
                match pipeline_name {
                    None => write!(
                        f,
                        "Sending request to URL {url} of pipeline {pipeline_id} failed: {error}"
                    ),
                    Some(name) => write!(
                        f,
                        "Sending request to URL {url} of pipeline {name} ({pipeline_id}) failed: {error}"
                    )
                }
            }
            Self::PipelineEndpointResponseBodyError {
                pipeline_id,
                pipeline_name,
                url,
                error,
            } => {
                match pipeline_name {
                    None => write!(
                        f,
                        "Response body from URL {url} of pipeline {pipeline_id} could not be read: {error}"
                    ),
                    Some(name) => write!(
                        f,
                        "Response body from URL {url} of pipeline {name} ({pipeline_id}) could not be read: {error}"
                    )
                }
            }
            Self::PipelineEndpointResponseJsonParseError {
                pipeline_id,
                pipeline_name,
                url,
                error,
            } => {
                match pipeline_name {
                    None => write!(
                        f,
                        "Response body of request to URL {url} of pipeline {pipeline_id} could not be parsed as JSON: {error}"
                    ),
                    Some(name) => write!(
                        f,
                        "Response body of request to URL {url} of pipeline {name} ({pipeline_id}) could not be parsed as JSON: {error}"
                    )
                }
            }
            Self::PipelineEndpointInvalidResponse {
                pipeline_id,
                error,
            } => {
                write!(
                    f,
                    "Pipeline {pipeline_id} received an invalid endpoint response: {error}"
                )
            }
            Self::PipelineProvisioningTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(
                    f,
                    "Waiting for provisioning of pipeline {pipeline_id} timed out after {timeout:?}"
                )
            }
            Self::PipelineInitializingTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(
                    f,
                    "Waiting for initialization of pipeline {pipeline_id} timed out after {timeout:?}"
                )
            }
            Self::PipelineShutdownTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(
                    f,
                    "Waiting for shutdown of pipeline {pipeline_id} timed out after {timeout:?}"
                )
            }
            Self::PipelineStartupError { pipeline_id, error } => {
                write!(f, "Failed to start pipeline {pipeline_id}: {error}")
            }
            Self::PipelineShutdownError { pipeline_id, error } => {
                write!(f, "Failed to shutdown pipeline {pipeline_id}: {error}")
            }
            Self::PortFileParseError { pipeline_id, error } => {
                write!(
                    f,
                    "Could not parse port for pipeline {pipeline_id} from port file: {error}"
                )
            }
            Self::BinaryFetchError { pipeline_id, error } => {
                write!(
                    f,
                    "Failed to fetch binary executable for running pipeline {pipeline_id}: {error}"
                )
            }
            Self::PipelineUnreachable { original_error } => {
                write!(f, "Pipeline is unreachable. This indicates that the pipeline ran out of memory or crashed unexpectedly (original error: {original_error}).")
            }
        }
    }
}

impl From<RunnerError> for ErrorResponse {
    fn from(val: RunnerError) -> Self {
        ErrorResponse::from(&val)
    }
}

impl StdError for RunnerError {}

impl ResponseError for RunnerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::PipelineMissingDeploymentLocation { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineMissingProgramInfo { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineMissingProgramBinaryUrl { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerEndpointSendError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerUnreachable { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::LogFollowRequestChannelFull { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::LogFollowRequestChannelClosed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineNotRunningOrPaused { .. } => StatusCode::BAD_REQUEST,
            Self::PipelineEndpointSendError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineEndpointResponseBodyError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineEndpointResponseJsonParseError { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::PipelineEndpointInvalidResponse { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineProvisioningTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineInitializingTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineStartupError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineShutdownError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineShutdownTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PortFileParseError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BinaryFetchError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PipelineUnreachable { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
