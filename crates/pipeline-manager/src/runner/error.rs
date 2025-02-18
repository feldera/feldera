use crate::db::types::pipeline::{PipelineDesiredStatus, PipelineStatus};
use crate::db::types::utils::ValidationError;
use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use serde::Serialize;
use std::{borrow::Cow, error::Error as StdError, fmt, fmt::Display, time::Duration};

/// The [`RunnerError`] encompasses runner-related errors, which primarily will show up
/// in the runner, but are also used by the API server in interaction with runner-internal API
/// to provide meaningful errors.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RunnerError {
    // Automaton encountered an error during one of its own operations
    AutomatonMissingProgramInfo,
    AutomatonMissingProgramBinaryUrl,
    AutomatonMissingDeploymentConfig,
    AutomatonMissingDeploymentLocation,
    AutomatonInvalidRuntimeConfig {
        value: serde_json::Value,
        error: ValidationError,
    },
    AutomatonInvalidProgramInfo {
        value: serde_json::Value,
        error: ValidationError,
    },
    AutomatonInvalidDeploymentConfig {
        value: serde_json::Value,
        error: ValidationError,
    },
    AutomatonFailedToSerializeDeploymentConfig {
        error: String,
    },
    AutomatonCannotProvisionDifferentPlatformVersion {
        pipeline_platform_version: String,
        runner_platform_version: String,
    },
    AutomatonProvisioningTimeout {
        timeout: Duration,
    },
    AutomatonInitializingTimeout {
        timeout: Duration,
    },
    AutomatonAfterInitializationBecameRunning,

    // The pipeline runner implementation encounters an error
    RunnerDeploymentProvisionError {
        error: String,
    },
    RunnerDeploymentCheckError {
        error: String,
    },
    RunnerDeploymentShutdownError {
        error: String,
    },

    // Interaction with the pipeline runner
    RunnerInteractionShutdown,
    RunnerInteractionUnreachable {
        error: String,
    },
    RunnerInteractionLogFollowRequestChannelFull,
    RunnerInteractionLogFollowRequestChannelClosed,

    // Interaction with the pipeline
    PipelineInteractionNotDeployed {
        status: PipelineStatus,
        desired_status: PipelineDesiredStatus,
    },
    PipelineInteractionUnreachable {
        error: String,
    },
    PipelineInteractionInvalidResponse {
        error: String,
    },
}

impl DetailedError for RunnerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            RunnerError::AutomatonMissingProgramInfo => Cow::from("AutomatonMissingProgramInfo"),
            RunnerError::AutomatonMissingProgramBinaryUrl => {
                Cow::from("AutomatonMissingProgramBinaryUrl")
            }
            RunnerError::AutomatonMissingDeploymentConfig => {
                Cow::from("AutomatonMissingDeploymentConfig")
            }
            RunnerError::AutomatonMissingDeploymentLocation => {
                Cow::from("AutomatonMissingDeploymentLocation")
            }
            RunnerError::AutomatonInvalidRuntimeConfig { .. } => {
                Cow::from("AutomatonInvalidRuntimeConfig")
            }
            RunnerError::AutomatonInvalidProgramInfo { .. } => {
                Cow::from("AutomatonInvalidProgramInfo")
            }
            RunnerError::AutomatonInvalidDeploymentConfig { .. } => {
                Cow::from("AutomatonInvalidDeploymentConfig")
            }
            RunnerError::AutomatonFailedToSerializeDeploymentConfig { .. } => {
                Cow::from("AutomatonFailedToSerializeDeploymentConfig")
            }
            RunnerError::AutomatonCannotProvisionDifferentPlatformVersion { .. } => {
                Cow::from("AutomatonCannotProvisionDifferentPlatformVersion")
            }
            RunnerError::AutomatonProvisioningTimeout { .. } => {
                Cow::from("AutomatonProvisioningTimeout")
            }
            RunnerError::AutomatonInitializingTimeout { .. } => {
                Cow::from("AutomatonInitializingTimeout")
            }
            RunnerError::AutomatonAfterInitializationBecameRunning => {
                Cow::from("AutomatonAfterInitializationBecameRunning")
            }
            RunnerError::RunnerDeploymentProvisionError { .. } => {
                Cow::from("RunnerDeploymentProvisionError")
            }
            RunnerError::RunnerDeploymentCheckError { .. } => {
                Cow::from("RunnerDeploymentCheckError")
            }
            RunnerError::RunnerDeploymentShutdownError { .. } => {
                Cow::from("RunnerDeploymentShutdownError")
            }
            RunnerError::RunnerInteractionShutdown => Cow::from("RunnerInteractionShutdown"),
            RunnerError::RunnerInteractionUnreachable { .. } => {
                Cow::from("RunnerInteractionUnreachable")
            }
            RunnerError::RunnerInteractionLogFollowRequestChannelFull => {
                Cow::from("RunnerInteractionLogFollowRequestChannelFull")
            }
            RunnerError::RunnerInteractionLogFollowRequestChannelClosed => {
                Cow::from("RunnerInteractionLogFollowRequestChannelClosed")
            }
            RunnerError::PipelineInteractionNotDeployed { .. } => {
                Cow::from("PipelineInteractionNotDeployed")
            }
            RunnerError::PipelineInteractionUnreachable { .. } => {
                Cow::from("PipelineInteractionUnreachable")
            }
            RunnerError::PipelineInteractionInvalidResponse { .. } => {
                Cow::from("PipelineInteractionInvalidResponse")
            }
        }
    }
}

impl Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AutomatonMissingProgramInfo => {
                write!(
                    f,
                    "Unable to provision pipeline because its program information is missing, which is necessary to construct the deployment configuration"
                )
            }
            Self::AutomatonMissingProgramBinaryUrl => {
                write!(
                    f,
                    "Unable to provision pipeline because its program binary URL is missing"
                )
            }
            Self::AutomatonMissingDeploymentConfig => {
                write!(
                    f,
                    "Unable to provision pipeline because its deployment configuration is missing"
                )
            }
            Self::AutomatonMissingDeploymentLocation => {
                write!(
                    f,
                    "Unable to status check the pipeline because its deployment location is missing"
                )
            }
            Self::AutomatonInvalidRuntimeConfig { value, error } => {
                write!(
                    f,
                    "The runtime configuration:\n{value:#}\n\n... is not valid due to: {error}.\n\n\
                    This indicates a backward-incompatible platform upgrade occurred. \
                    Shut down and update the 'runtime_config' field of the pipeline to resolve this."
                )
            }
            Self::AutomatonInvalidProgramInfo { value, error } => {
                write!(
                    f,
                    "The program information:\n{value:#}\n\n... is not valid due to: {error}.\n\n\
                    This indicates a backward-incompatible platform upgrade occurred. \
                    Shut down and recompile the pipeline to resolve this."
                )
            }
            Self::AutomatonInvalidDeploymentConfig { value, error } => {
                write!(
                    f,
                    "The deployment configuration:\n{value:#}\n\n... is not valid due to: {error}.\n\n\
                    This indicates a backward-incompatible platform upgrade occurred. \
                    Shut down and restart the pipeline to resolve this."
                )
            }
            Self::AutomatonFailedToSerializeDeploymentConfig { error } => {
                write!(
                    f,
                    "Failed to serialize deployment configuration due to: {error}"
                )
            }
            Self::AutomatonCannotProvisionDifferentPlatformVersion {
                pipeline_platform_version,
                runner_platform_version,
            } => {
                write!(
                    f,
                    "Unable to provision pipeline because the pipeline platform version ({pipeline_platform_version}) \
                    differs from the runner platform version ({runner_platform_version}) -- shut down and restart the pipeline to resolve this"
                )
            }
            Self::AutomatonProvisioningTimeout { timeout } => {
                write!(
                    f,
                    "Waiting for provisioning timed out after {}s \
                    -- this occurs when the required resources requested by the runner \
                    for deployment took too long to be provisioned",
                    timeout.as_secs()
                )
            }
            Self::AutomatonInitializingTimeout { timeout } => {
                write!(
                    f,
                    "Waiting for initialization timed out after {}s \
                    -- additional information about the cause can likely be found in the pipeline logs",
                    timeout.as_secs()
                )
            }
            Self::AutomatonAfterInitializationBecameRunning => {
                write!(
                    f,
                    "Pipeline immediately became running rather than paused after initialization"
                )
            }
            Self::RunnerDeploymentProvisionError { error } => {
                write!(f, "Deployment provision operation failed: {error}")
            }
            Self::RunnerDeploymentCheckError { error } => {
                write!(f, "Deployment check failed: one or more deployment resources encountered a fatal error.\n\n{error}")
            }
            Self::RunnerDeploymentShutdownError { error } => {
                write!(f, "Deployment shutdown failed (will retry): {error}")
            }
            Self::RunnerInteractionUnreachable { error } => {
                write!(
                    f,
                    "Unable to reach pipeline runner to interact due to: {error}"
                )
            }
            Self::RunnerInteractionShutdown => {
                write!(
                    f,
                    "Unable to interact with pipeline runner because the deployment status is 'shutdown' \
                    -- start the pipeline or wait if it has already been started"
                )
            }
            Self::RunnerInteractionLogFollowRequestChannelFull => {
                write!(f, "Log follow request channel is full -- this indicates that the runner logging is overwhelmed")
            }
            Self::RunnerInteractionLogFollowRequestChannelClosed => {
                write!(f, "Log follow request channel is closed -- this indicates that the runner crashed unexpectedly")
            }
            Self::PipelineInteractionNotDeployed {
                status,
                desired_status,
            } => {
                let resolution = match (status, desired_status) {
                    (PipelineStatus::Shutdown, PipelineDesiredStatus::Shutdown) => {
                        "start the pipeline"
                    }
                    (PipelineStatus::Failed, PipelineDesiredStatus::Running | PipelineDesiredStatus::Paused) => {
                        "investigate the reason why the pipeline failed by looking at the error and logs, \
                        shut down the pipeline to clear the 'failed' state, fix any issues, and start again afterwards"
                    }
                    (_, PipelineDesiredStatus::Running | PipelineDesiredStatus::Paused) => {
                        "wait for the pipeline to become running or paused"
                    }
                    (_, PipelineDesiredStatus::Shutdown) => {
                        "wait for the pipeline to become shutdown and start again afterwards"
                    }
                };
                write!(
                    f,
                    "Unable to interact with pipeline because the deployment status ('{status}') \
                    is not one of the deployed statuses ('running', 'paused' or 'unavailable') \
                    -- to resolve this: {resolution}"
                )
            }
            Self::PipelineInteractionUnreachable { error } => {
                write!(f, "Unable to reach pipeline to interact due to: {error}")
            }
            Self::PipelineInteractionInvalidResponse { error } => {
                write!(
                    f,
                    "During pipeline interaction an unexpected invalid response was encountered: {error}"
                )
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
            Self::AutomatonMissingProgramInfo => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonMissingProgramBinaryUrl => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonMissingDeploymentConfig => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonMissingDeploymentLocation => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonInvalidRuntimeConfig { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonInvalidProgramInfo { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonInvalidDeploymentConfig { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonFailedToSerializeDeploymentConfig { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::AutomatonCannotProvisionDifferentPlatformVersion { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::AutomatonProvisioningTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonInitializingTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonAfterInitializationBecameRunning => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerDeploymentProvisionError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerDeploymentCheckError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerDeploymentShutdownError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerInteractionShutdown { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::RunnerInteractionUnreachable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::RunnerInteractionLogFollowRequestChannelFull { .. } => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::RunnerInteractionLogFollowRequestChannelClosed { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::PipelineInteractionNotDeployed { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::PipelineInteractionUnreachable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::PipelineInteractionInvalidResponse { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}
