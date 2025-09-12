use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::utils::ValidationError;
use actix_web::{
    body::BoxBody, http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError,
};
use feldera_types::error::{DetailedError, ErrorResponse};
use indoc::writedoc;
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
    AutomatonCannotConstructProgramBinaryUrl {
        error: String,
    },
    AutomatonMissingDeploymentId,
    AutomatonMissingDeploymentConfig,
    AutomatonMissingDeploymentLocation,
    AutomatonMissingDeploymentInitial,
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
    AutomatonSuspendingComputeTimeout {
        timeout: Duration,
    },
    AutomatonAfterInitializationBecameRunning,
    AutomatonImpossibleDesiredStatus {
        current_status: ResourcesStatus,
        desired_status: ResourcesDesiredStatus,
    },

    // The pipeline runner implementation encounters an error
    RunnerProvisionError {
        error: String,
    },
    RunnerCheckError {
        error: String,
    },
    RunnerStopError {
        error: String,
    },
    RunnerClearError {
        error: String,
    },

    // Interaction with the pipeline runner
    RunnerInteractionUnreachable {
        error: String,
    },
    RunnerInteractionLogFollowRequestChannelFull,
    RunnerInteractionLogFollowRequestChannelClosed,

    // Interaction with the pipeline
    PipelineInteractionNotDeployed {
        status: ResourcesStatus,
        desired_status: ResourcesDesiredStatus,
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
            RunnerError::AutomatonCannotConstructProgramBinaryUrl { .. } => {
                Cow::from("AutomatonCannotConstructProgramBinaryUrl")
            }
            RunnerError::AutomatonMissingDeploymentId => Cow::from("AutomatonMissingDeploymentId"),
            RunnerError::AutomatonMissingDeploymentConfig => {
                Cow::from("AutomatonMissingDeploymentConfig")
            }
            RunnerError::AutomatonMissingDeploymentLocation => {
                Cow::from("AutomatonMissingDeploymentLocation")
            }
            RunnerError::AutomatonMissingDeploymentInitial => {
                Cow::from("AutomatonMissingDeploymentInitial")
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
            RunnerError::AutomatonSuspendingComputeTimeout { .. } => {
                Cow::from("AutomatonSuspendingComputeTimeout")
            }
            RunnerError::AutomatonAfterInitializationBecameRunning => {
                Cow::from("AutomatonAfterInitializationBecameRunning")
            }
            RunnerError::AutomatonImpossibleDesiredStatus { .. } => {
                Cow::from("AutomatonImpossibleDesiredStatus")
            }
            RunnerError::RunnerProvisionError { .. } => Cow::from("RunnerProvisionError"),
            RunnerError::RunnerCheckError { .. } => Cow::from("RunnerCheckError"),
            RunnerError::RunnerStopError { .. } => Cow::from("RunnerStopError"),
            RunnerError::RunnerClearError { .. } => Cow::from("RunnerClearError"),
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
            Self::AutomatonCannotConstructProgramBinaryUrl { error } => {
                write!(f, "Cannot construct program binary URL due to: {error}")
            }
            Self::AutomatonMissingDeploymentId => {
                write!(
                    f,
                    "Unable to provision pipeline because its deployment identifier is missing"
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
            Self::AutomatonMissingDeploymentInitial => {
                write!(
                    f,
                    "The deployment initial desired runtime status is missing"
                )
            }
            Self::AutomatonInvalidRuntimeConfig { value, error } => {
                write!(
                    f,
                    "The runtime configuration:\n{value:#}\n\n... is not valid due to: {error}.\n\n\
                    This indicates a backward-incompatible platform upgrade occurred. \
                    Stop and update the 'runtime_config' field of the pipeline to resolve this."
                )
            }
            Self::AutomatonInvalidProgramInfo { value, error } => {
                write!(
                    f,
                    "The program information:\n{value:#}\n\n... is not valid due to: {error}.\n\n\
                    This indicates a backward-incompatible platform upgrade occurred. \
                    Stop and recompile the pipeline to resolve this."
                )
            }
            Self::AutomatonInvalidDeploymentConfig { value, error } => {
                write!(
                    f,
                    "The deployment configuration:\n{value:#}\n\n... is not valid due to: {error}.\n\n\
                    This indicates a backward-incompatible platform upgrade occurred. \
                    Stop and restart the pipeline to resolve this."
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
                    differs from the runner platform version ({runner_platform_version}) -- stop and restart the pipeline to resolve this"
                )
            }
            Self::AutomatonProvisioningTimeout { timeout } => {
                writedoc! {
                    f,
                    "
                    Pipeline could not be provisioned within the timeout of {}s. Possible reasons:

                    1) It takes too long to bring up the pipeline (e.g., cloud resources) compared to the timeout.
                       If so, try increasing `provisioning_timeout_secs` in the Pipeline settings.

                    2) Your cloud environment could not provision some required resources like volumes or containers.
                       The pipeline logs might provide more insight if this is the case.
                    ",
                    timeout.as_secs()
                }
            }
            Self::AutomatonSuspendingComputeTimeout { timeout } => {
                writedoc! {
                    f,
                    "
                    The pipeline could not suspend its compute resources within the timeout of {}s.
                    This can happen if Kubernetes takes too long to scale down these resources.
                    ",
                    timeout.as_secs()
                }
            }
            Self::AutomatonAfterInitializationBecameRunning => {
                write!(
                    f,
                    "Pipeline immediately became running rather than paused after initialization"
                )
            }
            Self::AutomatonImpossibleDesiredStatus {
                current_status,
                desired_status,
            } => {
                write! {
                    f,
                    "
                    Current deployment status {current_status} cannot reach desired status {desired_status}
                    "
                }
            }
            Self::RunnerProvisionError { error } => {
                write!(f, "Pipeline provision failed: {error}")
            }
            Self::RunnerCheckError { error } => {
                write!(f, "Pipeline check failed: compute and/or storage resources encountered a fatal error.\n\n{error}")
            }
            Self::RunnerStopError { error } => {
                write!(f, "Pipeline stop failed (will retry): {error}")
            }
            Self::RunnerClearError { error } => {
                write!(f, "Pipeline storage clear failed (will retry): {error}")
            }
            Self::RunnerInteractionUnreachable { error } => {
                write!(
                    f,
                    "Unable to reach pipeline runner to interact due to: {error}"
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
                desired_status: _,
            } => {
                write!(
                    f,
                    "Unable to interact with pipeline because the deployment status ({status}) \
                    indicates it is not (yet) fully provisioned"
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
            Self::AutomatonCannotConstructProgramBinaryUrl { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::AutomatonMissingDeploymentId => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonMissingDeploymentConfig => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonMissingDeploymentLocation => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonMissingDeploymentInitial => StatusCode::INTERNAL_SERVER_ERROR,
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
            Self::AutomatonSuspendingComputeTimeout { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonAfterInitializationBecameRunning => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AutomatonImpossibleDesiredStatus { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerProvisionError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerCheckError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerStopError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RunnerClearError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
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
