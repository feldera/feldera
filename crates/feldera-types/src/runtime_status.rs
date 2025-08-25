use crate::error::ErrorResponse;
use actix_web::body::BoxBody;
use actix_web::http::StatusCode;
use actix_web::{HttpRequest, HttpResponse, HttpResponseBuilder, Responder, ResponseError};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// Runtime status of the pipeline.
///
/// Of the statuses, only `Unavailable` is determined by the runner. All other statuses are
/// determined by the pipeline and taken over by the runner.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum RuntimeStatus {
    /// The runner was unable to determine the pipeline runtime status. This status is never
    /// returned by the pipeline endpoint itself, but only determined by the runner.
    ///
    /// It can notably occur in two scenarios:
    /// 1. The runner is unable to (in time) receive a response for its sent request to the
    ///    pipeline `/status` endpoint, or it is unable to parse the response.
    /// 2. The runner received back a `503 Service Unavailable` as a response to the request.
    ///    This can occur for example if the pipeline is unable to acquire a lock necessary to
    ///    determine whether it is in any of the other runtime statuses.
    Unavailable,

    /// See `PipelineStatus::Standby`.
    Standby,

    /// See `PipelineStatus::Initializing`.
    Initializing,

    /// See `PipelineStatus::Bootstrapping`.
    Bootstrapping,

    /// See `PipelineStatus::Replaying`.
    Replaying,

    /// See `PipelineStatus::Paused`.
    Paused,

    /// See `PipelineStatus::Running`.
    Running,

    /// See `PipelineStatus::Suspended`.
    Suspended,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtendedRuntimeStatus {
    /// Runtime status of the pipeline.
    pub status: RuntimeStatus,

    /// Human-readable details about the runtime status. Its content can contain for instance an
    /// explanation why it is in this status and any other additional information about it (e.g.,
    /// progress).
    pub details: String,
}

impl Responder for ExtendedRuntimeStatus {
    type Body = BoxBody;

    fn respond_to(self, _req: &HttpRequest) -> HttpResponse<Self::Body> {
        HttpResponseBuilder::new(StatusCode::OK).json(self)
    }
}

impl From<ExtendedRuntimeStatus> for HttpResponse<BoxBody> {
    fn from(value: ExtendedRuntimeStatus) -> Self {
        HttpResponseBuilder::new(StatusCode::OK).json(value)
    }
}

/// Error returned by the pipeline `/status` endpoint.
#[derive(Clone, Debug)]
pub struct ExtendedRuntimeStatusError {
    /// Status code. Returning anything except `503 Service Unavailable` will cause the runner to
    /// forcefully stop the pipeline.
    pub status_code: StatusCode,

    /// Error response.
    pub error: ErrorResponse,
}

impl Display for ExtendedRuntimeStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.status_code, self.error)
    }
}

impl ResponseError for ExtendedRuntimeStatusError {
    fn status_code(&self) -> StatusCode {
        self.status_code
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(self.error.clone())
    }
}

pub enum RuntimeDesiredStatus {
    Standby,
    Paused,
    Running,
    Suspended,
}

/// Decides what endpoint action needs to be taken based on the current runtime status and the
/// desired runtime status. This is used by the runner while the pipeline is provisioned.
pub fn decide_pipeline_action_needed(
    current_status: RuntimeStatus,
    desired_status: RuntimeDesiredStatus,
) -> Option<(Method, String)> {
    match (current_status, desired_status) {
        (RuntimeStatus::Unavailable, _) => {
            // While unavailable, no actions are performed
            None
        }
        (RuntimeStatus::Standby, RuntimeDesiredStatus::Paused) => Some((Method::POST, "activate")),
        (RuntimeStatus::Standby, RuntimeDesiredStatus::Running) => Some((Method::POST, "activate")),
        (RuntimeStatus::Paused, RuntimeDesiredStatus::Running) => Some((Method::GET, "start")),
        (RuntimeStatus::Running, RuntimeDesiredStatus::Paused) => Some((Method::GET, "pause")),
        (RuntimeStatus::Paused, RuntimeDesiredStatus::Suspended) => Some((Method::POST, "suspend")),
        (RuntimeStatus::Running, RuntimeDesiredStatus::Suspended) => {
            Some((Method::POST, "suspend"))
        }
        _ => None,
    }
    .map(|(method, action)| (method, action.to_string()))
}
