use crate::error::ErrorResponse;
use actix_web::body::BoxBody;
use actix_web::http::StatusCode;
use actix_web::{HttpRequest, HttpResponse, HttpResponseBuilder, Responder, ResponseError};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;

/// Runtime status of the pipeline.
///
/// Of the statuses, only `Unavailable` is determined by the runner. All other statuses are
/// determined by the pipeline and taken over by the runner.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
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

    /// The pipeline constantly pulling the latest checkpoint to S3 but not processing any inputs.
    Standby,

    /// The input and output connectors are establishing connections to their data sources and sinks
    /// respectively.
    Initializing,

    /// The pipeline was modified since the last time it was started, and as such it is currently
    /// computing modified views.
    Bootstrapping,

    /// Input records that were stored in the journal but were not yet processed, are being
    /// processed first.
    Replaying,

    /// The input connectors are paused.
    Paused,

    /// The input connectors are running.
    Running,

    /// The pipeline finished checkpointing and pausing.
    Suspended,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum RuntimeDesiredStatus {
    Unavailable,
    Standby,
    #[default]
    Paused,
    Running,
    Suspended,
}

impl RuntimeDesiredStatus {
    pub fn may_transition_to(&self, target: Self) -> bool {
        match (*self, target) {
            (old, new) if old == new => true,
            (Self::Standby, Self::Paused | Self::Running) => true,
            (Self::Paused, Self::Running | Self::Suspended) => true,
            (Self::Running, Self::Paused | Self::Suspended) => true,
            _ => false,
        }
    }

    pub fn may_transition_to_at_startup(&self, target: Self) -> bool {
        match (*self, target) {
            (Self::Suspended, _) => {
                // A suspended pipeline must transition to "paused" or
                // "running".
                matches!(target, Self::Paused | Self::Running)
            }
            (old, new) if old.may_transition_to(new) => true,
            _ => false,
        }
    }
}

impl From<String> for RuntimeDesiredStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "unavailable" => Self::Unavailable,
            "standby" => Self::Standby,
            "paused" => Self::Paused,
            "running" => Self::Running,
            "suspended" => Self::Suspended,
            _ => panic!("Invalid runtime desired status: {value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtendedRuntimeStatus {
    /// Runtime status of the pipeline.
    pub runtime_status: RuntimeStatus,

    /// Human-readable details about the runtime status. Its content can contain for instance an
    /// explanation why it is in this status and any other additional information about it (e.g.,
    /// progress).
    pub runtime_status_details: String,

    /// Runtime desired status of the pipeline.
    pub runtime_desired_status: RuntimeDesiredStatus,
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
