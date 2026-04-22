use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Response returned by `POST /output_endpoints/{name}/reset`.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ResetTokenResponse {
    /// Opaque reset token.
    ///
    /// Pass this string to the `/reset_status` endpoint to check whether the
    /// reset has completed. The token is bound to the pipeline's current
    /// incarnation; if the pipeline is suspended, crashes, or is restarted
    /// before the reset completes, `/reset_status` returns an error and the
    /// client should reissue the reset.
    pub token: String,
}

impl ResetTokenResponse {
    pub fn new(token: String) -> Self {
        Self { token }
    }
}

/// URL-encoded arguments to the `/reset_status` endpoint.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ResetStatusArgs {
    /// Reset token returned by the `/reset` endpoint.
    pub token: String,
}

/// Reset token status returned by `/reset_status`.
#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub enum ResetStatus {
    /// The reset has completed: either the snapshot for this reset's
    /// generation has been delivered, or a later reset's snapshot has, which
    /// subsumes this one.
    #[serde(rename = "complete")]
    Complete,

    /// The reset is still pending: no snapshot at this generation or later
    /// has been delivered yet.
    #[serde(rename = "inprogress")]
    InProgress,
}

/// Response to a reset status request.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ResetStatusResponse {
    pub status: ResetStatus,
}

impl ResetStatusResponse {
    pub fn new(status: ResetStatus) -> Self {
        Self { status }
    }
}
