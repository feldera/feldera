use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Checkpoint status returned by the `/checkpoint_status` endpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointStatus {
    /// Most recently successful checkpoint.
    pub success: Option<u64>,

    /// Most recently failed checkpoint, and the associated error.
    pub failure: Option<(u64, String)>,
}
