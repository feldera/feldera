use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Checkpoint status returned by the `/checkpoint_status` endpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointStatus {
    /// Most recently successful checkpoint.
    pub success: Option<u64>,

    /// Most recently failed checkpoint, and the associated error.
    pub failure: Option<CheckpointFailure>,
}

/// Information about a failed checkpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointFailure {
    /// Sequence number of the failed checkpoint.
    pub sequence_number: u64,

    /// Error message associated with the failure.
    pub error: String,
}

/// Response to a checkpoint request.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointResponse {
    pub checkpoint_sequence_number: u64,
}

impl CheckpointResponse {
    pub fn new(checkpoint_sequence_number: u64) -> Self {
        Self {
            checkpoint_sequence_number,
        }
    }
}

/// Response to a sync checkpoint request.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointSyncResponse {
    pub checkpoint_uuid: uuid::Uuid,
}

impl CheckpointSyncResponse {
    pub fn new(checkpoint_uuid: uuid::Uuid) -> Self {
        Self { checkpoint_uuid }
    }
}

/// Checkpoint status returned by the `/checkpoint/sync_status` endpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointSyncStatus {
    /// Most recently successful checkpoint sync.
    pub success: Option<uuid::Uuid>,

    /// Most recently failed checkpoint sync, and the associated error.
    pub failure: Option<CheckpointSyncFailure>,
}

/// Information about a failed checkpoint sync.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointSyncFailure {
    /// UUID of the failed checkpoint.
    pub uuid: uuid::Uuid,

    /// Error message associated with the failure.
    pub error: String,
}
