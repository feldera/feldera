use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

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
    pub checkpoint_uuid: Uuid,
}

impl CheckpointSyncResponse {
    pub fn new(checkpoint_uuid: Uuid) -> Self {
        Self { checkpoint_uuid }
    }
}

/// Checkpoint status returned by the `/checkpoint/sync_status` endpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointSyncStatus {
    /// Most recently successful checkpoint sync.
    pub success: Option<Uuid>,

    /// Most recently failed checkpoint sync, and the associated error.
    pub failure: Option<CheckpointSyncFailure>,
}

/// Information about a failed checkpoint sync.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointSyncFailure {
    /// UUID of the failed checkpoint.
    pub uuid: Uuid,

    /// Error message associated with the failure.
    pub error: String,
}

/// Holds meta-data about a checkpoint that was taken for persistent storage
/// and recovery of a circuit's state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// A unique identifier for the given checkpoint.
    ///
    /// This is used to identify the checkpoint in the file-system hierarchy.
    pub uuid: Uuid,
    /// An optional name for the checkpoint.
    pub identifier: Option<String>,
    /// Fingerprint of the circuit at the time of the checkpoint.
    pub fingerprint: u64,
}

impl CheckpointMetadata {
    pub fn new(uuid: Uuid, identifier: Option<String>, fingerprint: u64) -> Self {
        CheckpointMetadata {
            uuid,
            identifier,
            fingerprint,
        }
    }
}

/// Format of `pspine-batches-*.dat` in storage.
///
/// These files exist to be a simple format for higher-level code and outside
/// tools to parse.  The spine itself writes them for that purpose, but it does
/// not read them.
#[derive(Debug, Serialize, Deserialize)]
pub struct PSpineBatches {
    pub files: Vec<String>,
}
