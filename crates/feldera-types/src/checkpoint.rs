use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::suspend::TemporarySuspendError;

/// Checkpoint status returned by the `/checkpoint_status` endpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointStatus {
    /// Most recently successful checkpoint.
    pub success: Option<u64>,

    /// Most recently failed checkpoint, and the associated error.
    ///
    /// This tracks transient checkpoint failures (e.g. I/O errors during
    /// writing).  A subsequent successful checkpoint will not clear this
    /// field — it always reflects the *last* failure that occurred.
    pub failure: Option<CheckpointFailure>,
}

/// Current checkpoint activity state.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CheckpointActivity {
    /// No checkpoint is pending or in progress.
    #[default]
    Idle,

    /// A checkpoint has been requested but is delayed for temporary reasons
    /// (e.g. replaying, bootstrapping, transaction in progress, or input
    /// endpoint barriers that require the coordinator to run steps).
    Delayed {
        /// Why the checkpoint cannot proceed yet.
        reasons: Vec<TemporarySuspendError>,
        /// When the delay started (serialized as ISO 8601).
        delayed_since: DateTime<Utc>,
    },

    /// A checkpoint is currently being written to storage.
    InProgress {
        /// When the checkpoint write started (serialized as ISO 8601).
        started_at: DateTime<Utc>,
    },
}

/// Information about a failed checkpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CheckpointFailure {
    /// Sequence number of the failed checkpoint.
    pub sequence_number: u64,

    /// Error message associated with the failure.
    pub error: String,

    /// When the failure occurred (serialized as ISO 8601).
    pub failed_at: DateTime<Utc>,
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

    /// Most recently successful automated periodic checkpoint sync.
    pub periodic: Option<Uuid>,
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CheckpointMetadata {
    /// A unique identifier for the given checkpoint.
    ///
    /// This is used to identify the checkpoint in the file-system hierarchy.
    pub uuid: Uuid,
    /// An optional name for the checkpoint.
    pub identifier: Option<String>,
    /// Fingerprint of the circuit at the time of the checkpoint.
    pub fingerprint: u64,
    /// Total size of the checkpoint files in bytes.
    pub size: Option<u64>,
    /// Total number of steps made.
    pub steps: Option<u64>,
    /// Total number of records processed.
    pub processed_records: Option<u64>,
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

#[derive(Debug)]
pub struct CheckpointSyncMetrics {
    pub duration: Duration,
    pub speed: u64,
    pub bytes: u64,
}
