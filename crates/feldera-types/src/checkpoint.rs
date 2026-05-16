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

/// Serialized form of `dependencies.json` on disk.
///
/// Two formats. New checkpoints write the struct form (`V2`) carrying both
/// the batch list referenced at the storage root *and* the list of per-operator
/// state files inside the checkpoint dir. Old checkpoints stored only the
/// batch-filename array (`V1`); they remain readable so a rolling upgrade
/// across in-flight checkpoints is safe.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum CheckpointDependencies {
    V2 {
        /// Batch filenames at the storage root (`w*.feldera`) that the
        /// checkpoint references for GC retention.
        batches: Vec<String>,
        /// Per-operator state filenames inside the checkpoint dir
        /// (e.g. `pspine-*.dat`, `z1-*.dat`, `CHECKPOINT`). Consumed by
        /// restore-time verification. Defaulted to empty for forward compat.
        #[serde(default)]
        state_files: Vec<String>,
    },
    /// Legacy form: JSON array of batch filenames at the storage root
    /// (`w*.feldera`). No state-file manifest.
    V1(Vec<String>),
}

impl CheckpointDependencies {
    /// Batch files the checkpoint references at the storage root
    /// (`w*.feldera`). Present in both V1 and V2 checkpoints.
    pub fn batches(&self) -> &[String] {
        match self {
            CheckpointDependencies::V2 { batches, .. } => batches,
            CheckpointDependencies::V1(batches) => batches,
        }
    }

    /// Per-operator state files the checkpoint owned at commit time. These
    /// live inside the checkpoint dir (e.g. `pspine-*.dat`, `z1-*.dat`).
    /// Empty for V1 checkpoints, which predate the state-file manifest.
    pub fn state_files(&self) -> &[String] {
        match self {
            CheckpointDependencies::V2 { state_files, .. } => state_files,
            CheckpointDependencies::V1(_) => &[],
        }
    }
}

/// Serialized form written to `dependencies.json`.  Always emits V2.
#[derive(Debug, Serialize)]
pub struct CheckpointDependenciesWrite<'a> {
    pub batches: &'a [String],
    pub state_files: &'a [String],
}

#[derive(Debug)]
pub struct CheckpointSyncMetrics {
    pub duration: Duration,
    pub speed: u64,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::{CheckpointDependencies, CheckpointDependenciesWrite};

    /// Legacy bare-array dependencies.json from older checkpoints must still
    /// parse, yielding an empty state-file list (no manifest verification).
    #[test]
    fn deserialize_v1_legacy_array() {
        let raw = r#"["w0-aaa.feldera", "w1-bbb.feldera"]"#;
        let deps: CheckpointDependencies = serde_json::from_str(raw).unwrap();
        assert!(deps.state_files().is_empty());
        assert_eq!(deps.batches(), &["w0-aaa.feldera", "w1-bbb.feldera"]);
    }

    /// Current struct form carries both lists.
    #[test]
    fn deserialize_v2_struct() {
        let raw = r#"{
            "batches": ["w0-aaa.feldera"],
            "state_files": ["pspine-0-zzz.dat", "CHECKPOINT"]
        }"#;
        let deps: CheckpointDependencies = serde_json::from_str(raw).unwrap();
        assert_eq!(deps.state_files(), &["pspine-0-zzz.dat", "CHECKPOINT"]);
        assert_eq!(deps.batches(), &["w0-aaa.feldera"]);
    }

    /// V2 without `state_files` (partial writer, partial migration)
    /// deserializes with an empty state-file list rather than failing.
    #[test]
    fn deserialize_v2_missing_state_files_defaults_to_empty() {
        let raw = r#"{"batches": ["w0-aaa.feldera"]}"#;
        let deps: CheckpointDependencies = serde_json::from_str(raw).unwrap();
        assert!(deps.state_files().is_empty());
        assert_eq!(deps.batches(), &["w0-aaa.feldera"]);
    }

    /// Writes emit V2 and round-trip back to the same content.
    #[test]
    fn write_v2_round_trips() {
        let batches = vec!["w0-x.feldera".to_string()];
        let state_files = vec!["pspine-0-y.dat".to_string()];
        let json = serde_json::to_string(&CheckpointDependenciesWrite {
            batches: &batches,
            state_files: &state_files,
        })
        .unwrap();
        let deps: CheckpointDependencies = serde_json::from_str(&json).unwrap();
        assert_eq!(deps.state_files(), state_files.as_slice());
        assert_eq!(deps.batches(), batches.as_slice());
    }
}
