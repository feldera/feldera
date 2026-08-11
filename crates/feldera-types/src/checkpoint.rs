use std::{fmt::Display, time::Duration};

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
    #[schema(inline)]
    pub fingerprint: Fingerprint,
    /// Total size of the checkpoint files in bytes.
    pub size: Option<u64>,
    /// Total number of steps made.
    pub steps: Option<u64>,
    /// Total number of records processed.
    pub processed_records: Option<u64>,
}

/// Fingerprint for a checkpoint.
///
/// Fingerprints are intentionally limited to the range `0..2**53` to be in the
/// safe integer range for JavaScript.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(u64);

/// The derive can't express `maximum` for a tuple struct (utoipa only allows
/// `Example`/`Default`/`Title`/`Format`/`ValueType`/`As`/`Deprecated` on one),
/// so this is hand-written to advertise the `0..2**53` invariant.
impl<'__s> utoipa::ToSchema<'__s> for Fingerprint {
    fn schema() -> (
        &'__s str,
        utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
    ) {
        (
            "Fingerprint",
            utoipa::openapi::ObjectBuilder::new()
                .schema_type(utoipa::openapi::SchemaType::Integer)
                .format(Some(utoipa::openapi::SchemaFormat::KnownFormat(
                    utoipa::openapi::KnownFormat::Int64,
                )))
                .minimum(Some(0.0))
                .maximum(Some(9007199254740991.0)) // 2**53 - 1
                .description(Some(
                    "Fingerprint of the circuit at the time of the checkpoint, \
                     limited to 0..2**53 to stay within JavaScript's safe integer range.",
                ))
                .into(),
        )
    }
}

impl Display for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for Fingerprint {
    fn from(value: u64) -> Self {
        Self(value & ((1 << 53) - 1))
    }
}

impl Fingerprint {
    /// Returns a `Fingerprint` with the given value, masking off the high bits
    /// to put it in in the correct range.
    pub fn new(value: u64) -> Self {
        value.into()
    }

    /// Returns the fingerprint's value, in the range `0..2**53`.
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl Serialize for Fingerprint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Fingerprint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::new(u64::deserialize(deserializer)?))
    }
}

/// Identifies a host within a multihost pipeline.
///
/// Used to scope checkpoint sync operations (push/pull) to the correct
/// remote subdirectory.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HostInfo {
    /// Zero-based index of this host in the pipeline layout.
    pub host_idx: usize,
    /// Total number of hosts in the pipeline layout.
    pub n_hosts: usize,
}

impl HostInfo {
    /// Returns the remote storage subdirectory prefix for this host,
    /// e.g. `"host0"` for index 0.
    pub fn prefix(&self) -> String {
        if self.host_idx >= self.n_hosts {
            log::warn!(
                "HostInfo::prefix: host_idx {} >= n_hosts {}",
                self.host_idx,
                self.n_hosts
            );
        }
        format!("host{}", self.host_idx)
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

/// A checkpoint that exists in remote object storage.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct RemoteCheckpoint {
    /// UUID of the checkpoint.
    pub uuid: Uuid,
}

#[derive(Debug)]
pub struct CheckpointSyncMetrics {
    pub duration: Duration,
    pub speed: u64,
    pub bytes: u64,
}

/// Status of a `POST /coordination/checkpoint/pull` operation.
///
/// Returned by `GET /coordination/checkpoint/pull_status`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CheckpointPullStatus {
    /// No pull has been requested yet.
    #[default]
    NotRequested,
    /// A pull is currently in progress.
    InProgress,
    /// The pull completed successfully.
    Ok,
    /// The pull failed.
    Error { error: String },
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn host_info_prefix_formats_index() {
        assert_eq!(
            HostInfo {
                host_idx: 0,
                n_hosts: 2
            }
            .prefix(),
            "host0"
        );
        assert_eq!(
            HostInfo {
                host_idx: 1,
                n_hosts: 2
            }
            .prefix(),
            "host1"
        );
        assert_eq!(
            HostInfo {
                host_idx: 42,
                n_hosts: 100
            }
            .prefix(),
            "host42"
        );
    }

    /// Boundary values around the 2**53 cutoff must mask exactly at the
    /// cutoff, not one-off.
    #[test]
    fn fingerprint_new_masks_high_bits() {
        assert_eq!(Fingerprint::new(0).value(), 0);
        assert_eq!(Fingerprint::new(42).value(), 42);
        assert_eq!(Fingerprint::new((1 << 53) - 1).value(), (1 << 53) - 1);
        assert_eq!(Fingerprint::new(1 << 53).value(), 0);
        assert_eq!(Fingerprint::new(u64::MAX).value(), (1 << 53) - 1);
    }

    /// Reproduces issue #6841: a fingerprint above `i64::MAX`, as found in a
    /// checkpoint written before this masking existed (or synced from an
    /// older host), must still deserialize into the safe range so that
    /// re-serializing it over the REST API stays parseable by an `i64`
    /// client such as `fda`. Loading it through `serde` (not just
    /// `Fingerprint::new`) is the point: that's the path a stored checkpoint
    /// list actually takes on startup.
    #[test]
    fn fingerprint_deserialize_masks_out_of_range_value() {
        let raw = "14128757731148314856"; // from the issue's repro, exceeds i64::MAX
        let fp: Fingerprint = serde_json::from_str(raw).unwrap();
        assert_eq!(fp.value(), 5469299714439400);
        assert!(fp.value() <= i64::MAX as u64);

        // Re-serializing must not resurrect the original out-of-range value.
        assert_eq!(serde_json::to_string(&fp).unwrap(), "5469299714439400");
    }

    /// Fingerprint serializes as a bare integer, not `{"0": ...}`, so
    /// existing REST clients that expect a plain number keep working.
    #[test]
    fn fingerprint_serializes_as_plain_integer() {
        assert_eq!(
            serde_json::to_string(&Fingerprint::new(123)).unwrap(),
            "123"
        );
    }

    #[cfg(feature = "testing")]
    mod fingerprint_proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// Every raw hash, however the high bits are set, must mask down
            /// into the JS-safe range and keep its low 53 bits intact.
            #[test]
            fn new_stays_in_safe_range(raw in any::<u64>()) {
                let fp = Fingerprint::new(raw);
                prop_assert!(fp.value() < (1u64 << 53));
                prop_assert_eq!(fp.value(), raw & ((1u64 << 53) - 1));
            }

            /// Values already inside the safe range round-trip through JSON
            /// unchanged, which is what makes the masking a no-op for the
            /// common case.
            #[test]
            fn json_round_trips_in_range(raw in 0..(1u64 << 53)) {
                let fp = Fingerprint::new(raw);
                let json = serde_json::to_string(&fp).unwrap();
                let back: Fingerprint = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(back.value(), raw);
            }
        }
    }
}
