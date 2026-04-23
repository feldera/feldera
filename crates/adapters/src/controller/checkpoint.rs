use chrono::{DateTime, Utc};
use dbsp::storage::backend::{StorageBackend, StoragePath};
use feldera_types::{
    adapter_stats::ConnectorError, checkpoint::CheckpointMetadata, config::PipelineConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::{
    ControllerError,
    controller::stats::{InputEndpointMetrics, InputEndpointStatus, OutputEndpointStatus},
    transport::Step,
};

/// Initial offsets for the input endpoints in a [Checkpoint].
///
/// This is a subset of [StepMetadata] that is useful for seeking input
/// endpoints to a starting point.
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct CheckpointOffsets(
    /// Maps from an input endpoint name to its metadata.
    pub HashMap<String, JsonValue>,
);

/// Checkpoint for a pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The circuit's checkpoint.
    pub circuit: Option<CheckpointMetadata>,

    /// Step number.
    pub step: Step,

    /// Pipeline configuration.
    pub config: PipelineConfig,

    /// Number of records processed.
    pub processed_records: u64,

    /// Time at which the ultimate ancestor pipeline process of this
    /// checkpoint started.
    #[serde(with = "chrono::serde::ts_seconds", default = "unix_epoch")]
    pub initial_start_time: DateTime<Utc>,

    /// Initial offsets for the input endpoints.
    pub input_metadata: CheckpointOffsets,

    /// Statistics for the input endpoints.
    #[serde(default)]
    pub input_statistics: HashMap<String, CheckpointInputEndpointMetrics>,

    /// Statistics for the output endpoints.
    #[serde(default)]
    pub output_statistics: HashMap<String, CheckpointOutputEndpointMetrics>,
}

impl Checkpoint {
    pub fn display_summary(&self) -> String {
        let input_metadata = self
            .input_metadata
            .0
            .iter()
            .map(|(name, value)| format!("    {name}: {}", serde_json::to_string(value).unwrap()))
            .collect::<Vec<_>>()
            .join("\n");

        let input_statistics = self
            .input_statistics
            .iter()
            .map(|(name, value)| format!("    {name}: {}", serde_json::to_string(value).unwrap()))
            .collect::<Vec<_>>()
            .join("\n");

        let output_statistics = self
            .output_statistics
            .iter()
            .map(|(name, value)| format!("    {name}: {}", serde_json::to_string(value).unwrap()))
            .collect::<Vec<_>>()
            .join("\n");

        let checkpoint_timestamp = self
            .circuit
            .as_ref()
            .and_then(|circuit| circuit.uuid.get_timestamp())
            .and_then(|timestamp| {
                DateTime::<Utc>::from_timestamp(timestamp.to_unix().0 as i64, timestamp.to_unix().1)
            })
            .map(|timestamp| timestamp.to_rfc3339())
            .unwrap_or_else(|| "Unknown".to_string());

        format!(
            r#"Checkpoint made at step {}:
  Initial pipeline start time: {}
  Checkpoint timestamp: {}
  Records processed before the checkpoint: {}
  Checkpoint metadata: {}
  Checkpointed input connector state:
{}
  Checkpointed input connector metrics:
{}
  Checkpointed output connector metrics:
{}"#,
            self.step,
            self.initial_start_time,
            checkpoint_timestamp,
            self.processed_records,
            if let Some(circuit) = &self.circuit {
                serde_json::to_string(circuit).unwrap()
            } else {
                "None".to_string()
            },
            input_metadata,
            input_statistics,
            output_statistics
        )
    }
}

/// Checkpoint for the statistics for an input endpoint.
///
/// This is the checkpointed form of [InputEndpointMetrics] and of the
/// recent error messages associated with the endpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckpointInputEndpointMetrics {
    /// Number of records pushed from the endpoint's queue to the circuit.
    pub circuit_input_records: u64,

    /// Number of bytes pushed from the endpoint's queue to the circuit.
    pub circuit_input_bytes: u64,

    /// Number of transport errors.
    pub num_transport_errors: u64,

    /// Number of parse errors.
    pub num_parse_errors: u64,

    /// Recent transport error messages captured at checkpoint time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transport_errors: Vec<ConnectorError>,

    /// Recent parse error messages captured at checkpoint time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parse_errors: Vec<ConnectorError>,
}

impl CheckpointInputEndpointMetrics {
    pub fn from_endpoint_status(status: &InputEndpointStatus) -> Self {
        Self {
            circuit_input_records: status.metrics.circuit_input_records.load(Ordering::Relaxed),
            circuit_input_bytes: status.metrics.circuit_input_bytes.load(Ordering::Relaxed),
            num_transport_errors: status.metrics.num_transport_errors.load(Ordering::Relaxed),
            num_parse_errors: status.metrics.num_parse_errors.load(Ordering::Relaxed),
            transport_errors: status.transport_errors.lock().unwrap().to_api_type(),
            parse_errors: status.parse_errors.lock().unwrap().to_api_type(),
        }
    }
}

impl From<&CheckpointInputEndpointMetrics> for InputEndpointMetrics {
    fn from(value: &CheckpointInputEndpointMetrics) -> Self {
        Self {
            total_bytes: AtomicU64::new(value.circuit_input_bytes),
            total_records: AtomicU64::new(value.circuit_input_records),
            buffered_records: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            circuit_input_records: AtomicU64::new(value.circuit_input_records),
            circuit_input_bytes: AtomicU64::new(value.circuit_input_bytes),
            num_transport_errors: AtomicU64::new(value.num_transport_errors),
            num_parse_errors: AtomicU64::new(value.num_parse_errors),
            end_of_input: AtomicBool::new(false),
            processing_latency_micros_histogram: Mutex::new(
                InputEndpointMetrics::processing_latency_histogram(),
            ),
            completion_latency_micros_histogram: Mutex::new(
                InputEndpointMetrics::completion_latency_histogram(),
            ),
        }
    }
}

/// Checkpoint for the statistics for an output endpoint.
///
/// This is the checkpointed form of [OutputEndpointMetrics] and of the
/// recent error messages associated with the endpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckpointOutputEndpointMetrics {
    /// Records sent on the underlying transport (HTTP, Kafka, etc.)  to the
    /// endpoint.
    pub transmitted_records: u64,

    /// Bytes sent on the underlying transport (HTTP, Kafka, etc.)  to the
    /// endpoint.
    pub transmitted_bytes: u64,

    /// Number of encoding errors.
    pub num_encode_errors: u64,

    /// Number of transport errors.
    pub num_transport_errors: u64,

    /// Recent encode error messages captured at checkpoint time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encode_errors: Vec<ConnectorError>,

    /// Recent transport error messages captured at checkpoint time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transport_errors: Vec<ConnectorError>,
}

impl CheckpointOutputEndpointMetrics {
    /// Build the checkpoint form from the live endpoint status.
    pub fn from_endpoint_status(status: &OutputEndpointStatus) -> Self {
        let snapshot = status.metrics.snapshot();
        Self {
            // This includes all the records that have been transmitted plus all
            // of the records that will be transmitted by the time we commit the
            // checkpoint.
            transmitted_records: snapshot.transmitted_records
                + snapshot.buffered_records
                + snapshot.queued_records,

            // Only the bytes and errors that have already been transmitted, not
            // including those that will be transmitted by the time we commit
            // the checkpoint (we don't have proper statistics for those).
            transmitted_bytes: snapshot.transmitted_bytes,

            // We can't predict how many errors there will be by the time we
            // commit.
            num_encode_errors: snapshot.num_encode_errors,
            num_transport_errors: snapshot.num_transport_errors,

            encode_errors: status.encode_errors.lock().unwrap().to_api_type(),
            transport_errors: status.transport_errors.lock().unwrap().to_api_type(),
        }
    }
}

/// This is only used if the checkpoint lacks an initial start time, which will
/// only happen if it is old enough that this feature did not exist when it was
/// written.
fn unix_epoch() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

impl Checkpoint {
    /// Reads a checkpoint in JSON format from `path`.
    pub(super) fn read(
        storage: &dyn StorageBackend,
        path: &StoragePath,
    ) -> Result<Self, ControllerError> {
        let data = storage.read(path).map_err(|error| {
            ControllerError::storage_error(format!("{path}: failed to read checkpoint"), error)
        })?;
        serde_json_path_to_error::from_slice::<Checkpoint>(&data).map_err(|e| {
            ControllerError::CheckpointParseError {
                error: e.to_string(),
            }
        })
    }

    /// Writes this checkpoint in JSON format to `path`, atomically replacing
    /// any file that was previously at `path`.
    pub(super) fn write(
        &self,
        storage: &dyn StorageBackend,
        path: &StoragePath,
    ) -> Result<(), ControllerError> {
        storage
            .write_json(path, self)
            .and_then(|file| file.commit())
            .map_err(|error| {
                ControllerError::storage_error(
                    format!("{path}: failed to write pipeline state"),
                    error,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckpointInputEndpointMetrics, CheckpointOutputEndpointMetrics};
    use crate::controller::stats::{ConnectorErrorList, MAX_CONNECTOR_ERRORS};
    use chrono::{TimeZone, Utc};
    use feldera_types::adapter_stats::ConnectorError;

    fn mk_error(index: u64, tag: Option<&str>, msg: &str) -> ConnectorError {
        ConnectorError {
            timestamp: Utc.with_ymd_and_hms(2026, 4, 22, 12, 0, 0).unwrap(),
            index,
            tag: tag.map(str::to_string),
            message: msg.to_string(),
        }
    }

    /// A checkpoint containing error messages round-trips through JSON
    /// serialization, and replaying the errors into a fresh
    /// `ConnectorErrorList` yields the same flat form.
    #[test]
    fn input_checkpoint_roundtrip_preserves_errors() {
        let original = CheckpointInputEndpointMetrics {
            circuit_input_records: 42,
            circuit_input_bytes: 1024,
            num_transport_errors: 2,
            num_parse_errors: 1,
            transport_errors: vec![
                mk_error(0, Some("kafka"), "broker unavailable"),
                mk_error(1, Some("kafka"), "offset out of range"),
            ],
            parse_errors: vec![mk_error(0, None, "bad JSON: unexpected token")],
        };

        let json = serde_json::to_string(&original).unwrap();
        let restored: CheckpointInputEndpointMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.num_transport_errors, 2);
        assert_eq!(restored.num_parse_errors, 1);

        // Replay into a ConnectorErrorList (what InputEndpointStatus::new does).
        let transport = ConnectorErrorList::from_api_type(restored.transport_errors.clone());
        let parse = ConnectorErrorList::from_api_type(restored.parse_errors.clone());

        assert_eq!(transport.to_api_type(), original.transport_errors);
        assert_eq!(parse.to_api_type(), original.parse_errors);
    }

    #[test]
    fn output_checkpoint_roundtrip_preserves_errors() {
        let original = CheckpointOutputEndpointMetrics {
            transmitted_records: 99,
            transmitted_bytes: 2048,
            num_encode_errors: 3,
            num_transport_errors: 0,
            encode_errors: vec![
                mk_error(0, Some("avro"), "schema mismatch"),
                mk_error(1, Some("avro"), "null field"),
                mk_error(2, None, "unknown"),
            ],
            transport_errors: vec![],
        };

        let json = serde_json::to_string(&original).unwrap();
        let restored: CheckpointOutputEndpointMetrics = serde_json::from_str(&json).unwrap();
        let encode = ConnectorErrorList::from_api_type(restored.encode_errors.clone());

        assert_eq!(encode.to_api_type(), original.encode_errors);
        assert!(restored.transport_errors.is_empty());
    }

    /// Old-format checkpoint (written before the error-list fields existed)
    /// must still deserialize — missing fields default to empty vectors
    /// which produce empty `ConnectorErrorList`s on restore, matching the
    /// behavior the pipeline had before this change.
    #[test]
    fn input_checkpoint_backcompat_accepts_missing_error_fields() {
        let old_format = r#"{
            "circuit_input_records": 7,
            "circuit_input_bytes": 128,
            "num_transport_errors": 0,
            "num_parse_errors": 0
        }"#;
        let m: CheckpointInputEndpointMetrics = serde_json::from_str(old_format).unwrap();

        assert_eq!(m.circuit_input_records, 7);
        assert!(m.transport_errors.is_empty());
        assert!(m.parse_errors.is_empty());
        assert!(
            ConnectorErrorList::from_api_type(m.transport_errors)
                .to_api_type()
                .is_empty()
        );
    }

    #[test]
    fn output_checkpoint_backcompat_accepts_missing_error_fields() {
        let old_format = r#"{
            "transmitted_records": 3,
            "transmitted_bytes": 64,
            "num_encode_errors": 0,
            "num_transport_errors": 0
        }"#;
        let m: CheckpointOutputEndpointMetrics = serde_json::from_str(old_format).unwrap();

        assert!(m.encode_errors.is_empty());
        assert!(m.transport_errors.is_empty());
    }

    /// Serializing a checkpoint whose error lists are empty must not emit
    /// the new fields as JSON keys, so checkpoint files written in the
    /// common case stay as compact as before. (Substring check uses
    /// quoted field names to avoid matching `num_transport_errors`.)
    #[test]
    fn input_checkpoint_omits_empty_error_fields_in_json() {
        let m = CheckpointInputEndpointMetrics::default();
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("\"transport_errors\""));
        assert!(!json.contains("\"parse_errors\""));
    }

    /// A malformed checkpoint could hold more errors for a single tag than
    /// `MAX_CONNECTOR_ERRORS` (e.g. written by a build with a higher cap).
    /// `from_api_type` must drop the oldest excess so the in-memory list
    /// stays bounded; the newest entries are the ones kept.
    #[test]
    fn from_api_type_truncates_excess_per_tag() {
        let overflow = 5usize;
        let errors: Vec<ConnectorError> = (0..(MAX_CONNECTOR_ERRORS + overflow) as u64)
            .map(|i| mk_error(i, Some("kafka"), &format!("err {i}")))
            .collect();

        let restored = ConnectorErrorList::from_api_type(errors).to_api_type();

        assert_eq!(restored.len(), MAX_CONNECTOR_ERRORS);
        // The first `overflow` entries must have been dropped; the oldest
        // retained entry has index == overflow.
        assert_eq!(restored.first().unwrap().index, overflow as u64);
        assert_eq!(
            restored.last().unwrap().index,
            (MAX_CONNECTOR_ERRORS + overflow - 1) as u64
        );
    }
}
