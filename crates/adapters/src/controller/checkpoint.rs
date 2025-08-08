use chrono::{DateTime, Utc};
use dbsp::storage::{
    backend::{StorageBackend, StoragePath},
    buffer_cache::FBuf,
};
use feldera_types::{checkpoint::CheckpointMetadata, config::PipelineConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use crate::{
    controller::stats::{InputEndpointMetrics, OutputEndpointMetrics},
    transport::Step,
    ControllerError,
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

/// Checkpoint for the statistics for an input endpoint.
///
/// This is the checkpointed form of [InputEndpointMetrics].
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
}

impl From<&InputEndpointMetrics> for CheckpointInputEndpointMetrics {
    fn from(value: &InputEndpointMetrics) -> Self {
        Self {
            circuit_input_records: value.circuit_input_records.load(Ordering::Relaxed),
            circuit_input_bytes: value.circuit_input_bytes.load(Ordering::Relaxed),
            num_transport_errors: value.num_transport_errors.load(Ordering::Relaxed),
            num_parse_errors: value.num_parse_errors.load(Ordering::Relaxed),
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
        }
    }
}

/// Checkpoint for the statistics for an output endpoint.
///
/// This is the checkpointed form of [OutputEndpointMetrics].
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
}

impl From<&OutputEndpointMetrics> for CheckpointOutputEndpointMetrics {
    fn from(value: &OutputEndpointMetrics) -> Self {
        Self {
            transmitted_records: value.transmitted_records.load(Ordering::Relaxed),
            transmitted_bytes: value.transmitted_bytes.load(Ordering::Relaxed),
            num_encode_errors: value.num_encode_errors.load(Ordering::Relaxed),
            num_transport_errors: value.num_transport_errors.load(Ordering::Relaxed),
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
        let mut content = FBuf::with_capacity(4096);
        serde_json::to_writer(&mut content, self).unwrap();
        storage.write(path, content).map_err(|error| {
            ControllerError::storage_error(format!("{path}: failed to write pipeline state"), error)
        })
    }
}
