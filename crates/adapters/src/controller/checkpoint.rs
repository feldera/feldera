use dbsp::{
    circuit::checkpointer::CheckpointMetadata,
    storage::{
        backend::{StorageBackend, StoragePath},
        buffer_cache::FBuf,
    },
};
use feldera_types::config::PipelineConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::{
    controller::{journal::StepMetadata, stats::GlobalControllerMetrics},
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

impl From<StepMetadata> for CheckpointOffsets {
    fn from(value: StepMetadata) -> Self {
        Self(
            value
                .input_logs
                .into_iter()
                .map(|(name, log)| (name, log.metadata))
                .collect(),
        )
    }
}

impl From<&StepMetadata> for CheckpointOffsets {
    fn from(value: &StepMetadata) -> Self {
        Self(
            value
                .input_logs
                .iter()
                .map(|(name, log)| (name.clone(), log.metadata.clone()))
                .collect(),
        )
    }
}

/// Checkpoint for a pipeline.
#[derive(Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The circuit's checkpoint.
    pub circuit: Option<CheckpointMetadata>,

    /// Step number.
    pub step: Step,

    /// Pipeline configuration.
    pub config: PipelineConfig,

    /// Pipeline statistics to preserve in checkpoints.
    pub stats: CheckpointStats,

    /// Initial offsets for the input endpoints.
    pub input_metadata: CheckpointOffsets,
}

/// Pipeline statistics to preserve in checkpoints.
///
/// So far, these statistics are ones that don't need to be journaled, only
/// checkpointed, because replaying steps from the journal will naturally update
/// them correctly. For example, `processed_records` does not need to be
/// journaled because replaying each step from the journal will also process the
/// same number of records and thus update the statistic correctly.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CheckpointStats {
    /// Number of records processed.
    pub processed_records: u64,

    /// Time elapsed while the pipeline is executing a step, multiplied by the
    /// number of foreground and background threads, in milliseconds.
    pub runtime_elapsed_msecs: u64,

    /// CPU time used by the pipeline process, in millseconds.
    pub cpu_msecs: u64,

    /// Time elapsed with the pipeline running or paused (but not suspended), in
    /// milliseconds.
    pub uptime_msecs: u64,
}

impl CheckpointStats {
    pub fn from_global_metrics(global_metrics: &GlobalControllerMetrics) -> Self {
        Self {
            processed_records: global_metrics.num_total_processed_records(),
            runtime_elapsed_msecs: global_metrics.runtime_elapsed_msecs(),
            cpu_msecs: global_metrics.cpu_msecs(),
            uptime_msecs: global_metrics.uptime_msecs(),
        }
    }
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
        serde_json::from_slice::<Checkpoint>(&data).map_err(|e| {
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
