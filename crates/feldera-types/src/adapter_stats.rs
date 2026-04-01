use bytemuck::NoUninit;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    coordination::Step,
    memory_pressure::MemoryPressure,
    suspend::SuspendError,
    transaction::{CommitProgressSummary, TransactionId},
};

/// Pipeline state.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub enum PipelineState {
    /// All input endpoints are paused (or are in the process of being paused).
    #[default]
    Paused,
    /// Controller is running.
    Running,
    /// Controller is being terminated.
    Terminated,
}

// Metrics for an input endpoint.
///
/// Serializes to match the subset of fields needed for error tracking.
#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct InputEndpointErrorMetrics {
    pub endpoint_name: String,
    #[serde(default)]
    pub num_transport_errors: u64,
    #[serde(default)]
    pub num_parse_errors: u64,
}

/// Metrics for an output endpoint.
///
/// Serializes to match the subset of fields needed for error tracking.
#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct OutputEndpointErrorMetrics {
    pub endpoint_name: String,
    #[serde(default)]
    pub num_encode_errors: u64,
    #[serde(default)]
    pub num_transport_errors: u64,
}

/// Endpoint statistics containing metrics.
///
/// Wraps metrics in a structure that matches the full stats API shape.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EndpointErrorStats<T> {
    #[serde(default)]
    pub metrics: T,
}

/// Schema definition for endpoint config that only includes the stream field.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ShortEndpointConfig {
    /// The name of the stream.
    pub stream: String,
}

/// Pipeline error statistics response from the runtime.
///
/// Lightweight response containing only error counts from all endpoints.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PipelineStatsErrorsResponse {
    #[serde(default)]
    pub inputs: Vec<EndpointErrorStats<InputEndpointErrorMetrics>>,
    #[serde(default)]
    pub outputs: Vec<EndpointErrorStats<OutputEndpointErrorMetrics>>,
}

// OpenAPI schema definitions for controller statistics
// These match the serialized JSON structure from the adapters crate

/// Transaction status summarized as a single value.
#[derive(
    Debug, Default, Copy, PartialEq, Eq, Clone, NoUninit, Serialize, Deserialize, ToSchema,
)]
#[repr(u8)]
pub enum TransactionStatus {
    #[default]
    NoTransaction,
    TransactionInProgress,
    CommitInProgress,
}

/// Transaction phase.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
#[schema(as = TransactionPhase)]
pub enum ExternalTransactionPhase {
    /// Transaction is in progress.
    Started,
    /// Transaction has been committed.
    Committed,
}

/// Connector transaction phase with debugging label.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = ConnectorTransactionPhase)]
pub struct ExternalConnectorTransactionPhase {
    /// Current phase of the transaction.
    #[schema(value_type = TransactionPhase)]
    pub phase: ExternalTransactionPhase,
    /// Optional label for debugging.
    pub label: Option<String>,
}

/// Information about entities that initiated the current transaction.
#[derive(Clone, Default, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = TransactionInitiators)]
pub struct ExternalTransactionInitiators {
    /// ID assigned to the transaction (None if no transaction is in progress).
    #[schema(value_type = Option<i64>)]
    pub transaction_id: Option<TransactionId>,
    /// Transaction phase initiated by the API.
    #[schema(value_type = Option<TransactionPhase>)]
    pub initiated_by_api: Option<ExternalTransactionPhase>,
    /// Transaction phases initiated by connectors, indexed by endpoint name.
    #[schema(value_type = BTreeMap<String, ConnectorTransactionPhase>)]
    pub initiated_by_connectors: BTreeMap<String, ExternalConnectorTransactionPhase>,
}

/// A watermark that has been fully processed by the pipeline.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CompletedWatermark {
    /// Metadata that describes the position in the input stream (e.g., Kafka partition/offset pairs).
    #[schema(value_type = Object)]
    pub metadata: JsonValue,
    /// Timestamp when the data was ingested from the wire.
    #[serde(serialize_with = "serialize_timestamp_micros")]
    pub ingested_at: DateTime<Utc>,
    /// Timestamp when the data was processed by the circuit.
    #[serde(serialize_with = "serialize_timestamp_micros")]
    pub processed_at: DateTime<Utc>,
    /// Timestamp when all outputs produced from this input have been pushed to all output endpoints.
    #[serde(serialize_with = "serialize_timestamp_micros")]
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Default, Deserialize, Serialize, ToSchema, Clone)]
pub enum ConnectorHealthStatus {
    #[default]
    Healthy,
    Unhealthy,
}

#[derive(Debug, Default, Deserialize, Serialize, ToSchema, Clone)]
pub struct ConnectorHealth {
    pub status: ConnectorHealthStatus,
    pub description: Option<String>,
}

impl ConnectorHealth {
    pub fn healthy() -> Self {
        Self {
            status: ConnectorHealthStatus::Healthy,
            description: None,
        }
    }
    pub fn unhealthy(description: &str) -> Self {
        Self {
            status: ConnectorHealthStatus::Unhealthy,
            description: Some(description.to_string()),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, ToSchema, Clone)]
pub struct ConnectorError {
    /// Timestamp when the error occurred, serialized as RFC3339 with microseconds.
    #[serde(serialize_with = "serialize_timestamp_micros")]
    pub timestamp: DateTime<Utc>,

    /// Sequence number of the error.
    ///
    /// The client can use this field to detect gaps in the error list reported
    /// by the pipeline. When the connector reports a large number of errors, the
    /// pipeline will only preserve and report the most recent errors of each kind.
    pub index: u64,

    /// Optional tag for the error.
    ///
    /// The tag is used to group errors by their type.
    pub tag: Option<String>,

    /// Error message.
    pub message: String,
}

/// Performance metrics for an input endpoint.
#[derive(Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(as = InputEndpointMetrics)]
pub struct ExternalInputEndpointMetrics {
    /// Total bytes pushed to the endpoint since it was created.
    pub total_bytes: u64,
    /// Total records pushed to the endpoint since it was created.
    pub total_records: u64,
    /// Number of records currently buffered by the endpoint (not yet consumed by the circuit).
    pub buffered_records: u64,
    /// Number of bytes currently buffered by the endpoint (not yet consumed by the circuit).
    pub buffered_bytes: u64,
    /// Number of transport errors.
    pub num_transport_errors: u64,
    /// Number of parse errors.
    pub num_parse_errors: u64,
    /// True if end-of-input has been signaled.
    pub end_of_input: bool,
}

/// Input endpoint status information.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(as = InputEndpointStatus)]
pub struct ExternalInputEndpointStatus {
    /// Endpoint name.
    pub endpoint_name: String,
    /// Endpoint configuration.
    pub config: ShortEndpointConfig,
    /// Performance metrics.
    #[schema(value_type = InputEndpointMetrics)]
    pub metrics: ExternalInputEndpointMetrics,
    /// The first fatal error that occurred at the endpoint.
    pub fatal_error: Option<String>,
    /// Recent parse errors on this endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parse_errors: Option<Vec<ConnectorError>>,
    /// Recent transport errors on this endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_errors: Option<Vec<ConnectorError>>,
    /// Health status of the connector.
    #[serde(default)]
    pub health: Option<ConnectorHealth>,
    /// Endpoint has been paused by the user.
    pub paused: bool,
    /// Endpoint is currently a barrier to checkpointing and suspend.
    pub barrier: bool,
    /// The latest completed watermark.
    #[schema(value_type = Option<CompletedWatermark>)]
    pub completed_frontier: Option<CompletedWatermark>,
}

/// Performance metrics for an output endpoint.
#[derive(Debug, Default, Deserialize, Serialize, ToSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schema(as = OutputEndpointMetrics)]
pub struct ExternalOutputEndpointMetrics {
    /// Records sent on the underlying transport.
    pub transmitted_records: u64,
    /// Bytes sent on the underlying transport.
    pub transmitted_bytes: u64,
    /// Number of queued records.
    pub queued_records: u64,
    /// Number of queued batches.
    pub queued_batches: u64,
    /// Number of records pushed to the output buffer.
    pub buffered_records: u64,
    /// Number of batches in the buffer.
    pub buffered_batches: u64,
    /// Number of encoding errors.
    pub num_encode_errors: u64,
    /// Number of transport errors.
    pub num_transport_errors: u64,
    /// The number of input records processed by the circuit.
    ///
    /// This metric tracks the end-to-end progress of the pipeline: the output
    /// of this endpoint is equal to the output of the circuit after
    /// processing `total_processed_input_records` records.
    ///
    /// In a multihost pipeline, this count reflects only the input records
    /// processed on the same host as the output endpoint, which is not usually
    /// meaningful.
    pub total_processed_input_records: u64,
    /// The number of steps whose input records have been processed by the
    /// endpoint.
    ///
    /// This is meaningful in a multihost pipeline because steps are
    /// synchronized across all of the hosts.
    ///
    /// # Interpretation
    ///
    /// This is a count, not a step number.  If `total_processed_steps` is 0, no
    /// steps have been processed to completion.  If `total_processed_steps >
    /// 0`, then the last step whose input records have been processed to
    /// completion is `total_processed_steps - 1`. A record that was ingested in
    /// step `n` is fully processed when `total_processed_steps > n`.
    #[schema(value_type = u64)]
    pub total_processed_steps: Step,
    /// Extra memory in use beyond that used for queuing records.
    pub memory: u64,
}

/// Output endpoint status information.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = OutputEndpointStatus)]
pub struct ExternalOutputEndpointStatus {
    /// Endpoint name.
    pub endpoint_name: String,
    /// Endpoint configuration.
    pub config: ShortEndpointConfig,
    /// Performance metrics.
    #[schema(value_type = OutputEndpointMetrics)]
    pub metrics: ExternalOutputEndpointMetrics,
    /// The first fatal error that occurred at the endpoint.
    pub fatal_error: Option<String>,
    /// Recent encoding errors on this endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encode_errors: Option<Vec<ConnectorError>>,
    /// Recent transport errors on this endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_errors: Option<Vec<ConnectorError>>,
    /// Health status of the connector.
    #[serde(default)]
    pub health: Option<ConnectorHealth>,
}

/// Global controller metrics.
#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
#[schema(as = GlobalControllerMetrics)]
pub struct ExternalGlobalControllerMetrics {
    /// State of the pipeline: running, paused, or terminating.
    pub state: PipelineState,
    /// The pipeline has been resumed from a checkpoint and is currently bootstrapping new and modified views.
    pub bootstrap_in_progress: bool,
    /// Status of the current transaction.
    pub transaction_status: TransactionStatus,
    /// ID of the current transaction or 0 if no transaction is in progress.
    #[schema(value_type = i64)]
    pub transaction_id: TransactionId,
    /// Elapsed time in milliseconds, according to `transaction_status`:
    ///
    /// - [TransactionStatus::TransactionInProgress]: Time that this transaction
    ///   has been in progress.
    ///
    /// - [TransactionStatus::CommitInProgress]: Time that this transaction has
    ///   been committing.
    pub transaction_msecs: Option<u64>,
    /// Number of records in this transaction, according to
    /// `transaction_status`:
    ///
    /// - [TransactionStatus::TransactionInProgress]: Number of records added so
    ///   far.  More records might be added.
    ///
    /// - [TransactionStatus::CommitInProgress]: Final number of records.
    pub transaction_records: Option<u64>,
    /// Progress of the current transaction commit, if one is in progress.
    pub commit_progress: Option<CommitProgressSummary>,
    /// Entities that initiated the current transaction.
    #[schema(value_type = TransactionInitiators)]
    pub transaction_initiators: ExternalTransactionInitiators,
    /// Resident set size of the pipeline process, in bytes.
    pub rss_bytes: u64,
    /// Memory pressure.
    pub memory_pressure: MemoryPressure,
    /// Memory pressure epoch.
    pub memory_pressure_epoch: u64,
    /// CPU time used by the pipeline across all threads, in milliseconds.
    pub cpu_msecs: u64,
    /// Time since the pipeline process started, including time that the
    /// pipeline was running or paused.
    ///
    /// This is the elapsed time since `start_time`.
    pub uptime_msecs: u64,
    /// Time at which the pipeline process started, in seconds since the epoch.
    #[serde(with = "chrono::serde::ts_seconds")]
    #[schema(value_type = u64)]
    pub start_time: DateTime<Utc>,
    /// Uniquely identifies the pipeline process that started at start_time.
    pub incarnation_uuid: Uuid,
    /// Time at which the pipeline process from which we resumed started, in seconds since the epoch.
    #[serde(with = "chrono::serde::ts_seconds")]
    #[schema(value_type = u64)]
    pub initial_start_time: DateTime<Utc>,
    /// Current storage usage in bytes.
    pub storage_bytes: u64,
    /// Storage usage integrated over time, in megabytes * seconds.
    pub storage_mb_secs: u64,
    /// Time elapsed while the pipeline is executing a step, multiplied by the number of threads, in milliseconds.
    pub runtime_elapsed_msecs: u64,
    /// Total number of records currently buffered by all endpoints.
    pub buffered_input_records: u64,
    /// Total number of bytes currently buffered by all endpoints.
    pub buffered_input_bytes: u64,
    /// Total number of records received from all endpoints.
    pub total_input_records: u64,
    /// Total number of bytes received from all endpoints.
    pub total_input_bytes: u64,
    /// Total number of input records processed by the DBSP engine.
    pub total_processed_records: u64,
    /// Total bytes of input records processed by the DBSP engine.
    pub total_processed_bytes: u64,
    /// Total number of input records processed to completion.
    pub total_completed_records: u64,
    /// If the pipeline is stalled because one or more output connectors' output
    /// buffers are full, this is the number of milliseconds that the current
    /// stall has lasted.
    ///
    /// If this is nonzero, then the output connectors causing the stall can be
    /// identified by noticing `ExternalOutputEndpointMetrics::queued_records`
    /// is greater than or equal to `ConnectorConfig::max_queued_records`.
    ///
    /// In the ordinary case, the pipeline is not stalled, and this value is 0.
    pub output_stall_msecs: u64,
    /// Number of steps that have been initiated.
    ///
    /// # Interpretation
    ///
    /// This is a count, not a step number.  If `total_initiated_steps` is 0, no
    /// steps have been initiated.  If `total_initiated_steps > 0`, then step
    /// `total_initiated_steps - 1` has been started and all steps previous to
    /// that have been completely processed by the circuit.
    #[schema(value_type = u64)]
    pub total_initiated_steps: Step,
    /// Number of steps whose input records have been processed to completion.
    ///
    /// A record is processed to completion if it has been processed by the DBSP engine and
    /// all outputs derived from it have been processed by all output connectors.
    ///
    /// # Interpretation
    ///
    /// This is a count, not a step number.  If `total_completed_steps` is 0, no
    /// steps have been processed to completion.  If `total_completed_steps >
    /// 0`, then the last step whose input records have been processed to
    /// completion is `total_completed_steps - 1`. A record that was ingested
    /// when `total_initiated_steps` was `n` is fully processed when
    /// `total_completed_steps >= n`.
    #[schema(value_type = u64)]
    pub total_completed_steps: Step,
    /// True if the pipeline has processed all input data to completion.
    pub pipeline_complete: bool,
}

/// Complete pipeline statistics returned by the `/stats` endpoint.
///
/// This schema definition matches the serialized JSON structure from
/// `adapters::controller::ControllerStatus`. The actual implementation with
/// atomics and mutexes lives in the adapters crate, which uses ExternalControllerStatus to
/// register this OpenAPI schema, making it available to pipeline-manager
/// without requiring a direct dependency on the adapters crate.
#[derive(Debug, Deserialize, Serialize, ToSchema, Default)]
#[schema(as = ControllerStatus)]
pub struct ExternalControllerStatus {
    /// Global controller metrics.
    #[schema(value_type = GlobalControllerMetrics)]
    pub global_metrics: ExternalGlobalControllerMetrics,
    /// Reason why the pipeline cannot be suspended or checkpointed (if any).
    pub suspend_error: Option<SuspendError>,
    /// Input endpoint configs and metrics.
    #[schema(value_type = Vec<InputEndpointStatus>)]
    pub inputs: Vec<ExternalInputEndpointStatus>,
    /// Output endpoint configs and metrics.
    #[schema(value_type = Vec<OutputEndpointStatus>)]
    pub outputs: Vec<ExternalOutputEndpointStatus>,
}

fn serialize_timestamp_micros<S>(
    timestamp: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&timestamp.to_rfc3339_opts(SecondsFormat::Micros, true))
}

#[cfg(test)]
mod tests {
    use super::ConnectorError;
    use chrono::{DateTime, Utc};

    #[test]
    fn connector_error_timestamp_serializes_with_microsecond_precision() {
        let error = ConnectorError {
            timestamp: DateTime::parse_from_rfc3339("2026-03-08T05:26:42.442438448Z")
                .unwrap()
                .with_timezone(&Utc),
            index: 1,
            tag: None,
            message: "boom".to_string(),
        };

        let json = serde_json::to_string(&error).unwrap();
        assert!(json.contains(r#""timestamp":"2026-03-08T05:26:42.442438Z""#));
    }
}
