use bytemuck::NoUninit;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{suspend::SuspendError, transaction::TransactionId};

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
#[derive(Deserialize, Serialize, ToSchema)]
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
#[schema(as = CompletedWatermark)]
pub struct ExternalCompletedWatermark {
    /// Metadata that describes the position in the input stream (e.g., Kafka partition/offset pairs).
    #[schema(value_type = Object)]
    pub metadata: JsonValue,
    /// Timestamp when the data was ingested from the wire.
    pub ingested_at: String,
    /// Timestamp when the data was processed by the circuit.
    pub processed_at: String,
    /// Timestamp when all outputs produced from this input have been pushed to all output endpoints.
    pub completed_at: String,
}

/// Performance metrics for an input endpoint.
#[derive(Default, Deserialize, Serialize, ToSchema)]
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
#[derive(Serialize, Deserialize, ToSchema)]
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
    /// Endpoint has been paused by the user.
    pub paused: bool,
    /// Endpoint is currently a barrier to checkpointing and suspend.
    pub barrier: bool,
    /// The latest completed watermark.
    #[schema(value_type = Option<CompletedWatermark>)]
    pub completed_frontier: Option<ExternalCompletedWatermark>,
}

/// Performance metrics for an output endpoint.
#[derive(Default, Deserialize, Serialize, ToSchema, PartialEq, Eq, PartialOrd, Ord)]
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
    pub total_processed_input_records: u64,
    /// Extra memory in use beyond that used for queuing records.
    pub memory: u64,
}

/// Output endpoint status information.
#[derive(Deserialize, Serialize, ToSchema)]
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
}

/// Global controller metrics.
#[derive(Default, Serialize, Deserialize, ToSchema)]
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
    /// Entities that initiated the current transaction.
    #[schema(value_type = TransactionInitiators)]
    pub transaction_initiators: ExternalTransactionInitiators,
    /// Resident set size of the pipeline process, in bytes.
    pub rss_bytes: u64,
    /// CPU time used by the pipeline across all threads, in milliseconds.
    pub cpu_msecs: u64,
    /// Time since the pipeline process started, in milliseconds.
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
#[derive(Deserialize, Serialize, ToSchema, Default)]
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
