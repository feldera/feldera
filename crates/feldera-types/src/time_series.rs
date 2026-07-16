use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Time series to make graphs in the web console easier.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct TimeSeries {
    /// Current time as of the creation of the structure.
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub now: DateTime<Utc>,

    /// Time series.
    ///
    /// These report 60 seconds of samples, one per second.
    pub samples: Vec<SampleStatistics>,
}

/// One sample of time-series data.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SampleStatistics {
    /// Sample time.
    #[serde(with = "chrono::serde::ts_milliseconds")]
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,

    /// Records processed.
    #[serde(rename = "r")]
    pub total_processed_records: u64,

    /// Memory usage in bytes.
    #[serde(rename = "m")]
    pub memory_bytes: u64,

    /// Storage usage in bytes.
    #[serde(rename = "s")]
    pub storage_bytes: u64,

    /// Processing latency (ingest to circuit-processed), microseconds:
    /// p50 across connectors of each connector's median. Absent without samples.
    #[serde(rename = "pp50", skip_serializing_if = "Option::is_none", default)]
    pub processing_latency_p50_micros: Option<u64>,

    /// Processing latency, microseconds: p99 across connectors.
    #[serde(rename = "pp99", skip_serializing_if = "Option::is_none", default)]
    pub processing_latency_p99_micros: Option<u64>,

    /// Completion latency (ingest to all outputs pushed), microseconds:
    /// p50 across connectors.
    #[serde(rename = "cp50", skip_serializing_if = "Option::is_none", default)]
    pub completion_latency_p50_micros: Option<u64>,

    /// Completion latency, microseconds: p99 across connectors.
    #[serde(rename = "cp99", skip_serializing_if = "Option::is_none", default)]
    pub completion_latency_p99_micros: Option<u64>,
}
