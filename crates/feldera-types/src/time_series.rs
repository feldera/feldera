use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Time series to make graphs in the web console easier.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
}
