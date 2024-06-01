use crate::config::TransportConfigVariant;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Delta table write mode.
///
/// Determines how the Delta table connector handles an existing table at the target location.
#[derive(Default, Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum DeltaTableWriteMode {
    /// New updates will be appended to the existing table at the target location.
    #[default]
    #[serde(rename = "append")]
    Append,

    /// Existing table at the specified location will get truncated.
    ///
    /// The connector truncates the table by outputing delete actions for all
    /// files in the latest snapshot of the table.
    #[serde(rename = "truncate")]
    Truncate,

    /// If a table exists at the specified location, the operation must fail.
    #[serde(rename = "error_if_exists")]
    ErrorIfExists,
}

/// Delta table output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DeltaTableWriterConfig {
    /// Table URI.
    pub uri: String,

    /// Determines how the Delta table connector handles an existing table at the target location.
    #[serde(default)]
    pub mode: DeltaTableWriteMode,

    /// Storage options for configuring backend object store.
    ///
    /// For specific options available for different storage backends, see:
    /// * [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
    /// * [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
    /// * [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
    #[serde(flatten)]
    pub object_store_config: HashMap<String, String>,
}

impl TransportConfigVariant for DeltaTableWriterConfig {
    fn name(&self) -> String {
        "delta_table_output".to_string()
    }
}

/// Delta table read mode.
///
/// Three options are available:
///
/// * `snapshot` - read a snapshot of the table and stop.
///
/// * `follow` - continuously ingest changes to the table, starting from a specified version
///   or timestamp.
///
/// * `snapshot_and_follow` - read a snapshot of the table before switching to continuous ingestion
///   mode.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum DeltaTableIngestMode {
    /// Read a snapshot of the table and stop.
    #[serde(rename = "snapshot")]
    Snapshot,

    /// Follow the changelog of the table, only ingesting changes (new and deleted rows).
    #[serde(rename = "follow")]
    Follow,

    /// Take a snapshot of the table before switching to the `follow` mode.
    #[serde(rename = "snapshot_and_follow")]
    SnapshotAndFollow,
}

/// Delta table output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DeltaTableReaderConfig {
    /// Table URI.
    ///
    /// Example: "s3://feldera-fraud-detection-data/demographics_train"
    pub uri: String,

    /// Table read mode.
    pub mode: DeltaTableIngestMode,

    /// Table column that serves as an event timestamp.
    ///
    ///
    /// When this option is specified, and `mode` is one of `snapshot` or `snapshot_and_follow`,
    /// the snapshot of the table is ingested in the timestamp order.  This setting is required
    /// for tables declared with the
    /// [`LATENESS`](https://www.feldera.com/docs/sql/streaming#lateness-expressions) attribute
    /// in Feldera SQL. It impacts the performance of the connector, since data must be sorted
    /// before pushing it to the pipeline; therefore it is not recommended to use this
    /// settings for tables without `LATENESS`.
    // TODO: The above semantics is expensive and unnecessary.  What is really needed is to maintain
    // the `LATENESS` guarantee of the column, for which it is sufficient to ingest rows sorted up
    // to lateness by issuing a series of queries for lateness-width time ranges to the table.
    pub timestamp_column: Option<String>,

    /// Optional row filter.
    ///
    /// This option is only valid when `mode` is set to `snapshot` or `snapshot_and_follow`.
    ///
    /// When specified, only rows that satisfy the filter condition are included in the
    /// snapshot.  The condition must be a valid SQL Boolean expression that can be used in
    /// the `where` clause of the `select * from snapshot where ...` query.
    ///
    /// This option can be used to specify the range of event times to include in the snapshot,
    /// e.g.: `ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'`.
    pub snapshot_filter: Option<String>,

    /// Optional table version.
    ///
    /// When this option is set, the connector finds and opens the specified version of the table.
    /// In `snapshot` and `snapshot_and_follow` modes, it retrieves the snapshot of this version of
    /// the table.  In `follow` and `snapshot_and_follow` modes, it follows transaction log records
    /// **after** this version.
    ///
    /// Note: at most one of `version` and `datetime` options can be specified.
    /// When neither of the two options is specified, the latest committed version of the table
    /// is used.
    pub version: Option<i64>,

    /// Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format, e.g.,
    /// "2024-12-09T16:09:53+00:00.
    ///
    /// When this option is set, the connector finds and opens the version of the table as of the
    /// specified point in time.  In `snapshot` and `snapshot_and_follow` modes, it retrieves the
    /// snapshot of this version of the table (based on the server time recorded in the transaction
    /// log, not the event time encoded in the data).  In `follow` and `snapshot_and_follow` modes, it
    /// follows transaction log records **after** this version.
    ///
    /// Note: at most one of `version` and `datetime` options can be specified.
    /// When neither of the two options is specified, the latest committed version of the table
    /// is used.
    pub datetime: Option<String>,

    /// Storage options for configuring backend object store.
    ///
    /// For specific options available for different storage backends, see:
    /// * [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
    /// * [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
    /// * [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
    #[serde(flatten)]
    pub object_store_config: HashMap<String, String>,
}

#[cfg(test)]
#[test]
fn test_delta_reader_config_serde() {
    let config_str = r#"{
            "uri": "protocol:/path/to/somewhere",
            "datetime": "2010-12-31 00:00:00Z",
            "snapshot_filter": "ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'",
            "timestamp_column": "ts",
            "customoption1": "val1",
            "customoption2": "val2",
            "mode": "follow"
        }"#;

    let config = serde_json::from_str::<DeltaTableReaderConfig>(config_str).unwrap();

    let serialized_config = serde_json::to_string(&config).unwrap();

    let expected = r#"{"uri":"protocol:/path/to/somewhere","timestamp_column":"ts","mode":"follow","snapshot_filter":"ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'","version":null,"datetime":"2010-12-31 00:00:00Z","customoption1":"val1","customoption2":"val2"}"#;

    assert_eq!(serialized_config, expected);
}

impl DeltaTableReaderConfig {
    /// `true` if the configuration requires taking an initial snapshot of the table.
    pub fn snapshot(&self) -> bool {
        matches!(
            &self.mode,
            DeltaTableIngestMode::Snapshot | DeltaTableIngestMode::SnapshotAndFollow
        )
    }

    /// `true` if the configuration requires following the transaction log of the table
    /// (possibly after taking an initial snapshot).s
    pub fn follow(&self) -> bool {
        matches!(
            &self.mode,
            DeltaTableIngestMode::SnapshotAndFollow | DeltaTableIngestMode::Follow
        )
    }
}

impl TransportConfigVariant for DeltaTableReaderConfig {
    fn name(&self) -> String {
        "delta_table_input".to_string()
    }
}
