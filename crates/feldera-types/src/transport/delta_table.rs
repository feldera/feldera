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

    /// Change-Data-Capture (CDC) mode.
    ///
    /// The table behaves as an append-only log where every row represents an insert
    /// or delete action.  The order of actions is determined by the `cdc_order_by`
    /// property, and the type of each action is determined by the `cdc_delete_filter`
    /// property.
    ///
    /// In this mode, the connector does not read the initial snapshot of the table
    /// and follows the transaction log starting from the version of the table
    /// specified by the `version` or `datetime` property.
    #[serde(rename = "cdc")]
    Cdc,
}

fn default_num_parsers() -> u32 {
    4
}

/// Delta table input connector configuration.
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
    /// When this option is specified, and `mode` is one of `snapshot` or `snapshot_and_follow`,
    /// table rows are ingested in the timestamp order, respecting the
    /// [`LATENESS`](https://docs.feldera.com/sql/streaming#lateness-expressions)
    /// property of the column: each ingested row has a timestamp no more than `LATENESS`
    /// time units earlier than the most recent timestamp of any previously ingested row.
    /// The ingestion is performed by partitioning the table into timestamp ranges of width
    /// `LATENESS`. Each range is processed sequentially, in increasing timestamp order.
    ///
    /// # Example
    ///
    /// Consider a table with timestamp column of type `TIMESTAMP` and lateness attribute
    /// `INTERVAL 1 DAY`. Assuming that the oldest timestamp in the table is
    /// `2024-01-01T00:00:00``, the connector will fetch all records with timestamps
    /// from `2024-01-01`, then all records for `2024-01-02`, `2024-01-03`, etc., until all records
    /// in the table have been ingested.
    ///
    /// # Requirements
    ///
    /// * The timestamp column must be of a supported type: integer, `DATE`, or `TIMESTAMP`.
    /// * The timestamp column must be declared with non-zero `LATENESS`.
    /// * For efficient ingest, the table must be optimized for timestamp-based
    ///   queries using partitioning, Z-ordering, or liquid clustering.
    pub timestamp_column: Option<String>,

    /// Optional row filter.
    ///
    /// When specified, only rows that satisfy the filter condition are read from the delta table.
    /// The condition must be a valid SQL Boolean expression that can be used in
    /// the `where` clause of the `select * from my_table where ...` query.
    pub filter: Option<String>,

    /// Don't read unused columns from the Delta table.
    ///
    /// When set to `true`, this option instructs the connector to avoid reading
    /// columns from the Delta table that are not used in any view definitions.
    /// To be skipped, the columns must be either nullable or have default
    /// values. This can improve ingestion performance, especially for wide
    /// tables.
    ///
    /// Note: The simplest way to exclude unused columns is to omit them from the Feldera SQL table
    /// declaration. The connector never reads columns that aren't declared in the SQL schema.
    /// Additionally, the SQL compiler emits warnings for declared but unused columns—use these as
    /// a guide to optimize your schema.
    #[serde(default)]
    pub skip_unused_columns: bool,

    /// Optional snapshot filter.
    ///
    /// This option is only valid when `mode` is set to `snapshot` or `snapshot_and_follow`.
    ///
    /// When specified, only rows that satisfy the filter condition are included in the
    /// snapshot.  The condition must be a valid SQL Boolean expression that can be used in
    /// the `where` clause of the `select * from snapshot where ...` query.
    ///
    /// Unlike the `filter` option, which applies to all records retrieved from the table, this
    /// filter only applies to rows in the initial snapshot of the table.
    /// For instance, it can be used to specify the range of event times to include in the snapshot,
    /// e.g.: `ts BETWEEN TIMESTAMP '2005-01-01 00:00:00' AND TIMESTAMP '2010-12-31 23:59:59'`.
    ///
    /// This option can be used together with the `filter` option. During the initial snapshot,
    /// only rows that satisfy both `filter` and `snapshot_filter` are retrieved from the Delta table.
    /// When subsequently following changes in the the transaction log (`mode = snapshot_and_follow`),
    /// all rows that meet the `filter` condition are ingested, regardless of `snapshot_filter`.
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
    /// "2024-12-09T16:09:53+00:00".
    ///
    /// When this option is set, the connector finds and opens the version of the table as of the
    /// specified point in time (based on the server time recorded in the transaction log, not the
    /// event time encoded in the data).  In `snapshot` and `snapshot_and_follow` modes, it
    /// retrieves the snapshot of this version of the table.  In `follow` and `snapshot_and_follow`
    /// modes, it follows transaction log records **after** this version.
    ///
    /// Note: at most one of `version` and `datetime` options can be specified.
    /// When neither of the two options is specified, the latest committed version of the table
    /// is used.
    pub datetime: Option<String>,

    /// A predicate that determines whether the record represents a deletion.
    ///
    /// This setting is only valid in the 'cdc' mode. It specifies a predicate applied to
    /// each row in the Delta table to determine whether the row represents a deletion event.
    /// Its value must be a valid Boolean SQL expression that can be used in a query of the
    /// form `SELECT * from <table> WHERE <cdc_delete_filter>`.
    pub cdc_delete_filter: Option<String>,

    /// An expression that determines the ordering of updates in the Delta table.
    ///
    /// This setting is only valid in the 'cdc' mode. It specifies a predicate applied to
    /// each row in the Delta table to determine the order in which updates in the table should
    /// be applied. Its value must be a valid SQL expression that can be used in a query of the
    /// form `SELECT * from <table> ORDER BY <cdc_order_by>`.
    pub cdc_order_by: Option<String>,

    /// The number of parallel parsing tasks the connector uses to process data read from the
    /// table. Increasing this value can enhance performance by allowing more concurrent processing.
    /// Recommended range: 1–10. The default is 4.
    #[serde(default = "default_num_parsers")]
    pub num_parsers: u32,

    /// Maximum number of concurrent object store reads performed by all Delta Lake connectors.
    ///
    /// This setting is used to limit the number of concurrent reads of the object store in a
    /// pipeline with a large number of Delta Lake connectors. When multiple connectors are simultaneously
    /// reading from the object store, this can lead to transport timeouts.
    ///
    /// When enabled, this setting limits the number of concurrent reads across all connectors.
    /// This is a global setting that affects all Delta Lake connectors, and not just the connector
    /// where it is specified. It should therefore be used at most once in a pipeline.  If multiple
    /// connectors specify this setting, they must all use the same value.
    ///
    /// The default value is 6.
    pub max_concurrent_readers: Option<u32>,

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
            "mode": "follow",
            "cdc_delete_filter": null,
            "cdc_order_by": null
        }"#;

    let config = serde_json::from_str::<DeltaTableReaderConfig>(config_str).unwrap();

    let serialized_config = serde_json::to_string(&config).unwrap();

    let expected = r#"{"uri":"protocol:/path/to/somewhere","timestamp_column":"ts","filter":null,"skip_unused_columns":false,"max_concurrent_readers":null,"mode":"follow","snapshot_filter":"ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'","version":null,"datetime":"2010-12-31 00:00:00Z","customoption1":"val1","customoption2":"val2","cdc_delete_filter":null,"cdc_order_by":null,"num_parsers":4}"#;

    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&serialized_config).unwrap(),
        serde_json::from_str::<serde_json::Value>(expected).unwrap()
    );
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
    /// (possibly after taking an initial snapshot).
    pub fn follow(&self) -> bool {
        matches!(
            &self.mode,
            DeltaTableIngestMode::SnapshotAndFollow
                | DeltaTableIngestMode::Follow
                | DeltaTableIngestMode::Cdc
        )
    }

    pub fn is_cdc(&self) -> bool {
        matches!(&self.mode, DeltaTableIngestMode::Cdc)
    }
}
