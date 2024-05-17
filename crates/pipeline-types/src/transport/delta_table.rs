use crate::config::TransportConfigVariant;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Delta table output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DeltaTableWriterConfig {
    /// Table URI.
    pub uri: String,
    /// Storage options for configuring backend object store.
    ///
    /// For specific options available for different storage backends, see:
    /// * [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
    /// * [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
    /// * [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
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
    Snapshot {
        /// Optional row filter.
        ///
        /// When specified, only rows that satisfy the filter condition are included in the
        /// snapshot.  The condition must be a valid SQL Boolean expression that can be used in
        /// the `where` clause of the `select * from snapshot where ...` query.
        ///
        /// This option can be used to specify the range of event times to include in the snapshot,
        /// e.g.: `ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'`.
        snapshot_filter: Option<String>,

        /// Optional table version for the snapshot.
        ///
        /// When specified, the connector retrieves the snapshot of the smallest version,
        /// `v >= snapshot_version` in the transaction log of the table.
        ///
        /// Note: at most one of `snapshot_version` and `snapshot_datetime` options can be specified.
        /// When neither of the two options is specified, the latest committed version of the table
        /// is used.
        snapshot_version: Option<i64>,

        /// Optional timestamp for the snapshot.
        ///
        /// When specified, the connector computes the snapshot of the smallest version
        /// created at time `t >= snapshot_datetime` in the transaction log of the table.
        ///
        /// Note: at most one of `snapshot_version` and `snapshot_datetime` options can be specified.
        /// When neither of the two options is specified, the latest committed version of the table
        /// is used.
        snapshot_datetime: Option<String>,
    },
    /// Follow the changelog of the table, only ingesting changes (new and deleted rows).
    #[serde(rename = "follow")]
    Follow {
        /// Optional table version to start reading from.
        ///
        /// When specified, the connector follows the changelog starting from the first version
        /// `v >= start_version` of the table.
        ///
        /// Note: at most one of `start_version` and `start_datetime` options can be specified.
        /// When neither of the two options is specified, the connector ingests changes that show
        /// up _after_ the latest committed version of the table.
        start_version: Option<i64>,

        /// Optional timestamp to start reading from.
        ///
        /// When specified, the connector follows the changelog starting from the first version
        /// of the table created at time `t >= start_datetime`.
        ///
        /// Note: at most one of `start_version` and `start_datetime` options can be specified.
        /// When neither of the two options is specified, the connector ingests changes that show
        /// up _after_ the latest committed version of the table.
        start_datetime: Option<String>,
    },

    /// Take a snapshot of the table before switching to the `follow` mode.
    #[serde(rename = "snapshot_and_follow")]
    SnapshotAndFollow {
        /// Optional row filter.
        ///
        /// When specified, only rows that satisfy the filter condition are included in the
        /// snapshot.  The condition must be a valid SQL Boolean expression that can be used in
        /// the `where` clause of the `select * from snapshot where ...` query.
        ///
        /// This option can be used to specify the range of event times to include in the snapshot,
        /// e.g.: `ts > '2005-01-01 00:00:00'`.
        ///
        /// Note: the filter condition only applies to the initial snapshot of the table, and not
        /// to the records subsequently ingested in the `follow` mode.
        snapshot_filter: Option<String>,

        /// Optional table version to start following from.
        ///
        /// When specified, the connector retrieves the snapshot of the first version
        /// `v >= start_version` of the table and starts following transaction log after that.
        ///
        /// Note: at most one of `start_version` and `start_datetime` options can be specified.
        /// When neither of the two options is specified, the connector uses the latest committed
        /// version.
        start_version: Option<i64>,

        /// Optional timestamp to start following from.
        ///
        /// When specified, the connector retrives the snapshot of the first version
        /// of the table created at time `t >= start_datetime` and starts following the change
        /// log after that.
        ///
        /// Note: at most one of `start_version` and `start_datetime` options can be specified.
        /// When neither of the two options is specified, the connector uses the latest committed
        /// version.
        start_datetime: Option<String>,
    },
}

/// Delta table output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DeltaTableReaderConfig {
    /// Table URI.
    pub uri: String,
    /// Storage options for configuring backend object store.
    ///
    /// For specific options available for different storage backends, see:
    /// * [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
    /// * [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
    /// * [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
    pub object_store_config: HashMap<String, String>,

    /// Table column that serves as an event timestamp.
    ///
    /// When this option is specified, and `mode` is one of `snapshot` or `snapshot_and_follow`,
    /// the snapshot of the table will be sorted by the corresponding column.
    // TODO: The above semantics is expensive and unnecessary.  What is really needed is to maintain
    // the `LATENESS` guarantee of the column, for which it is sufficient to ingest rows sorted up
    // to lateness by issuing a series of queries for lateness-width time ranges to the table.
    pub timestamp_column: Option<String>,

    /// Table read mode.
    pub mode: DeltaTableIngestMode,
}

impl DeltaTableReaderConfig {
    /// `true` if the configuration requires taking an initial snapshot of the table.
    pub fn snapshot(&self) -> bool {
        matches!(
            &self.mode,
            DeltaTableIngestMode::Snapshot { .. } | DeltaTableIngestMode::SnapshotAndFollow { .. }
        )
    }

    /// `true` if the configuration requires following the transaction log of the table
    /// (possibly after taking an initial snapshot).s
    pub fn follow(&self) -> bool {
        matches!(
            &self.mode,
            DeltaTableIngestMode::SnapshotAndFollow { .. } | DeltaTableIngestMode::Follow { .. }
        )
    }

    /// Filter expression to use during snapshotting.
    pub fn snapshot_filter(&self) -> Option<&str> {
        match &self.mode {
            DeltaTableIngestMode::Snapshot {
                snapshot_filter, ..
            } => snapshot_filter.as_ref().map(AsRef::as_ref),
            DeltaTableIngestMode::SnapshotAndFollow {
                snapshot_filter, ..
            } => snapshot_filter.as_ref().map(AsRef::as_ref),
            _ => None,
        }
    }
}

impl TransportConfigVariant for DeltaTableReaderConfig {
    fn name(&self) -> String {
        "delta_table_input".to_string()
    }
}
