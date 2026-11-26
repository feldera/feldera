use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use utoipa::ToSchema;

/// Iceberg table read mode.
///
/// Three options are available:
///
/// * `snapshot` - read a snapshot of the table and stop.
///
/// * `follow` - continuously ingest changes to the table, starting from a specified snapshot
///   or timestamp.
///
/// * `snapshot_and_follow` - read a snapshot of the table before switching to continuous ingestion
///   mode.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum IcebergIngestMode {
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

impl Display for IcebergIngestMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IcebergIngestMode::Snapshot => f.write_str("snapshot"),
            IcebergIngestMode::Follow => f.write_str("follow"),
            IcebergIngestMode::SnapshotAndFollow => f.write_str("snapshot_and_follow"),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum IcebergCatalogType {
    #[serde(rename = "rest")]
    Rest,
    #[serde(rename = "glue")]
    Glue,
    #[serde(rename = "s3tables")]
    S3Tables,
}

/// Iceberg table transaction mode.
///
/// Determines how the connector breaks up its input into Feldera transactions.
///
/// * `none` - the connector does not group its input into transactions.
/// * `snapshot` - ingest the initial snapshot in one or more transactions (see below). Changes
///   ingested afterward, in the follow phase, are not grouped into transactions.
/// * `catchup` - ingest the initial snapshot like `snapshot`. In the follow phase, the connector
///   batches all currently available table commits into a single transaction. Once that transaction
///   completes, it checks for commits added since the transaction began, ingests them in the next
///   transaction, and repeats continuously. Most efficient for backfill and steady-state following.
/// * `always` - ingest the initial snapshot like `snapshot`. In the follow phase, each table commit
///   is ingested in its own transaction.
///
/// # How the table snapshot is ingested using transactions
///
/// For the initial snapshot (`snapshot`, `catchup`, and `always` all behave the same), the
/// connector ingests the snapshot in one or several transactions, depending on `timestamp_column`.
/// If `timestamp_column` is not set, the whole snapshot is ingested in a single Feldera
/// transaction. If `timestamp_column` is set, the connector ingests the snapshot in a series of
/// timestamp ranges of width equal to the `LATENESS` attribute of the column, each range in a
/// separate transaction.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub enum IcebergTransactionMode {
    #[default]
    #[serde(rename = "none")]
    None,
    #[serde(rename = "snapshot")]
    Snapshot,
    #[serde(rename = "catchup")]
    Catchup,
    #[serde(rename = "always")]
    Always,
}

/// AWS Glue catalog config.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct GlueCatalogConfig {
    /// Location for table metadata.
    ///
    /// Example: `"s3://my-data-warehouse/tables/"`
    #[serde(rename = "glue.warehouse")]
    pub warehouse: Option<String>,

    /// Configure an alternative endpoint of the Glue service for Glue catalog to access.
    ///
    /// Example: `"https://glue.us-east-1.amazonaws.com"`
    #[serde(rename = "glue.endpoint")]
    pub endpoint: Option<String>,

    /// Access key id used to access the Glue catalog.
    #[serde(rename = "glue.access-key-id")]
    pub access_key_id: Option<String>,

    /// Secret access key used to access the Glue catalog.
    #[serde(rename = "glue.secret-access-key")]
    pub secret_access_key: Option<String>,

    /// Profile used to access the Glue catalog.
    #[serde(rename = "glue.profile-name")]
    pub profile_name: Option<String>,

    /// Region of the Glue catalog.
    #[serde(rename = "glue.region")]
    pub region: Option<String>,

    // Static session token used to access the Glue catalog.
    #[serde(rename = "glue.session-token")]
    pub session_token: Option<String>,

    /// The 12-digit ID of the Glue catalog.
    #[serde(rename = "glue.id")]
    pub id: Option<String>,
}

/// Iceberg REST catalog config.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RestCatalogConfig {
    /// URI identifying the REST catalog server.
    #[serde(rename = "rest.uri")]
    pub uri: Option<String>,

    /// The default location for managed tables created by the catalog.
    #[serde(rename = "rest.warehouse")]
    pub warehouse: Option<String>,

    /// Authentication URL to use for client credentials authentication (default: uri + 'v1/oauth/tokens')
    #[serde(rename = "rest.oauth2-server-uri")]
    pub oauth2_server_uri: Option<String>,

    /// Credential to use for OAuth2 credential flow when initializing the catalog.
    ///
    /// A key and secret pair separated by ":" (key is optional).
    #[serde(rename = "rest.credential")]
    pub credential: Option<String>,

    /// Bearer token value to use for `Authorization` header.
    #[serde(rename = "rest.token")]
    pub token: Option<String>,

    // Desired scope of the requested security token (default: catalog).
    #[serde(rename = "rest.scope")]
    pub scope: Option<String>,

    /// Customize table storage paths.
    ///
    /// When combined with the `warehouse` property, the prefix determines
    /// how table data is organized within the storage.
    #[serde(rename = "rest.prefix")]
    pub prefix: Option<String>,

    /// Additional HTTP request headers added to each catalog REST API call.
    #[serde(default)]
    #[serde(rename = "rest.headers")]
    pub headers: Option<Vec<(String, String)>>,

    /// Logical name of target resource or service.
    #[serde(rename = "rest.audience")]
    pub audience: Option<String>,

    /// URI for the target resource or service.
    #[serde(rename = "rest.resource")]
    pub resource: Option<String>,
}

/// Amazon S3 Tables catalog config.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct S3TablesCatalogConfig {
    /// ARN of the S3 table bucket that contains the table.
    ///
    /// Note that this is the ARN of the table *bucket*, not of an individual table,
    /// e.g., `"arn:aws:s3tables:us-east-2:123456789012:bucket/my-bucket"`.
    #[serde(rename = "s3tables.table-bucket-arn")]
    pub table_bucket_arn: Option<String>,

    /// Custom endpoint URL for the S3 Tables service.
    ///
    /// Primarily used to target a local or mock S3 Tables implementation for testing.
    /// When omitted, the default regional endpoint is used.
    #[serde(rename = "s3tables.endpoint")]
    pub endpoint: Option<String>,

    /// Access key id used to access the S3 Tables catalog.
    #[serde(rename = "s3tables.access-key-id")]
    pub access_key_id: Option<String>,

    /// Secret access key used to access the S3 Tables catalog.
    #[serde(rename = "s3tables.secret-access-key")]
    pub secret_access_key: Option<String>,

    /// Static session token used to access the S3 Tables catalog.
    #[serde(rename = "s3tables.session-token")]
    pub session_token: Option<String>,

    /// Profile used to access the S3 Tables catalog.
    #[serde(rename = "s3tables.profile-name")]
    pub profile_name: Option<String>,

    /// Region of the S3 Tables catalog.
    #[serde(rename = "s3tables.region")]
    pub region: Option<String>,
}

/// The number of CPU cores, at most 16. Each parsing task holds a record batch.
fn default_num_parsers() -> u32 {
    std::thread::available_parallelism().map_or(4, |n| n.get().min(16) as u32)
}

/// Iceberg input connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct IcebergReaderConfig {
    /// Table read mode.
    pub mode: IcebergIngestMode,

    /// Transaction mode.
    ///
    /// Determines how the connector breaks up its input into Feldera transactions.
    /// See [`IcebergTransactionMode`]. Defaults to [`IcebergTransactionMode::None`].
    #[serde(default)]
    pub transaction_mode: IcebergTransactionMode,

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
    /// This option is only valid when `mode` is set to `snapshot` or `snapshot_and_follow`.
    ///
    /// When specified, only rows that satisfy the filter condition are included in the
    /// snapshot.  The condition must be a valid SQL Boolean expression that can be used in
    /// the `where` clause of the `select * from snapshot where ...` query.
    ///
    /// This option can be used to specify the range of event times to include in the snapshot,
    /// e.g.: `ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'`.
    pub snapshot_filter: Option<String>,

    /// Optional snapshot id.
    ///
    /// When this option is set, the connector finds the specified snapshot of the table.
    /// In `snapshot` and `snapshot_and_follow` modes, it loads this snapshot.
    /// In `follow` and `snapshot_and_follow` modes, it follows table updates
    /// **after** this snapshot.
    ///
    /// Note: at most one of `snapshot_id` and `datetime` options can be specified.
    /// When neither of the two options is specified, the latest committed version of the table
    /// is used.
    pub snapshot_id: Option<i64>,

    /// Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format, e.g.,
    /// "2024-12-09T16:09:53+00:00".
    ///
    /// When this option is set, the connector finds and opens the snapshot of the table as of the
    /// specified point in time (based on the server time recorded in the transaction
    /// log, not the event time encoded in the data).  In `snapshot` and `snapshot_and_follow`
    /// modes, it retrieves this snapshot.  In `follow` and `snapshot_and_follow` modes, it
    /// follows transaction log records **after** this snapshot.
    ///
    /// Note: at most one of `snapshot_id` and `datetime` options can be specified.
    /// When neither of the two options is specified, the latest committed version of the table
    /// is used.
    pub datetime: Option<String>,

    /// Optional final snapshot id.
    ///
    /// Valid only in `follow` and `snapshot_and_follow` modes.
    ///
    /// When set, the connector stops after fully ingesting the snapshot with
    /// this id, signaling end-of-input. Iceberg snapshot ids are not ordered, so
    /// the bound is an exact match: the id must name a snapshot committed after
    /// the starting snapshot and already present in the table's current history.
    /// The connector rejects any other value at startup, including a
    /// not-yet-committed id, rather than follow forever.
    pub end_snapshot_id: Option<i64>,

    /// Location of the table metadata JSON file.
    ///
    /// This propery is used to access an Iceberg table without a catalog. It is mutually
    /// exclusive with the `catalog_type` property.
    pub metadata_location: Option<String>,

    /// Specifies the Iceberg table name in the "namespace.table" format.
    ///
    /// This option is applicable when an Iceberg catalog is configured using the `catalog_type` property.
    pub table_name: Option<String>,

    /// Specifies the catalog type used to access the Iceberg table.
    ///
    /// Supported options include "rest", "glue", and "s3tables". This property is mutually
    /// exclusive with `metadata_location`.
    pub catalog_type: Option<IcebergCatalogType>,

    /// The number of record batches the connector parses at the same time. Parsing runs on a
    /// pool with one thread per CPU core, which all connectors share, so a larger value only
    /// helps while the pool has idle threads. Each batch in flight holds memory.
    /// All connectors together keep at most two batches per CPU core in flight.
    /// The default is the number of CPU cores available to the pipeline, at most 16.
    ///
    /// When set, this is also the number of files the connector reads at the same time,
    /// which is 4 otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1)]
    pub num_parsers: Option<u32>,

    /// Maximum number of retries for reading the table snapshot.
    ///
    /// When reading the snapshot fails partway through, for example because an
    /// object-store read times out or is throttled, the connector retries the
    /// entire read with exponential backoff. This is in addition to the
    /// lower-level retries performed by the object-store client.
    ///
    /// Defaults to unlimited retries. Set to 0 to disable retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,

    #[serde(flatten)]
    pub glue_catalog_config: GlueCatalogConfig,

    #[serde(flatten)]
    pub rest_catalog_config: RestCatalogConfig,

    #[serde(flatten)]
    pub s3tables_catalog_config: S3TablesCatalogConfig,

    /// Storage options for configuring backend object store.
    ///
    /// See the [list of available options in PyIceberg documentation](https://py.iceberg.apache.org/configuration/#fileio).
    #[serde(flatten)]
    pub fileio_config: HashMap<String, String>,
}

impl IcebergReaderConfig {
    pub fn validate_catalog_config(&self) -> Result<(), String> {
        self.validate_metadata_location()?;
        self.validate_table_name()?;
        self.validate_glue_catalog_config()?;
        self.validate_rest_catalog_config()?;
        self.validate_s3tables_catalog_config()?;

        Ok(())
    }

    /// Reject Glue catalog config properties when 'catalog_type' isn't set to 'glue'.
    pub fn validate_glue_catalog_config(&self) -> Result<(), String> {
        validate_glue_catalog_config(self.catalog_type.as_ref(), &self.glue_catalog_config)
    }

    /// Reject Rest catalog config when 'catalog_type' isn't set to 'rest'.
    pub fn validate_rest_catalog_config(&self) -> Result<(), String> {
        validate_rest_catalog_config(self.catalog_type.as_ref(), &self.rest_catalog_config)
    }

    /// Reject S3 Tables catalog config when 'catalog_type' isn't set to 's3tables'.
    pub fn validate_s3tables_catalog_config(&self) -> Result<(), String> {
        validate_s3tables_catalog_config(self.catalog_type.as_ref(), &self.s3tables_catalog_config)
    }

    /// Table name must be configured iff 'catalog_type' is set.
    pub fn validate_table_name(&self) -> Result<(), String> {
        if self.catalog_type.is_none() && self.table_name.is_some() {
            Err("unexpected 'table_name' property: the 'table_name' property is valid only when an Iceberg catalog is configured using 'catalog_type'".to_string())
        } else if self.catalog_type.is_some() && self.table_name.is_none() {
            Err("missing 'table_name' property—'table_name' must be specified when Iceberg catalog is configured using 'catalog_type'".to_string())
        } else {
            Ok(())
        }
    }

    /// 'metadata_location' must be configured iff 'catalog_type' is set.
    pub fn validate_metadata_location(&self) -> Result<(), String> {
        if self.catalog_type.is_none() && self.metadata_location.is_none() {
            Err("missing metadata location: you must either specify an Iceberg catalog configuration by setting the 'catalog_type' property or provide a table metadata location directly via the 'metadata_location' property".to_string())
        } else if self.catalog_type.is_some() && self.metadata_location.is_some() {
            Err("unexpected 'metadata_location' property: the 'metadata_location' property is not supported when an Iceberg catalog is configured using 'catalog_type'".to_string())
        } else {
            Ok(())
        }
    }
}

/// Reject Glue catalog properties unless `catalog_type` is `glue`, which needs a warehouse.
fn validate_glue_catalog_config(
    catalog_type: Option<&IcebergCatalogType>,
    config: &GlueCatalogConfig,
) -> Result<(), String> {
    if catalog_type == Some(&IcebergCatalogType::Glue) {
        if config.warehouse.is_none() {
            return Err(r#"missing Iceberg warehouse location—set the 'glue.warehouse' property to the location of the Iceberg tables managed by the catalog (e.g., 's3://my-data-warehouse/tables/') when using "catalog_type" = "glue""#.to_string());
        }
    } else {
        ensure_glue_property_not_set(&config.warehouse, "warehouse")?;
        ensure_glue_property_not_set(&config.endpoint, "uri")?;
        ensure_glue_property_not_set(&config.access_key_id, "access-key-id")?;
        ensure_glue_property_not_set(&config.secret_access_key, "secret-access-key")?;
        ensure_glue_property_not_set(&config.profile_name, "profile-name")?;
        ensure_glue_property_not_set(&config.region, "region")?;
        ensure_glue_property_not_set(&config.session_token, "session-token")?;
        ensure_glue_property_not_set(&config.id, "id")?;
    }

    Ok(())
}

/// Reject Rest catalog properties unless `catalog_type` is `rest`, which needs a URI.
fn validate_rest_catalog_config(
    catalog_type: Option<&IcebergCatalogType>,
    config: &RestCatalogConfig,
) -> Result<(), String> {
    if catalog_type == Some(&IcebergCatalogType::Rest) {
        if config.uri.is_none() {
            return Err(r#"missing Iceberg Rest catalog URI—set the 'rest.uri' property when using "catalog_type" = "rest""#.to_string());
        }
    } else {
        ensure_rest_property_not_set(&config.uri, "uri")?;
        ensure_rest_property_not_set(&config.warehouse, "warehouse")?;
        ensure_rest_property_not_set(&config.oauth2_server_uri, "oauth2_server_uri")?;
        ensure_rest_property_not_set(&config.credential, "credential")?;
        ensure_rest_property_not_set(&config.token, "token")?;
        ensure_rest_property_not_set(&config.scope, "scope")?;
        ensure_rest_property_not_set(&config.prefix, "prefix")?;
        ensure_rest_property_not_set(&config.headers, "headers")?;
        ensure_rest_property_not_set(&config.audience, "audience")?;
        ensure_rest_property_not_set(&config.resource, "resource")?;
    }

    Ok(())
}

/// Reject S3 Tables catalog properties unless `catalog_type` is `s3tables`, which needs a bucket ARN.
fn validate_s3tables_catalog_config(
    catalog_type: Option<&IcebergCatalogType>,
    config: &S3TablesCatalogConfig,
) -> Result<(), String> {
    if catalog_type == Some(&IcebergCatalogType::S3Tables) {
        if config.table_bucket_arn.is_none() {
            return Err(r#"missing S3 table bucket ARN; set the 's3tables.table-bucket-arn' property to the ARN of the S3 table bucket (e.g., 'arn:aws:s3tables:us-east-2:123456789012:bucket/my-bucket') when using "catalog_type" = "s3tables""#.to_string());
        }
    } else {
        ensure_s3tables_property_not_set(&config.table_bucket_arn, "table-bucket-arn")?;
        ensure_s3tables_property_not_set(&config.endpoint, "endpoint")?;
        ensure_s3tables_property_not_set(&config.access_key_id, "access-key-id")?;
        ensure_s3tables_property_not_set(&config.secret_access_key, "secret-access-key")?;
        ensure_s3tables_property_not_set(&config.session_token, "session-token")?;
        ensure_s3tables_property_not_set(&config.profile_name, "profile-name")?;
        ensure_s3tables_property_not_set(&config.region, "region")?;
    }

    Ok(())
}

fn ensure_glue_property_not_set<T>(property: &Option<T>, name: &str) -> Result<(), String> {
    if property.is_some() {
        Err(format!(
            r#"unexpected 'glue.{name}' property—Glue catalog configuration properties are only valid when "catalog_type" = "glue""#
        ))
    } else {
        Ok(())
    }
}

fn ensure_rest_property_not_set<T>(property: &Option<T>, name: &str) -> Result<(), String> {
    if property.is_some() {
        Err(format!(
            r#"unexpected 'rest.{name}' property—Rest catalog configuration properties are only valid when "catalog_type" = "rest""#
        ))
    } else {
        Ok(())
    }
}

fn ensure_s3tables_property_not_set<T>(property: &Option<T>, name: &str) -> Result<(), String> {
    if property.is_some() {
        Err(format!(
            r#"unexpected 's3tables.{name}' property—S3 Tables catalog configuration properties are only valid when "catalog_type" = "s3tables""#
        ))
    } else {
        Ok(())
    }
}

impl IcebergReaderConfig {
    /// Maximum number of high-level operation retries. Defaults to unlimited.
    pub fn max_retries(&self) -> u32 {
        self.max_retries.unwrap_or(u32::MAX)
    }

    /// The number of record batches to parse at the same time.
    ///
    /// The default depends on the host, so the pipeline computes it, not the
    /// service that stores the configuration.
    pub fn num_parsers(&self) -> u32 {
        self.num_parsers.unwrap_or_else(default_num_parsers)
    }

    /// The number of data files, delete files, and manifests to read at the
    /// same time: `num_parsers` when set, otherwise 4. Each read holds decoded
    /// data, so this does not follow the CPU count.
    pub fn read_concurrency(&self) -> usize {
        self.num_parsers.unwrap_or(4) as usize
    }

    /// `true` if the configuration requires taking an initial snapshot of the table.
    pub fn snapshot(&self) -> bool {
        matches!(
            &self.mode,
            IcebergIngestMode::Snapshot | IcebergIngestMode::SnapshotAndFollow
        )
    }

    /// `true` if the configuration requires following the transaction log of the table
    /// (possibly after taking an initial snapshot).s
    pub fn follow(&self) -> bool {
        matches!(
            &self.mode,
            IcebergIngestMode::SnapshotAndFollow | IcebergIngestMode::Follow
        )
    }
}

/// Behavior of the Iceberg output connector when its table already exists.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub enum IcebergWriteMode {
    /// Append to the table. Create it if it does not exist.
    #[default]
    #[serde(rename = "append")]
    Append,

    /// Create the table, and fail if it already exists. A pipeline that resumes from a
    /// checkpoint reopens the table that it created.
    #[serde(rename = "error_if_exists")]
    ErrorIfExists,
}

/// Iceberg output connector configuration.
///
/// The connector appends view changes to an Iceberg table as a change log. Each row holds
/// the view columns, `__feldera_op` (`i` insert, `d` delete, `u` update), and the batch
/// time `__feldera_ts`. Each output batch is one Iceberg snapshot.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct IcebergWriterConfig {
    /// Table name in the "namespace.table" format.
    pub table_name: String,

    /// Catalog that holds the table: "rest", "glue", or "s3tables".
    pub catalog_type: IcebergCatalogType,

    /// What to do if the table already exists.
    #[serde(default)]
    pub mode: IcebergWriteMode,

    /// Maximum retries of a failed write or commit, with backoff of up to 10 seconds. Writing
    /// a batch again after the table changed also counts. The connector reports itself
    /// unhealthy while it retries.
    ///
    /// Defaults to unlimited, except when the table is opened at pipeline start, which is
    /// retried at most 10 times, for at most 5 minutes. Set to 0 to disable retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,

    /// Number of threads that encode and upload a batch in parallel. Defaults to 1.
    ///
    /// More than 1 requires an `index` on the connector, so that all changes of a key go to
    /// one thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1)]
    pub threads: Option<usize>,

    #[serde(flatten)]
    pub glue_catalog_config: GlueCatalogConfig,

    #[serde(flatten)]
    pub rest_catalog_config: RestCatalogConfig,

    #[serde(flatten)]
    pub s3tables_catalog_config: S3TablesCatalogConfig,

    /// Storage options for configuring backend object store.
    ///
    /// See the [list of available options in PyIceberg documentation](https://py.iceberg.apache.org/configuration/#fileio).
    #[serde(flatten)]
    pub fileio_config: HashMap<String, String>,
}

impl IcebergWriterConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.threads == Some(0) {
            return Err("'threads' must be at least 1".to_string());
        }
        if !self.table_name.contains('.') {
            return Err(format!(
                "invalid 'table_name' '{}': expected the \"namespace.table\" format",
                self.table_name
            ));
        }
        if self.table_name.split('.').any(str::is_empty) {
            return Err(format!(
                "invalid 'table_name' '{}': the namespace and the table must not be empty",
                self.table_name
            ));
        }
        let catalog_type = Some(&self.catalog_type);
        validate_glue_catalog_config(catalog_type, &self.glue_catalog_config)?;
        validate_rest_catalog_config(catalog_type, &self.rest_catalog_config)?;
        validate_s3tables_catalog_config(catalog_type, &self.s3tables_catalog_config)
    }
}

impl crate::sensitive::SensitiveValues for IcebergReaderConfig {
    fn visit_sensitive_values(&self, visit: &mut dyn FnMut(&str, &str)) {
        visit_catalog_sensitive_values(
            &self.glue_catalog_config,
            &self.rest_catalog_config,
            &self.s3tables_catalog_config,
            &self.fileio_config,
            visit,
        );
    }
}

impl crate::sensitive::SensitiveValues for IcebergWriterConfig {
    fn visit_sensitive_values(&self, visit: &mut dyn FnMut(&str, &str)) {
        visit_catalog_sensitive_values(
            &self.glue_catalog_config,
            &self.rest_catalog_config,
            &self.s3tables_catalog_config,
            &self.fileio_config,
            visit,
        );
    }
}

/// Visits the secrets in the catalog and FileIO settings of an Iceberg connector.
fn visit_catalog_sensitive_values(
    glue: &GlueCatalogConfig,
    rest: &RestCatalogConfig,
    s3tables: &S3TablesCatalogConfig,
    fileio: &HashMap<String, String>,
    visit: &mut dyn FnMut(&str, &str),
) {
    use crate::sensitive::{credential_header, visit_optional, visit_url};
    visit_optional(
        "glue.secret-access-key",
        glue.secret_access_key.as_deref(),
        visit,
    );
    visit_optional("glue.session-token", glue.session_token.as_deref(), visit);
    visit_optional(
        "s3tables.secret-access-key",
        s3tables.secret_access_key.as_deref(),
        visit,
    );
    visit_optional(
        "s3tables.session-token",
        s3tables.session_token.as_deref(),
        visit,
    );
    visit_optional("rest.credential", rest.credential.as_deref(), visit);
    visit_optional("rest.token", rest.token.as_deref(), visit);
    if let Some(uri) = &rest.uri {
        visit_url("rest.uri", uri, visit);
    }
    if let Some(headers) = &rest.headers {
        for (name, value) in headers {
            if credential_header(name) {
                visit(
                    &format!("rest.headers.{}", name.to_ascii_lowercase()),
                    value,
                );
            }
        }
    }
    // Iceberg FileIO uses dotted keys, independently of object_store's
    // keys. These are the keys of the PyIceberg FileIO list (see
    // `fileio_config`) that hold a secret key, a token or a connection
    // string. A key that is not here is not registered; connector errors
    // still leave its value out.
    for (key, value) in fileio {
        if matches!(
            key.as_str(),
            "s3.secret-access-key"
                | "s3.session-token"
                | "s3.sse.key"
                | "gcs.credentials-json"
                | "gcs.oauth2.token"
                | "oss.access-key-secret"
                | "adls.connection-string"
                | "adls.account-key"
                | "adls.sas-token"
                | "adls.client-secret"
        ) {
            visit(key, value);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    fn config(value: serde_json::Value) -> IcebergReaderConfig {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn s3tables_config_deserializes() {
        let config = config(json!({
            "mode": "snapshot",
            "catalog_type": "s3tables",
            "table_name": "namespace.table",
            "s3tables.table-bucket-arn": "arn:aws:s3tables:us-east-2:123456789012:bucket/my-bucket",
            "s3tables.region": "us-east-2",
            "s3tables.access-key-id": "key",
            "s3tables.secret-access-key": "secret",
            "s3tables.session-token": "token",
            "s3tables.profile-name": "profile",
            "s3tables.endpoint": "http://localhost:4566",
        }));

        // Each prefixed key must land on its struct field, not in the catch-all
        // `fileio_config` map (which is what happens on a rename mismatch).
        let s3tables = &config.s3tables_catalog_config;
        assert_eq!(
            s3tables.table_bucket_arn.as_deref(),
            Some("arn:aws:s3tables:us-east-2:123456789012:bucket/my-bucket")
        );
        assert_eq!(s3tables.region.as_deref(), Some("us-east-2"));
        assert_eq!(s3tables.access_key_id.as_deref(), Some("key"));
        assert_eq!(s3tables.secret_access_key.as_deref(), Some("secret"));
        assert_eq!(s3tables.session_token.as_deref(), Some("token"));
        assert_eq!(s3tables.profile_name.as_deref(), Some("profile"));
        assert_eq!(s3tables.endpoint.as_deref(), Some("http://localhost:4566"));
        assert!(config.fileio_config.is_empty());

        config.validate_catalog_config().unwrap();
    }

    #[test]
    fn s3tables_requires_table_bucket_arn() {
        let err = config(json!({
            "mode": "snapshot",
            "catalog_type": "s3tables",
            "table_name": "namespace.table",
        }))
        .validate_catalog_config()
        .unwrap_err();
        assert!(err.contains("s3tables.table-bucket-arn"), "{err}");
    }

    #[test]
    fn s3tables_props_rejected_for_other_catalog() {
        let err = config(json!({
            "mode": "snapshot",
            "catalog_type": "glue",
            "table_name": "namespace.table",
            "glue.warehouse": "s3://warehouse/",
            "s3tables.region": "us-east-2",
        }))
        .validate_catalog_config()
        .unwrap_err();
        assert!(err.contains("s3tables.region"), "{err}");
    }

    #[test]
    fn s3tables_props_rejected_without_catalog() {
        let err = config(json!({
            "mode": "snapshot",
            "metadata_location": "s3://warehouse/metadata.json",
            "s3tables.table-bucket-arn": "arn:aws:s3tables:us-east-2:123456789012:bucket/my-bucket",
        }))
        .validate_catalog_config()
        .unwrap_err();
        assert!(err.contains("s3tables.table-bucket-arn"), "{err}");
    }

    #[test]
    fn num_parsers_and_max_retries_defaults() {
        // With neither field set, num_parsers takes its default and retries are unlimited.
        let config: IcebergReaderConfig = serde_json::from_str(
            r#"{"mode":"snapshot","metadata_location":"file:///tmp/t/metadata.json"}"#,
        )
        .unwrap();
        assert_eq!(config.num_parsers(), default_num_parsers());
        assert_eq!(config.read_concurrency(), 4);
        // An unset `num_parsers` stays unset: the pipeline computes the default.
        assert!(
            !serde_json::to_string(&config)
                .unwrap()
                .contains("num_parsers")
        );
        assert_eq!(config.max_retries, None);
        assert_eq!(config.max_retries(), u32::MAX);
    }

    #[test]
    fn num_parsers_and_max_retries_explicit() {
        let config: IcebergReaderConfig = serde_json::from_str(
            r#"{"mode":"snapshot","metadata_location":"file:///tmp/t/metadata.json","num_parsers":8,"max_retries":0}"#,
        )
        .unwrap();
        assert_eq!(config.num_parsers(), 8);
        assert_eq!(config.read_concurrency(), 8);
        assert_eq!(config.max_retries, Some(0));
        // 0 disables retries: the first attempt is the last.
        assert_eq!(config.max_retries(), 0);
    }

    #[test]
    fn transaction_mode_defaults_to_none() {
        let config: IcebergReaderConfig = serde_json::from_str(
            r#"{"mode":"snapshot","metadata_location":"file:///tmp/t/metadata.json"}"#,
        )
        .unwrap();
        assert_eq!(config.transaction_mode, IcebergTransactionMode::None);
    }

    #[test]
    fn transaction_mode_snapshot_parses() {
        let config: IcebergReaderConfig = serde_json::from_str(
            r#"{"mode":"snapshot","metadata_location":"file:///tmp/t/metadata.json","transaction_mode":"snapshot"}"#,
        )
        .unwrap();
        assert_eq!(config.transaction_mode, IcebergTransactionMode::Snapshot);
    }

    #[test]
    fn reader_config_roundtrips() {
        let config: IcebergReaderConfig = serde_json::from_str(
            r#"{"mode":"snapshot","metadata_location":"file:///tmp/t/metadata.json","num_parsers":2}"#,
        )
        .unwrap();
        let serialized = serde_json::to_string(&config).unwrap();
        let reparsed: IcebergReaderConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, reparsed);
        assert_eq!(reparsed.num_parsers(), 2);
    }

    fn writer_config(value: serde_json::Value) -> IcebergWriterConfig {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn writer_config_defaults_and_roundtrips() {
        let config = writer_config(json!({
            "catalog_type": "rest",
            "table_name": "ns.t",
            "rest.uri": "http://localhost:8181",
            "s3.region": "us-east-1",
        }));
        config.validate().unwrap();
        assert_eq!(config.mode, IcebergWriteMode::Append);
        assert_eq!(config.max_retries, None);
        assert_eq!(config.threads, None);
        // A key no catalog struct owns is a FileIO option.
        assert_eq!(config.fileio_config.len(), 1);
        let reparsed: IcebergWriterConfig =
            serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
        assert_eq!(config, reparsed);
    }

    #[test]
    fn writer_config_rejects_what_it_cannot_use() {
        let invalid = |value: serde_json::Value| writer_config(value).validate().unwrap_err();

        let err = invalid(json!({"catalog_type": "rest", "table_name": "ns.t"}));
        assert!(err.contains("rest.uri"), "{err}");

        let err = invalid(json!({
            "catalog_type": "rest", "table_name": "t", "rest.uri": "http://localhost:8181",
        }));
        assert!(err.contains("namespace.table"), "{err}");

        // An empty level of a nested namespace.
        let err = invalid(json!({
            "catalog_type": "rest", "table_name": "db..t", "rest.uri": "http://localhost:8181",
        }));
        assert!(err.contains("must not be empty"), "{err}");

        let err = invalid(json!({
            "catalog_type": "rest", "table_name": "ns.t", "rest.uri": "http://localhost:8181",
            "glue.region": "us-east-1",
        }));
        assert!(err.contains("glue.region"), "{err}");

        let err = invalid(json!({
            "catalog_type": "rest", "table_name": "ns.t", "rest.uri": "http://localhost:8181",
            "threads": 0,
        }));
        assert!(err.contains("threads"), "{err}");

        // No truncate mode: iceberg-rust has no overwrite or delete action.
        serde_json::from_value::<IcebergWriterConfig>(json!({
            "catalog_type": "rest", "table_name": "ns.t", "rest.uri": "http://localhost:8181",
            "mode": "truncate",
        }))
        .unwrap_err();
    }

    #[test]
    fn writer_config_names_its_secrets() {
        use crate::sensitive::SensitiveValues;
        let config = writer_config(json!({
            "catalog_type": "rest",
            "table_name": "ns.t",
            "rest.uri": "http://localhost:8181",
            "rest.token": "catalog-token",
            "s3.secret-access-key": "store-secret",
        }));
        let mut seen = Vec::new();
        config.visit_sensitive_values(&mut |key, _| seen.push(key.to_string()));
        seen.sort();
        assert_eq!(seen, ["rest.token", "s3.secret-access-key"]);
    }
}
