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

/// Iceberg input connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct IcebergReaderConfig {
    /// Table read mode.
    pub mode: IcebergIngestMode,

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
    /// Supported options include "rest" and "glue". This property is mutually exclusive with `metadata_location`.
    pub catalog_type: Option<IcebergCatalogType>,

    #[serde(flatten)]
    pub glue_catalog_config: GlueCatalogConfig,

    #[serde(flatten)]
    pub rest_catalog_config: RestCatalogConfig,

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

        Ok(())
    }

    /// Reject Glue catalog config properties when 'catalog_type' isn't set to 'glue'.
    pub fn validate_glue_catalog_config(&self) -> Result<(), String> {
        if self.catalog_type == Some(IcebergCatalogType::Glue) {
            if self.glue_catalog_config.warehouse.is_none() {
                return Err(r#"missing Iceberg warehouse location—set the 'glue.warehouse' property to the location of the Iceberg tables managed by the catalog (e.g., 's3://my-data-warehouse/tables/') when using "catalog_type" = "glue""#.to_string());
            }
        } else {
            ensure_glue_property_not_set(&self.glue_catalog_config.warehouse, "warehouse")?;
            ensure_glue_property_not_set(&self.glue_catalog_config.endpoint, "uri")?;
            ensure_glue_property_not_set(&self.glue_catalog_config.access_key_id, "access-key-id")?;
            ensure_glue_property_not_set(
                &self.glue_catalog_config.secret_access_key,
                "secret-access-key",
            )?;
            ensure_glue_property_not_set(&self.glue_catalog_config.profile_name, "profile-name")?;
            ensure_glue_property_not_set(&self.glue_catalog_config.region, "region")?;
            ensure_glue_property_not_set(&self.glue_catalog_config.session_token, "session-token")?;
            ensure_glue_property_not_set(&self.glue_catalog_config.id, "id")?;
        }

        Ok(())
    }

    /// Reject Rest catalog config when 'catalog_type' isn't set to 'rest'.
    pub fn validate_rest_catalog_config(&self) -> Result<(), String> {
        if self.catalog_type == Some(IcebergCatalogType::Rest) {
            if self.rest_catalog_config.uri.is_none() {
                return Err(r#"missing Iceberg Rest catalog URI—set the 'rest.uri' property when using "catalog_type" = "rest""#.to_string());
            }
        } else {
            ensure_rest_property_not_set(&self.rest_catalog_config.uri, "uri")?;
            ensure_rest_property_not_set(&self.rest_catalog_config.warehouse, "warehouse")?;
            ensure_rest_property_not_set(
                &self.rest_catalog_config.oauth2_server_uri,
                "oauth2_server_uri",
            )?;
            ensure_rest_property_not_set(&self.rest_catalog_config.credential, "credential")?;
            ensure_rest_property_not_set(&self.rest_catalog_config.token, "token")?;
            ensure_rest_property_not_set(&self.rest_catalog_config.scope, "scope")?;
            ensure_rest_property_not_set(&self.rest_catalog_config.prefix, "prefix")?;
            ensure_rest_property_not_set(&self.rest_catalog_config.headers, "headers")?;
            ensure_rest_property_not_set(&self.rest_catalog_config.audience, "audience")?;
            ensure_rest_property_not_set(&self.rest_catalog_config.resource, "resource")?;
        }

        Ok(())
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

impl IcebergReaderConfig {
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
