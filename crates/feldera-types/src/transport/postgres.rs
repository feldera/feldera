use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

/// PostgreSQL write mode.
///
/// Determines how the PostgreSQL output connector writes data to the target table.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub enum PostgresWriteMode {
    /// Materialized mode: perform direct INSERT, UPDATE, and DELETE operations on the table.
    /// This is the default behavior and maintains the postgres table as a materialized snapshot of the output view.
    #[default]
    #[serde(rename = "materialized")]
    Materialized,

    /// CDC (Change Data Capture) mode: write all operations as INSERT operations
    /// into a Postgres table that serves as an append-only event log.
    /// In this mode, inserts, updates, and deletes are all represented as new rows
    /// with metadata columns describing the operation type and timestamp.
    #[serde(rename = "cdc")]
    Cdc,
}

impl Display for PostgresWriteMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Materialized => write!(f, "materialized"),
            Self::Cdc => write!(f, "cdc"),
        }
    }
}

/// Postgres input connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresReaderConfig {
    /// Postgres URI.
    /// See: <https://docs.rs/tokio-postgres/0.7.12/tokio_postgres/config/struct.Config.html>
    pub uri: String,

    /// Query that specifies what data to fetch from postgres.
    pub query: String,
}

/// Postgres output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresWriterConfig {
    /// Postgres URI.
    /// See: <https://docs.rs/tokio-postgres/0.7.12/tokio_postgres/config/struct.Config.html>
    pub uri: String,

    /// The table to write the output to.
    pub table: String,

    /// Write mode for the connector.
    ///
    /// - `materialized` (default): Perform direct INSERT, UPDATE, and DELETE operations on the table.
    /// - `cdc`: Write all operations as INSERT operations into an append-only event log
    ///   with additional metadata columns describing the operation type and timestamp.
    #[serde(default)]
    #[schema(default = PostgresWriteMode::default)]
    pub mode: PostgresWriteMode,

    /// Name of the operation metadata column in CDC mode.
    ///
    /// Only used when `mode = "cdc"`. This column will contain:
    /// - `"i"` for insert operations
    /// - `"u"` for upsert operations
    /// - `"d"` for delete operations
    ///
    /// Default: `"__feldera_op"`
    #[serde(default = "default_cdc_op_column")]
    #[schema(default = default_cdc_op_column)]
    pub cdc_op_column: String,

    /// Name of the timestamp metadata column in CDC mode.
    ///
    /// Only used when `mode = "cdc"`. This column will contain the timestamp
    /// (in RFC 3339 format) when the batch of updates was output
    /// by the pipeline.
    ///
    /// Default: `"__feldera_ts"`
    #[serde(default = "default_cdc_ts_column")]
    #[schema(default = default_cdc_ts_column)]
    pub cdc_ts_column: String,

    /// A sequence of CA certificates in PEM format.
    pub ssl_ca_pem: Option<String>,

    /// Path to a file containing a sequence of CA certificates in PEM format.
    pub ssl_ca_location: Option<String>,

    /// The client certificate in PEM format.
    pub ssl_client_pem: Option<String>,

    /// Path to the client certificate.
    pub ssl_client_location: Option<String>,

    /// The client certificate key in PEM format.
    pub ssl_client_key: Option<String>,

    /// Path to the client certificate key.
    pub ssl_client_key_location: Option<String>,

    /// The path to the certificate chain file.
    /// The file must contain a sequence of PEM-formatted certificates,
    /// the first being the leaf certificate, and the remainder forming
    /// the chain of certificates up to and including the trusted root certificate.
    pub ssl_certificate_chain_location: Option<String>,

    /// True to enable hostname verification when using TLS. True by default.
    pub verify_hostname: Option<bool>,

    /// The maximum number of records in a single buffer.
    pub max_records_in_buffer: Option<usize>,

    /// The maximum buffer size in for a single operation.
    /// Note that the buffers of `INSERT`, `UPDATE` and `DELETE` queries are
    /// separate.
    /// Default: 1 MiB
    #[schema(default = default_max_buffer_size)]
    #[serde(default = "default_max_buffer_size")]
    pub max_buffer_size_bytes: usize,

    /// Specifies how the connector handles conflicts when executing an `INSERT`
    /// into a table with a primary key. By default, an existing row with the same
    /// key is overwritten. Setting this flag to `true` preserves the existing row
    /// and ignores the new insert.
    ///
    /// This setting does not affect `UPDATE` statements, which always replace the
    /// value associated with the key.
    ///
    /// This setting is not supported when `mode = "cdc"`, since all operations
    /// are performed as append-only `INSERT`s into the target table.
    /// Any conflict in CDC mode will result in an error.
    ///
    /// Default: `false`
    #[serde(default)]
    pub on_conflict_do_nothing: bool,
}

fn default_max_buffer_size() -> usize {
    usize::pow(2, 20)
}

fn default_cdc_op_column() -> String {
    "__feldera_op".to_string()
}

fn default_cdc_ts_column() -> String {
    "__feldera_ts".to_string()
}

impl PostgresWriterConfig {
    pub fn validate(&self) -> Result<(), String> {
        match self.mode {
            PostgresWriteMode::Cdc => {
                if self.cdc_op_column.trim().is_empty() {
                    return Err("cdc_op_column cannot be empty in CDC mode".to_string());
                }
                if self.cdc_ts_column.trim().is_empty() {
                    return Err("cdc_ts_column cannot be empty in CDC mode".to_string());
                }

                if !self.cdc_op_column.is_ascii() {
                    return Err("cdc_op_column must contain only ASCII characters".to_string());
                }

                if !self.cdc_ts_column.is_ascii() {
                    return Err("cdc_ts_column must contain only ASCII characters".to_string());
                }

                if self.on_conflict_do_nothing {
                    return Err("on_conflict_do_nothing not supported in CDC mode since all operations are performed as append-only INSERTs into the target table".to_string());
                }
            }
            PostgresWriteMode::Materialized => {
                if self.cdc_ts_column != default_cdc_ts_column()
                    && !self.cdc_ts_column.trim().is_empty()
                {
                    return Err(
                        "cdc_ts_column must not be set when in MATERIALIZED mode".to_string()
                    );
                }
                if self.cdc_op_column != default_cdc_op_column()
                    && !self.cdc_op_column.trim().is_empty()
                {
                    return Err(
                        "cdc_op_column must not be set when in MATERIALIZED mode".to_string()
                    );
                }
            }
        };

        Ok(())
    }
}
