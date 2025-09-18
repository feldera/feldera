use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

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

    /// The CA certificate in PEM format.
    pub ssl_ca_pem: Option<String>,

    /// The client certificate in PEM format.
    pub ssl_client_pem: Option<String>,

    /// The client certificate key in PEM format.
    pub ssl_client_key: Option<String>,

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
    /// Default: `false`
    #[serde(default)]
    pub on_conflict_do_nothing: bool,
}

fn default_max_buffer_size() -> usize {
    usize::pow(2, 20)
}
