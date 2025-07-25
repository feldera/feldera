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
}
