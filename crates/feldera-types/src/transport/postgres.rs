use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Postgres input connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresReaderConfig {
    /// Postgres URI.
    pub uri: String,

    /// Query that specifies what data to fetch from postgres.
    pub query: String,
}

/// Postgres output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresWriterConfig {
    /// Postgres URI.
    pub uri: String,

    /// The table to write the output to.
    pub table: String,
}
