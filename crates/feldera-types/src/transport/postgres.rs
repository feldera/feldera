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
