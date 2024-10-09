use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for the parquet parser.
#[derive(Deserialize, Serialize, ToSchema)]
pub struct ParquetParserConfig {}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct ParquetEncoderConfig {
    /// Number of records before a new parquet file is written.
    ///
    /// The default is 100_000.
    pub buffer_size_records: usize,
}

impl Default for ParquetEncoderConfig {
    fn default() -> Self {
        Self {
            buffer_size_records: 100_000,
        }
    }
}
