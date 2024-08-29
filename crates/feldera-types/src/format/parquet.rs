use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const fn default_buffer_size_records() -> usize {
    100_000
}

/// Configuration for the parquet parser.
#[derive(Deserialize, Serialize, ToSchema)]
pub struct ParquetParserConfig {}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct ParquetEncoderConfig {
    /// Number of records before a new parquet file is written.
    ///
    /// The default is 100_000.
    #[serde(default = "default_buffer_size_records")]
    pub buffer_size_records: usize,
}
