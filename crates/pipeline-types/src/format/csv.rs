use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct CsvParserConfig {}

const fn default_buffer_size_records() -> usize {
    10_000
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct CsvEncoderConfig {
    #[serde(default = "default_buffer_size_records")]
    pub buffer_size_records: usize,
}
