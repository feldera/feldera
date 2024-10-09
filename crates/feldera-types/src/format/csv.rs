use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct CsvParserConfig {}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct CsvEncoderConfig {
    pub buffer_size_records: usize,
}

impl Default for CsvEncoderConfig {
    fn default() -> Self {
        Self {
            buffer_size_records: 10_000,
        }
    }
}
