use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub enum RawParserMode {
    #[default]
    #[serde(rename = "blob")]
    Blob,

    #[serde(rename = "lines")]
    Lines,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct RawParserConfig {
    /// Ingestion mode.
    ///
    /// * `blob` (default) - ingest the entire data chunk received from the transport connector as a single SQL row.
    ///   For message-oriented transports, such as Kafka or Pub/Sub, an input chunk corresponds to a message.
    ///   For file-based transports, e.g., the URL connector or the S3 connector, a chunk represents an entire file or object.
    /// * `lines` - split the input byte stream on the new line character (`\n`) and ingest each line as a separate SQL row.
    pub mode: RawParserMode,

    /// Table column that will store the raw value.
    ///
    /// This setting is required if the table has more than 1 column.
    pub column_name: Option<String>,
}
