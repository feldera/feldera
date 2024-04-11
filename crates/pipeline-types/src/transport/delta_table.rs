use crate::config::TransportConfigVariant;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Delta table output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DeltaTableWriterConfig {
    /// Table URI.
    pub uri: String,
    /// Storage options for configuring backend object store.
    ///
    /// For specific options available for different storage backends, see:
    /// * [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
    /// * [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
    /// * [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
    #[serde(flatten)]
    pub object_store_config: HashMap<String, String>,
    /// A bound on the size in bytes of a single Parquet file.
    pub max_parquet_file_size: Option<usize>,
}

impl TransportConfigVariant for DeltaTableWriterConfig {
    fn name(&self) -> String {
        "delta_table_output".to_string()
    }
}
