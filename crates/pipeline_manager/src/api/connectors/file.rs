use crate::api::connectors::format::FormatConfig;
use crate::api::connectors::ConnectorConfigVariant;
use crate::db::{DBError, ServiceDescr};
use pipeline_types::config::{
    default_max_buffered_records, PipelineConnectorConfig, TransportConfig,
};
use pipeline_types::transport::file::{FileInputConfig, FileOutputConfig};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// Configuration for a file input connector.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FileInput {
    /// Path of the file to read from (e.g., /path/to/file.csv)
    pub path: String,

    /// Format of the data in the file (e.g., CSV, JSON)
    pub format: FormatConfig,

    /// (Optional) Backpressure threshold (default: 1,000,000)
    #[serde(default = "default_max_buffered_records")]
    pub max_buffered_records: u64,
}

impl ConnectorConfigVariant for FileInput {
    fn service_names(&self) -> Vec<String> {
        vec![]
    }

    fn new_replace_service_names(self, replacement_service_names: &[String]) -> Self {
        assert_eq!(
            replacement_service_names.len(),
            0,
            "File input connector should have no replacement service names but got: {:?}",
            replacement_service_names
        );
        self
    }

    fn to_pipeline_connector_config(
        &self,
        services: &BTreeMap<String, ServiceDescr>,
    ) -> Result<PipelineConnectorConfig, DBError> {
        assert_eq!(
            services.len(),
            0,
            "File input connector should have no services but got: {:?}",
            services
        );
        Ok(PipelineConnectorConfig {
            transport: TransportConfig {
                name: Cow::from("file"),
                config: serde_yaml::to_value(FileInputConfig {
                    path: self.path.clone(),
                    buffer_size_bytes: None,
                    follow: false,
                })
                .unwrap(),
            },
            format: self.format.to_pipeline_format_config(),
            max_buffered_records: self.max_buffered_records,
        })
    }
}

/// Configuration for a file output connector.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FileOutput {
    /// Path of the file to read from (e.g., /path/to/file.csv)
    pub path: String,

    /// Format of the data in the file (e.g., CSV, JSON)
    pub format: FormatConfig,

    /// (Optional) Backpressure threshold (default: 1,000,000)
    #[serde(default = "default_max_buffered_records")]
    pub max_buffered_records: u64,
}

impl ConnectorConfigVariant for FileOutput {
    fn service_names(&self) -> Vec<String> {
        vec![]
    }

    fn new_replace_service_names(self, replacement_service_names: &[String]) -> Self {
        assert_eq!(
            replacement_service_names.len(),
            0,
            "File output connector should have no replacement service names but got: {:?}",
            replacement_service_names
        );
        self
    }

    fn to_pipeline_connector_config(
        &self,
        services: &BTreeMap<String, ServiceDescr>,
    ) -> Result<PipelineConnectorConfig, DBError> {
        assert_eq!(
            services.len(),
            0,
            "File output connector should have no services but got: {:?}",
            services
        );
        Ok(PipelineConnectorConfig {
            transport: TransportConfig {
                name: Cow::from("file"),
                config: serde_yaml::to_value(FileOutputConfig {
                    path: self.path.clone(),
                })
                .unwrap(),
            },
            format: self.format.to_pipeline_format_config(),
            max_buffered_records: self.max_buffered_records,
        })
    }
}
