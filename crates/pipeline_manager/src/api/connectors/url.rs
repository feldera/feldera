use crate::api::connectors::format::FormatConfig;
use crate::api::connectors::ConnectorConfigVariant;
use crate::db::{DBError, ServiceDescr};
use pipeline_types::config::{
    default_max_buffered_records, PipelineConnectorConfig, TransportConfig,
};
use pipeline_types::transport::url::UrlInputConfig;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// Configuration for a URL input connector which fetches data from the
/// provided URL using HTTP GET.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UrlInput {
    /// URL which to HTTP GET (e.g., http://example.com/file.csv)
    pub url: String,

    /// Format of the data located at the URL (e.g., CSV, JSON)
    pub format: FormatConfig,

    /// (Optional) Backpressure threshold (default: 1,000,000)
    #[serde(default = "default_max_buffered_records")]
    pub max_buffered_records: u64,
}

impl ConnectorConfigVariant for UrlInput {
    fn service_names(&self) -> Vec<String> {
        vec![]
    }

    fn new_replace_service_names(self, replacement_service_names: &[String]) -> Self {
        assert_eq!(
            replacement_service_names.len(),
            0,
            "URL input connector should have no replacement service names but got: {:?}",
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
            "URL input connector should have no services but got: {:?}",
            services
        );
        Ok(PipelineConnectorConfig {
            transport: TransportConfig {
                name: Cow::from("url"),
                config: serde_yaml::to_value(UrlInputConfig {
                    path: self.url.clone(),
                })
                .unwrap(),
            },
            format: self.format.to_pipeline_format_config(),
            max_buffered_records: self.max_buffered_records,
        })
    }
}
