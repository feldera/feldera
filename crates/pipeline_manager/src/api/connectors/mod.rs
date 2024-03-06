mod file;
mod format;
mod kafka;
mod url;

use crate::db::{DBError, ServiceDescr};
use pipeline_types::config::PipelineConnectorConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

pub use file::{FileInput, FileOutput};
pub use format::FormatConfig;
pub use kafka::{KafkaInput, KafkaOutput};
pub use url::UrlInput;

/// Connector configuration.
///
/// A connector configuration specifies the parameters for a particular
/// connector. For example, a Kafka input connector specifies the Kafka
/// service and the topics to read from. Connectors use services as their
/// basis to reduce the number of parameters as well as to allow
/// a single common service configuration to be updatable rather than spread
/// across many connectors.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
// snake_case such that the enumeration variants are not capitalized in (de-)serialization
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum ConnectorConfig {
    FileInput(FileInput),
    FileOutput(FileOutput),
    UrlInput(UrlInput),
    KafkaInput(KafkaInput),
    KafkaOutput(KafkaOutput),
}

/// Required trait for every stored connector configuration, as it needs to be
/// convertible to its storable variant with the service names resolved to
/// identifiers.
pub(crate) trait ConnectorConfigVariant {
    /// Retrieves all the service names in the connector.
    /// This is used just before storing in the database, such that the names
    /// are resolved to identifiers while stored. Upon deserialization from
    /// the database, the exact same order of names is expected in
    /// [new_replace_service_names](Self::new_replace_service_names).
    fn service_names(&self) -> Vec<String>;

    /// Replaces the service names in the direct deserialized version with the
    /// service names fetched using the identifier foreign key relations.
    fn new_replace_service_names(self, replacement_service_names: &[String]) -> Self;

    /// Converts the connector configuration to its pipeline connector
    /// counterpart by resolving the services that it refers to.
    fn to_pipeline_connector_config(
        &self,
        services: &BTreeMap<String, ServiceDescr>,
    ) -> Result<PipelineConnectorConfig, DBError>;
}

// Implement trait for the enumeration for convenience.
impl ConnectorConfigVariant for ConnectorConfig {
    fn service_names(&self) -> Vec<String> {
        match self {
            ConnectorConfig::FileInput(config) => config.service_names(),
            ConnectorConfig::FileOutput(config) => config.service_names(),
            ConnectorConfig::UrlInput(config) => config.service_names(),
            ConnectorConfig::KafkaInput(config) => config.service_names(),
            ConnectorConfig::KafkaOutput(config) => config.service_names(),
        }
    }

    fn new_replace_service_names(self, replacement_service_names: &[String]) -> Self {
        match self {
            ConnectorConfig::FileInput(config) => ConnectorConfig::FileInput(
                config.new_replace_service_names(replacement_service_names),
            ),
            ConnectorConfig::FileOutput(config) => ConnectorConfig::FileOutput(
                config.new_replace_service_names(replacement_service_names),
            ),
            ConnectorConfig::UrlInput(config) => ConnectorConfig::UrlInput(
                config.new_replace_service_names(replacement_service_names),
            ),
            ConnectorConfig::KafkaInput(config) => ConnectorConfig::KafkaInput(
                config.new_replace_service_names(replacement_service_names),
            ),
            ConnectorConfig::KafkaOutput(config) => ConnectorConfig::KafkaOutput(
                config.new_replace_service_names(replacement_service_names),
            ),
        }
    }

    fn to_pipeline_connector_config(
        &self,
        services: &BTreeMap<String, ServiceDescr>,
    ) -> Result<PipelineConnectorConfig, DBError> {
        match self {
            ConnectorConfig::FileOutput(config) => config.to_pipeline_connector_config(services),
            ConnectorConfig::FileInput(config) => config.to_pipeline_connector_config(services),
            ConnectorConfig::UrlInput(config) => config.to_pipeline_connector_config(services),
            ConnectorConfig::KafkaInput(config) => config.to_pipeline_connector_config(services),
            ConnectorConfig::KafkaOutput(config) => config.to_pipeline_connector_config(services),
        }
    }
}

impl ConnectorConfig {
    /// Deserialize from provided YAML.
    pub fn from_yaml_str(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    /// Serialize to YAML.
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(&self).unwrap()
    }
}
