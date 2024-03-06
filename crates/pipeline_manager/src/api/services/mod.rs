use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

mod kafka;

pub use kafka::KafkaService;

pub trait ServiceConfigType {
    /// Unique service configuration type used for classification.
    fn config_type() -> String;
}

/// Service configuration for the API
///
/// A Service is an API object, with as one of its properties its config.
/// The config is a variant of this enumeration, and is stored serialized
/// in the database.
///
/// How a service configuration is applied can vary by connector, e.g., some
/// might have options that are mutually exclusive whereas others might be
/// defaults that can be overriden.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
// snake_case such that the enumeration variants are not capitalized in (de-)serialization
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum ServiceConfig {
    Kafka(KafkaService),
}

impl ServiceConfig {
    /// Unique service configuration type used for classification.
    pub fn config_type(&self) -> String {
        match self {
            ServiceConfig::Kafka(_) => KafkaService::config_type(),
        }
    }

    /// Deserialize from provided YAML.
    pub fn from_yaml_str(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    /// Serialize to YAML for storage.
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(&self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::KafkaService;
    use super::ServiceConfig;
    use std::collections::BTreeMap;

    #[test]
    fn test_config_type_and_de_serialization() {
        let service_config = ServiceConfig::Kafka(KafkaService {
            bootstrap_servers: vec!["example:1234".to_string()],
            options: BTreeMap::from([("key".to_string(), "value".to_string())]),
        });
        assert_eq!(service_config.config_type(), "kafka");
        assert_eq!(
            service_config,
            ServiceConfig::from_yaml_str(&service_config.to_yaml())
        );
    }
}
