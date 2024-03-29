use crate::service::KafkaService;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Required trait for every service configuration.
pub trait ServiceConfigVariant {
    /// Unique service configuration type used for classification.
    fn config_type() -> String;
}

/// Configuration for a Service, which typically includes how to establish a
/// connection (e.g., hostname, port) and authenticate (e.g., credentials).
///
/// This configuration can be used to easily derive connectors for the service
/// as well as probe it for information.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
// snake_case such that the enumeration variants are not capitalized in (de-)serialization
#[serde(rename_all = "snake_case")]
pub enum ServiceConfig {
    Kafka(KafkaService),
}

impl ServiceConfig {
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
    use super::ServiceConfig;
    use crate::service::KafkaService;
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
