use crate::service::config::ServiceConfigVariant;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error as ThisError;
use utoipa::ToSchema;

/// Configuration for accessing a Kafka service.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
pub struct KafkaService {
    /// List of bootstrap servers, each formatted as hostname:port (e.g.,
    /// "example.com:1234"). It will be used to set the `bootstrap.servers`
    /// Kafka option.
    pub bootstrap_servers: Vec<String>,

    /// Additional Kafka options.
    ///
    /// Should not contain the bootstrap.servers key
    /// as it is passed explicitly via its field.
    ///
    /// These options will likely encompass things
    /// like SSL and authentication configuration.
    pub options: BTreeMap<String, String>,
}

#[derive(ThisError, Debug)]
pub enum KafkaServiceError {
    #[error("bootstrap.servers cannot be set in options as it is a separate field")]
    DuplicateBootstrapServers,
}

impl ServiceConfigVariant for KafkaService {
    fn config_type() -> String {
        "kafka".to_string()
    }
}

impl KafkaService {
    pub fn generate_final_options(&self) -> Result<BTreeMap<String, String>, KafkaServiceError> {
        if self.options.contains_key("bootstrap.servers") {
            return Err(KafkaServiceError::DuplicateBootstrapServers);
        }
        let mut result = self.options.clone();
        result.insert(
            "bootstrap.servers".to_string(),
            self.bootstrap_servers.join(","),
        );
        Ok(result)
    }
}
