use crate::api::ServiceConfigType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// Configuration for accessing a Kafka service.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct KafkaService {
    /// List of bootstrap servers, each formatted as hostname:port (e.g.,
    /// "example.com:1234"). It will be used to set the bootstrap.servers
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

impl ServiceConfigType for KafkaService {
    fn config_type() -> String {
        "kafka".to_string()
    }
}
