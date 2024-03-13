use crate::api::services::probe::{
    ServiceProbe, ServiceProbeError, ServiceProbeRequest, ServiceProbeResult,
};
use crate::api::ServiceProbeResponse;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::Metadata;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use utoipa::ToSchema;

/// Configuration for accessing a Kafka service.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
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

impl KafkaService {
    /// Create Kafka client configuration using the service configuration.
    fn client_config(&self) -> Result<ClientConfig, ServiceProbeError> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", self.bootstrap_servers.join(","));
        for (k, v) in &self.options {
            if k == "bootstrap.servers" {
                return Err(ServiceProbeError::Other(
                    "bootstrap.servers cannot be set in options as it is a separate field"
                        .to_string(),
                ));
            }
            client_config.set(k.clone(), v.clone());
        }
        Ok(client_config)
    }

    /// Fetch the metadata using the Kafka client.
    fn fetch_metadata(&self, timeout: Duration) -> Result<Metadata, ServiceProbeError> {
        let client_config = self.client_config()?;
        match client_config.create::<BaseConsumer>() {
            Ok(consumer) => match consumer.fetch_metadata(None, timeout) {
                Ok(metadata) => Ok(metadata),
                Err(e) => Err(ServiceProbeError::Other(e.to_string())),
            },
            Err(e) => Err(ServiceProbeError::Other(e.to_string())),
        }
    }
}

impl ServiceProbe for KafkaService {
    // TODO: use a context to fetch more detailed log messages
    //       which are asynchronously reported
    fn probe(&self, probe: ServiceProbeRequest, timeout: Duration) -> ServiceProbeResponse {
        // Only general and Kafka probe requests are valid.
        // When new services are added, a match will be done
        // with UnsupportedRequest error being returned for others.
        match probe {
            ServiceProbeRequest::TestConnectivity => match self.fetch_metadata(timeout) {
                Ok(_metadata) => ServiceProbeResponse::Success(ServiceProbeResult::Connected),
                Err(e) => ServiceProbeResponse::Error(e),
            },
            ServiceProbeRequest::KafkaGetTopics => match self.fetch_metadata(timeout) {
                Ok(metadata) => ServiceProbeResponse::Success(ServiceProbeResult::KafkaTopics(
                    metadata
                        .topics()
                        .iter()
                        .map(|topic| topic.name().to_string())
                        .collect::<Vec<String>>(),
                )),
                Err(e) => ServiceProbeResponse::Error(e),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KafkaService;
    use crate::api::ServiceProbeError;
    use std::collections::BTreeMap;

    #[test]
    fn test_client_config() {
        // Successful
        let kafka_service = KafkaService {
            bootstrap_servers: vec!["a".to_string(), "b".to_string()],
            options: BTreeMap::from([("key".to_string(), "value".to_string())]),
        };
        let client_config = kafka_service.client_config().unwrap();
        assert_eq!(client_config.get("bootstrap.servers").unwrap(), "a,b");
        assert_eq!(client_config.get("key").unwrap(), "value");

        // Failure: bootstrap.servers is specified in options
        let kafka_service = KafkaService {
            bootstrap_servers: vec!["a".to_string(), "b".to_string()],
            options: BTreeMap::from([("bootstrap.servers".to_string(), "value".to_string())]),
        };
        assert_eq!(
            kafka_service.client_config().unwrap_err(),
            ServiceProbeError::Other(
                "bootstrap.servers cannot be set in options as it is a separate field".to_string()
            )
        )
    }
}
