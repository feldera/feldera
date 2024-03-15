use crate::prober::service::{
    ServiceProbeError, ServiceProbeRequest, ServiceProbeResponse, ServiceProbeResult,
};
use pipeline_types::service::KafkaService;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::Metadata;
use rdkafka::ClientConfig;
use std::time::Duration;

/// Create Kafka client configuration using the service configuration.
fn create_client_config(kafka_service: &KafkaService) -> Result<ClientConfig, ServiceProbeError> {
    let mut client_config = ClientConfig::new();
    let final_options = kafka_service
        .generate_final_options()
        .map_err(|e| ServiceProbeError::Other(format!("{}", e)))?;
    for (k, v) in &final_options {
        client_config.set(k.clone(), v.clone());
    }
    Ok(client_config)
}

/// Fetch the metadata using the Kafka client created from the service configuration.
fn fetch_metadata(
    kafka_service: &KafkaService,
    timeout: Duration,
) -> Result<Metadata, ServiceProbeError> {
    let client_config = create_client_config(kafka_service)?;
    match client_config.create::<BaseConsumer>() {
        Ok(consumer) => match consumer.fetch_metadata(None, timeout) {
            Ok(metadata) => Ok(metadata),
            Err(e) => Err(ServiceProbeError::Other(e.to_string())),
        },
        Err(e) => Err(ServiceProbeError::Other(e.to_string())),
    }
}

/// Perform a probe for the Kafka service.
// TODO: use a context to fetch more detailed log messages
//       which are asynchronously reported
pub fn probe_kafka_service(
    kafka_service: &KafkaService,
    probe: ServiceProbeRequest,
    timeout: Duration,
) -> ServiceProbeResponse {
    // Only general and Kafka probe requests are valid.
    // When new services are added, a match will be done
    // with UnsupportedRequest error being returned for others.
    match probe {
        ServiceProbeRequest::TestConnectivity => match fetch_metadata(kafka_service, timeout) {
            Ok(_metadata) => ServiceProbeResponse::Success(ServiceProbeResult::Connected),
            Err(e) => ServiceProbeResponse::Error(e),
        },
        ServiceProbeRequest::KafkaGetTopics => match fetch_metadata(kafka_service, timeout) {
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

#[cfg(test)]
mod tests {
    use crate::prober::service::kafka::create_client_config;
    use crate::prober::service::ServiceProbeError;
    use pipeline_types::service::KafkaService;
    use std::collections::BTreeMap;

    #[test]
    fn test_create_client_config() {
        // Successful
        let kafka_service = KafkaService {
            bootstrap_servers: vec!["a".to_string(), "b".to_string()],
            options: BTreeMap::from([("key".to_string(), "value".to_string())]),
        };
        let client_config = create_client_config(&kafka_service).unwrap();
        assert_eq!(client_config.get("bootstrap.servers").unwrap(), "a,b");
        assert_eq!(client_config.get("key").unwrap(), "value");

        // Failure: bootstrap.servers is specified in options
        let kafka_service = KafkaService {
            bootstrap_servers: vec!["a".to_string(), "b".to_string()],
            options: BTreeMap::from([("bootstrap.servers".to_string(), "value".to_string())]),
        };
        assert_eq!(
            create_client_config(&kafka_service).unwrap_err(),
            ServiceProbeError::Other(
                "bootstrap.servers cannot be set in options as it is a separate field".to_string()
            )
        )
    }
}
