use crate::api::connectors::ConnectorConfigVariant;
use crate::api::{FormatConfig, KafkaService, ServiceConfig, ServiceConfigType};
use crate::db::{DBError, ServiceDescr};
use pipeline_types::config::{
    default_max_buffered_records, PipelineConnectorConfig, TransportConfig,
};
use pipeline_types::transport::kafka::{
    default_group_join_timeout_secs, default_initialization_timeout_secs,
    default_max_inflight_messages, KafkaInputConfig, KafkaInputFtConfig, KafkaLogLevel,
    KafkaOutputConfig, KafkaOutputFtConfig,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// Generates the Kafka options by incorporating both the mandatory fields
/// as well as the option mappings.
///
/// The options are applied in the order of: (1) service, (2) connector,
/// and (3) the bootstrap.servers must be taken from the service.
///
/// Throws error if either the service or connector options contain the
/// bootstrap.servers key.
pub fn generate_final_kafka_options(
    service: &KafkaService,
    connector_kafka_options: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, DBError> {
    // Check service options
    if service.options.contains_key("bootstrap.servers") {
        return Err(DBError::InvalidServiceConfig {
            misconfiguration: "Service Kafka options contain bootstrap.servers, should be passed explicitly as field to the service".to_string(),
        });
    }

    // 1) Apply service options
    let mut final_kafka_options = service.options.clone();

    // Check connector options
    if connector_kafka_options.contains_key("bootstrap.servers") {
        return Err(DBError::InvalidConnectorConfig {
            misconfiguration: "Connector Kafka options contain bootstrap.servers, should be passed explicitly as field to the service".to_string(),
        });
    }

    // 2) Apply connector options
    for (key, value) in connector_kafka_options.iter() {
        final_kafka_options.insert(key.clone(), value.clone());
    }

    // 3) Add bootstrap.servers from the mandatory field of the service
    final_kafka_options.insert(
        "bootstrap.servers".to_string(),
        service.bootstrap_servers.join(","),
    );

    Ok(final_kafka_options)
}

/// Configuration for a Kafka input connector.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct KafkaInput {
    /// Unique name of the Kafka service.
    pub kafka_service: String,

    /// Kafka options.
    ///
    /// These will override any duplicate keys
    /// in the Kafka options of the service.
    ///
    /// Should not contain the bootstrap.servers key
    /// as it is passed explicitly in the service.
    pub kafka_options: BTreeMap<String, String>,

    /// Kafka topics from which to consume data.
    pub topics: Vec<String>,

    /// Format of the data consumed from the topic.
    pub format: FormatConfig,

    /// (Optional) Backpressure threshold (default: 1,000,000 records)
    #[serde(default = "default_max_buffered_records")]
    pub max_buffered_records: u64,

    /// (Optional) Client log level (default: global log level of crate)
    pub log_level: Option<KafkaLogLevel>,

    /// (Optional) Maximum timeout in seconds to wait for the endpoint
    /// to join the Kafka consumer group during initialization
    /// (default: 10 seconds)
    #[serde(default = "default_group_join_timeout_secs")]
    pub group_join_timeout_secs: u32,

    /// (Optional) If specified, this enables fault tolerance
    /// in the Kafka input connector (default: none)
    pub fault_tolerance: Option<KafkaInputFtConfig>,
}

impl ConnectorConfigVariant for KafkaInput {
    fn service_names(&self) -> Vec<String> {
        vec![self.kafka_service.clone()]
    }

    fn new_replace_service_names(mut self, replacement_service_names: &[String]) -> Self {
        assert_eq!(
            replacement_service_names.len(),
            1,
            "Kafka input connector should have a single service replacement name but got: {:?}",
            replacement_service_names
        );
        self.kafka_service = replacement_service_names[0].clone();
        self
    }

    #[allow(irrefutable_let_patterns)]
    fn to_pipeline_connector_config(
        &self,
        services: &BTreeMap<String, ServiceDescr>,
    ) -> Result<PipelineConnectorConfig, DBError> {
        assert_eq!(
            services.len(),
            1,
            "Kafka input connector should have a single service but got: {:?}",
            services
        );
        let received_service = services.get(&self.kafka_service).unwrap().clone();
        if let ServiceConfig::Kafka(kafka_service) = received_service.config {
            Ok(PipelineConnectorConfig {
                transport: TransportConfig {
                    name: Cow::from("kafka"),
                    config: serde_yaml::to_value(KafkaInputConfig {
                        kafka_options: generate_final_kafka_options(
                            &kafka_service,
                            &self.kafka_options,
                        )?,
                        topics: self.topics.clone(),
                        log_level: self.log_level,
                        group_join_timeout_secs: self.group_join_timeout_secs,
                        fault_tolerance: self.fault_tolerance.clone(),
                    })
                    .unwrap(),
                },
                format: self.format.to_pipeline_format_config(),
                max_buffered_records: self.max_buffered_records,
            })
        } else {
            Err(DBError::UnexpectedServiceType {
                expected: format!("{:?}", KafkaService::config_type()),
                actual: format!("{:?}", received_service.config.config_type()),
            })
        }
    }
}

/// Configuration for a Kafka output connector.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct KafkaOutput {
    /// Unique name of the Kafka service.
    pub kafka_service: String,

    /// Kafka options.
    ///
    /// These will override any duplicate keys
    /// in the Kafka options of the service.
    ///
    /// Should not contain the bootstrap.servers key
    /// as it is passed explicitly in the service.
    pub kafka_options: BTreeMap<String, String>,

    /// Kafka topic to which to produce data.
    pub topic: String,

    /// Desired format of the data produced to the topic.
    pub format: FormatConfig,

    /// (Optional) Backpressure threshold (default: 1,000,000)
    #[serde(default = "default_max_buffered_records")]
    pub max_buffered_records: u64,

    /// (Optional) Client log level (default: global log level of crate)
    pub log_level: Option<KafkaLogLevel>,

    /// (Optional) Maximum number of unacknowledged messages
    /// buffered by the Kafka producer (default: 1,000)
    #[serde(default = "default_max_inflight_messages")]
    pub max_inflight_messages: u32,

    /// (Optional) Maximum timeout in seconds to wait for the endpoint to
    /// connect to a Kafka broker (default: 10 seconds)
    #[serde(default = "default_initialization_timeout_secs")]
    pub initialization_timeout_secs: u32,

    /// (Optional) If specified, this enables fault tolerance
    /// in the Kafka output connector (default: none)
    pub fault_tolerance: Option<KafkaOutputFtConfig>,
}

impl ConnectorConfigVariant for KafkaOutput {
    fn service_names(&self) -> Vec<String> {
        vec![self.kafka_service.clone()]
    }

    fn new_replace_service_names(mut self, replacement_service_names: &[String]) -> Self {
        assert_eq!(
            replacement_service_names.len(),
            1,
            "Kafka output connector should have a single service replacement name but got: {:?}",
            replacement_service_names
        );
        self.kafka_service = replacement_service_names[0].clone();
        self
    }

    #[allow(irrefutable_let_patterns)]
    fn to_pipeline_connector_config(
        &self,
        services: &BTreeMap<String, ServiceDescr>,
    ) -> Result<PipelineConnectorConfig, DBError> {
        assert_eq!(
            services.len(),
            1,
            "Kafka output connector should have a single service but got: {:?}",
            services
        );
        let received_service = services.get(&self.kafka_service).unwrap().clone();
        if let ServiceConfig::Kafka(kafka_service) = received_service.config {
            Ok(PipelineConnectorConfig {
                transport: TransportConfig {
                    name: Cow::from("kafka"),
                    config: serde_yaml::to_value(KafkaOutputConfig {
                        kafka_options: generate_final_kafka_options(
                            &kafka_service,
                            &self.kafka_options,
                        )?,
                        topic: self.topic.clone(),
                        log_level: self.log_level,
                        max_inflight_messages: self.max_inflight_messages,
                        initialization_timeout_secs: self.initialization_timeout_secs,
                        fault_tolerance: self.fault_tolerance.clone(),
                    })
                    .unwrap(),
                },
                format: self.format.to_pipeline_format_config(),
                max_buffered_records: self.max_buffered_records,
            })
        } else {
            Err(DBError::UnexpectedServiceType {
                expected: format!("{:?}", KafkaService::config_type()),
                actual: format!("{:?}", received_service.config.config_type()),
            })
        }
    }
}
