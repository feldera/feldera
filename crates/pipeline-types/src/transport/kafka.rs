use serde_json::Value as JsonValue;
use std::fmt::Formatter;
use std::{
    collections::BTreeMap,
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::config::TransportConfigVariant;
use crate::service::{KafkaService, ServiceConfig, ServiceConfigVariant};
use crate::transport::error::{TransportReplaceError, TransportResolveError};
use anyhow::{Error as AnyError, Result as AnyResult};
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from Kafka topics with `InputTransport`.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct KafkaInputConfig {
    /// Options passed directly to `rdkafka`.
    ///
    /// [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    /// used to configure the Kafka consumer.  Not all options are valid with
    /// this Kafka adapter:
    ///
    /// * "enable.auto.commit", if present, must be set to "false",
    /// * "enable.auto.offset.store", if present, must be set to "false"
    #[serde(flatten)]
    pub kafka_options: BTreeMap<String, String>,

    /// List of topics to subscribe to.
    pub topics: Vec<String>,

    /// The log level of the client.
    ///
    /// If not specified, the log level will be calculated based on the global
    /// log level of the `log` crate.
    pub log_level: Option<KafkaLogLevel>,

    /// Maximum timeout in seconds to wait for the endpoint to join the Kafka
    /// consumer group during initialization.
    #[serde(default = "default_group_join_timeout_secs")]
    pub group_join_timeout_secs: u32,

    /// If specified, this enables fault tolerance in the Kafka input connector.
    pub fault_tolerance: Option<KafkaInputFtConfig>,

    /// If specified, this service is used to provide defaults for the Kafka options.
    pub kafka_service: Option<String>,
}

/// Fault tolerance configuration for Kafka input connector.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct KafkaInputFtConfig {
    /// Options passed to `rdkafka` for consumers only, as documented at
    /// [`librdkafka`
    /// options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
    ///
    /// These options override `kafka_options` for consumers, and may be empty.
    #[serde(default)]
    pub consumer_options: BTreeMap<String, String>,

    /// Options passed to `rdkafka` for producers only, as documented at
    /// [`librdkafka`
    /// options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
    ///
    /// These options override `kafka_options` for producers, and may be empty.
    #[serde(default)]
    pub producer_options: BTreeMap<String, String>,

    /// Suffix to append to each data topic name, to give the name of a topic
    /// that the connector uses for recording the division of the corresponding
    /// data topic into steps.  Defaults to `_input-index`.
    ///
    /// An index topic must have the same number of partitions as its
    /// corresponding data topic.
    ///
    /// If two or more fault-tolerant Kafka endpoints read from overlapping sets
    /// of topics, they must specify different `index_suffix` values.
    pub index_suffix: Option<String>,

    /// If this is true or unset, then the connector will create missing index
    /// topics as needed.  If this is false, then a missing index topic is a
    /// fatal error.
    #[serde(default)]
    pub create_missing_index: Option<bool>,

    /// Maximum number of bytes in a step.  Any individual message bigger than
    /// this will be given a step of its own.
    pub max_step_bytes: Option<u64>,

    /// Maximum number of messages in a step.
    pub max_step_messages: Option<u64>,
}

/// Kafka logging levels.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ToSchema)]
pub enum KafkaLogLevel {
    #[serde(rename = "emerg")]
    Emerg,
    #[serde(rename = "alert")]
    Alert,
    #[serde(rename = "critical")]
    Critical,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "warning")]
    Warning,
    #[serde(rename = "notice")]
    Notice,
    #[serde(rename = "info")]
    Info,
    #[serde(rename = "debug")]
    Debug,
}

/// On startup, the endpoint waits to join the consumer group.
/// This constant defines the default wait timeout.
const fn default_group_join_timeout_secs() -> u32 {
    10
}

impl KafkaInputConfig {
    /// Set `option` to `val`; return an error if `option` is set to a different
    /// value.
    #[allow(dead_code)]
    fn enforce_option(&mut self, option: &str, val: &str) -> AnyResult<()> {
        let option_val = self
            .kafka_options
            .entry(option.to_string())
            .or_insert_with(|| val.to_string());
        if option_val != val {
            Err(AnyError::msg("cannot override '{option}' option: the Kafka transport adapter sets this option to '{val}'"))?;
        }
        Ok(())
    }

    /// Set `option` to `val`, if missing.
    fn set_option_if_missing(&mut self, option: &str, val: &str) {
        self.kafka_options
            .entry(option.to_string())
            .or_insert_with(|| val.to_string());
    }

    /// Validate configuration, set default option values required by this
    /// adapter.
    pub fn validate(&mut self) -> AnyResult<()> {
        self.set_option_if_missing("bootstrap.servers", &default_redpanda_server());

        // These options will prevent librdkafka from automatically committing offsets
        // of consumed messages to the broker, meaning that next time the
        // connector is instantiated it will start reading from the offset
        // specified in `auto.offset.reset`.  We used to set these to
        // `true`, which caused `rdkafka` to hang in some circumstances
        // (https://github.com/confluentinc/librdkafka/issues/3954).  Besides, the new behavior
        // is probably more correct given that circuit state currently does not survive
        // across pipeline restarts, so it makes sense to start feeding messages
        // from the start rather than from the last offset consumed by the
        // previous instance of the pipeline, whose state is lost.  Once we add
        // fault tolerance, we will likely use explicit commits, which also do
        // not require these options.
        //
        // See https://docs.confluent.io/platform/current/clients/consumer.html#offset-management
        //
        // Note: we allow the user to override the options, so they can still enable
        // auto commit if they know what they are doing, e.g., the secops demo
        // requires the pipeline to commit its offset for the generator to know
        // when to resume sending.
        self.set_option_if_missing("enable.auto.commit", "false");
        self.set_option_if_missing("enable.auto.offset.store", "false");

        let group_id = format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        self.set_option_if_missing("group.id", &group_id);
        self.set_option_if_missing("enable.partition.eof", "false");

        Ok(())
    }
}

pub fn default_redpanda_server() -> String {
    env::var("REDPANDA_BROKERS").unwrap_or_else(|_| "localhost".to_string())
}

const fn default_max_inflight_messages() -> u32 {
    1000
}

const fn default_initialization_timeout_secs() -> u32 {
    60
}

/// Kafka header value encoded as a UTF-8 string or a byte array.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, ToSchema)]
#[repr(transparent)]
pub struct KafkaHeaderValue(pub Vec<u8>);

/// Visitor for deserializing Kafka headers value.
struct HeaderVisitor;

impl<'de> Visitor<'de> for HeaderVisitor {
    type Value = KafkaHeaderValue;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a string (e.g., \"xyz\") or a byte array (e.g., '[1,2,3])")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(KafkaHeaderValue(v.as_bytes().to_owned()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(KafkaHeaderValue(v.into_bytes()))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut result = Vec::with_capacity(seq.size_hint().unwrap_or_default());

        while let Some(b) = seq.next_element()? {
            result.push(b);
        }

        Ok(KafkaHeaderValue(result))
    }
}

impl<'de> Deserialize<'de> for KafkaHeaderValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(HeaderVisitor)
    }
}

#[cfg(test)]
#[test]
fn test_kafka_header_value_deserialize() {
    assert_eq!(
        serde_json::from_str::<KafkaHeaderValue>(r#""foobar""#).unwrap(),
        KafkaHeaderValue(br#"foobar"#.to_vec())
    );

    assert_eq!(
        serde_json::from_str::<KafkaHeaderValue>(r#"[1,2,3,4,5]"#).unwrap(),
        KafkaHeaderValue(vec![1u8, 2, 3, 4, 5])
    );

    assert!(serde_json::from_str::<KafkaHeaderValue>(r#"150"#).is_err());

    assert!(serde_json::from_str::<KafkaHeaderValue>(r#"{{"foo": "bar"}}"#).is_err());
}

/// Kafka message header.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct KafkaHeader {
    pub key: String,
    pub value: Option<KafkaHeaderValue>,
}

/// Configuration for writing data to a Kafka topic with `OutputTransport`.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct KafkaOutputConfig {
    /// Options passed directly to `rdkafka`.
    ///
    /// See [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    /// used to configure the Kafka producer.
    #[serde(flatten)]
    pub kafka_options: BTreeMap<String, String>,

    /// Topic to write to.
    pub topic: String,

    /// Kafka headers to be added to each message produced by this connector.
    #[serde(default)]
    pub headers: Vec<KafkaHeader>,

    /// The log level of the client.
    ///
    /// If not specified, the log level will be calculated based on the global
    /// log level of the `log` crate.
    pub log_level: Option<KafkaLogLevel>,

    /// Maximum number of unacknowledged messages buffered by the Kafka
    /// producer.
    ///
    /// Kafka producer buffers outgoing messages until it receives an
    /// acknowledgement from the broker.  This configuration parameter
    /// bounds the number of unacknowledged messages.  When the number of
    /// unacknowledged messages reaches this limit, sending of a new message
    /// blocks until additional acknowledgements arrive from the broker.
    ///
    /// Defaults to 1000.
    #[serde(default = "default_max_inflight_messages")]
    pub max_inflight_messages: u32,

    /// Maximum timeout in seconds to wait for the endpoint to connect to
    /// a Kafka broker.
    ///
    /// Defaults to 60.
    #[serde(default = "default_initialization_timeout_secs")]
    pub initialization_timeout_secs: u32,

    /// If specified, this enables fault tolerance in the Kafka output
    /// connector.
    pub fault_tolerance: Option<KafkaOutputFtConfig>,

    /// If specified, this service is used to provide defaults for the Kafka options.
    pub kafka_service: Option<String>,
}

/// Fault tolerance configuration for Kafka output connector.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct KafkaOutputFtConfig {
    /// Options passed to `rdkafka` for consumers only, as documented at
    /// [`librdkafka`
    /// options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
    ///
    /// These options override `kafka_options` for consumers, and may be empty.
    #[serde(default)]
    pub consumer_options: BTreeMap<String, String>,

    /// Options passed to `rdkafka` for producers only, as documented at
    /// [`librdkafka`
    /// options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
    ///
    /// These options override `kafka_options` for producers, and may be empty.
    #[serde(default)]
    pub producer_options: BTreeMap<String, String>,
}

impl KafkaOutputConfig {
    #[allow(dead_code)]
    /// Set `option` to `val`, if missing.
    fn set_option_if_missing(&mut self, option: &str, val: &str) {
        self.kafka_options
            .entry(option.to_string())
            .or_insert_with(|| val.to_string());
    }

    #[allow(dead_code)]
    /// Validate configuration, set default option values required by this
    /// adapter.
    pub fn validate(&mut self) -> AnyResult<()> {
        self.set_option_if_missing("bootstrap.servers", &default_redpanda_server());
        Ok(())
    }
}

/// A set of updates to a SQL table or view.
///
/// The `sequence_number` field stores the offset of the chunk relative to the
/// start of the stream and can be used to implement reliable delivery.
/// The payload is stored in the `bin_data`, `text_data`, or `json_data` field
/// depending on the data format used.
#[derive(Deserialize, ToSchema)]
pub struct Chunk {
    pub sequence_number: u64,

    // Exactly one of the following fields must be set.
    // This should be an enum inlined with `#[serde(flatten)]`, but `utoipa`
    // struggles to generate a schema for that.
    /// Base64 encoded binary payload, e.g., bincode.
    pub bin_data: Option<Vec<u8>>,

    /// Text payload, e.g., CSV.
    pub text_data: Option<String>,

    /// JSON payload.
    #[schema(value_type = Option<Object>)]
    pub json_data: Option<JsonValue>,
}

/// Generates the final Kafka options combining those of the service,
/// followed by the connector Kafka options.
pub fn combine_service_and_connector_kafka_options(
    service: &KafkaService,
    connector_kafka_options: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, TransportResolveError> {
    // 1) Service Kafka options
    let mut kafka_options =
        service
            .generate_final_options()
            .map_err(|e| TransportResolveError::InvalidConfig {
                reason: format!("{:?}", e),
            })?;

    // 2) Apply connector options overriding any duplicates
    for (key, value) in connector_kafka_options.iter() {
        kafka_options.insert(key.clone(), value.clone());
    }

    Ok(kafka_options)
}

impl TransportConfigVariant for KafkaInputConfig {
    fn name(&self) -> String {
        "kafka_input".to_string()
    }

    fn service_names(&self) -> Vec<String> {
        match &self.kafka_service {
            None => vec![],
            Some(service_name) => vec![service_name.clone()],
        }
    }

    fn replace_any_service_names(
        mut self,
        replacement_service_names: &[String],
    ) -> Result<Self, TransportReplaceError> {
        match self.kafka_service {
            None => {
                if replacement_service_names.is_empty() {
                    Ok(self)
                } else {
                    Err(TransportReplaceError {
                        expected: 0,
                        actual: replacement_service_names.len(),
                    })
                }
            }
            Some(_) => {
                if replacement_service_names.len() == 1 {
                    self.kafka_service = Some(replacement_service_names.first().unwrap().clone());
                    Ok(self)
                } else {
                    Err(TransportReplaceError {
                        expected: 1,
                        actual: replacement_service_names.len(),
                    })
                }
            }
        }
    }

    #[allow(irrefutable_let_patterns)]
    fn resolve_any_services(
        self,
        services: &BTreeMap<String, ServiceConfig>,
    ) -> Result<Self, TransportResolveError> {
        match self.kafka_service {
            None => Ok(self),
            Some(kafka_service_name) => {
                // There must be exactly one service present
                if services.len() != 1 {
                    return Err(TransportResolveError::IncorrectNumServices {
                        expected: 1,
                        actual: services.len(),
                    });
                }

                // The service must be a Kafka service
                let received_service = services.get(&kafka_service_name).unwrap().clone();
                if let ServiceConfig::Kafka(kafka_service) = received_service {
                    Ok(KafkaInputConfig {
                        kafka_options: combine_service_and_connector_kafka_options(
                            &kafka_service,
                            &self.kafka_options,
                        )?,
                        topics: self.topics,
                        log_level: self.log_level,
                        group_join_timeout_secs: self.group_join_timeout_secs,
                        fault_tolerance: self.fault_tolerance,
                        kafka_service: None, // The service has been resolved, as such it is set to None
                    })
                } else {
                    Err(TransportResolveError::UnexpectedServiceType {
                        expected: format!("{:?}", KafkaService::config_type()),
                        actual: format!("{:?}", received_service.config_type()),
                    })
                }
            }
        }
    }
}

impl TransportConfigVariant for KafkaOutputConfig {
    fn name(&self) -> String {
        "kafka_output".to_string()
    }

    fn service_names(&self) -> Vec<String> {
        match &self.kafka_service {
            None => vec![],
            Some(service_name) => vec![service_name.clone()],
        }
    }

    fn replace_any_service_names(
        mut self,
        replacement_service_names: &[String],
    ) -> Result<Self, TransportReplaceError> {
        match self.kafka_service {
            None => {
                if replacement_service_names.is_empty() {
                    Ok(self)
                } else {
                    Err(TransportReplaceError {
                        expected: 0,
                        actual: replacement_service_names.len(),
                    })
                }
            }
            Some(_) => {
                if replacement_service_names.len() == 1 {
                    self.kafka_service = Some(replacement_service_names.first().unwrap().clone());
                    Ok(self)
                } else {
                    Err(TransportReplaceError {
                        expected: 1,
                        actual: replacement_service_names.len(),
                    })
                }
            }
        }
    }

    #[allow(irrefutable_let_patterns)]
    fn resolve_any_services(
        self,
        services: &BTreeMap<String, ServiceConfig>,
    ) -> Result<Self, TransportResolveError> {
        match self.kafka_service {
            None => Ok(self),
            Some(kafka_service_name) => {
                // There must be exactly one service present
                if services.len() != 1 {
                    return Err(TransportResolveError::IncorrectNumServices {
                        expected: 1,
                        actual: services.len(),
                    });
                }

                // The service must be a Kafka service
                let received_service = services.get(&kafka_service_name).unwrap().clone();
                if let ServiceConfig::Kafka(kafka_service) = received_service {
                    Ok(KafkaOutputConfig {
                        kafka_options: combine_service_and_connector_kafka_options(
                            &kafka_service,
                            &self.kafka_options,
                        )?,
                        topic: self.topic,
                        headers: self.headers,
                        log_level: self.log_level,
                        max_inflight_messages: self.max_inflight_messages,
                        initialization_timeout_secs: self.initialization_timeout_secs,
                        fault_tolerance: self.fault_tolerance,
                        kafka_service: None, // The service has been resolved, as such it is set to None
                    })
                } else {
                    Err(TransportResolveError::UnexpectedServiceType {
                        expected: format!("{:?}", KafkaService::config_type()),
                        actual: format!("{:?}", received_service.config_type()),
                    })
                }
            }
        }
    }
}
