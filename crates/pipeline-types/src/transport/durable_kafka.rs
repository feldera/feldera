use std::collections::BTreeMap;

use serde::Deserialize;
use utoipa::ToSchema;

use super::kafka::KafkaLogLevel;

const fn default_max_inflight_messages() -> u32 {
    1000
}

const fn default_initialization_timeout_secs() -> u64 {
    10
}

/// Common configuration for reading and writing durable data in Kafka topics.
///
/// This is flattened into the durable Kafka input and output configs.
#[derive(Deserialize, Clone, Debug, ToSchema)]
pub struct CommonConfigSchema {
    /// Options passed to `rdkafka` for consumer, producer, and admin clients,
    /// as documented at [`librdkafka`
    /// options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
    ///
    /// The `bootstrap.servers` option should ordinarily be supplied.  If it is
    /// omitted, it defaults to the value of environment variable
    /// `REDPANDA_BROKERS` if it is set, or to `localhost` otherwise.  Other
    /// configuration is optional.
    #[serde(default)]
    pub kafka_options: BTreeMap<String, String>,

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

    /// The log level of the client.
    ///
    /// If not specified, the log level will be calculated based on the global
    /// log level of the `log` crate.
    pub log_level: Option<KafkaLogLevel>,
}

/// Configuration for reading data from Kafka topics with `InputTransport`.
#[derive(Deserialize, Clone, Debug, ToSchema)]
pub struct KafkaDurableInputConfig {
    /// Configuration in common with durable Kafka output.
    #[serde(flatten)]
    pub common: CommonConfigSchema,

    /// List of topics to subscribe to.  At least one topic is required.
    pub topics: Vec<String>,

    /// Suffix to append to each data topic name, to give the name of a topic
    /// that the connector uses for recording the division of the corresponding
    /// data topic into steps.  Defaults to `_input-index`.
    ///
    /// An index topic must have the same number of partitions as its
    /// corresponding data topic.
    ///
    /// If two or more durable Kafka endpoints read from overlapping sets of
    /// topics, they must specify different `index_suffix` values.
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

/// Configuration for writing data to a Kafka topic with `OutputTransport`.
#[derive(Deserialize, Debug, ToSchema)]
pub struct KafkaDurableOutputConfig {
    /// Configuration in common with durable Kafka output.
    #[serde(flatten)]
    pub common: CommonConfigSchema,

    /// Topic to write to.
    pub topic: String,

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
    /// Defaults to 10.
    #[serde(default = "default_initialization_timeout_secs")]
    pub initialization_timeout_secs: u64,
}
