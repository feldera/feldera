use serde_json::Value as JsonValue;
use std::{
    collections::BTreeMap,
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Error as AnyError, Result as AnyResult};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from Kafka topics with `InputTransport`.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, ToSchema)]
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
}

/// Fault tolerance configuration for Kafka input connector.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, ToSchema)]
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
#[derive(Serialize, Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ToSchema)]
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
pub const fn default_group_join_timeout_secs() -> u32 {
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

pub const fn default_max_inflight_messages() -> u32 {
    1000
}

pub const fn default_initialization_timeout_secs() -> u32 {
    10
}

/// Configuration for writing data to a Kafka topic with `OutputTransport`.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, ToSchema)]
pub struct KafkaOutputConfig {
    /// Options passed directly to `rdkafka`.
    ///
    /// See [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    /// used to configure the Kafka producer.
    #[serde(flatten)]
    pub kafka_options: BTreeMap<String, String>,

    /// Topic to write to.
    pub topic: String,

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
    /// Defaults to 10.
    #[serde(default = "default_initialization_timeout_secs")]
    pub initialization_timeout_secs: u32,

    /// If specified, this enables fault tolerance in the Kafka output
    /// connector.
    pub fault_tolerance: Option<KafkaOutputFtConfig>,
}

/// Fault tolerance configuration for Kafka output connector.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, ToSchema)]
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
