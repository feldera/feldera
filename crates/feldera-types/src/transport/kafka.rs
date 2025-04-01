use anyhow::{Error as AnyError, Result as AnyResult};
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use std::fmt::Formatter;
use std::{collections::BTreeMap, env};
use utoipa::ToSchema;
use uuid::Uuid;

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

    /// Deprecated.
    pub fault_tolerance: Option<String>,

    /// Deprecated.
    pub kafka_service: Option<String>,

    /// Set to 1 or more to fix the number of threads used to poll
    /// `rdkafka`. Multiple threads can increase performance with small Kafka
    /// messages; for large messages, one thread is enough. In either case, too
    /// many threads can harm performance. If unset, the default is 3, which
    /// helps with small messages but will not harm performance with large
    /// messagee
    pub poller_threads: Option<usize>,

    /// A list of offsets and partitions specifying where to begin reading individual input topics.
    ///
    /// When specified, this property must contain a list of JSON objects with the following fields:
    /// - `topic`: The name of the Kafka topic.
    /// - `partition`: The partition number within the topic.
    /// - `offset`: The specific offset from which to start consuming messages.
    #[serde(default)]
    pub start_from: Vec<KafkaStartFromConfig>,
}

/// Configuration for starting from a specific offset in a Kafka topic partition.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct KafkaStartFromConfig {
    /// The Kafka topic.
    pub topic: String,

    /// The parition within the topic.
    pub partition: u32,

    /// The offset in the specified partition to start reading from.
    pub offset: u64,
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

        let group_id = format!("{}", Uuid::now_v7());
        self.set_option_if_missing("group.id", &group_id);
        self.set_option_if_missing("enable.partition.eof", "false");

        Ok(())
    }
}

pub fn default_redpanda_server() -> String {
    env::var("REDPANDA_BROKERS").unwrap_or_else(|_| "localhost".to_string())
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

    /// Maximum timeout in seconds to wait for the endpoint to connect to
    /// a Kafka broker.
    ///
    /// Defaults to 60.
    #[serde(default = "default_initialization_timeout_secs")]
    pub initialization_timeout_secs: u32,

    /// Optional configuration for fault tolerance.
    pub fault_tolerance: Option<KafkaOutputFtConfig>,

    /// If specified, this service is used to provide defaults for the Kafka options.
    pub kafka_service: Option<String>,
}

/// Fault tolerance configuration for Kafka output connector.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct KafkaOutputFtConfig {
    /// Options passed to `rdkafka` for consumers only, as documented at
    /// [`librdkafka`
    /// options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
    ///
    /// These options override `kafka_options` for consumers, and may be empty.
    pub consumer_options: BTreeMap<String, String>,

    /// Options passed to `rdkafka` for producers only, as documented at
    /// [`librdkafka`
    /// options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
    ///
    /// These options override `kafka_options` for producers, and may be empty.
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
