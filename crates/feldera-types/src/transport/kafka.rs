use anyhow::{Error as AnyError, Result as AnyResult};
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use std::fmt::Formatter;
use std::num::NonZeroUsize;
use std::thread::available_parallelism;
use std::{collections::BTreeMap, env};
use utoipa::ToSchema;
use uuid::Uuid;

/// Configuration for reading data from Kafka topics with `InputTransport`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, ToSchema)]
pub struct KafkaInputConfig {
    /// Options passed directly to `rdkafka`.
    ///
    /// [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    /// used to configure the Kafka consumer.
    ///
    /// This input connector does not use consumer groups, so options related to
    /// consumer groups are rejected, including:
    ///
    /// * `group.id`, if present, is ignored.
    /// * `auto.offset.reset` (use `start_from` instead).
    /// * "enable.auto.commit", if present, must be set to "false".
    /// * "enable.auto.offset.store", if present, must be set to "false".
    #[serde(flatten)]
    pub kafka_options: BTreeMap<String, String>,

    /// Topic to subscribe to.
    pub topic: String,

    /// The log level of the client.
    ///
    /// If not specified, the log level will be calculated based on the global
    /// log level of the `log` crate.
    pub log_level: Option<KafkaLogLevel>,

    /// Maximum timeout in seconds to wait for the endpoint to join the Kafka
    /// consumer group during initialization.
    #[serde(default = "default_group_join_timeout_secs")]
    pub group_join_timeout_secs: u32,

    /// Set to 1 or more to fix the number of threads used to poll
    /// `rdkafka`. Multiple threads can increase performance with small Kafka
    /// messages; for large messages, one thread is enough. In either case, too
    /// many threads can harm performance. If unset, the default is 3, which
    /// helps with small messages but will not harm performance with large
    /// messagee
    pub poller_threads: Option<usize>,

    /// Where to begin reading the topic.
    #[serde(default)]
    pub start_from: KafkaStartFromConfig,

    /// The AWS region to use while connecting to AWS Managed Streaming for Kafka (MSK).
    pub region: Option<String>,
}

impl KafkaInputConfig {
    // Returns the number of threads to use based on configuration, defaults,
    // and system resources.
    pub fn poller_threads(&self) -> usize {
        let max_threads = available_parallelism().map_or(16, NonZeroUsize::get);
        self.poller_threads.unwrap_or(3).clamp(1, max_threads)
    }
}

impl<'de> Deserialize<'de> for KafkaInputConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let compat = compat::KafkaInputConfigCompat::deserialize(deserializer)?;
        Self::try_from(compat).map_err(D::Error::custom)
    }
}

/// Where to begin reading a Kafka topic.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum KafkaStartFromConfig {
    /// Start from the beginning of the topic.
    Earliest,

    /// Start from the current end of the topic.
    ///
    /// This will only read any data that is added to the topic after the
    /// connector initializes.
    #[default]
    Latest,

    /// Start from particular offsets in the topic.
    ///
    /// The number of offsets must match the number of partitions in the topic.
    Offsets(Vec<i64>),
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

        let group_id = format!("{}", Uuid::now_v7());
        self.set_option_if_missing("group.id", &group_id);
        self.set_option_if_missing("enable.partition.eof", "false");

        // We link with openssl statically, which means that the default OPENSSLDIR location
        // baked into openssl is not correct (see https://github.com/fede1024/rust-rdkafka/issues/594).
        // We set the ssl.ca.location to "probe" so that librdkafka can find the CA certificates in a
        // standard location (e.g., /etc/ssl/).
        self.set_option_if_missing("ssl.ca.location", "probe");

        // Enable client context `stats` callback so we can periodically check
        // up on librdkafka memory usage.
        self.set_option_if_missing("statistics.interval.ms", "10000");

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

    /// The AWS region to use while connecting to AWS Managed Streaming for Kafka (MSK).
    pub region: Option<String>,
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

        // We link with openssl statically, which means that the default OPENSSLDIR location
        // baked into openssl is not correct (see https://github.com/fede1024/rust-rdkafka/issues/594).
        // We set the ssl.ca.location to "probe" so that librdkafka can find the CA certificates in a
        // standard location (e.g., /etc/ssl/).
        self.set_option_if_missing("ssl.ca.location", "probe");

        // Enable client context `stats` callback so we can periodically check
        // up on librdkafka memory usage.
        self.set_option_if_missing("statistics.interval.ms", "10000");

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

mod compat {
    use std::collections::BTreeMap;

    use serde::Deserialize;

    use crate::transport::kafka::{KafkaLogLevel, KafkaStartFromConfig};

    #[derive(Deserialize)]
    pub struct KafkaInputConfigCompat {
        /// Current, long-standing configuration option.
        log_level: Option<KafkaLogLevel>,

        /// Current, long-standing configuration option.
        #[serde(default = "super::default_group_join_timeout_secs")]
        group_join_timeout_secs: u32,

        /// Current configuration option.
        poller_threads: Option<usize>,

        /// Current configuration option, which changed type in an incompatible
        /// way soon after it was introduced. No backward compatibility for the
        /// initial form.
        start_from: Option<KafkaStartFromConfig>,

        /// Current configuration option that replaces the old `topics`
        /// option. Currently mandatory.
        topic: Option<String>,

        /// Old form of `topic`. Currently accepted as a substitute as long as
        /// it has exactly one element.
        #[serde(default)]
        topics: Vec<String>,

        /// Legacy, now ignored.
        fault_tolerance: Option<String>,

        /// Legacy, now ignored.
        kafka_service: Option<String>,

        /// Options passed directly to `rdkafka`.
        #[serde(flatten)]
        kafka_options: BTreeMap<String, String>,

        /// The AWS region to use while connecting to AWS Managed Streaming for Kafka (MSK).
        region: Option<String>,
    }

    impl TryFrom<KafkaInputConfigCompat> for super::KafkaInputConfig {
        type Error = String;

        fn try_from(mut compat: KafkaInputConfigCompat) -> Result<Self, Self::Error> {
            let (topic, start_from) = if !compat.topics.is_empty() {
                // Legacy mode. Convert to modern form.
                if compat.topic.is_some() {
                    return Err(
                        "Kafka input adapter may not have both (modern) `topic` and (legacy) `topics`."
                            .into(),
                    );
                }
                if compat.topics.len() != 1 {
                    return Err(format!(
                        "Kafka input adapter must have exactly one topic (not {}).",
                        compat.topics.len()
                    ));
                }
                let start_from = if let Some(start_from) = compat.start_from {
                    start_from
                } else if let Some(auto_offset_reset) =
                    compat.kafka_options.get("auto.offset.reset")
                {
                    match auto_offset_reset.as_str() {
                        "smallest" | "earliest" | "beginning" => KafkaStartFromConfig::Earliest,
                        "largest" | "latest" | "end" => KafkaStartFromConfig::Latest,
                        _ => return Err(format!("Unrecognized value {auto_offset_reset:?} for `auto.offset.reset` in Kafka legacy input adapter configuration")),
                    }
                } else {
                    KafkaStartFromConfig::default()
                };
                (compat.topics.pop().unwrap(), start_from)
            } else if let Some(topic) = compat.topic {
                // Modern mode. Forbid legacy settings.
                if compat.fault_tolerance.is_some() {
                    return Err("Kafka input adapter `fault_tolerance` setting is obsolete.".into());
                }
                if compat.kafka_service.is_some() {
                    return Err("Kafka input adapter `kafka_service` setting is obsolete.".into());
                }
                (topic, compat.start_from.unwrap_or_default())
            } else {
                return Err("Kafka input adapter is missing required `topic` setting.".into());
            };

            for key in compat.kafka_options.keys() {
                if !key.contains('.')
                    && key != "debug"
                    && key != "enabled_events"
                    && key != "retries"
                {
                    return Err(format!("Invalid Kafka input connector configuration key {key:?}: it is not valid for the input connector, nor does it contain `.` as librdkafka configuration options generally do (nor is it one of the few special exceptions to that rule)."));
                }
            }

            Ok(Self {
                topic,
                kafka_options: compat.kafka_options,
                log_level: compat.log_level,
                group_join_timeout_secs: compat.group_join_timeout_secs,
                poller_threads: compat.poller_threads,
                start_from,
                region: compat.region,
            })
        }
    }
}
