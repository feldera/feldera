use anyhow::{Error as AnyError, Result as AnyResult, anyhow, bail};
use regex::bytes::Regex;
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
    #[serde(default, with = "crate::serde_via_value")]
    pub start_from: KafkaStartFromConfig,

    /// The AWS region to use while connecting to AWS Managed Streaming for Kafka (MSK).
    pub region: Option<String>,

    /// The list of Kafka partitions to read from.
    ///
    /// Only the specified partitions will be consumed. If this field is not set,
    /// the connector will consume from all available partitions.
    ///
    /// If `start_from` is set to `offsets` and this field is provided, the
    /// number of partitions must exactly match the number of offsets, and the
    /// order of partitions must correspond to the order of offsets.
    ///
    /// If offsets are provided for all partitions, this field can be omitted.
    pub partitions: Option<Vec<i32>>,

    /// By default, if the input connector resumes from a checkpoint and the
    /// data where it needs to resume has expired from the Kafka topic, the
    /// input connector fails initialization and the pipeline will fail to start.
    ///
    /// Set this to true to change the behavior so that, if data is not
    /// available on resume, the input connector starts from the earliest
    /// offsets that are now available.
    pub resume_earliest_if_data_expires: bool,

    /// Whether to include Kafka headers in the record metadata.
    ///
    /// When `true`, Kafka message headers are available via the `CONNECTOR_METADATA()` function.
    /// See <https://docs.feldera.com/connectors/sources/kafka#metadata> for details.
    #[serde(default)]
    pub include_headers: Option<bool>,

    /// Whether to include Kafka message timestamp in the record metadata.
    ///
    /// When `true`, Kafka message timestamp is available via the `CONNECTOR_METADATA()` function.
    /// See <https://docs.feldera.com/connectors/sources/kafka#metadata> for details.
    #[serde(default)]
    pub include_timestamp: Option<bool>,

    /// Whether to include Kafka partition in the record metadata.
    ///
    /// When `true`, Kafka partition from which the message was read is available via the `CONNECTOR_METADATA()` function.
    /// See <https://docs.feldera.com/connectors/sources/kafka#metadata> for details.
    #[serde(default)]
    pub include_partition: Option<bool>,

    /// Whether to include Kafka message offset in the record metadata.
    ///
    /// When `true`, Kafka message offset is available via the `CONNECTOR_METADATA()` function.
    /// See <https://docs.feldera.com/connectors/sources/kafka#metadata> for details.
    #[serde(default)]
    pub include_offset: Option<bool>,

    /// Whether to include Kafka topic in the record metadata.
    ///
    /// When `true`, Kafka topic from which the message was read is available via the `CONNECTOR_METADATA()` function.
    /// See <https://docs.feldera.com/connectors/sources/kafka#metadata> for details.
    #[serde(default)]
    pub include_topic: Option<bool>,

    /// When lateness is enabled on a Feldera table, Feldera only produces
    /// correct output if input arrives approximately in order within the bounds
    /// of the lateness.  The Feldera Kafka input connector can reorder input
    /// when there are multiple partitions:
    ///
    /// - If partitions start at different times, then reading all the
    ///   partitions in parallel will naturally consume data out of order.
    ///
    /// - Even if they start at the same time, partitions might contain events
    ///   at different rates.
    ///
    /// - Even if the partitions start at the same time and have the same number
    ///   of events per unit time, if partitions are spread across brokers,
    ///   different brokers may fetch data at different rates.
    ///
    /// - Even if all of the partitions are on a single broker, one cannot
    ///   expect all of the partitions to naturally remain exactly in sync
    ///   forever.
    ///
    /// Setting this option to `true` addresses the issue by synchronizing
    /// ingestion across partitions, ingesting records in order of their Kafka
    /// event timestamps.
    ///
    /// Pitfalls of this solution include:
    ///
    /// - Kafka event timestamps are not necessarily monotonically increasing
    ///   even within a single partition.  If timestamps jump backward beyond
    ///   the lateness, then this can also cause correctness problems.
    ///
    ///   (This can be avoided by keeping clocks on Kafka producers and brokers
    ///   synchronized.)
    ///
    /// - If an event with a timestamp far in the future is added to a
    ///   partition, that event, and all those that follow it, will never be
    ///   processed.
    ///
    /// - If one or a few partitions have timestamps far behind the others, only
    ///   those partitions will be processed until all the old events are
    ///   processed.  (This is the flip side of the previous pitfall.)
    ///
    /// - One or more empty partitions will prevent any data from being
    ///   processed at all, because there is no way to know the timestamp for
    ///   the first event that will be added to that partition.
    ///
    /// - In a topic with `N` nonempty partitions, at least `N - 1` events will
    ///   always be left unprocessed (one in each of `N - 1` partitions), because
    ///   there is no way to know the timestamp for the next event to be added to
    ///   the partition whose events have been completely processed.
    #[serde(default)]
    pub synchronize_partitions: bool,

    /// Drop incoming messages whose Kafka headers do not satisfy this filter.
    ///
    /// The filter is a boolean expression (`and`, `or`, `not`) over regular
    /// expression tests on individual header values (see [`HeaderFilter`]).
    /// Messages that do not satisfy the filter are dropped before parsing and
    /// never reach the pipeline.  When absent (the default), all messages are
    /// admitted.
    ///
    /// Filtering is independent of `include_headers`: it works whether or not
    /// the matched headers are also surfaced to SQL via `CONNECTOR_METADATA()`.
    #[serde(default, with = "crate::serde_via_value")]
    pub header_filter: Option<HeaderFilter>,
}

impl KafkaInputConfig {
    /// Returns a default [KafkaInputConfig] with the given `kafka_options` and
    /// `topic`.  To be a usable configuration, `kafka_options` must contain at
    /// least `bootstrap.servers`.
    pub fn default(kafka_options: BTreeMap<String, String>, topic: impl Into<String>) -> Self {
        Self {
            kafka_options,
            topic: topic.into(),
            log_level: None,
            group_join_timeout_secs: default_group_join_timeout_secs(),
            poller_threads: None,
            start_from: KafkaStartFromConfig::default(),
            region: None,
            partitions: None,
            resume_earliest_if_data_expires: false,
            include_headers: None,
            include_timestamp: None,
            include_partition: None,
            include_offset: None,
            include_topic: None,
            synchronize_partitions: false,
            header_filter: None,
        }
    }

    // Returns the number of threads to use based on configuration, defaults,
    // and system resources.
    pub fn poller_threads(&self) -> usize {
        let max_threads = available_parallelism().map_or(16, NonZeroUsize::get);
        self.poller_threads.unwrap_or(3).clamp(1, max_threads)
    }

    pub fn metadata_requested(&self) -> bool {
        self.include_topic == Some(true)
            || self.include_timestamp == Some(true)
            || self.include_partition == Some(true)
            || self.include_offset == Some(true)
            || self.include_headers == Some(true)
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

/// Maximum nesting depth of a [`HeaderFilter`], to bound recursion during
/// compilation and evaluation.
const MAX_HEADER_FILTER_DEPTH: usize = 64;

/// A boolean filter over the headers of a Kafka message.
///
/// The Kafka input connector uses this to drop messages whose headers do not
/// satisfy a predicate.  It is a tree of boolean operators (`and`, `or`, `not`)
/// whose leaves are regular expression tests on individual header values.  It
/// serializes as an externally tagged JSON object, for example:
///
/// ```json
/// {
///   "and": [
///     { "header": { "name": "event-type", "pattern": "created|updated" } },
///     { "not": { "header": { "name": "source", "pattern": "test-.*" } } }
///   ]
/// }
/// ```
///
/// This admits a message only if it has an `event-type` header valued exactly
/// `created` or `updated` and does not have a `source` header whose value
/// starts with `test-`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HeaderFilter {
    /// Leaf test: matches when the message has a header named
    /// [`HeaderMatch::name`] whose value matches [`HeaderMatch::pattern`].
    Header(HeaderMatch),

    /// Conjunction: matches when every nested filter matches.  Must have at
    /// least one operand.
    And(Vec<HeaderFilter>),

    /// Disjunction: matches when at least one nested filter matches.  Must have
    /// at least one operand.
    Or(Vec<HeaderFilter>),

    /// Negation: matches when the nested filter does not match.
    Not(Box<HeaderFilter>),
}

/// A leaf of a [`HeaderFilter`]: a regular expression tested against the value
/// of a named Kafka header.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HeaderMatch {
    /// Name of the header to test, matched exactly against the header key.
    pub name: String,

    /// Regular expression ([Rust `regex` crate
    /// syntax](https://docs.rs/regex/latest/regex/#syntax)) tested against the
    /// header value.
    ///
    /// The value is matched as raw bytes, so non-UTF-8 values and byte patterns
    /// work.  The pattern must match the *entire* value: it is anchored
    /// automatically, so `^`/`$` are unnecessary (though harmless).  A header
    /// present with a null value is matched as an empty byte sequence; a header
    /// that appears more than once matches if any of its values match.
    pub pattern: String,
}

impl HeaderFilter {
    /// Validate the filter without retaining the compiled form: rejects empty
    /// `and`/`or`, nesting deeper than `MAX_HEADER_FILTER_DEPTH`, and invalid
    /// leaf patterns.
    pub fn validate(&self) -> AnyResult<()> {
        self.compile().map(|_| ())
    }

    /// Compile into a [`CompiledHeaderFilter`] ready for evaluation, compiling
    /// each leaf regular expression exactly once.  Returns an error for the same
    /// conditions as [`HeaderFilter::validate`].
    pub fn compile(&self) -> AnyResult<CompiledHeaderFilter> {
        Ok(CompiledHeaderFilter(self.compile_node(1)?))
    }

    fn compile_node(&self, depth: usize) -> AnyResult<CompiledNode> {
        if depth > MAX_HEADER_FILTER_DEPTH {
            bail!("Kafka header filter is nested more than {MAX_HEADER_FILTER_DEPTH} levels deep");
        }
        match self {
            HeaderFilter::Header(HeaderMatch { name, pattern }) => {
                // Anchor to the whole value so the pattern matches the entire
                // header value rather than a substring.  The non-capturing group
                // keeps any top-level alternation inside `pattern` bounded.
                let regex = Regex::new(&format!(r"\A(?:{pattern})\z")).map_err(|error| {
                    anyhow!(
                        "invalid regular expression {pattern:?} for Kafka header {name:?}: {error}"
                    )
                })?;
                Ok(CompiledNode::Header {
                    name: name.clone(),
                    regex,
                })
            }
            HeaderFilter::And(children) => {
                if children.is_empty() {
                    bail!("Kafka header filter 'and' must have at least one operand");
                }
                Ok(CompiledNode::And(Self::compile_children(children, depth)?))
            }
            HeaderFilter::Or(children) => {
                if children.is_empty() {
                    bail!("Kafka header filter 'or' must have at least one operand");
                }
                Ok(CompiledNode::Or(Self::compile_children(children, depth)?))
            }
            HeaderFilter::Not(child) => {
                Ok(CompiledNode::Not(Box::new(child.compile_node(depth + 1)?)))
            }
        }
    }

    fn compile_children(children: &[HeaderFilter], depth: usize) -> AnyResult<Vec<CompiledNode>> {
        children.iter().map(|c| c.compile_node(depth + 1)).collect()
    }
}

/// A [`HeaderFilter`] compiled for evaluation.  Holds the leaf regular
/// expressions compiled once so that matching a message allocates nothing.
#[derive(Debug)]
pub struct CompiledHeaderFilter(CompiledNode);

#[derive(Debug)]
enum CompiledNode {
    Header { name: String, regex: Regex },
    And(Vec<CompiledNode>),
    Or(Vec<CompiledNode>),
    Not(Box<CompiledNode>),
}

impl CompiledHeaderFilter {
    /// Evaluate the filter against a message's headers.
    ///
    /// `headers` lists the message's `(key, value)` pairs in order.  A `None`
    /// value denotes a header present with a null value and is matched as an
    /// empty byte sequence.  Returns `true` if the message satisfies the filter
    /// and should be admitted.
    pub fn matches(&self, headers: &[(&str, Option<&[u8]>)]) -> bool {
        self.0.matches(headers)
    }
}

impl CompiledNode {
    fn matches(&self, headers: &[(&str, Option<&[u8]>)]) -> bool {
        match self {
            CompiledNode::Header { name, regex } => headers
                .iter()
                .any(|&(key, value)| key == name.as_str() && regex.is_match(value.unwrap_or(b""))),
            CompiledNode::And(children) => children.iter().all(|c| c.matches(headers)),
            CompiledNode::Or(children) => children.iter().any(|c| c.matches(headers)),
            CompiledNode::Not(child) => !child.matches(headers),
        }
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

    /// Start from a particular timestamp in the topic.
    ///
    /// Kafka timestamps are in milliseconds since the epoch.
    Timestamp(i64),
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
            Err(AnyError::msg(
                "cannot override '{option}' option: the Kafka transport adapter sets this option to '{val}'",
            ))?;
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

        if let (Some(partitions), KafkaStartFromConfig::Offsets(offsets)) =
            (&self.partitions, &self.start_from)
            && partitions.len() != offsets.len()
        {
            anyhow::bail!(
                "the number of partitions ('{partitions:?}') should be equal to the number of offsets '{offsets:?}' specified"
            )
        }

        // Reject malformed filters (empty `and`/`or`, excessive nesting, or an
        // invalid regular expression) at configuration time rather than when the
        // connector starts.
        if let Some(header_filter) = &self.header_filter {
            header_filter.validate()?;
        }

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
        #[serde(default, with = "crate::serde_via_value")]
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

        /// The Kafka partitions to read from.
        partitions: Option<Vec<i32>>,

        /// By default, if the input connector resumes from a checkpoint and the
        /// data where it needs to resume has expired from the Kafka topic, the
        /// input connector fails the pipeline.
        ///
        /// Set this to true to change the behavior so that, if data is not
        /// available on resume, the input connector starts from the earliest
        /// offsets that are now available.
        #[serde(default)]
        pub resume_earliest_if_data_expires: bool,

        include_headers: Option<bool>,
        include_timestamp: Option<bool>,
        include_partition: Option<bool>,
        include_offset: Option<bool>,
        include_topic: Option<bool>,
        #[serde(default)]
        synchronize_partitions: bool,
        #[serde(default, with = "crate::serde_via_value")]
        header_filter: Option<super::HeaderFilter>,
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
                        _ => {
                            return Err(format!(
                                "Unrecognized value {auto_offset_reset:?} for `auto.offset.reset` in Kafka legacy input adapter configuration"
                            ));
                        }
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
                    return Err(format!(
                        "Invalid Kafka input connector configuration key {key:?}: it is not valid for the input connector, nor does it contain `.` as librdkafka configuration options generally do (nor is it one of the few special exceptions to that rule)."
                    ));
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
                partitions: compat.partitions,
                resume_earliest_if_data_expires: compat.resume_earliest_if_data_expires,
                include_headers: compat.include_headers,
                include_timestamp: compat.include_timestamp,
                include_partition: compat.include_partition,
                include_offset: compat.include_offset,
                include_topic: compat.include_topic,
                synchronize_partitions: compat.synchronize_partitions,
                header_filter: compat.header_filter,
            })
        }
    }
}

#[cfg(test)]
mod header_filter_tests {
    use super::{HeaderFilter, KafkaInputConfig, MAX_HEADER_FILTER_DEPTH};
    use std::collections::BTreeMap;

    /// Compile the filter described by `json` and evaluate it against `headers`.
    fn admits(json: &str, headers: &[(&str, Option<&[u8]>)]) -> bool {
        let filter: HeaderFilter = serde_json::from_str(json).unwrap();
        filter.compile().unwrap().matches(headers)
    }

    #[test]
    fn leaf_matches_whole_value() {
        let f = r#"{"header": {"name": "type", "pattern": "created|updated"}}"#;
        assert!(admits(f, &[("type", Some(&b"created"[..]))]));
        assert!(admits(f, &[("type", Some(&b"updated"[..]))]));

        // Patterns are anchored to the whole value: a substring must not match.
        assert!(!admits(f, &[("type", Some(&b"created-x"[..]))]));
        assert!(!admits(f, &[("type", Some(&b"x-created"[..]))]));

        // Wrong value, wrong header name, and missing header all fail a leaf.
        assert!(!admits(f, &[("type", Some(&b"deleted"[..]))]));
        assert!(!admits(f, &[("other", Some(&b"created"[..]))]));
        assert!(!admits(f, &[]));
    }

    #[test]
    fn null_value_is_empty_bytes() {
        let empty = r#"{"header": {"name": "h", "pattern": ""}}"#;
        let nonempty = r#"{"header": {"name": "h", "pattern": ".+"}}"#;

        // A header with a null value matches an empty pattern but not `.+`.
        assert!(admits(empty, &[("h", None)]));
        assert!(!admits(nonempty, &[("h", None)]));
        assert!(admits(empty, &[("h", Some(&b""[..]))]));
    }

    #[test]
    fn duplicate_header_any_value_matches() {
        let f = r#"{"header": {"name": "k", "pattern": "b"}}"#;
        assert!(admits(f, &[("k", Some(&b"a"[..])), ("k", Some(&b"b"[..]))]));
        assert!(!admits(
            f,
            &[("k", Some(&b"a"[..])), ("k", Some(&b"c"[..]))]
        ));
    }

    #[test]
    fn non_utf8_value_bytes() {
        // A single-byte match against a non-UTF-8 value must work without panic.
        let f = r#"{"header": {"name": "b", "pattern": "(?s-u:.)"}}"#;
        assert!(admits(f, &[("b", Some(&[0xffu8][..]))]));
        // Anchoring still applies: two bytes do not match a single-byte pattern.
        assert!(!admits(f, &[("b", Some(&[0xffu8, 0xfe][..]))]));
    }

    #[test]
    fn boolean_combinations() {
        // A AND (B OR C).
        let f = r#"{"and": [
            {"header": {"name": "a", "pattern": "1"}},
            {"or": [
                {"header": {"name": "b", "pattern": "1"}},
                {"header": {"name": "c", "pattern": "1"}}
            ]}
        ]}"#;
        assert!(admits(f, &[("a", Some(&b"1"[..])), ("b", Some(&b"1"[..]))]));
        assert!(admits(f, &[("a", Some(&b"1"[..])), ("c", Some(&b"1"[..]))]));
        assert!(!admits(f, &[("a", Some(&b"1"[..]))])); // neither b nor c
        assert!(!admits(f, &[("b", Some(&b"1"[..]))])); // missing a
    }

    #[test]
    fn not_over_leaf() {
        let f = r#"{"not": {"header": {"name": "env", "pattern": "prod"}}}"#;
        assert!(!admits(f, &[("env", Some(&b"prod"[..]))])); // matching -> dropped
        assert!(admits(f, &[("env", Some(&b"dev"[..]))])); // non-matching -> admitted
        assert!(admits(f, &[])); // absent header -> admitted
    }

    #[test]
    fn de_morgan_equivalence() {
        let lhs = r#"{"not": {"and": [
            {"header": {"name": "a", "pattern": "1"}},
            {"header": {"name": "b", "pattern": "1"}}
        ]}}"#;
        let rhs = r#"{"or": [
            {"not": {"header": {"name": "a", "pattern": "1"}}},
            {"not": {"header": {"name": "b", "pattern": "1"}}}
        ]}"#;
        let l = serde_json::from_str::<HeaderFilter>(lhs)
            .unwrap()
            .compile()
            .unwrap();
        let r = serde_json::from_str::<HeaderFilter>(rhs)
            .unwrap()
            .compile()
            .unwrap();
        for a in [None, Some(&b"1"[..])] {
            for b in [None, Some(&b"1"[..])] {
                let mut headers: Vec<(&str, Option<&[u8]>)> = Vec::new();
                if let Some(v) = a {
                    headers.push(("a", Some(v)));
                }
                if let Some(v) = b {
                    headers.push(("b", Some(v)));
                }
                assert_eq!(
                    l.matches(&headers),
                    r.matches(&headers),
                    "headers={headers:?}"
                );
            }
        }
    }

    #[test]
    fn rejects_empty_combinators() {
        for json in [r#"{"and": []}"#, r#"{"or": []}"#] {
            let filter: HeaderFilter = serde_json::from_str(json).unwrap();
            assert!(filter.validate().is_err(), "expected {json} to be rejected");
        }
    }

    #[test]
    fn rejects_deep_nesting() {
        // Wrap a leaf in `not` more times than the depth limit allows.
        let mut json = String::from(r#"{"header": {"name": "x", "pattern": "y"}}"#);
        for _ in 0..MAX_HEADER_FILTER_DEPTH + 5 {
            json = format!(r#"{{"not": {json}}}"#);
        }
        let filter: HeaderFilter = serde_json::from_str(&json).unwrap();
        assert!(filter.validate().is_err());
    }

    #[test]
    fn rejects_invalid_regex() {
        let filter: HeaderFilter =
            serde_json::from_str(r#"{"header": {"name": "x", "pattern": "("}}"#).unwrap();
        assert!(filter.validate().is_err());
    }

    #[test]
    fn serde_round_trip_external_tagging() {
        let json = r#"{"and":[{"header":{"name":"t","pattern":"a|b"}},{"not":{"header":{"name":"s","pattern":"x"}}}]}"#;
        let filter: HeaderFilter = serde_json::from_str(json).unwrap();
        let reserialized = serde_json::to_string(&filter).unwrap();
        assert_eq!(reserialized, json);
    }

    #[test]
    fn config_validate_rejects_bad_filter() {
        let mut config = KafkaInputConfig::default(BTreeMap::new(), "topic");
        config.header_filter = Some(serde_json::from_str(r#"{"or": []}"#).unwrap());
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_deserializes_header_filter() {
        // Exercises the compat deserialization path for the new field.
        let config: KafkaInputConfig = serde_json::from_value(serde_json::json!({
            "topic": "t",
            "bootstrap.servers": "localhost:9092",
            "header_filter": {"header": {"name": "k", "pattern": "v"}}
        }))
        .unwrap();
        assert!(config.header_filter.is_some());
    }

    #[test]
    fn config_with_header_filter_serializes_to_yaml() {
        // Regression: the pipeline config is serialized to YAML when a pipeline
        // is provisioned. `HeaderFilter` is a nested enum, which `serde_yaml`
        // cannot serialize directly; `serde_via_value` keeps this working.
        // Without it, the pipeline runner panics and the pipeline is stuck
        // provisioning.
        let mut config = KafkaInputConfig::default(
            BTreeMap::from([(
                "bootstrap.servers".to_string(),
                "localhost:9092".to_string(),
            )]),
            "topic",
        );
        config.header_filter = Some(
            serde_json::from_str(
                r#"{"and":[{"header":{"name":"a","pattern":"b"}},{"not":{"header":{"name":"c","pattern":"d"}}}]}"#,
            )
            .unwrap(),
        );

        // Must not error (this is what panicked in the pipeline runner).
        let yaml = serde_yaml::to_string(&config).expect("serialize config to YAML");

        // And the config round-trips back through YAML.
        let reparsed: KafkaInputConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(reparsed.header_filter, config.header_filter);
    }
}
