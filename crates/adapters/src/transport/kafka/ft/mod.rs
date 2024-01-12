//! Fault-tolerant Kafka input and output transports.
//!
//! For input from Kafka to be repeatable, we need to ensure that a given worker
//! reads the same messages from the same partitions in a given step.  To do
//! that, we store the mapping from steps to Kafka offsets in a separate Kafka
//! topic, called the "index topic", which the fault-tolerant input transport
//! maintains automatically.
//!
//! For output to Kafka, we need to be able to discard duplicate output.  We do
//! that by recording the step number as the key in each output message.  On
//! initialization, we read the final step number and discard any output for
//! duplicate steps.  We use Kafka transactions to avoid writing partial output
//! for a step.
mod input;
mod output;

use crate::transport::kafka::refine_kafka_error;
use anyhow::{anyhow, bail, Context, Error as AnyError, Result as AnyResult};
use log::{debug, error, info, warn};
use pipeline_types::transport::kafka::{default_redpanda_server, KafkaLogLevel};
use rdkafka::{
    client::Client as KafkaClient,
    config::RDKafkaLogLevel,
    consumer::{base_consumer::PartitionQueue, BaseConsumer, Consumer, ConsumerContext},
    error::{KafkaError, KafkaResult},
    message::BorrowedMessage,
    producer::{Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
    util::Timeout,
    ClientConfig, ClientContext, Offset, TopicPartitionList,
};
use std::{
    collections::BTreeMap,
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    ops::Range,
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

pub use input::Endpoint as KafkaFtInputEndpoint;
pub use output::KafkaOutputEndpoint as KafkaFtOutputEndpoint;

use super::{rdkafka_loglevel_from, DeferredLogging};

#[cfg(test)]
pub mod test;

const POLL_TIMEOUT: Duration = Duration::from_millis(1);

/// Set `option` to `val`; return an error if `option` is set to a different
/// value.
fn enforce_option<'a>(
    settings: &mut BTreeMap<&'a str, &'a str>,
    option: &'a str,
    val: &'a str,
) -> AnyResult<()> {
    if *settings.entry(option).or_insert(val) != val {
        bail!("cannot override '{option}' option: the Kafka transport adapter sets this option to '{val}'");
    }
    Ok(())
}

/// Set `option` to `val`, if missing.
fn set_option_if_missing<'a>(
    settings: &mut BTreeMap<&'a str, &'a str>,
    option: &'a str,
    val: &'a str,
) {
    settings.entry(option).or_insert(val);
}

/// Returns a Kafka client configuration from `source.kafka_options` as
/// overridden by `type_specific_options`.  The latter should be
/// `&source.consumer_options` or `&source.producer_options` depending on
/// the type of client to be configured.
///
/// `overrides` specifies key-value pairs to override. This function flags
/// an error on if `source` and `type_specific_options` conflict with
/// `overrides`.
///
/// `config_name` is used only in the returned error message, if any.
fn kafka_config(
    kafka_options: &BTreeMap<String, String>,
    type_specific_options: &BTreeMap<String, String>,
    log_level: Option<KafkaLogLevel>,
    overrides: &[(&str, &str)],
    add_group_id: bool,
    config_name: &str,
) -> AnyResult<ClientConfig> {
    let mut settings: BTreeMap<&str, &str> = kafka_options
        .iter()
        .chain(type_specific_options.iter())
        .map(|(o, v)| (o.as_str(), v.as_str()))
        .collect();
    for (option, val) in overrides {
        enforce_option(&mut settings, option, val)
            .with_context(|| format!("Failed to validate Kafka options for {config_name}"))?;
    }

    // Set a unique `group.id` to ensure that we don't conflict with any
    // existing consumer group.  In experiments, Kafka won't create any
    // consumer group on the backend unless we implicitly (with
    // `enable.auto.commit`) or explicitly commit an offset.  We don't do
    // that, so this doesn't waste space on the Kafka brokers.
    let group_id = &Uuid::new_v4().to_string();
    if add_group_id {
        enforce_option(&mut settings, "group.id", group_id)
            .with_context(|| format!("Failed to validate Kafka options for {config_name}"))?;
    }

    let default_redpanda_server = default_redpanda_server();
    set_option_if_missing(&mut settings, "bootstrap.servers", &default_redpanda_server);

    let mut config: ClientConfig = settings
        .iter()
        .map(|(&o, &v)| (String::from(o), String::from(v)))
        .collect();
    if let Some(log_level) = log_level {
        config.set_log_level(rdkafka_loglevel_from(log_level));
    }

    Ok(config)
}

/// A collection of Kafka client configurations for use by the input and output
/// endpoints.
#[derive(Clone)]
struct CommonConfig {
    /// Kafka client configuration for reading `data_topic`.
    data_consumer_config: ClientConfig,

    /// Kafka client configuration for reading with multiple seeks.
    seekable_consumer_config: ClientConfig,

    /// Kafka client configuration for writing to the data and index topics.
    producer_config: ClientConfig,

    /// Kafka admin client configuration for adding new topics.
    admin_config: ClientConfig,
}

impl CommonConfig {
    fn new(
        kafka_options: &BTreeMap<String, String>,
        consumer_options: &BTreeMap<String, String>,
        producer_options: &BTreeMap<String, String>,
        log_level: Option<KafkaLogLevel>,
    ) -> AnyResult<Self> {
        const CONSUMER_SETTINGS: &[(&str, &str)] = &[
            ("enable.auto.commit", "false"),
            ("enable.auto.offset.store", "false"),
            ("auto.offset.reset", "earliest"),
            ("isolation.level", "read_committed"),
            ("fetch.wait.max.ms", "0"),
            ("fetch.min.bytes", "1"),
        ];
        let mut seekable_consumer_config = kafka_config(
            kafka_options,
            consumer_options,
            log_level,
            CONSUMER_SETTINGS,
            true,
            "consumer",
        )?;
        let mut data_consumer_config = seekable_consumer_config.clone();
        seekable_consumer_config.set("enable.partition.eof", "true");
        data_consumer_config.set("fetch.wait.max.ms", "1000");

        const PRODUCER_SETTINGS: &[(&str, &str)] = &[
            ("acks", "all"),
            ("enable.idempotence", "true"),
            ("batch.size", "1"),
            ("batch.num.messages", "1"),
            ("retries", "5"),
            ("socket.nagle.disable", "true"),
            ("linger.ms", "0"),
        ];
        let mut producer_config = kafka_config(
            kafka_options,
            producer_options,
            log_level,
            PRODUCER_SETTINGS,
            true,
            "producer",
        )?;
        producer_config.remove("group.id");

        let admin_config = kafka_config(
            kafka_options,
            &BTreeMap::new(),
            log_level,
            &[],
            false,
            "admin",
        )?;

        Ok(Self {
            seekable_consumer_config,
            data_consumer_config,
            producer_config,
            admin_config,
        })
    }
}

/// Provides access to the `KafkaClient` inside Kafka consumers and producers.
trait AsKafkaClient<C: ClientContext> {
    /// Returns this type's Kafka client.
    fn as_kafka_client(&self) -> &KafkaClient<C>;
}

impl<C: ClientContext + ConsumerContext> AsKafkaClient<C> for BaseConsumer<C> {
    fn as_kafka_client(&self) -> &KafkaClient<C> {
        self.client()
    }
}

impl<C: ClientContext + ConsumerContext> AsKafkaClient<C> for Arc<BaseConsumer<C>> {
    fn as_kafka_client(&self) -> &KafkaClient<C> {
        self.client()
    }
}

impl<C: ClientContext + ProducerContext> AsKafkaClient<C> for ThreadedProducer<C> {
    fn as_kafka_client(&self) -> &KafkaClient<C> {
        self.client()
    }
}

/// Client, topic, and partition.
///
/// [`rdkafka`] uses these three pieces together for a lot of calls, without
/// providing a type to bind them together.  This helps.
struct Ctp<'a, T, C>
where
    T: AsKafkaClient<C>,
    C: ClientContext,
{
    client: &'a T,
    topic: &'a str,
    partition: i32,
    _marker: PhantomData<C>,
}

impl<'a, T, C> Ctp<'a, T, C>
where
    T: AsKafkaClient<C>,
    C: ClientContext,
{
    /// Returns a new `Ctp` for `client`, `topic`, and `partition`.
    fn new(client: &'a T, topic: &'a str, partition: i32) -> Ctp<'a, T, C> {
        Self {
            client,
            partition,
            topic,
            _marker: PhantomData,
        }
    }

    /// Fetches the watermarks for this client, topic, and particular, and
    /// returns them as a `Range`.
    fn fetch_watermarks<W>(&self, timeout: W) -> KafkaResult<Range<i64>>
    where
        W: Into<Timeout>,
    {
        self.client
            .as_kafka_client()
            .fetch_watermarks(self.topic, self.partition, timeout)
            .map(|(low, high)| {
                assert!(high >= low);
                low..high
            })
    }

    /// Fetches the watermarks for this client, topic, and particular, and
    /// returns them as a `Range`.
    ///
    /// This method will retry forever for errors that typically indicate that a
    /// topic was just created and not yet available for use.
    fn fetch_watermarks_patiently(&self) -> KafkaResult<Range<i64>> {
        loop {
            match self.fetch_watermarks(None) {
                Ok(watermarks) => return Ok(watermarks),
                Err(error)
                    if error.rdkafka_error_code()
                        == Some(RDKafkaErrorCode::NotLeaderForPartition) => {}
                Err(error)
                    if error.rdkafka_error_code() == Some(RDKafkaErrorCode::UnknownPartition) => {}
                Err(error) => return Err(error),
            }
        }
    }
}

/// Consumer, topic, and partition.
impl<'a, T: Consumer<C> + AsKafkaClient<C>, C: ClientContext + ConsumerContext> Ctp<'a, T, C> {
    /// Assigns this consumer to read this topic and partition starting at
    /// `offset`.
    ///
    /// For consuming a single partition, this has the effect of a Kafka seek
    /// operation.  However, a seek operation only works after an assign
    /// operation, and only if there was a poll operation in between, whereas
    /// assign always works.
    fn assign(&self, offset: i64) -> KafkaResult<()> {
        let assignment =
            make_topic_partition_list([(self.topic, self.partition, Offset::Offset(offset))])?;
        self.client.assign(&assignment)
    }
}

impl<'a, C: ClientContext + ConsumerContext> Ctp<'a, BaseConsumer<C>, C> {
    /// Finds and returns the position for `self`.
    fn position(&self) -> KafkaResult<i64> {
        let list = self.client.position().map_err(|error| {
            warn!("Failed to obtain position for {self} ({error})");
            error
        })?;
        let elem = list
            .find_partition(self.topic, self.partition)
            .ok_or_else(|| {
                warn!("Client lacks position for {self}");
                KafkaError::OffsetFetch(RDKafkaErrorCode::UnknownTopicOrPartition)
            })?;
        match elem.offset() {
            Offset::Offset(offset) => Ok(offset),
            other => {
                warn!("Client reports invalid position {other:?} for {self}");
                Err(KafkaError::OffsetFetch(RDKafkaErrorCode::InvalidArgument))
            }
        }
    }

    /// Reads a message from `self`.  Returns the message.
    ///
    /// Occasionally, librdkafka does something really weird.  It hangs without
    /// ever returning either a message or an EOF or other error.  I don't know
    /// why it does this.  In this case, librdkafka can't even report the
    /// current position for `self`.  This code detects the problem and recovers
    /// by seeking to `start_offset` and trying again, which in practice works
    /// OK.
    fn read_toward_end(&self, start_offset: i64) -> KafkaResult<BorrowedMessage<'a>> {
        let timeout = Duration::from_millis(100);
        for loops in 0u128.. {
            if let Some(result) = self.client.poll(timeout) {
                return result;
            }
            if loops > 50 && loops.is_power_of_two() {
                // Never seen yet in practice.
                error!(
                    "Waited over {} ms for librdkafka to read a message",
                    timeout.as_millis() * loops
                );
            }

            if self.position().is_err() {
                warn!("Can't get current position for {self}, starting over from offset {start_offset}");
                self.assign(start_offset)?;
            }
        }
        unreachable!();
    }
    fn read_at_offset(&self, offset: i64) -> AnyResult<BorrowedMessage<'a>> {
        self.assign(offset)?;
        self.client
            .poll(None)
            .expect("poll(None) should always return a message or an error")
            .map_err(|err| err.into())
    }

    // Read the last message in the partition, which has the given `watermarks`.
    // The consumer should have `enable.partition.eof` set to `true`.
    //
    // This is harder than it seems because the high watermark probably points
    // to a Kafka "control record" that indicates the end of a transaction.  In
    // fact, if transactions were aborted, there can be any number of these.  So
    // we have to try reading earlier offsets too.  We step backward at an
    // exponentially growing rate to allow Kafka to do some of the work for us.
    fn read_last_message(&self, watermarks: &Range<i64>) -> AnyResult<Option<BorrowedMessage<'a>>> {
        if watermarks.is_empty() {
            return Ok(None);
        }

        let mut offset = watermarks.end - 1;
        let mut delta = 1;
        loop {
            self.assign(offset)?;

            // Read messages until we get an error.  Retain the last message we
            // read.
            let mut last_message = None;
            loop {
                match self.read_toward_end(offset) {
                    Ok(message) => last_message = Some(message),
                    Err(KafkaError::PartitionEOF(p)) if p == self.partition => break,
                    Err(error) => return Err(error.into()),
                }
            }

            // Return the message if we got one.
            if let Some(message) = last_message {
                return Ok(Some(message));
            }

            // Step backward.
            if offset == watermarks.start {
                return Ok(None);
            }
            offset = offset.saturating_sub(delta).max(watermarks.start);
            delta = delta.saturating_mul(2);
        }
    }
}

impl<'a, T, C> Display for Ctp<'a, T, C>
where
    T: AsKafkaClient<C>,
    C: ClientContext,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "topic {} partition {}", self.topic, self.partition)
    }
}

/// Returns the number of partitions in `topic`.  A topic that exists always has
/// at least one partition.
///
/// This function doesn't retry failed calls because it is currently used
/// only early on in endpoint initialization.  Limiting retries at
/// initialization time is useful to make sure that the configuration is
/// correct.
fn count_partitions_in_topic<C: ConsumerContext>(
    consumer: &impl Consumer<C>,
    topic: &str,
) -> AnyResult<usize> {
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .with_context(|| format!("Failed to read metadata for topic {topic}"))?;
    let Some(metadata_topic) = metadata.topics().first() else {
        // Should not happen: if `topic` doesn't exist, the server should
        // tell us that.
        bail!("Kafka server returned no results for {topic}")
    };
    if let Some(error) = metadata_topic.error() {
        Err(KafkaError::MetadataFetch(error.into()))
            .with_context(|| format!("Error reading metadata for topic {topic}"))?;
    }
    if metadata_topic.partitions().is_empty() {
        bail!("Kafka server reports {topic} has zero partitions but it should have at least one");
    }
    debug!(
        "{topic} has {} partitions",
        metadata_topic.partitions().len()
    );
    Ok(metadata_topic.partitions().len())
}

/// Returns a `TopicPartitionList` that contains `elements`.
fn make_topic_partition_list<'a>(
    elements: impl IntoIterator<Item = (&'a str, i32, Offset)>,
) -> KafkaResult<TopicPartitionList> {
    let mut list = TopicPartitionList::new();
    for (topic, partition, offset) in elements {
        list.add_partition_offset(topic, partition, offset)?;
    }
    Ok(list)
}

impl<'a, C: ClientContext + ConsumerContext> Ctp<'a, Arc<BaseConsumer<C>>, C> {
    fn split_partition_queue(&self) -> AnyResult<PartitionQueue<C>> {
        self.client
            .split_partition_queue(self.topic, self.partition)
            .ok_or_else(|| anyhow!("Failed to split {self} from consumer"))
    }
}

fn check_fatal_errors<C: ClientContext>(client: &KafkaClient<C>) -> AnyResult<()> {
    if let Some((_errcode, errstr)) = client.fatal_error() {
        Err(AnyError::msg(errstr))
    } else {
        Ok(())
    }
}

struct ErrorHandler<'a, C>
where
    C: ClientContext,
{
    client: &'a KafkaClient<C>,
}

impl<'a, C> ErrorHandler<'a, C>
where
    C: ClientContext,
{
    fn new(client: &'a KafkaClient<C>) -> Self {
        Self { client }
    }

    /// Calls `f` until it returns success or a fatal error and returns the
    /// result.  For non-fatal errors, refines the error with `client` and
    /// reports it and calls `f` again.  Periodically calls `check_exit` to see
    /// if there's an exit request, and if so then passes it on.
    fn retry_errors<T, F, CE, CEF, CET>(
        &self,
        f: F,
        client: &KafkaClient<C>,
        check_exit: CE,
    ) -> AnyResult<T>
    where
        F: Fn() -> KafkaResult<T>,
        CE: Fn() -> Result<CET, CEF>,
        CEF: Into<AnyError> + Send + Sync + StdError + 'static,
    {
        loop {
            match f() {
                Ok(result) => return Ok(result),
                Err(error)
                    if error.rdkafka_error_code() == Some(RDKafkaErrorCode::OperationTimedOut) => {}
                Err(error) => match refine_kafka_error(client, error) {
                    (true, error) => return Err(error),
                    (false, error) => info!("Kafka non-fatal error: {error}"),
                },
            }
            check_exit().map_err(|e| AnyError::from(e))?;
        }
    }

    /// Checks for errors.  Passes non-fatal errors to the error callback and
    /// returns fatal ones.
    fn check_errors(&self) -> AnyResult<()> {
        check_fatal_errors(self.client)
    }

    /// Calls `consumer.poll`, as must be done periodically, and checks for
    /// errors reported to consumer and producer contexts.  Periodically calls
    /// `check_exit` to see if there's an exit request, and if so then passes it
    /// on.
    fn poll_consumer<CE, CET, CEF>(
        &self,
        consumer: &BaseConsumer<C>,
        check_exit: CE,
    ) -> AnyResult<()>
    where
        C: ConsumerContext,
        CE: Fn() -> Result<CET, CEF>,
        CEF: Into<AnyError> + Send + Sync + StdError + 'static,
    {
        self.check_errors()?;

        // Call `consumer.poll`.
        match self.retry_errors(
            || consumer.poll(Duration::ZERO).transpose(),
            consumer.client(),
            check_exit,
        )? {
            None => Ok(()),
            Some(_message) => Err(anyhow!(
                "Partition queues should be split off for all subscribed partitions"
            )),
        }
    }

    /// Reads a message from `consumer`, blocking if necessary.  Returns a
    /// message or fatal error.  Periodically calls `check_exit` to see if
    /// there's an exit request, and if so then passes it on.
    fn read_partition_queue<CE, CET, CEF>(
        &self,
        consumer: &BaseConsumer<C>,
        partition: &'a PartitionQueue<C>,
        check_exit: CE,
    ) -> AnyResult<BorrowedMessage<'a>>
    where
        C: ConsumerContext,
        CE: Fn() -> Result<CET, CEF>,
        CEF: Into<AnyError> + Send + Sync + StdError + 'static,
    {
        loop {
            self.poll_consumer(consumer, &check_exit)?;
            if let Some(result) = self.retry_errors(
                || partition.poll(POLL_TIMEOUT).transpose(),
                consumer.client(),
                &check_exit,
            )? {
                return Ok(result);
            }
        }
    }
}

struct DataConsumerContext<F>
where
    F: Fn(AnyError) + Send + Sync,
{
    error_cb: F,
    deferred_logging: DeferredLogging,
}

impl<F> DataConsumerContext<F>
where
    F: Fn(AnyError) + Send + Sync,
{
    fn new(error_cb: F) -> Self {
        Self {
            error_cb,
            deferred_logging: DeferredLogging::new(),
        }
    }
}

impl<F> ClientContext for DataConsumerContext<F>
where
    F: Fn(AnyError) + Send + Sync,
{
    fn error(&self, error: KafkaError, reason: &str) {
        let fatal = error
            .rdkafka_error_code()
            .is_some_and(|code| code == RDKafkaErrorCode::Fatal);
        if !fatal {
            (self.error_cb)(anyhow!(reason.to_string()));
        } else {
            // The caller will detect this later and bail out with it as its
            // final action.
        }
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }
}

impl<F> ConsumerContext for DataConsumerContext<F> where F: Fn(AnyError) + Send + Sync {}
