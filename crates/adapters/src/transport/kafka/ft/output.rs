use crate::transport::kafka::{
    OauthbearerAuth, buffered_bytes, build_headers, generate_oauthbearer_token, kafka_send,
    resolve_oauthbearer_auth,
};
use crate::util::MemoryUseReporter;
use crate::{AsyncErrorCallback, OutputEndpoint, transport::kafka::DeferredLogging};
use anyhow::{Context, Error as AnyError, Result as AnyResult, anyhow, bail};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::transport::kafka::KafkaOutputConfig;
use rdkafka::client::OAuthToken;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::{
    ClientConfig, ClientContext, Message,
    config::FromClientConfigAndContext,
    consumer::BaseConsumer,
    error::KafkaError,
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Mutex;
use std::{cmp::max, sync::RwLock, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::span::EnteredSpan;
use tracing::{debug, info, info_span, warn};

use super::{CommonConfig, Ctp, count_partitions_in_topic};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_000_000;

/// Header that carries the [`OutputPosition`] when the caller supplies the
/// message key.  Keyless messages store the position as the message key
/// instead, so the header is only written for keyed messages.
///
/// A keyed message carries this header first, ahead of any configured or
/// per-message headers, and [`OutputPosition::from_message`] takes the first
/// match, so a user header with the same name cannot shadow it.
pub(crate) const POSITION_HEADER: &str = "__feldera_position";

/// Max metadata overhead added by Kafka to each message.  Useful payload size
/// plus this overhead must not exceed `message.max.bytes`.
// This value was established empirically.
const MAX_MESSAGE_OVERHEAD: usize = 64;

/// State of the `KafkaOutputEndpoint`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum State {
    /// Just created.
    New,

    /// `connect` has been called.
    Connected,

    /// `batch_start_step()` has been called.  The next call to `push_buffer()`
    /// will write at position `.0`.
    BatchOpen(OutputPosition),

    /// `batch_end` has been called for transaction `.0`.
    BatchClosed(u64),
}

/// A position in the output partition.
///
/// A keyless message stores this as the Kafka message key.  A keyed message
/// keeps the caller's key and stores this in the [`POSITION_HEADER`] header.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OutputPosition {
    /// The transaction number.
    ///
    /// Exactly-once dedup keys on the transaction (not the physical step):
    /// a transaction produces output once per output stream, and the
    /// transaction number is reproduced deterministically across replay,
    /// whereas the step at which a commit lands is not (streaming exchange
    /// makes the commit-step count vary between the original run and replay).
    transaction: u64,

    /// An index within the transaction's output.  The first message has
    /// substep 0, the second has substep 1, and so on.
    ///
    /// For a keyless message, the substep number gives every message a unique
    /// key, so compaction never removes one.
    ///
    /// Keyed messages repeat keys, so compaction can remove an older message
    /// for a key, but it always keeps the newest message for each key.  The
    /// newest message in a partition is the newest for its own key, so the
    /// message that recovery reads survives compaction.  The exception is a
    /// keyed tombstone (a message without a value), which compaction
    /// eventually removes too.  That is why [`KafkaOutputEndpoint::push_key`]
    /// refuses keyed messages without a value.
    substep: u64,
}

impl OutputPosition {
    fn from_message<M>(msg: &M) -> AnyResult<OutputPosition>
    where
        M: Message,
    {
        // Keyed messages carry the position in a header; keyless messages
        // (and messages written before keyed output was supported) carry it
        // as the message key.
        if let Some(header) = msg
            .headers()
            .and_then(|headers| headers.iter().find(|header| header.key == POSITION_HEADER))
        {
            return Ok(serde_json::from_slice(header.value.unwrap_or(&[]))?);
        }
        Ok(serde_json::from_slice(msg.key().unwrap_or(&[]))?)
    }
}

pub struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<DataProducerContext>,
    topic: String,
    headers: OwnedHeaders,
    next_partition: usize,
    n_partitions: usize,
    max_message_size: usize,
    next_transaction: u64,
    state: State,
    /// `ControllerInner::shutdown_token`, which ends a wait on a full
    /// producer queue.  See [kafka_send].
    shutdown: CancellationToken,
}

pub fn span(topic: &str) -> EnteredSpan {
    info_span!("kafka_output", ft = true, topic = String::from(topic)).entered()
}

impl KafkaOutputEndpoint {
    pub fn new(config: KafkaOutputConfig, shutdown: CancellationToken) -> AnyResult<Self> {
        let _guard = span(&config.topic);
        let ft = config.clone().fault_tolerance.unwrap_or_default();
        let mut common = CommonConfig::new(
            &config.kafka_options,
            &ft.consumer_options,
            &ft.producer_options,
            config.log_level,
        )?;
        common
            .producer_config
            .set("transactional.id", &config.topic);

        let message_max_bytes = common
            .producer_config
            .get("message.max.bytes")
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
        if message_max_bytes <= MAX_MESSAGE_OVERHEAD {
            bail!(
                "Invalid setting 'message.max.bytes={message_max_bytes}'. 'message.max.bytes' must be greater than {MAX_MESSAGE_OVERHEAD}"
            );
        }

        let max_message_size = message_max_bytes - MAX_MESSAGE_OVERHEAD;
        debug!(
            "Configured max message size: {max_message_size} ('message.max.bytes={message_max_bytes}')"
        );

        // Initialize our producer.
        //
        // This makes first contact with the broker and gives up after a
        // timeout.  After this, Kafka will retry indefinitely, but limiting the
        // time for initialization is useful to make sure that the configuration
        // is correct.
        //
        // Since we initialize transactions, this has the effect of achieving
        // mutual exclusion with other instances of ourselves and any other
        // producers cooperating with us by using the same `transactional.id`.
        let context = DataProducerContext::new(&config)?;
        let kafka_producer =
            ThreadedProducer::from_config_and_context(&common.producer_config, context)
                .map_err(|e| anyhow!("error creating Kafka producer: {e}"))?;
        kafka_producer
            .context()
            .deferred_logging
            .with_deferred_logging(|| {
                kafka_producer.init_transactions(Duration::from_secs(
                    config.initialization_timeout_secs.into(),
                ))
            })?;

        // Read the number of partitions and the next step number.  We do this
        // after initializing transactions to avoid a race.
        let (n_partitions, next_transaction) =
            Self::read_next_transaction(&common.seekable_consumer_config, &config)?;

        Ok(Self {
            kafka_producer,
            topic: config.topic.clone(),
            headers: build_headers(&config.headers),
            n_partitions,
            next_partition: 0,
            max_message_size,
            next_transaction,
            state: State::New,
            shutdown,
        })
    }

    /// Reads the tail of `topic` using `seekable_consumer_config`. Returns the
    /// number of partitions in `topic` and the transaction number for the next
    /// transaction to be written.
    fn read_next_transaction(
        seekable_consumer_config: &ClientConfig,
        kafka_config: &KafkaOutputConfig,
    ) -> AnyResult<(usize, u64)> {
        let topic = &kafka_config.topic;
        let context = DataConsumerContext::new(|error| warn!("{error}"), kafka_config)?;
        let consumer = BaseConsumer::from_config_and_context(seekable_consumer_config, context)?;
        let n_partitions = count_partitions_in_topic(&consumer, topic)?;
        let mut next_transaction = 0;
        for partition in 0..n_partitions {
            let ctp = Ctp::new(&consumer, topic, partition as i32);
            let watermarks = ctp
                .fetch_watermarks(None)
                .map_err(|e| anyhow!("error retrieving watermarks for topic '{topic}': {e}",))?;
            if !watermarks.is_empty() {
                if let Some(msg) = ctp.read_last_message(&watermarks)? {
                    let key = OutputPosition::from_message(&msg).with_context(|| {
                        format!(
                            "message at offset {} in {ctp} should have transaction and substep as its key or in its '{POSITION_HEADER}' header",
                            msg.offset()
                        )
                    })?;
                    next_transaction = max(next_transaction, key.transaction + 1);
                }
            } else if watermarks != (0..0) {
                // The partition is empty, but it has nonzero watermarks:
                //
                // - If it once had some content, which is now all deleted or expired, we can't
                //   continue because we need to know about at least the most recent step.
                //
                // - Maybe it has always been empty of real content, but a producer once started
                //   a transaction and either aborted it or didn't write anything, and then the
                //   segment was compacted.  We could have a heuristic for that by checking for
                //   a relatively small `high` value, e.g. <1000.
                //
                // For now, just warn.
                warn!(
                    "{ctp} is empty but has nonzero high watermark {}",
                    watermarks.end
                );
            };
        }
        Ok((n_partitions, next_transaction))
    }
}

impl OutputEndpoint for KafkaOutputEndpoint {
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        debug_assert_eq!(self.state, State::New);
        let _guard = span(&self.topic);
        self.state = State::Connected;

        *self
            .kafka_producer
            .context()
            .async_error_callback
            .write()
            .unwrap() = Some(async_error_callback);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        self.max_message_size
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        self.push_key(None, Some(buffer), &[])
    }

    fn push_key(
        &mut self,
        provided_key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let _guard = span(&self.topic);
        let State::BatchOpen(OutputPosition {
            transaction,
            substep,
        }) = self.state
        else {
            unreachable!(
                "state should be BatchOpen (not {:?}) in `push_buffer()`",
                self.state
            )
        };

        // Refuse a keyed message before it takes a substep, the same way
        // keyed messages were refused before they were supported, so that a
        // refused message affects nothing else.
        let position_json = serde_json::to_string(&OutputPosition {
            transaction,
            substep,
        })
        .unwrap();
        if let Some(key) = provided_key {
            let Some(val) = val else {
                // A keyed tombstone can be the newest message in its
                // partition, and compaction eventually removes it, so recovery
                // would read an older position and write committed output
                // again.  See the `substep` documentation.
                bail!(
                    "Kafka output transport in exactly once fault-tolerant mode does not support messages with a key but no value (tombstones), which the 'confluent_jdbc' and 'redis' formats produce for deletions. Use a format that represents a deletion with a value, such as 'debezium', or at-least-once fault tolerance."
                );
            };
            let size = key.len() + val.len() + POSITION_HEADER.len() + position_json.len();
            if size > self.max_message_size {
                bail!(
                    "Kafka message with a {}-byte key, a {}-byte value and a {}-byte position header exceeds the maximum of {} bytes ('message.max.bytes' minus {MAX_MESSAGE_OVERHEAD} bytes of overhead)",
                    key.len(),
                    val.len(),
                    POSITION_HEADER.len() + position_json.len(),
                    self.max_message_size
                );
            }
        }

        self.state = State::BatchOpen(OutputPosition {
            transaction,
            substep: substep + 1,
        });

        if transaction >= self.next_transaction {
            let mut record = if let Some(key) = provided_key {
                // With a caller-supplied key, the position rides in a header so
                // that the caller's key stays the message key, and librdkafka
                // partitions by key hash instead of our round-robin, so that
                // messages with the same key stay in order within one
                // partition (issue #7355).  The position header goes first; see
                // `POSITION_HEADER`.
                let mut all_headers = OwnedHeaders::new().insert(Header {
                    key: POSITION_HEADER,
                    value: Some(position_json.as_bytes()),
                });
                for header in self.headers.iter() {
                    all_headers = all_headers.insert(header);
                }
                for (key, value) in headers {
                    all_headers = all_headers.insert(Header { key, value: *value });
                }
                BaseRecord::to(&self.topic).key(key).headers(all_headers)
            } else {
                let mut all_headers = self.headers.clone();
                for (key, value) in headers {
                    all_headers = all_headers.insert(Header { key, value: *value });
                }
                BaseRecord::to(&self.topic)
                    .key(position_json.as_bytes())
                    .partition(self.next_partition as i32)
                    .headers(all_headers)
            };
            if let Some(val) = val {
                record = record.payload(val);
            }
            kafka_send(&self.kafka_producer, &self.topic, record, &self.shutdown)?;

            if provided_key.is_none() {
                self.next_partition += 1;
                if self.next_partition >= self.n_partitions {
                    self.next_partition = 0;
                }
            }
        }
        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        let _guard = span(&self.topic);
        let State::BatchOpen(position) = self.state else {
            unreachable!(
                "state should be BatchOpen (not {:?}) in `batch_end()`",
                self.state
            )
        };
        self.state = State::BatchClosed(position.transaction);

        if position.transaction >= self.next_transaction {
            self.kafka_producer.commit_transaction(None)?;
            self.next_transaction = position.transaction + 1;
        }
        Ok(())
    }

    fn batch_start(&mut self, transaction: u64, _batch_type: OutputBatchType) -> AnyResult<()> {
        let _guard = span(&self.topic);
        // The caller invokes `batch_start` exactly once per transaction, with
        // strictly increasing transaction numbers (the controller skips empty
        // output batches, so an in-progress step that produced no output does
        // not open a batch). That keeps each `(transaction, substep)` key
        // unique.
        let first_transaction = match self.state {
            State::New => unreachable!("connect() should be called first"),
            State::Connected => true,
            State::BatchClosed(closed_transaction) => {
                if transaction <= closed_transaction {
                    unreachable!(
                        "transaction numbers should increase, not go from {closed_transaction} to {transaction}"
                    );
                };
                false
            }
            State::BatchOpen(_) => {
                unreachable!("batch_end() should be called before the next batch_start()")
            }
        };

        if transaction >= self.next_transaction {
            if transaction > self.next_transaction {
                debug!(
                    "skipping from transaction {} to {transaction}",
                    self.next_transaction
                );
            }
            self.kafka_producer.begin_transaction()?;
        } else if first_transaction {
            info!(
                "dropping transactions {transaction}..{} that were already output in a previous run",
                self.next_transaction
            );
        }
        self.state = State::BatchOpen(OutputPosition {
            transaction,
            substep: 0,
        });
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        true
    }

    fn memory(&self) -> usize {
        self.kafka_producer
            .context()
            .memory_use_reporter
            .lock()
            .unwrap()
            .current()
    }
}

struct DataProducerContext {
    /// Callback to notify the controller about delivery failure.
    async_error_callback: RwLock<Option<AsyncErrorCallback>>,

    deferred_logging: DeferredLogging,

    oauthbearer_config: OauthbearerAuth,

    memory_use_reporter: Mutex<MemoryUseReporter>,

    topic: String,
}

impl DataProducerContext {
    fn new(kafka_config: &KafkaOutputConfig) -> AnyResult<Self> {
        let oauthbearer_config = resolve_oauthbearer_auth(
            &kafka_config.kafka_options,
            kafka_config.oauth_provider,
            kafka_config.region.clone(),
        )?;

        Ok(Self {
            async_error_callback: RwLock::new(None),
            deferred_logging: DeferredLogging::new(),
            oauthbearer_config,
            topic: kafka_config.topic.clone(),
            memory_use_reporter: Mutex::new(MemoryUseReporter::new("buffers", 1024 * 1024)),
        })
    }
}

impl ClientContext for DataProducerContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        if let Some(cb) = self.async_error_callback.read().unwrap().as_ref() {
            let fatal = error
                .rdkafka_error_code()
                .is_some_and(|code| code == RDKafkaErrorCode::Fatal);
            cb(
                fatal,
                anyhow!("Kafka producer error: {error}; Reason: {reason}"),
                Some("kafka_ft_err"),
            );
        } else {
            warn!("{error}");
        }
    }

    fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        generate_oauthbearer_token(&self.oauthbearer_config)
    }

    fn stats(&self, statistics: rdkafka::Statistics) {
        let _guard = span(&self.topic);
        self.memory_use_reporter
            .lock()
            .unwrap()
            .update(buffered_bytes(&statistics));
    }
}

impl ProducerContext for DataProducerContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        if let Err((error, _message)) = delivery_result
            && let Some(cb) = self.async_error_callback.read().unwrap().as_ref()
        {
            cb(
                false,
                AnyError::new(error.clone()),
                Some("kafka_ft_delivery"),
            );
        }
    }
}

struct DataConsumerContext<F>
where
    F: Fn(AnyError) + Send + Sync,
{
    error_cb: F,
    deferred_logging: DeferredLogging,
    oauthbearer_config: OauthbearerAuth,
    memory_use_reporter: Mutex<MemoryUseReporter>,
    topic: String,
}

impl<F> DataConsumerContext<F>
where
    F: Fn(AnyError) + Send + Sync,
{
    fn new(error_cb: F, kafka_config: &KafkaOutputConfig) -> AnyResult<Self> {
        let oauthbearer_config = resolve_oauthbearer_auth(
            &kafka_config.kafka_options,
            kafka_config.oauth_provider,
            kafka_config.region.clone(),
        )?;

        Ok(Self {
            error_cb,
            deferred_logging: DeferredLogging::new(),
            oauthbearer_config,
            topic: kafka_config.topic.clone(),
            memory_use_reporter: Mutex::new(MemoryUseReporter::new("buffers", 1024 * 1024)),
        })
    }
}

impl<F> ClientContext for DataConsumerContext<F>
where
    F: Fn(AnyError) + Send + Sync,
{
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

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

    fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        generate_oauthbearer_token(&self.oauthbearer_config)
    }

    fn stats(&self, statistics: rdkafka::Statistics) {
        let _guard = span(&self.topic);
        self.memory_use_reporter
            .lock()
            .unwrap()
            .update(buffered_bytes(&statistics));
    }
}

impl<F> ConsumerContext for DataConsumerContext<F> where F: Fn(AnyError) + Send + Sync {}
