use crate::transport::kafka::metrics::KafkaOutputMetrics;
use crate::transport::kafka::{
    OauthbearerAuth, SendError, buffered_bytes, generate_oauthbearer_token, kafka_send,
    resolve_oauthbearer_auth,
};
use crate::util::MemoryUseReporter;
use crate::{OutputEndpoint, OutputEndpointCallbacks, transport::kafka::DeferredLogging};
use anyhow::{Context, Error as AnyError, Result as AnyResult, anyhow, bail};
use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::transport::kafka::KafkaOutputConfig;
use rdkafka::client::OAuthToken;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::{
    ClientConfig, ClientContext, Message,
    config::FromClientConfigAndContext,
    consumer::BaseConsumer,
    error::KafkaError,
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeSet;
use std::error::Error;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::span::EnteredSpan;
use tracing::{debug, error, info, info_span, warn};

use super::spool::{ReplaySpool, SpooledRecord};
use super::{CommonConfig, Ctp, count_partitions_in_topic};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_000_000;

/// Max metadata overhead added by Kafka to each message.  Useful payload size
/// plus this overhead must not exceed `message.max.bytes`.
// This value was established empirically.
const MAX_MESSAGE_OVERHEAD: usize = 64;

/// Memory the replay spool holds before it spills to a temporary file.
const SPOOL_MEMORY_LIMIT: usize = 64 << 20;

/// Longest wait for one transaction API call. A call that times out returns a
/// retriable error and the next call resumes it, so this bounds the shutdown
/// latency of the commit loop.
const TRANSACTION_API_TIMEOUT: Duration = Duration::from_secs(10);

/// Backoff between attempts in the commit loop.
const MIN_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// How often a wait looks at the shutdown token.
const SHUTDOWN_POLL: Duration = Duration::from_millis(100);

/// A wait on a full producer queue this long makes the sink unhealthy.
const QUEUE_STALL_TIMEOUT: Duration = Duration::from_secs(5);

/// Aborts in a row that a delivery timeout does not explain, and the time
/// they must span, before the sink reports them as a connector error. See
/// [`KafkaOutputEndpoint::note_abort`].
const REFUSAL_REPORT_ABORTS: u32 = 3;
const REFUSAL_REPORT_DELAY: Duration = Duration::from_secs(5);

/// `transaction.timeout.ms` when the configuration does not set it.
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Metrics of the exactly-once sink: the shared Kafka output metrics plus the
/// transaction recovery counters.
#[derive(Default)]
pub(crate) struct KafkaFtOutputMetrics {
    base: KafkaOutputMetrics,
    /// Transactions aborted and replayed after a delivery failure.
    transaction_aborts: AtomicU64,
    /// Commit calls repeated after a retriable error or a timeout.
    commit_retries: AtomicU64,
    /// Bytes of the open transaction kept for replay.
    spool_bytes: AtomicU64,
}

impl ConnectorMetrics for KafkaFtOutputMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        let mut metrics = self.base.metrics();
        metrics.push((
            "kafka_output_transaction_aborts_total",
            "Transactions the exactly-once sink aborted and sent again after a delivery failure.",
            ValueType::Counter,
            self.transaction_aborts.load(Ordering::Relaxed) as f64,
        ));
        metrics.push((
            "kafka_output_commit_retries_total",
            "Transaction commits the exactly-once sink repeated after a retriable error or a timeout.",
            ValueType::Counter,
            self.commit_retries.load(Ordering::Relaxed) as f64,
        ));
        metrics.push((
            "kafka_output_replay_spool_bytes",
            "Bytes of the open transaction the exactly-once sink keeps for replay, in memory and on disk.",
            ValueType::Gauge,
            self.spool_bytes.load(Ordering::Relaxed) as f64,
        ));
        metrics
    }
}

/// One step of the commit loop. See [`KafkaOutputEndpoint::drive`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Phase {
    /// Commit the open transaction.
    Commit,
    /// Abort the open transaction, then begin a new one and replay.
    Abort,
    /// Begin a transaction. After an abort, replay the spool next.
    Begin { replay: bool },
    /// Send the spooled records of the open transaction again.
    Replay,
}

/// What librdkafka said about a transaction API error.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ErrorClass {
    /// The same call can be repeated.
    Retriable,
    /// The transaction must be aborted and its records sent again.
    Abortable,
    /// The producer is unusable, for example because another instance with
    /// the same `transactional.id` fenced it.
    Fatal,
    /// None of the above: an invalid state or argument that a retry cannot
    /// fix.
    Terminal,
}

fn classify(error: &KafkaError) -> ErrorClass {
    match error {
        KafkaError::Transaction(error) if error.is_fatal() => ErrorClass::Fatal,
        KafkaError::Transaction(error) if error.txn_requires_abort() => ErrorClass::Abortable,
        KafkaError::Transaction(error) if error.is_retriable() => ErrorClass::Retriable,
        _ => ErrorClass::Terminal,
    }
}

/// Result of handing one record to the producer.
enum SendOutcome {
    /// The record is in the producer queue.
    Sent,
    /// The open transaction cannot complete as it is: it is in an error
    /// state, or librdkafka refuses the whole topic for now, for example
    /// after an authorization error. It must be aborted and replayed.
    NeedsAbort(KafkaError),
    /// librdkafka refused this record for a reason a retry cannot fix, for
    /// example its size.
    Refused(AnyError),
    /// The producer is unusable.
    Fatal(KafkaError),
    /// The pipeline is shutting down.
    Shutdown(AnyError),
}

/// Hands `record` to `producer`, waiting out a full queue, and sorts the
/// result for the commit loop.
fn send_record(
    producer: &ThreadedProducer<DataProducerContext>,
    topic: &str,
    shutdown: &CancellationToken,
    record: &SpooledRecord,
) -> SendOutcome {
    if shutdown.is_cancelled() {
        return SendOutcome::Shutdown(anyhow!(
            "gave up sending to Kafka topic '{topic}' because the pipeline is shutting down"
        ));
    }
    let context = producer.context();
    let mut headers = OwnedHeaders::new_with_capacity(record.headers.len());
    for (key, value) in &record.headers {
        headers = headers.insert(Header {
            key,
            value: value.as_deref(),
        });
    }
    let mut base = BaseRecord::<[u8], [u8], ()>::to(topic)
        .partition(record.partition)
        .headers(headers);
    if let Some(key) = &record.key {
        base = base.key(key.as_slice());
    }
    if let Some(payload) = &record.payload {
        base = base.payload(payload.as_slice());
    }
    let mut count_wait = context.metrics.base.count_queue_full_wait();
    let result = kafka_send(producer, topic, base, shutdown, &mut |waited| {
        count_wait(waited);
        if waited >= QUEUE_STALL_TIMEOUT {
            context.report_unhealthy(&format!(
                "the producer queue for topic '{topic}' has been full for {waited:.0?}: the broker is not keeping up"
            ));
        }
    });
    match result {
        Ok(_) => SendOutcome::Sent,
        Err(error) if shutdown.is_cancelled() => SendOutcome::Shutdown(error),
        Err(error) => match error.downcast_ref::<SendError>().map(|e| &e.error) {
            Some(KafkaError::MessageProduction(RDKafkaErrorCode::Fatal)) => {
                SendOutcome::Fatal(KafkaError::MessageProduction(RDKafkaErrorCode::Fatal))
            }
            // A refusal that is about this record alone. Every other produce
            // error is about the transaction or the topic: librdkafka keeps
            // a topic's last permanent error until the next metadata refresh
            // and refuses every record for it meanwhile, which a replay must
            // wait out rather than drop the records.
            Some(KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageSizeTooLarge | RDKafkaErrorCode::InvalidArgument,
            )) => SendOutcome::Refused(error),
            Some(KafkaError::MessageProduction(code)) => {
                SendOutcome::NeedsAbort(KafkaError::MessageProduction(*code))
            }
            _ => SendOutcome::Refused(error),
        },
    }
}

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
/// This is stored as the Kafka message key.
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
    /// We don't have an a priori need to store the substep number in the Kafka
    /// message key, but the substep number allows us to have a unique key for
    /// every message.  That is valuable because Kafka can be configured to
    /// deduplicate messages based on key and we do not want to lose data in
    /// that case.
    substep: u64,
}

impl OutputPosition {
    fn from_message<M>(msg: &M) -> AnyResult<OutputPosition>
    where
        M: Message,
    {
        Ok(serde_json::from_slice(msg.key().unwrap_or(&[]))?)
    }
}

pub struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<DataProducerContext>,
    topic: String,
    /// Headers from the connector configuration, added to every record.
    configured_headers: Vec<(String, Option<Vec<u8>>)>,
    next_partition: usize,
    n_partitions: usize,
    max_message_size: usize,
    next_transaction: u64,
    state: State,
    /// `ControllerInner::shutdown_token`, which ends a wait on a full
    /// producer queue and the commit loop.  See [kafka_send].
    shutdown: CancellationToken,
    /// The records of the open transaction, for replay after an abort. See
    /// [`ReplaySpool`] for why a retry of the transaction is not enough.
    spool: ReplaySpool,
    /// Indexes of spooled records that a replay found refused, so that a later
    /// replay skips them and reports each once.
    refused: BTreeSet<u64>,
    /// Set when a send found the open transaction in an error state. The
    /// remaining sends of the batch are skipped and `batch_end` recovers.
    needs_abort: bool,
    /// The transaction error behind the next abort, for [`Self::note_abort`].
    abort_reason: Option<KafkaError>,
    /// Aborts in a row that a delivery timeout does not explain.
    refusals: RefusalStreak,
    /// When the current attempt at the open transaction began.
    attempt_started: Instant,
    /// `transaction.timeout.ms`, named in the abort report when an attempt
    /// outlived it.
    transaction_timeout: Duration,
    metrics: Arc<KafkaFtOutputMetrics>,
}

/// Aborts in a row that a delivery timeout does not explain. See
/// [`KafkaOutputEndpoint::note_abort`].
#[derive(Default)]
struct RefusalStreak {
    /// When the first abort of the streak completed.
    since: Option<Instant>,
    aborts: u32,
    /// The open transaction's streak was reported; once is enough.
    reported: bool,
}

/// What to do about a refusal, by its error code.
fn refusal_hint(error: &KafkaError, attempt: Duration, transaction_timeout: Duration) -> String {
    use RDKafkaErrorCode::*;
    let timeout_hint = || {
        format!(
            "the attempt took {attempt:.0?} and transaction.timeout.ms is {transaction_timeout:?}; raise it in the connector's producer options, up to the broker's transaction.max.timeout.ms"
        )
    };
    match error.rdkafka_error_code() {
        Some(MessageSizeTooLarge | InvalidMessageSize | MessageBatchTooLarge) => {
            "a record or a batch is larger than the topic's max.message.bytes; set message.max.bytes in the connector below the topic limit or raise the topic limit".to_owned()
        }
        Some(
            TopicAuthorizationFailed | ClusterAuthorizationFailed | TransactionalIdAuthorizationFailed,
        ) => "check the permissions of the producer on the topic and the transactional.id".to_owned(),
        Some(UnknownTopicOrPartition) => "check that the topic exists".to_owned(),
        Some(InvalidRecord | InvalidMessage) => {
            "the broker rejects the records; check the topic's record validation and compaction settings".to_owned()
        }
        Some(InvalidProducerEpoch) => timeout_hint(),
        _ if attempt >= transaction_timeout => timeout_hint(),
        _ => "check the broker logs".to_owned(),
    }
}

pub fn span(topic: &str) -> EnteredSpan {
    info_span!("kafka_output", ft = true, topic = String::from(topic)).entered()
}

impl KafkaOutputEndpoint {
    /// Creates the sink. `spill_dir` is where the replay spool goes once it
    /// outgrows its memory limit: the pipeline's storage directory, or the
    /// system temporary directory when `None`.
    pub fn new(
        config: KafkaOutputConfig,
        shutdown: CancellationToken,
        spill_dir: Option<PathBuf>,
    ) -> AnyResult<Self> {
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
        let transaction_timeout = common
            .producer_config
            .get("transaction.timeout.ms")
            .and_then(|s| s.parse().ok())
            .map_or(DEFAULT_TRANSACTION_TIMEOUT, Duration::from_millis);

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
        let metrics = Arc::new(KafkaFtOutputMetrics::default());
        let context = DataProducerContext::new(&config, metrics.clone())?;
        let kafka_producer =
            ThreadedProducer::from_config_and_context(&common.producer_config, context)
                .map_err(|e| anyhow!("error creating Kafka producer: {e}"))?;
        kafka_producer
            .context()
            .deferred_logging
            .with_deferred_logging(|| {
                kafka_producer.init_transactions(Duration::from(config.initialization_timeout()))
            })?;

        // Read the number of partitions and the next step number.  We do this
        // after initializing transactions to avoid a race.
        let (n_partitions, next_transaction) =
            Self::read_next_transaction(&common.seekable_consumer_config, &config)?;

        Ok(Self {
            kafka_producer,
            topic: config.topic.clone(),
            configured_headers: config
                .headers
                .iter()
                .map(|header| {
                    (
                        header.key.clone(),
                        header.value.as_ref().map(|value| value.0.clone()),
                    )
                })
                .collect(),
            n_partitions,
            next_partition: 0,
            max_message_size,
            next_transaction,
            state: State::New,
            shutdown,
            spool: ReplaySpool::new(SPOOL_MEMORY_LIMIT, spill_dir),
            refused: BTreeSet::new(),
            needs_abort: false,
            abort_reason: None,
            refusals: RefusalStreak::default(),
            attempt_started: Instant::now(),
            transaction_timeout,
            metrics,
        })
    }

    fn context(&self) -> &DataProducerContext {
        self.kafka_producer.context()
    }

    /// Sleeps for `duration`, or less if the pipeline shuts down.
    fn wait(&self, duration: Duration) {
        let mut remaining = duration;
        while !remaining.is_zero() && !self.shutdown.is_cancelled() {
            let slice = remaining.min(SHUTDOWN_POLL);
            sleep(slice);
            remaining -= slice;
        }
    }

    /// Reports a fatal producer error and holds the output thread until the
    /// pipeline shuts down. Returning would let the controller go on and drop
    /// every later record of this view; this way the pipeline stalls behind
    /// the sink, and the error and the health status say why.
    ///
    /// A fenced producer is never reinitialized: the instance that fenced it
    /// owns the `transactional.id` now, and taking it back would fence that
    /// one in turn.
    fn fail_fatally<T>(&self, description: String) -> AnyResult<T> {
        error!("{description}");
        self.context().report_fatal(&description);
        while !self.shutdown.is_cancelled() {
            sleep(SHUTDOWN_POLL);
        }
        Err(anyhow!(description))
    }

    /// Runs the commit loop from `phase` until the transaction is committed,
    /// or begun when `phase` is `Begin { replay: false }`.
    ///
    /// | Error | Action |
    /// |---|---|
    /// | retriable | repeat the call after a backoff |
    /// | abortable | abort, begin a new transaction, replay the spool, commit |
    /// | fatal | report it and hold until shutdown |
    /// | other | report a terminal error and hold until shutdown |
    fn drive(&mut self, transaction: u64, mut phase: Phase) -> AnyResult<()> {
        // A loop that only has to begin a transaction has nothing to replay
        // after an abort, and must not commit the new transaction either.
        let replay_after_abort = phase != Phase::Begin { replay: false };
        let mut backoff = MIN_BACKOFF;
        let mut attempts = 0u32;
        loop {
            if self.shutdown.is_cancelled() {
                bail!(
                    "gave up on transaction {transaction} for Kafka topic '{}' at {phase:?} because the pipeline is shutting down",
                    self.topic
                );
            }
            let next = match phase {
                Phase::Commit => self
                    .kafka_producer
                    .commit_transaction(TRANSACTION_API_TIMEOUT)
                    .map(|()| None),
                Phase::Abort => self
                    .kafka_producer
                    .abort_transaction(TRANSACTION_API_TIMEOUT)
                    .map(|()| {
                        Some(Phase::Begin {
                            replay: replay_after_abort,
                        })
                    }),
                Phase::Begin { replay } => self
                    .kafka_producer
                    .begin_transaction()
                    .map(|()| replay.then_some(Phase::Replay)),
                Phase::Replay => Ok(Some(if self.replay()? {
                    Phase::Commit
                } else {
                    Phase::Abort
                })),
            };
            if matches!(phase, Phase::Begin { .. }) && next.is_ok() {
                self.attempt_started = Instant::now();
            }
            match next {
                Ok(None) => {
                    self.context().report_healthy();
                    return Ok(());
                }
                Ok(Some(next)) => {
                    if phase == Phase::Abort {
                        self.metrics
                            .transaction_aborts
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(
                            "aborted transaction {transaction} for topic '{}'; sending its {} messages again",
                            self.topic,
                            self.spool.len()
                        );
                        self.note_abort(transaction);
                    }
                    if next == Phase::Abort {
                        // The replay found the new transaction in an error
                        // state too: the broker is still failing.
                        self.wait(backoff);
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                    phase = next;
                }
                Err(error) => {
                    attempts += 1;
                    let what = format!(
                        "{phase:?} of transaction {transaction} for Kafka topic '{}' failed: {error}",
                        self.topic
                    );
                    match classify(&error) {
                        ErrorClass::Retriable => {
                            if phase == Phase::Commit {
                                self.metrics.commit_retries.fetch_add(1, Ordering::Relaxed);
                            }
                            warn!("{what}; retrying in {backoff:?} (attempt {attempts})");
                            self.context().report_unhealthy(&format!(
                                "{what}; retrying (attempt {attempts}); check broker availability"
                            ));
                        }
                        ErrorClass::Abortable => {
                            warn!(
                                "{what}; aborting and sending its {} messages again in {backoff:?} (attempt {attempts})",
                                self.spool.len()
                            );
                            self.context().report_unhealthy(&format!(
                                "{what}; aborting and sending its {} messages again (attempt {attempts}); check broker availability",
                                self.spool.len()
                            ));
                            self.abort_reason = Some(error);
                            phase = Phase::Abort;
                        }
                        ErrorClass::Fatal => {
                            return self.fail_fatally(format!(
                                "{what}; the producer is unusable, for example because another pipeline instance with transactional.id '{}' fenced it; fix the broker or deployment problem and restart the pipeline",
                                self.topic
                            ));
                        }
                        ErrorClass::Terminal => {
                            return self.fail_fatally(format!(
                                "{what}; the transaction outcome is unknown; check the broker logs and connector configuration, then restart the pipeline"
                            ));
                        }
                    }
                    self.wait(backoff);
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    /// Accounts for a completed abort of `transaction` and reports a streak
    /// of refusals once.
    ///
    /// Deliveries that time out mean an outage, which the next replay waits
    /// out. Anything else is a refusal that no replay fixes on its own: a
    /// batch larger than the topic's `max.message.bytes`, a revoked
    /// permission, a deleted topic, or a transaction longer than
    /// `transaction.timeout.ms`. librdkafka fails every record of a batch
    /// with the broker's error, so the sink cannot single out a refused
    /// record and drops nothing. It keeps replaying, and after enough
    /// refusals in a row it reports a connector error that names the cause.
    fn note_abort(&mut self, transaction: u64) {
        let failures = self.context().take_delivery_failures();
        let reason = self.abort_reason.take();
        let refusal = failures
            .refused
            .or_else(|| (failures.timed_out == 0).then_some(reason).flatten());
        let Some(error) = refusal else {
            self.refusals.since = None;
            self.refusals.aborts = 0;
            return;
        };
        let now = Instant::now();
        let since = *self.refusals.since.get_or_insert(now);
        self.refusals.aborts += 1;
        if self.refusals.reported
            || self.refusals.aborts < REFUSAL_REPORT_ABORTS
            || now.duration_since(since) < REFUSAL_REPORT_DELAY
        {
            return;
        }
        self.refusals.reported = true;
        let hint = refusal_hint(
            &error,
            now.duration_since(self.attempt_started),
            self.transaction_timeout,
        );
        let description = format!(
            "transaction {transaction} for Kafka topic '{}' was aborted {} times in a row and cannot complete: {error}; no record is dropped and the connector retries (records kept: {}); {hint}",
            self.topic,
            self.refusals.aborts,
            self.spool.len()
        );
        error!("{description}");
        if let Some(cb) = self.context().callbacks.read().unwrap().as_ref() {
            cb.error(false, anyhow!(description), Some("kafka_ft_replay"));
        }
    }

    /// Sends the spooled records again. Returns `Ok(true)` when every record
    /// is in the producer queue and `Ok(false)` when the transaction has to
    /// be aborted again.
    fn replay(&mut self) -> AnyResult<bool> {
        self.needs_abort = false;
        let producer = &self.kafka_producer;
        let topic = &self.topic;
        let shutdown = &self.shutdown;
        let refused = &mut self.refused;
        let mut stop = None;
        let result = self.spool.replay(|index, record| {
            if refused.contains(&index) {
                return Ok(());
            }
            match send_record(producer, topic, shutdown, record) {
                SendOutcome::Sent => Ok(()),
                SendOutcome::Refused(error) => {
                    // Records appended after `needs_abort` bypass the first
                    // send, so a replay can be the first attempt that rejects
                    // them. The record stays in the spool; `refused` keeps the
                    // next replay from sending and reporting it again.
                    refused.insert(index);
                    producer.context().report_refused_record(&error);
                    Ok(())
                }
                outcome => {
                    stop = Some(outcome);
                    Err(anyhow!("replay interrupted"))
                }
            }
        });
        match stop {
            None => match result {
                Ok(()) => Ok(true),
                Err(error) => self.fail_fatally(format!(
                    "cannot read back the output kept for replay to Kafka topic '{topic}': {error:#}; check the free space and permissions of the replay spool directory {}, then restart the pipeline",
                    self.spool.location()
                )),
            },
            Some(SendOutcome::NeedsAbort(error)) => {
                let what = format!(
                    "the replay to Kafka topic '{topic}' cannot complete the transaction: {error}"
                );
                warn!("{what}; aborting and sending its records again");
                self.context().report_unhealthy(&format!(
                    "{what}; aborting and sending its records again; check broker availability and topic permissions"
                ));
                self.abort_reason = Some(error);
                Ok(false)
            }
            Some(SendOutcome::Fatal(error)) => self.fail_fatally(format!(
                "sending to Kafka topic '{topic}' failed with a fatal producer error: {error}; fix the broker or deployment problem and restart the pipeline"
            )),
            Some(SendOutcome::Shutdown(error)) => Err(error),
            Some(SendOutcome::Sent | SendOutcome::Refused(_)) => unreachable!(),
        }
    }

    /// Forgets the open transaction's records.
    fn clear_spool(&mut self) {
        self.spool.clear();
        self.refused.clear();
        self.needs_abort = false;
        self.abort_reason = None;
        self.refusals = RefusalStreak::default();
        self.context().take_delivery_failures();
        self.metrics.spool_bytes.store(0, Ordering::Relaxed);
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
                            "message at offset {} in {ctp} should have transaction and substep as key",
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
    fn connect(&mut self, callbacks: Arc<dyn OutputEndpointCallbacks>) -> AnyResult<()> {
        debug_assert_eq!(self.state, State::New);
        let _guard = span(&self.topic);
        self.state = State::Connected;

        callbacks.set_custom_metrics(self.metrics.clone());
        *self.kafka_producer.context().callbacks.write().unwrap() = Some(callbacks);
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
        if provided_key.is_some() {
            bail!(
                "Kafka output transport does not support key-value pairs when configured in exactly once fault-tolerant mode."
            );
        }

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
        self.state = State::BatchOpen(OutputPosition {
            transaction,
            substep: substep + 1,
        });

        if transaction >= self.next_transaction {
            let mut all_headers = self.configured_headers.clone();
            all_headers.extend(
                headers
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), value.map(<[u8]>::to_vec))),
            );
            let key = OutputPosition {
                transaction,
                substep,
            };
            let record = SpooledRecord {
                partition: self.next_partition as i32,
                key: Some(serde_json::to_vec(&key).unwrap()),
                payload: val.map(<[u8]>::to_vec),
                headers: all_headers,
            };
            self.next_partition += 1;
            if self.next_partition >= self.n_partitions {
                self.next_partition = 0;
            }

            if !self.needs_abort {
                // Once the transaction is lost, `batch_end` sends every
                // record again, so there is no point in sending it now.
                match send_record(&self.kafka_producer, &self.topic, &self.shutdown, &record) {
                    SendOutcome::Sent => {}
                    SendOutcome::NeedsAbort(error) => {
                        warn!(
                            "transaction {transaction} for topic '{}' is in an error state ({error}); it will be aborted and sent again at the end of the batch",
                            self.topic
                        );
                        self.needs_abort = true;
                        self.abort_reason = Some(error);
                    }
                    // A refused record is reported to the caller once, here,
                    // and is not kept: a replay would only refuse it again.
                    SendOutcome::Refused(error) => return Err(error),
                    SendOutcome::Fatal(error) => {
                        return self.fail_fatally(format!(
                            "sending to Kafka topic '{}' failed with a fatal producer error: {error}; fix the broker or deployment problem and restart the pipeline",
                            self.topic
                        ));
                    }
                    SendOutcome::Shutdown(error) => return Err(error),
                }
            }
            // Keep the record for a replay. The spool is complete only if this
            // succeeds, so an error here must not let the transaction commit.
            if let Err(error) = self.spool.push(record) {
                return self.fail_fatally(format!(
                    "cannot keep output for replay to Kafka topic '{}': {error:#}; check the free space and permissions of the replay spool directory {}, then restart the pipeline",
                    self.topic,
                    self.spool.location()
                ));
            }
            self.metrics
                .spool_bytes
                .store(self.spool.bytes(), Ordering::Relaxed);
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

        let result = if position.transaction >= self.next_transaction {
            let phase = if self.needs_abort {
                Phase::Abort
            } else {
                Phase::Commit
            };
            self.drive(position.transaction, phase).map(|()| {
                self.next_transaction = position.transaction + 1;
            })
        } else {
            Ok(())
        };
        if result.is_ok() {
            self.clear_spool();
        }
        result
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
            self.clear_spool();
            self.drive(transaction, Phase::Begin { replay: false })?;
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
        self.spool.memory_bytes()
            + self
                .kafka_producer
                .context()
                .memory_use_reporter
                .lock()
                .unwrap()
                .current()
    }
}

/// Health of the sink as last reported to the controller.
#[derive(Default)]
struct HealthState {
    unhealthy: bool,
    /// A fatal error pins the status to unhealthy.
    fatal: Option<String>,
}

struct DataProducerContext {
    /// Channel to the controller, set by `connect`.
    callbacks: RwLock<Option<Arc<dyn OutputEndpointCallbacks>>>,

    deferred_logging: DeferredLogging,

    oauthbearer_config: OauthbearerAuth,

    memory_use_reporter: Mutex<MemoryUseReporter>,

    topic: String,

    metrics: Arc<KafkaFtOutputMetrics>,

    /// Also serializes health publication across the output thread and the
    /// librdkafka callbacks.
    health: Mutex<HealthState>,

    /// Delivery failures of the open transaction since the last abort, for
    /// [`KafkaOutputEndpoint::note_abort`].
    delivery_failures: Mutex<DeliveryFailures>,
}

/// Failed deliveries, sorted into an outage and a refusal.
#[derive(Default)]
struct DeliveryFailures {
    timed_out: u64,
    /// The last delivery error that was neither a timeout nor a purge.
    refused: Option<KafkaError>,
}

impl DataProducerContext {
    fn new(
        kafka_config: &KafkaOutputConfig,
        metrics: Arc<KafkaFtOutputMetrics>,
    ) -> AnyResult<Self> {
        let oauthbearer_config = resolve_oauthbearer_auth(
            &kafka_config.kafka_options,
            kafka_config.oauth_provider,
            kafka_config.region.clone(),
        )?;

        Ok(Self {
            callbacks: RwLock::new(None),
            deferred_logging: DeferredLogging::new(),
            oauthbearer_config,
            topic: kafka_config.topic.clone(),
            memory_use_reporter: Mutex::new(MemoryUseReporter::new("buffers", 1024 * 1024)),
            metrics,
            health: Mutex::new(HealthState::default()),
            delivery_failures: Mutex::new(DeliveryFailures::default()),
        })
    }

    fn take_delivery_failures(&self) -> DeliveryFailures {
        std::mem::take(&mut *self.delivery_failures.lock().unwrap())
    }

    /// Reports a record that a replay found refused and drops.
    fn report_refused_record(&self, error: &AnyError) {
        if let Some(cb) = self.callbacks.read().unwrap().as_ref() {
            cb.error(
                false,
                anyhow!(
                    "cannot replay a record to Kafka topic '{}': {error:#}; the record is being dropped; check message limits and topic configuration",
                    self.topic
                ),
                Some("kafka_ft_replay"),
            );
        }
    }

    fn publish(&self, health: ConnectorHealth) {
        if let Some(cb) = self.callbacks.read().unwrap().as_ref() {
            cb.update_connector_health(health);
        }
    }

    /// Reports the sink unhealthy with `description`, unless a fatal error
    /// already pinned the status.
    fn report_unhealthy(&self, description: &str) {
        let mut health = self.health.lock().unwrap();
        if health.fatal.is_some() {
            return;
        }
        health.unhealthy = true;
        self.publish(ConnectorHealth::unhealthy(description));
    }

    /// Reports the sink healthy again, once per transition and never after a
    /// fatal error.
    fn report_healthy(&self) {
        let mut health = self.health.lock().unwrap();
        if health.fatal.is_some() || !health.unhealthy {
            return;
        }
        health.unhealthy = false;
        self.publish(ConnectorHealth::healthy());
    }

    /// Reports a fatal error once, as an error and as the health status.
    fn report_fatal(&self, description: &str) {
        let mut health = self.health.lock().unwrap();
        if health.fatal.is_some() {
            return;
        }
        health.fatal = Some(description.to_owned());
        health.unhealthy = true;
        if let Some(cb) = self.callbacks.read().unwrap().as_ref() {
            cb.error(true, anyhow!("{description}"), Some("kafka_ft_fatal"));
        }
        self.publish(ConnectorHealth::unhealthy(description));
    }
}

impl ClientContext for DataProducerContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        let _guard = span(&self.topic);
        if matches!(
            error.rdkafka_error_code(),
            Some(RDKafkaErrorCode::AllBrokersDown | RDKafkaErrorCode::BrokerTransportFailure)
        ) {
            // librdkafka reconnects, and the commit loop replays whatever the
            // outage breaks, so no output is lost.
            warn!(
                "Kafka producer lost its broker connection and is reconnecting: {error}; {reason}"
            );
            return;
        }
        let fatal = error
            .rdkafka_error_code()
            .is_some_and(|code| code == RDKafkaErrorCode::Fatal);
        if fatal {
            self.report_fatal(&format!(
                "Kafka producer for topic '{}' failed: {error}; {reason}; fix the broker or configuration error and restart the pipeline",
                self.topic
            ));
        } else if let Some(cb) = self.callbacks.read().unwrap().as_ref() {
            cb.error(
                false,
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
        self.metrics.base.update_from_statistics(&statistics);
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
        match delivery_result {
            Ok(_) => {
                self.metrics
                    .base
                    .delivered_messages
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err((error, _message)) => {
                // Not an output error: the transaction cannot commit with a
                // failed delivery, and the commit loop sends the record again.
                self.metrics
                    .base
                    .delivery_errors
                    .fetch_add(1, Ordering::Relaxed);
                let mut failures = self.delivery_failures.lock().unwrap();
                match error.rdkafka_error_code() {
                    Some(RDKafkaErrorCode::MessageTimedOut) => failures.timed_out += 1,
                    // An abort purges the queue: the sink's own doing.
                    Some(RDKafkaErrorCode::PurgeQueue | RDKafkaErrorCode::PurgeInflight) => {}
                    _ => failures.refused = Some(error.clone()),
                }
                drop(failures);
                debug!(
                    "a delivery to topic '{}' failed ({error}); the transaction will be sent again",
                    self.topic
                );
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

#[cfg(test)]
mod tests;
