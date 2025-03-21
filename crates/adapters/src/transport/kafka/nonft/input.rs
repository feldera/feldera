use crate::transport::kafka::PemToLocation;
use crate::transport::secret_resolver::resolve_secret;
use crate::transport::{InputEndpoint, InputQueue, InputReaderCommand, NonFtInputReaderCommand};
use crate::Parser;
use crate::{
    transport::{
        kafka::{rdkafka_loglevel_from, refine_kafka_error, DeferredLogging},
        InputReader,
    },
    InputConsumer, PipelineState, TransportInputEndpoint,
};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use atomic::Atomic;
use crossbeam::queue::ArrayQueue;
use feldera_types::program_schema::Relation;
use feldera_types::transport::kafka::KafkaInputConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::TopicPartitionList;
use rdkafka::{
    config::FromClientConfigAndContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, RebalanceProtocol},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message,
};
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::thread::{self, available_parallelism, JoinHandle, Thread};
use std::{
    collections::HashSet,
    sync::{atomic::Ordering, Arc, Mutex, Weak},
    thread::spawn,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::span::EnteredSpan;
use tracing::{debug, info_span};

/// Poll timeout must be low, as it bounds the amount of time it takes to resume the connector.
const POLL_TIMEOUT: Duration = Duration::from_millis(5);

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

// Size of the circular buffer used to pass errors from ClientContext
// to the worker thread.
const ERROR_BUFFER_SIZE: usize = 1000;

pub struct KafkaInputEndpoint {
    config: Arc<KafkaInputConfig>,
    name: String,
}

impl KafkaInputEndpoint {
    pub fn new(mut config: KafkaInputConfig, name: &str) -> AnyResult<KafkaInputEndpoint> {
        config.validate()?;
        Ok(KafkaInputEndpoint {
            config: Arc::new(config),
            name: name.to_owned(),
        })
    }
}

struct KafkaInputReader {
    _inner: Arc<KafkaInputReaderInner>,
    command_sender: UnboundedSender<NonFtInputReaderCommand>,
    poller_thread: Thread,
}

/// Client context used to intercept rebalancing events.
///
/// `rdkafka` allows consumers to register callbacks invoked on various
/// Kafka events.  We need to intercept rebalancing events, when the
/// consumer gets assigned new partitions, since these new partitions are
/// may not be in the paused/unpaused state required by the endpoint,
/// so we may need to pause or unpause them as appropriate.
///
/// See <https://github.com/edenhill/librdkafka/issues/1849> for a discussion
/// of the pause/unpause behavior.
struct KafkaInputContext {
    // We keep a weak reference to the endpoint to avoid a reference cycle:
    // endpoint->BaseConsumer->context->endpoint.
    endpoint: Mutex<Weak<KafkaInputReaderInner>>,

    deferred_logging: DeferredLogging,
}

impl KafkaInputContext {
    fn new() -> Self {
        Self {
            endpoint: Mutex::new(Weak::new()),
            deferred_logging: DeferredLogging::new(),
        }
    }
}

impl ClientContext for KafkaInputContext {
    fn error(&self, error: KafkaError, reason: &str) {
        // eprintln!("Kafka error: {error}");
        if let Some(endpoint) = self.endpoint.lock().unwrap().upgrade() {
            endpoint.push_error(error, reason);
        }
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }
}

impl ConsumerContext for KafkaInputContext {
    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if matches!(rebalance, Rebalance::Assign(_)) {
            if let Some(endpoint) = self.endpoint.lock().unwrap().upgrade() {
                endpoint.rebalanced.store(true, Ordering::Release);
            }
        }
    }
}

struct KafkaInputReaderInner {
    config: Arc<KafkaInputConfig>,
    kafka_consumer: BaseConsumer<KafkaInputContext>,
    errors: ArrayQueue<(KafkaError, String)>,
    rebalanced: AtomicBool,
}

impl KafkaInputReaderInner {
    /// The main poller thread for a Kafka input. Polls `endpoint` as long as
    /// the pipeline is running, and passes the data to `consumer`.
    #[allow(clippy::borrowed_box)]
    fn poller_thread(
        self: &Arc<Self>,
        consumer: &Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        mut command_receiver: UnboundedReceiver<NonFtInputReaderCommand>,
        queue: InputQueue,
    ) -> AnyResult<()> {
        // Figure out the number of threads based on configuration, defaults,
        // and system resources.
        let max_threads = available_parallelism().map_or(16, NonZeroUsize::get);
        let n_threads = self
            .config
            .poller_threads
            .unwrap_or(3)
            .clamp(1, max_threads);

        let mut partition_eofs = HashSet::new();
        let (feedback_sender, mut feedback_receiver) = unbounded_channel();
        let helper_state = Arc::new(Atomic::new(PipelineState::Paused));
        let mut threads: Vec<HelperThread> = Vec::with_capacity(n_threads - 1);
        let queue = Arc::new(queue);

        // Create the rest of threads (start from 1 instead of 0
        // because we're one of the threads).
        for _ in 1..n_threads {
            threads.push(HelperThread::new(
                Arc::clone(self),
                consumer.clone(),
                parser.fork(),
                Arc::clone(&helper_state),
                feedback_sender.clone(),
                queue.clone(),
            ));
        }

        let mut running = false;
        let mut kafka_paused = true;
        let mut when_paused = Instant::now();
        const PAUSE_TIMEOUT: Duration = Duration::from_millis(0);
        loop {
            loop {
                match command_receiver.try_recv() {
                    Ok(NonFtInputReaderCommand::Queue) => queue.queue(),
                    Ok(NonFtInputReaderCommand::Transition(PipelineState::Running)) => {
                        running = true;
                    }
                    Ok(NonFtInputReaderCommand::Transition(PipelineState::Paused)) => {
                        running = false;
                        when_paused = Instant::now();
                        helper_state.store(PipelineState::Paused, Ordering::Release);
                    }
                    Ok(NonFtInputReaderCommand::Transition(PipelineState::Terminated)) => {
                        return Ok(())
                    }
                    Err(TryRecvError::Disconnected) => return Ok(()),
                    Err(TryRecvError::Empty) => break,
                }
            }

            if !running && !kafka_paused && when_paused.elapsed() >= PAUSE_TIMEOUT {
                self.pause_partitions()
                    .map_err(|e| anyhow!("error pausing Kafka consumer: {e}"))?;
                kafka_paused = true;
            } else if running && kafka_paused {
                self.resume_partitions()
                    .map_err(|e| anyhow!("error resuming Kafka consumer: {e}"))?;
                helper_state.store(PipelineState::Running, Ordering::Release);
                for thread in threads.iter() {
                    thread.unpark();
                }
                kafka_paused = false;
            }

            if self.rebalanced.swap(false, Ordering::Acquire) {
                if kafka_paused {
                    self.pause_partitions().map_err(|e| {
                        anyhow!("error pausing Kafka consumer after partition rebalancing: {e}")
                    })?;
                } else {
                    self.resume_partitions().map_err(|e| {
                        anyhow!("error resuming Kafka consumer after partition rebalancing: {e}")
                    })?;
                }
            }

            // Skip polling while we're paused, even though we need to poll to
            // keep the Kafka broker connection going. We'll only do that for
            // `PAUSE_TIMEOUT` at most, which is probably OK.
            if running {
                self.poll(consumer, &mut parser, &feedback_sender, &queue);
            }

            while let Ok(feedback) = feedback_receiver.try_recv() {
                match feedback {
                    HelperFeedback::PartitionEOF(p) => {
                        // If all the partitions we're subscribed to have received
                        // an EOF, then we're done.
                        partition_eofs.insert(p);
                        if self
                            .kafka_consumer
                            .assignment()
                            .unwrap_or_default()
                            .elements()
                            .iter()
                            .map(|tpl| tpl.partition())
                            .all(|p| partition_eofs.contains(&p))
                        {
                            consumer.eoi();
                            return Ok(());
                        }
                    }
                    HelperFeedback::FatalError => return Ok(()),
                }
            }

            while let Some((error, reason)) = self.pop_error() {
                let (fatal, error) = self.refine_error(error);
                // `reason` contains a human-readable description of the
                // error.
                consumer.error(
                    fatal,
                    anyhow!(format!("Kafka consumer error: {error}; Reason: {reason}")),
                );
                if fatal {
                    return Ok(());
                }
            }
        }
    }

    /// Tries to read a message from this Kafka consumer. If successful, passes
    /// it to `consumer`. If there's a problem or an EOF, sends it to
    /// `feedback`.
    #[allow(clippy::borrowed_box)]
    fn poll(
        &self,
        consumer: &Box<dyn InputConsumer>,
        parser: &mut Box<dyn Parser>,
        feedback: &UnboundedSender<HelperFeedback>,
        queue: &InputQueue,
    ) {
        match self.kafka_consumer.poll(POLL_TIMEOUT) {
            None => (),
            Some(Err(KafkaError::PartitionEOF(p))) => {
                feedback.send(HelperFeedback::PartitionEOF(p)).unwrap()
            }
            Some(Err(e)) => {
                // println!("poll returned error");
                let (fatal, e) = self.refine_error(e);
                consumer.error(fatal, anyhow!("error polling Kafka consumer: {e}"));
                if fatal {
                    feedback.send(HelperFeedback::FatalError).unwrap();
                }
            }
            Some(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    queue.push(parser.parse(payload), payload.len());
                }
            }
        }
    }

    fn push_error(&self, error: KafkaError, reason: &str) {
        // `force_push` makes the queue operate as a circular buffer.
        self.errors.force_push((error, reason.to_string()));
    }

    fn pop_error(&self) -> Option<(KafkaError, String)> {
        self.errors.pop()
    }

    #[allow(dead_code)]
    fn debug_consumer(&self) {
        /*let topic_metadata = self.kafka_consumer.fetch_metadata(None, Duration::from_millis(1000)).unwrap();
        for topic in topic_metadata.topics() {
            println!("topic: {}", topic.name());
            for partition in topic.partitions() {
                println!("  partition: {}, leader: {}, error: {:?}, replicas: {:?}, isr: {:?}", partition.id(), partition.leader(), partition.error(), partition.replicas(), partition.isr());
            }
        }*/
        // println!("Subscription: {:?}", self.kafka_consumer.subscription());
        println!("Assignment: {:?}", self.kafka_consumer.assignment());
    }

    /// Pause all partitions assigned to the consumer.
    fn pause_partitions(&self) -> KafkaResult<()> {
        // println!("pause");
        // self.debug_consumer();

        self.kafka_consumer
            .pause(&self.kafka_consumer.assignment()?)?;
        Ok(())
    }

    /// Resume all partitions assigned to the consumer.
    fn resume_partitions(&self) -> KafkaResult<()> {
        self.kafka_consumer
            .resume(&self.kafka_consumer.assignment()?)?;
        Ok(())
    }

    fn refine_error(&self, e: KafkaError) -> (bool, AnyError) {
        refine_kafka_error(self.kafka_consumer.client(), e)
    }
}

fn span(config: &KafkaInputConfig) -> EnteredSpan {
    info_span!("kafka_input", ft = false, topics = config.topics.join(",")).entered()
}

impl KafkaInputReader {
    fn new(
        config: &Arc<KafkaInputConfig>,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        endpoint_name: &str,
    ) -> AnyResult<Self> {
        let _guard = span(config);

        // Create Kafka consumer configuration.
        debug!("Starting Kafka input endpoint: {:?}", config);

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            client_config.set(key, resolve_secret(value)?);
        }

        client_config.pem_to_location(endpoint_name)?;

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        // Context object to intercept rebalancing events and errors.
        let context = KafkaInputContext::new();

        debug!("Creating Kafka consumer");
        let inner = Arc::new(KafkaInputReaderInner {
            config: config.clone(),
            kafka_consumer: BaseConsumer::from_config_and_context(&client_config, context)
                .map_err(|e| anyhow!("error creating Kafka consumer: {e}"))?,
            errors: ArrayQueue::new(ERROR_BUFFER_SIZE),
            rebalanced: AtomicBool::new(false),
        });

        *inner.kafka_consumer.context().endpoint.lock().unwrap() = Arc::downgrade(&inner);

        let topics = config.topics.iter().map(String::as_str).collect::<Vec<_>>();

        if config.start_from.is_empty() {
            // Subscribe consumer to `topics`.
            inner
                .kafka_consumer
                .subscribe(&topics)
                .map_err(|e| anyhow!("error subscribing to Kafka topic(s): {e}"))?;
        } else {
            let mut tpl = TopicPartitionList::new();

            for start_from in config.start_from.iter() {
                let (low, high) = inner
                    .kafka_consumer
                    .fetch_watermarks(
                        &start_from.topic,
                        start_from.partition as i32,
                        METADATA_TIMEOUT,
                    )
                    .map_err(|e| {
                        anyhow!(
                            "error fetching metadata for topic '{}' and partition '{}': {e}",
                            &start_from.topic,
                            start_from.partition
                        )
                    })?;

                if !(low..=high).contains(&(start_from.offset as i64)) {
                    bail!("configuration error: provided offset '{}' not currently in topic '{}' partition '{}'", start_from.offset, start_from.topic, start_from.partition);
                }

                debug!(
                    "starting to read from topic: '{}', partition: '{}', offset: '{}'",
                    start_from.topic, start_from.partition, start_from.offset
                );
                tpl.add_partition_offset(
                    &start_from.topic,
                    start_from.partition as i32,
                    rdkafka::Offset::Offset(start_from.offset as i64),
                )
                .map_err(|e| anyhow!("error assigning partition and offset: {e}"))?;
            }

            let all_topics: HashSet<&str> = topics.iter().copied().collect();
            let specified_topics: HashSet<&str> =
                config.start_from.iter().map(|x| x.topic.as_str()).collect();

            let specifed_but_not_listed: Vec<_> =
                specified_topics.difference(&all_topics).cloned().collect();
            if !specifed_but_not_listed.is_empty() {
                let topics = specifed_but_not_listed.join(", ");
                bail!("topic(s): '{topics}' defined in 'start_from' but not in the 'topics' list");
            }

            for topic in all_topics.difference(&specified_topics) {
                let metadata = inner
                    .kafka_consumer
                    .fetch_metadata(Some(topic), METADATA_TIMEOUT)?;
                let topic = metadata.topics().first().ok_or(anyhow!(
                    "failed to subscribe to topic: '{topic}' doesn't exist"
                ))?;
                for partition in topic.partitions() {
                    tpl.add_partition(topic.name(), partition.id());
                }
            }

            inner
                .kafka_consumer
                .assign(&tpl)
                .map_err(|e| anyhow!("error assigning partition and offset: {e}"))?;
        };

        let start = Instant::now();

        let queue = InputQueue::new(consumer.clone());
        // Wait for the consumer to join the group by waiting for the group
        // rebalance protocol to be set.
        loop {
            // We don't need to rebalance if starting from a specific offset.
            if !config.start_from.is_empty() {
                debug!("start offsets specified; skipping rebalancing");
                break;
            }
            // We must poll in order to receive connection failures; otherwise
            // we'd have to rely on timeouts only.
            match inner
                .kafka_consumer
                .context()
                .deferred_logging
                .with_deferred_logging(|| inner.kafka_consumer.poll(POLL_TIMEOUT))
            {
                Some(Err(e)) => {
                    // Topic-does-not-exist error will be reported here.
                    bail!(
                        "failed to subscribe to topics '{topics:?}' (consumer group id '{}'): {e}",
                        inner.config.kafka_options.get("group.id").unwrap(),
                    );
                }
                Some(Ok(message)) => {
                    // `KafkaInputContext` should instantly pause the topic upon connecting to it.
                    // Hopefully, this guarantees that we won't see any messages from it, but if
                    // that's not the case, there shouldn't be any harm in sending them downstream.
                    if let Some(payload) = message.payload() {
                        queue.push(parser.parse(payload), payload.len());
                    }
                }
                None => (),
            }

            // Invalid broker address and other global errors are reported here.
            if let Some((_error, reason)) = inner.pop_error() {
                bail!("error subscribing to topics {topics:?}: {reason}");
            }

            if matches!(
                inner.kafka_consumer.rebalance_protocol(),
                RebalanceProtocol::None
            ) {
                if start.elapsed()
                    >= Duration::from_secs(inner.config.group_join_timeout_secs as u64)
                {
                    bail!(
                        "failed to subscribe to topics '{topics:?}' (consumer group id '{}'), giving up after {}s",
                        inner.config.kafka_options.get("group.id").unwrap(),
                        inner.config.group_join_timeout_secs
                    );
                }
                // println!("waiting to join the group");
            } else {
                break;
            }
        }

        let (command_sender, command_receiver) = unbounded_channel();
        let poller_handle = spawn({
            let endpoint = inner.clone();
            move || {
                let _guard = span(&endpoint.config);
                if let Err(e) = endpoint.poller_thread(&consumer, parser, command_receiver, queue) {
                    consumer.error(true, anyhow!("Kafka consumer polling thread terminated with the following error: {e}"));
                }
            }
        });
        let poller_thread = poller_handle.thread().clone();
        Ok(KafkaInputReader {
            _inner: inner,
            command_sender,
            poller_thread,
        })
    }
}

/// A thread that will help the main poller thread by processing messages
/// received from Kafka.
struct HelperThread {
    /// Used by the poller thread to tell us what to do.
    state: Arc<Atomic<PipelineState>>,

    /// Our own join handle so we can wait when dropped.
    join_handle: Option<JoinHandle<()>>,
}

impl HelperThread {
    fn new(
        endpoint: Arc<KafkaInputReaderInner>,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        state: Arc<Atomic<PipelineState>>,
        feedback_sender: UnboundedSender<HelperFeedback>,
        queue: Arc<InputQueue>,
    ) -> Self {
        Self {
            state: state.clone(),
            join_handle: {
                Some(spawn(move || {
                    let _guard = span(&endpoint.config);
                    loop {
                        match state.load(Ordering::Acquire) {
                            PipelineState::Paused => thread::park(),
                            PipelineState::Running => {
                                endpoint.poll(&consumer, &mut parser, &feedback_sender, &queue)
                            }
                            PipelineState::Terminated => break,
                        }
                    }
                }))
            },
        }
    }

    fn unpark(&self) {
        if let Some(h) = &self.join_handle {
            h.thread().unpark()
        }
    }
}

impl Drop for HelperThread {
    /// When we're dropped, make the thread exit.
    ///
    /// *Careful*: `state` is shared with all the helper threads, so if one gets
    /// dropped, the others will exit too. Currently this is OK because we
    /// always drop all of them together. It is in fact desirable because if
    /// they had separate `should_exit` flags then we'd have to block up to
    /// `POLL_TIMEOUT` per thread whereas since it is shared we will only block
    /// that long once.
    fn drop(&mut self) {
        self.state
            .store(PipelineState::Terminated, Ordering::Release);

        let _ = self.join_handle.take().map(|handle| {
            handle.thread().unpark();
            handle.join()
        });
    }
}

enum HelperFeedback {
    /// Helper delivered a fatal error to the consumer.
    FatalError,

    /// Helper received [`KafkaError::PartitionEOF`] on partition `p`.
    PartitionEOF(i32),
}

impl InputEndpoint for KafkaInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

impl TransportInputEndpoint for KafkaInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(KafkaInputReader::new(
            &self.config,
            consumer,
            parser,
            &self.name,
        )?))
    }
}

impl InputReader for KafkaInputReader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command.as_nonft().unwrap());
        self.poller_thread.unpark();
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}

impl Drop for KafkaInputReader {
    fn drop(&mut self) {
        self.request(InputReaderCommand::Disconnect);
    }
}
