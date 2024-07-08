use crate::transport::InputEndpoint;
use crate::{
    transport::{
        kafka::{rdkafka_loglevel_from, refine_kafka_error, DeferredLogging},
        secret_resolver::MaybeSecret,
        InputReader, Step,
    },
    InputConsumer, PipelineState, TransportInputEndpoint,
};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use crossbeam::queue::ArrayQueue;
use log::debug;
use num_traits::FromPrimitive;
use pipeline_types::{secret_ref::MaybeSecretRef, transport::kafka::KafkaInputConfig};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::{
    config::FromClientConfigAndContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, RebalanceProtocol},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message,
};
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Sender};
use std::thread::{available_parallelism, JoinHandle};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, Weak,
    },
    thread::spawn,
    time::{Duration, Instant},
};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);

// Size of the circular buffer used to pass errors from ClientContext
// to the worker thread.
const ERROR_BUFFER_SIZE: usize = 1000;

pub struct KafkaInputEndpoint {
    config: Arc<KafkaInputConfig>,
}

impl KafkaInputEndpoint {
    pub fn new(mut config: KafkaInputConfig) -> AnyResult<KafkaInputEndpoint> {
        config.validate()?;
        Ok(KafkaInputEndpoint {
            config: Arc::new(config),
        })
    }
}

struct KafkaInputReader(Arc<KafkaInputReaderInner>);

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
    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        // println!("Rebalance: {rebalance:?}");
        if matches!(rebalance, Rebalance::Assign(_)) {
            if let Some(endpoint) = self.endpoint.lock().unwrap().upgrade() {
                if endpoint.state() == PipelineState::Running {
                    let _ = endpoint.resume_partitions();
                } else {
                    let _ = endpoint.pause_partitions();
                }
            }
        }

        // println!("Rebalance complete");
    }
}

struct KafkaInputReaderInner {
    config: Arc<KafkaInputConfig>,
    state: AtomicU32,
    kafka_consumer: BaseConsumer<KafkaInputContext>,
    errors: ArrayQueue<(KafkaError, String)>,
}

impl KafkaInputReaderInner {
    /// Tries to read a message from this Kafka consumer. If successful, passes
    /// it to `consumer`. If there's a problem or an EOF, sends it to
    /// `feedback`.
    fn poll(
        self: &Arc<Self>,
        consumer: &mut Box<dyn InputConsumer>,
        feedback: &Sender<HelperFeedback>,
    ) {
        match self.kafka_consumer.poll(POLL_TIMEOUT) {
            None => (),
            Some(Err(KafkaError::PartitionEOF(p))) => {
                feedback.send(HelperFeedback::PartitionEOF(p)).unwrap()
            }
            Some(Err(e)) => {
                // println!("poll returned error");
                let (fatal, e) = self.refine_error(e);
                consumer.error(fatal, e);
                if fatal {
                    feedback.send(HelperFeedback::FatalError).unwrap();
                }
            }
            Some(Ok(message)) => {
                // println!("received {} bytes", message.payload().unwrap().len());
                // message.payload().map(|payload| consumer.input(payload));

                if let Some(payload) = message.payload() {
                    // Leave it to the controller to handle errors.  There is noone we can
                    // forward the error to upstream.
                    let _ = consumer.input_chunk(payload);
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

    fn state(&self) -> PipelineState {
        PipelineState::from_u32(self.state.load(Ordering::Acquire)).unwrap()
    }

    fn set_state(&self, state: PipelineState) {
        self.state.store(state as u32, Ordering::Release);
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

impl KafkaInputReader {
    fn new(
        config: &Arc<KafkaInputConfig>,
        mut consumer: Box<dyn InputConsumer>,
    ) -> AnyResult<Self> {
        // Create Kafka consumer configuration.
        debug!("Starting Kafka input endpoint: {:?}", config);

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            // If it is a secret reference, resolve it to the actual secret string
            match MaybeSecret::new_using_default_directory(
                MaybeSecretRef::new_using_pattern_match(value.clone()),
            )? {
                MaybeSecret::String(simple_string) => {
                    client_config.set(key, simple_string);
                }
                MaybeSecret::Secret(secret_string) => {
                    client_config.set(key, secret_string);
                }
            }
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        // Context object to intercept rebalancing events and errors.
        let context = KafkaInputContext::new();

        debug!("Creating Kafka consumer");
        let inner = Arc::new(KafkaInputReaderInner {
            config: config.clone(),
            state: AtomicU32::new(PipelineState::Paused as u32),
            kafka_consumer: BaseConsumer::from_config_and_context(&client_config, context)?,
            errors: ArrayQueue::new(ERROR_BUFFER_SIZE),
        });

        *inner.kafka_consumer.context().endpoint.lock().unwrap() = Arc::downgrade(&inner);

        let topics = config.topics.iter().map(String::as_str).collect::<Vec<_>>();

        // Subscribe consumer to `topics`.
        inner.kafka_consumer.subscribe(&topics)?;

        let start = Instant::now();

        // Wait for the consumer to join the group by waiting for the group
        // rebalance protocol to be set.
        loop {
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
                        // Leave it to the controller to handle errors.  There is noone we can
                        // forward the error to upstream.
                        let _ = consumer.input_chunk(payload);
                    }
                }
                _ => (),
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

        let endpoint_clone = inner.clone();
        spawn(move || KafkaInputReader::poller_thread(endpoint_clone, consumer));
        Ok(KafkaInputReader(inner))
    }

    /// The main poller thread for a Kafka input. Polls `endpoint` as long as
    /// the pipeline is running, and passes the data to `consumer`.
    fn poller_thread(endpoint: Arc<KafkaInputReaderInner>, mut consumer: Box<dyn InputConsumer>) {
        // Figure out the number of threads based on configuration, defaults,
        // and system resources.
        let max_threads = available_parallelism().map_or(16, NonZeroUsize::get);
        let n_threads = endpoint
            .config
            .poller_threads
            .unwrap_or(3)
            .clamp(1, max_threads);

        let mut actual_state = PipelineState::Paused;
        let mut partition_eofs = HashSet::new();
        let (feedback_sender, feedback_receiver) = channel();
        let should_exit = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::with_capacity(n_threads - 1);
        loop {
            match endpoint.state() {
                PipelineState::Paused if actual_state != PipelineState::Paused => {
                    actual_state = PipelineState::Paused;
                    if let Err(e) = endpoint.pause_partitions() {
                        let (_fatal, e) = endpoint.refine_error(e);
                        consumer.error(true, e);
                        return;
                    }
                    threads.clear();
                }
                PipelineState::Running if actual_state != PipelineState::Running => {
                    actual_state = PipelineState::Running;
                    if let Err(e) = endpoint.resume_partitions() {
                        let (_fatal, e) = endpoint.refine_error(e);
                        consumer.error(true, e);
                        return;
                    };

                    // Create the rest of threads (start from 1 instead of 0
                    // because we're one of the threads).
                    should_exit.store(false, Ordering::Release);
                    for _ in 1..n_threads {
                        threads.push(HelperThread::new(
                            Arc::clone(&endpoint),
                            consumer.fork(),
                            Arc::clone(&should_exit),
                            feedback_sender.clone(),
                        ));
                    }
                }
                PipelineState::Terminated => return,
                _ => {}
            }

            // Keep polling even while the consumer is paused as `BaseConsumer`
            // processes control messages (including rebalancing and errors)
            // within the polling thread.
            endpoint.poll(&mut consumer, &feedback_sender);

            for feedback in feedback_receiver.try_iter() {
                match feedback {
                    HelperFeedback::PartitionEOF(p) => {
                        // If all the partitions we're subscribed to have received
                        // an EOF, then we're done.
                        partition_eofs.insert(p);
                        if endpoint
                            .kafka_consumer
                            .assignment()
                            .unwrap_or_default()
                            .elements()
                            .iter()
                            .map(|tpl| tpl.partition())
                            .all(|p| partition_eofs.contains(&p))
                        {
                            consumer.eoi();
                            return;
                        }
                    }
                    HelperFeedback::FatalError => return,
                }
            }

            while let Some((error, reason)) = endpoint.pop_error() {
                let (fatal, _e) = endpoint.refine_error(error);
                // `reason` contains a human-readable description of the
                // error.
                consumer.error(fatal, anyhow!(reason));
                if fatal {
                    return;
                }
            }
        }
    }
}

/// A thread that will help the main poller thread by processing messages
/// received from Kafka.
struct HelperThread {
    /// Used by the poller thread to tell us to exit.
    should_exit: Arc<AtomicBool>,

    /// Our own join handle so we can wait when dropped.
    join_handle: Option<JoinHandle<()>>,
}

impl HelperThread {
    fn new(
        endpoint: Arc<KafkaInputReaderInner>,
        mut consumer: Box<dyn InputConsumer>,
        should_exit: Arc<AtomicBool>,
        feedback_sender: Sender<HelperFeedback>,
    ) -> Self {
        Self {
            should_exit: Arc::clone(&should_exit),
            join_handle: {
                Some(spawn(move || {
                    while !should_exit.load(Ordering::Acquire) {
                        endpoint.poll(&mut consumer, &feedback_sender);
                    }
                }))
            },
        }
    }
}
impl Drop for HelperThread {
    /// When we're dropped, make the thread exit.
    ///
    /// *Careful*: `should_exit` is shared with all the helper threads, so if
    /// one gets dropped, the others will exit too. Currently this is OK because
    /// we always drop all of them together. It is in fact desirable because if
    /// they had separate `should_exit` flags then we'd have to block up to
    /// `POLL_TIMEOUT` per thread whereas since it is shared we will only block
    /// that long once.
    fn drop(&mut self) {
        self.should_exit.store(true, Ordering::Release);
        let _ = self.join_handle.take().map(|handle| handle.join());
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
        _start_step: Step,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(KafkaInputReader::new(&self.config, consumer)?))
    }
}

impl InputReader for KafkaInputReader {
    fn pause(&self) -> AnyResult<()> {
        // Notify worker thread via the state flag.  The worker may
        // send another buffer downstream before the flag takes effect.
        self.0.set_state(PipelineState::Paused);
        Ok(())
    }

    fn start(&self, _step: Step) -> AnyResult<()> {
        self.0.set_state(PipelineState::Running);
        Ok(())
    }

    fn disconnect(&self) {
        self.0.set_state(PipelineState::Terminated);
    }
}

impl Drop for KafkaInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}
