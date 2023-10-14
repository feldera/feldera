use super::refine_kafka_error;
use crate::{
    transport::kafka::rdkafka_loglevel_from, InputConsumer, InputEndpoint, InputTransport,
    PipelineState,
};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use crossbeam::queue::ArrayQueue;
use log::debug;
use num_traits::FromPrimitive;
use pipeline_types::transport::kafka::KafkaInputConfig;
use rdkafka::{
    config::FromClientConfigAndContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, RebalanceProtocol},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message,
};
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
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

/// [`InputTransport`] implementation that reads data from one or more
/// Kafka topics.
///
/// This input transport is only available if the crate is configured with
/// `with-kafka` feature.
///
/// The input transport factory gives this transport the name `kafka`.
pub struct KafkaInputTransport;

impl InputTransport for KafkaInputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("kafka")
    }

    /// Creates a new [`InputEndpoint`] for reading from Kafka topics,
    /// interpreting `config` as a [`KafkaInputConfig`].
    ///
    /// See [`InputTransport::new_endpoint()`] for more information.
    fn new_endpoint(&self, config: &YamlValue) -> AnyResult<Box<dyn InputEndpoint>> {
        let config = KafkaInputConfig::deserialize(config)?;
        let ep = KafkaInputEndpoint::new(config)?;
        Ok(Box::new(ep))
    }
}

struct KafkaInputEndpoint(Arc<KafkaInputEndpointInner>);

impl KafkaInputEndpoint {
    fn new(config: KafkaInputConfig) -> AnyResult<Self> {
        Ok(Self(KafkaInputEndpointInner::new(config)?))
    }
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
    endpoint: Mutex<Weak<KafkaInputEndpointInner>>,
}

impl KafkaInputContext {
    fn new() -> Self {
        Self {
            endpoint: Mutex::new(Weak::new()),
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

    /*fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        println!("log: {} {}", fac, log_message);
    }

    fn stats(&self, statistics: rdkafka::Statistics) {
        println!("stats: {:?}", statistics)
    }*/
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

struct KafkaInputEndpointInner {
    config: KafkaInputConfig,
    state: AtomicU32,
    kafka_consumer: BaseConsumer<KafkaInputContext>,
    errors: ArrayQueue<(KafkaError, String)>,
}

impl KafkaInputEndpointInner {
    fn new(mut config: KafkaInputConfig) -> AnyResult<Arc<Self>> {
        // Create Kafka consumer configuration.
        config.validate()?;
        debug!("Starting Kafka input endpoint: {config:?}");

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            client_config.set(key, value);
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        // Context object to intercept rebalancing events and errors.
        let context = KafkaInputContext::new();

        debug!("Creating Kafka consumer");
        // Create Kafka consumer.
        let kafka_consumer = BaseConsumer::from_config_and_context(&client_config, context)?;

        let endpoint = Arc::new(Self {
            config,
            state: AtomicU32::new(PipelineState::Paused as u32),
            kafka_consumer,
            errors: ArrayQueue::new(ERROR_BUFFER_SIZE),
        });

        Ok(endpoint)
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

impl KafkaInputEndpoint {
    fn worker_thread(endpoint: Arc<KafkaInputEndpointInner>, mut consumer: Box<dyn InputConsumer>) {
        let mut actual_state = PipelineState::Paused;
        loop {
            // endpoint.debug_consumer();
            match endpoint.state() {
                PipelineState::Paused if actual_state != PipelineState::Paused => {
                    actual_state = PipelineState::Paused;
                    if let Err(e) = endpoint.pause_partitions() {
                        let (_fatal, e) = endpoint.refine_error(e);
                        consumer.error(true, e);
                        return;
                    }
                }
                PipelineState::Running if actual_state != PipelineState::Running => {
                    actual_state = PipelineState::Running;
                    if let Err(e) = endpoint.resume_partitions() {
                        let (_fatal, e) = endpoint.refine_error(e);
                        consumer.error(true, e);
                        return;
                    };
                }
                PipelineState::Terminated => return,
                _ => {}
            }

            // Keep polling even while the consumer is paused as `BaseConsumer`
            // processes control messages (including rebalancing and errors)
            // within the polling thread.
            //
            // `POLL_TIMEOUT` makes sure that the thread will periodically
            // check for termination and pause commands.
            match endpoint.kafka_consumer.poll(POLL_TIMEOUT) {
                None => {
                    // println!("poll returned None");
                }
                Some(Err(e)) => {
                    // println!("poll returned error");
                    let (fatal, e) = endpoint.refine_error(e);
                    consumer.error(fatal, e);
                    if fatal {
                        return;
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

impl InputEndpoint for KafkaInputEndpoint {
    fn connect(&mut self, mut consumer: Box<dyn InputConsumer>) -> AnyResult<()> {
        *self.0.kafka_consumer.context().endpoint.lock().unwrap() = Arc::downgrade(&self.0);

        let topics = self
            .0
            .config
            .topics
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();

        // Subscibe consumer to `topics`.
        self.0.kafka_consumer.subscribe(&topics)?;

        let start = Instant::now();

        // Wait for the consumer to join the group by waiting for the group
        // rebalance protocol to be set.
        loop {
            // We must poll in order to receive connection failures; otherwise
            // we'd have to rely on timeouts only.
            match self.0.kafka_consumer.poll(POLL_TIMEOUT) {
                Some(Err(e)) => {
                    // Topic-does-not-exist error will be reported here.
                    bail!(
                        "failed to subscribe to topics '{topics:?}' (consumer group id '{}'): {e}",
                        self.0.config.kafka_options.get("group.id").unwrap(),
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
            if let Some((_error, reason)) = self.0.pop_error() {
                // let (_fatal, e) = self.0.refine_error(error);
                bail!("error subscribing to topics {topics:?}: {reason}");
            }

            if matches!(
                self.0.kafka_consumer.rebalance_protocol(),
                RebalanceProtocol::None
            ) {
                if start.elapsed()
                    >= Duration::from_secs(self.0.config.group_join_timeout_secs as u64)
                {
                    bail!(
                        "failed to subscribe to topics '{topics:?}' (consumer group id '{}'), giving up after {}s",
                        self.0.config.kafka_options.get("group.id").unwrap(),
                        self.0.config.group_join_timeout_secs
                    );
                }
                // println!("waiting to join the group");
            } else {
                break;
            }
        }

        let endpoint_clone = self.0.clone();
        spawn(move || Self::worker_thread(endpoint_clone, consumer));
        Ok(())
    }

    fn pause(&self) -> AnyResult<()> {
        // Notify worker thread via the state flag.  The worker may
        // send another buffer downstream before the flag takes effect.
        self.0.set_state(PipelineState::Paused);
        Ok(())
    }

    fn start(&self) -> AnyResult<()> {
        self.0.set_state(PipelineState::Running);
        Ok(())
    }

    fn disconnect(&self) {
        self.0.set_state(PipelineState::Terminated);
    }
}

impl Drop for KafkaInputEndpoint {
    fn drop(&mut self) {
        self.disconnect();
    }
}
