use super::{default_redpanda_server, refine_kafka_error, KafkaLogLevel};
use crate::{InputConsumer, InputEndpoint, InputTransport, PipelineState};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use crossbeam::queue::ArrayQueue;
use log::debug;
use num_traits::FromPrimitive;
use rdkafka::{
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, RebalanceProtocol},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message,
};
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, Weak,
    },
    thread::spawn,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use utoipa::{
    openapi::{
        schema::{KnownFormat, Schema},
        ArrayBuilder, ObjectBuilder, RefOr, SchemaFormat, SchemaType,
    },
    ToSchema,
};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);

// Size of the circular buffer used to pass errors from ClientContext
// to the worker thread.
const ERROR_BUFFER_SIZE: usize = 1000;

/// On startup, the endpoint waits to join the consumer group.
/// This constant defines the default wait timeout.
const fn default_group_join_timeout_secs() -> u32 {
    10
}

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
    fn new_endpoint(&self, _name: &str, config: &YamlValue) -> AnyResult<Box<dyn InputEndpoint>> {
        let config = KafkaInputConfig::deserialize(config)?;
        let ep = KafkaInputEndpoint::new(config)?;
        Ok(Box::new(ep))
    }
}

/// Configuration for reading data from Kafka topics with [`InputTransport`].
#[derive(Deserialize, Debug)]
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
}

// The auto-derived implementation gets confused by the flattened
// `kafka_options` field. FIXME: I didn't figure out how to attach a
// `description` to the `topics` property.
impl<'s> ToSchema<'s> for KafkaInputConfig {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "KafkaInputConfig",
            ObjectBuilder::new()
                .property(
                    "topics",
                    ArrayBuilder::new().items(
                        ObjectBuilder::new()
                            .schema_type(SchemaType::String)
                    )
                )
                .required("topics")
                .property(
                    "log_level",
                    KafkaLogLevel::schema().1
                )
                .property(
                    "group_join_timeout_secs",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Integer)
                        .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
                        .description(Some("Maximum timeout in seconds to wait for the endpoint to join the Kafka consumer group during initialization.")),
                )
                .additional_properties(Some(
                        ObjectBuilder::new()
                        .schema_type(SchemaType::String)
                        .description(Some(r#"Options passed directly to `rdkafka`.

See [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
used to configure the Kafka producer."#))))
                .into(),
        )
    }
}

impl KafkaInputConfig {
    /// Set `option` to `val`; return an error if `option` is set to a different
    /// value.
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
    fn validate(&mut self) -> AnyResult<()> {
        self.set_option_if_missing("bootstrap.servers", &default_redpanda_server());

        // These options will prevent librdkafka from automatically committing offsets of consumed
        // messages to the broker, meaning that next time the connector is instantiated it will
        // start reading from the offset specified in `auto.offset.reset`.  We used to set these to
        // `true`, which caused `rdkafka` to hang in some circumstances
        // (https://github.com/confluentinc/librdkafka/issues/3954).  Besides, the new behavior
        // is probably more correct given that circuit state currently does not survive across
        // pipeline restarts, so it makes sense to start feeding messages from the start rather
        // than from the last offset consumed by the previous instance of the pipeline, whose state
        // is lost.  Once we add fault tolerance, we will likely use explicit commits,
        // which also do not require these options.
        //
        // See https://docs.confluent.io/platform/current/clients/consumer.html#offset-management
        self.enforce_option("enable.auto.commit", "false")?;
        self.enforce_option("enable.auto.offset.store", "false")?;

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
/// See https://github.com/edenhill/librdkafka/issues/1849 for a discussion
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
            client_config.set_log_level(RDKafkaLogLevel::from(log_level));
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
