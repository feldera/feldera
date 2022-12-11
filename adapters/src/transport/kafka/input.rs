use super::{refine_kafka_error, KafkaLogLevel};
use crate::{InputConsumer, InputEndpoint, InputTransport, PipelineState};
use anyhow::{Error as AnyError, Result as AnyResult};
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
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// On startup, the endpoint waits to join the consumer group.
/// This constant defines the default wait timeout.
const fn default_group_join_timeout_secs() -> u32 {
    10
}

/// `InputTransport` implementation that reads data from one or more
/// Kafka topics.
pub struct KafkaInputTransport;

impl InputTransport for KafkaInputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("kafka")
    }

    fn new_endpoint(
        &self,
        config: &YamlValue,
        consumer: Box<dyn InputConsumer>,
    ) -> AnyResult<Box<dyn InputEndpoint>> {
        let config = KafkaInputConfig::deserialize(config)?;
        let ep = KafkaInputEndpoint::new(config, consumer)?;
        Ok(Box::new(ep))
    }
}

/// Input endpoint configuration.
#[derive(Deserialize)]
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
    kafka_options: BTreeMap<String, String>,

    /// List of topics to subscribe to.
    topics: Vec<String>,

    /// The log level of the client.
    ///
    /// If not specified, the log level will be calculated based on the global
    /// log level of the `log` crate.
    log_level: Option<KafkaLogLevel>,

    /// Maximum timeout in seconds to wait for the endpoint to join the Kafka
    /// consumer group during initialization.
    #[serde(default = "default_group_join_timeout_secs")]
    group_join_timeout_secs: u32,
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
        // Commit automatically.
        // See https://docs.confluent.io/platform/current/clients/consumer.html#offset-management
        self.enforce_option("enable.auto.commit", "true")?;
        self.enforce_option("enable.auto.offset.store", "true")?;

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
    fn new(config: KafkaInputConfig, consumer: Box<dyn InputConsumer>) -> AnyResult<Self> {
        Ok(Self(KafkaInputEndpointInner::new(config, consumer)?))
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

impl ClientContext for KafkaInputContext {}

impl ConsumerContext for KafkaInputContext {
    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        // println!("Rebalance: {rebalance:?}");
        if matches!(rebalance, Rebalance::Assign(_)) {
            if let Some(endpoint) = self.endpoint.lock().unwrap().upgrade() {
                if endpoint.state() == PipelineState::Running {
                    // TODO: handle errors by storing them inside `endpoint`
                    // for later processing in `poll`.
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
    state: AtomicU32,
    kafka_consumer: BaseConsumer<KafkaInputContext>,
}

impl KafkaInputEndpointInner {
    fn new(mut config: KafkaInputConfig, consumer: Box<dyn InputConsumer>) -> AnyResult<Arc<Self>> {
        // Create Kafka consumer configuration.
        config.validate()?;

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            client_config.set(key, value);
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(RDKafkaLogLevel::from(log_level));
        }

        // Context object to intercept rebalancing events.
        let context = KafkaInputContext::new();

        // Create Kafka consumer.
        let kafka_consumer = BaseConsumer::from_config_and_context(&client_config, context)?;

        let endpoint = Arc::new(Self {
            state: AtomicU32::new(PipelineState::Paused as u32),
            kafka_consumer,
        });

        *endpoint.kafka_consumer.context().endpoint.lock().unwrap() = Arc::downgrade(&endpoint);

        // Subscibe consumer to `topics`.
        endpoint
            .kafka_consumer
            .subscribe(&config.topics.iter().map(String::as_str).collect::<Vec<_>>())?;

        // Wait for the consumer to join the group by waiting for the group
        // rebalance protocol to be set.
        for attempt in 0..=config.group_join_timeout_secs {
            if matches!(
                endpoint.kafka_consumer.rebalance_protocol(),
                RebalanceProtocol::None
            ) {
                if attempt == config.group_join_timeout_secs {
                    return Err(AnyError::msg(format!(
                        "failed to join consumer group '{}', giving up after {}s",
                        config.kafka_options.get("group.id").unwrap(),
                        config.group_join_timeout_secs
                    )));
                }
                std::thread::sleep(std::time::Duration::from_millis(1000));
                // kafka_consumer.poll(POLL_TIMEOUT);
                // println!("waiting to join the group");
            } else {
                break;
            }
        }

        let endpoint_clone = endpoint.clone();
        spawn(move || Self::worker_thread(endpoint_clone, consumer));

        Ok(endpoint)
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

            // According to `rdkafka` docs, we must keep polling even while
            // the consumer is paused as `BaseConsumer` processes control
            // messages (including rebalancing) within the polling thread.
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
                        consumer.input(payload);
                    }
                }
            }
        }
    }
}

impl InputEndpoint for KafkaInputEndpoint {
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
