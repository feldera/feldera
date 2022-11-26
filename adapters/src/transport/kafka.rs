use crate::{InputConsumer, InputEndpoint, InputTransport, PipelineState};
use anyhow::{Error as AnyError, Result as AnyResult};
use num_traits::FromPrimitive;
use rdkafka::{
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, RebalanceProtocol},
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

/// Kafka logging levels.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

impl From<RDKafkaLogLevel> for KafkaLogLevel {
    fn from(level: RDKafkaLogLevel) -> Self {
        match level {
            RDKafkaLogLevel::Emerg => Self::Emerg,
            RDKafkaLogLevel::Alert => Self::Alert,
            RDKafkaLogLevel::Critical => Self::Critical,
            RDKafkaLogLevel::Error => Self::Error,
            RDKafkaLogLevel::Warning => Self::Warning,
            RDKafkaLogLevel::Notice => Self::Notice,
            RDKafkaLogLevel::Info => Self::Info,
            RDKafkaLogLevel::Debug => Self::Debug,
        }
    }
}

impl From<KafkaLogLevel> for RDKafkaLogLevel {
    fn from(level: KafkaLogLevel) -> Self {
        match level {
            KafkaLogLevel::Emerg => RDKafkaLogLevel::Emerg,
            KafkaLogLevel::Alert => RDKafkaLogLevel::Alert,
            KafkaLogLevel::Critical => RDKafkaLogLevel::Critical,
            KafkaLogLevel::Error => RDKafkaLogLevel::Error,
            KafkaLogLevel::Warning => RDKafkaLogLevel::Warning,
            KafkaLogLevel::Notice => RDKafkaLogLevel::Notice,
            KafkaLogLevel::Info => RDKafkaLogLevel::Info,
            KafkaLogLevel::Debug => RDKafkaLogLevel::Debug,
        }
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
struct KafkaEndpointContext {
    // We keep a weak reference to the endpoint to avoid a reference cycle:
    // endpoint->BaseConsumer->context->endpoint.
    endpoint: Mutex<Weak<KafkaInputEndpointInner>>,
}

impl KafkaEndpointContext {
    fn new() -> Self {
        Self {
            endpoint: Mutex::new(Weak::new()),
        }
    }
}

impl ClientContext for KafkaEndpointContext {}

impl ConsumerContext for KafkaEndpointContext {
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
    kafka_consumer: BaseConsumer<KafkaEndpointContext>,
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
        let context = KafkaEndpointContext::new();

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
    fn pause_partitions(&self) -> AnyResult<()> {
        // println!("pause");
        // self.debug_consumer();

        self.kafka_consumer
            .pause(&self.kafka_consumer.assignment()?)?;
        Ok(())
    }

    /// Resume all partitions assigned to the consumer.
    fn resume_partitions(&self) -> AnyResult<()> {
        self.kafka_consumer
            .resume(&self.kafka_consumer.assignment()?)?;
        Ok(())
    }

    fn worker_thread(endpoint: Arc<KafkaInputEndpointInner>, mut consumer: Box<dyn InputConsumer>) {
        let mut actual_state = PipelineState::Paused;
        loop {
            // endpoint.debug_consumer();
            match endpoint.state() {
                PipelineState::Paused if actual_state != PipelineState::Paused => {
                    actual_state = PipelineState::Paused;
                    if let Err(e) = endpoint.pause_partitions() {
                        consumer.error(e);
                        return;
                    }
                }
                PipelineState::Running if actual_state != PipelineState::Running => {
                    actual_state = PipelineState::Running;
                    if let Err(e) = endpoint.resume_partitions() {
                        consumer.error(e);
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
                    consumer.error(AnyError::from(e));
                    if endpoint.kafka_consumer.client().fatal_error().is_some() {
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

#[cfg(test)]
mod test {

    use crate::test::{generate_test_data, mock_input_pipeline, wait, MockDeZSet, TestStruct};
    use csv::WriterBuilder as CsvWriterBuilder;
    use futures::executor::block_on;
    use log::{LevelFilter, Log, Metadata, Record};
    use proptest::{collection, prelude::*};
    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication},
        client::DefaultClientContext,
        config::{FromClientConfig, RDKafkaLogLevel},
        producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
        ClientConfig,
    };
    use std::{thread::sleep, time::Duration};

    struct TestLogger;
    static TEST_LOGGER: TestLogger = TestLogger;

    impl Log for TestLogger {
        fn enabled(&self, _metadata: &Metadata) -> bool {
            true
        }

        fn log(&self, record: &Record) {
            println!("{} - {}", record.level(), record.args());
        }

        fn flush(&self) {}
    }

    struct KafkaResources {
        admin_client: AdminClient<DefaultClientContext>,
    }

    /// An object that creates a couple of topics on startup and deletes them
    /// on drop.  Helps make sure that test runs don't leave garbage behind.
    impl KafkaResources {
        fn create_topics() -> Self {
            let mut admin_config = ClientConfig::new();
            admin_config
                .set("bootstrap.servers", "localhost")
                .set_log_level(RDKafkaLogLevel::Debug);
            let admin_client = AdminClient::from_config(&admin_config).unwrap();

            let input_topic1 = NewTopic::new("input_topic1", 1, TopicReplication::Fixed(1));
            let input_topic2 = NewTopic::new("input_topic2", 2, TopicReplication::Fixed(1));

            // Delete topics if they exist from previous failed runs that crashed before
            // cleaning up.  Otherwise, it may take a long time to re-join a
            // group whose members are dead, plus the old topics may contain
            // messages that will mess up our tests.
            let _ = block_on(
                admin_client.delete_topics(&["input_topic1", "input_topic2"], &AdminOptions::new()),
            );

            block_on(
                admin_client.create_topics([&input_topic1, &input_topic2], &AdminOptions::new()),
            )
            .unwrap();

            Self { admin_client }
        }

        fn add_partition(&self, topic: &str) {
            block_on(
                self.admin_client
                    .create_partitions(&[NewPartitions::new(topic, 1)], &AdminOptions::new()),
            )
            .unwrap();
        }
    }

    impl Drop for KafkaResources {
        fn drop(&mut self) {
            let _ = block_on(
                self.admin_client
                    .delete_topics(&["input_topic1", "input_topic2"], &AdminOptions::new()),
            );
        }
    }

    fn send_to_topic(
        producer: &ThreadedProducer<DefaultProducerContext>,
        data: &[Vec<TestStruct>],
        topic: &str,
    ) {
        for batch in data {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::with_capacity(batch.len() * 32));

            for val in batch.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();
            let bytes = writer.into_inner().unwrap();

            let record = <BaseRecord<(), [u8], ()>>::to(topic).payload(&bytes);
            producer.send(record).unwrap();
            // println!("sent {} bytes", bytes.len());
        }
        // producer.flush(Timeout::Never).unwrap();
        println!("Data written to '{topic}'");
    }

    /// Wait to receive all records in `data` in the same order.
    fn wait_for_output_ordered(zset: &MockDeZSet<TestStruct>, data: &[Vec<TestStruct>]) {
        let num_records: usize = data.iter().map(Vec::len).sum();

        wait(|| zset.state().flushed.len() == num_records, None);

        for (i, val) in data.iter().flat_map(|data| data.iter()).enumerate() {
            assert_eq!(&zset.state().flushed[i].0, val);
        }
    }

    /// Wait to receive all records in `data` in some order.
    fn wait_for_output_unordered(zset: &MockDeZSet<TestStruct>, data: &[Vec<TestStruct>]) {
        let num_records: usize = data.iter().map(Vec::len).sum();

        wait(|| zset.state().flushed.len() == num_records, None);

        let mut data_sorted = data
            .iter()
            .flat_map(|data| data.clone().into_iter())
            .collect::<Vec<_>>();
        data_sorted.sort();

        let mut zset_sorted = zset
            .state()
            .flushed
            .iter()
            .map(|(val, polarity)| {
                assert_eq!(*polarity, true);
                val.clone()
            })
            .collect::<Vec<_>>();
        zset_sorted.sort();

        assert_eq!(zset_sorted, data_sorted);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2))]

        #[test]
        fn proptest_kafka_input(data in collection::vec(generate_test_data(1000), 0..=100)) {

            let _ = log::set_logger(&TEST_LOGGER);
            log::set_max_level(LevelFilter::Debug);

            let kafka_resources = KafkaResources::create_topics();

            // auto.offset.reset: "earliest" - guarantees that on startup the
            // consumer will observe all messages sent by the producer even if
            // the producer starts earlier (the consumer won't start until the
            // rebalancing protocol kicks in).
            let config_str = format!(
                r#"
transport:
    name: kafka
    config:
        bootstrap.servers: "localhost"
        auto.offset.reset: "earliest"
        topics: [input_topic1, input_topic2]
        log_level: debug
format:
    name: csv
    config:
        input_stream: test_input
"#);

            println!("Building input pipeline");

            let (endpoint, _consumer, zset) = mock_input_pipeline::<TestStruct>(
                "test_input",
                serde_yaml::from_str(&config_str).unwrap(),
            );

            endpoint.start().unwrap();

            let mut producer_config = ClientConfig::new();
            producer_config
                .set("bootstrap.servers", "localhost")
                .set("message.timeout.ms", "0") // infinite timeout
                .set_log_level(RDKafkaLogLevel::Debug);
            let producer = ThreadedProducer::from_config(&producer_config)?;

            println!("Test: Receive from a topic with a single partition");

            // Send data to a topic with a single partition;
            // Make sure all records arrive in the original order.
            send_to_topic(&producer, &data, "input_topic1");

            wait_for_output_ordered(&zset, &data);
            zset.reset();

            println!("Test: Receive from a topic with multiple partitions");

            // Send data to a topic with multiple partitions.
            // Make sure all records are delivered, but not necessarily in the original order.
            send_to_topic(&producer, &data, "input_topic2");

            wait_for_output_unordered(&zset, &data);
            zset.reset();

            println!("Test: pause/resume");
            //println!("records before pause: {}", zset.state().flushed.len());

            // Paused endpoint shouldn't receive any data.
            endpoint.pause().unwrap();
            sleep(Duration::from_millis(1000));

            kafka_resources.add_partition("input_topic2");

            send_to_topic(&producer, &data, "input_topic2");
            sleep(Duration::from_millis(1000));
            assert_eq!(zset.state().flushed.len(), 0);

            // Receive everything after unpause.
            endpoint.start().unwrap();
            wait_for_output_unordered(&zset, &data);

            zset.reset();

            println!("Test: Disconnect");
            // Disconnected endpoint should not receive any data.
            endpoint.disconnect();
            sleep(Duration::from_millis(1000));

            send_to_topic(&producer, &data, "input_topic2");
            sleep(Duration::from_millis(1000));
            assert_eq!(zset.state().flushed.len(), 0);

            sleep(2 * super::POLL_TIMEOUT);

            println!("Delete Kafka resources");
            drop(kafka_resources);
        }
    }
}
