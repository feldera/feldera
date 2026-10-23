//! Kafka topics, a producer, and a consumer of an output topic, for the Kafka suites and the
//! Kafka transport's tests.

use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle, sleep};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result as AnyResult, anyhow, bail};
use feldera_adapterlib::format::{InputBuffer, Parser};
use feldera_adapterlib::test::{
    ConnectorTestEngine, InputOptions, TestCollection, Updates, ZSet, default_timeout_ms, wait,
};
use feldera_types::config::FormatConfig;
use feldera_types::program_schema::Relation;
use feldera_types::transport::kafka::default_redpanda_server;
use futures::executor::block_on;
use rdkafka::message::{BorrowedMessage, Header, OwnedHeaders};
use rdkafka::{
    ClientConfig, ClientContext, Message,
    admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication},
    client::{Client, DefaultClientContext},
    config::{FromClientConfig, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer},
    producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer},
    util::Timeout,
};
use serde_json::Value;
use tracing::{error, info};

use crate::test::parser;

static MAX_TOPIC_PROBE_TIMEOUT: Duration = Duration::from_millis(20_000);

pub struct KafkaResources {
    admin_client: AdminClient<DefaultClientContext>,
    topics: Vec<String>,
}

// Checks `consumer` to make sure that all of the topics in `topics` are
// accessible and have the specified number of partitions.
fn check_topics<C: ClientContext>(consumer: &Client<C>, topics: &[(&str, i32)]) -> AnyResult<()> {
    // Retrieve the list of all topics and then filter it.  If the broker is
    // configured to automatically create topics, then asking for each of them
    // individually will create them, which is no good for topics we want to
    // delete.
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(1))?;

    for &(topic, partitions) in topics.iter() {
        let cur_partitions = metadata
            .topics()
            .iter()
            .find(|m| m.name() == topic)
            .map(|mt| mt.partitions().len())
            .unwrap_or(0) as i32;
        if cur_partitions != partitions {
            bail!("{topic} has {cur_partitions} partitions, waiting for {partitions}");
        }
        if partitions > 0 {
            for partition in 0..partitions {
                consumer
                    .fetch_watermarks(topic, partition, Duration::from_secs(1))
                    .map_err(|e| anyhow!("topic {topic}: {e}"))?;
            }
        }
    }
    Ok(())
}

fn wait_for_completion<C: ClientContext>(admin_client: &AdminClient<C>, topics: &[(&str, i32)]) {
    let topic_names = topics
        .iter()
        .map(|(topic_name, _partitions)| &**topic_name)
        .collect::<Vec<_>>();

    let start = Instant::now();
    let mut backoff = 100;
    let mut n_retries = 0;
    while let Err(err) = check_topics(admin_client.inner(), topics) {
        info!(
            "KafkaResources::create_topics {topic_names:?}: unable to connect to newly created topics, retrying: {err}"
        );
        if start.elapsed() > MAX_TOPIC_PROBE_TIMEOUT {
            panic!(
                "KafkaResources::create_topics {topic_names:?}: unable to connect to newly created topics, giving up after {}ms: {err}",
                MAX_TOPIC_PROBE_TIMEOUT.as_millis()
            );
        }
        sleep(Duration::from_millis(backoff));
        backoff = 1000.min(backoff * 2);
        n_retries += 1;
    }
    if n_retries > 0 {
        info!("KafkaResources::create_topics {topic_names:?}: success after {n_retries} tries");
    }
}

/// An object that creates Kafka topics on startup and deletes them
/// on drop.  Helps make sure that test runs don't leave garbage behind.
impl KafkaResources {
    /// `(topic, n_partitions)` creates a topic with `n_partitions` partitions.
    /// If `n_partitions` is 0 then the topic is deleted instead.
    pub fn create_topics(topics: &[(&str, i32)]) -> Self {
        // Kafka does not handle topic deletion and creation consistently when
        // multiple operations are performed in parallel, so serialize calls to
        // this function.
        static LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
        let _guard = LOCK.lock().expect("panicking because a previous attempt to create a Kafka topic panicked already (probably the Kafka broker is down)");

        let mut admin_config = ClientConfig::new();
        admin_config
            .set("bootstrap.servers", default_redpanda_server())
            .set_log_level(RDKafkaLogLevel::Debug);
        let admin_client = AdminClient::from_config(&admin_config).unwrap();

        // Delete topics if they exist from previous failed runs that crashed before
        // cleaning up.  Otherwise, it may take a long time to re-join a
        // group whose members are dead, plus the old topics may contain
        // messages that will mess up our tests.
        //
        // We wait for deletion to finish before creating topics, because Kafka
        // doesn't seem to always do what we want if we tell it to re-create a
        // topic before it's been fully deleted.
        let delete_topics = topics
            .iter()
            .map(|(topic_name, _partitions)| &**topic_name)
            .collect::<Vec<_>>();
        let _ = block_on(admin_client.delete_topics(&delete_topics, &AdminOptions::new()));
        let deleted_topics: Vec<_> = topics.iter().map(|(name, _)| (*name, 0)).collect();
        wait_for_completion(&admin_client, &deleted_topics[..]);

        // Now create the topics and wait for the creations to complete.
        let new_topics = topics
            .iter()
            .filter(|&(_topic_name, partitions)| *partitions > 0)
            .map(|(topic_name, partitions)| {
                NewTopic::new(topic_name, *partitions, TopicReplication::Fixed(1))
            })
            .collect::<Vec<_>>();
        if !new_topics.is_empty() {
            block_on(admin_client.create_topics(&new_topics, &AdminOptions::new())).unwrap();
            wait_for_completion(&admin_client, topics);
        }
        Self {
            admin_client,
            topics: topics
                .iter()
                .map(|(topic_name, _)| topic_name.to_string())
                .collect::<Vec<_>>(),
        }
    }

    pub fn add_partition(&self, topic: &str, new_partition_count: usize) {
        block_on(self.admin_client.create_partitions(
            &[NewPartitions::new(topic, new_partition_count)],
            &AdminOptions::new(),
        ))
        .unwrap();
        wait_for_completion(&self.admin_client, &[(topic, new_partition_count as i32)]);
    }
}

impl Drop for KafkaResources {
    fn drop(&mut self) {
        let topic_names = self
            .topics
            .iter()
            .map(|topic_name| &**topic_name)
            .collect::<Vec<_>>();
        let _ = block_on(
            self.admin_client
                .delete_topics(&topic_names, &AdminOptions::new()),
        )
        .map_err(|e| error!("Failed to delete topics {topic_names:?}: {e}"));
    }
}

/// A producer of test messages to the local broker.
pub struct TestProducer {
    pub producer: ThreadedProducer<DefaultProducerContext>,
}

impl Default for TestProducer {
    fn default() -> Self {
        Self::new()
    }
}

impl TestProducer {
    pub fn new() -> Self {
        let mut producer_config = ClientConfig::new();
        producer_config
            .set("bootstrap.servers", default_redpanda_server())
            .set("message.timeout.ms", "0") // infinite timeout
            .set_log_level(RDKafkaLogLevel::Debug);
        let producer = ThreadedProducer::from_config(&producer_config).unwrap();

        Self { producer }
    }

    /// Sends each of `messages` to `topic` as one message, without waiting for delivery.
    pub fn send_to_topic(&self, messages: &[Vec<u8>], topic: &str) {
        for message in messages {
            let record = <BaseRecord<(), [u8], ()>>::to(topic).payload(message);
            self.producer.send(record).unwrap();
        }
    }

    /// Sends each of `messages` to `partition` of `topic` as one message, without waiting for
    /// delivery.
    pub fn send_to_topic_partition(&self, messages: &[Vec<u8>], topic: &str, partition: i32) {
        for message in messages {
            let record = <BaseRecord<(), [u8], ()>>::to(topic)
                .partition(partition)
                .payload(message);
            self.producer.send(record).unwrap();
        }
    }

    /// Sends `data` to `topic` with `headers`, where a header without a value is `None`, and
    /// waits for delivery.
    pub fn send_message(
        &self,
        data: &[u8],
        topic: &str,
        headers: Option<BTreeMap<String, Option<Vec<u8>>>>,
    ) {
        let mut kafka_headers = OwnedHeaders::new();

        for (key, value) in headers.unwrap_or_default().iter() {
            kafka_headers = kafka_headers.insert(Header {
                key,
                value: value.as_ref().map(|value| value.as_slice()),
            });
        }
        let record = <BaseRecord<(), [u8], ()>>::to(topic)
            .payload(data)
            .headers(kafka_headers);
        self.producer.send(record).unwrap();
        self.producer.flush(Timeout::Never).unwrap();
    }

    /// Sends a message with `key` and no payload to `topic`, and waits for delivery.
    pub fn send_tombstone(&self, key: &[u8], topic: &str) {
        let record = BaseRecord::<[u8], (), ()>::to(topic).key(key);
        self.producer.send(record).unwrap();
        self.producer.flush(Timeout::Never).unwrap();
    }

    /// Sends `string` to `topic`, and waits for delivery.
    pub fn send_string(&self, string: &str, topic: &str) {
        let record = <BaseRecord<(), str, ()>>::to(topic).payload(string);
        self.producer.send(record).unwrap();
        self.producer.flush(Timeout::Never).unwrap();
    }

    /// Sends `string` to `partition` of `topic`, and waits for delivery.
    pub fn send_string_partition(&self, string: &str, topic: &str, partition: i32) {
        let record = <BaseRecord<(), str, ()>>::to(topic)
            .partition(partition)
            .payload(string);
        self.producer.send(record).unwrap();
        self.producer.flush(Timeout::Never).unwrap();
    }
}

/// A callback that a [`BufferConsumer`] calls for each message it receives.
type MessageCallback = Box<dyn Fn(&BorrowedMessage) + Send>;

/// A thread that reads an output topic from the start and parses each message into a
/// collection of the engine fixture.
pub struct BufferConsumer {
    thread_handle: Option<JoinHandle<()>>,
    collection: Box<dyn TestCollection>,
    shutdown_flag: Arc<AtomicBool>,
}

impl Drop for BufferConsumer {
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.thread_handle.take().unwrap().join().unwrap();
    }
}

impl BufferConsumer {
    /// Starts reading `topic`, parsing each message in the input format `format` with
    /// `format_config` into a fresh collection of `relation`, after passing it to
    /// `message_cb`.
    pub fn new(
        engine: &dyn ConnectorTestEngine,
        relation: &Relation,
        topic: &str,
        format: &str,
        format_config: Value,
        message_cb: Option<MessageCallback>,
    ) -> Self {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        let topic = topic.to_string();
        let collection = engine
            .input_collection(relation, &InputOptions::default())
            .unwrap();
        let mut parser = parser(
            collection.as_ref(),
            &FormatConfig {
                name: format.to_string().into(),
                config: format_config,
            },
        );

        let thread_handle = thread::Builder::new()
            .name("test consumer".to_string())
            .spawn(move || {
                let group_id = format!(
                    "test_group_{}",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                );

                let kafka_consumer = ClientConfig::new()
                    .set("bootstrap.servers", default_redpanda_server())
                    .set("enable.auto.commit", "true")
                    .set("enable.auto.offset.store", "true")
                    .set("auto.offset.reset", "earliest")
                    .set("group.id", &group_id)
                    .create::<BaseConsumer>()
                    .unwrap();

                kafka_consumer.subscribe(&[&topic]).unwrap();

                loop {
                    if shutdown_flag_clone.load(Ordering::Acquire) {
                        return;
                    }

                    match kafka_consumer.poll(Duration::from_millis(100)) {
                        None => {}
                        Some(Err(e)) => {
                            panic!("poll returned error: {e}");
                        }
                        Some(Ok(message)) => {
                            if let Some(cb) = &message_cb {
                                cb(&message)
                            };

                            if let Some(payload) = message.payload() {
                                parser.parse(payload, None).0.flush().unwrap();
                            }
                        }
                    }
                }
            })
            .unwrap();

        Self {
            thread_handle: Some(thread_handle),
            collection,
            shutdown_flag,
        }
    }

    /// Returns the number of rows read so far.
    pub fn len(&self) -> usize {
        self.collection.len()
    }

    /// Returns `true` if no rows were read since the last [`Self::clear`].
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Forgets the rows read so far.
    pub fn clear(&self) {
        self.collection.reset()
    }

    /// Waits for the rows of `batches` to arrive, then checks that the consumer read exactly
    /// those rows, in any order, each inserted once per unit of weight.
    pub fn wait_for_output_unordered(&self, batches: &[Updates]) {
        let count: usize = batches.iter().map(Updates::len).sum();
        let mut received = 0;
        wait(
            move || {
                let n = self.len();
                if n != received {
                    info!("waiting for {count} records (received {n})");
                    received = n;
                }
                n >= count
            },
            default_timeout_ms(),
        )
        .unwrap();
        let mut expected = ZSet::empty(self.collection.schema()).unwrap();
        for batch in batches {
            expected.add(&batch.zset().unwrap()).unwrap();
        }
        let updates = self.collection.updates().unwrap();
        assert!(updates.weights().iter().all(|weight| *weight == 1));
        assert_eq!(updates.zset().unwrap(), expected);
    }
}

/// Returns each batch of `data` as one message of CSV records.
#[cfg(test)]
pub fn csv_messages<T: serde::Serialize>(data: &[Vec<T>]) -> Vec<Vec<u8>> {
    data.iter()
        .map(|batch| {
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::with_capacity(batch.len() * 32));
            for val in batch {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();
            writer.into_inner().unwrap()
        })
        .collect()
}
