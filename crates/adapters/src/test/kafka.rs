use crate::{
    test::{wait, MockDeZSet, TestStruct},
    transport::kafka::default_redpanda_server,
    InputFormat,
};
use anyhow::{anyhow, bail, Result as AnyResult};
use csv::WriterBuilder as CsvWriterBuilder;
use futures::executor::block_on;
use log::{error, info};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication},
    client::{Client, DefaultClientContext},
    config::{FromClientConfig, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer},
    producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer},
    util::Timeout,
    ClientConfig, ClientContext, Message,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    thread::{sleep, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

static MAX_TOPIC_PROBE_TIMEOUT: Duration = Duration::from_millis(20_000);

pub struct KafkaResources {
    admin_client: AdminClient<DefaultClientContext>,
    topics: Vec<String>,
}

// Checks `consumer` to make sure that all of the topics in `topics` are
// accessible and have the specified number of partitions.
fn check_topics<C: ClientContext>(consumer: &Client<C>, topics: &[(&str, i32)]) -> AnyResult<()> {
    for &(topic, partitions) in topics.iter() {
        let m = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(1))
            .map_err(|e| anyhow!("topic {topic}: {e}"))?;
        let cur_partitions = m
            .topics()
            .get(0)
            .map(|topic| topic.partitions().len() as i32)
            .unwrap_or_default();
        if cur_partitions != partitions {
            bail!("{topic} only has {cur_partitions} partitions, waiting for {partitions}");
        }
        consumer
            .fetch_watermarks(topic, 0, Duration::from_secs(1))
            .map_err(|e| anyhow!("topic {topic}: {e}"))?;
    }
    Ok(())
}

/// An object that creates Kafka topics on startup and deletes them
/// on drop.  Helps make sure that test runs don't leave garbage behind.
impl KafkaResources {
    pub fn create_topics(topics: &[(&str, i32)]) -> Self {
        let mut admin_config = ClientConfig::new();
        admin_config
            .set("bootstrap.servers", &default_redpanda_server())
            .set_log_level(RDKafkaLogLevel::Debug);
        let admin_client = AdminClient::from_config(&admin_config).unwrap();

        let new_topics = topics
            .iter()
            .map(|(topic_name, partitions)| {
                NewTopic::new(topic_name, *partitions, TopicReplication::Fixed(1))
            })
            .collect::<Vec<_>>();
        let topic_names = topics
            .iter()
            .map(|(topic_name, _partitions)| &**topic_name)
            .collect::<Vec<_>>();

        // Delete topics if they exist from previous failed runs that crashed before
        // cleaning up.  Otherwise, it may take a long time to re-join a
        // group whose members are dead, plus the old topics may contain
        // messages that will mess up our tests.
        let _ = block_on(admin_client.delete_topics(&topic_names, &AdminOptions::new()));

        block_on(admin_client.create_topics(&new_topics, &AdminOptions::new())).unwrap();

        let start = Instant::now();
        let mut backoff = 100;
        let mut n_retries = 0;
        while let Err(err) = check_topics(admin_client.inner(), topics) {
            info!("KafkaResources::create_topics {topic_names:?}: unable to connect to newly created topics, retrying: {err}");
            if start.elapsed() > MAX_TOPIC_PROBE_TIMEOUT {
                panic!("KafkaResources::create_topics {topic_names:?}: unable to connect to newly created topics, giving up after {}ms: {err}", MAX_TOPIC_PROBE_TIMEOUT.as_millis());
            }
            sleep(Duration::from_millis(backoff));
            backoff = 1000.min(backoff * 2);
            n_retries += 1;
        }
        if n_retries > 0 {
            info!("KafkaResources::create_topics {topic_names:?}: success after {n_retries} tries");
        }

        Self {
            admin_client,
            topics: topics
                .iter()
                .map(|(topic_name, _)| topic_name.to_string())
                .collect::<Vec<_>>(),
        }
    }

    pub fn add_partition(&self, topic: &str) {
        block_on(
            self.admin_client
                .create_partitions(&[NewPartitions::new(topic, 1)], &AdminOptions::new()),
        )
        .unwrap();
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

pub struct TestProducer {
    producer: ThreadedProducer<DefaultProducerContext>,
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
            .set("bootstrap.servers", &default_redpanda_server())
            .set("message.timeout.ms", "0") // infinite timeout
            .set_log_level(RDKafkaLogLevel::Debug);
        let producer = ThreadedProducer::from_config(&producer_config).unwrap();

        Self { producer }
    }

    pub fn send_to_topic(&self, data: &[Vec<TestStruct>], topic: &str) {
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
            self.producer.send(record).unwrap();
        }
        // producer.flush(Timeout::Never).unwrap();
        // println!("Data written to '{topic}'");
    }

    pub fn send_string(&self, string: &str, topic: &str) {
        let record = <BaseRecord<(), str, ()>>::to(topic).payload(string);
        self.producer.send(record).unwrap();
        self.producer.flush(Timeout::Never).unwrap();
    }
}

/// Consumer thread: read from output topic, deserialize to a shared buffer.
pub struct BufferConsumer {
    thread_handle: Option<JoinHandle<()>>,
    buffer: MockDeZSet<TestStruct>,
    shutdown_flag: Arc<AtomicBool>,
}

impl Drop for BufferConsumer {
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.thread_handle.take().unwrap().join().unwrap();
    }
}

impl BufferConsumer {
    pub fn new(topic: &str, format: &str, format_config_yaml: &str) -> Self {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        let topic = topic.to_string();
        let format = <dyn InputFormat>::get_format(format).unwrap();
        let buffer = MockDeZSet::new();
        let mut parser = format
            .new_parser(
                "BaseConsumer",
                &buffer,
                &serde_yaml::from_str::<serde_yaml::Value>(format_config_yaml).unwrap(),
            )
            .unwrap();

        // Consumer thread: read from output topic, deserialize to a shared buffer.
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
                    .set("bootstrap.servers", &default_redpanda_server())
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
                        None => {
                            // println!("poll returned None");
                        }
                        Some(Err(e)) => {
                            panic!("poll returned error: {e}");
                        }
                        Some(Ok(message)) => {
                            // println!("received {} bytes", message.payload().unwrap().len());
                            // message.payload().map(|payload| consumer.input(payload));

                            if let Some(payload) = message.payload() {
                                parser.input_chunk(payload);
                            }
                        }
                    }
                }
            })
            .unwrap();

        Self {
            thread_handle: Some(thread_handle),
            buffer,
            shutdown_flag,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.buffer.state().flushed.len()
    }

    pub fn clear(&self) {
        self.buffer.state().reset()
    }

    pub fn wait_for_output_unordered(&self, data: &[Vec<TestStruct>]) {
        let num_records: usize = data.iter().map(Vec::len).sum();

        // println!("waiting for {num_records} records");
        wait(|| self.len() == num_records, None);
        //println!("{num_records} records received: {:?}",
        // received_data.lock().unwrap().iter().map(|r| r.id).collect::<Vec<_>>());

        let mut expected = data
            .iter()
            .flat_map(|data| data.iter())
            .cloned()
            .collect::<Vec<_>>();
        expected.sort();

        let mut received = self.buffer.state().flushed.clone();
        received.sort();
        assert_eq!(
            expected.into_iter().map(|x| (x, true)).collect::<Vec<_>>(),
            received
        );
    }
}
