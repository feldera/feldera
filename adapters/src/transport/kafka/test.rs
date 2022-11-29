use crate::{
    test::{
        generate_test_batches, mock_input_pipeline, test_circuit, wait, MockDeZSet, TestStruct,
    },
    Controller, ControllerConfig,
};
use csv::{ReaderBuilder as CsvReaderBuilder, WriterBuilder as CsvWriterBuilder};
use futures::executor::block_on;
use log::{LevelFilter, Log, Metadata, Record};
use proptest::prelude::*;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::{FromClientConfig, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer},
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    ClientConfig, Message,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    thread::sleep,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
    topics: Vec<String>,
}

/// An object that creates a couple of topics on startup and deletes them
/// on drop.  Helps make sure that test runs don't leave garbage behind.
impl KafkaResources {
    fn create_topics(topics: &[(&str, i32)]) -> Self {
        let mut admin_config = ClientConfig::new();
        admin_config
            .set("bootstrap.servers", "localhost")
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

        Self {
            admin_client,
            topics: topics
                .iter()
                .map(|(topic_name, _)| topic_name.to_string())
                .collect::<Vec<_>>(),
        }
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
        let topic_names = self
            .topics
            .iter()
            .map(|topic_name| &**topic_name)
            .collect::<Vec<_>>();
        let _ = block_on(
            self.admin_client
                .delete_topics(&topic_names, &AdminOptions::new()),
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
    fn proptest_kafka_input(data in generate_test_batches(100, 1000)) {

        let _ = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let kafka_resources = KafkaResources::create_topics(&[("input_test_topic1", 1), ("input_test_topic2", 2)]);

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
        topics: [input_test_topic1, input_test_topic2]
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
        send_to_topic(&producer, &data, "input_test_topic1");

        wait_for_output_ordered(&zset, &data);
        zset.reset();

        println!("Test: Receive from a topic with multiple partitions");

        // Send data to a topic with multiple partitions.
        // Make sure all records are delivered, but not necessarily in the original order.
        send_to_topic(&producer, &data, "input_test_topic2");

        wait_for_output_unordered(&zset, &data);
        zset.reset();

        println!("Test: pause/resume");
        //println!("records before pause: {}", zset.state().flushed.len());

        // Paused endpoint shouldn't receive any data.
        endpoint.pause().unwrap();
        sleep(Duration::from_millis(1000));

        kafka_resources.add_partition("input_test_topic2");

        send_to_topic(&producer, &data, "input_test_topic2");
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

        send_to_topic(&producer, &data, "input_test_topic2");
        sleep(Duration::from_millis(1000));
        assert_eq!(zset.state().flushed.len(), 0);

        sleep(Duration::from_millis(1000));

        println!("Delete Kafka resources");
        drop(kafka_resources);
    }

    #[test]
    fn proptest_kafka_end_to_end(data in generate_test_batches(100, 1000)) {
            let _ = log::set_logger(&TEST_LOGGER);
            log::set_max_level(LevelFilter::Debug);

            // Create topics.
            let kafka_resources = KafkaResources::create_topics(&[("end_to_end_test_input_topic", 1), ("end_to_end_test_output_topic", 1)]);

            // Create controller.

            // auto.offset.reset: "earliest" - guarantees that on startup the
            // consumer will observe all messages sent by the producer even if
            // the producer starts earlier (the consumer won't start until the
            // rebalancing protocol kicks in).
            let config_str = format!(
                r#"
inputs:
    test_input1:
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                auto.offset.reset: "earliest"
                topics: [end_to_end_test_input_topic]
                log_level: debug
        format:
            name: csv
            config:
                input_stream: test_input1
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                topic: end_to_end_test_output_topic
                max_inflight_messages: 0
        format:
            name: csv
"#);

            println!("Creating circuit");
            let (circuit, catalog) = test_circuit(4);

            println!("Starting controller");
            let config: ControllerConfig = serde_yaml::from_str(&config_str).unwrap();

            let controller = Controller::with_config(
                circuit,
                catalog,
                &config,
                Box::new(|e| panic!("error: {e}")),
                )
                .unwrap();

            let received_data: Arc<Mutex<Vec<TestStruct>>> = Arc::new(Mutex::new(Vec::new()));
            let received_data_clone = received_data.clone();

            let shutdown_flag = Arc::new(AtomicBool::new(false));
            let shutdown_flag_clone = shutdown_flag.clone();

            // Consumer thread: read from output topic, deserialize to a shared buffer.
            let consumer_handle = thread::Builder::new().name("test consumer".to_string()).spawn(move || {
                let group_id = format!(
                    "test_group_{}",
                    SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    );

                let kafka_consumer = ClientConfig::new()
                    .set("bootstrap.servers", "localhost")
                    .set("enable.auto.commit", "true")
                    .set("enable.auto.offset.store", "true")
                    .set("auto.offset.reset", "earliest")
                    .set("group.id", &group_id)
                    .create::<BaseConsumer>()
                    .unwrap();

                kafka_consumer.subscribe(&["end_to_end_test_output_topic"]).unwrap();

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
                                let mut builder = CsvReaderBuilder::new();
                                builder.has_headers(false);
                                let mut reader = builder.from_reader(payload);
                                let mut received_data = received_data_clone.lock().unwrap();
                                // let mut num_received = 0;
                                for (record, w) in reader.deserialize::<(TestStruct, i32)>().map(Result::unwrap) {
                                    // num_received += 1;
                                    assert_eq!(w, 1);
                                    // println!("received record: {:?}", record);
                                    received_data.push(record);
                                }
                                // println!("received {num_received} records");
                            }
                        }
                    }
                }

            }).unwrap();

            // Producer: write `data` to input topic.
            let kafka_producer = ClientConfig::new()
                .set("bootstrap.servers", "localhost")
                .set("message.timeout.ms", "0") // infinite timeout
                .create::<ThreadedProducer<_>>()
                .unwrap();

            send_to_topic(&kafka_producer, &data, "end_to_end_test_input_topic");

            // Start controller.
            controller.start();

            // Wait for output buffer to contain all of `data`.
            {
                let num_records: usize = data.iter().map(Vec::len).sum();

                println!("waiting for {num_records} records");
                wait(|| received_data.lock().unwrap().len() == num_records, None);
                //println!("{num_records} records received: {:?}", received_data.lock().unwrap().iter().map(|r| r.id).collect::<Vec<_>>());

                let mut expected = data.iter().flat_map(|data| data.iter()).cloned().collect::<Vec<_>>();
                expected.sort();

                let mut received = received_data.lock().unwrap().clone();
                received.sort();
                assert_eq!(expected, received);
            }

            shutdown_flag.store(true, Ordering::Release);
            consumer_handle.join().unwrap();

            controller.stop().unwrap();
            sleep(Duration::from_millis(100));

            println!("Delete Kafka resources");
            drop(kafka_resources);
    }
}
