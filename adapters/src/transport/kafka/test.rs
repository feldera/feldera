use crate::{
    test::{
        generate_test_batches,
        kafka::{BufferConsumer, KafkaResources, TestProducer},
        mock_input_pipeline, test_circuit, wait, MockDeZSet, TestStruct, TEST_LOGGER,
    },
    Controller, PipelineConfig,
};
use log::LevelFilter;
use proptest::prelude::*;
use std::{thread::sleep, time::Duration};

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
            assert!(polarity);
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
        let config_str = r#"
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
"#;

        println!("Building input pipeline");

        let (endpoint, _consumer, zset) = mock_input_pipeline::<TestStruct>(
            "test_input",
            serde_yaml::from_str(config_str).unwrap(),
        );

        endpoint.start().unwrap();

        let producer = TestProducer::new();

        println!("Test: Receive from a topic with a single partition");

        // Send data to a topic with a single partition;
        // Make sure all records arrive in the original order.
        producer.send_to_topic(&data, "input_test_topic1");

        wait_for_output_ordered(&zset, &data);
        zset.reset();

        println!("Test: Receive from a topic with multiple partitions");

        // Send data to a topic with multiple partitions.
        // Make sure all records are delivered, but not necessarily in the original order.
        producer.send_to_topic(&data, "input_test_topic2");

        wait_for_output_unordered(&zset, &data);
        zset.reset();

        println!("Test: pause/resume");
        //println!("records before pause: {}", zset.state().flushed.len());

        // Paused endpoint shouldn't receive any data.
        endpoint.pause().unwrap();
        sleep(Duration::from_millis(1000));

        kafka_resources.add_partition("input_test_topic2");

        producer.send_to_topic(&data, "input_test_topic2");
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

        producer.send_to_topic(&data, "input_test_topic2");
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
        let config_str = r#"
inputs:
    test_input1:
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                auto.offset.reset: "earliest"
                group.instance.id: "group0"
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
"#;

        println!("Creating circuit");
        let (circuit, catalog) = test_circuit(4);

        println!("Starting controller");
        let config: PipelineConfig = serde_yaml::from_str(config_str).unwrap();

        let controller = Controller::with_config(
            circuit,
            catalog,
            &config,
            Box::new(|e| panic!("error: {e}"))
        ).unwrap();

        let buffer_consumer = BufferConsumer::new("end_to_end_test_output_topic");

        let producer = TestProducer::new();
        producer.send_to_topic(&data, "end_to_end_test_input_topic");

        // Start controller.
        controller.start();

        // Wait for output buffer to contain all of `data`.

        buffer_consumer.wait_for_output_unordered(&data);

        drop(buffer_consumer);

        controller.stop().unwrap();
        sleep(Duration::from_millis(100));

        println!("Delete Kafka resources");
        drop(kafka_resources);
    }
}
