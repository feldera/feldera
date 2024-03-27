use crate::{
    test::{
        generate_test_batches,
        kafka::{BufferConsumer, KafkaResources, TestProducer},
        mock_input_pipeline, test_circuit, wait, MockDeZSet, TestStruct, DEFAULT_TIMEOUT_MS,
    },
    Controller, PipelineConfig,
};
use env_logger::Env;
use log::info;
use proptest::prelude::*;
use std::{
    io::Write,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};

/// Wait to receive all records in `data` in the same order.
fn wait_for_output_ordered(zset: &MockDeZSet<TestStruct, TestStruct>, data: &[Vec<TestStruct>]) {
    let num_records: usize = data.iter().map(Vec::len).sum();

    wait(
        || zset.state().flushed.len() == num_records,
        DEFAULT_TIMEOUT_MS,
    );

    for (i, val) in data.iter().flat_map(|data| data.iter()).enumerate() {
        assert_eq!(zset.state().flushed[i].unwrap_insert(), val);
    }
}

/// Wait to receive all records in `data` in some order.
fn wait_for_output_unordered(zset: &MockDeZSet<TestStruct, TestStruct>, data: &[Vec<TestStruct>]) {
    let num_records: usize = data.iter().map(Vec::len).sum();

    wait(
        || zset.state().flushed.len() == num_records,
        DEFAULT_TIMEOUT_MS,
    );

    let mut data_sorted = data
        .iter()
        .flat_map(|data| data.clone().into_iter())
        .collect::<Vec<_>>();
    data_sorted.sort();

    let mut zset_sorted = zset
        .state()
        .flushed
        .iter()
        .map(|upd| upd.unwrap_insert().clone())
        .collect::<Vec<_>>();
    zset_sorted.sort();

    assert_eq!(zset_sorted, data_sorted);
}

fn init_test_logger() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .format(move |buf, record| {
            let t = chrono::Utc::now();
            let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S"));
            writeln!(
                buf,
                "{t} {} {}",
                buf.default_styled_level(record.level()),
                record.args()
            )
        })
        .try_init();
}

#[test]
fn test_kafka_output_errors() {
    init_test_logger();

    info!("test_kafka_output_errors: Test invalid Kafka broker address");

    let config_str = r#"
name: test
workers: 4
inputs:
outputs:
    test_output:
        stream: test_output1
        transport:
            name: kafka_output
            config:
                bootstrap.servers: localhost:11111
                topic: end_to_end_test_output_topic
                max_inflight_messages: 0
        format:
            name: csv
"#;

    info!("test_kafka_output_errors: Creating circuit");

    info!("test_kafka_output_errors: Starting controller");
    let config: PipelineConfig = serde_yaml::from_str(config_str).unwrap();

    match Controller::with_config(
        |workers| Ok(test_circuit(workers)),
        &config,
        Box::new(|e| panic!("error: {e}")),
    ) {
        Ok(_) => panic!("expected an error"),
        Err(e) => info!("test_kafka_output_errors: error: {e}"),
    }
}

fn kafka_end_to_end_test(
    test_name: &str,
    format: &str,
    format_config: &str,
    message_max_bytes: usize,
    data: Vec<Vec<TestStruct>>,
) {
    init_test_logger();
    let input_topic = format!("{test_name}_input_topic");
    let output_topic = format!("{test_name}_output_topic");

    // Create topics.
    let _kafka_resources = KafkaResources::create_topics(&[(&input_topic, 1), (&output_topic, 1)]);

    // Create controller.

    // auto.offset.reset: "earliest" - guarantees that on startup the
    // consumer will observe all messages sent by the producer even if
    // the producer starts earlier (the consumer won't start until the
    // rebalancing protocol kicks in).
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: kafka_input
            config:
                auto.offset.reset: "earliest"
                group.instance.id: "{test_name}"
                topics: [{input_topic}]
                log_level: debug
        format:
            name: csv
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: kafka_output
            config:
                topic: {output_topic}
                max_inflight_messages: 0
                message.max.bytes: "{message_max_bytes}"
        format:
            name: {format}
            config:
                {format_config}
"#
    );

    info!("{test_name}: Creating circuit. Config {config_str}");

    info!("{test_name}: Starting controller");
    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let test_name_clone = test_name.to_string();

    let controller = Controller::with_config(
        |workers| Ok(test_circuit(workers)),
        &config,
        Box::new(move |e| if running_clone.load(Ordering::Acquire) {
            panic!("{test_name_clone}: error: {e}")
        } else {
            info!("{test_name_clone}: error during shutdown (likely caused by Kafka topics being deleted): {e}")
        }),
    )
    .unwrap();

    let buffer_consumer = BufferConsumer::new(&output_topic, format, format_config);

    info!("{test_name}: Sending inputs");
    let producer = TestProducer::new();
    producer.send_to_topic(&data, &input_topic);

    info!("{test_name}: Starting controller");
    // Start controller.
    controller.start();

    // Wait for output buffer to contain all of `data`.

    info!("{test_name}: Waiting for output");
    buffer_consumer.wait_for_output_unordered(&data);

    drop(buffer_consumer);

    controller.stop().unwrap();

    // Endpoint threads might still be running (`controller.stop()` doesn't wait
    // for them to terminate).  Once `KafkaResources` is dropped, these threads
    // may start throwing errors due to deleted Kafka topics.  Make sure these
    // errors don't cause panics.
    running.store(false, Ordering::Release);
}

fn test_kafka_input(data: Vec<Vec<TestStruct>>, topic1: &str, topic2: &str) {
    init_test_logger();

    let kafka_resources = KafkaResources::create_topics(&[(topic1, 1), (topic2, 2)]);

    info!("proptest_kafka_input: Test: Specify invalid Kafka broker address");

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: kafka_input
    config:
        bootstrap.servers: localhost:11111
        auto.offset.reset: "earliest"
        topics: [{topic1}, {topic2}]
        log_level: debug
format:
    name: csv
"#
    );

    match mock_input_pipeline::<TestStruct, TestStruct>(serde_yaml::from_str(&config_str).unwrap())
    {
        Ok(_) => panic!("expected an error"),
        Err(e) => info!("proptest_kafka_input: Error: {e}"),
    };

    info!("proptest_kafka_input: Test: Specify invalid Kafka topic name");

    let config_str = r#"
stream: test_input
transport:
    name: kafka_input
    config:
        auto.offset.reset: "earliest"
        topics: [input_test_topic1, this_topic_does_not_exist]
        log_level: debug
format:
    name: csv
"#;

    match mock_input_pipeline::<TestStruct, TestStruct>(serde_yaml::from_str(config_str).unwrap()) {
        Ok(_) => panic!("expected an error"),
        Err(e) => info!("proptest_kafka_input: Error: {e}"),
    };

    // auto.offset.reset: "earliest" - guarantees that on startup the
    // consumer will observe all messages sent by the producer even if
    // the producer starts earlier (the consumer won't start until the
    // rebalancing protocol kicks in).
    let config_str = format!(
        r#"
stream: test_input
transport:
    name: kafka_input
    config:
        auto.offset.reset: "earliest"
        topics: [{topic1}, {topic2}]
        log_level: debug
format:
    name: csv
"#
    );

    info!("proptest_kafka_input: Building input pipeline");

    let (endpoint, _consumer, zset) =
        mock_input_pipeline::<TestStruct, TestStruct>(serde_yaml::from_str(&config_str).unwrap())
            .unwrap();

    endpoint.start(0).unwrap();

    let producer = TestProducer::new();

    info!("proptest_kafka_input: Test: Receive from a topic with a single partition");

    // Send data to a topic with a single partition;
    // Make sure all records arrive in the original order.
    producer.send_to_topic(&data, topic1);

    wait_for_output_ordered(&zset, &data);
    zset.reset();

    info!("proptest_kafka_input: Test: Receive from a topic with multiple partitions");

    // Send data to a topic with multiple partitions.
    // Make sure all records are delivered, but not necessarily in the original
    // order.
    producer.send_to_topic(&data, topic2);

    wait_for_output_unordered(&zset, &data);
    zset.reset();

    info!("proptest_kafka_input: Test: pause/resume");
    //println!("records before pause: {}", zset.state().flushed.len());

    // Paused endpoint shouldn't receive any data.
    endpoint.pause().unwrap();
    sleep(Duration::from_millis(1000));

    kafka_resources.add_partition(topic2);

    producer.send_to_topic(&data, topic2);
    sleep(Duration::from_millis(1000));
    assert_eq!(zset.state().flushed.len(), 0);

    // Receive everything after unpause.
    endpoint.start(0).unwrap();
    wait_for_output_unordered(&zset, &data);

    zset.reset();

    info!("proptest_kafka_input: Test: Disconnect");
    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));

    producer.send_to_topic(&data, topic2);
    sleep(Duration::from_millis(1000));
    assert_eq!(zset.state().flushed.len(), 0);
}

/// If Kafka tests are going to fail because the server is not running or
/// not functioning properly, it's good to fail quickly without printing a
/// thousand records as part of the failure.
#[test]
fn kafka_input_trivial() {
    test_kafka_input(Vec::new(), "trivial_test_topic1", "trivial_test_topic2");
}

/// Test the output endpoint buffer.
#[test]
fn buffer_test() {
    init_test_logger();
    let input_topic = format!("buffer_test_input_topic");
    let output_topic = format!("buffer_test_output_topic");

    // Total number of records to push.
    let num_records = 1_000;

    // Output buffer size.
    // Requires: `num_records % buffer_size = 0`.
    let buffer_size = 100;

    // Buffer timeout.  Must be much longer than what it takes the pipeline
    // to process `buffer_size` records.
    let buffer_timeout_ms = 10_000;

    // Create topics.
    let _kafka_resources = KafkaResources::create_topics(&[(&input_topic, 1), (&output_topic, 1)]);

    // Create controller.
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: kafka_input
            config:
                auto.offset.reset: "earliest"
                group.instance.id: "buffer_test"
                topics: [{input_topic}]
                log_level: debug
        format:
            name: csv
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: kafka_output
            config:
                topic: {output_topic}
                max_inflight_messages: 0
                message.max.bytes: "1000000"
        format:
            name: csv
            config:
        enable_buffer: true
        max_buffer_size_records: {buffer_size}
        max_buffer_time_millis: {buffer_timeout_ms}
"#
    );

    info!("buffer_test: Creating circuit. Config {config_str}");

    info!("buffer_test: Starting controller");
    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();

    let controller = Controller::with_config(
        |workers| Ok(test_circuit(workers)),
        &config,
        Box::new(move |e| if running_clone.load(Ordering::Acquire) {
            panic!("buffer_test: error: {e}")
        } else {
            info!("buffer_test: error during shutdown (likely caused by Kafka topics being deleted): {e}")
        }),
    )
        .unwrap();

    let buffer_consumer = BufferConsumer::new(&output_topic, "csv", "");

    info!("buffer_test: Sending inputs");
    let producer = TestProducer::new();

    info!("buffer_test: Starting controller");
    // Start controller.
    controller.start();

    // Send `num_records+1` records to the pipeline, make sure they show up in groups of `buffer_size`.
    // Note: we push `num_records + 1` to keep one leftover record in the buffer, which will get
    // pushed out on timeout later.
    let mut buffer = vec![];
    for i in 0..num_records + 1 {
        // eprintln!("{i}");
        let val = TestStruct {
            id: i,
            b: false,
            i: None,
            s: "foo".to_string(),
        };

        producer.send_to_topic(&[vec![val.clone()]], &input_topic);
        buffer.push(val);

        if buffer.len() >= buffer_size {
            // eprintln!("waiting");
            buffer_consumer.wait_for_output_unordered(&[std::mem::take(&mut buffer)]);
            buffer_consumer.clear();
        } else {
            assert_eq!(buffer_consumer.len(), 0);
        }
    }

    info!("waiting for the leftover records to be pushed out");
    buffer_consumer.wait_for_output_unordered(&[std::mem::take(&mut buffer)]);
    buffer_consumer.clear();

    drop(buffer_consumer);

    controller.stop().unwrap();

    // Endpoint threads might still be running (`controller.stop()` doesn't wait
    // for them to terminate).  Once `KafkaResources` is dropped, these threads
    // may start throwing errors due to deleted Kafka topics.  Make sure these
    // errors don't cause panics.
    running.store(false, Ordering::Release);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]

    #[test]
    fn proptest_kafka_input(data in generate_test_batches(0, 100, 1000)) {
        test_kafka_input(data, "input_test_topic1", "input_test_topic2");
    }

    #[test]
    fn proptest_kafka_end_to_end_csv_large(data in generate_test_batches(0, 30, 1000)) {
        kafka_end_to_end_test("proptest_kafka_end_to_end_csv_large", "csv", "", 1000000, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_csv_small(data in generate_test_batches(0, 30, 1000)) {
        kafka_end_to_end_test("proptest_kafka_end_to_end_csv_small", "csv", "", 1500, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_json_small(data in generate_test_batches(0, 30, 1000)) {
        kafka_end_to_end_test("proptest_kafka_end_to_end_json_small", "json", "", 2048, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_json_array_small(data in generate_test_batches(0, 30, 1000)) {
        kafka_end_to_end_test("proptest_kafka_end_to_end_json_array_small", "json", "array: true", 5000, data);
    }

}
