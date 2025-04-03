use crate::{
    test::{
        generate_test_batches, init_test_logger,
        kafka::{BufferConsumer, KafkaResources, TestProducer},
        mock_input_pipeline, test_circuit, wait_for_output_ordered, wait_for_output_unordered,
        TestStruct,
    },
    Controller, PipelineConfig,
};
use feldera_types::{
    config::{
        default_max_batch_size, default_max_queued_records, ConnectorConfig, FormatConfig,
        InputEndpointConfig, OutputBufferConfig, TransportConfig,
    },
    program_schema::Relation,
    transport::kafka::{
        default_group_join_timeout_secs, KafkaInputConfig, KafkaLogLevel, KafkaStartFromConfig,
    },
};
use parquet::data_type::AsBytes;
use proptest::prelude::*;
use rdkafka::message::{BorrowedMessage, Header, Headers};
use rdkafka::Message;
use serde_yaml::Mapping;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};
use tracing::info;

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
        format:
            name: csv
"#;

    info!("test_kafka_output_errors: Creating circuit");

    info!("test_kafka_output_errors: Starting controller");
    let config: PipelineConfig = serde_yaml::from_str(config_str).unwrap();

    match Controller::with_config(
        |workers| Ok(test_circuit::<TestStruct>(workers, &TestStruct::schema())),
        &config,
        Box::new(|e| panic!("error: {e}")),
    ) {
        Ok(_) => panic!("expected an error"),
        Err(e) => info!("test_kafka_output_errors: error: {e}"),
    }
}

/// Creates `topic` with `partitions` partitions. Then produces `data` to it and
/// consumes it back with a consumer that starts out at `starts_from`, and
/// checks that everything works out OK.
///
/// With `partitions > 0`, `start_from` shouldn't specify offsets (unless
/// they're meant to cause a panic) because it's relatively hard to predict
/// which records go to which partition.
fn test_offset(
    data: Vec<Vec<TestStruct>>,
    topic: &str,
    partitions: i32,
    start_from: KafkaStartFromConfig,
) {
    let _kafka = KafkaResources::create_topics(&[(topic, partitions)]);

    let config = InputEndpointConfig {
        stream: Cow::from("test_input"),
        connector_config: ConnectorConfig {
            transport: TransportConfig::KafkaInput(KafkaInputConfig {
                kafka_options: {
                    let mut kafka_options = BTreeMap::new();
                    let auto_offset_reset = match start_from {
                        KafkaStartFromConfig::Earliest => Some("earliest"),
                        KafkaStartFromConfig::Latest => Some("latest"),
                        KafkaStartFromConfig::Offsets(_) => None,
                    };
                    if let Some(auto_offset_reset) = auto_offset_reset {
                        kafka_options.insert("auto.offset.reset".into(), auto_offset_reset.into());
                    }
                    kafka_options.insert("bootstrap.servers".into(), "localhost:9092".into());
                    kafka_options.insert("group.id".into(), "test-client".into());
                    kafka_options
                },
                topic: topic.into(),
                log_level: Some(KafkaLogLevel::Debug),
                group_join_timeout_secs: default_group_join_timeout_secs(),
                poller_threads: None,
                start_from: start_from.clone(),
            }),
            format: Some(FormatConfig {
                name: Cow::from("csv"),
                config: serde_yaml::Value::Mapping(Mapping::default()),
            }),
            index: None,
            output_buffer_config: OutputBufferConfig::default(),
            max_batch_size: default_max_batch_size(),
            max_queued_records: default_max_queued_records(),
            paused: false,
            labels: Vec::new(),
            start_after: None,
        },
    };

    info!("proptest_kafka_input_offset: Building input pipeline");

    let producer = TestProducer::new();
    let expected = match &start_from {
        KafkaStartFromConfig::Earliest | KafkaStartFromConfig::Latest => Some(data.as_slice()),
        KafkaStartFromConfig::Offsets(vec) => data.get(vec[0] as usize..),
    };

    // Front load data if auto.offset.reset: earliest
    if start_from == KafkaStartFromConfig::Earliest {
        producer.send_to_topic(data.as_slice(), topic);
        producer.send_string("", topic);
    }

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<TestStruct, TestStruct>(config, Relation::empty(), false).unwrap();

    endpoint.extend();

    // If auto.offset.reset: latest, send data after starting the pipeline.
    if start_from == KafkaStartFromConfig::Latest {
        producer.send_to_topic(data.as_slice(), topic);
        producer.send_string("", topic);
    }

    info!("proptest_kafka_input: Test: Receive from topic");

    let flush = || {
        endpoint.queue();
    };
    if let Some(expected) = expected {
        wait_for_output_unordered(&zset, expected, flush);
    } else {
        sleep(Duration::from_millis(1000));
        panic!("the connector should have already panicked but it didn't");
    }
    zset.reset();

    info!("proptest_kafka_input: Test: Disconnect");

    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));
    flush();
    assert_eq!(zset.state().flushed.len(), 0);
}

#[test]
#[should_panic(expected = "provided offset '3' not currently in partition '0'")]
fn test_kafka_input_offset_doesnt_exist() {
    test_offset(
        Vec::new(),
        "test_kafka_input_offset_doesnt_exist",
        1,
        KafkaStartFromConfig::Offsets(vec![3]),
    );
}

#[test]
#[should_panic(expected = "provided offset '3' not currently in partition '0'")]
fn test_kafka_input_offset_doesnt_exist_2() {
    test_offset(
        Vec::new(),
        "test_kafka_input_offset_doesnt_exist_2",
        3,
        KafkaStartFromConfig::Offsets(vec![3, 4, 5]),
    );
}

fn testdata() -> Vec<Vec<TestStruct>> {
    vec![
        vec![TestStruct {
            id: 0,
            b: true,
            i: Some(0),
            s: "0".to_owned(),
        }],
        vec![
            TestStruct {
                id: 1,
                b: true,
                i: Some(1),
                s: "1".to_owned(),
            },
            TestStruct {
                id: 2,
                b: true,
                i: Some(2),
                s: "2".to_owned(),
            },
            TestStruct {
                id: 3,
                b: true,
                i: Some(3),
                s: "3".to_owned(),
            },
        ],
    ]
}

#[test]
fn test_kafka_input_offset_earliest() {
    test_offset(
        testdata(),
        "test_kafka_input_offset_earliest",
        1,
        KafkaStartFromConfig::Earliest,
    );
}

#[test]
fn test_kafka_input_offset_earliest_2() {
    test_offset(
        testdata(),
        "test_kafka_input_offset_earliest_2",
        2,
        KafkaStartFromConfig::Earliest,
    );
}

#[test]
fn test_kafka_input_offset_latest() {
    test_offset(
        testdata(),
        "test_kafka_input_offset_latest",
        1,
        KafkaStartFromConfig::Latest,
    );
}

#[test]
fn test_kafka_input_offset_latest_2() {
    test_offset(
        testdata(),
        "test_kafka_input_offset_latest_2",
        2,
        KafkaStartFromConfig::Latest,
    );
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
        |workers| Ok(test_circuit::<TestStruct>(workers, &TestStruct::schema())),
        &config,
        Box::new(move |e| if running_clone.load(Ordering::Acquire) {
            panic!("{test_name_clone}: error: {e}")
        } else {
            info!("{test_name_clone}: error during shutdown (likely caused by Kafka topics being deleted): {e}")
        }),
    )
    .unwrap();

    let buffer_consumer = BufferConsumer::new(&output_topic, format, format_config, None);

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

fn test_kafka_input(data: Vec<Vec<TestStruct>>, topic: &str, poller_threads: usize) {
    init_test_logger();

    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);

    info!("proptest_kafka_input: Test: Specify invalid Kafka broker address");

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: kafka_input
    config:
        topic: {topic}
        log_level: debug
        kafka_options:
            bootstrap.servers: localhost:11111
            auto.offset.reset: "earliest"
format:
    name: csv
"#
    );

    match mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
        false,
    ) {
        Ok(_) => panic!("expected an error"),
        Err(e) => info!("proptest_kafka_input: Error: {e}"),
    };

    info!("proptest_kafka_input: Test: Specify invalid Kafka topic name");

    let config_str = r#"
stream: test_input
transport:
    name: kafka_input
    config:
        topic: this_topic_does_not_exist
        log_level: debug
        kafka_options:
            auto.offset.reset: "earliest"
format:
    name: csv
"#;

    match mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(config_str).unwrap(),
        Relation::empty(),
        false,
    ) {
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
        topic: {topic}
        log_level: debug
        poller_threads: {poller_threads}
        kafka_options:
            auto.offset.reset: "earliest"
format:
    name: csv
max_batch_size: 10000000
"#
    );

    info!("proptest_kafka_input: Building input pipeline");

    let (endpoint, _consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
        false,
    )
    .unwrap();

    endpoint.extend();

    let producer = TestProducer::new();

    info!("proptest_kafka_input: Test: Receive from a topic with a single partition");

    // Send data to a topic with a single partition;
    producer.send_to_topic(&data, topic);

    let flush = || {
        endpoint.queue();
    };
    if poller_threads == 1 {
        // Make sure all records arrive in the original order.
        wait_for_output_ordered(&zset, &data, flush);
    } else {
        wait_for_output_unordered(&zset, &data, flush);
    }
    zset.reset();

    info!("proptest_kafka_input: Test: Disconnect");
    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));
    flush();

    producer.send_to_topic(&data, topic);
    sleep(Duration::from_millis(1000));
    flush();
    assert_eq!(zset.state().flushed.len(), 0);
}

/// If Kafka tests are going to fail because the server is not running or
/// not functioning properly, it's good to fail quickly without printing a
/// thousand records as part of the failure.
#[test]
fn kafka_input_trivial() {
    test_kafka_input(Vec::new(), "trivial_test_topic", 1);
}

/// If Kafka tests are going to fail because the server is not running or
/// not functioning properly, it's good to fail quickly without printing a
/// thousand records as part of the failure.
#[test]
fn kafka_input_trivial_threaded() {
    test_kafka_input(Vec::new(), "threaded_trivial_test_topic1", 3);
}

/// Test the output endpoint buffer.
#[test]
fn buffer_test() {
    init_test_logger();
    let input_topic = "buffer_test_input_topic".to_string();
    let output_topic = "buffer_test_output_topic".to_string();

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
                message.max.bytes: "1000000"
                headers:
                    - key: header1
                      value: "foobar"
                    - key: header2
                      value: [1,2,3,4,5]
        format:
            name: csv
            config:
        enable_output_buffer: true
        max_output_buffer_size_records: {buffer_size}
        max_output_buffer_time_millis: {buffer_timeout_ms}
"#
    );

    info!("buffer_test: Creating circuit. Config {config_str}");

    info!("buffer_test: Starting controller");
    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();

    let controller = Controller::with_config(
        |workers| Ok(test_circuit::<TestStruct>(workers, &TestStruct::schema())),
        &config,
        Box::new(move |e| if running_clone.load(Ordering::Acquire) {
            panic!("buffer_test: error: {e}")
        } else {
            info!("buffer_test: error during shutdown (likely caused by Kafka topics being deleted): {e}")
        }),
    )
        .unwrap();

    let cb = Box::new(|message: &BorrowedMessage| {
        let headers = message.headers().unwrap();
        assert_eq!(headers.count(), 2);
        assert_eq!(
            headers.try_get(0).unwrap(),
            Header {
                key: "header1",
                value: Some(b"foobar".as_bytes())
            }
        );
        assert_eq!(
            headers.try_get(1).unwrap(),
            Header {
                key: "header2",
                value: Some([1u8, 2, 3, 4, 5].as_bytes())
            }
        );
    });
    let buffer_consumer = BufferConsumer::new(&output_topic, "csv", "", Some(cb));

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
        test_kafka_input(data, "input_test_topic1", 1);
    }
    #[test]
    fn proptest_kafka_input_threaded(data in generate_test_batches(0, 100, 1000)) {
        test_kafka_input(data, "threaded_test_topic1", 3);
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
