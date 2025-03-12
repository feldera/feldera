use crate::{
    test::{
        generate_test_batches, init_test_logger,
        kafka::{BufferConsumer, KafkaResources, TestProducer},
        mock_input_pipeline, test_circuit, wait_for_output_ordered, wait_for_output_unordered,
        TestStruct,
    },
    Controller, PipelineConfig,
};
use feldera_types::{program_schema::Relation, transport::kafka::KafkaStartFromConfig};
use parquet::data_type::AsBytes;
use proptest::prelude::*;
use rdkafka::message::{BorrowedMessage, Header, Headers};
use rdkafka::Message;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};
use tracing::info;

enum AutoOffsetReset {
    Latest,
    Earliest,
}

impl AsRef<str> for AutoOffsetReset {
    fn as_ref(&self) -> &str {
        match self {
            AutoOffsetReset::Latest => "latest",
            AutoOffsetReset::Earliest => "earliest",
        }
    }
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

#[test]
fn test_kafka_start_from_error() {
    info!("test_start_from_error: Test invalid, topic listed in start from is not listed in topics list");

    let config_str = r#"
name: test
workers: 4
inputs:
outputs:
    test_input:
        stream: test_input1
        transport:
            name: kafka_input
            config:
                bootstrap.servers: localhost:9092
                topics:
                - a
                start_from:
                - topic: b
                  partition: 0
                  offset: 2
                - topic: c
                  partition: 0
                  offset: 2
        format:
            name: csv
"#;

    info!("test_kafka_start_from_errors: Creating circuit");

    info!("test_kafka_start_from_errors: Starting controller");
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

fn test_offset(
    topic1_data: Vec<Vec<TestStruct>>,
    mut topic2_data: Vec<Vec<TestStruct>>,
    topic1: &str,
    topic2: &str,
    topic1_cfg: KafkaStartFromConfig,
    auto_offset_reset: AutoOffsetReset,
) {
    let _kafka = KafkaResources::create_topics(&[(topic1, 1), (topic2, 2)]);

    let config_str = format!(
        r#"
stream: test_input
transport:
  name: kafka_input
  config:
    group.id: test-client
    auto.offset.reset: {}
    bootstrap.servers: localhost:9092
    topics:
    - {topic1}
    - {topic2}
    log_level: debug
    start_from:
    - topic: {topic1}
      partition: {}
      offset: {}
format:
  name: csv
"#,
        auto_offset_reset.as_ref(),
        topic1_cfg.partition,
        topic1_cfg.offset
    );

    info!("proptest_kafka_input_offset: Building input pipeline");

    let producer = TestProducer::new();
    let mut expected: Vec<_> = topic1_data
        .clone()
        .into_iter()
        .skip(topic1_cfg.offset as usize)
        .collect();

    // Send data to a topic with a single partition
    for datum in topic1_data.into_iter() {
        let x = &[datum];
        producer.send_to_topic(x, topic1);
    }

    // Front load data if auto.offset.reset: earliest
    if matches!(auto_offset_reset, AutoOffsetReset::Earliest) {
        // Send data to a topic with multiple partitions
        for datum in topic2_data.clone().into_iter() {
            let x = &[datum];
            producer.send_to_topic(x, topic2);
        }
        producer.send_string("", topic2);
        expected.append(&mut topic2_data);
    }
    producer.send_string("", topic1);

    let (endpoint, _consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
        false,
    )
    .unwrap();

    endpoint.extend();

    // If auto.offset.reset: latest, send data after starting the pipeline.
    if matches!(auto_offset_reset, AutoOffsetReset::Latest) {
        // Send data to a topic with multiple partitions
        for datum in topic2_data.clone().into_iter() {
            let x = &[datum];
            producer.send_to_topic(x, topic2);
        }
        producer.send_string("", topic2);
        expected.append(&mut topic2_data);
    }

    info!("proptest_kafka_input: Test: Receive from a topic with a single partition");

    let flush = || {
        endpoint.queue();
    };
    wait_for_output_unordered(&zset, &expected, flush);
    zset.reset();

    info!("proptest_kafka_input: Test: Disconnect");
    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));
    flush();
    assert_eq!(zset.state().flushed.len(), 0);
}

#[test]
fn test_kafka_input_offset_trivial_earliest() {
    let topic1 = "test_kafka_input_offset_trivial_earliest_topic1";
    test_offset(
        Vec::new(),
        Vec::new(),
        topic1,
        "test_kafka_input_offset_trivial_earliest_topic2",
        KafkaStartFromConfig {
            topic: topic1.to_owned(),
            partition: 0,
            offset: 3,
        },
        AutoOffsetReset::Earliest,
    );
}

#[test]
fn test_kafka_input_offset_trivial_latest() {
    let topic1 = "test_kafka_input_offset_trivial_latest_topic1";
    test_offset(
        Vec::new(),
        Vec::new(),
        topic1,
        "test_kafka_input_offset_trivial_latest_topic2",
        KafkaStartFromConfig {
            topic: topic1.to_owned(),
            partition: 0,
            offset: 3,
        },
        AutoOffsetReset::Latest,
    );
}

#[test]
fn test_kafka_input_offset_earliest() {
    let topic1 = "test_kafka_input_offset_earliest_topic1";
    let data = vec![
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
    ];

    let data2 = vec![vec![
        TestStruct {
            id: 4,
            b: true,
            i: Some(4),
            s: "4".to_owned(),
        },
        TestStruct {
            id: 5,
            b: true,
            i: Some(5),
            s: "5".to_owned(),
        },
        TestStruct {
            id: 6,
            b: true,
            i: Some(6),
            s: "6".to_owned(),
        },
    ]];

    test_offset(
        data,
        data2,
        topic1,
        "test_kafka_input_offset_earliest_topic2",
        KafkaStartFromConfig {
            topic: topic1.to_owned(),
            partition: 0,
            offset: 1,
        },
        AutoOffsetReset::Earliest,
    );
}

#[test]
fn test_kafka_input_offset_latest() {
    let topic1 = "test_kafka_input_offset_latest_topic1";
    let data = vec![
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
    ];

    let data2 = vec![vec![
        TestStruct {
            id: 4,
            b: true,
            i: Some(4),
            s: "4".to_owned(),
        },
        TestStruct {
            id: 5,
            b: true,
            i: Some(5),
            s: "5".to_owned(),
        },
        TestStruct {
            id: 6,
            b: true,
            i: Some(6),
            s: "6".to_owned(),
        },
    ]];

    test_offset(
        data,
        data2,
        topic1,
        "test_kafka_input_offset_latest_topic2",
        KafkaStartFromConfig {
            topic: topic1.to_owned(),
            partition: 0,
            offset: 1,
        },
        AutoOffsetReset::Latest,
    );
}

// Partitions not specified in the `start_from` config should be ignored.
fn test_kafka_offset_partitions(
    topic: &str,
    data_part1: Vec<Vec<TestStruct>>,
    data_part2: Vec<Vec<TestStruct>>,
    offset: u32,
    auto_offset_reset: AutoOffsetReset,
) {
    let _kafka = KafkaResources::create_topics(&[(topic, 2)]);

    let config_str = format!(
        r#"
stream: test_input
transport:
  name: kafka_input
  config:
    group.id: test-client
    auto.offset.reset: {}
    bootstrap.servers: localhost:9092
    topics:
    - {topic}
    log_level: debug
    start_from:
    - topic: {topic}
      partition: 0
      offset: {}
format:
  name: csv
"#,
        auto_offset_reset.as_ref(),
        offset
    );

    info!("proptest_kafka_offset_partitions: Building input pipeline");

    let producer = TestProducer::new();
    let expected: Vec<_> = data_part1
        .clone()
        .into_iter()
        .skip(offset as usize)
        .collect();

    // Send data to a topic with a single partition
    for datum in data_part1.into_iter() {
        let x = &[datum];
        producer.send_to_topic_partition(x, topic, 0);
    }
    producer.send_string_partition("", topic, 0);

    // Front load data if auto.offset.reset: earliest
    if matches!(auto_offset_reset, AutoOffsetReset::Earliest) {
        // Send data to a topic with multiple partitions
        for datum in data_part2.clone().into_iter() {
            let x = &[datum];
            producer.send_to_topic_partition(x, topic, 1);
        }
        producer.send_string_partition("", topic, 1);
    }

    let (endpoint, _consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
        false,
    )
    .unwrap();

    endpoint.extend();

    // If auto.offset.reset: latest, send data after starting the pipeline.
    if matches!(auto_offset_reset, AutoOffsetReset::Latest) {
        // Send data to a topic with multiple partitions
        for datum in data_part2.clone().into_iter() {
            let x = &[datum];
            producer.send_to_topic_partition(x, topic, 1);
        }
        producer.send_string_partition("", topic, 1);
    }

    info!("proptest_kafka_input: Test: Receive from a topic with a single partition");

    let flush = || {
        endpoint.queue();
    };
    wait_for_output_unordered(&zset, &expected, flush);
    zset.reset();

    info!("proptest_kafka_input: Test: Disconnect");
    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));
    flush();
    assert_eq!(zset.state().flushed.len(), 0);
}

#[test]
fn test_kafka_input_offset_earliest_partition() {
    let topic1 = "test_kafka_input_offset_earliest_partition_topic1";
    let data = vec![
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
    ];

    let data2 = vec![vec![
        TestStruct {
            id: 4,
            b: true,
            i: Some(4),
            s: "4".to_owned(),
        },
        TestStruct {
            id: 5,
            b: true,
            i: Some(5),
            s: "5".to_owned(),
        },
        TestStruct {
            id: 6,
            b: true,
            i: Some(6),
            s: "6".to_owned(),
        },
    ]];

    test_kafka_offset_partitions(topic1, data, data2, 1, AutoOffsetReset::Earliest);
}

#[test]
fn test_kafka_input_offset_latest_partition() {
    let topic1 = "test_kafka_input_offset_latest_partition_topic1";
    let data = vec![
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
    ];

    let data2 = vec![vec![
        TestStruct {
            id: 4,
            b: true,
            i: Some(4),
            s: "4".to_owned(),
        },
        TestStruct {
            id: 5,
            b: true,
            i: Some(5),
            s: "5".to_owned(),
        },
        TestStruct {
            id: 6,
            b: true,
            i: Some(6),
            s: "6".to_owned(),
        },
    ]];

    test_kafka_offset_partitions(topic1, data, data2, 1, AutoOffsetReset::Latest);
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

fn test_kafka_input(data: Vec<Vec<TestStruct>>, topic1: &str, topic2: &str, poller_threads: usize) {
    init_test_logger();

    let _kafka_resources = KafkaResources::create_topics(&[(topic1, 1), (topic2, 2)]);

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
        auto.offset.reset: "earliest"
        topics: [input_test_topic1, this_topic_does_not_exist]
        log_level: debug
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
        auto.offset.reset: "earliest"
        topics: [{topic1}, {topic2}]
        log_level: debug
        poller_threads: {poller_threads}
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
    producer.send_to_topic(&data, topic1);

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

    info!("proptest_kafka_input: Test: Receive from a topic with multiple partitions");

    // Send data to a topic with multiple partitions.
    // Make sure all records are delivered, but not necessarily in the original
    // order.
    producer.send_to_topic(&data, topic2);

    wait_for_output_unordered(&zset, &data, flush);
    zset.reset();

    info!("proptest_kafka_input: Test: Disconnect");
    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));
    flush();

    producer.send_to_topic(&data, topic2);
    sleep(Duration::from_millis(1000));
    flush();
    assert_eq!(zset.state().flushed.len(), 0);
}

/// If Kafka tests are going to fail because the server is not running or
/// not functioning properly, it's good to fail quickly without printing a
/// thousand records as part of the failure.
#[test]
fn kafka_input_trivial() {
    test_kafka_input(Vec::new(), "trivial_test_topic1", "trivial_test_topic2", 1);
}

/// If Kafka tests are going to fail because the server is not running or
/// not functioning properly, it's good to fail quickly without printing a
/// thousand records as part of the failure.
#[test]
fn kafka_input_trivial_threaded() {
    test_kafka_input(
        Vec::new(),
        "threaded_trivial_test_topic1",
        "threaded_trivial_test_topic2",
        3,
    );
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
    fn proptest_kafka_input_offset_earliest(
        data in generate_test_batches(5, 50, 500),
        data2 in generate_test_batches(5, 50, 500)
    ) {
        let topic1 = "proptest_kafka_input_offset_earliest_topic1";
        let data_len = data.len() as u64;
        test_offset(
            data,
            data2,
            topic1,
            "test_kafka_input_offset_earliest_topic2",
            KafkaStartFromConfig {
                topic: topic1.to_owned(),
                partition: 0,
                offset: (data_len / 2) + 1,
            },
        AutoOffsetReset::Earliest,
        );
    }

    #[test]
    fn proptest_kafka_input_offset_latest(
        data in generate_test_batches(5, 50, 500),
        data2 in generate_test_batches(5, 50, 500)
    ) {
        let topic1 = "proptest_kafka_input_offset_latest_topic1";
        let data_len = data.len() as u64;
        test_offset(
            data,
            data2,
            topic1,
            "proptest_kafka_input_offset_latest_topic2",
            KafkaStartFromConfig {
                topic: topic1.to_owned(),
                partition: 0,
                offset: (data_len / 2) + 1,
            },
        AutoOffsetReset::Latest,
        );
    }

    #[test]
    fn proptest_kafka_input(data in generate_test_batches(0, 100, 1000)) {
        test_kafka_input(data, "input_test_topic1", "input_test_topic2", 1);
    }
    #[test]
    fn proptest_kafka_input_threaded(data in generate_test_batches(0, 100, 1000)) {
        test_kafka_input(data, "threaded_test_topic1", "threaded_test_topic2", 3);
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
