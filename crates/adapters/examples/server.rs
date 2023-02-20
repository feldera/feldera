//! This example creates a DBSP server on port 8080 and feeds
//! an infinite stream of Kafka messages to it.

use dbsp_adapters::{
    server,
    test::{
        generate_test_batches,
        kafka::{BufferConsumer, KafkaResources, TestProducer},
        test_circuit, TEST_LOGGER,
    },
};
use log::LevelFilter;
use proptest::{
    strategy::{Strategy, ValueTree},
    test_runner::TestRunner,
};
use std::{
    io::{stdout, Write},
    thread::spawn,
};

fn main() {
    let _ = log::set_logger(&TEST_LOGGER);
    log::set_max_level(LevelFilter::Debug);

    // Create topics.
    let kafka_resources = KafkaResources::create_topics(&[
        ("server_example_input_topic", 1),
        ("server_example_output_topic", 1),
    ]);

    // Config string
    let config_str = r#"
inputs:
    test_input1:
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                auto.offset.reset: "earliest"
                topics: [server_example_input_topic]
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
                topic: server_example_output_topic
                max_inflight_messages: 0
        format:
            name: csv
"#;

    spawn(move || {
        let producer = TestProducer::new();
        let buffer_consumer = BufferConsumer::new("server_example_output_topic");

        let mut runner = TestRunner::default();

        loop {
            // Generate some data, send it to the circuit, wait for it to
            // appear in the output topic before sending more.
            let data = generate_test_batches(5, 100)
                .new_tree(&mut runner)
                .unwrap()
                .current();

            producer.send_to_topic(&data, "server_example_input_topic");
            buffer_consumer.wait_for_output_unordered(&data);
            buffer_consumer.clear();
            print!(".");
            stdout().flush().unwrap();
        }
    });

    server::run_server(
        &test_circuit,
        config_str,
        "{\"name\": \"example\"}".to_string(),
        Some(8080),
    )
    .unwrap();

    drop(kafka_resources);
}
