//! Typed checks of the Kafka metadata on the static engine's record layer. The shared suites
//! see a VARIANT as its JSON text, which cannot tell binary from an array of numbers, or a
//! VARIANT null from a SQL NULL.

use std::collections::BTreeMap;
use std::sync::Arc;

use feldera_sqllib::{ByteArray, SqlString, Variant};
use feldera_types::config::InputEndpointConfig;
use feldera_types::program_schema::Relation;
use serde_json::{Value as JsonValue, json};

use feldera_adapterlib::test::wait_for_rows;

use crate::test::data::TestStructMetadata;
use crate::test::kafka::{KafkaResources, TestProducer};
use crate::test::{
    KafkaRawMetadata, MockDeZSet, TypedCollection, init_test_logger, input_pipeline_into, suites,
};

/// Sends the two messages of the metadata tests to `topic`: `first` with a header that holds a
/// value and a header without one, then `second` without headers.
fn send_metadata_messages(topic: &str, first: &[u8], second: &[u8]) {
    let producer = TestProducer::new();
    producer.send_message(
        first,
        topic,
        Some(BTreeMap::from([
            ("header1".to_string(), Some(b"foobar".to_vec())),
            ("header2".to_string(), None),
        ])),
    );
    producer.send_message(second, topic, None);
}

/// The `kafka_headers` of the messages that [`send_metadata_messages`] sends: a header value
/// is binary, and a header without a value is the VARIANT null.
fn expected_kafka_headers() -> Vec<Variant> {
    vec![
        Variant::Map(Arc::new(BTreeMap::from([
            (
                Variant::String(SqlString::from("header1")),
                Variant::Binary(ByteArray::new(b"foobar")),
            ),
            (
                Variant::String(SqlString::from("header2")),
                Variant::VariantNull,
            ),
        ]))),
        Variant::Map(Arc::new(BTreeMap::new())),
    ]
}

/// Returns the configuration of a Kafka input endpoint that reads `topic` with every Kafka
/// attribute, in `format`.
fn metadata_input_config(topic: &str, format: JsonValue) -> InputEndpointConfig {
    serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "kafka_input",
            "config": {
                "topic": topic,
                "log_level": "debug",
                "auto.offset.reset": "earliest",
                "include_headers": true,
                "include_topic": true,
                "include_timestamp": true,
                "include_partition": true,
                "include_offset": true
            }
        },
        "format": format
    }))
    .unwrap()
}

#[test]
fn test_kafka_metadata_json() {
    init_test_logger();

    let topic = "kafka_metadata_json_typed_test_topic";
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);
    let config = metadata_input_config(
        topic,
        json!({ "name": "json", "config": { "update_format": "raw" } }),
    );
    let relation = Relation::new(
        "test_input".into(),
        TestStructMetadata::schema(),
        false,
        BTreeMap::new(),
    );
    let zset = MockDeZSet::<TestStructMetadata, TestStructMetadata>::new();
    let pipeline = input_pipeline_into(
        Box::new(TypedCollection::new(relation.clone(), zset.clone())),
        &relation,
        config,
    )
    .unwrap();
    pipeline.reader.extend();

    send_metadata_messages(topic, b"{\"i\": 0}", b"{\"i\": 1}");
    wait_for_rows(pipeline.collection.as_ref(), 2, || {
        pipeline.reader.queue(false)
    });
    let headers: Vec<Variant> = zset
        .state()
        .flushed
        .iter()
        .map(|update| update.unwrap_insert().kafka_headers.clone())
        .collect();
    assert_eq!(headers, expected_kafka_headers());
}

#[test]
fn test_kafka_metadata_raw() {
    init_test_logger();

    let topic = "kafka_metadata_raw_typed_test_topic";
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);
    let config = metadata_input_config(
        topic,
        json!({ "name": "raw", "config": { "mode": "blob", "column_name": "data" } }),
    );
    let relation = suites::kafka::raw_metadata_relation();
    let zset = MockDeZSet::<KafkaRawMetadata, KafkaRawMetadata>::new();
    let pipeline = input_pipeline_into(
        Box::new(TypedCollection::new(relation.clone(), zset.clone())),
        &relation,
        config,
    )
    .unwrap();
    pipeline.reader.extend();

    send_metadata_messages(topic, b"foo", b"bar");
    wait_for_rows(pipeline.collection.as_ref(), 2, || {
        pipeline.reader.queue(false)
    });
    let headers: Vec<Variant> = zset
        .state()
        .flushed
        .iter()
        .map(|update| update.unwrap_insert().kafka_headers.clone())
        .collect();
    assert_eq!(headers, expected_kafka_headers());
}
