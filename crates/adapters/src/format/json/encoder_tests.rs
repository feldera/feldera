//! The JSON encoder: the insert/delete, Snowflake, and Debezium envelopes as lines or arrays,
//! Debezium and Redis keys, and records too long for the transport.

use crate::test::engine_test;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use feldera_adapterlib::catalog::SerBatchReader;
use feldera_adapterlib::format::Encoder;
use feldera_adapterlib::test::{ConnectorTestEngine, MockOutputConsumer, Updates};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::format::json::{JsonEncoderConfig, JsonUpdateFormat};
use feldera_types::program_schema::Relation;
use proptest::prelude::*;
use serde_json::{Value, json};

use crate::format::json::output::JsonEncoder;
use crate::test::{published_schema, read, relations, rows};

use JsonUpdateFormat::{Debezium, InsertDelete, Redis, Snowflake};

/// The view that the cases encode.
pub fn relation() -> Relation {
    relations::basic("json_output")
}

/// Three batches of changes to the view: rows with their weights.
fn batches() -> Vec<Vec<(i64, &'static str)>> {
    vec![
        vec![
            (1, r#"{"id": 0, "b": true, "s": "foo"}"#),
            (-1, r#"{"id": 1, "b": false, "i": 10, "s": "bar"}"#),
        ],
        vec![
            (-2, r#"{"id": 2, "b": true, "s": "foo"}"#),
            (2, r#"{"id": 3, "b": false, "i": 10, "s": "bar"}"#),
        ],
        vec![
            (-1, r#"{"id": 4, "b": true, "i": 15, "s": "buzz"}"#),
            (3, r#"{"id": 5, "b": false, "s": ""}"#),
        ],
    ]
}

/// An encoder of `relation` with `config`, and the output that its consumer collects.
type Output = Arc<
    Mutex<
        Vec<(
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Vec<(String, Option<Vec<u8>>)>,
        )>,
    >,
>;

fn encoder(config: JsonEncoderConfig, consumer: MockOutputConsumer) -> (JsonEncoder, Output) {
    let output = consumer.data.clone();
    let encoder = JsonEncoder::new(Box::new(consumer), config, &relation(), None, false);
    (encoder, output)
}

/// The engine's output batch of `rows`.
fn batch(engine: &dyn ConnectorTestEngine, rows: &[(i64, &str)]) -> Arc<dyn SerBatchReader> {
    let rows = Updates::from_json(published_schema(&relation()), rows).unwrap();
    engine
        .output_batch(&relation(), rows.rows(), rows.weights())
        .unwrap()
}

/// Returns [`batches`] as rows.
fn fixed_batches() -> Vec<Updates> {
    let schema = published_schema(&relation());
    batches()
        .into_iter()
        .map(|rows| Updates::from_json(schema.clone(), &rows).unwrap())
        .collect()
}

/// Encodes `batches` in `update_format`, as lines or arrays, and checks that each batch
/// encodes every change once per unit of weight: the deletes, then the inserts, each in the
/// order of the batch's cursor.
fn check(
    engine: &dyn ConnectorTestEngine,
    update_format: JsonUpdateFormat,
    array: bool,
    batches: &[Updates],
) {
    let config = JsonEncoderConfig {
        update_format: update_format.clone(),
        json_flavor: None,
        buffer_size_records: 3,
        array,
        key_fields: None,
    };
    let (mut encoder, output) = encoder(config, MockOutputConsumer::new());
    let schema = published_schema(&relation());
    let mut next_sequence_number = 0;
    for (step, rows) in batches.iter().enumerate() {
        let batch = engine
            .output_batch(&relation(), rows.rows(), rows.weights())
            .unwrap();
        encoder
            .consumer()
            .batch_start(step as u64, OutputBatchType::Delta);
        encoder.encode(batch.clone()).unwrap();
        encoder.consumer().batch_end();

        let bytes: Vec<u8> = output
            .lock()
            .unwrap()
            .drain(..)
            .filter_map(|(_key, value, _headers)| value)
            .flatten()
            .collect();
        let encoded = decode(
            &update_format,
            array,
            &bytes,
            schema.clone(),
            encoder.stream_id,
            &mut next_sequence_number,
        );
        assert_eq!(
            encoded.zset().unwrap(),
            rows.zset().unwrap(),
            "batch {step}"
        );
        assert_eq!(
            encoded,
            read::encoding_order(batch.as_ref(), &relation()).unwrap(),
            "batch {step} is not one envelope per unit of weight, deletes first, in cursor order"
        );
    }
}

/// Decodes the envelopes in `bytes`, as lines or arrays, into one row each: +1 for an insert,
/// -1 for a delete. Checks that Snowflake records carry `stream_id` and sequence numbers that
/// count up from `next_sequence_number`.
fn decode(
    update_format: &JsonUpdateFormat,
    array: bool,
    bytes: &[u8],
    schema: SchemaRef,
    stream_id: u64,
    next_sequence_number: &mut u64,
) -> Updates {
    let mut envelopes = Vec::new();
    for value in serde_json::Deserializer::from_slice(bytes).into_iter::<Value>() {
        let value = value.unwrap();
        if array {
            envelopes.extend(value.as_array().unwrap().iter().cloned());
        } else {
            envelopes.push(value);
        }
    }
    let rows: Vec<(i64, String)> = envelopes
        .into_iter()
        .map(|envelope| unwrap_envelope(update_format, envelope, stream_id, next_sequence_number))
        .collect();
    let rows: Vec<(i64, &str)> = rows
        .iter()
        .map(|(weight, row)| (*weight, row.as_str()))
        .collect();
    Updates::from_json(schema, &rows).unwrap()
}

/// Returns the row in `envelope` as JSON, with +1 for an insert and -1 for a delete.
///
/// Checks the shape of the envelope: an insert/delete envelope has no other member, and a
/// Debezium event has no `before` for a create and no `after` for a delete.
fn unwrap_envelope(
    update_format: &JsonUpdateFormat,
    mut envelope: Value,
    stream_id: u64,
    next_sequence_number: &mut u64,
) -> (i64, String) {
    match update_format {
        InsertDelete => {
            let object = envelope.as_object_mut().unwrap();
            let change = match (object.remove("insert"), object.remove("delete")) {
                (Some(row), None) => (1, row.to_string()),
                (None, Some(row)) => (-1, row.to_string()),
                _ => panic!("not an insert/delete envelope: {envelope}"),
            };
            assert!(
                object.is_empty(),
                "an insert/delete envelope has other members: {object:?}"
            );
            change
        }
        Snowflake => {
            let object = envelope.as_object_mut().unwrap();
            assert_eq!(object.remove("__stream_id"), Some(json!(stream_id)));
            assert_eq!(
                object.remove("__seq_number"),
                Some(json!(*next_sequence_number))
            );
            *next_sequence_number += 1;
            let weight = match object.remove("__action").as_ref().and_then(Value::as_str) {
                Some("insert") => 1,
                Some("delete") => -1,
                action => panic!("unexpected Snowflake action {action:?}"),
            };
            (weight, envelope.to_string())
        }
        Debezium => {
            let payload = &envelope["payload"];
            let absent = |side: &str| payload.get(side).is_none_or(Value::is_null);
            match payload["op"].as_str() {
                Some("c") => {
                    assert!(absent("before"), "a create with a 'before': {envelope}");
                    (1, payload["after"].to_string())
                }
                Some("d") => {
                    assert!(absent("after"), "a delete with an 'after': {envelope}");
                    (-1, payload["before"].to_string())
                }
                op => panic!("unexpected Debezium operation {op:?}"),
            }
        }
        _ => unreachable!("no envelope for {update_format:?}"),
    }
}

/// Insert/delete envelopes, one per line.
#[engine_test]
pub fn insert_delete_lines(engine: &dyn ConnectorTestEngine) {
    check(engine, InsertDelete, false, &fixed_batches());
}

/// Insert/delete envelopes in JSON arrays.
#[engine_test]
pub fn insert_delete_arrays(engine: &dyn ConnectorTestEngine) {
    check(engine, InsertDelete, true, &fixed_batches());
}

/// Snowflake records, one per line, numbered in order.
#[engine_test]
pub fn snowflake_lines(engine: &dyn ConnectorTestEngine) {
    check(engine, Snowflake, false, &fixed_batches());
}

/// Snowflake records in JSON arrays, numbered in order.
#[engine_test]
pub fn snowflake_arrays(engine: &dyn ConnectorTestEngine) {
    check(engine, Snowflake, true, &fixed_batches());
}

/// Debezium change events, one per line.
#[engine_test]
pub fn debezium_lines(engine: &dyn ConnectorTestEngine) {
    check(engine, Debezium, false, &fixed_batches());
}

/// Runs [`check`] on random batches of rows with weights from -2 to 2.
fn check_random(engine: &dyn ConnectorTestEngine, update_format: JsonUpdateFormat, array: bool) {
    let relation = relation();
    proptest!(|(batches in rows::weighted_batches(&relation, "id", 0..=10, 20, -2..=2))| {
        check(engine, update_format.clone(), array, &batches);
    });
}

/// Random insert/delete envelopes, one per line.
#[engine_test]
pub fn random_insert_delete_lines(engine: &dyn ConnectorTestEngine) {
    check_random(engine, InsertDelete, false);
}

/// Random insert/delete envelopes in JSON arrays.
#[engine_test]
pub fn random_insert_delete_arrays(engine: &dyn ConnectorTestEngine) {
    check_random(engine, InsertDelete, true);
}

/// Random Snowflake records, one per line.
#[engine_test]
pub fn random_snowflake_lines(engine: &dyn ConnectorTestEngine) {
    check_random(engine, Snowflake, false);
}

/// Random Snowflake records in JSON arrays.
#[engine_test]
pub fn random_snowflake_arrays(engine: &dyn ConnectorTestEngine) {
    check_random(engine, Snowflake, true);
}

/// Random Debezium change events, one per line.
#[engine_test]
pub fn random_debezium_lines(engine: &dyn ConnectorTestEngine) {
    check_random(engine, Debezium, false);
}

/// A record longer than the transport's largest buffer is an error that shows the record.
#[engine_test]
pub fn record_longer_than_buffer(engine: &dyn ConnectorTestEngine) {
    let config = JsonEncoderConfig {
        update_format: InsertDelete,
        json_flavor: None,
        buffer_size_records: 3,
        array: false,
        key_fields: None,
    };
    let (mut encoder, _output) =
        encoder(config, MockOutputConsumer::with_max_buffer_size_bytes(32));
    let error = encoder.encode(batch(engine, &batches()[0])).unwrap_err();
    assert_eq!(
        error.to_string(),
        "JSON record exceeds maximum buffer size supported by the output transport. Max supported buffer size is 32 bytes, but the following record requires 46 bytes: '{\"delete\":{\"id\":1,\"b\":false,\"i\":10,\"s\":\"bar\"}}'."
    );
}

/// With `key_fields`, a Debezium record carries a key of those fields, both with their
/// schemas.
#[engine_test]
pub fn debezium_key_fields(engine: &dyn ConnectorTestEngine) {
    let config = JsonEncoderConfig {
        update_format: Debezium,
        json_flavor: None,
        buffer_size_records: 1,
        array: false,
        key_fields: Some(vec!["id".to_string(), "s".to_string()]),
    };
    let (mut encoder, output) = encoder(config, MockOutputConsumer::new());
    encoder.encode(batch(engine, &batches()[0])).unwrap();

    let key_schema = json!({"type":"struct","fields":[{"field":"id","optional":false,"type":"int64"},{"field":"s","optional":false,"type":"string"}],"name":"Key"});
    let value_schema = json!({"type":"struct","fields":[{"field":"after","type":"struct","fields":[{"field":"id","type":"int64","optional":false},{"field":"b","type":"boolean","optional":false},{"field":"i","type":"int64","optional":true},{"field":"s","type":"string","optional":false}],"optional":true},{"field":"op","type":"string","optional":false}],"name":"Envelope"});
    let records: Vec<(Option<Value>, Value)> = output
        .lock()
        .unwrap()
        .iter()
        .map(|(key, value, _headers)| {
            (
                key.as_ref().map(|key| serde_json::from_slice(key).unwrap()),
                serde_json::from_slice(value.as_ref().unwrap()).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        records,
        vec![
            (
                Some(json!({"schema": key_schema, "payload": {"id": 1, "s": "bar"}})),
                json!({"schema": value_schema, "payload": {"op": "d", "before": {"b": false, "i": 10, "id": 1, "s": "bar"}}}),
            ),
            (
                Some(json!({"schema": key_schema, "payload": {"id": 0, "s": "foo"}})),
                json!({"schema": value_schema, "payload": {"op": "c", "after": {"id": 0, "b": true, "i": null, "s": "foo"}}}),
            ),
        ]
    );
}

/// Redis output keys a record by its key fields: a delete has no value, and an insert's
/// value is the whole record.
#[engine_test]
pub fn redis(engine: &dyn ConnectorTestEngine) {
    let config = JsonEncoderConfig {
        update_format: Redis,
        json_flavor: None,
        buffer_size_records: 1,
        array: false,
        key_fields: Some(vec!["id".to_string(), "s".to_string()]),
    };
    let (mut encoder, output) = encoder(config, MockOutputConsumer::new());
    encoder.encode(batch(engine, &batches()[0])).unwrap();

    let records: Vec<(String, Option<Value>)> = output
        .lock()
        .unwrap()
        .iter()
        .map(|(key, value, _headers)| {
            (
                String::from_utf8(key.clone().unwrap()).unwrap(),
                value
                    .as_ref()
                    .map(|value| serde_json::from_slice(value).unwrap()),
            )
        })
        .collect();
    assert_eq!(
        records,
        vec![
            ("1:bar".to_string(), None),
            (
                "0:foo".to_string(),
                Some(json!({"id": 0, "s": "foo", "b": true, "i": null}))
            ),
        ]
    );
}
