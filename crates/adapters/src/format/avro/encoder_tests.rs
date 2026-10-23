//! The Avro encoder: raw and Confluent JDBC output of plain and indexed batches, keys, VARIANT
//! columns, a key with several values, and encoding on several threads.

use std::sync::{Arc, Mutex};

use apache_avro::{Schema as AvroSchema, from_avro_datum};
use arrow::array::{AsArray, BooleanArray, RecordBatch};
use arrow::compute::{concat_batches, not};
use feldera_adapterlib::catalog::SerBatchReader;
use feldera_adapterlib::format::Encoder;
use feldera_adapterlib::test::{ConnectorTestEngine, MockOutputConsumer, Updates, ZSet};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::format::avro::{AvroEncoderConfig, AvroEncoderKeyMode, AvroUpdateFormat};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use proptest::prelude::*;
use serde_json::Value;

use crate::format::avro::output::AvroEncoder;
use crate::test::suites::avro_parser::{VARIANT_ROW, VARIANT_SCHEMA, variant_relation};
use crate::test::{avro, published_schema, read, relations, rows};

/// The view that most cases encode.
pub fn relation() -> Relation {
    relations::basic("avro_output")
}

/// The key of the index on [`relation`].
pub fn key_relation() -> Relation {
    relations::basic_key("avro_output_key")
}

/// The Avro schema of [`relation`].
const SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestStruct",
    "fields": [
        { "name": "id", "type": "long" },
        { "name": "b", "type": "boolean" },
        { "name": "i", "type": ["null", "long"] },
        { "name": "s", "type": "string" }
    ]
}"#;

/// [`SCHEMA`] with the CDC field `op`, which an encoder with a `cdc_field` appends.
const SCHEMA_WITH_OP: &str = r#"{
    "type": "record",
    "name": "TestStruct",
    "fields": [
        { "name": "id", "type": "long" },
        { "name": "b", "type": "boolean" },
        { "name": "i", "type": ["null", "long"] },
        { "name": "s", "type": "string" },
        { "name": "op", "type": "string" }
    ]
}"#;

/// A message that an encoder writes: its key, value, and headers.
type Message = (
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Vec<(String, Option<Vec<u8>>)>,
);

/// What the consumer of an encoder collects: each message.
type Output = Arc<Mutex<Vec<Message>>>;

/// Returns an encoder of `relation`, or of the index `key` on it, with `config`, and the
/// messages that it writes.
fn encoder(
    key: Option<&Relation>,
    relation: &Relation,
    config: AvroEncoderConfig,
) -> (AvroEncoder, Output) {
    let consumer = MockOutputConsumer::new();
    let output = consumer.data.clone();
    let encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &key.cloned(),
        relation,
        Box::new(consumer),
        config,
        None,
        key.is_some(),
    )
    .unwrap();
    (encoder, output)
}

/// Encodes `batch` as one step, and returns the messages that the encoder writes for it.
fn encode(
    encoder: &mut AvroEncoder,
    output: &Output,
    batch: Arc<dyn SerBatchReader>,
) -> Vec<Message> {
    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    encoder.encode(batch).unwrap();
    encoder.consumer().batch_end();
    output.lock().unwrap().drain(..).collect()
}

/// Returns the row of `relation` in a message, `bytes` with a 5-byte header, decoded with
/// `schema`, as JSON in the arrow-json form.
fn decode(relation: &Relation, schema: &AvroSchema, bytes: &[u8]) -> String {
    let value = from_avro_datum(schema, &mut &bytes[5..], None).unwrap();
    avro::to_json(relation, &value).unwrap().to_string()
}

/// Returns `rows` of `relation`, JSON in the arrow-json form, each with its weight.
fn updates(relation: &Relation, rows: &[(i64, String)]) -> Updates {
    let rows: Vec<(i64, &str)> = rows
        .iter()
        .map(|(weight, row)| (*weight, row.as_str()))
        .collect();
    Updates::from_json(published_schema(relation), &rows).unwrap()
}

/// Returns the operation in the `op` header of a message.
fn operation(headers: &[(String, Option<Vec<u8>>)]) -> String {
    let (name, value) = &headers[0];
    assert_eq!(name, "op");
    String::from_utf8(value.clone().unwrap()).unwrap()
}

/// Returns the rows with positive weights in `zset`, and the rows with negative weights,
/// negated.
fn split_by_sign(zset: &ZSet) -> (ZSet, ZSet) {
    let (rows, weights) = zset.to_batch().unwrap();
    let mut inserts = ZSet::empty(rows.schema()).unwrap();
    let mut deletes = ZSet::empty(rows.schema()).unwrap();
    for (index, weight) in weights.iter().enumerate() {
        let row = rows.slice(index, 1);
        if *weight > 0 {
            inserts.insert(&row, &[*weight]).unwrap();
        } else {
            deletes.insert(&row, &[-weight]).unwrap();
        }
    }
    (inserts, deletes)
}

/// Returns the columns of `key` in `row`, a row of the indexed relation as JSON.
fn key_of(key: &Relation, row: &str) -> Value {
    let row: Value = serde_json::from_str(row).unwrap();
    let columns = key
        .fields
        .iter()
        .map(|field| {
            let name = field.name.name();
            let value = row[&name].clone();
            (name, value)
        })
        .collect();
    Value::Object(columns)
}

/// Returns `rows` with the column `b` negated.
fn flip_b(rows: &RecordBatch) -> RecordBatch {
    let index = rows.schema().index_of("b").unwrap();
    let mut columns = rows.columns().to_vec();
    let flipped: BooleanArray = not(columns[index].as_boolean()).unwrap();
    columns[index] = Arc::new(flipped);
    RecordBatch::try_new(rows.schema(), columns).unwrap()
}

/// The VARIANT columns encode as JSON in Avro strings.
pub fn variant_output(engine: &dyn ConnectorTestEngine) {
    let relation = variant_relation();
    let config = AvroEncoderConfig {
        schema: Some(VARIANT_SCHEMA.to_string()),
        ..Default::default()
    };
    let schema = AvroSchema::parse_str(VARIANT_SCHEMA).unwrap();
    let (mut encoder, output) = encoder(None, &relation, config);
    let rows = Updates::from_json(published_schema(&relation), &[(1, VARIANT_ROW)]).unwrap();
    let messages = encode(
        &mut encoder,
        &output,
        engine
            .output_batch(&relation, rows.rows(), rows.weights())
            .unwrap(),
    );
    let decoded: Vec<(i64, String)> = messages
        .iter()
        .map(|(_key, value, _headers)| (1, decode(&relation, &schema, value.as_ref().unwrap())))
        .collect();
    assert_eq!(updates(&relation, &decoded), rows);
}

/// Each change of each random batch encodes once per unit of weight, with its operation in
/// the `op` header.
pub fn random_raw_output(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let schema = AvroSchema::parse_str(SCHEMA).unwrap();
    proptest!(|(batches in rows::weighted_batches(&relation, "id", 0..=10, 20, -2..=2))| {
        let config = AvroEncoderConfig {
            schema: Some(SCHEMA.to_string()),
            ..Default::default()
        };
        let (mut encoder, output) = encoder(None, &relation, config);
        for rows in &batches {
            let messages = encode(
                &mut encoder,
                &output,
                engine.output_batch(&relation, rows.rows(), rows.weights()).unwrap(),
            );
            let decoded: Vec<(i64, String)> = messages
                .iter()
                .map(|(_key, value, headers)| {
                    let weight = if operation(headers) == "insert" { 1 } else { -1 };
                    (weight, decode(&relation, &schema, value.as_ref().unwrap()))
                })
                .collect();
            prop_assert_eq!(updates(&relation, &decoded).zset().unwrap(), rows.zset().unwrap());
        }
    });
}

/// Encodes three steps of indexed batches for each batch of `rows`: an insert of each row, an
/// update of each row to its copy with `b` negated, and a delete of the copy. Checks that the
/// steps encode the rows with the operations `insert`, `update`, and `delete`, and, when the
/// encoder writes keys, that each key holds the `id` of its row.
fn check_indexed_steps(
    engine: &dyn ConnectorTestEngine,
    config: AvroEncoderConfig,
    batches: &[Updates],
) {
    let (relation, key) = (relation(), key_relation());
    let (mut encoder, output) = encoder(Some(&key), &relation, config.clone());
    let key_schema = encoder.key_avro_schema.clone();
    if config.key_mode != Some(AvroEncoderKeyMode::None) {
        assert!(key_schema.is_some());
    }
    let value_schema = encoder.value_avro_schema.clone();
    for rows in batches {
        let old = rows.rows().clone();
        let new = flip_b(&old);
        let both = concat_batches(&old.schema(), [&old, &new]).unwrap();
        let n = old.num_rows();
        let steps = [
            ("insert", old.clone(), vec![1; n], &old),
            ("update", both, [vec![-1; n], vec![1; n]].concat(), &new),
            ("delete", new.clone(), vec![-1; n], &new),
        ];
        for (expected_operation, step_rows, weights, expected_rows) in steps {
            let messages = encode(
                &mut encoder,
                &output,
                engine
                    .indexed_output_batch(&key, &relation, &step_rows, &weights)
                    .unwrap(),
            );
            let mut decoded = Vec::new();
            for (message_key, value, headers) in &messages {
                assert_eq!(operation(headers), expected_operation);
                let row = decode(&relation, &value_schema, value.as_ref().unwrap());
                if let Some(key_schema) = &key_schema {
                    let key_row = decode(&key, key_schema, message_key.as_ref().unwrap());
                    let id = serde_json::from_str::<serde_json::Value>(&row).unwrap()["id"].clone();
                    assert_eq!(key_row, serde_json::json!({"id": id}).to_string());
                }
                decoded.push((1, row));
            }
            assert_eq!(
                updates(&relation, &decoded).zset().unwrap(),
                ZSet::from_batch(expected_rows, &vec![1; n]).unwrap(),
                "{expected_operation}"
            );
        }
    }
}

/// An indexed view encodes inserts, updates, and deletes, without keys.
pub fn random_raw_output_indexed(engine: &dyn ConnectorTestEngine) {
    proptest!(|(batches in rows::batches(&relation(), "id", 10..=10, 20))| {
        check_indexed_steps(
            engine,
            AvroEncoderConfig {
                schema: Some(SCHEMA.to_string()),
                key_mode: Some(AvroEncoderKeyMode::None),
                ..Default::default()
            },
            &batches,
        );
    });
}

/// By default, an indexed view encodes each change with a key of the key columns, under
/// schemas that the encoder derives from the relations.
pub fn random_raw_output_indexed_with_key(engine: &dyn ConnectorTestEngine) {
    proptest!(|(batches in rows::batches(&relation(), "id", 10..=10, 20))| {
        check_indexed_steps(engine, AvroEncoderConfig::default(), &batches);
    });
}

/// In the Confluent JDBC format, an insert is a record once per unit of weight, and a delete is
/// a tombstone whose key is the whole record. The deletes come first, then the inserts, each in
/// the order of the batch's cursor.
pub fn random_confluent_output(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let schema = AvroSchema::parse_str(SCHEMA).unwrap();
    proptest!(|(batches in rows::weighted_batches(&relation, "id", 0..=10, 20, -2..=2))| {
        let config = AvroEncoderConfig {
            schema: Some(SCHEMA.to_string()),
            namespace: Some("foo.bar".to_string()),
            update_format: AvroUpdateFormat::ConfluentJdbc,
            ..Default::default()
        };
        let (mut encoder, output) = encoder(None, &relation, config);
        for rows in &batches {
            let batch = engine.output_batch(&relation, rows.rows(), rows.weights()).unwrap();
            let messages = encode(&mut encoder, &output, batch.clone());
            let (mut inserts, mut deletes, mut changes) = (Vec::new(), Vec::new(), Vec::new());
            for (key, value, _headers) in &messages {
                match value {
                    Some(value) => {
                        let row = decode(&relation, &schema, value);
                        inserts.push((1, row.clone()));
                        changes.push((1, row));
                    }
                    None => {
                        let row = decode(&relation, &schema, key.as_ref().unwrap());
                        deletes.push((1, row.clone()));
                        changes.push((-1, row));
                    }
                }
            }
            let (expected_inserts, expected_deletes) = split_by_sign(&rows.zset().unwrap());
            prop_assert_eq!(updates(&relation, &inserts).zset().unwrap(), expected_inserts);
            prop_assert_eq!(updates(&relation, &deletes).zset().unwrap(), expected_deletes);
            prop_assert_eq!(
                updates(&relation, &changes),
                read::encoding_order(batch.as_ref(), &relation).unwrap()
            );
        }
    });
}

/// In the Confluent JDBC format, an indexed view encodes an insert and an update as a record
/// under its own key, and a delete as a tombstone of the key.
pub fn random_confluent_output_indexed(engine: &dyn ConnectorTestEngine) {
    let (relation, key) = (relation(), key_relation());
    proptest!(|(batches in rows::batches(&relation, "id", 10..=10, 20))| {
        let config = AvroEncoderConfig {
            namespace: Some("foo.bar".to_string()),
            update_format: AvroUpdateFormat::ConfluentJdbc,
            ..Default::default()
        };
        let (mut encoder, output) = encoder(Some(&key), &relation, config);
        let key_schema = encoder.key_avro_schema.clone().unwrap();
        let value_schema = encoder.value_avro_schema.clone();
        for rows in &batches {
            let old = rows.rows().clone();
            let new = flip_b(&old);
            let both = concat_batches(&old.schema(), [&old, &new]).unwrap();
            let n = old.num_rows();
            let steps = [
                (old.clone(), vec![1; n], Some(&old)),
                (both, [vec![-1; n], vec![1; n]].concat(), Some(&new)),
                (new.clone(), vec![-1; n], None),
            ];
            for (step_rows, weights, expected_values) in steps {
                let messages = encode(
                    &mut encoder,
                    &output,
                    engine.indexed_output_batch(&key, &relation, &step_rows, &weights).unwrap(),
                );
                let mut keys = Vec::new();
                let mut values = Vec::new();
                for (message_key, value, _headers) in &messages {
                    let message_key = decode(&key, &key_schema, message_key.as_ref().unwrap());
                    if let Some(value) = value {
                        let value = decode(&relation, &value_schema, value);
                        prop_assert_eq!(
                            serde_json::from_str::<Value>(&message_key).unwrap(),
                            key_of(&key, &value),
                            "the key of a message is not the key of its value {}",
                            value
                        );
                        values.push((1, value));
                    }
                    keys.push((1, message_key));
                }
                let expected_keys = Updates::new(
                    old.project(&[old.schema().index_of("id").unwrap()]).unwrap(),
                    vec![1; n],
                )
                .unwrap();
                prop_assert_eq!(
                    updates(&key, &keys).zset().unwrap(),
                    expected_keys.zset().unwrap()
                );
                match expected_values {
                    Some(expected_values) => prop_assert_eq!(
                        updates(&relation, &values).zset().unwrap(),
                        ZSet::from_batch(expected_values, &vec![1; n]).unwrap()
                    ),
                    None => prop_assert!(values.is_empty()),
                }
            }
        }
    });
}

/// An indexed batch with a key that has two values, or one value with a weight past one, is
/// an error that says so.
pub fn non_unique_keys(engine: &dyn ConnectorTestEngine) {
    let (relation, key) = (relation(), key_relation());
    let config = AvroEncoderConfig {
        schema: Some(SCHEMA.to_string()),
        key_mode: Some(AvroEncoderKeyMode::None),
        ..Default::default()
    };
    let (mut encoder, _output) = encoder(Some(&key), &relation, config);
    let v1 = r#"{"id": 1, "b": true, "s": "foo"}"#;
    let v2 = r#"{"id": 1, "b": false, "s": "bar"}"#;
    for (rows, expected_error) in [
        (vec![(2, v1)], "is inserted 2 times"),
        (vec![(-2, v1)], "is deleted 2 times"),
        (
            vec![(1, v1), (1, v2)],
            "Error description: Multiple new values for the same key.",
        ),
        (
            vec![(-1, v1), (-1, v2)],
            "Error description: Multiple deleted values for the same key.",
        ),
    ] {
        let rows = Updates::from_json(published_schema(&relation), &rows).unwrap();
        let batch = engine
            .indexed_output_batch(&key, &relation, rows.rows(), rows.weights())
            .unwrap();
        encoder.consumer().batch_start(0, OutputBatchType::Delta);
        let error = encoder.encode(batch).unwrap_err();
        assert!(
            error.to_string().contains(expected_error),
            "expected '{expected_error}' in '{error}'"
        );
        encoder.consumer().batch_end();
    }
}

/// An encoder with ten threads encodes each change of a large indexed batch once.
pub fn parallel_indexed_output(engine: &dyn ConnectorTestEngine) {
    let (relation, key) = (relation(), key_relation());
    let config = AvroEncoderConfig {
        schema: Some(SCHEMA.to_string()),
        key_mode: Some(AvroEncoderKeyMode::None),
        threads: 10,
        ..Default::default()
    };
    let (mut encoder, output) = encoder(Some(&key), &relation, config);
    let schema = AvroSchema::parse_str(SCHEMA).unwrap();
    let rows: Vec<(i64, String)> = (0..2000)
        .map(|id| {
            let weight = if id % 3 == 0 { -1 } else { 1 };
            let i = if id % 2 == 0 {
                id.to_string()
            } else {
                "null".to_string()
            };
            (
                weight,
                format!(
                    r#"{{"id": {id}, "b": {}, "i": {i}, "s": "{}"}}"#,
                    id % 5 == 0,
                    id * 7
                ),
            )
        })
        .collect();
    let rows = updates(&relation, &rows);
    let messages = encode(
        &mut encoder,
        &output,
        engine
            .indexed_output_batch(&key, &relation, rows.rows(), rows.weights())
            .unwrap(),
    );
    let decoded: Vec<(i64, String)> = messages
        .iter()
        .map(|(_key, value, headers)| {
            let weight = match operation(headers).as_str() {
                "insert" => 1,
                "delete" => -1,
                operation => panic!("unexpected operation {operation}"),
            };
            (weight, decode(&relation, &schema, value.as_ref().unwrap()))
        })
        .collect();
    assert_eq!(decoded.len(), rows.len());
    assert_eq!(
        updates(&relation, &decoded).zset().unwrap(),
        rows.zset().unwrap()
    );
}

/// An indexed encoder with a `cdc_field` writes each change with the field set to its
/// operation: `I` for an insert, `U` for an update, and `D` for a delete.
pub fn cdc_field_output(engine: &dyn ConnectorTestEngine) {
    let (relation, key) = (relation(), key_relation());
    let config = AvroEncoderConfig {
        schema: Some(SCHEMA.to_string()),
        key_mode: Some(AvroEncoderKeyMode::None),
        cdc_field: Some("op".to_string()),
        ..Default::default()
    };
    let (mut encoder, output) = encoder(Some(&key), &relation, config);
    let rows = updates(
        &relation,
        &[
            (
                1,
                r#"{"id": 1, "b": true, "i": null, "s": "new"}"#.to_string(),
            ),
            (
                -1,
                r#"{"id": 2, "b": true, "i": 2, "s": "old"}"#.to_string(),
            ),
            (
                1,
                r#"{"id": 2, "b": false, "i": 2, "s": "new"}"#.to_string(),
            ),
            (
                -1,
                r#"{"id": 3, "b": false, "i": 3, "s": "gone"}"#.to_string(),
            ),
        ],
    );
    let messages = encode(
        &mut encoder,
        &output,
        engine
            .indexed_output_batch(&key, &relation, rows.rows(), rows.weights())
            .unwrap(),
    );

    let mut with_op = relation.clone();
    with_op
        .fields
        .push(Field::new("op".into(), ColumnType::varchar(false)));
    let schema = AvroSchema::parse_str(SCHEMA_WITH_OP).unwrap();
    let decoded: Vec<(i64, String)> = messages
        .iter()
        .map(|(_key, value, _headers)| (1, decode(&with_op, &schema, value.as_ref().unwrap())))
        .collect();
    let expected = [
        r#"{"id": 1, "b": true, "i": null, "s": "new", "op": "I"}"#,
        r#"{"id": 2, "b": false, "i": 2, "s": "new", "op": "U"}"#,
        r#"{"id": 3, "b": false, "i": 3, "s": "gone", "op": "D"}"#,
    ]
    .map(|row| (1, row.to_string()));
    assert_eq!(decoded.len(), expected.len());
    assert_eq!(
        updates(&with_op, &decoded).zset().unwrap(),
        updates(&with_op, &expected).zset().unwrap()
    );
}
