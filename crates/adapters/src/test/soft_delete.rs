//! Tests for the `soft_delete` connector property.
//!
//! Each test drives records into a table whose `is_delete` column is populated
//! from record metadata, and checks that:
//!
//! * every record arrives as an insertion, and
//! * `is_delete` is `true` for records the input stream deleted and NULL for
//!   records it inserted.
//!
//! Records reach the table through a real parser or, for integrated connectors
//! that parse the data themselves, through the input stream the connector
//! writes to: the Arrow stream for Delta Lake and Iceberg, the record stream
//! for Postgres CDC.

use crate::test::{
    engine_test, parser,
    pipeline::{PipelineTestEngine, TestCircuit},
    published_schema, relations,
};
use arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field as ArrowField, Schema},
};
use feldera_adapterlib::{
    catalog::RecordFormat,
    format::{InputBuffer, Parser},
    test::{ConnectorTestEngine, TestCollection, Updates, default_timeout_ms, wait},
};
use feldera_types::{
    config::{FormatConfig, PipelineConfig},
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
    serde_with_context::SqlSerdeConfig,
};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use std::{borrow::Cow, collections::BTreeMap, io::Write, sync::Arc};
use tempfile::NamedTempFile;

/// A table with a primary key, which soft deletes do not support.
pub fn keyed_relation() -> Relation {
    relations::basic("soft_delete_keyed").with_primary_key(&[SqlIdentifier::from("id")])
}

/// The key of [`keyed_relation`].
pub fn key_relation() -> Relation {
    relations::basic_key("soft_delete_key")
}

/// The identity circuit over [`relation`], whose `is_delete` column reads the connector
/// attribute of that name.
fn circuit() -> TestCircuit {
    TestCircuit::identity(relation())
        .with_attribute_defaults(relations::soft_delete_options().attribute_defaults)
}

/// Returns a format configuration.
fn format(name: &str, config: serde_json::Value) -> FormatConfig {
    FormatConfig {
        name: Cow::from(name.to_string()),
        config,
    }
}

/// Returns `rows` of `relation`, each inserted once.
fn inserts(relation: &Relation, rows: &[&str]) -> Updates {
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, *row)).collect();
    Updates::from_json(published_schema(relation), &rows).unwrap()
}

/// Feeds `chunks` in `format` to a fresh soft-delete table of `relation`, and returns the
/// table.
fn ingest(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    format: &FormatConfig,
    chunks: &[&[u8]],
) -> Box<dyn TestCollection> {
    let collection = engine
        .input_collection(relation, &relations::soft_delete_options())
        .unwrap();
    let mut parser = parser(collection.as_ref(), format);
    for chunk in chunks {
        let (mut buffer, errors) = parser.parse(chunk, None);
        assert_eq!(errors, [], "parse errors");
        buffer.flush().unwrap();
    }
    collection
}

/// The JSON insert/delete format states both polarities.
#[engine_test]
pub fn json_insert_delete(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let collection = ingest(
        engine,
        &relation,
        &format("json", json!({"update_format": "insert_delete"})),
        &[
            br#"{"insert": {"id": 1, "s": "one"}}"#,
            br#"{"delete": {"id": 2, "s": "two"}}"#,
            // A record deleted and inserted again arrives twice, once with each polarity:
            // the table keeps the whole history.
            br#"{"delete": {"id": 3, "s": "three"}}"#,
            br#"{"insert": {"id": 3, "s": "three"}}"#,
        ],
    );
    assert_eq!(
        collection.updates().unwrap(),
        inserts(
            &relation,
            &[
                r#"{"id": 1, "s": "one", "is_delete": null}"#,
                r#"{"id": 2, "s": "two", "is_delete": true}"#,
                r#"{"id": 3, "s": "three", "is_delete": true}"#,
                r#"{"id": 3, "s": "three", "is_delete": null}"#,
            ]
        )
    );
}

/// A Debezium update deletes the record before and inserts the record after, so the table
/// records both.
#[engine_test]
pub fn json_debezium(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let collection = ingest(
        engine,
        &relation,
        &format("json", json!({"update_format": "debezium"})),
        &[
            br#"{"payload": {"op": "c", "after": {"id": 1, "s": "one"}}}"#,
            br#"{"payload": {"op": "d", "before": {"id": 2, "s": "two"}}}"#,
            br#"{"payload": {"op": "u", "before": {"id": 3, "s": "old"}, "after": {"id": 3, "s": "new"}}}"#,
        ],
    );
    assert_eq!(
        collection.updates().unwrap(),
        inserts(
            &relation,
            &[
                r#"{"id": 1, "s": "one", "is_delete": null}"#,
                r#"{"id": 2, "s": "two", "is_delete": true}"#,
                r#"{"id": 3, "s": "old", "is_delete": true}"#,
                r#"{"id": 3, "s": "new", "is_delete": null}"#,
            ]
        )
    );
}

/// The JSON raw format only inserts.
#[engine_test]
pub fn json_raw(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let collection = ingest(
        engine,
        &relation,
        &format("json", json!({"update_format": "raw", "array": true})),
        &[br#"[{"id": 1, "s": "one"}, {"id": 2, "s": "two"}]"#],
    );
    assert_eq!(
        collection.updates().unwrap(),
        inserts(
            &relation,
            &[
                r#"{"id": 1, "s": "one", "is_delete": null}"#,
                r#"{"id": 2, "s": "two", "is_delete": null}"#,
            ]
        )
    );
}

/// A CSV record is positional, so it holds the `is_delete` column, and a value in the record
/// takes precedence over the metadata, as a column default does.
#[engine_test]
pub fn csv(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let collection = ingest(
        engine,
        &relation,
        &format("csv", json!({})),
        &[b"1,one,\n2,two,true\n"],
    );
    assert_eq!(
        collection.updates().unwrap(),
        inserts(
            &relation,
            &[
                r#"{"id": 1, "s": "one", "is_delete": null}"#,
                r#"{"id": 2, "s": "two", "is_delete": true}"#,
            ]
        )
    );
}

/// The table that most cases read into.
pub fn relation() -> Relation {
    relations::soft_delete("soft_delete")
}

/// A table that the raw format fills: one VARCHAR column and `is_delete`.
pub fn raw_relation() -> Relation {
    Relation::new(
        "soft_delete_raw".into(),
        vec![
            Field::new("s".into(), ColumnType::varchar(false)),
            Field::new("is_delete".into(), ColumnType::boolean(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The raw format only inserts.
#[engine_test(requires(RawFormat))]
pub fn raw(engine: &dyn ConnectorTestEngine) {
    let relation = raw_relation();
    let collection = ingest(
        engine,
        &relation,
        &format("raw", json!({"column_name": "s"})),
        &[b"raw message"],
    );
    assert_eq!(
        collection.updates().unwrap(),
        inserts(&relation, &[r#"{"s": "raw message", "is_delete": null}"#])
    );
}

/// Returns rows of `id` and `s`, without `is_delete`, as columnar sources hold them.
fn arrow_batch(rows: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("s", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, s)| *s).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// The Parquet format only inserts.
#[engine_test]
pub fn parquet(engine: &dyn ConnectorTestEngine) {
    let rows = arrow_batch(&[(1, "one"), (2, "two")]);
    let mut file = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut file, rows.schema(), None).unwrap();
    writer.write(&rows).unwrap();
    writer.close().unwrap();

    let relation = relation();
    let collection = ingest(engine, &relation, &format("parquet", json!({})), &[&file]);
    assert_eq!(
        collection.updates().unwrap(),
        inserts(
            &relation,
            &[
                r#"{"id": 1, "s": "one", "is_delete": null}"#,
                r#"{"id": 2, "s": "two", "is_delete": null}"#,
            ]
        )
    );
}

/// Returns a fresh soft-delete table and the Arrow stream into it, the path of integrated
/// connectors that parse columnar data themselves.
fn arrow_stream(
    engine: &dyn ConnectorTestEngine,
) -> (
    Box<dyn TestCollection>,
    Box<dyn crate::catalog::ArrowStream>,
) {
    let collection = engine
        .input_collection(&relation(), &relations::soft_delete_options())
        .unwrap();
    let stream = collection
        .input_handle()
        .handle
        .configure_arrow_deserializer(SqlSerdeConfig::default())
        .unwrap();
    (collection, stream)
}

/// A batch of deletes is a batch of inserts.
#[engine_test]
pub fn arrow_delete(engine: &dyn ConnectorTestEngine) {
    let (collection, mut stream) = arrow_stream(engine);
    stream.insert(&arrow_batch(&[(1, "one")]), &None).unwrap();
    stream.delete(&arrow_batch(&[(2, "two")]), &None).unwrap();
    stream.flush().unwrap();
    assert_eq!(
        collection.updates().unwrap(),
        inserts(
            &relation(),
            &[
                r#"{"id": 1, "s": "one", "is_delete": null}"#,
                r#"{"id": 2, "s": "two", "is_delete": true}"#,
            ]
        )
    );
}

/// A CDC connector gives the polarity of each row of a batch. A batch of one polarity arrives
/// as it is; a mixed batch is split, inserts first, since metadata applies to a whole batch.
#[engine_test]
pub fn arrow_polarities(engine: &dyn ConnectorTestEngine) {
    let (collection, mut stream) = arrow_stream(engine);
    stream
        .insert_with_polarities(
            &arrow_batch(&[(1, "one"), (2, "two")]),
            &[true, true],
            &None,
        )
        .unwrap();
    stream
        .insert_with_polarities(
            &arrow_batch(&[(3, "three"), (4, "four")]),
            &[false, false],
            &None,
        )
        .unwrap();
    stream
        .insert_with_polarities(
            &arrow_batch(&[(5, "five"), (6, "six"), (7, "seven")]),
            &[false, true, false],
            &None,
        )
        .unwrap();
    stream.flush().unwrap();
    assert_eq!(
        collection.updates().unwrap(),
        inserts(
            &relation(),
            &[
                r#"{"id": 1, "s": "one", "is_delete": null}"#,
                r#"{"id": 2, "s": "two", "is_delete": null}"#,
                r#"{"id": 3, "s": "three", "is_delete": true}"#,
                r#"{"id": 4, "s": "four", "is_delete": true}"#,
                r#"{"id": 6, "s": "six", "is_delete": null}"#,
                r#"{"id": 5, "s": "five", "is_delete": true}"#,
                r#"{"id": 7, "s": "seven", "is_delete": true}"#,
            ]
        )
    );
}

/// Polarities that do not match the batch are an error of the caller, which the stream
/// reports rather than misapplies.
#[engine_test]
pub fn arrow_polarities_length_mismatch(engine: &dyn ConnectorTestEngine) {
    let (_collection, mut stream) = arrow_stream(engine);
    let error = stream
        .insert_with_polarities(&arrow_batch(&[(1, "one")]), &[true, false], &None)
        .unwrap_err();
    assert!(error.to_string().contains("polarities"), "{error}");
}

/// A transport that stages buffers instead of flushing them, as the fault-tolerant Kafka
/// connector does, delivers the same records.
#[engine_test]
pub fn staged_buffers(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let collection = engine
        .input_collection(&relation, &relations::soft_delete_options())
        .unwrap();
    let mut parser = parser(
        collection.as_ref(),
        &format("json", json!({"update_format": "insert_delete"})),
    );
    let (buffer, errors) = parser.parse(br#"{"delete": {"id": 1, "s": "one"}}"#, None);
    assert_eq!(errors, []);
    parser.stage(vec![buffer.unwrap()]).flush().unwrap();
    assert_eq!(
        collection.updates().unwrap(),
        inserts(&relation, &[r#"{"id": 1, "s": "one", "is_delete": true}"#])
    );
}

/// A fork of a soft-delete stream or handle, as transports make to run parsers in parallel,
/// keeps soft deletes.
#[engine_test]
pub fn fork_preserves_soft_delete(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let collection = engine
        .input_collection(&relation, &relations::soft_delete_options())
        .unwrap();
    let handle = collection.input_handle().handle;
    let stream = handle
        .configure_deserializer(RecordFormat::Json(Default::default()))
        .unwrap();
    let mut fork = stream.fork();
    fork.delete(br#"{"id": 1, "s": "one"}"#, &None).unwrap();
    fork.flush().unwrap();

    let mut stream = handle
        .fork()
        .configure_deserializer(RecordFormat::Json(Default::default()))
        .unwrap();
    stream.delete(br#"{"id": 2, "s": "two"}"#, &None).unwrap();
    stream.flush().unwrap();

    assert_eq!(
        collection.updates().unwrap(),
        inserts(
            &relation,
            &[
                r#"{"id": 1, "s": "one", "is_delete": true}"#,
                r#"{"id": 2, "s": "two", "is_delete": true}"#,
            ]
        )
    );
}

/// A pipeline whose input connector sets `soft_delete` fills its table with the history of
/// the input stream.
#[engine_test]
pub fn controller_ingests_history(engine: &dyn PipelineTestEngine) {
    let mut input = NamedTempFile::new().unwrap();
    input
        .write_all(
            br#"{"insert": {"id": 1, "s": "one"}}
{"delete": {"id": 2, "s": "two"}}
{"insert": {"id": 3, "s": "three"}}
{"delete": {"id": 3, "s": "three"}}
"#,
        )
        .unwrap();
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_soft_delete",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "soft_delete": true,
                "transport": {
                    "name": "file_input",
                    "config": { "path": input.path(), "follow": false }
                },
                "format": { "name": "json", "config": { "update_format": "insert_delete" } }
            }
        }
    }))
    .unwrap();
    let controller = engine
        .controller(
            &circuit(),
            &config,
            Box::new(|error, _| panic!("error: {error}")),
        )
        .unwrap();
    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();
    let contents = controller
        .execute_query_text_sync("select * from test_output1 order by id, is_delete nulls first")
        .unwrap();
    assert_eq!(
        contents,
        r#"+----+-------+-----------+
| id | s     | is_delete |
+----+-------+-----------+
| 1  | one   |           |
| 2  | two   | true      |
| 3  | three |           |
| 3  | three | true      |
+----+-------+-----------+"#
    );
    controller.stop().unwrap();
}

/// A delete in a table with a primary key names a key, not a record, so there is no record to
/// insert in its place, and a soft-delete connector on such a table does not start.
#[engine_test]
pub fn controller_rejects_primary_key(engine: &dyn PipelineTestEngine) {
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_soft_delete_primary_key",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "soft_delete": true,
                "transport": { "name": "empty_input" },
                "format": { "name": "json", "config": { "update_format": "insert_delete" } }
            }
        }
    }))
    .unwrap();
    let error = engine
        .controller(
            &TestCircuit::Keyed {
                key: key_relation(),
                relation: keyed_relation(),
                persistent_output_ids: vec![None],
                aliased: false,
            },
            &config,
            Box::new(|error, _| panic!("error: {error}")),
        )
        .err()
        .expect("the connector must not start");
    assert!(error.to_string().contains("primary key"), "{error}");
}

/// Soft deletes are how an input connector ingests records, so an output connector that sets
/// the property does not start.
#[engine_test]
pub fn controller_rejects_output_connector(engine: &dyn PipelineTestEngine) {
    let output = NamedTempFile::new().unwrap();
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_soft_delete_output",
        "workers": 1,
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "soft_delete": true,
                "transport": { "name": "file_output", "config": { "path": output.path() } },
                "format": { "name": "json" }
            }
        }
    }))
    .unwrap();
    let error = engine
        .controller(
            &circuit(),
            &config,
            Box::new(|error, _| panic!("error: {error}")),
        )
        .err()
        .expect("the connector must not start");
    assert!(error.to_string().contains("input connectors"), "{error}");
}
