//! Tests for the `soft_delete` connector property.
//!
//! Each test drives records through a real parser (or, for connectors that
//! bypass parsers, through the Arrow input stream) into a table whose
//! `is_delete` column is populated from record metadata, and checks that:
//!
//! * every record arrives as an insertion, and
//! * `is_delete` is `true` for records the input stream deleted and NULL for
//!   records it inserted.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::io::Write;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use feldera_adapterlib::soft_delete::SoftDeleteHandle;
use feldera_sqllib::Variant;
use feldera_types::config::PipelineConfig;
use feldera_types::deserialize_table_record;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use tempfile::NamedTempFile;

use crate::catalog::ArrowStream;
use crate::format::Parser;
use crate::test::{
    DEFAULT_TIMEOUT_MS, KeyStruct, MockDeZSet, MockUpdate, TestStruct, TestStructSoftDelete,
    init_test_logger, mock_soft_delete_parser_pipeline, test_circuit, test_circuit_with_index,
    wait,
};
use crate::{Controller, DeCollectionHandle, FormatConfig, InputBuffer, RecordFormat};

fn format_config(name: &str, config: serde_json::Value) -> FormatConfig {
    FormatConfig {
        name: Cow::from(name.to_string()),
        config,
    }
}

/// Feed `batches` to a soft-delete connector using `format` and return the
/// updates that reach the table.
fn ingest<T>(schema: Vec<Field>, format: &FormatConfig, batches: &[&[u8]]) -> Vec<MockUpdate<T, T>>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    let relation = Relation::new("test_input".into(), schema, false, BTreeMap::new());
    let (_consumer, mut parser, table) =
        mock_soft_delete_parser_pipeline::<T, T>(&relation, format).unwrap();

    for batch in batches {
        let (mut buffer, errors) = parser.parse(batch, None);
        assert_eq!(&errors, &[], "unexpected parse errors");
        buffer.flush();
    }

    table.state().flushed.clone()
}

/// The JSON `insert_delete` format expresses both polarities explicitly.
#[test]
fn json_insert_delete() {
    let updates = ingest::<TestStructSoftDelete>(
        TestStructSoftDelete::schema(),
        &format_config("json", json!({"update_format": "insert_delete"})),
        &[
            br#"{"insert": {"id": 1, "s": "one"}}"#,
            br#"{"delete": {"id": 2, "s": "two"}}"#,
            // A record deleted and re-inserted lands twice, once with each
            // polarity: the table keeps the whole history.
            br#"{"delete": {"id": 3, "s": "three"}}"#,
            br#"{"insert": {"id": 3, "s": "three"}}"#,
        ],
    );

    assert_eq!(
        updates,
        vec![
            MockUpdate::Insert(TestStructSoftDelete::inserted(1, "one")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(2, "two")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(3, "three")),
            MockUpdate::Insert(TestStructSoftDelete::inserted(3, "three")),
        ]
    );
}

/// A Debezium update deletes the `before` image and inserts the `after` image,
/// so a soft-delete connector records both.
#[test]
fn json_debezium() {
    let updates = ingest::<TestStructSoftDelete>(
        TestStructSoftDelete::schema(),
        &format_config("json", json!({"update_format": "debezium"})),
        &[
            br#"{"payload": {"op": "c", "after": {"id": 1, "s": "one"}}}"#,
            br#"{"payload": {"op": "d", "before": {"id": 2, "s": "two"}}}"#,
            br#"{"payload": {"op": "u", "before": {"id": 3, "s": "old"}, "after": {"id": 3, "s": "new"}}}"#,
        ],
    );

    assert_eq!(
        updates,
        vec![
            MockUpdate::Insert(TestStructSoftDelete::inserted(1, "one")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(2, "two")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(3, "old")),
            MockUpdate::Insert(TestStructSoftDelete::inserted(3, "new")),
        ]
    );
}

/// The JSON `raw` format only inserts, so soft deletes leave it alone.
#[test]
fn json_raw() {
    let updates = ingest::<TestStructSoftDelete>(
        TestStructSoftDelete::schema(),
        &format_config("json", json!({"update_format": "raw", "array": true})),
        &[br#"[{"id": 1, "s": "one"}, {"id": 2, "s": "two"}]"#],
    );

    assert_eq!(
        updates,
        vec![
            MockUpdate::Insert(TestStructSoftDelete::inserted(1, "one")),
            MockUpdate::Insert(TestStructSoftDelete::inserted(2, "two")),
        ]
    );
}

/// CSV records are positional, so the `is_delete` column has to appear in the
/// record.  A value in the record takes precedence over metadata, the way a
/// column default does.
#[test]
fn csv() {
    let updates = ingest::<TestStructSoftDelete>(
        TestStructSoftDelete::schema(),
        &format_config("csv", json!({})),
        &[b"1,one,\n2,two,true\n"],
    );

    assert_eq!(
        updates,
        vec![
            MockUpdate::Insert(TestStructSoftDelete::inserted(1, "one")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(2, "two")),
        ]
    );
}

/// Row of a table fed by the `raw` format, which stores each message in a
/// single column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RawRecord {
    s: String,
    is_delete: Option<bool>,
}

deserialize_table_record!(RawRecord["RawRecord", Variant, 2] {
    (s, "s", false, String, |_| None),
    (is_delete, "is_delete", false, Option<bool>, |__feldera_metadata: &Option<Variant>| Some(__feldera_metadata.as_ref().and_then(|metadata| bool::try_from(metadata.index_string("is_delete")).ok())))
});

/// The `raw` format only inserts, so soft deletes leave it alone.
#[test]
fn raw() {
    let updates = ingest::<RawRecord>(
        vec![
            Field::new("s".into(), ColumnType::varchar(false)),
            Field::new("is_delete".into(), ColumnType::boolean(true)),
        ],
        &format_config("raw", json!({"column_name": "s"})),
        &[b"raw message"],
    );

    assert_eq!(
        updates,
        vec![MockUpdate::Insert(RawRecord {
            s: "raw message".to_string(),
            is_delete: None,
        })]
    );
}

/// Records of the soft-delete test table as an Arrow batch, the encoding used
/// by Parquet and by connectors that read columnar sources.
fn arrow_batch(records: &[(i64, &str)]) -> RecordBatch {
    let ids = records.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let strings = records.iter().map(|(_, s)| *s).collect::<Vec<_>>();

    RecordBatch::try_new(
        TestStructSoftDelete::arrow_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(strings)),
        ],
    )
    .unwrap()
}

/// The Parquet format only inserts, so soft deletes leave it alone.
#[test]
fn parquet() {
    let mut parquet = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut parquet, TestStructSoftDelete::arrow_schema(), None).unwrap();
    writer
        .write(&arrow_batch(&[(1, "one"), (2, "two")]))
        .unwrap();
    writer.close().unwrap();

    let updates = ingest::<TestStructSoftDelete>(
        TestStructSoftDelete::schema(),
        &format_config("parquet", json!({})),
        &[&parquet],
    );

    assert_eq!(
        updates,
        vec![
            MockUpdate::Insert(TestStructSoftDelete::inserted(1, "one")),
            MockUpdate::Insert(TestStructSoftDelete::inserted(2, "two")),
        ]
    );
}

/// Arrow input stream of a soft-delete connector, the path taken by integrated
/// connectors such as Delta Lake and Iceberg, which parse columnar data
/// themselves.
fn arrow_stream() -> (
    MockDeZSet<TestStructSoftDelete, TestStructSoftDelete>,
    Box<dyn ArrowStream>,
) {
    let table = MockDeZSet::<TestStructSoftDelete, TestStructSoftDelete>::new();
    let handle = SoftDeleteHandle::new(Box::new(table.clone()));
    let stream = handle
        .configure_arrow_deserializer(SqlSerdeConfig::default())
        .unwrap();

    (table, stream)
}

/// A batch of deletions becomes a batch of insertions.
#[test]
fn arrow_delete() {
    let (table, mut stream) = arrow_stream();

    stream.insert(&arrow_batch(&[(1, "one")]), &None).unwrap();
    stream.delete(&arrow_batch(&[(2, "two")]), &None).unwrap();
    stream.flush();

    assert_eq!(
        table.state().flushed,
        vec![
            MockUpdate::Insert(TestStructSoftDelete::inserted(1, "one")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(2, "two")),
        ]
    );
}

/// A CDC connector reports the polarity of each row of a batch separately.  A
/// batch of one polarity is ingested as is; a mixed batch is split, insertions
/// first, since metadata applies to a whole batch.
#[test]
fn arrow_polarities() {
    let (table, mut stream) = arrow_stream();

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
    stream.flush();

    assert_eq!(
        table.state().flushed,
        vec![
            MockUpdate::Insert(TestStructSoftDelete::inserted(1, "one")),
            MockUpdate::Insert(TestStructSoftDelete::inserted(2, "two")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(3, "three")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(4, "four")),
            MockUpdate::Insert(TestStructSoftDelete::inserted(6, "six")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(5, "five")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(7, "seven")),
        ]
    );
}

/// A polarity array that does not match the batch is a bug in the caller, not
/// input data, so it must be reported rather than silently misapplied.
#[test]
fn arrow_polarities_length_mismatch() {
    let (_table, mut stream) = arrow_stream();

    let error = stream
        .insert_with_polarities(&arrow_batch(&[(1, "one")]), &[true, false], &None)
        .unwrap_err();
    assert!(
        error.to_string().contains("polarities"),
        "unexpected error: {error}"
    );
}

/// Transports that stage input buffers instead of flushing them, e.g., the
/// fault-tolerant Kafka connector, push the same records into the circuit.
#[test]
fn staged_buffers() {
    let relation = Relation::new(
        "test_input".into(),
        TestStructSoftDelete::schema(),
        false,
        BTreeMap::new(),
    );
    let (_consumer, mut parser, table) =
        mock_soft_delete_parser_pipeline::<TestStructSoftDelete, TestStructSoftDelete>(
            &relation,
            &format_config("json", json!({"update_format": "insert_delete"})),
        )
        .unwrap();

    let (buffer, errors) = parser.parse(br#"{"delete": {"id": 1, "s": "one"}}"#, None);
    assert_eq!(&errors, &[]);
    parser.stage(vec![buffer.unwrap()]).flush();

    assert_eq!(
        table.state().flushed,
        vec![MockUpdate::Insert(TestStructSoftDelete::deleted(1, "one"))]
    );
}

/// Forking a soft-delete stream, which multithreaded transports do to run
/// several parsers in parallel, keeps soft deletes enabled.
#[test]
fn fork_preserves_soft_delete() {
    let table = MockDeZSet::<TestStructSoftDelete, TestStructSoftDelete>::new();
    let handle = SoftDeleteHandle::new(Box::new(table.clone()));

    let stream = handle
        .configure_deserializer(RecordFormat::Json(Default::default()))
        .unwrap();
    let mut fork = stream.fork();
    fork.delete(br#"{"id": 1, "s": "one"}"#, &None).unwrap();
    fork.flush();

    let forked_handle = handle.fork();
    let mut stream = forked_handle
        .configure_deserializer(RecordFormat::Json(Default::default()))
        .unwrap();
    stream.delete(br#"{"id": 2, "s": "two"}"#, &None).unwrap();
    stream.flush();

    assert_eq!(
        table.state().flushed,
        vec![
            MockUpdate::Insert(TestStructSoftDelete::deleted(1, "one")),
            MockUpdate::Insert(TestStructSoftDelete::deleted(2, "two")),
        ]
    );
}

/// End to end: a connector configured with `soft_delete` fills the table with
/// the history of its input stream.
#[test]
fn controller_ingests_history() {
    init_test_logger();

    let mut input_file = NamedTempFile::new().unwrap();
    input_file
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
                    "config": {
                        "path": input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete"
                    }
                }
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStructSoftDelete>(
                circuit_config,
                &TestStructSoftDelete::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

    let contents = controller
        .execute_query_text_sync("select * from test_output1 order by id, is_delete nulls first")
        .unwrap();

    let expected = r#"+----+-------+-----------+
| id | s     | is_delete |
+----+-------+-----------+
| 1  | one   |           |
| 2  | two   | true      |
| 3  | three |           |
| 3  | three | true      |
+----+-------+-----------+"#;

    assert_eq!(&contents, expected);
    controller.stop().unwrap();
}

/// A deletion in a table with a primary key identifies a key, not a record, so
/// there is nothing to insert in its place: the connector must be rejected
/// rather than ingest the key as a row.
#[test]
fn controller_rejects_primary_key() {
    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_soft_delete_primary_key",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "soft_delete": true,
                "transport": {"name": "empty_input"},
                "format": {"name": "json", "config": {"update_format": "insert_delete"}}
            }
        }
    }))
    .unwrap();

    let error = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit_with_index::<TestStruct, KeyStruct, _>(
                circuit_config,
                &TestStruct::schema(),
                &["id".into()],
                |test_struct: &TestStruct| KeyStruct { id: test_struct.id },
                &[None],
                false,
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .err()
    .expect("connector creation must fail");

    assert!(
        error.to_string().contains("primary key"),
        "unexpected error: {error}"
    );
}

/// Soft deletes describe how a connector ingests records, so an output
/// connector that sets the property is misconfigured.
#[test]
fn controller_rejects_output_connector() {
    init_test_logger();

    let output_file = NamedTempFile::new().unwrap();
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_soft_delete_output",
        "workers": 1,
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "soft_delete": true,
                "transport": {
                    "name": "file_output",
                    "config": {"path": output_file.path()}
                },
                "format": {"name": "json"}
            }
        }
    }))
    .unwrap();

    let error = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStructSoftDelete>(
                circuit_config,
                &TestStructSoftDelete::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .err()
    .expect("connector creation must fail");

    assert!(
        error.to_string().contains("input connectors"),
        "unexpected error: {error}"
    );
}
