//! The `arrow_ipc` format: weighted rows written as one Arrow IPC stream per step, in the
//! relation's published schema, and read back from messages of one or more streams.

use crate::test::engine_test;
use std::borrow::Cow;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use feldera_adapterlib::format::{InputBuffer, ParseError, Parser};
use feldera_adapterlib::test::{
    ConnectorTestEngine, InputOptions, MockOutputConsumer, TestCollection, Updates,
};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::config::{ConnectorConfig, FormatConfig};
use feldera_types::program_schema::Relation;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use serde_json::json;

use super::WEIGHT_COLUMN;
use crate::format::get_output_format;
use crate::test::{encoder, parser, published_schema, relations, rows};

/// A table of assorted types, with a case-sensitive column name.
pub fn assorted_relation() -> Relation {
    relations::assorted("arrow_ipc_assorted")
}

/// A table of every type.
pub fn every_type_relation() -> Relation {
    relations::every_type("arrow_ipc_every_type")
}

/// The format, with no options.
fn format() -> FormatConfig {
    FormatConfig {
        name: Cow::from("arrow_ipc"),
        config: json!({}),
    }
}

/// Returns `n` distinct rows of `relation`, numbered by its column `numbered`.
fn sample_rows(relation: &Relation, numbered: &str, n: usize) -> RecordBatch {
    let batches = rows::batches(relation, numbered, 1..=1, n)
        .prop_filter("exactly n rows", move |batches| batches[0].len() == n)
        .new_tree(&mut TestRunner::deterministic())
        .unwrap()
        .current();
    batches[0].rows().clone()
}

/// Encodes each of `steps`, rows of `relation` with their weights, as one step, and returns
/// what the encoder wrote, the steps one after another.
fn encode(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    steps: &[(&RecordBatch, &[i64])],
) -> Vec<u8> {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let mut encoder = encoder(
        &format(),
        None,
        relation,
        Box::new(MockOutputConsumer::with_buffer(buffer.clone())),
    );
    for (step, (rows, weights)) in steps.iter().enumerate() {
        encoder
            .consumer()
            .batch_start(step as u64, OutputBatchType::Delta);
        encoder
            .encode(engine.output_batch(relation, rows, weights).unwrap())
            .unwrap();
        encoder.consumer().batch_end();
    }
    buffer
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_key, value, _headers)| value.clone())
        .flatten()
        .collect()
}

/// Returns the streams in `bytes`: the schema and the batches of each.
fn streams(bytes: &[u8]) -> Vec<(Arc<Schema>, Vec<RecordBatch>)> {
    let mut cursor = Cursor::new(bytes);
    let mut streams = Vec::new();
    while (cursor.position() as usize) < bytes.len() {
        let reader = StreamReader::try_new(&mut cursor, None).unwrap();
        let schema = reader.schema();
        streams.push((schema, reader.collect::<Result<_, _>>().unwrap()));
    }
    streams
}

/// Returns `batch`, a stream batch, as rows with their weights.
fn weighted(batch: &RecordBatch) -> Updates {
    let weight_index = batch.schema().index_of(WEIGHT_COLUMN).unwrap();
    let weights = batch
        .column(weight_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    let columns: Vec<usize> = (0..batch.num_columns())
        .filter(|&index| index != weight_index)
        .collect();
    Updates::new(batch.project(&columns).unwrap(), weights).unwrap()
}

/// Returns one stream of `rows`, with `weights` as the weight column if there are any.
fn stream(rows: &RecordBatch, weights: Option<ArrayRef>) -> Vec<u8> {
    let mut fields = rows.schema().fields().to_vec();
    let mut columns = rows.columns().to_vec();
    if let Some(weights) = weights {
        fields.push(Arc::new(ArrowField::new(
            WEIGHT_COLUMN,
            weights.data_type().clone(),
            true,
        )));
        columns.push(weights);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}

/// Parses `bytes` into a fresh table of `relation`, and returns the table and the errors.
fn parse(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    bytes: &[u8],
) -> (Box<dyn TestCollection>, Vec<ParseError>) {
    let collection = engine
        .input_collection(relation, &InputOptions::default())
        .unwrap();
    let mut parser = parser(collection.as_ref(), &format());
    // The cases check the errors that `parse` returns.
    parser.on_error(Some(Box::new(|_fatal, _error| ())));
    let (mut buffer, errors) = parser.parse(bytes, None);
    buffer.flush().unwrap();
    (collection, errors)
}

/// Encodes three rows of `relation` with weights 2, -1, and 0, checks that the stream holds
/// the two rows with a nonzero weight once each, in the published schema plus the weight,
/// and that the stream parses back into the same Z-set.
fn check_round_trip(engine: &dyn ConnectorTestEngine, relation: &Relation, numbered: &str) {
    let rows = sample_rows(relation, numbered, 3);
    let weights = [2, -1, 0];
    let expected = Updates::new(rows.clone(), weights.to_vec())
        .unwrap()
        .zset()
        .unwrap();

    let bytes = encode(engine, relation, &[(&rows, &weights)]);
    let streams = streams(&bytes);
    assert_eq!(streams.len(), 1);
    let (schema, batches) = &streams[0];
    let mut fields = published_schema(relation).fields().to_vec();
    fields.push(Arc::new(ArrowField::new(
        WEIGHT_COLUMN,
        DataType::Int64,
        false,
    )));
    assert_eq!(**schema, Schema::new(fields));
    assert_eq!(batches.len(), 1);
    let written = weighted(&batches[0]);
    assert_eq!(written.len(), 2);
    assert_eq!(written.zset().unwrap(), expected);

    let (collection, errors) = parse(engine, relation, &bytes);
    assert_eq!(errors, []);
    assert_eq!(collection.zset().unwrap(), expected);
}

/// Rows of assorted types, a case-sensitive column name among them, survive a round trip.
#[engine_test]
pub fn round_trip_assorted(engine: &dyn ConnectorTestEngine) {
    check_round_trip(engine, &assorted_relation(), "id");
}

/// Rows of every type survive a round trip.
#[engine_test]
pub fn round_trip_every_type(engine: &dyn ConnectorTestEngine) {
    check_round_trip(engine, &every_type_relation(), "bigint");
}

/// The encoder writes one stream per step, and the parser reads every stream of a message.
#[engine_test]
pub fn reads_every_stream_of_a_message(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    let rows = sample_rows(&relation, "id", 4);
    let first = rows.slice(0, 2);
    let second = rows.slice(2, 2);
    let bytes = encode(engine, &relation, &[(&first, &[1, 1]), (&second, &[1, 3])]);
    assert_eq!(streams(&bytes).len(), 2);

    let (collection, errors) = parse(engine, &relation, &bytes);
    assert_eq!(errors, []);
    let expected = Updates::new(rows, vec![1, 1, 1, 3])
        .unwrap()
        .zset()
        .unwrap();
    assert_eq!(collection.zset().unwrap(), expected);
}

/// A weight of 2 inserts its row twice, -1 deletes it once, and 0 drops it; a stream
/// without weights inserts each row once.
#[engine_test]
pub fn weights_become_inserts_deletes_and_copies(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    let rows = sample_rows(&relation, "id", 3);

    let weights = Arc::new(Int64Array::from(vec![2, -1, 0])) as ArrayRef;
    let (collection, errors) = parse(engine, &relation, &stream(&rows, Some(weights)));
    assert_eq!(errors, []);
    let take =
        |indices: &[u32]| take_record_batch(&rows, &UInt32Array::from(indices.to_vec())).unwrap();
    assert_eq!(
        collection.updates().unwrap(),
        Updates::new(take(&[0, 0, 1]), vec![1, 1, -1]).unwrap()
    );

    let (collection, errors) = parse(engine, &relation, &stream(&rows, None));
    assert_eq!(errors, []);
    assert_eq!(
        collection.updates().unwrap(),
        Updates::new(rows.clone(), vec![1, 1, 1]).unwrap()
    );
}

/// An empty step writes nothing.
#[engine_test]
pub fn an_empty_step_writes_nothing(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    let rows = sample_rows(&relation, "id", 1);
    assert!(encode(engine, &relation, &[(&rows.slice(0, 0), &[])]).is_empty());
}

/// Returns the error that the parser reports for a stream that `bytes` cannot start.
fn stream_error(bytes: &[u8]) -> ParseError {
    let error = StreamReader::try_new(Cursor::new(bytes), None)
        .err()
        .unwrap();
    ParseError::bin_envelope_error(
        format!("error parsing an Arrow IPC stream: {error}"),
        &[],
        None,
    )
}

/// Returns the error that the parser reports for batch `index` with `error`.
fn batch_error(index: usize, error: &str) -> ParseError {
    ParseError::bin_envelope_error(
        format!("error parsing arrow_ipc data (batch {index}): {error}"),
        &[],
        None,
    )
}

/// The parser reports what is wrong with a message: bytes that are not a stream, bytes after
/// the last stream, and weights that are not Int64, are null, or expand to too many rows.
#[engine_test]
pub fn malformed_input_reports_what_is_wrong(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    let rows = sample_rows(&relation, "id", 2);

    let (_, errors) = parse(engine, &relation, b"not arrow");
    assert_eq!(errors, [stream_error(b"not arrow")]);

    let mut bytes = stream(&rows, None);
    bytes.extend_from_slice(b"junk");
    let (collection, errors) = parse(engine, &relation, &bytes);
    assert_eq!(errors, [stream_error(b"junk")]);
    assert_eq!(collection.len(), 2);

    let int32 = Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef;
    let (_, errors) = parse(engine, &relation, &stream(&rows, Some(int32)));
    assert_eq!(
        errors,
        [batch_error(
            0,
            "column '__feldera_weight' is Int32, not Int64"
        )]
    );

    let null = Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef;
    let (_, errors) = parse(engine, &relation, &stream(&rows, Some(null)));
    assert_eq!(
        errors,
        [batch_error(
            0,
            "column '__feldera_weight' holds a null weight"
        )]
    );

    let huge = Arc::new(Int64Array::from(vec![i32::MAX as i64, 1])) as ArrayRef;
    let (_, errors) = parse(engine, &relation, &stream(&rows, Some(huge)));
    assert_eq!(
        errors,
        [batch_error(
            0,
            "the batch's weights expand to more than 2147483647 rows"
        )]
    );
}

/// The encoder refuses an index, and both directions accept options they do not know.
#[engine_test]
pub fn configuration_refuses_an_index_and_ignores_unknown_options(
    engine: &dyn ConnectorTestEngine,
) {
    let relation = assorted_relation();
    let connector: ConnectorConfig = serde_json::from_value(json!({
        "transport": { "name": "file_output", "config": { "path": "/dev/null" } },
        "format": { "name": "arrow_ipc", "config": { "rows": 3 } },
    }))
    .unwrap();
    let format = get_output_format("arrow_ipc").unwrap();
    let error = format
        .new_encoder(
            "test_output",
            &connector,
            &None,
            &relation,
            Box::new(MockOutputConsumer::new()),
            true,
        )
        .err()
        .unwrap();
    assert_eq!(
        error.to_string(),
        "invalid controller configuration: invalid format configuration for output endpoint \
         'test_output': 'arrow_ipc' encoder cannot be attached to an index"
    );
    assert!(
        format
            .new_encoder(
                "test_output",
                &connector,
                &None,
                &relation,
                Box::new(MockOutputConsumer::new()),
                false,
            )
            .is_ok()
    );

    let collection = engine
        .input_collection(&relation, &InputOptions::default())
        .unwrap();
    parser(
        collection.as_ref(),
        &FormatConfig {
            name: Cow::from("arrow_ipc"),
            config: json!({ "rows": 3 }),
        },
    );
}
