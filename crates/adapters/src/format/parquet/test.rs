use std::collections::BTreeMap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use arrow::array::{Array, ListArray, RecordBatch, StructArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use bytes::Bytes;
use chrono::DateTime;
use dbsp::OrdZSet;
use dbsp::utils::Tup2;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_sqllib::Timestamp;
use feldera_sqllib::Variant;
use feldera_types::format::json::JsonFlavor;
use feldera_types::format::parquet::ParquetEncoderConfig;
use feldera_types::program_schema::Relation;
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::serialized_reader::SerializedFileReader;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::NamedTempFile;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::{
    catalog::SerBatchReader,
    format::{Encoder, parquet::ParquetEncoder, parquet::open_parquet_reader},
    static_compile::seroutput::SerBatchImpl,
    test::{
        DEFAULT_TIMEOUT_MS, MockOutputConsumer, TestStruct2, TimestampTestStruct,
        mock_input_pipeline,
        parquet_timestamps::{
            TimestampEncoding, timestamp_test_data, write_mixed_column_parquet,
            write_nested_int96_parquet, write_sub_microsecond_int96_parquet,
            write_timestamp_parquet,
        },
        wait,
    },
};

/// Parse Parquet file into an array of `T`.
pub fn load_parquet_file<T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>>(
    path: &Path,
) -> Vec<T> {
    let file = File::open(path).unwrap();

    SerializedFileReader::new(file)
        .unwrap_or_else(|_| panic!("error opening parquet file {path:?}"))
        .into_iter()
        .map(|row| {
            let row = row.unwrap();

            let row = row.to_json_value();
            // println!("row: {row}");

            T::deserialize_with_context(row, &SqlSerdeConfig::from(JsonFlavor::ParquetConverter))
                .unwrap()
        })
        .collect::<Vec<_>>()
}

#[test]
fn rel_to_schema() {
    use super::relation_to_parquet_schema;
    relation_to_parquet_schema(&TestStruct2::schema(), false).expect("Can convert");
}

fn parquet_input_test(compression: Compression) {
    // Prepare input data & pipeline
    let test_data = TestStruct2::data();
    let temp_file = NamedTempFile::new().unwrap();
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "file_input",
            "config": {
                "path": temp_file.path(),
                "buffer_size_bytes": 5
            }
        },
        "format": {
            "name": "parquet"
        }
    }))
    .unwrap();

    let batch = RecordBatch::try_new(
        TestStruct2::arrow_schema(),
        TestStruct2::make_arrow_array(&test_data),
    )
    .expect("RecordBatch creation should succeed");
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(&temp_file, TestStruct2::arrow_schema(), Some(props))
        .expect("Writer creation should succeed");
    writer
        .write(&batch)
        .expect("Writing to parquet should succeed");
    writer.close().expect("Closing the writer should succeed");

    // Send the data through the mock pipeline
    let (endpoint, consumer, parser, zset) = mock_input_pipeline::<TestStruct2, TestStruct2>(
        config,
        Relation::new("test".into(), TestStruct2::schema(), false, BTreeMap::new()),
    )
    .unwrap();
    sleep(Duration::from_millis(10));
    assert!(parser.state().data.is_empty());
    assert!(!consumer.state().eoi);
    endpoint.extend();
    wait(
        || {
            endpoint.queue(false);
            zset.state().flushed.len() == test_data.len()
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    // Make sure the input data matches original test structs
    for (i, upd) in zset.state().flushed.iter().enumerate() {
        assert_eq!(upd.unwrap_insert(), &test_data[i]);
    }
}

/// The `parquet` input format decodes INT96 timestamps without going through
/// nanoseconds, so a far-future or far-past date survives ingest.
#[test]
fn parquet_input_int96_timestamps() {
    let test_data = timestamp_test_data();
    let temp_file = NamedTempFile::new().unwrap();
    write_timestamp_parquet(temp_file.path(), &test_data, TimestampEncoding::Int96);

    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "file_input",
            "config": {
                "path": temp_file.path()
            }
        },
        "format": {
            "name": "parquet"
        }
    }))
    .unwrap();

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<TimestampTestStruct, TimestampTestStruct>(
            config,
            Relation::new(
                "test".into(),
                TimestampTestStruct::schema(),
                false,
                BTreeMap::new(),
            ),
        )
        .unwrap();
    endpoint.extend();
    wait(
        || {
            endpoint.queue(false);
            zset.state().flushed.len() == test_data.len()
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    for (i, upd) in zset.state().flushed.iter().enumerate() {
        assert_eq!(upd.unwrap_insert(), &test_data[i]);
    }
}

/// Sub-microsecond precision in an INT96 timestamp is dropped, not rounded.
///
/// INT96 counts nanoseconds within the day, so a writer can store a remainder
/// finer than Feldera's `TIMESTAMP` holds. `Int96::to_micros` truncates it
/// toward zero, which is a floor even before the epoch, where the negative
/// part sits in the day number rather than the nanoseconds.
///
/// Each row below reads back as exactly the microsecond it was built from, and
/// every remainder is large enough that rounding would give the next one.
#[test]
fn parquet_input_int96_truncates_sub_microseconds() {
    fn at(rfc3339: &str) -> Timestamp {
        Timestamp::from_dateTime(DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc())
    }

    let rows = [
        (at("2024-06-01T12:00:00.123456Z"), 789),
        // Before the epoch, so the day number is negative while the
        // nanoseconds within the day stay positive.
        (at("1600-01-01T00:00:00.123456Z"), 789),
        // Past the nanosecond range, and one nanosecond below a whole
        // microsecond.
        (at("4000-12-31T00:00:00.999999Z"), 999),
    ];

    let temp_file = NamedTempFile::new().unwrap();
    write_sub_microsecond_int96_parquet(temp_file.path(), &rows);

    let data = Bytes::from(std::fs::read(temp_file.path()).unwrap());
    let batches: Vec<RecordBatch> = open_parquet_reader(data, 1024)
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect();
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("INT96 must decode at microsecond resolution");

    let read_back: Vec<i64> = column.values().to_vec();
    let expected: Vec<i64> = rows.iter().map(|(ts, _)| ts.microseconds()).collect();
    assert_eq!(read_back, expected);
}

/// A file whose two timestamp columns carry different physical encodings
/// decodes each one on its own terms: the INT96 column reads back retyped to
/// microseconds, the INT64 microsecond column exactly as it was written, down
/// to its UTC marker.
///
/// Coercion keys off a column's own physical type, so neither column's
/// treatment depends on the other. Spark leaves a file in this shape when it
/// rewrites some timestamp columns and not others.
#[test]
fn parquet_input_mixed_timestamp_columns_in_one_file() {
    let test_data = timestamp_test_data();
    let temp_file = NamedTempFile::new().unwrap();
    write_mixed_column_parquet(temp_file.path(), &test_data);

    let data = Bytes::from(std::fs::read(temp_file.path()).unwrap());
    let batches: Vec<RecordBatch> = open_parquet_reader(data, 1024)
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect();
    let batch = &batches[0];

    // INT96 carries no time zone, so coercion lands on a naive microsecond
    // timestamp; the INT64 column keeps the UTC marker it was written with.
    let schema = batch.schema();
    assert_eq!(
        schema.field_with_name("ts_int96").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    assert_eq!(
        schema.field_with_name("ts_micros").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    );

    let expected: Vec<Option<i64>> = test_data
        .iter()
        .map(|row| row.ts.map(|ts| ts.microseconds()))
        .collect();
    for name in ["ts_int96", "ts_micros"] {
        let column = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap_or_else(|| panic!("{name} must decode at microsecond resolution"));
        let read_back: Vec<Option<i64>> = column.iter().collect();
        assert_eq!(read_back, expected, "column {name}");
    }
}

/// An INT96 timestamp inside a struct field and inside a list element decodes
/// at microsecond resolution, the same as one at the top level.
///
/// Delta permits timestamps at any depth and Spark writes those as INT96 too.
/// Coercion reaches them by walking struct, list and map paths, including the
/// `.list.element` name Parquet gives a list's elements.
#[test]
fn parquet_input_nested_int96_timestamps() {
    let test_data = timestamp_test_data();
    let temp_file = NamedTempFile::new().unwrap();
    write_nested_int96_parquet(temp_file.path(), &test_data);

    let data = Bytes::from(std::fs::read(temp_file.path()).unwrap());
    let batches: Vec<RecordBatch> = open_parquet_reader(data, 1024)
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect();
    let batch = &batches[0];

    // The fixture skips rows without a timestamp.
    let expected: Vec<i64> = test_data
        .iter()
        .filter_map(|row| row.ts)
        .map(|ts| ts.microseconds())
        .collect();

    let in_struct = batch
        .column_by_name("nested")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column_by_name("ts")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("INT96 inside a struct must decode at microsecond resolution")
        .values()
        .to_vec();
    assert_eq!(in_struct, expected, "struct field");

    let in_list = batch
        .column_by_name("ts_list")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("INT96 inside a list must decode at microsecond resolution")
        .values()
        .to_vec();
    assert_eq!(in_list, expected, "list element");
}

#[test]
fn parquet_input_uncompressed() {
    parquet_input_test(Compression::UNCOMPRESSED);
}

#[test]
fn parquet_input_snappy() {
    parquet_input_test(Compression::SNAPPY);
}

#[test]
fn parquet_output() {
    let buffer = Arc::new(Mutex::new(Vec::with_capacity(4096)));
    let consumer = MockOutputConsumer::with_buffer(buffer.clone());
    let _consumer_data = consumer.data.clone();

    let config = ParquetEncoderConfig {
        buffer_size_records: usize::MAX,
    };

    let test_data = TestStruct2::data();
    let mut encoder = ParquetEncoder::new(
        Box::new(consumer),
        config,
        Relation::new(
            "TestStruct2".into(),
            TestStruct2::schema(),
            false,
            BTreeMap::new(),
        ),
        false,
    )
    .expect("Can't create encoder");
    let zset = OrdZSet::from_keys(
        (),
        vec![Tup2(test_data[0].clone(), 2), Tup2(test_data[1].clone(), 1)],
    );

    let zset = Arc::new(SerBatchImpl::<_, TestStruct2, ()>::new(zset)) as Arc<dyn SerBatchReader>;
    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    encoder.encode(zset).unwrap();
    encoder.consumer().batch_end();

    // Verify output buffer...
    // Construct the expected file manually:
    let test_denorm = vec![
        test_data[0].clone(),
        test_data[0].clone(),
        test_data[1].clone(),
    ];
    let batch = RecordBatch::try_new(
        TestStruct2::arrow_schema(),
        TestStruct2::make_arrow_array(&test_denorm),
    )
    .expect("RecordBatch creation should succeed");
    let props = WriterProperties::builder().build();

    let mut expected_buffer: Vec<u8> = vec![];
    let mut expected_buffer_cursor = Cursor::new(&mut expected_buffer);
    let mut writer = ArrowWriter::try_new(
        &mut expected_buffer_cursor,
        TestStruct2::arrow_schema(),
        Some(props),
    )
    .expect("Writer creation should succeed");
    writer
        .write(&batch)
        .expect("Writing to parquet should succeed");
    writer.close().expect("Closing the writer should succeed");
    debug_parquet_buffer(
        buffer
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_k, v, _headers)| v.clone())
            .flatten()
            .collect(),
    );

    let buffer_copy = buffer
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_k, v, _headers)| v.clone())
        .flatten()
        .collect::<Vec<_>>();

    assert_eq!(expected_buffer, buffer_copy);
}

fn debug_parquet_buffer(buffer: Vec<u8>) {
    use bytes::Bytes;
    use parquet::file::reader::FileReader;

    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::default()))
        .try_init();
    let buffer_copy = Bytes::from(buffer);
    let reader = SerializedFileReader::new(buffer_copy).expect("Reader creation should succeed");
    let row_iter = reader
        .get_row_iter(None)
        .expect("Row iterator creation should succeed");
    for maybe_record in row_iter {
        let record = maybe_record.expect("Record should be read successfully");
        tracing::info!("record = {:?}", record.to_string());
    }
}
