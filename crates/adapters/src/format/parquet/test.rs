use std::collections::BTreeMap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use dbsp::OrdZSet;
use dbsp::utils::Tup2;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_sqllib::Variant;
use feldera_types::deserialize_table_record;
use feldera_types::format::parquet::ParquetEncoderConfig;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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
    format::{Encoder, parquet::ParquetEncoder},
    static_compile::seroutput::SerBatchImpl,
    test::{DEFAULT_TIMEOUT_MS, MockOutputConsumer, TestStruct2, mock_input_pipeline, wait},
};

/// Parse a Parquet file into an array of `T`, reading it as Arrow.
///
/// `config` must match the one the writer used. Reading through Arrow rather
/// than through the row reader's JSON projection matters for any column whose
/// value is binary: the JSON projection base64-encodes it, which a VARIANT
/// column stored as a Parquet variant cannot survive.
pub fn load_parquet_file<T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>>(
    path: &Path,
    config: &SqlSerdeConfig,
) -> Vec<T> {
    let file = File::open(path).unwrap_or_else(|e| panic!("error opening {path:?}: {e}"));
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|e| panic!("error reading parquet file {path:?}: {e}"))
        .build()
        .unwrap();

    let mut records = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let deserializer = serde_arrow::Deserializer::from_record_batch(&batch).unwrap();
        records.extend(
            Vec::<T>::deserialize_with_context(deserializer, config)
                .unwrap_or_else(|e| panic!("error deserializing {path:?}: {e}")),
        );
    }
    records
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

/// Record with two VARIANT columns: one held as a Parquet variant, one as JSON
/// text.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Default)]
struct VariantRecord {
    id: i64,
    v: Option<Variant>,
    json_v: Option<Variant>,
}

deserialize_table_record!(VariantRecord["VariantRecord", Variant, 3] {
    (id, "id", false, i64, |_| None),
    (v, "v", false, Option<Variant>, |_| Some(None)),
    (json_v, "json_v", false, Option<Variant>, |_| Some(None))
});

impl VariantRecord {
    fn schema() -> Vec<Field> {
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("v".into(), ColumnType::variant(true)),
            Field::new("json_v".into(), ColumnType::variant(true)),
        ]
    }

    fn arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("v", variant_arrow_type(), true),
            ArrowField::new("json_v", ArrowDataType::Utf8, true),
        ]))
    }
}

/// The unshredded Parquet variant storage type.
fn variant_arrow_type() -> ArrowDataType {
    ArrowDataType::Struct(
        vec![
            ArrowField::new("metadata", ArrowDataType::Binary, false),
            ArrowField::new("value", ArrowDataType::Binary, false),
        ]
        .into(),
    )
}

/// A Parquet file whose VARIANT column holds the binary variant encoding is
/// read through the same Arrow path every table-format connector uses, with no
/// Delta or Iceberg table involved.
///
/// The second row leaves both VARIANT columns NULL, and `json_v` proves a
/// column of JSON text still reads the way it always did.
#[test]
fn parquet_input_variant_test() {
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use chrono::NaiveDate;
    use feldera_sqllib::{ByteArray, Date, SqlString};
    use parquet_variant::{Variant as PqVariant, VariantBuilderExt, VariantDecimal4};
    use parquet_variant_compute::VariantArrayBuilder;

    let temp_file = NamedTempFile::new().unwrap();
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "file_input",
            "config": { "path": temp_file.path() }
        },
        "format": { "name": "parquet" }
    }))
    .unwrap();

    let mut builder = VariantArrayBuilder::new(2);
    {
        let mut object = builder.new_object();
        object.insert("int", 42i64);
        object.insert("str", "hello");
        object.insert("bool", true);
        object.insert("nul", PqVariant::Null);
        object.insert(
            "dec",
            PqVariant::Decimal4(VariantDecimal4::try_new(12_345i32, 2u8).unwrap()),
        );
        object.insert(
            "date",
            PqVariant::Date(NaiveDate::from_ymd_opt(2026, 9, 1).unwrap()),
        );
        object.insert("bin", PqVariant::Binary(&[1u8, 2, 3]));
        {
            let mut list = object.new_list("list");
            list.append_value(1i64);
            list.append_value("two");
            list.finish();
        }
        object.finish();
    }
    builder.append_null();

    // The Parquet variant builder produces `BinaryView` sub-fields.
    let variant_column =
        arrow::compute::cast(&ArrayRef::from(builder.build()), &variant_arrow_type()).unwrap();

    let batch = RecordBatch::try_new(
        VariantRecord::arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
            variant_column,
            Arc::new(StringArray::from(vec![Some(r#"{"legacy": true}"#), None])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut writer = ArrowWriter::try_new(&temp_file, VariantRecord::arrow_schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let (endpoint, _consumer, parser, zset) = mock_input_pipeline::<VariantRecord, VariantRecord>(
        config,
        Relation::new(
            "test".into(),
            VariantRecord::schema(),
            false,
            BTreeMap::new(),
        ),
    )
    .unwrap();
    endpoint.extend();
    wait(
        || {
            endpoint.queue(false);
            // Fail on the first parse error rather than spinning until the
            // timeout: a VARIANT the parser cannot read shows up here.
            let errors = parser.state().parser_result.clone().unwrap_or_default();
            assert!(errors.is_empty(), "parse errors: {errors:?}");
            zset.state().flushed.len() == 2
        },
        10_000,
    )
    .unwrap();

    let key = |k: &str| Variant::String(SqlString::from_ref(k));
    let expected = [
        VariantRecord {
            id: 1,
            // Every value keeps the type the writer encoded.
            v: Some(Variant::Map(
                BTreeMap::from([
                    (key("bin"), Variant::Binary(ByteArray::new(&[1, 2, 3]))),
                    (key("bool"), Variant::Boolean(true)),
                    (
                        key("date"),
                        Variant::Date(Date::from_date(
                            NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
                        )),
                    ),
                    (key("dec"), Variant::SqlDecimal((12_345, 2))),
                    (key("int"), Variant::BigInt(42)),
                    (
                        key("list"),
                        Variant::Array(
                            vec![
                                Variant::BigInt(1),
                                Variant::String(SqlString::from_ref("two")),
                            ]
                            .into(),
                        ),
                    ),
                    (key("nul"), Variant::VariantNull),
                    (key("str"), Variant::String(SqlString::from_ref("hello"))),
                ])
                .into(),
            )),
            json_v: Some(Variant::Map(
                BTreeMap::from([(key("legacy"), Variant::Boolean(true))]).into(),
            )),
        },
        VariantRecord {
            id: 2,
            v: None,
            json_v: None,
        },
    ];

    let flushed = zset.state().flushed.clone();
    for (update, expected) in flushed.iter().zip(expected.iter()) {
        assert_eq!(update.unwrap_insert(), expected);
    }
    assert_eq!(flushed.len(), expected.len());
}
