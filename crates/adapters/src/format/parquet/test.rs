//! Unit tests of the Parquet format that need no engine. The `parquet` connector suite in
//! `tests` covers the format with an engine.

use crate::test::{encoder, engine_test, input_pipeline, published_schema, read, relations};
#[cfg(test)]
use arrow::datatypes::Schema as ArrowSchema;
use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field as ArrowField, Fields, Schema, TimeUnit},
};
use bytes::Bytes;
use feldera_adapterlib::{
    test::{
        Capability, ConnectorTestEngine, InputOptions, MockOutputConsumer, Updates, ZSet,
        wait_for_rows,
    },
    transport::OutputBatchType,
};
use feldera_types::{
    config::{FormatConfig, InputEndpointConfig},
    program_schema::{ColumnType, Field, Relation},
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
#[cfg(test)]
use pretty_assertions::assert_eq;
use serde_json::json;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    sync::{Arc, Mutex},
    thread::sleep,
    time::Duration,
};
use tempfile::NamedTempFile;

#[test]
fn rel_to_schema() {
    use super::relation_to_parquet_schema;
    relation_to_parquet_schema(&relations::assorted("TestStruct2").fields, false)
        .expect("Can convert");
}

/// The table and view of the cases of assorted types.
pub fn relation() -> Relation {
    relations::assorted("parquet_assorted")
}

/// Two rows of [`relation`].
fn rows() -> Updates {
    Updates::from_json(
        published_schema(&relation()),
        &[
            (1, r#"{"id": 1, "nAmE": "test", "b": false, "ts": "1970-01-01T00:00:01", "dt": "1970-01-02", "es": {"a": false}, "m": {"foo": 100, "bar": 200}, "dec": "10.000"}"#),
            (1, r#"{"id": 2, "nAmE": null, "b": true, "ts": "1970-01-01T00:00:02", "dt": "1970-01-13", "es": {"a": true}, "m": {}, "dec": "0.001"}"#),
        ],
    )
    .unwrap()
}

/// A table of a BIGINT and two VARIANT columns.
pub fn variant_relation() -> Relation {
    Relation::new(
        "parquet_variant".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("v".into(), ColumnType::variant(true)),
            Field::new("json_v".into(), ColumnType::variant(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Returns a configuration of a file input endpoint that reads the Parquet file `path`, with
/// a buffer of `buffer_size_bytes`.
fn file_input(path: &std::path::Path, buffer_size_bytes: Option<usize>) -> InputEndpointConfig {
    let mut transport = json!({ "path": path });
    if let Some(buffer_size_bytes) = buffer_size_bytes {
        transport["buffer_size_bytes"] = json!(buffer_size_bytes);
    }
    serde_json::from_value(json!({
        "stream": "test_input",
        "transport": { "name": "file_input", "config": transport },
        "format": { "name": "parquet" }
    }))
    .unwrap()
}

/// Writes the rows of [`rows`] to a Parquet file with `compression`, reads the file through a
/// file endpoint with a 5-byte buffer, and checks that the rows arrive only after the
/// endpoint starts, unchanged and in order.
fn check_input(engine: &dyn ConnectorTestEngine, compression: Compression) {
    let rows = rows();
    let file = NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(&file, rows.rows().schema(), Some(properties)).unwrap();
    writer.write(rows.rows()).unwrap();
    writer.close().unwrap();

    let pipeline = input_pipeline(
        engine,
        &relation(),
        &InputOptions::default(),
        file_input(file.path(), Some(5)),
    )
    .unwrap();
    sleep(Duration::from_millis(10));
    assert!(pipeline.parser.state().data.is_empty());
    assert!(!pipeline.consumer.state().eoi);
    pipeline.reader.extend();
    let updates = wait_for_rows(pipeline.collection.as_ref(), rows.len(), || {
        pipeline.reader.queue(false)
    });
    assert_eq!(updates, rows);
}

/// An uncompressed file reads back unchanged.
#[engine_test]
pub fn input_uncompressed(engine: &dyn ConnectorTestEngine) {
    check_input(engine, Compression::UNCOMPRESSED);
}

/// A file compressed with Snappy reads back unchanged.
#[engine_test]
pub fn input_snappy(engine: &dyn ConnectorTestEngine) {
    check_input(engine, Compression::SNAPPY);
}

/// The Arrow schema of the files that the encoder writes for [`relation`].
fn file_schema() -> Schema {
    Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("nAmE", DataType::Utf8, true),
        ArrowField::new("b", DataType::Boolean, false),
        ArrowField::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        ArrowField::new("dt", DataType::Date32, false),
        ArrowField::new(
            "es",
            DataType::Struct(Fields::from(vec![ArrowField::new(
                "a",
                DataType::Boolean,
                false,
            )])),
            true,
        ),
        ArrowField::new_map(
            "m",
            "entries",
            ArrowField::new("key", DataType::Utf8, false),
            ArrowField::new("value", DataType::Int64, false),
            false,
            true,
        ),
        ArrowField::new("dec", DataType::Decimal128(10, 3), false),
    ])
}

/// The encoder writes one Parquet row per unit of weight, in the order of the batch's cursor,
/// in a file with the column names, types, and nullability of the relation.
#[engine_test]
pub fn output(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let rows = rows();
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let mut encoder = encoder(
        &FormatConfig {
            name: Cow::from("parquet"),
            config: json!({ "buffer_size_records": usize::MAX }),
        },
        None,
        &relation,
        Box::new(MockOutputConsumer::with_buffer(buffer.clone())),
    );
    let batch = engine
        .output_batch(&relation, rows.rows(), &[2, 1])
        .unwrap();
    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    encoder.encode(batch.clone()).unwrap();
    encoder.consumer().batch_end();

    let bytes: Vec<u8> = buffer
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_key, value, _headers)| value.clone())
        .flatten()
        .collect();
    let file = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone())).unwrap();
    assert_eq!(file.schema().fields(), file_schema().fields());
    let written = read::parquet_updates(&relation, bytes).unwrap();
    assert!(written.weights().iter().all(|weight| *weight == 1));
    assert_eq!(
        written.zset().unwrap(),
        Updates::new(rows.rows().clone(), vec![2, 1])
            .unwrap()
            .zset()
            .unwrap()
    );
    assert_eq!(
        written,
        read::encoding_order(batch.as_ref(), &relation).unwrap()
    );
}

/// An encoder whose buffer holds two records writes a batch of three as two files: one when
/// the buffer fills, and one with the rest.
#[engine_test]
pub fn output_in_buffers(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let rows = rows();
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let mut encoder = encoder(
        &FormatConfig {
            name: Cow::from("parquet"),
            config: json!({ "buffer_size_records": 2 }),
        },
        None,
        &relation,
        Box::new(MockOutputConsumer::with_buffer(buffer.clone())),
    );
    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    encoder
        .encode(
            engine
                .output_batch(&relation, rows.rows(), &[2, 1])
                .unwrap(),
        )
        .unwrap();
    encoder.consumer().batch_end();

    let files: Vec<Vec<u8>> = buffer
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_key, value, _headers)| value.clone())
        .collect();
    assert_eq!(files.len(), 2);
    let mut written = ZSet::empty(published_schema(&relation)).unwrap();
    for file in files {
        let rows = read::parquet_updates(&relation, file).unwrap();
        assert!(rows.weights().iter().all(|weight| *weight == 1));
        written.add(&rows.zset().unwrap()).unwrap();
    }
    assert_eq!(
        written,
        Updates::new(rows.rows().clone(), vec![2, 1])
            .unwrap()
            .zset()
            .unwrap()
    );
}

/// A VARIANT column stored as a Parquet variant, and one stored as JSON text, read into
/// VARIANTs; NULLs of both read as NULL. The binary value reads back as an array of bytes on an
/// engine with [`Capability::VariantBinaryAsArray`], and as base64 text on any other.
#[engine_test]
pub fn input_variant(engine: &dyn ConnectorTestEngine) {
    use chrono::NaiveDate;
    use parquet_variant::{Variant as ParquetVariant, VariantBuilderExt, VariantDecimal4};
    use parquet_variant_compute::VariantArrayBuilder;

    // The unshredded storage type of a Parquet variant.
    let variant_type = DataType::Struct(
        vec![
            ArrowField::new("metadata", DataType::Binary, false),
            ArrowField::new("value", DataType::Binary, false),
        ]
        .into(),
    );
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("v", variant_type.clone(), true),
        ArrowField::new("json_v", DataType::Utf8, true),
    ]));

    let mut builder = VariantArrayBuilder::new(2);
    {
        let mut object = builder.new_object();
        object.insert("int", 42i64);
        object.insert("str", "hello");
        object.insert("bool", true);
        object.insert("nul", ParquetVariant::Null);
        object.insert(
            "dec",
            ParquetVariant::Decimal4(VariantDecimal4::try_new(12_345i32, 2u8).unwrap()),
        );
        object.insert(
            "date",
            ParquetVariant::Date(NaiveDate::from_ymd_opt(2026, 9, 1).unwrap()),
        );
        object.insert("bin", ParquetVariant::Binary(&[1u8, 2, 3]));
        {
            let mut list = object.new_list("list");
            list.append_value(1i64);
            list.append_value("two");
            list.finish();
        }
        object.finish();
    }
    builder.append_null();
    // The builder makes `BinaryView` fields.
    let variants = arrow::compute::cast(&ArrayRef::from(builder.build()), &variant_type).unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
            variants,
            Arc::new(StringArray::from(vec![Some(r#"{"legacy": true}"#), None])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = NamedTempFile::new().unwrap();
    let mut writer = ArrowWriter::try_new(&file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let relation = variant_relation();
    let pipeline = input_pipeline(
        engine,
        &relation,
        &InputOptions::default(),
        file_input(file.path(), None),
    )
    .unwrap();
    pipeline.reader.extend();
    let updates = wait_for_rows(pipeline.collection.as_ref(), 2, || {
        pipeline.reader.queue(false);
        let errors = pipeline
            .parser
            .state()
            .parser_result
            .clone()
            .unwrap_or_default();
        assert!(errors.is_empty(), "parse errors: {errors:?}");
    });
    let bin = if engine.has(Capability::VariantBinaryAsArray) {
        "[1,2,3]"
    } else {
        r#"\"AQID\""#
    };
    let row = format!(
        r#"{{"id": 1, "v": "{{\"bin\":{bin},\"bool\":true,\"date\":\"2026-09-01\",\"dec\":123.45,\"int\":42,\"list\":[1,\"two\"],\"nul\":null,\"str\":\"hello\"}}", "json_v": "{{\"legacy\":true}}"}}"#
    );
    let expected = Updates::from_json(
        published_schema(&relation),
        &[(1, &row), (1, r#"{"id": 2, "v": null, "json_v": null}"#)],
    )
    .unwrap();
    assert_eq!(updates, expected);
}

/// `MAP[k, v]` over a nullable `k` gives a view a nullable map key.
#[test]
fn map_with_nullable_sql_key_is_valid_arrow() {
    use crate::format::parquet::{ArrowSchemaOptions, relation_to_arrow_fields};

    let fields = vec![Field::new(
        "m".into(),
        ColumnType::map(false, ColumnType::varchar(true), ColumnType::int(true)),
    )];
    #[derive(serde::Serialize)]
    struct Row {
        m: BTreeMap<Option<String>, Option<i32>>,
    }
    let encode = |schema: &ArrowSchema, key: Option<&str>| {
        let row = Row {
            m: BTreeMap::from([(key.map(str::to_string), Some(1))]),
        };
        serde_arrow::to_record_batch(schema.fields(), &vec![row]).map(|batch| batch.num_rows())
    };
    for delta_lake in [false, true] {
        let arrow = relation_to_arrow_fields(&fields, ArrowSchemaOptions::new(delta_lake));
        arrow::array::ArrayData::new_empty(arrow[0].data_type())
            .validate()
            .unwrap();
        let schema = ArrowSchema::new(arrow);
        assert_eq!(encode(&schema, Some("a")).unwrap(), 1);
        // Parquet has no NULL map key; the changelog quotes this error.
        let error = encode(&schema, None).unwrap_err().to_string();
        assert!(
            error.contains("cannot serialize null into non-nullable array"),
            "{error}"
        );
    }
}
