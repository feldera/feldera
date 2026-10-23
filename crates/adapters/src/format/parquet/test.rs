use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use feldera_sqllib::Variant;
use feldera_types::deserialize_table_record;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::NamedTempFile;

use crate::test::{TestStruct2, mock_input_pipeline, wait};

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

/// A Parquet file whose VARIANT column holds the binary variant encoding reads into the
/// static engine's record layer with every value's type intact, which the published JSON
/// text that the `parquet` connector suite compares cannot show.
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
