//! A Parquet file whose VARIANT column holds the binary variant encoding, read into the
//! static engine's record layer, which keeps the type of each value. The shared suites see a
//! VARIANT as its JSON text.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use feldera_adapterlib::test::wait_for_rows_within;
use feldera_sqllib::Variant;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::{deserialize_table_record, serialize_table_record};
use parquet::arrow::ArrowWriter;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::NamedTempFile;

use crate::test::{MockDeZSet, TypedCollection, input_pipeline_into};

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

serialize_table_record!(VariantRecord[3]{
    id["id"]: i64,
    v["v"]: Option<Variant>,
    json_v["json_v"]: Option<Variant>
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

    let relation = Relation::new(
        "test".into(),
        VariantRecord::schema(),
        false,
        BTreeMap::new(),
    );
    let zset = MockDeZSet::<VariantRecord, VariantRecord>::new();
    let pipeline = input_pipeline_into(
        Box::new(TypedCollection::new(relation.clone(), zset.clone())),
        &relation,
        config,
    )
    .unwrap();
    pipeline.reader.extend();
    wait_for_rows_within(pipeline.collection.as_ref(), 2, 10_000, || {
        pipeline.reader.queue(false);
        // Fail on the first parse error rather than spinning until the
        // timeout: a VARIANT the parser cannot read shows up here.
        let errors = pipeline
            .parser
            .state()
            .parser_result
            .clone()
            .unwrap_or_default();
        assert!(errors.is_empty(), "parse errors: {errors:?}");
    });

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
