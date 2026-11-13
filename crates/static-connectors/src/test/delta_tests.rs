//! How the static engine's compiled records serialize typed VARIANT values for the Delta
//! writer. The shared suites see a VARIANT as its JSON text, which loses the types.

use std::collections::BTreeMap;

use arrow::datatypes::Schema as ArrowSchema;
use dbsp_connectors::format::parquet::relation_to_arrow_fields;
use dbsp_connectors::integrated::delta_table::{
    delta_input_serde_config, delta_output_serde_config,
};
use feldera_sqllib::Variant;
use feldera_types::serde_with_context::DeserializeWithContext;
use feldera_types::serde_with_context::serialize::SerializeWithContextWrapper;
use proptest::prelude::{Arbitrary, Strategy};
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

use crate::test::DeltaTestStruct;
use crate::test::suites::delta_table::{delta_schema_options, init_logging};

/// One deterministic test row (even `bigint` so `bigint % 2 = 0` filters pass).
fn delta_test_record(bigint: i64) -> DeltaTestStruct {
    let mut runner = TestRunner::default();
    let mut record = DeltaTestStruct::arbitrary()
        .new_tree(&mut runner)
        .unwrap()
        .current();
    record.bigint = bigint;
    record
}

/// A typed value survives the Delta writer's serialization.
///
/// `DeltaTestStruct::variant` is always a map of strings, and the round trips
/// feed their data through a JSON file, which turns a date into a string and
/// binary into an array before the writer ever sees it. So nothing covered a
/// date or a decimal reaching the Delta writer as itself. This drives the
/// connector's own serde configuration over the connector's own Arrow schema,
/// and reads the batch back the way the input connector does.
#[test]
fn delta_output_typed_variant_serialization_test() {
    use chrono::NaiveDate;
    use feldera_sqllib::{ByteArray, Date, SqlString, Timestamp};
    use feldera_types::transport::delta_table::DeltaVariantEncoding;

    init_logging();

    let key = |k: &str| Variant::String(SqlString::from_ref(k));
    let variant = Variant::Map(
        BTreeMap::from([
            (
                key("date"),
                Variant::Date(Date::from_date(
                    NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
                )),
            ),
            (key("dec"), Variant::SqlDecimal((12_345, 2))),
            (key("bin"), Variant::Binary(ByteArray::new(&[1, 2, 3]))),
            (key("int"), Variant::BigInt(42)),
            (
                key("ts"),
                Variant::Timestamp(Timestamp::from_microseconds(1_756_684_800_000_000)),
            ),
        ])
        .into(),
    );

    let mut record = delta_test_record(0);
    record.variant = variant.clone();

    let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
        &DeltaTestStruct::schema(),
        delta_schema_options(),
    ));
    let batch = serde_arrow::to_record_batch(
        arrow_schema.fields(),
        &SerializeWithContextWrapper::new(
            &vec![record.clone()],
            &delta_output_serde_config(DeltaVariantEncoding::Variant),
        ),
    )
    .unwrap();

    // The column is the binary encoding, not JSON text.
    assert!(
        matches!(
            batch
                .schema()
                .field_with_name("variant")
                .unwrap()
                .data_type(),
            arrow::datatypes::DataType::Struct(_)
        ),
        "variant column is {:?}",
        batch
            .schema()
            .field_with_name("variant")
            .unwrap()
            .data_type()
    );

    let deserializer = serde_arrow::Deserializer::from_record_batch(&batch).unwrap();
    let read: Vec<DeltaTestStruct> =
        Vec::deserialize_with_context(deserializer, &delta_input_serde_config()).unwrap();

    assert_eq!(read.len(), 1);
    assert_eq!(read[0].variant, variant);
}

/// The records as a batch in the Delta schema, changed column by column:
/// `change` gives the new field and column, or `None` to remove the column.
fn changed_batch(
    records: &[DeltaTestStruct],
    change: impl Fn(
        &arrow::datatypes::Field,
        &arrow::array::ArrayRef,
    ) -> Option<(arrow::datatypes::Field, arrow::array::ArrayRef)>,
) -> arrow::array::RecordBatch {
    use feldera_types::transport::delta_table::DeltaVariantEncoding;

    let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
        &DeltaTestStruct::schema(),
        delta_schema_options(),
    ));
    let batch = serde_arrow::to_record_batch(
        arrow_schema.fields(),
        &SerializeWithContextWrapper::new(
            &records.to_vec(),
            &delta_output_serde_config(DeltaVariantEncoding::Variant),
        ),
    )
    .unwrap();
    let (fields, columns): (Vec<_>, Vec<_>) = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .filter_map(|(field, column)| change(field, column))
        .unzip();
    arrow::array::RecordBatch::try_new(std::sync::Arc::new(ArrowSchema::new(fields)), columns)
        .unwrap()
}

/// Reads `batch` the way the input connector does. Also tells whether the
/// records were read by position.
fn read_like_connector(batch: arrow::array::RecordBatch) -> (Vec<DeltaTestStruct>, bool) {
    use feldera_adapterlib::utils::arrow_layout::{has_positional_fields, order_columns};
    use feldera_adapterlib::utils::arrow_timestamps::{
        check_timestamp_columns, timestamps_to_micros,
    };

    let fields = DeltaTestStruct::schema();
    check_timestamp_columns(&fields, batch.schema_ref()).unwrap();
    let batch = timestamps_to_micros(batch, &fields).unwrap();
    let (batch, _) = order_columns(batch, &fields);
    let positional = has_positional_fields(batch.schema_ref());
    let config = crate::deinput::ArrowSerdeConfig::new(delta_input_serde_config());
    let read = Vec::deserialize_with_context(
        serde_arrow::Deserializer::from_record_batch(&batch).unwrap(),
        config.for_batch(&batch).as_ref(),
    )
    .unwrap();
    (read, positional)
}

/// A record without an unused column of the table, which the compiler can
/// remove, takes its value from the column with its name.
#[test]
fn delta_record_without_a_table_column_reads_its_own_columns() {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use feldera_adapterlib::utils::arrow_layout::order_columns;
    use feldera_types::deserialize_table_record;
    use feldera_types::program_schema::{ColumnType, Field, SqlIdentifier};
    use std::sync::Arc;

    #[derive(Debug, Eq, PartialEq)]
    struct Wanted {
        wanted: i64,
    }
    deserialize_table_record!(Wanted["Wanted", 1] {
        (wanted, "wanted", false, i64, |_| None)
    });

    let field = |name: &str| Field::new(SqlIdentifier::new(name, false), ColumnType::bigint(true));
    let fields = [field("unused"), field("wanted")];
    let column = |value: i64| Arc::new(Int64Array::from(vec![value])) as ArrayRef;
    let config = crate::deinput::ArrowSerdeConfig::new(delta_input_serde_config());
    let read = |batch: RecordBatch| {
        let (batch, reason) = order_columns(batch, &fields);
        assert_eq!(reason, None);
        Vec::<Wanted>::deserialize_with_context(
            serde_arrow::Deserializer::from_record_batch(&batch).unwrap(),
            config.for_batch(&batch).as_ref(),
        )
        .unwrap()
    };

    let all = RecordBatch::try_from_iter([("unused", column(11)), ("wanted", column(22))]);
    assert_eq!(read(all.unwrap()), [Wanted { wanted: 22 }]);
    let skipped = RecordBatch::try_from_iter([("wanted", column(22))]);
    assert_eq!(read(skipped.unwrap()), [Wanted { wanted: 22 }]);
}

/// A batch with reversed columns, an extra column, and nanosecond timestamps
/// is read by position and gives back the records that were written.
#[test]
fn delta_positional_read_matches_written_records() {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field as ArrowField, TimeUnit};
    use std::sync::Arc;

    let records: Vec<DeltaTestStruct> = (0..50).map(|i| delta_test_record(i * 2)).collect();
    let batch = changed_batch(&records, |field, column| match field.data_type() {
        DataType::Timestamp(_, tz) => {
            let ns = DataType::Timestamp(TimeUnit::Nanosecond, tz.clone());
            let column = cast(column, &ns).unwrap();
            Some((field.clone().with_data_type(ns), column))
        }
        _ => Some((field.clone(), column.clone())),
    });

    let mut fields = vec![ArrowField::new("extra", DataType::Int64, false)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![7; records.len()]))];
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()).rev() {
        fields.push(field.as_ref().clone());
        columns.push(column.clone());
    }
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();

    let (read, positional) = read_like_connector(batch);
    assert!(positional);
    assert_eq!(read, records);
}

/// A batch without a nullable column is read by position, and the column is NULL.
#[test]
fn delta_absent_column_is_read_by_position_as_null() {
    let records: Vec<DeltaTestStruct> = (0..50)
        .map(|i| {
            let mut record = delta_test_record(i * 2);
            record.unused = Some("value".to_string());
            record
        })
        .collect();
    let batch = changed_batch(&records, |field, column| {
        (field.name() != "unused").then(|| (field.clone(), column.clone()))
    });

    let (read, positional) = read_like_connector(batch);
    assert!(positional);
    let expected: Vec<DeltaTestStruct> = records
        .into_iter()
        .map(|record| DeltaTestStruct {
            unused: None,
            ..record
        })
        .collect();
    assert_eq!(read, expected);
}

/// A string column in the format `2024-01-01T10:00:00Z` is read into a SQL
/// `TIMESTAMP` column, by position and by name.
#[test]
fn delta_string_column_is_read_as_timestamp() {
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::DataType;
    use chrono::DateTime;
    use std::sync::Arc;

    let records: Vec<DeltaTestStruct> = (0..50).map(|i| delta_test_record(i * 2)).collect();
    let strings: ArrayRef = Arc::new(StringArray::from_iter_values(records.iter().map(|r| {
        DateTime::from_timestamp_micros(r.timestamp_ntz.microseconds())
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S%.fZ")
            .to_string()
    })));
    let batch = changed_batch(&records, |field, column| match field.name().as_str() {
        "timestamp_ntz" => Some((
            field.clone().with_data_type(DataType::Utf8),
            strings.clone(),
        )),
        _ => Some((field.clone(), column.clone())),
    });

    let (read, positional) = read_like_connector(batch.clone());
    assert!(positional);
    assert_eq!(read, records);

    let read: Vec<DeltaTestStruct> = Vec::deserialize_with_context(
        serde_arrow::Deserializer::from_record_batch(&batch).unwrap(),
        &delta_input_serde_config(),
    )
    .unwrap();
    assert_eq!(read, records);
}

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct InnerRow {
    a: bool,
    b: Option<i64>,
}

feldera_types::deserialize_table_record!(InnerRow["InnerRow", feldera_sqllib::SqlVariant, 2] {
    (a, "a", false, bool, |_| None),
    (b, "b", false, Option<i64>, |_| Some(None))
});

#[derive(Debug, Default, PartialEq, Eq, Clone)]
struct OuterRow {
    id: i64,
    inner: Option<InnerRow>,
    list: Vec<InnerRow>,
}

feldera_types::deserialize_table_record!(OuterRow["OuterRow", feldera_sqllib::SqlVariant, 3] {
    (id, "id", false, i64, |_| None),
    (inner, "inner", false, Option<InnerRow>, |_| Some(None)),
    (list, "list", false, Vec<InnerRow>, |_| None)
});

/// A nullable nested ROW and a ROW in an ARRAY, with records as the SQL
/// compiler generates them and fields in another order, are read by position.
#[test]
fn delta_nested_rows_are_read_by_position() {
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field as ArrowField, Fields};
    use feldera_adapterlib::utils::arrow_layout::{has_positional_fields, order_columns};
    use feldera_types::program_schema::{ColumnType, Field, SqlIdentifier};
    use std::sync::Arc;

    // Stored as (b, A).
    let inner_fields = Fields::from(vec![
        ArrowField::new("b", DataType::Int64, true),
        ArrowField::new("A", DataType::Boolean, false),
    ]);
    let inner_columns = |b: Vec<Option<i64>>, a: Vec<bool>| -> Vec<ArrayRef> {
        vec![
            Arc::new(Int64Array::from(b)),
            Arc::new(BooleanArray::from(a)),
        ]
    };
    let inner = StructArray::new(
        inner_fields.clone(),
        inner_columns(vec![Some(1), None, Some(3)], vec![true, false, true]),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let items = StructArray::new(
        inner_fields.clone(),
        inner_columns(vec![Some(7), None], vec![false, true]),
        None,
    );
    let list = ListArray::new(
        Arc::new(ArrowField::new(
            "item",
            DataType::Struct(inner_fields),
            false,
        )),
        OffsetBuffer::from_lengths([2, 0, 0]),
        Arc::new(items),
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("list", Arc::new(list) as ArrayRef),
        ("inner", Arc::new(inner) as ArrayRef),
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
    ])
    .unwrap();

    let field = |name: &str, column_type| Field::new(SqlIdentifier::new(name, false), column_type);
    let row = |nullable| {
        ColumnType::structure(
            nullable,
            &[
                field("a", ColumnType::boolean(false)),
                field("b", ColumnType::bigint(true)),
            ],
        )
    };
    let fields = [
        field("id", ColumnType::bigint(false)),
        field("inner", row(true)),
        field("list", ColumnType::array(false, row(false))),
    ];
    let (batch, reason) = order_columns(batch, &fields);
    assert_eq!(reason, None);
    assert!(has_positional_fields(batch.schema_ref()));

    let read: Vec<OuterRow> = Vec::deserialize_with_context(
        serde_arrow::Deserializer::from_record_batch(&batch).unwrap(),
        &delta_input_serde_config().with_positional_fields(true),
    )
    .unwrap();
    let inner = |a, b| InnerRow { a, b };
    assert_eq!(
        read,
        vec![
            OuterRow {
                id: 1,
                inner: Some(inner(true, Some(1))),
                list: vec![inner(false, Some(7)), inner(true, None)],
            },
            OuterRow {
                id: 2,
                inner: None,
                list: vec![],
            },
            OuterRow {
                id: 3,
                inner: Some(inner(true, Some(3))),
                list: vec![],
            },
        ]
    );
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
struct DecimalRow {
    arr: Vec<Option<feldera_sqllib::SqlDecimal<10, 2>>>,
    map: BTreeMap<String, feldera_sqllib::SqlDecimal<10, 2>>,
}

feldera_types::deserialize_table_record!(DecimalRow["DecimalRow", feldera_sqllib::SqlVariant, 2] {
    (arr, "arr", false, Vec<Option<feldera_sqllib::SqlDecimal<10, 2>>>, |_| None),
    (map, "map", false, BTreeMap<String, feldera_sqllib::SqlDecimal<10, 2>>, |_| None)
});

/// A DECIMAL in an ARRAY and in a MAP is read from its significand.
#[test]
fn delta_nested_decimals_are_read_from_significands() {
    use arrow::array::{
        Array, ArrayRef, Decimal128Array, ListArray, MapArray, RecordBatch, StringArray,
        StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field as ArrowField, Fields};
    use feldera_adapterlib::utils::arrow_layout::order_columns;
    use feldera_types::program_schema::{ColumnType, Field, SqlIdentifier};
    use std::sync::Arc;

    let decimals = |values: Vec<Option<i128>>| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ) as ArrayRef
    };
    let arr = ListArray::new(
        Arc::new(ArrowField::new("item", DataType::Decimal128(10, 2), true)),
        OffsetBuffer::from_lengths([2]),
        decimals(vec![Some(12345), None]),
        None,
    );
    let entries = StructArray::new(
        Fields::from(vec![
            ArrowField::new("key", DataType::Utf8, false),
            ArrowField::new("value", DataType::Decimal128(10, 2), false),
        ]),
        vec![
            Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
            decimals(vec![Some(-5)]),
        ],
        None,
    );
    let map = MapArray::new(
        Arc::new(ArrowField::new(
            "entries",
            entries.data_type().clone(),
            false,
        )),
        OffsetBuffer::from_lengths([1]),
        entries,
        None,
        false,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("map", Arc::new(map) as ArrayRef),
        ("arr", Arc::new(arr) as ArrayRef),
    ])
    .unwrap();

    let field = |name: &str, column_type| Field::new(SqlIdentifier::new(name, false), column_type);
    let fields = [
        field(
            "arr",
            ColumnType::array(false, ColumnType::decimal(10, 2, true)),
        ),
        field(
            "map",
            ColumnType::map(
                false,
                ColumnType::varchar(false),
                ColumnType::decimal(10, 2, false),
            ),
        ),
    ];
    let (batch, reason) = order_columns(batch, &fields);
    assert_eq!(reason, None);
    // The values are significands, not `Decimal128`, at both places.
    assert!(!format!("{:?}", batch.schema()).contains("Decimal128"));

    let read: Vec<DecimalRow> = Vec::deserialize_with_context(
        serde_arrow::Deserializer::from_record_batch(&batch).unwrap(),
        &delta_input_serde_config().with_positional_fields(true),
    )
    .unwrap();
    assert_eq!(read[0].arr, vec![Some("123.45".parse().unwrap()), None]);
    assert_eq!(read[0].map.get("k"), Some(&"-0.05".parse().unwrap()));
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
struct MidRow {
    inner: InnerRow,
    n: i64,
}

feldera_types::deserialize_table_record!(MidRow["MidRow", feldera_sqllib::SqlVariant, 2] {
    (inner, "inner", false, InnerRow, |_| None),
    (n, "n", false, i64, |_| None)
});

#[derive(Debug, Default, PartialEq, Eq, Clone)]
struct DeepRow {
    mid: Option<MidRow>,
    by_key: BTreeMap<String, InnerRow>,
    by_row: BTreeMap<InnerRow, i64>,
    large: Vec<InnerRow>,
    fixed: Vec<InnerRow>,
}

feldera_types::deserialize_table_record!(DeepRow["DeepRow", feldera_sqllib::SqlVariant, 5] {
    (mid, "mid", false, Option<MidRow>, |_| Some(None)),
    (by_key, "by_key", false, BTreeMap<String, InnerRow>, |_| None),
    (by_row, "by_row", false, BTreeMap<InnerRow, i64>, |_| None),
    (large, "large", false, Vec<InnerRow>, |_| None),
    (fixed, "fixed", false, Vec<InnerRow>, |_| None)
});

/// A ROW in a ROW, a ROW as a MAP key and as a MAP value, and a ROW in a
/// large list and in a fixed-size list are read by position. The source has
/// the fields of each ROW in another order.
#[test]
fn delta_rows_in_every_container_are_read_by_position() {
    use arrow::array::{
        Array, ArrayRef, BooleanArray, FixedSizeListArray, Int64Array, LargeListArray, MapArray,
        RecordBatch, StringArray, StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field as ArrowField, Fields};
    use feldera_adapterlib::utils::arrow_layout::{has_positional_fields, order_columns};
    use feldera_types::program_schema::{ColumnType, Field, SqlIdentifier};
    use std::sync::Arc;

    // An inner ROW is stored as (b, A).
    let inner_fields = Fields::from(vec![
        ArrowField::new("b", DataType::Int64, true),
        ArrowField::new("A", DataType::Boolean, false),
    ]);
    let inner_type = DataType::Struct(inner_fields.clone());
    let inner = |b: Vec<Option<i64>>, a: Vec<bool>| {
        StructArray::new(
            inner_fields.clone(),
            vec![
                Arc::new(Int64Array::from(b)) as ArrayRef,
                Arc::new(BooleanArray::from(a)),
            ],
            None,
        )
    };
    let item = |nullable| Arc::new(ArrowField::new("item", inner_type.clone(), nullable));
    let map = |key: ArrayRef, value: ArrayRef, lengths: [usize; 2]| {
        let entries = StructArray::new(
            Fields::from(vec![
                ArrowField::new("key", key.data_type().clone(), false),
                ArrowField::new("value", value.data_type().clone(), false),
            ]),
            vec![key, value],
            None,
        );
        MapArray::new(
            Arc::new(ArrowField::new(
                "entries",
                entries.data_type().clone(),
                false,
            )),
            OffsetBuffer::from_lengths(lengths),
            entries,
            None,
            false,
        )
    };

    // Stored as (n, inner); NULL in the second row.
    let mid = StructArray::new(
        Fields::from(vec![
            ArrowField::new("n", DataType::Int64, false),
            ArrowField::new("inner", inner_type.clone(), false),
        ]),
        vec![
            Arc::new(Int64Array::from(vec![7, 0])) as ArrayRef,
            Arc::new(inner(vec![Some(1), None], vec![true, false])),
        ],
        Some(NullBuffer::from(vec![true, false])),
    );
    let by_key = map(
        Arc::new(StringArray::from(vec!["k1", "k2"])),
        Arc::new(inner(vec![Some(2), None], vec![false, true])),
        [2, 0],
    );
    let by_row = map(
        Arc::new(inner(vec![Some(3)], vec![true])),
        Arc::new(Int64Array::from(vec![30])),
        [0, 1],
    );
    let large = LargeListArray::new(
        item(false),
        OffsetBuffer::from_lengths([1, 2]),
        Arc::new(inner(vec![Some(4), Some(5), None], vec![true, false, true])),
        None,
    );
    let fixed = FixedSizeListArray::new(
        item(false),
        1,
        Arc::new(inner(vec![Some(6), Some(8)], vec![false, true])),
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("fixed", Arc::new(fixed) as ArrayRef),
        ("large", Arc::new(large) as ArrayRef),
        ("by_row", Arc::new(by_row) as ArrayRef),
        ("by_key", Arc::new(by_key) as ArrayRef),
        ("mid", Arc::new(mid) as ArrayRef),
    ])
    .unwrap();

    let field = |name: &str, column_type| Field::new(SqlIdentifier::new(name, false), column_type);
    let row = ColumnType::structure(
        false,
        &[
            field("a", ColumnType::boolean(false)),
            field("b", ColumnType::bigint(true)),
        ],
    );
    let fields = [
        field(
            "mid",
            ColumnType::structure(
                true,
                &[
                    field("inner", row.clone()),
                    field("n", ColumnType::bigint(false)),
                ],
            ),
        ),
        field(
            "by_key",
            ColumnType::map(false, ColumnType::varchar(false), row.clone()),
        ),
        field(
            "by_row",
            ColumnType::map(false, row.clone(), ColumnType::bigint(false)),
        ),
        field("large", ColumnType::array(false, row.clone())),
        field("fixed", ColumnType::array(false, row)),
    ];
    let (batch, reason) = order_columns(batch, &fields);
    assert_eq!(reason, None);
    assert!(has_positional_fields(batch.schema_ref()));

    let read: Vec<DeepRow> = Vec::deserialize_with_context(
        serde_arrow::Deserializer::from_record_batch(&batch).unwrap(),
        &delta_input_serde_config().with_positional_fields(true),
    )
    .unwrap();
    let inner = |a, b| InnerRow { a, b };
    assert_eq!(
        read,
        vec![
            DeepRow {
                mid: Some(MidRow {
                    inner: inner(true, Some(1)),
                    n: 7,
                }),
                by_key: BTreeMap::from([
                    ("k1".to_string(), inner(false, Some(2))),
                    ("k2".to_string(), inner(true, None)),
                ]),
                by_row: BTreeMap::new(),
                large: vec![inner(true, Some(4))],
                fixed: vec![inner(false, Some(6))],
            },
            DeepRow {
                mid: None,
                by_key: BTreeMap::new(),
                by_row: BTreeMap::from([(inner(true, Some(3)), 30)]),
                large: vec![inner(false, Some(5)), inner(true, None)],
                fixed: vec![inner(true, Some(8))],
            },
        ]
    );
}

/// `array` with the fields of each ROW, at any depth, in a random order.
fn shuffle_rows(
    array: &arrow::array::ArrayRef,
    rng: &mut impl rand::Rng,
) -> arrow::array::ArrayRef {
    use arrow::array::{Array, AsArray, ListArray, MapArray, StructArray};
    use arrow::datatypes::{DataType, Field as ArrowField};
    use rand::seq::SliceRandom;
    use std::sync::Arc;

    let field_of = |field: &ArrowField, column: &arrow::array::ArrayRef| {
        Arc::new(field.clone().with_data_type(column.data_type().clone()))
    };
    match array.data_type() {
        DataType::Struct(fields) => {
            let array = array.as_struct();
            let mut order: Vec<usize> = (0..fields.len()).collect();
            order.shuffle(rng);
            let columns: Vec<_> = order
                .iter()
                .map(|&i| shuffle_rows(array.column(i), rng))
                .collect();
            let fields: Vec<_> = order
                .iter()
                .zip(&columns)
                .map(|(&i, column)| field_of(&fields[i], column))
                .collect();
            Arc::new(StructArray::new(
                fields.into(),
                columns,
                array.nulls().cloned(),
            ))
        }
        DataType::List(field) => {
            let array = array.as_list::<i32>();
            let values = shuffle_rows(array.values(), rng);
            Arc::new(ListArray::new(
                field_of(field, &values),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            ))
        }
        DataType::Map(field, sorted) => {
            let array = array.as_map();
            let entries = array.entries();
            // The key and the value keep their places; only ROWs in them change.
            let columns: Vec<_> = entries
                .columns()
                .iter()
                .map(|column| shuffle_rows(column, rng))
                .collect();
            let fields: Vec<_> = entries
                .fields()
                .iter()
                .zip(&columns)
                .map(|(field, column)| field_of(field, column))
                .collect();
            let entries = StructArray::new(fields.into(), columns, entries.nulls().cloned());
            Arc::new(MapArray::new(
                field_of(
                    field,
                    &(Arc::new(entries.clone()) as arrow::array::ArrayRef),
                ),
                array.offsets().clone(),
                entries,
                array.nulls().cloned(),
                *sorted,
            ))
        }
        _ => array.clone(),
    }
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

    /// The read by position gives the same records as the read by name, for
    /// random records and a source with its columns in a random order at
    /// every depth, names in another case, an extra column, or an absent
    /// nullable column.
    ///
    /// The nested ROWs of this record have derived deserializers, which read
    /// by name in both cases. The tests above cover nested ROWs by position.
    #[test]
    fn delta_positional_read_matches_read_by_name(
        records in proptest::collection::vec(DeltaTestStruct::arbitrary(), 1..8),
        seed: u64,
        uppercase: bool,
        extra: bool,
        drop_unused: bool,
    ) {
        use arrow::array::{ArrayRef, Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field as ArrowField};
        use feldera_adapterlib::utils::arrow_timestamps::timestamps_to_micros;
        use rand::SeedableRng;
        use rand::seq::SliceRandom;
        use std::sync::Arc;

        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        let batch = changed_batch(&records, |field, column| {
            (!(drop_unused && field.name() == "unused")).then(|| (field.clone(), column.clone()))
        });
        let mut columns: Vec<(ArrowField, ArrayRef)> = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| {
                // A VARIANT is stored as a struct that is not a ROW.
                let column = if field.name() == "variant" {
                    column.clone()
                } else {
                    shuffle_rows(column, &mut rng)
                };
                let name = if uppercase {
                    field.name().to_uppercase()
                } else {
                    field.name().clone()
                };
                let field = field
                    .as_ref()
                    .clone()
                    .with_name(name)
                    .with_data_type(column.data_type().clone());
                (field, column)
            })
            .collect();
        if extra {
            columns.push((
                ArrowField::new("extra", DataType::Int64, false),
                Arc::new(Int64Array::from(vec![7; records.len()])),
            ));
        }
        columns.shuffle(&mut rng);
        let (fields, columns): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();

        let by_name: Vec<DeltaTestStruct> = Vec::deserialize_with_context(
            serde_arrow::Deserializer::from_record_batch(
                &timestamps_to_micros(batch.clone(), &DeltaTestStruct::schema()).unwrap(),
            )
            .unwrap(),
            &delta_input_serde_config(),
        )
        .unwrap();
        let (read, positional) = read_like_connector(batch);

        proptest::prop_assert!(positional);
        proptest::prop_assert_eq!(read, by_name);
    }
}
