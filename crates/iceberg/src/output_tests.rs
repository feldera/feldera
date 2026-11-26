use super::test_support::{Failure, Faults, FlakyCatalog, TestWarehouse, NAMESPACE};
use super::*;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
    Time64MicrosecondType, TimestampMicrosecondType,
};
use feldera_sqllib::{parse_json_fv_s, SqlVariant, F32, F64};
use feldera_sqllib::{ByteArray, Date, SqlDecimal, SqlString, Time, Timestamp, TimestampTz};
use feldera_types::program_schema::SqlIdentifier;
use feldera_types::serde_with_context::serialize::SerializeWithContextWrapper;
use feldera_types::serde_with_context::SerializeWithContext;
use feldera_types::transport::iceberg::IcebergCatalogType;
use feldera_types::{serialize_struct, serialize_table_record};
use iceberg::spec::{PartitionSpec, Transform};
use iceberg::NamespaceIdent;
use serde_arrow::schema::SerdeArrowSchema;
use serde_arrow::ArrayBuilder;
use std::collections::BTreeMap;
use std::sync::Mutex;

fn relation(fields: Vec<Field>) -> Relation {
    Relation::new(SqlIdentifier::from("v"), fields, false, BTreeMap::new())
}

fn field(name: &str, columntype: ColumnType) -> Field {
    Field::new(name.into(), columntype)
}

/// The rows as the pipeline engine gives them to the connector.
fn row_batch<T: SerializeWithContext<SqlSerdeConfig>>(layout: &Layout, rows: &[T]) -> RecordBatch {
    let schema = SerdeArrowSchema::try_from(layout.row_schema().fields().as_ref()).unwrap();
    let mut builder = ArrayBuilder::new(schema).unwrap();
    let config = iceberg_output_serde_config();
    for row in rows {
        builder
            .push(SerializeWithContextWrapper::new(row, &config))
            .unwrap();
    }
    builder.to_record_batch().unwrap()
}

/// Writes `rows` as inserts and returns their files, not yet in the table.
async fn write<T: SerializeWithContext<SqlSerdeConfig>>(
    sink: &IcebergSink,
    rows: &[T],
) -> PendingCommit {
    let layout = sink.begin().await.unwrap();
    let mut writer = layout.writer().await.unwrap();
    let batch = row_batch(&layout, rows);
    writer
        .write(&batch, &vec!["i"; rows.len()], 7)
        .await
        .unwrap();
    sink.prepare(&layout, writer.close().await.unwrap())
}

async fn append<T: SerializeWithContext<SqlSerdeConfig>>(sink: &IcebergSink, rows: &[T]) {
    let mut pending = write(sink, rows).await;
    sink.commit(&mut pending).await.unwrap();
}

async fn read(warehouse: &TestWarehouse, table: &str) -> RecordBatch {
    let batches = warehouse.read(table).await;
    concat_batches(&batches[0].schema(), &batches).unwrap()
}

/// The values of BIGINT column `name`, sorted.
fn bigints(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let column = batch.column_by_name(name).unwrap();
    let mut values: Vec<i64> = column.as_primitive::<Int64Type>().values().to_vec();
    values.sort();
    values
}

fn permanent<T>(result: Result<T, SinkError>) -> String {
    match result {
        Err(SinkError::Permanent(e)) => e.to_string(),
        Err(e) => panic!("expected a permanent error, got {e:?}"),
        Ok(_) => panic!("expected a permanent error"),
    }
}

#[derive(Clone)]
struct Row {
    id: i64,
    name: Option<String>,
}

serialize_table_record!(Row[2]{
    id["id"]: i64,
    name["name"]: Option<String>
});

fn rows(ids: std::ops::Range<i64>) -> Vec<Row> {
    ids.map(|id| Row {
        id,
        name: Some(format!("name{id}")),
    })
    .collect()
}

fn row_relation() -> Relation {
    relation(vec![
        field("id", ColumnType::bigint(false)),
        field("name", ColumnType::varchar(true)),
    ])
}

#[derive(Clone)]
struct Nested {
    x: i32,
    y: Option<String>,
}

serialize_struct!(Nested()[2]{
    x["x"]: i32,
    y["y"]: Option<String>
});

struct AllTypes {
    c_boolean: bool,
    c_tinyint: i8,
    c_smallint: i16,
    c_int: i32,
    c_bigint: i64,
    c_utinyint: u8,
    c_usmallint: u16,
    c_uint: u32,
    c_real: F32,
    c_double: F64,
    c_decimal: SqlDecimal<10, 3>,
    c_varchar: SqlString,
    c_date: Date,
    c_time: Time,
    c_timestamp: Timestamp,
    c_timestamptz: TimestampTz,
    c_uuid: feldera_sqllib::Uuid,
    c_varbinary: ByteArray,
    c_variant: SqlVariant,
    c_array: Vec<Option<i32>>,
    c_map: BTreeMap<String, i64>,
    c_struct: Nested,
    c_struct_array: Vec<Nested>,
    c_null: Option<i64>,
}

serialize_table_record!(AllTypes[24]{
    c_boolean["c_boolean"]: bool,
    c_tinyint["c_tinyint"]: i8,
    c_smallint["c_smallint"]: i16,
    c_int["c_int"]: i32,
    c_bigint["c_bigint"]: i64,
    c_utinyint["c_utinyint"]: u8,
    c_usmallint["c_usmallint"]: u16,
    c_uint["c_uint"]: u32,
    c_real["c_real"]: F32,
    c_double["c_double"]: F64,
    c_decimal["c_decimal"]: SqlDecimal<10, 3>,
    c_varchar["c_varchar"]: SqlString,
    c_date["c_date"]: Date,
    c_time["c_time"]: Time,
    c_timestamp["c_timestamp"]: Timestamp,
    c_timestamptz["c_timestamptz"]: TimestampTz,
    c_uuid["c_uuid"]: feldera_sqllib::Uuid,
    c_varbinary["c_varbinary"]: ByteArray,
    c_variant["c_variant"]: SqlVariant,
    c_array["c_array"]: Vec<Option<i32>>,
    c_map["c_map"]: BTreeMap<String, i64>,
    c_struct["c_struct"]: Nested,
    c_struct_array["c_struct_array"]: Vec<Nested>,
    c_null["c_null"]: Option<i64>
});

fn all_types_relation() -> Relation {
    let nested = [
        field("x", ColumnType::int(false)),
        field("y", ColumnType::varchar(true)),
    ];
    relation(vec![
        field("c_boolean", ColumnType::boolean(false)),
        field("c_tinyint", ColumnType::tinyint(false)),
        field("c_smallint", ColumnType::smallint(false)),
        field("c_int", ColumnType::int(false)),
        field("c_bigint", ColumnType::bigint(false)),
        field("c_utinyint", ColumnType::utinyint(false)),
        field("c_usmallint", ColumnType::usmallint(false)),
        field("c_uint", ColumnType::uint(false)),
        field("c_real", ColumnType::real(false)),
        field("c_double", ColumnType::double(false)),
        field("c_decimal", ColumnType::decimal(10, 3, false)),
        field("c_varchar", ColumnType::varchar(false)),
        field("c_date", ColumnType::date(false)),
        field("c_time", ColumnType::time(false)),
        field("c_timestamp", ColumnType::timestamp(false)),
        field("c_timestamptz", ColumnType::timestamp_tz(false)),
        field("c_uuid", ColumnType::uuid(false)),
        field("c_varbinary", ColumnType::varbinary(false)),
        field("c_variant", ColumnType::variant(false)),
        field("c_array", ColumnType::array(false, ColumnType::int(true))),
        field(
            "c_map",
            ColumnType::map(false, ColumnType::varchar(false), ColumnType::bigint(false)),
        ),
        field("c_struct", ColumnType::structure(false, &nested)),
        field(
            "c_struct_array",
            ColumnType::array(false, ColumnType::structure(false, &nested)),
        ),
        field("c_null", ColumnType::bigint(true)),
    ])
}

const UUID: [u8; 16] = [
    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
];

/// 2024-05-06 07:08:09.123456 UTC.
const TIMESTAMP_MICROS: i64 = 1_714_979_289_123_456;

fn all_types_row() -> AllTypes {
    let nested = Nested {
        x: 7,
        y: Some("seven".to_string()),
    };
    AllTypes {
        c_boolean: true,
        c_tinyint: -8,
        c_smallint: -16,
        c_int: -32,
        c_bigint: -64,
        c_utinyint: u8::MAX,
        c_usmallint: u16::MAX,
        c_uint: u32::MAX,
        c_real: F32::new(1.5),
        c_double: F64::new(-2.25),
        c_decimal: SqlDecimal::<10, 3>::new(-1_234_567, 3).unwrap(),
        c_varchar: SqlString::from_ref("text"),
        c_date: Date::from_days(19_849),
        // 01:02:03.000004
        c_time: Time::from_nanoseconds(3_723_000_004_000),
        c_timestamp: Timestamp::from_microseconds(TIMESTAMP_MICROS),
        c_timestamptz: TimestampTz::from(Timestamp::from_microseconds(TIMESTAMP_MICROS)),
        c_uuid: feldera_sqllib::Uuid::from_bytes(UUID),
        c_varbinary: ByteArray::from_vec(vec![0, 1, 255]),
        c_variant: parse_json_fv_s(SqlString::from_ref(r#"{"a":5}"#)),
        c_array: vec![Some(1), None, Some(3)],
        c_map: BTreeMap::from([("k".to_string(), 9)]),
        c_struct: nested.clone(),
        c_struct_array: vec![nested],
        c_null: None,
    }
}

/// A unit bug writes wrong values without an error, so this reads every value back.
#[tokio::test]
async fn every_sql_type_is_written_in_the_unit_of_its_iceberg_type() {
    let warehouse = TestWarehouse::new().await;
    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &all_types_relation(), false)
        .await
        .unwrap();
    append(&sink, &[all_types_row()]).await;

    let batch = read(&warehouse, "t").await;
    assert_eq!(batch.num_rows(), 1);
    let column = |name: &str| batch.column_by_name(name).unwrap().clone();

    assert!(column("c_boolean").as_boolean().value(0));
    assert_eq!(column("c_tinyint").as_primitive::<Int32Type>().value(0), -8);
    assert_eq!(
        column("c_smallint").as_primitive::<Int32Type>().value(0),
        -16
    );
    assert_eq!(column("c_int").as_primitive::<Int32Type>().value(0), -32);
    assert_eq!(column("c_bigint").as_primitive::<Int64Type>().value(0), -64);
    // Each unsigned type maps to the next wider signed type.
    assert_eq!(
        column("c_utinyint").as_primitive::<Int32Type>().value(0),
        255
    );
    assert_eq!(
        column("c_usmallint").as_primitive::<Int32Type>().value(0),
        65_535
    );
    assert_eq!(
        column("c_uint").as_primitive::<Int64Type>().value(0),
        4_294_967_295
    );
    assert_eq!(column("c_real").as_primitive::<Float32Type>().value(0), 1.5);
    assert_eq!(
        column("c_double").as_primitive::<Float64Type>().value(0),
        -2.25
    );
    let decimal = column("c_decimal");
    let decimal = decimal.as_primitive::<Decimal128Type>();
    assert_eq!((decimal.value(0), decimal.scale()), (-1_234_567, 3));
    assert_eq!(column("c_varchar").as_string::<i32>().value(0), "text");
    assert_eq!(
        column("c_date").as_primitive::<Date32Type>().value(0),
        19_849
    );
    assert_eq!(
        column("c_time")
            .as_primitive::<Time64MicrosecondType>()
            .value(0),
        3_723_000_004
    );
    for name in ["c_timestamp", "c_timestamptz"] {
        assert_eq!(
            column(name)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            TIMESTAMP_MICROS,
            "{name}"
        );
    }
    assert_eq!(column("c_uuid").as_fixed_size_binary().value(0), UUID);
    assert_eq!(
        column("c_varbinary").as_binary::<i64>().value(0),
        [0, 1, 255]
    );
    let variant = column("c_variant");
    let variant: serde_json::Value =
        serde_json::from_str(variant.as_string::<i32>().value(0)).unwrap();
    assert_eq!(variant, serde_json::json!({"a": 5}));

    let array = column("c_array");
    let array = array.as_list::<i32>().value(0);
    let array = array.as_primitive::<Int32Type>();
    assert_eq!(array.iter().collect::<Vec<_>>(), [Some(1), None, Some(3)]);

    let map = column("c_map");
    let map = map.as_map();
    assert_eq!(map.keys().as_string::<i32>().value(0), "k");
    assert_eq!(map.values().as_primitive::<Int64Type>().value(0), 9);

    let check_nested = |nested: &dyn Array| {
        let nested = nested.as_struct();
        let x = nested.column_by_name("x").unwrap();
        let y = nested.column_by_name("y").unwrap();
        assert_eq!(x.as_primitive::<Int32Type>().value(0), 7);
        assert_eq!(y.as_string::<i32>().value(0), "seven");
    };
    check_nested(&column("c_struct"));
    check_nested(&column("c_struct_array").as_list::<i32>().value(0));

    assert!(column("c_null").is_null(0));
    assert_eq!(column(OP_COLUMN).as_string::<i32>().value(0), "i");
    assert_eq!(column(TS_COLUMN).as_primitive::<Int64Type>().value(0), 7);
}

#[tokio::test]
async fn the_created_table_has_the_columns_of_the_view() {
    let warehouse = TestWarehouse::new().await;
    warehouse
        .sink("t", IcebergWriteMode::Append, &all_types_relation(), false)
        .await
        .unwrap();

    let table = warehouse.table("t").await;
    let schema = table.metadata().current_schema();
    let column = |name: &str| schema.field_by_name(name).unwrap();
    let primitive = |name: &str| column(name).field_type.as_primitive_type().unwrap().clone();

    assert_eq!(primitive("c_tinyint"), PrimitiveType::Int);
    assert_eq!(primitive("c_time"), PrimitiveType::Time);
    assert_eq!(primitive("c_timestamp"), PrimitiveType::Timestamp);
    assert_eq!(primitive("c_timestamptz"), PrimitiveType::Timestamptz);
    assert_eq!(primitive("c_uuid"), PrimitiveType::Uuid);
    assert_eq!(primitive("c_variant"), PrimitiveType::String);
    assert_eq!(
        primitive("c_decimal"),
        PrimitiveType::Decimal {
            precision: 10,
            scale: 3
        }
    );
    // NOT NULL maps to `required`, nested fields included.
    assert!(column("c_bigint").required);
    assert!(!column("c_null").required);
    assert!(column("c_struct.x").required);
    assert!(!column("c_struct.y").required);
    assert!(!column("c_array.element").required);
    // The change log columns.
    assert_eq!(primitive(OP_COLUMN), PrimitiveType::String);
    assert_eq!(primitive(TS_COLUMN), PrimitiveType::Long);
    assert!(table.metadata().default_partition_spec().is_unpartitioned());
}

#[tokio::test]
async fn a_type_with_no_iceberg_type_is_refused() {
    let warehouse = TestWarehouse::new().await;
    let open = |columntype: ColumnType| {
        let relation = relation(vec![field("c", columntype)]);
        let warehouse = &warehouse;
        async move {
            permanent(
                warehouse
                    .sink("t", IcebergWriteMode::Append, &relation, false)
                    .await,
            )
        }
    };
    let error = open(ColumnType::ubigint(false)).await;
    assert!(
        error.contains("column 'c'") && error.contains("DECIMAL(20, 0)"),
        "{error}"
    );
    let mut interval = ColumnType::bigint(false);
    interval.typ = SqlType::Interval(feldera_types::program_schema::IntervalUnit::Day);
    let error = open(interval).await;
    assert!(error.contains("no Iceberg type"), "{error}");
    // Nested, to check that the error names the path.
    let error = open(ColumnType::array(false, ColumnType::ubigint(false))).await;
    assert!(error.contains("column 'c.element'"), "{error}");

    let relation = relation(vec![field(OP_COLUMN, ColumnType::varchar(true))]);
    let error = permanent(
        warehouse
            .sink("t", IcebergWriteMode::Append, &relation, false)
            .await,
    );
    assert!(error.contains(OP_COLUMN), "{error}");
    // Nothing was created.
    assert!(!warehouse
        .catalog
        .table_exists(&TableIdent::from_strs([NAMESPACE, "t"]).unwrap())
        .await
        .unwrap());
}

#[tokio::test]
async fn each_batch_is_one_snapshot_that_names_its_commit() {
    let warehouse = TestWarehouse::new().await;
    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();
    assert!(warehouse.read("t").await.is_empty());

    append(&sink, &rows(0..3)).await;
    append(&sink, &rows(3..5)).await;

    assert_eq!(bigints(&read(&warehouse, "t").await, "id"), [0, 1, 2, 3, 4]);
    let table = warehouse.table("t").await;
    let ids: Vec<_> = table
        .metadata()
        .snapshots()
        .map(|s| s.summary().additional_properties[COMMIT_ID_PROPERTY].clone())
        .collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
}

/// Sink of table `t` whose catalog injects `failures` into the next commits.
async fn flaky_sink(warehouse: &TestWarehouse, failures: &[Failure]) -> IcebergSink {
    let faults = Faults {
        commits: failures.iter().copied().collect(),
        ..Faults::default()
    };
    IcebergSink::open_with_catalog(
        FlakyCatalog::new(&warehouse.catalog, faults),
        &format!("{NAMESPACE}.t"),
        IcebergWriteMode::Append,
        &row_relation(),
        false,
        "create-1",
    )
    .await
    .unwrap()
}

fn transient(result: Result<(), SinkError>) {
    assert!(
        matches!(result, Err(SinkError::Transient(_))),
        "expected a transient error, got {result:?}"
    );
}

#[tokio::test]
async fn a_commit_that_reached_the_table_is_not_repeated() {
    let warehouse = TestWarehouse::new().await;
    let sink = flaky_sink(&warehouse, &[Failure::ResponseLost]).await;

    let mut pending = write(&sink, &rows(0..3)).await;
    transient(sink.commit(&mut pending).await);
    sink.commit(&mut pending).await.unwrap();

    assert_eq!(bigints(&read(&warehouse, "t").await, "id"), [0, 1, 2]);
    assert_eq!(warehouse.snapshots("t").await, 1);
}

#[tokio::test]
async fn a_commit_that_did_not_reach_the_table_is_repeated() {
    let warehouse = TestWarehouse::new().await;
    let sink = flaky_sink(&warehouse, &[Failure::Lost, Failure::Lost]).await;

    let mut pending = write(&sink, &rows(0..3)).await;
    transient(sink.commit(&mut pending).await);
    transient(sink.commit(&mut pending).await);
    sink.commit(&mut pending).await.unwrap();

    assert_eq!(bigints(&read(&warehouse, "t").await, "id"), [0, 1, 2]);
    assert_eq!(warehouse.snapshots("t").await, 1);
}

/// The first attempt lands after the retry's load. Only a check tied to the commit sees it.
#[tokio::test]
async fn a_first_attempt_that_reaches_the_table_late_is_not_repeated() {
    let warehouse = TestWarehouse::new().await;
    let sink = flaky_sink(&warehouse, &[Failure::Late]).await;

    let mut pending = write(&sink, &rows(0..3)).await;
    transient(sink.commit(&mut pending).await);
    // The table moved, so the catalog rejects the retry. The next attempt finds the snapshot.
    let mut attempts = 0;
    while sink.commit(&mut pending).await.is_err() {
        attempts += 1;
        assert!(attempts < 3);
    }

    assert_eq!(bigints(&read(&warehouse, "t").await, "id"), [0, 1, 2]);
    assert_eq!(warehouse.snapshots("t").await, 1);
}

/// Another writer commits after the first attempt, so its snapshot is no longer current.
#[tokio::test]
async fn a_retry_finds_its_snapshot_behind_the_snapshot_of_another_writer() {
    let warehouse = TestWarehouse::new().await;
    let sink = flaky_sink(&warehouse, &[Failure::ResponseLost]).await;
    let other = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();
    append(&other, &rows(100..101)).await;

    let mut pending = write(&sink, &rows(0..3)).await;
    transient(sink.commit(&mut pending).await);
    append(&other, &rows(101..102)).await;
    sink.commit(&mut pending).await.unwrap();

    assert_eq!(
        bigints(&read(&warehouse, "t").await, "id"),
        [0, 1, 2, 100, 101]
    );
    assert_eq!(warehouse.snapshots("t").await, 3);
}

/// A batch written before another writer's commit is appended on top of it.
#[tokio::test]
async fn a_commit_keeps_the_rows_of_a_concurrent_writer() {
    let warehouse = TestWarehouse::new().await;
    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();
    let other = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();

    let mut pending = write(&sink, &rows(0..3)).await;
    append(&other, &rows(100..101)).await;
    sink.commit(&mut pending).await.unwrap();

    assert_eq!(bigints(&read(&warehouse, "t").await, "id"), [0, 1, 2, 100]);
}

/// Creates table `t` with `fields` and, if given, an identity partition on a column.
async fn create_table(
    warehouse: &TestWarehouse,
    fields: Vec<NestedField>,
    partition: Option<&str>,
) {
    let schema = Schema::builder()
        .with_fields(fields.into_iter().map(Arc::new))
        .build()
        .unwrap();
    let spec = partition.map(|column| {
        PartitionSpec::builder(schema.clone())
            .add_partition_field(column, column, Transform::Identity)
            .unwrap()
            .build()
            .unwrap()
    });
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .partition_spec_opt(spec.map(Into::into))
        .build();
    warehouse
        .catalog
        .create_table(&NamespaceIdent::new(NAMESPACE.to_string()), creation)
        .await
        .unwrap();
}

fn change_log_fields(first_id: i32) -> Vec<NestedField> {
    vec![
        NestedField::optional(first_id, OP_COLUMN, Type::Primitive(PrimitiveType::String)),
        NestedField::optional(
            first_id + 1,
            TS_COLUMN,
            Type::Primitive(PrimitiveType::Long),
        ),
    ]
}

fn long(id: i32, name: &str, required: bool) -> NestedField {
    NestedField::new(id, name, Type::Primitive(PrimitiveType::Long), required)
}

fn string(id: i32, name: &str, required: bool) -> NestedField {
    NestedField::new(id, name, Type::Primitive(PrimitiveType::String), required)
}

/// Another column order, an extra optional column, and required change log columns.
#[tokio::test]
async fn an_existing_table_takes_each_column_of_the_view_by_name() {
    let warehouse = TestWarehouse::new().await;
    let mut fields = vec![string(1, OP_COLUMN, true), long(2, TS_COLUMN, true)];
    fields.extend([
        string(3, "name", false),
        string(4, "extra", false),
        long(5, "id", true),
    ]);
    create_table(&warehouse, fields, None).await;

    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();
    append(&sink, &rows(0..2)).await;

    let batch = read(&warehouse, "t").await;
    assert_eq!(bigints(&batch, "id"), [0, 1]);
    let names = batch.column_by_name("name").unwrap().as_string::<i32>();
    let mut names: Vec<_> = names.iter().map(Option::unwrap).collect();
    names.sort();
    assert_eq!(names, ["name0", "name1"]);
    assert_eq!(batch.column_by_name("extra").unwrap().null_count(), 2);
}

#[tokio::test]
async fn a_table_that_does_not_fit_the_view_is_refused() {
    // Table columns besides the change log columns, and the expected error.
    let cases: Vec<(Vec<NestedField>, &str)> = vec![
        (vec![long(3, "id", true)], "no column 'name'"),
        (
            vec![long(3, "id", true), long(4, "name", false)],
            "'name' has type long, but the view column needs type string",
        ),
        (
            vec![long(3, "id", true), string(4, "name", true)],
            "'name' is required, but the view column can be NULL",
        ),
        (
            vec![
                long(3, "id", true),
                string(4, "name", false),
                long(5, "extra", true),
            ],
            "'extra' is required, but the view has no such column",
        ),
    ];
    for (columns, expected) in cases {
        let warehouse = TestWarehouse::new().await;
        let mut fields = change_log_fields(1);
        fields.extend(columns);
        create_table(&warehouse, fields, None).await;
        let error = permanent(
            warehouse
                .sink("t", IcebergWriteMode::Append, &row_relation(), false)
                .await,
        );
        assert!(error.contains(expected), "{error}");
    }

    // TIMESTAMP has no zone, so it cannot go into `timestamptz`.
    let warehouse = TestWarehouse::new().await;
    let mut fields = change_log_fields(1);
    fields.push(NestedField::optional(
        3,
        "ts",
        Type::Primitive(PrimitiveType::Timestamptz),
    ));
    create_table(&warehouse, fields, None).await;
    let relation = relation(vec![field("ts", ColumnType::timestamp(false))]);
    let error = permanent(
        warehouse
            .sink("t", IcebergWriteMode::Append, &relation, false)
            .await,
    );
    assert!(
        error.contains("timestamptz") && error.contains("timestamp"),
        "{error}"
    );

    // A table without the change log columns is not a change log.
    let warehouse = TestWarehouse::new().await;
    create_table(
        &warehouse,
        vec![long(1, "id", true), string(2, "name", false)],
        None,
    )
    .await;
    let error = permanent(
        warehouse
            .sink("t", IcebergWriteMode::Append, &row_relation(), false)
            .await,
    );
    assert!(error.contains(OP_COLUMN), "{error}");
}

#[tokio::test]
async fn a_partitioned_table_is_refused() {
    let warehouse = TestWarehouse::new().await;
    let mut fields = change_log_fields(1);
    fields.extend([long(3, "id", true), string(4, "name", false)]);
    create_table(&warehouse, fields, Some("name")).await;

    let error = permanent(
        warehouse
            .sink("t", IcebergWriteMode::Append, &row_relation(), false)
            .await,
    );
    assert!(error.contains("partitioned"), "{error}");
}

#[tokio::test]
async fn error_if_exists_refuses_a_table_that_a_new_pipeline_finds() {
    let warehouse = TestWarehouse::new().await;
    let relation = row_relation();
    let open = |mode, resume| warehouse.sink("t", mode, &relation, resume);

    // Creates the table, and reopens it after a restart from a checkpoint.
    open(IcebergWriteMode::ErrorIfExists, false).await.unwrap();
    open(IcebergWriteMode::ErrorIfExists, true).await.unwrap();
    open(IcebergWriteMode::Append, false).await.unwrap();
    open(IcebergWriteMode::Append, true).await.unwrap();

    let error = permanent(open(IcebergWriteMode::ErrorIfExists, false).await);
    assert!(error.contains("error_if_exists"), "{error}");
}

#[tokio::test]
async fn a_namespace_that_does_not_exist_is_a_permanent_error() {
    let warehouse = TestWarehouse::new().await;
    // Some catalogs report it on the table lookup, others on create.
    for lookup_finds_nothing in [false, true] {
        let faults = Faults {
            lookup_finds_nothing,
            ..Faults::default()
        };
        let error = permanent(
            IcebergSink::open_with_catalog(
                FlakyCatalog::new(&warehouse.catalog, faults),
                "nowhere.t",
                IcebergWriteMode::Append,
                &row_relation(),
                false,
                "create-1",
            )
            .await,
        );
        assert!(error.contains("namespace 'nowhere'"), "{error}");
    }
}

/// The table gets a column while the connector runs. The next batch writes NULL to it.
#[tokio::test]
async fn each_batch_is_written_for_the_table_as_it_is_then() {
    let warehouse = TestWarehouse::new().await;
    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();
    append(&sink, &rows(0..1)).await;

    warehouse
        .add_column("t", "added", PrimitiveType::Long)
        .await;

    append(&sink, &rows(1..2)).await;

    let batch = read(&warehouse, "t").await;
    assert_eq!(bigints(&batch, "id"), [0, 1]);
    assert_eq!(batch.column_by_name("added").unwrap().null_count(), 2);
}

/// The schema changes between write and commit, so the batch is written again.
#[tokio::test]
async fn a_batch_written_for_an_older_schema_is_not_committed() {
    let warehouse = TestWarehouse::new().await;
    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();

    let mut pending = write(&sink, &rows(0..2)).await;
    warehouse
        .add_column("t", "added", PrimitiveType::Long)
        .await;
    let result = sink.commit(&mut pending).await;
    assert!(
        matches!(result, Err(SinkError::TableChanged(_))),
        "{result:?}"
    );
    assert!(warehouse.read("t").await.is_empty());

    append(&sink, &rows(0..2)).await;
    let batch = read(&warehouse, "t").await;
    assert_eq!(bigints(&batch, "id"), [0, 1]);
    assert_eq!(batch.column_by_name("added").unwrap().null_count(), 2);
}

/// The REST client never refreshes its token, so a failed or old client is replaced.
#[tokio::test]
async fn the_catalog_client_is_replaced_when_it_fails_or_is_old() {
    let warehouse = TestWarehouse::new().await;
    let connects = Arc::new(Mutex::new(0));
    let catalog = FlakyCatalog::new(&warehouse.catalog, Faults::default());
    let connect: Connect = {
        let (connects, catalog) = (connects.clone(), catalog.clone());
        Box::new(move || {
            *connects.lock().unwrap() += 1;
            let catalog: Arc<dyn Catalog> = catalog.clone();
            Box::pin(async move { Ok(catalog) })
        })
    };
    let sink = IcebergSink::open_with(
        connect,
        None,
        &format!("{NAMESPACE}.t"),
        IcebergWriteMode::Append,
        &row_relation(),
        false,
        "create-1",
    )
    .await
    .unwrap();
    let connects = || *connects.lock().unwrap();
    assert_eq!(connects(), 1);

    sink.begin().await.unwrap();
    assert_eq!(connects(), 1);

    catalog.faults.lock().unwrap().loads = 1;
    assert!(matches!(sink.begin().await, Err(SinkError::Transient(_))));
    sink.begin().await.unwrap();
    assert_eq!(connects(), 2);

    tokio::time::pause();
    tokio::time::advance(CATALOG_MAX_AGE).await;
    tokio::time::resume();
    sink.begin().await.unwrap();
    assert_eq!(connects(), 3);
}

/// Not every catalog returns `TableAlreadyExists` when a create finds the table.
#[tokio::test]
async fn a_create_whose_response_is_lost_still_opens_the_table() {
    let warehouse = TestWarehouse::new().await;
    let faults = Faults {
        create_response_lost: true,
        ..Faults::default()
    };
    let sink = IcebergSink::open_with_catalog(
        FlakyCatalog::new(&warehouse.catalog, faults),
        &format!("{NAMESPACE}.t"),
        IcebergWriteMode::Append,
        &row_relation(),
        false,
        "create-1",
    )
    .await
    .unwrap();
    append(&sink, &rows(0..1)).await;
    assert_eq!(bigints(&read(&warehouse, "t").await, "id"), [0]);
}

/// Table properties set the codec, row group size, and file size. Row groups cap at 16 MiB.
#[tokio::test]
async fn data_files_follow_the_write_properties_of_the_table() {
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

    let warehouse = TestWarehouse::new().await;
    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();
    warehouse
        .set_properties(
            "t",
            &[
                ("write.parquet.compression-codec", "snappy"),
                ("write.parquet.row-group-size-bytes", "20000"),
                ("write.target-file-size-bytes", "100000"),
            ],
        )
        .await;

    let mut pending = write(&sink, &rows(0..50_000)).await;

    // The small target file size splits the batch into several files.
    assert!(pending.files.len() > 1, "{} files", pending.files.len());
    let mut row_groups = 0;
    for file in &pending.files {
        let reader = SerializedFileReader::new(std::fs::File::open(file.file_path()).unwrap());
        let metadata = reader.unwrap().metadata().clone();
        for row_group in metadata.row_groups() {
            row_groups += 1;
            assert_eq!(row_group.column(0).compression(), Compression::SNAPPY);
            // The writer checks sizes once per 1024-row write call.
            assert!(row_group.num_rows() <= 3 * WRITE_BATCH_ROWS as i64);
        }
    }
    assert!(row_groups > pending.files.len(), "{row_groups} row groups");

    sink.commit(&mut pending).await.unwrap();
    assert_eq!(bigints(&read(&warehouse, "t").await, "id").len(), 50_000);
}

/// Without the table property, the 16 MiB cap applies, not the 128 MiB format default.
#[tokio::test]
async fn a_row_group_is_bounded_without_a_table_property() {
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

    #[derive(Clone)]
    struct Wide {
        id: i64,
        name: Option<String>,
    }
    serialize_table_record!(Wide[2]{ id["id"]: i64, name["name"]: Option<String> });

    let warehouse = TestWarehouse::new().await;
    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();
    // Incompressible text: 40 MiB in 2,000 rows.
    let mut state = 0x9e3779b97f4a7c15u64;
    let mut text = |len: usize| -> String {
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (b'!' + (state % 90) as u8) as char
            })
            .collect()
    };
    let rows: Vec<Wide> = (0..2_000)
        .map(|id| Wide {
            id,
            name: Some(text(20_000)),
        })
        .collect();

    let pending = write(&sink, &rows).await;

    assert_eq!(pending.files.len(), 1);
    let reader =
        SerializedFileReader::new(std::fs::File::open(pending.files[0].file_path()).unwrap());
    let metadata = reader.unwrap().metadata().clone();
    assert!(
        metadata.num_row_groups() >= 2,
        "{}",
        metadata.num_row_groups()
    );
    for row_group in metadata.row_groups() {
        // One 1024-row slice of 20 kB rows can overshoot the cap before the writer checks.
        let limit = (MAX_ROW_GROUP_BYTES + 2 * WRITE_BATCH_ROWS * 20_000) as i64;
        assert!(
            row_group.compressed_size() <= limit,
            "{}",
            row_group.compressed_size()
        );
    }
}

/// The table is replaced between write and commit, so the batch is written again.
#[tokio::test]
async fn a_table_that_replaced_another_does_not_get_its_files() {
    let warehouse = TestWarehouse::new().await;
    let relation = row_relation();
    let open = || warehouse.sink("t", IcebergWriteMode::Append, &relation, false);
    let sink = open().await.unwrap();

    let mut pending = write(&sink, &rows(0..2)).await;
    let ident = TableIdent::from_strs([NAMESPACE, "t"]).unwrap();
    warehouse.catalog.drop_table(&ident).await.unwrap();
    open().await.unwrap();

    let result = sink.commit(&mut pending).await;
    assert!(
        matches!(result, Err(SinkError::TableChanged(_))),
        "{result:?}"
    );
    assert!(warehouse.read("t").await.is_empty());
}

/// A rewritten batch keeps its commit id, so a late commit of its first files settles it.
#[tokio::test]
async fn a_batch_written_again_is_settled_by_a_commit_of_its_first_files() {
    let warehouse = TestWarehouse::new().await;
    let sink = flaky_sink(&warehouse, &[Failure::ResponseLost]).await;

    let mut pending = write(&sink, &rows(0..2)).await;
    transient(sink.commit(&mut pending).await);
    // The connector writes the batch again, as it does when the table changed.
    let layout = sink.begin().await.unwrap();
    let mut writer = layout.writer().await.unwrap();
    let batch = row_batch(&layout, &rows(0..2));
    writer.write(&batch, &["i", "i"], 7).await.unwrap();
    let mut pending = pending.rewritten(&layout, writer.close().await.unwrap());
    sink.commit(&mut pending).await.unwrap();

    assert_eq!(bigints(&read(&warehouse, "t").await, "id"), [0, 1]);
    assert_eq!(warehouse.snapshots("t").await, 1);
}

/// The Glue client cannot create or commit to a table with a `timestamptz` column.
#[tokio::test]
async fn the_glue_catalog_refuses_a_table_with_a_timestamptz_column() {
    let warehouse = TestWarehouse::new().await;
    let open = |relation: Relation| {
        let catalog = warehouse.catalog.clone();
        async move {
            let connect: Connect = Box::new(move || {
                let catalog = catalog.clone();
                Box::pin(async move { Ok(catalog) })
            });
            IcebergSink::open_with(
                connect,
                Some(IcebergCatalogType::Glue),
                &format!("{NAMESPACE}.t"),
                IcebergWriteMode::Append,
                &relation,
                false,
                "create-1",
            )
            .await
        }
    };

    // The connector does not create such a table.
    let with_zone = relation(vec![field(
        "a",
        ColumnType::array(false, ColumnType::timestamp_tz(false)),
    )]);
    let error = permanent(open(with_zone).await);
    assert!(
        error.contains("'a.element'") && error.contains("Glue"),
        "{error}"
    );
    assert!(!warehouse
        .catalog
        .table_exists(&TableIdent::from_strs([NAMESPACE, "t"]).unwrap())
        .await
        .unwrap());

    // Nor write to an existing one, even when the view lacks that column.
    let mut fields = change_log_fields(1);
    fields.extend([
        long(3, "id", true),
        string(4, "name", false),
        NestedField::optional(5, "at", Type::Primitive(PrimitiveType::Timestamptz)),
    ]);
    create_table(&warehouse, fields, None).await;
    let error = permanent(open(row_relation()).await);
    assert!(error.contains("'at'") && error.contains("Glue"), "{error}");
}

/// A load fails right after the create. The next open still accepts the table.
#[tokio::test]
async fn error_if_exists_opens_the_table_that_it_just_created() {
    let warehouse = TestWarehouse::new().await;
    let faults = Faults {
        loads: 1,
        ..Faults::default()
    };
    IcebergSink::open_with_catalog(
        FlakyCatalog::new(&warehouse.catalog, faults),
        &format!("{NAMESPACE}.t"),
        IcebergWriteMode::ErrorIfExists,
        &row_relation(),
        false,
        "create-1",
    )
    .await
    .unwrap();
}

/// Opens table `t` with `error_if_exists` through a catalog with `faults`.
async fn open_error_if_exists(
    warehouse: &TestWarehouse,
    faults: Faults,
    create_id: &str,
) -> Result<IcebergSink, SinkError> {
    IcebergSink::open_with_catalog(
        FlakyCatalog::new(&warehouse.catalog, faults),
        &format!("{NAMESPACE}.t"),
        IcebergWriteMode::ErrorIfExists,
        &row_relation(),
        false,
        create_id,
    )
    .await
}

/// The create lands and its response is lost. `error_if_exists` still opens the table.
#[tokio::test]
async fn error_if_exists_opens_its_table_when_the_create_response_is_lost() {
    let warehouse = TestWarehouse::new().await;
    let faults = Faults {
        create_response_lost: true,
        ..Faults::default()
    };
    open_error_if_exists(&warehouse, faults, "create-1")
        .await
        .unwrap();
}

/// The next attempt of the same open accepts its own table. Another open refuses it.
#[tokio::test]
async fn error_if_exists_opens_its_table_in_the_next_attempt() {
    let warehouse = TestWarehouse::new().await;
    let faults = Faults {
        create_response_lost: true,
        lookup_finds_nothing: true,
        ..Faults::default()
    };
    let first = open_error_if_exists(&warehouse, faults, "create-1").await;
    assert!(
        matches!(first, Err(SinkError::Transient(_))),
        "{:?}",
        first.err()
    );

    open_error_if_exists(&warehouse, Faults::default(), "create-1")
        .await
        .unwrap();
    let other = permanent(open_error_if_exists(&warehouse, Faults::default(), "create-2").await);
    assert!(other.contains("error_if_exists"), "{other}");
}

/// A table column that the view lacks gets its write default, also a required one.
#[tokio::test]
async fn a_column_that_the_view_lacks_gets_its_write_default() {
    let warehouse = TestWarehouse::new().await;
    let decimal = Type::Primitive(PrimitiveType::Decimal {
        precision: 10,
        scale: 2,
    });
    let mut fields = change_log_fields(1);
    fields.extend([
        long(3, "id", true),
        string(4, "name", false),
        long(5, "count", false).with_write_default(Literal::long(42)),
        NestedField::required(6, "price", decimal).with_write_default(Literal::decimal(1234)),
        string(7, "tag", true).with_write_default(Literal::string("none")),
        NestedField::optional(8, "day", Type::Primitive(PrimitiveType::Date))
            .with_write_default(Literal::date(19000)),
    ]);
    let schema = Schema::builder()
        .with_fields(fields.into_iter().map(Arc::new))
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .format_version(iceberg::spec::FormatVersion::V3)
        .build();
    warehouse
        .catalog
        .create_table(&NamespaceIdent::new(NAMESPACE.to_string()), creation)
        .await
        .unwrap();

    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &row_relation(), false)
        .await
        .unwrap();
    append(&sink, &rows(0..2)).await;

    let batch = read(&warehouse, "t").await;
    let column = |name: &str| batch.column_by_name(name).unwrap();
    assert_eq!(bigints(&batch, "count"), [42, 42]);
    let price = column("price").as_primitive::<Decimal128Type>();
    assert_eq!(
        (price.values().as_ref(), price.scale()),
        ([1234, 1234].as_ref(), 2)
    );
    let tag = column("tag").as_string::<i32>();
    assert_eq!((tag.value(0), tag.value(1)), ("none", "none"));
    assert_eq!(
        column("day").as_primitive::<Date32Type>().values(),
        &[19000, 19000]
    );
}

/// Glue converts every schema on commit, so a dropped `timestamptz` column still fails it.
#[tokio::test]
async fn the_glue_catalog_refuses_a_table_that_once_had_a_timestamptz_column() {
    let warehouse = TestWarehouse::new().await;
    let mut fields = change_log_fields(1);
    fields.extend([
        long(3, "id", true),
        string(4, "name", false),
        NestedField::optional(5, "at", Type::Primitive(PrimitiveType::Timestamptz)),
    ]);
    create_table(&warehouse, fields, None).await;
    let table = warehouse.table("t").await;
    let transaction = Transaction::new(&table);
    transaction
        .update_schema()
        .delete_column("at")
        .apply(transaction)
        .unwrap()
        .commit(warehouse.catalog.as_ref())
        .await
        .unwrap();

    let catalog = warehouse.catalog.clone();
    let connect: Connect = Box::new(move || {
        let catalog = catalog.clone();
        Box::pin(async move { Ok(catalog) })
    });
    let error = permanent(
        IcebergSink::open_with(
            connect,
            Some(IcebergCatalogType::Glue),
            &format!("{NAMESPACE}.t"),
            IcebergWriteMode::Append,
            &row_relation(),
            false,
            "create-1",
        )
        .await,
    );
    assert!(
        error.contains("'at'") && error.contains("earlier schema"),
        "{error}"
    );
}

#[derive(Clone)]
struct WithNested {
    id: i64,
    s: Nested,
}

serialize_table_record!(WithNested[2]{
    id["id"]: i64,
    s["s"]: Nested
});

/// An optional struct field that the view lacks is NULL. A required one is refused.
#[tokio::test]
async fn an_optional_struct_field_that_the_view_lacks_is_null() {
    let warehouse = TestWarehouse::new().await;
    let table_struct = |z: NestedField| {
        Type::Struct(StructType::new(vec![
            Arc::new(NestedField::required(
                11,
                "x",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                12,
                "y",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(z),
        ]))
    };
    let mut fields = change_log_fields(1);
    fields.extend([
        long(3, "id", true),
        NestedField::required(4, "s", table_struct(long(13, "z", false))),
    ]);
    create_table(&warehouse, fields, None).await;
    let view_struct = [
        field("x", ColumnType::int(false)),
        field("y", ColumnType::varchar(true)),
    ];
    let relation = relation(vec![
        field("id", ColumnType::bigint(false)),
        field("s", ColumnType::structure(false, &view_struct)),
    ]);

    let sink = warehouse
        .sink("t", IcebergWriteMode::Append, &relation, false)
        .await
        .unwrap();
    let rows: Vec<_> = (0..2)
        .map(|id| WithNested {
            id,
            s: Nested {
                x: id as i32 + 10,
                y: None,
            },
        })
        .collect();
    append(&sink, &rows).await;

    let batch = read(&warehouse, "t").await;
    let s = batch.column_by_name("s").unwrap().as_struct();
    assert_eq!(s.column_by_name("z").unwrap().null_count(), 2);
    let x = s.column_by_name("x").unwrap().as_primitive::<Int32Type>();
    assert_eq!(x.values(), &[10, 11]);

    let view_type = iceberg_type(&relation.fields[1].columntype, "s", &mut 10).unwrap();
    let error = check_type("s", &view_type, &table_struct(long(13, "z", true)))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("'z'") && error.contains("required"),
        "{error}"
    );
}

/// The catalog never answers a commit. The request times out, and the retry commits once.
#[tokio::test]
async fn a_commit_that_the_catalog_never_answers_times_out() {
    let warehouse = TestWarehouse::new().await;
    let sink = flaky_sink(&warehouse, &[Failure::Hang]).await;
    let mut pending = write(&sink, &rows(0..3)).await;

    // A paused clock jumps to the next timer when every task waits.
    tokio::time::pause();
    let attempt = timeout(2 * CATALOG_TIMEOUT, sink.commit(&mut pending)).await;
    transient(attempt.expect("the commit request has no timeout"));
    sink.commit(&mut pending).await.unwrap();

    assert_eq!(bigints(&read(&warehouse, "t").await, "id"), [0, 1, 2]);
    assert_eq!(warehouse.snapshots("t").await, 1);
}
