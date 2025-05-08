use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int64Array, Int64Builder, MapBuilder,
    MapFieldNames, StringArray, StringBuilder, StructArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use dbsp::utils::Tup2;
use feldera_sqllib::{
    ByteArray, Date, SqlDecimal, SqlString, Time, Timestamp, Uuid, Variant, F32, F64,
};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use feldera_types::{
    deserialize_table_record, deserialize_without_context, serialize_struct, serialize_table_record,
};
use prop::sample::SizeRange;
use proptest::{collection, prelude::*};
use proptest_derive::Arbitrary;
use rand::distributions::Standard;
use rand::prelude::Distribution;
use size_of::SizeOf;
use std::collections::BTreeMap;
use std::string::ToString;
use std::sync::Arc;

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    Arbitrary,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct TestStruct {
    pub id: u32,
    pub b: bool,
    pub i: Option<i64>,
    pub s: String,
}

impl TestStruct {
    pub fn for_id(id: u32) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("b", DataType::Boolean, false),
            arrow::datatypes::Field::new("i", DataType::Int64, true),
            arrow::datatypes::Field::new("s", DataType::Utf8, false),
        ]))
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::bigint(true)),
            Field::new("s".into(), ColumnType::varchar(false)),
        ]
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TestStruct", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
        }
    }

    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TestStruct",
            "fields": [
                { "name": "id", "type": "long" },
                { "name": "b", "type": "boolean" },
                { "name": "i", "type": ["null", "long"] },
                { "name": "s", "type": "string" }
            ]
        }"#
    }
}

impl Distribution<TestStruct> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> TestStruct {
        TestStruct {
            id: rng.gen_range(0..(i32::MAX as u32)),
            b: rng.gen(),
            i: rng.gen(),
            s: rng.gen::<u32>().to_string(),
        }
    }
}

deserialize_without_context!(TestStruct);

serialize_struct!(TestStruct()[4]{
    id["id"]: u32,
    b["b"]: bool,
    i["i"]: Option<i64>,
    s["s"]: String
});

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    Arbitrary,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct KeyStruct {
    pub id: u32,
}

impl KeyStruct {
    pub fn for_id(id: u32) -> Self {
        Self { id }
    }

    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            DataType::Int64,
            false,
        )]))
    }

    pub fn schema() -> Vec<Field> {
        vec![Field::new("id".into(), ColumnType::bigint(false))]
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("KeyStruct", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
        }
    }

    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "KeyStruct",
            "fields": [
                { "name": "id", "type": "long" }
            ]
        }"#
    }
}

deserialize_without_context!(KeyStruct);

serialize_struct!(KeyStruct()[1]{
    id["id"]: u32
});

/// Generate a batch of records no larger that `size`.
///
/// Makes sure all elements in the vector are unique and ordered.
/// This guarantees that the number and order of values remains the same
/// when they are assembled in a Z-set, which simplifies testing.
pub fn generate_test_batch(size: usize) -> impl Strategy<Value = Vec<TestStruct>> {
    collection::vec(any::<TestStruct>(), 0..=size).prop_map(|v| {
        v.into_iter()
            .enumerate()
            .map(|(i, mut val)| {
                val.id = i as u32;
                val
            })
            .collect::<Vec<_>>()
    })
}

/// Generate up to `max_batches` batches, up to `max_records` each.
///
/// Makes sure elements are unique and ordered across all batches.
pub fn generate_test_batches(
    min_batches: usize,
    max_batches: usize,
    max_records: usize,
) -> impl Strategy<Value = Vec<Vec<TestStruct>>> {
    collection::vec(
        collection::vec(any::<TestStruct>(), 0..=max_records),
        min_batches..=max_batches,
    )
    .prop_map(|batches| {
        let mut index = 0;
        batches
            .into_iter()
            .map(|batch| {
                batch
                    .into_iter()
                    .map(|mut val| {
                        val.id = index;
                        index += 1;
                        val
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    })
}

pub fn generate_test_batches_with_weights(
    max_batches: usize,
    max_records: usize,
) -> impl Strategy<Value = Vec<Vec<Tup2<TestStruct, i64>>>> {
    collection::vec(
        collection::vec(
            (any::<TestStruct>(), -2i64..=2i64).prop_map(|(x, y)| Tup2(x, y)),
            0..=max_records,
        ),
        0..=max_batches,
    )
    .prop_map(|batches| {
        let mut index = 0;
        batches
            .into_iter()
            .map(|batch| {
                batch
                    .into_iter()
                    .map(|mut val| {
                        val.0.id = index;
                        index += 1;
                        val
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    })
}

/// This struct mimics the field naming schema of the compiler.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Arbitrary,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct EmbeddedStruct {
    #[serde(rename = "a")]
    pub field: bool,
}

serialize_table_record!(EmbeddedStruct[1]{
    r#field["a"]: bool
});

deserialize_table_record!(EmbeddedStruct["EmbeddedStruct", 1] {
    (r#field, "a", false, bool, None)
});

/// This struct mimics the field naming schema of the compiler.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct TestStruct2 {
    #[serde(rename = "id")]
    pub field: i64,
    #[serde(rename = "nAmE")]
    pub field_0: Option<String>,
    #[serde(rename = "b")]
    pub field_1: bool,
    #[serde(rename = "ts")]
    pub field_2: Timestamp,
    #[serde(rename = "dt")]
    pub field_3: Date,
    // DeltaLake doesn't understand time
    // #[serde(rename = "t")]
    // pub field_4: Time,
    #[serde(rename = "es")]
    pub field_5: Option<EmbeddedStruct>,
    #[serde(rename = "m")]
    pub field_6: Option<BTreeMap<String, i64>>,
    pub field_7: SqlDecimal,
}

impl Arbitrary for TestStruct2 {
    type Parameters = ();

    type Strategy = BoxedStrategy<TestStruct2>;

    fn arbitrary_with(_params: Self::Parameters) -> Self::Strategy {
        (
            i64::arbitrary(),
            String::arbitrary(),
            bool::arbitrary(),
            // Generate timestamps within a 1-year range
            1704070800u32..1735693200,
            0u32..100_000,
            // (0u64..24 * 60 * 60 * 1_000_000),
            EmbeddedStruct::arbitrary_with(()),
            // Generate small maps, otherwise proptests take forever.
            BTreeMap::<String, i64>::arbitrary_with((
                SizeRange::from(0..2),
                Default::default(),
                Default::default(),
            )),
            0..1_000_000i128,
            // Scale
            0..3u32,
        )
            .prop_map(
                |(f, f0, f1, f2, f3, /*f4,*/ f5, f6, f7_num, f7_scale)| TestStruct2 {
                    field: f,
                    field_0: if f1 { Some(f0) } else { None },
                    field_1: f1,
                    field_2: Timestamp::new(f2 as i64 * 1_000),
                    field_3: Date::new(f3 as i32),
                    // field_4: Time::new(f4 * 1000),
                    field_5: Some(f5),
                    field_6: Some(f6),
                    field_7: SqlDecimal::from_i128_with_scale(f7_num, f7_scale as i32),
                },
            )
            .boxed()
    }
}

impl TestStruct2 {
    pub fn data() -> Vec<TestStruct2> {
        vec![
            TestStruct2 {
                field: 1,
                field_0: Some("test".to_string()),
                field_1: false,
                field_2: Timestamp::new(1000),
                field_3: Date::new(1),
                // field_4: Time::new(1),
                field_5: Some(EmbeddedStruct { field: false }),
                field_6: Some(BTreeMap::from([
                    ("foo".to_string(), 100),
                    ("bar".to_string(), 200),
                ])),
                field_7: SqlDecimal::from_i128_with_scale(10000, 3),
            },
            TestStruct2 {
                field: 2,
                field_0: None,
                field_1: true,
                field_2: Timestamp::new(2000),
                field_3: Date::new(12),
                // field_4: Time::new(1_000_000_000),
                field_5: Some(EmbeddedStruct { field: true }),
                field_6: Some(BTreeMap::new()),
                field_7: SqlDecimal::from_i128_with_scale(1, 3),
            },
        ]
    }

    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("nAmE", DataType::Utf8, true),
            arrow::datatypes::Field::new("b", DataType::Boolean, false),
            arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            arrow::datatypes::Field::new("dt", DataType::Date32, false),
            // arrow::datatypes::Field::new("t", DataType::Time64(TimeUnit::Nanosecond), false),
            arrow::datatypes::Field::new(
                "es",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    arrow::datatypes::Field::new("a", DataType::Boolean, false),
                ])),
                true,
            ),
            arrow::datatypes::Field::new_map(
                "m",
                "entries",
                arrow::datatypes::Field::new("key", DataType::Utf8, false),
                arrow::datatypes::Field::new("value", DataType::Int64, false),
                false,
                true,
            ),
            arrow::datatypes::Field::new("dec", DataType::Decimal128(10, 3), false),
        ]))
    }

    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TestStruct2",
            "connect.name": "test_namespace.TestStruct2",
            "fields": [
                { "name": "id", "type": "long" },
                { "name": "nAmE", "type": ["string", "null"] },
                { "name": "b", "type": "boolean" },
                { "name": "ts", "type": "long", "logicalType": "timestamp-micros" },
                { "name": "dt", "type": "int", "logicalType": "date" },
                {
                    "name": "es",
                    "type":
                        [{
                            "type": "record",
                            "name": "EmbeddedStruct",
                            "fields": [
                                { "name": "a", "type": "boolean" }
                            ]
                        }, "null"]
                },
                {
                    "name": "m",
                    "type":
                        [{
                            "type": "map",
                            "values": "long"
                        }, "null"]
                },
                {
                    "name": "dec",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 10,
                        "scale": 3
                    }
                }
            ]
        }"#
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("\"nAmE\"".into(), ColumnType::varchar(true)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("ts".into(), ColumnType::timestamp(false)),
            Field::new("dt".into(), ColumnType::date(false)),
            Field::new(
                "es".into(),
                ColumnType::structure(true, &[Field::new("a".into(), ColumnType::boolean(false))]),
            ),
            Field::new(
                "m".into(),
                ColumnType::map(true, ColumnType::varchar(false), ColumnType::bigint(false)),
            ),
            Field::new("dec".into(), ColumnType::decimal(10, 3, false)),
        ]
    }

    pub fn schema_with_lateness() -> Vec<Field> {
        let fields = vec![
            Field::new("id".into(), ColumnType::bigint(false)).with_lateness("1000"),
            Field::new("\"nAmE\"".into(), ColumnType::varchar(true)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("ts".into(), ColumnType::timestamp(false))
                .with_lateness("interval '10 days'"),
            Field::new("dt".into(), ColumnType::date(false)),
            Field::new(
                "es".into(),
                ColumnType::structure(true, &[Field::new("a".into(), ColumnType::boolean(false))]),
            ),
            Field::new(
                "m".into(),
                ColumnType::map(true, ColumnType::varchar(false), ColumnType::bigint(false)),
            ),
            Field::new("dec".into(), ColumnType::decimal(10, 3, false)),
        ];

        fields
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TestStruct2", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
        }
    }

    pub fn make_arrow_array(data: &[TestStruct2]) -> Vec<ArrayRef> {
        let row0: Vec<i64> = data.iter().map(|r| r.field).collect();
        let row1: Vec<Option<String>> = data.iter().map(|r| r.field_0.clone()).collect();
        let row2: Vec<bool> = data.iter().map(|r| r.field_1).collect();
        let row3: Vec<i64> = data
            .iter()
            .map(|r| r.field_2.milliseconds() * 1000)
            .collect();
        let row4: Vec<i32> = data.iter().map(|r| r.field_3.days()).collect();
        /*let row5: Vec<i64> = data
        .iter()
        .map(|r| r.field_4.nanoseconds() as i64)
        .collect();*/
        let row6_field = Arc::new(arrow::datatypes::Field::new("a", DataType::Boolean, false));
        let row6: Vec<Option<bool>> = data
            .iter()
            .map(|r| r.field_5.as_ref().map(|emb_struct| emb_struct.field))
            .collect();
        let row6_booleans = Arc::new(BooleanArray::from(row6));
        let row7: Vec<i128> = data.iter().map(|r| r.field_7.mantissa()).collect();

        // Create an Arrow Decimal128Array
        let decimal_array = Decimal128Array::from(row7).with_data_type(DataType::Decimal128(10, 3));

        let string_builder = StringBuilder::new();
        let int_builder = Int64Builder::new();

        let mut map_builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            string_builder,
            int_builder,
        )
        .with_values_field(arrow::datatypes::Field::new(
            "value",
            DataType::Int64,
            false,
        ));
        for x in data.iter() {
            if let Some(mp) = &x.field_6 {
                for (key, val) in mp.iter() {
                    map_builder.keys().append_value(key);
                    map_builder.values().append_value(*val);
                }
                map_builder.append(true).unwrap()
            } else {
                map_builder.append(false).unwrap()
            }
        }
        let map_array = map_builder.finish();

        vec![
            Arc::new(Int64Array::from(row0)),
            Arc::new(StringArray::from(row1)),
            Arc::new(BooleanArray::from(row2)),
            Arc::new(TimestampMicrosecondArray::from(row3)),
            Arc::new(Date32Array::from(row4)),
            // Arc::new(Time64NanosecondArray::from(row5)),
            Arc::new(StructArray::from(vec![(
                row6_field,
                row6_booleans as ArrayRef,
            )])),
            Arc::new(map_array),
            Arc::new(decimal_array),
        ]
    }
}

serialize_table_record!(TestStruct2[8]{
    r#field["id"]: i64,
    r#field_0["nAmE"]: Option<String>,
    r#field_1["b"]: bool,
    r#field_2["ts"]: Timestamp,
    r#field_3["dt"]: Date,
    // r#field_4["t"]: Time,
    r#field_5["es"]: Option<EmbeddedStruct>,
    r#field_6["m"]: Option<Map<String, i64>>,
    r#field_7["dec"]: SqlDecimal
});

deserialize_table_record!(TestStruct2["TestStruct", 8] {
    (r#field, "id", false, i64, None),
    (r#field_0, "nAmE", true, Option<String>, Some(None)),
    (r#field_1, "b", false, bool, None),
    (r#field_2, "ts", false, Timestamp, None),
    (r#field_3, "dt", false, Date, None),
    // (r#field_4, "t", false, Time, None),
    (r#field_5, "es", false, Option<EmbeddedStruct>, Some(None)),
    (r#field_6, "m", false, Option<BTreeMap<String, i64>>, Some(None)),
    (r#field_7, "dec", false, SqlDecimal, None)
});

/// Record in the databricks people dataset.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct DatabricksPeople {
    pub id: i32,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    pub gender: Option<String>,
    pub birth_date: Option<Timestamp>,
    pub ssn: Option<String>,
    pub salary: Option<i32>,
}

impl DatabricksPeople {
    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("id".into(), ColumnType::int(false)),
            Field::new("firstName".into(), ColumnType::varchar(true)),
            Field::new("middleName".into(), ColumnType::varchar(true)),
            Field::new("lastName".into(), ColumnType::varchar(true)),
            Field::new("gender".into(), ColumnType::varchar(true)),
            Field::new("birthDate".into(), ColumnType::timestamp(true)),
            Field::new("ssn".into(), ColumnType::varchar(true)),
            Field::new("salary".into(), ColumnType::int(true)),
        ]
    }
}

serialize_table_record!(DatabricksPeople[8]{
    r#id["id"]: i32,
    r#first_name["firstname"]: Option<String>,
    r#middle_name["middlename"]: Option<String>,
    r#last_name["lastname"]: Option<String>,
    r#gender["gender"]: Option<String>,
    r#birth_date["birthdate"]: Option<Timestamp>,
    r#ssn["ssn"]: Option<String>,
    r#salary["salary"]: Option<i32>
});

deserialize_table_record!(DatabricksPeople["DatabricksPeople", 8] {
    (r#id, "id", false, i32, None),
    (r#first_name, "firstname", false, Option<String>, Some(None)),
    (r#middle_name, "middlename", false, Option<String>, Some(None)),
    (r#last_name, "lastname", false, Option<String>, Some(None)),
    (r#gender, "gender", false, Option<String>, Some(None)),
    (r#birth_date, "birthdate", false, Option<Timestamp>, Some(None)),
    (r#ssn, "ssn", false, Option<String>, Some(None)),
    (r#salary, "salary", false, Option<i32>, Some(None))
});

/// Struct will all types supported by the Iceberg connector.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct IcebergTestStruct {
    pub b: bool,
    pub i: i32,
    pub l: i64,
    pub r: F32,
    pub d: F64,
    pub dec: SqlDecimal,
    pub dt: Date,
    pub tm: Time,
    pub ts: Timestamp,
    pub s: String,
    //pub uuid: ByteArray,
    pub fixed: ByteArray,
    pub varbin: ByteArray,
}

impl Arbitrary for IcebergTestStruct {
    type Parameters = ();

    type Strategy = BoxedStrategy<IcebergTestStruct>;

    fn arbitrary_with(_params: Self::Parameters) -> Self::Strategy {
        (
            // Split into two tuples with <12 fields each.
            (
                bool::arbitrary(),
                i32::arbitrary(),
                i64::arbitrary(),
                f32::arbitrary(),
                f64::arbitrary(),
                0..1_000_000i128,
                // Scale
                0..3u32,
                0i32..100_000,
                // Time in nanos
                0u64..24 * 3600 * 1_000_000_000,
                // Generate timestamps within a 1-year range
                1704070800i64..1735693200,
            ),
            // String in the range "0".."1000"
            (
                0i32..1000,
                // // UUID
                // collection::vec(u8::arbitrary(), 16..=16),
                // Fixed
                collection::vec(u8::arbitrary(), 5..=5),
                // Varbinary
                collection::vec(u8::arbitrary(), 0..=10),
            ),
        )
            .prop_map(
                |(
                    (b, i, l, r, d, dec_num, dec_scale, dt, tm, ts),
                    (s, /*uuid,*/ fixed, varbin),
                ): (
                    (bool, i32, i64, f32, f64, i128, u32, i32, u64, i64),
                    (i32, /*Vec<u8>,*/ Vec<u8>, Vec<u8>),
                )| {
                    IcebergTestStruct {
                        b,
                        i,
                        l,
                        r: F32::new(r),
                        d: F64::new(d),
                        dec: SqlDecimal::from_i128_with_scale(dec_num, dec_scale as i32),
                        dt: Date::new(dt),
                        tm: Time::new(tm),
                        ts: Timestamp::new(ts),
                        s: s.to_string(),
                        // uuid: ByteArray::from_vec(uuid),
                        fixed: ByteArray::from_vec(fixed),
                        varbin: ByteArray::new(&varbin),
                    }
                },
            )
            .boxed()
    }
}

impl IcebergTestStruct {
    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("b", DataType::Boolean, false),
            arrow::datatypes::Field::new("i", DataType::Int32, false),
            arrow::datatypes::Field::new("l", DataType::Int64, false),
            arrow::datatypes::Field::new("r", DataType::Float32, false),
            arrow::datatypes::Field::new("d", DataType::Float64, false),
            arrow::datatypes::Field::new("dec", DataType::Decimal128(10, 3), false),
            arrow::datatypes::Field::new("dt", DataType::Date32, false),
            arrow::datatypes::Field::new("tm", DataType::Time64(TimeUnit::Microsecond), false),
            arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            arrow::datatypes::Field::new("s", DataType::Utf8, false),
            // arrow::datatypes::Field::new("uuid", DataType::FixedSizeBinary(16), false),
            arrow::datatypes::Field::new("fixed", DataType::FixedSizeBinary(5), false),
            arrow::datatypes::Field::new("varbin", DataType::Binary, false),
        ]))
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::int(false)),
            Field::new("l".into(), ColumnType::bigint(false)),
            Field::new("r".into(), ColumnType::real(false)),
            Field::new("d".into(), ColumnType::double(false)),
            Field::new("dec".into(), ColumnType::decimal(10, 3, false)),
            Field::new("dt".into(), ColumnType::date(false)),
            Field::new("tm".into(), ColumnType::time(false)),
            Field::new("ts".into(), ColumnType::timestamp(false)),
            Field::new("s".into(), ColumnType::varchar(false)),
            // Field::new("uuid".into(), ColumnType::fixed(16, false)),
            Field::new("fixed".into(), ColumnType::fixed(5, false)),
            Field::new("varbin".into(), ColumnType::varbinary(false)),
        ]
    }

    pub fn schema_with_lateness() -> Vec<Field> {
        let fields = vec![
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::int(false)),
            Field::new("l".into(), ColumnType::bigint(false)),
            Field::new("r".into(), ColumnType::real(false)),
            Field::new("d".into(), ColumnType::double(false)),
            Field::new("dec".into(), ColumnType::decimal(10, 3, false)),
            Field::new("dt".into(), ColumnType::date(false)),
            Field::new("tm".into(), ColumnType::time(false)),
            Field::new("ts".into(), ColumnType::timestamp(false))
                .with_lateness("interval '10 days'"),
            Field::new("s".into(), ColumnType::varchar(false)),
            // Field::new("uuid".into(), ColumnType::fixed(16, false)),
            Field::new("fixed".into(), ColumnType::fixed(5, false)),
            Field::new("varbin".into(), ColumnType::varbinary(false)),
        ];

        fields
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("IcebergTestStruct", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
        }
    }
}

serialize_table_record!(IcebergTestStruct[12]{
    b["b"]: bool,
    i["i"]: i32,
    l["l"]: i64,
    r["r"]: F32,
    d["d"]: F64,
    dec["dec"]: SqlDecimal,
    dt["dt"]: Date,
    tm["tm"]: Time,
    ts["ts"]: Timestamp,
    s["s"]: String,
    // uuid["uuid"]: ByteArray,
    fixed["fixed"]: ByteArray,
    varbin["varbin"]: ByteArray
});

deserialize_table_record!(IcebergTestStruct["IcebergTestStruct", 12] {
    (b, "b", false, bool, None),
    (i, "i", false, i32, None),
    (l, "l", false, i64, None),
    (r, "r", false, F32, None),
    (d, "d", false, F64, None),
    (dec, "dec", false, SqlDecimal, None),
    (dt, "dt", false, Date, None),
    (tm, "tm", false, Time, None),
    (ts, "ts", false, Timestamp, None),
    (s, "s", false, String, None),
    // (uuid, "uuid", false, ByteArray, None),
    (fixed, "fixed", false, ByteArray, None),
    (varbin, "varbin", false, ByteArray, None)
});

/// Struct will all types supported by the DeltaLake connector.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct DeltaTestStruct {
    pub bigint: i64,
    pub binary: ByteArray,
    pub boolean: bool,
    pub date: Date,
    pub decimal_10_3: SqlDecimal,
    pub double: F64,
    pub float: F32,
    pub int: i32,
    pub smallint: i16,
    pub string: String,
    pub unused: Option<String>,
    pub timestamp_ntz: Timestamp,
    pub tinyint: i8,
    pub string_array: Vec<String>,
    pub struct1: TestStruct,
    pub struct_array: Vec<TestStruct>,
    pub string_string_map: BTreeMap<String, String>,
    pub string_struct_map: BTreeMap<String, TestStruct>,
    pub variant: Variant,
    pub uuid: Uuid,
}

// TODO: INTERVAL, VOID, TIMESTAMP (with tz), Object, map with non-string keys

impl Arbitrary for DeltaTestStruct {
    type Parameters = ();

    type Strategy = BoxedStrategy<DeltaTestStruct>;

    fn arbitrary_with(_params: Self::Parameters) -> Self::Strategy {
        (
            (
                i64::arbitrary(),
                collection::vec(u8::arbitrary(), 0..=10),
                bool::arbitrary(),
                0i32..100_000,    // Date
                0..1_000_000i128, // Decimal digits
                -1_000_000f64..1_000_000f64,
                -1_000_000f32..1_000_000f32,
                i32::arbitrary(),
                i16::arbitrary(),
            ),
            (
                0i32..1000, // string
                1000i32..2000,
                1704070800i64..1735693200, // Generate timestamps within a 1-year range
                i8::arbitrary(),
                collection::vec(i32::arbitrary().prop_map(|x| x.to_string()), 0..=2), // string array
                TestStruct::arbitrary(),
                collection::vec(TestStruct::arbitrary(), 0..=2),
                BTreeMap::<String, String>::arbitrary_with((
                    SizeRange::from(0..2),
                    Default::default(),
                    Default::default(),
                )),
                BTreeMap::<String, TestStruct>::arbitrary_with((
                    SizeRange::from(0..2),
                    Default::default(),
                    Default::default(),
                )),
                (0i32..1000),
                <[u8; 16]>::arbitrary(),
            ),
        )
            .prop_map(
                |(
                    (bigint, binary, boolean, date, decimal_digits, double, float, int, smallint),
                    (
                        string,
                        unused,
                        timestamp_ntz,
                        tinyint,
                        string_array,
                        struct1,
                        struct_array,
                        string_string_map,
                        string_struct_map,
                        variant,
                        uuid,
                    ),
                )| {
                    DeltaTestStruct {
                        bigint,
                        binary: ByteArray::from_vec(binary),
                        boolean,
                        date: Date::new(date),
                        decimal_10_3: SqlDecimal::from_i128_with_scale(decimal_digits, 3),
                        double: F64::new(double.trunc()), // truncate to avoid rounding errors when serializing floats to/from JSON
                        float: F32::new(float.trunc()),
                        int,
                        smallint,
                        string: string.to_string(),
                        unused: Some(unused.to_string()),
                        timestamp_ntz: Timestamp::new(timestamp_ntz * 1000),
                        tinyint,
                        string_array,
                        struct1,
                        struct_array,
                        string_string_map,
                        string_struct_map,
                        variant: Variant::Map(
                            std::iter::once((
                                (Variant::String(SqlString::from_ref("foo"))),
                                Variant::String(SqlString::from(variant.to_string())),
                            ))
                            .collect::<BTreeMap<Variant, Variant>>()
                            .into(),
                        ),
                        uuid: Uuid::from_bytes(uuid),
                    }
                },
            )
            .boxed()
    }
}

impl DeltaTestStruct {
    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("bigint", DataType::Int64, false),
            arrow::datatypes::Field::new("binary", DataType::Binary, false),
            arrow::datatypes::Field::new("boolean", DataType::Boolean, false),
            arrow::datatypes::Field::new("date", DataType::Date32, false),
            arrow::datatypes::Field::new("decimal_10_3", DataType::Decimal128(10, 3), false),
            arrow::datatypes::Field::new("double", DataType::Float64, false),
            arrow::datatypes::Field::new("float", DataType::Float32, false),
            arrow::datatypes::Field::new("int", DataType::Int32, false),
            arrow::datatypes::Field::new("smallint", DataType::Int16, false),
            arrow::datatypes::Field::new("string", DataType::Utf8, false),
            arrow::datatypes::Field::new("unused", DataType::Utf8, true),
            arrow::datatypes::Field::new(
                "timestamp_ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            arrow::datatypes::Field::new("tinyint", DataType::Int8, false),
            arrow::datatypes::Field::new(
                "string_array",
                DataType::new_list(DataType::Utf8, false),
                false,
            ),
            arrow::datatypes::Field::new(
                "struct1",
                DataType::Struct(TestStruct::arrow_schema().fields.clone()),
                false,
            ),
            arrow::datatypes::Field::new(
                "struct_array",
                DataType::new_list(
                    DataType::Struct(TestStruct::arrow_schema().fields.clone()),
                    false,
                ),
                false,
            ),
            arrow::datatypes::Field::new_map(
                "string_string_map",
                "entries",
                arrow::datatypes::Field::new("key", DataType::Utf8, false),
                arrow::datatypes::Field::new("value", DataType::Utf8, false),
                false,
                false,
            ),
            arrow::datatypes::Field::new_map(
                "string_struct_map",
                "entries",
                arrow::datatypes::Field::new("key", DataType::Utf8, false),
                arrow::datatypes::Field::new(
                    "value",
                    DataType::Struct(TestStruct::arrow_schema().fields.clone()),
                    false,
                ),
                false,
                false,
            ),
            arrow::datatypes::Field::new("variant", DataType::Utf8, false),
            arrow::datatypes::Field::new("uuid", DataType::Utf8, false),
        ]))
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("bigint".into(), ColumnType::bigint(false)),
            Field::new("binary".into(), ColumnType::varbinary(false)),
            Field::new("boolean".into(), ColumnType::boolean(false)),
            Field::new("date".into(), ColumnType::date(false)),
            Field::new("decimal_10_3".into(), ColumnType::decimal(10, 3, false)),
            Field::new("double".into(), ColumnType::double(false)),
            Field::new("float".into(), ColumnType::real(false)),
            Field::new("int".into(), ColumnType::int(false)),
            Field::new("smallint".into(), ColumnType::smallint(false)),
            Field::new("string".into(), ColumnType::varchar(false)),
            Field::new("unused".into(), ColumnType::varchar(true)).with_unused(true),
            Field::new("timestamp_ntz".into(), ColumnType::timestamp(false)),
            Field::new("tinyint".into(), ColumnType::tinyint(false)),
            Field::new(
                "string_array".into(),
                ColumnType::array(false, ColumnType::varchar(false)),
            ),
            Field::new(
                "struct1".into(),
                ColumnType::structure(false, &TestStruct::schema()),
            ),
            Field::new(
                "struct_array".into(),
                ColumnType::array(false, ColumnType::structure(false, &TestStruct::schema())),
            ),
            Field::new(
                "string_string_map".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::varchar(false),
                ),
            ),
            Field::new(
                "string_struct_map".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::structure(false, &TestStruct::schema()),
                ),
            ),
            Field::new("variant".into(), ColumnType::variant(false)),
            Field::new("uuid".into(), ColumnType::uuid(false)),
        ]
    }

    pub fn schema_with_lateness() -> Vec<Field> {
        let fields = vec![
            Field::new("bigint".into(), ColumnType::bigint(false)).with_lateness("1000"),
            Field::new("binary".into(), ColumnType::varbinary(false)),
            Field::new("boolean".into(), ColumnType::boolean(false)),
            Field::new("date".into(), ColumnType::date(false)),
            Field::new("decimal_10_3".into(), ColumnType::decimal(10, 3, false)),
            Field::new("double".into(), ColumnType::double(false)),
            Field::new("float".into(), ColumnType::real(false)),
            Field::new("int".into(), ColumnType::int(false)),
            Field::new("smallint".into(), ColumnType::smallint(false)),
            Field::new("string".into(), ColumnType::varchar(false)),
            Field::new("unused".into(), ColumnType::varchar(true)).with_unused(true),
            Field::new("timestamp_ntz".into(), ColumnType::timestamp(false))
                .with_lateness("interval '10 days'"),
            Field::new("tinyint".into(), ColumnType::tinyint(false)),
            Field::new(
                "string_array".into(),
                ColumnType::array(false, ColumnType::varchar(false)),
            ),
            Field::new(
                "struct1".into(),
                ColumnType::structure(false, &TestStruct::schema()),
            ),
            Field::new(
                "struct_array".into(),
                ColumnType::array(false, ColumnType::structure(false, &TestStruct::schema())),
            ),
            Field::new(
                "string_string_map".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::varchar(false),
                ),
            ),
            Field::new(
                "string_struct_map".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::structure(false, &TestStruct::schema()),
                ),
            ),
            Field::new("variant".into(), ColumnType::variant(false)),
            Field::new("uuid".into(), ColumnType::uuid(false)),
        ];

        fields
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("DeltaTestStruct", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
        }
    }
}

serialize_table_record!(DeltaTestStruct[20]{
    bigint["bigint"]: i64,
    binary["binary"]: ByteArray,
    boolean["boolean"]: bool,
    date["date"]: Date,
    decimal_10_3["decimal_10_3"]: SqlDecimal,
    double["double"]: F64,
    float["float"]: F32,
    int["int"]: i32,
    smallint["smallint"]: i16,
    string["string"]: String,
    unused["unused"]: Option<String>,
    timestamp_ntz["timestamp_ntz"]: Timestamp,
    tinyint["tinyint"]: i8,
    string_array["string_array"]: Vec<String>,
    struct1["struct1"]: TestStruct,
    struct_array["struct_array"]: Vec<TestStruct>,
    string_string_map["string_string_map"]: BTreeMap<String, String>,
    string_struct_map["string_struct_map"]: BTreeMap<String, TestStruct>,
    variant["variant"]: Variant,
    uuid["uuid"]: Uuid
});

deserialize_table_record!(DeltaTestStruct["DeltaTestStruct", 20] {
    (bigint, "bigint", false, i64, None),
    (binary, "binary", false, ByteArray, None),
    (boolean, "boolean", false, bool, None),
    (date, "date", false, Date, None),
    (decimal_10_3, "decimal_10_3", false, SqlDecimal, None),
    (double, "double", false, F64, None),
    (float, "float", false, F32, None),
    (int, "int", false, i32, None),
    (smallint, "smallint", false, i16, None),
    (string, "string", false, String, None),
    (unused, "unused", false, Option<String>, Some(None)),
    (timestamp_ntz, "timestamp_ntz", false, Timestamp, None),
    (tinyint, "tinyint", false, i8, None),
    (string_array, "string_array", false, Vec<String>, None),
    (struct1, "struct1", false, TestStruct, None),
    (struct_array, "struct_array", false, Vec<TestStruct>, None),
    (string_string_map, "string_string_map", false, BTreeMap<String, String>, None),
    (string_struct_map, "string_struct_map", false, BTreeMap<String, TestStruct>, None),
    (variant, "variant", false, Variant, None),
    (uuid, "uuid", false, Uuid, None)
});
