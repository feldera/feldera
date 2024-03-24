use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Int64Array, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use dbsp::utils::Tup2;
use pipeline_types::program_schema::{ColumnType, Field, SqlType};
use proptest::{collection, prelude::*};
use proptest_derive::Arbitrary;
use size_of::SizeOf;
use std::string::ToString;
use std::sync::Arc;

use pipeline_types::{
    deserialize_table_record, deserialize_without_context, serialize_struct, serialize_table_record,
};
use sqllib::{Date, Timestamp};

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
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct TestStruct {
    pub id: u32,
    pub b: bool,
    pub i: Option<i64>,
    pub s: String,
}

impl TestStruct {
    pub fn schema() -> Vec<Field> {
        vec![
            Field {
                name: "id".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::BigInt,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
            Field {
                name: "b".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::Boolean,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
            Field {
                name: "i".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::BigInt,
                    nullable: true,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
            Field {
                name: "s".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::Varchar,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
        ]
    }
}

deserialize_without_context!(TestStruct);

serialize_struct!(TestStruct()[4]{
    id["id"]: u32,
    b["b"]: bool,
    i["i"]: Option<i64>,
    s["s"]: String
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
struct EmbeddedStruct {
    #[serde(rename = "a")]
    field: bool,
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
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct TestStruct2 {
    #[serde(rename = "id")]
    pub field: i64,
    #[serde(rename = "name")]
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
    field_5: EmbeddedStruct,
}

impl Arbitrary for TestStruct2 {
    type Parameters = ();

    type Strategy = BoxedStrategy<TestStruct2>;

    fn arbitrary_with(_params: Self::Parameters) -> Self::Strategy {
        (
            i64::arbitrary(),
            String::arbitrary(),
            bool::arbitrary(),
            u32::arbitrary(),
            0u32..100_000,
            // (0u64..24 * 60 * 60 * 1_000_000),
            EmbeddedStruct::arbitrary_with(()),
        )
            .prop_map(|(f, f0, f1, f2, f3, /*f4,*/ f5)| TestStruct2 {
                field: f,
                field_0: if f1 { Some(f0) } else { None },
                field_1: f1,
                field_2: Timestamp::new(f2 as i64 * 1_000),
                field_3: Date::new(f3 as i32),
                // field_4: Time::new(f4 * 1000),
                field_5: f5,
            })
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
                field_5: EmbeddedStruct { field: false },
            },
            TestStruct2 {
                field: 2,
                field_0: None,
                field_1: true,
                field_2: Timestamp::new(2000),
                field_3: Date::new(12),
                // field_4: Time::new(1_000_000_000),
                field_5: EmbeddedStruct { field: true },
            },
        ]
    }

    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, true),
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
                false,
            ),
        ]))
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field {
                name: "id".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::BigInt,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
            Field {
                name: "name".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::Varchar,
                    nullable: true,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
            Field {
                name: "b".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::Boolean,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
            Field {
                name: "ts".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::Timestamp,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
            Field {
                name: "dt".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::Date,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },
            /*Field {
                name: "t".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::Time,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                },
            },*/
            Field {
                name: "es".to_string(),
                case_sensitive: false,
                columntype: ColumnType {
                    typ: SqlType::Struct,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: Some(vec![Field {
                        name: "a".to_string(),
                        case_sensitive: false,
                        columntype: ColumnType {
                            typ: SqlType::Boolean,
                            nullable: false,
                            precision: None,
                            scale: None,
                            component: None,
                            fields: None,
                        },
                    }]),
                },
            },
        ]
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
        let row6: Vec<bool> = data.iter().map(|r| r.field_5.field).collect();
        let row6_booleans = Arc::new(BooleanArray::from(row6));

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
        ]
    }
}

serialize_table_record!(TestStruct2[6]{
    r#field["id"]: i64,
    r#field_0["name"]: Option<String>,
    r#field_1["b"]: bool,
    r#field_2["ts"]: Timestamp,
    r#field_3["dt"]: Date,
    // r#field_4["t"]: Time,
    r#field_5["es"]: EmbeddedStruct
});

deserialize_table_record!(TestStruct2["TestStruct", 6] {
    (r#field, "id", false, i64, None),
    (r#field_0, "name", false, Option<String>, Some(None)),
    (r#field_1, "b", false, bool, None),
    (r#field_2, "ts", false, Timestamp, None),
    (r#field_3, "dt", false, Date, None),
    // (r#field_4, "t", false, Time, None),
    (r#field_5, "es", false, EmbeddedStruct, None)
});
