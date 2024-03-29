use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Int64Array, LargeStringArray, RecordBatch, StructArray,
    Time64NanosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use dbsp::utils::Tup2;
use dbsp::OrdZSet;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pipeline_types::format::parquet::ParquetEncoderConfig;
use pipeline_types::program_schema::{ColumnType, Field, Relation, SqlType};
use pretty_assertions::assert_eq;
use size_of::SizeOf;
use sqllib::{Date, Time, Timestamp};
use tempfile::NamedTempFile;

use crate::catalog::SerBatchReader;
use crate::format::parquet::ParquetEncoder;
use crate::format::Encoder;
use crate::static_compile::seroutput::SerBatchImpl;
use crate::test::{mock_input_pipeline, wait, MockOutputConsumer, DEFAULT_TIMEOUT_MS};
use pipeline_types::{deserialize_table_record, serialize_table_record};

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
struct TestStruct {
    #[serde(rename = "id")]
    field: i64,
    #[serde(rename = "name")]
    field_0: Option<String>,
    #[serde(rename = "b")]
    field_1: bool,
    #[serde(rename = "ts")]
    field_2: Timestamp,
    #[serde(rename = "dt")]
    field_3: Date,
    #[serde(rename = "t")]
    field_4: Time,
    #[serde(rename = "es")]
    field_5: EmbeddedStruct,
}

impl TestStruct {
    fn data() -> Vec<TestStruct> {
        vec![
            TestStruct {
                field: 1,
                field_0: Some("test".to_string()),
                field_1: false,
                field_2: Timestamp::new(1000),
                field_3: Date::new(1),
                field_4: Time::new(1),
                field_5: EmbeddedStruct { field: false },
            },
            TestStruct {
                field: 2,
                field_0: None,
                field_1: true,
                field_2: Timestamp::new(2000),
                field_3: Date::new(12),
                field_4: Time::new(1_000_000_000),
                field_5: EmbeddedStruct { field: true },
            },
        ]
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::LargeUtf8, true),
            arrow::datatypes::Field::new("b", DataType::Boolean, false),
            arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            arrow::datatypes::Field::new("dt", DataType::Date32, false),
            arrow::datatypes::Field::new("t", DataType::Time64(TimeUnit::Nanosecond), false),
            arrow::datatypes::Field::new(
                "es",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    arrow::datatypes::Field::new("a", DataType::Boolean, false),
                ])),
                false,
            ),
        ]))
    }

    fn relation() -> Relation {
        Relation::new(
            "TestStruct",
            false,
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
                Field {
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
                },
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
            ],
        )
    }

    fn make_arrow_array(data: &[TestStruct]) -> Vec<ArrayRef> {
        let row0: Vec<i64> = data.iter().map(|r| r.field).collect();
        let row1: Vec<Option<String>> = data.iter().map(|r| r.field_0.clone()).collect();
        let row2: Vec<bool> = data.iter().map(|r| r.field_1).collect();
        let row3: Vec<i64> = data.iter().map(|r| r.field_2.milliseconds()).collect();
        let row4: Vec<i32> = data.iter().map(|r| r.field_3.days()).collect();
        let row5: Vec<i64> = data
            .iter()
            .map(|r| r.field_4.nanoseconds() as i64)
            .collect();
        let row6_field = Arc::new(arrow::datatypes::Field::new("a", DataType::Boolean, false));
        let row6: Vec<bool> = data.iter().map(|r| r.field_5.field).collect();
        let row6_booleans = Arc::new(BooleanArray::from(row6));

        vec![
            Arc::new(Int64Array::from(row0)),
            Arc::new(LargeStringArray::from(row1)),
            Arc::new(BooleanArray::from(row2)),
            Arc::new(TimestampMillisecondArray::from(row3)),
            Arc::new(Date32Array::from(row4)),
            Arc::new(Time64NanosecondArray::from(row5)),
            Arc::new(StructArray::from(vec![(
                row6_field,
                row6_booleans as ArrayRef,
            )])),
        ]
    }
}

serialize_table_record!(TestStruct[7]{
    r#field["id"]: i64,
    r#field_0["name"]: Option<String>,
    r#field_1["b"]: bool,
    r#field_2["ts"]: Timestamp,
    r#field_3["dt"]: Date,
    r#field_4["t"]: Time,
    r#field_5["es"]: EmbeddedStruct
});

deserialize_table_record!(TestStruct["TestStruct", 7] {
    (r#field, "id", false, i64, None),
    (r#field_0, "name", false, Option<String>, Some(None)),
    (r#field_1, "b", false, bool, None),
    (r#field_2, "ts", false, Timestamp, None),
    (r#field_3, "dt", false, Date, None),
    (r#field_4, "t", false, Time, None),
    (r#field_5, "es", false, EmbeddedStruct, None)
});

#[test]
fn rel_to_schema() {
    use super::relation_to_parquet_schema;
    relation_to_parquet_schema(&TestStruct::relation()).expect("Can convert");
}

#[test]
fn parquet_input() {
    // Prepare input data & pipeline
    let test_data = TestStruct::data();
    let temp_file = NamedTempFile::new().unwrap();
    let config_str = format!(
        r#"
stream: test_input
transport:
    name: file
    config:
        path: {:?}
        buffer_size_bytes: 5
format:
    name: parquet
"#,
        temp_file.path().to_str().unwrap()
    );

    let batch = RecordBatch::try_new(
        TestStruct::schema(),
        TestStruct::make_arrow_array(&test_data),
    )
    .expect("RecordBatch creation should succeed");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&temp_file, TestStruct::schema(), Some(props))
        .expect("Writer creation should succeed");
    writer
        .write(&batch)
        .expect("Writing to parquet should succeed");
    writer.close().expect("Closing the writer should succeed");

    // Send the data through the mock pipeline
    let (endpoint, consumer, zset) =
        mock_input_pipeline::<TestStruct, TestStruct>(serde_yaml::from_str(&config_str).unwrap())
            .unwrap();
    sleep(Duration::from_millis(10));
    assert!(consumer.state().data.is_empty());
    assert!(!consumer.state().eoi);
    endpoint.start(0).unwrap();
    wait(
        || zset.state().flushed.len() == test_data.len(),
        DEFAULT_TIMEOUT_MS,
    );

    // Make sure the input data matches original test structs
    for (i, upd) in zset.state().flushed.iter().enumerate() {
        assert_eq!(upd.unwrap_insert(), &test_data[i]);
    }
}

#[test]
fn parquet_output() {
    let buffer = Arc::new(Mutex::new(Vec::with_capacity(4096)));
    let consumer = MockOutputConsumer::with_buffer(buffer.clone());
    let _consumer_data = consumer.data.clone();

    let config = ParquetEncoderConfig {
        buffer_size_records: usize::MAX,
    };

    let test_data = TestStruct::data();
    let mut encoder = ParquetEncoder::new(Box::new(consumer), config, TestStruct::relation())
        .expect("Can't create encoder");
    let zset = OrdZSet::from_keys(
        (),
        vec![Tup2(test_data[0].clone(), 2), Tup2(test_data[1].clone(), 1)],
    );

    let zset = &SerBatchImpl::<_, TestStruct, ()>::new(zset) as &dyn SerBatchReader;
    encoder.encode(zset).unwrap();

    // Verify output buffer...
    // Construct the expected file manually:
    let test_denorm = vec![
        test_data[0].clone(),
        test_data[0].clone(),
        test_data[1].clone(),
    ];
    let batch = RecordBatch::try_new(
        TestStruct::schema(),
        TestStruct::make_arrow_array(&test_denorm),
    )
    .expect("RecordBatch creation should succeed");
    let props = WriterProperties::builder().build();

    let mut expected_buffer: Vec<u8> = vec![];
    let mut expected_buffer_cursor = Cursor::new(&mut expected_buffer);
    let mut writer = ArrowWriter::try_new(
        &mut expected_buffer_cursor,
        TestStruct::schema(),
        Some(props),
    )
    .expect("Writer creation should succeed");
    writer
        .write(&batch)
        .expect("Writing to parquet should succeed");
    writer.close().expect("Closing the writer should succeed");
    debug_parquet_buffer(buffer.lock().unwrap().clone());

    let buffer_copy = buffer.lock().unwrap().clone();
    assert_eq!(expected_buffer, buffer_copy);
}

fn debug_parquet_buffer(buffer: Vec<u8>) {
    use bytes::Bytes;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let _r = env_logger::try_init();
    let buffer_copy = Bytes::from(buffer);
    let reader = SerializedFileReader::new(buffer_copy).expect("Reader creation should succeed");
    let row_iter = reader
        .get_row_iter(None)
        .expect("Row iterator creation should succeed");
    for maybe_record in row_iter {
        let record = maybe_record.expect("Record should be read successfully");
        log::info!("record = {:?}", record.to_string());
    }
}
