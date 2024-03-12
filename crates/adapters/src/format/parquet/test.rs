use std::borrow::Cow;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Int64Array, LargeStringArray, RecordBatch,
    Time64NanosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use dbsp::trace::Batch;
use dbsp::OrdZSet;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pipeline_types::format::parquet::ParquetEncoderConfig;
use pipeline_types::program_schema::{ColumnType, Field, Relation, SqlType};
use pretty_assertions::assert_eq;
use serde::{Deserialize, Deserializer, Serializer};
use size_of::SizeOf;
use tempfile::NamedTempFile;

use crate::format::parquet::ParquetEncoder;
use crate::format::Encoder;
use crate::static_compile::seroutput::SerBatchImpl;
use crate::test::{mock_input_pipeline, wait, MockOutputConsumer, DEFAULT_TIMEOUT_MS};
use crate::{
    deserialize_table_record, deserialize_without_context, serialize_table_record,
    serialize_without_context, DateFormat, DeserializeWithContext, SerBatch, SerializeWithContext,
    SqlSerdeConfig, TimestampFormat,
};

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Timestamp {
    // since unix epoch
    milliseconds: i64,
}

impl Timestamp {
    fn new(milliseconds: i64) -> Self {
        Self { milliseconds }
    }

    fn milliseconds(&self) -> i64 {
        self.milliseconds
    }
}

impl SerializeWithContext<SqlSerdeConfig> for Timestamp {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.timestamp_format {
            TimestampFormat::String(format_string) => {
                let datetime = DateTime::from_timestamp(
                    self.milliseconds / 1000,
                    ((self.milliseconds % 1000) * 1000) as u32,
                )
                .expect("Timestamp should be valid");
                serializer.serialize_str(&datetime.format(format_string).to_string())
            }
            TimestampFormat::MillisSinceEpoch => serializer.serialize_i64(self.milliseconds),
        }
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for Timestamp {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match config.timestamp_format {
            TimestampFormat::String(format) => {
                // `timestamp_str: &'de` doesn't work for JSON, which escapes strings
                // and can only deserialize into an owned string.
                let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
                let timestamp = NaiveDateTime::parse_from_str(timestamp_str.trim(), format)
                    .expect("Timestamp should be valid");
                Ok(Self::new(timestamp.timestamp_millis()))
            }
            TimestampFormat::MillisSinceEpoch => {
                let millis: i64 = Deserialize::deserialize(deserializer)?;
                Ok(Self::new(millis))
            }
        }
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Time {
    nanoseconds: u64,
}

impl Time {
    fn new(nanoseconds: u64) -> Self {
        Self { nanoseconds }
    }

    fn nanoseconds(&self) -> u64 {
        self.nanoseconds
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Date {
    // since unix epoch
    days: i32,
}

impl Date {
    fn new(days: i32) -> Self {
        Self { days }
    }

    fn days(&self) -> i32 {
        self.days
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for Date {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match config.date_format {
            DateFormat::String(format) => {
                let str: &'de str = Deserialize::deserialize(deserializer)?;
                let date =
                    NaiveDate::parse_from_str(str.trim(), format).expect("Date should be valid");
                Ok(Self::new(
                    (date.and_time(NaiveTime::default()).timestamp() / 86_400) as i32,
                ))
            }
            DateFormat::DaysSinceEpoch => Ok(Self {
                days: i32::deserialize(deserializer)?,
            }),
        }
    }
}

serialize_without_context!(Date);
serialize_without_context!(Time);
deserialize_without_context!(Time);

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
            },
            TestStruct {
                field: 2,
                field_0: None,
                field_1: true,
                field_2: Timestamp::new(2000),
                field_3: Date::new(12),
                field_4: Time::new(1_000_000_000),
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

        vec![
            Arc::new(Int64Array::from(row0)),
            Arc::new(LargeStringArray::from(row1)),
            Arc::new(BooleanArray::from(row2)),
            Arc::new(TimestampMillisecondArray::from(row3)),
            Arc::new(Date32Array::from(row4)),
            Arc::new(Time64NanosecondArray::from(row5)),
        ]
    }
}

serialize_table_record!(TestStruct[6]{
    r#field["id"]: i64,
    r#field_0["name"]: Option<String>,
    r#field_1["b"]: bool,
    r#field_2["ts"]: Timestamp,
    r#field_3["dt"]: Date,
    r#field_4["t"]: Time
});

deserialize_table_record!(TestStruct["TestStruct", 6] {
    (r#field, "id", false, i64, None),
    (r#field_0, "name", false, Option<String>, Some(None)),
    (r#field_1, "b", false, bool, None),
    (r#field_2, "ts", false, Timestamp, None),
    (r#field_3, "dt", false, Date, None),
    (r#field_4, "t", false, Time, None)
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
        vec![(test_data[0].clone(), 2), (test_data[1].clone(), 1)],
    );

    let zset = Arc::new(<SerBatchImpl<_, TestStruct, ()>>::new(zset)) as Arc<dyn SerBatch>;
    encoder.encode(&[zset]).unwrap();

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
