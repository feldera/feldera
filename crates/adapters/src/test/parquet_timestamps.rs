//! Parquet and Delta fixtures covering the physical encodings Delta writers use
//! for timestamps.
//!
//! Delta pins the logical `timestamp` type at microsecond precision but leaves
//! the physical encoding to the writer, so one table's files can mix Spark's
//! deprecated INT96 with INT64 microseconds. See
//! [`crate::format::parquet::INT96_TIME_UNIT`] for why the two differ.
//!
//! The fixtures use Parquet's low-level writer rather than `ArrowWriter`, which
//! cannot emit INT96 and which records an `ARROW:schema` key that would steer
//! the reader away from its default INT96 mapping, the mapping these fixtures
//! exist to exercise.

use std::fs::{File, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parquet::data_type::{Int64Type, Int96, Int96Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use serde_json::json;
use uuid::Uuid;

use crate::test::TimestampTestStruct;
use feldera_sqllib::Timestamp;

/// Julian day number of 1970-01-01, the epoch INT96 counts days from.
const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;

const SECONDS_PER_DAY: i64 = 86_400;

/// Delta schema of the fixtures. `timestamp` is Delta's microsecond-precision,
/// UTC-adjusted type, whichever physical encoding a file happens to use.
const DELTA_SCHEMA: &str = r#"{"type":"struct","fields":[
    {"name":"id","type":"long","nullable":false,"metadata":{}},
    {"name":"ts","type":"timestamp","nullable":true,"metadata":{}}]}"#;

/// Physical Parquet encoding of a fixture's `ts` column.
#[derive(Clone, Copy, Debug)]
pub enum TimestampEncoding {
    /// Spark's deprecated INT96, still written by Databricks and Photon.
    Int96,
    /// INT64 microseconds since the epoch, what delta-rs and Spark's
    /// `TIMESTAMP_MICROS` setting write.
    Int64Micros,
}

impl TimestampEncoding {
    /// Physical schema, spelled the way Spark spells it.
    fn message_type(self) -> &'static str {
        match self {
            Self::Int96 => {
                "message spark_schema {
                    REQUIRED INT64 id;
                    OPTIONAL INT96 ts;
                }"
            }
            Self::Int64Micros => {
                "message spark_schema {
                    REQUIRED INT64 id;
                    OPTIONAL INT64 ts (TIMESTAMP(MICROS,true));
                }"
            }
        }
    }
}

/// Encode `timestamp` plus `sub_micro_nanos` as Parquet does INT96:
/// nanoseconds within the day in the low 8 bytes, Julian day number in the
/// high 4.
fn to_int96(timestamp: Timestamp, sub_micro_nanos: u32) -> Int96 {
    let date_time = timestamp.to_dateTime();
    let seconds = date_time.timestamp();
    // Euclidean division keeps the nanosecond part non-negative for dates
    // before the epoch, which is what Parquet readers expect.
    let day = seconds.div_euclid(SECONDS_PER_DAY) + JULIAN_DAY_OF_EPOCH;
    let nanos = seconds.rem_euclid(SECONDS_PER_DAY) * 1_000_000_000
        + i64::from(date_time.timestamp_subsec_nanos())
        + i64::from(sub_micro_nanos);

    let mut value = Int96::new();
    value.set_data(nanos as u32, (nanos >> 32) as u32, day as u32);
    value
}

/// Write `rows` to `path`, storing `ts` in `encoding`.
pub fn write_timestamp_parquet(
    path: &Path,
    rows: &[TimestampTestStruct],
    encoding: TimestampEncoding,
) {
    let schema = Arc::new(parse_message_type(encoding.message_type()).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(File::create(path).unwrap(), schema, props).unwrap();
    let mut row_group = writer.next_row_group().unwrap();

    let ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    column.close().unwrap();

    // A definition level of 1 marks an optional value as present, 0 as null,
    // so nulls contribute a level but no value.
    let definition_levels: Vec<i16> = rows.iter().map(|row| i16::from(row.ts.is_some())).collect();
    let present = rows.iter().filter_map(|row| row.ts);
    let mut column = row_group.next_column().unwrap().unwrap();
    match encoding {
        TimestampEncoding::Int96 => {
            let values: Vec<Int96> = present.map(|ts| to_int96(ts, 0)).collect();
            column
                .typed::<Int96Type>()
                .write_batch(&values, Some(&definition_levels), None)
                .unwrap();
        }
        TimestampEncoding::Int64Micros => {
            let values: Vec<i64> = present.map(|ts| ts.microseconds()).collect();
            column
                .typed::<Int64Type>()
                .write_batch(&values, Some(&definition_levels), None)
                .unwrap();
        }
    }
    column.close().unwrap();

    row_group.close().unwrap();
    writer.close().unwrap();
}

/// Write a single INT96 column holding each `ts` plus `sub_micro_nanos`.
///
/// Keeping every `sub_micro_nanos` under 1000 makes the reader's rounding mode
/// observable: truncating returns `ts` unchanged, rounding returns the next
/// microsecond.
pub fn write_sub_microsecond_int96_parquet(path: &Path, rows: &[(Timestamp, u32)]) {
    const MESSAGE_TYPE: &str = "message spark_schema { REQUIRED INT96 ts; }";

    let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(File::create(path).unwrap(), schema, props).unwrap();
    let mut row_group = writer.next_row_group().unwrap();

    let values: Vec<Int96> = rows
        .iter()
        .map(|(ts, sub_micro_nanos)| {
            assert!(
                *sub_micro_nanos < 1_000,
                "remainder must be sub-microsecond"
            );
            to_int96(*ts, *sub_micro_nanos)
        })
        .collect();
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int96Type>()
        .write_batch(&values, None, None)
        .unwrap();
    column.close().unwrap();

    row_group.close().unwrap();
    writer.close().unwrap();
}

/// Write `rows` with the timestamp in both encodings side by side, the shape a
/// Spark job leaves when it rewrites some timestamp columns but not others.
pub fn write_mixed_column_parquet(path: &Path, rows: &[TimestampTestStruct]) {
    const MESSAGE_TYPE: &str = "
        message spark_schema {
            REQUIRED INT64 id;
            OPTIONAL INT96 ts_int96;
            OPTIONAL INT64 ts_micros (TIMESTAMP(MICROS,true));
        }
    ";

    let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(File::create(path).unwrap(), schema, props).unwrap();
    let mut row_group = writer.next_row_group().unwrap();

    let ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    column.close().unwrap();

    let definition_levels: Vec<i16> = rows.iter().map(|row| i16::from(row.ts.is_some())).collect();

    let int96: Vec<Int96> = rows
        .iter()
        .filter_map(|row| row.ts)
        .map(|ts| to_int96(ts, 0))
        .collect();
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int96Type>()
        .write_batch(&int96, Some(&definition_levels), None)
        .unwrap();
    column.close().unwrap();

    let micros: Vec<i64> = rows
        .iter()
        .filter_map(|row| row.ts)
        .map(|ts| ts.microseconds())
        .collect();
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&micros, Some(&definition_levels), None)
        .unwrap();
    column.close().unwrap();

    row_group.close().unwrap();
    writer.close().unwrap();
}

/// Write `rows` with the timestamp nested inside a struct and inside a list,
/// both as INT96.
///
/// Rows without a timestamp are skipped and every row gets exactly one list
/// element, which keeps the definition and repetition levels uniform; nesting,
/// not nullability, is what this fixture is for.
pub fn write_nested_int96_parquet(path: &Path, rows: &[TimestampTestStruct]) {
    const MESSAGE_TYPE: &str = "
        message spark_schema {
            REQUIRED INT64 id;
            OPTIONAL group nested {
                OPTIONAL INT96 ts;
            }
            OPTIONAL group ts_list (LIST) {
                REPEATED group list {
                    OPTIONAL INT96 element;
                }
            }
        }
    ";

    let rows: Vec<&TimestampTestStruct> = rows.iter().filter(|row| row.ts.is_some()).collect();
    let timestamps: Vec<Int96> = rows
        .iter()
        .map(|row| to_int96(row.ts.unwrap(), 0))
        .collect();

    let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(File::create(path).unwrap(), schema, props).unwrap();
    let mut row_group = writer.next_row_group().unwrap();

    let ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    column.close().unwrap();

    // `nested` and `nested.ts` are both optional, so a value present at both
    // levels is definition level 2. Nothing along the path repeats.
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int96Type>()
        .write_batch(&timestamps, Some(&vec![2; rows.len()]), None)
        .unwrap();
    column.close().unwrap();

    // `ts_list`, its repeated `list` group and `element` give definition level
    // 3; repetition level 0 starts a new list, so one element per row.
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int96Type>()
        .write_batch(
            &timestamps,
            Some(&vec![3; rows.len()]),
            Some(&vec![0; rows.len()]),
        )
        .unwrap();
    column.close().unwrap();

    row_group.close().unwrap();
    writer.close().unwrap();
}

/// Write one data file into `table_dir` and return its `add` action.
fn add_data_file(
    table_dir: &Path,
    rows: &[TimestampTestStruct],
    encoding: TimestampEncoding,
) -> serde_json::Value {
    let name = format!("part-00000-{}.parquet", Uuid::new_v4());
    let path = PathBuf::from(table_dir).join(&name);
    write_timestamp_parquet(&path, rows, encoding);

    json!({"add": {
        "path": name,
        "partitionValues": {},
        "size": path.metadata().unwrap().len(),
        "modificationTime": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        "dataChange": true,
    }})
}

/// Commit `actions` as Delta table version `version`.
///
/// Staged elsewhere and renamed in, the way real Delta writers commit. Written
/// in place the file is published empty and filled over dozens of `write`
/// syscalls, so a connector polling the log can read an empty commit, whose
/// rows are then never ingested, or a truncated one, which fails to parse.
///
/// The staging path sits in the table directory rather than `_delta_log`, so
/// the log listing never has to classify it, and on the same filesystem, so
/// the rename is atomic.
fn commit(table_dir: &Path, version: u64, actions: &[serde_json::Value]) {
    let name = format!("{version:020}.json");
    let staged = table_dir.join(format!(".staged-{name}"));

    let mut file = File::create(&staged).unwrap();
    for action in actions {
        writeln!(file, "{action}").unwrap();
    }
    drop(file);

    std::fs::rename(staged, table_dir.join("_delta_log").join(name)).unwrap();
}

/// Create a Delta table at `table_dir` holding `rows` as version 0.
///
/// The log is written by hand because delta-rs writes exclusively through
/// arrow-rs's `ArrowWriter`, which cannot emit INT96.
pub fn create_delta_table(
    table_dir: &Path,
    rows: &[TimestampTestStruct],
    encoding: TimestampEncoding,
) {
    create_dir_all(table_dir.join("_delta_log")).unwrap();
    commit(
        table_dir,
        0,
        &[
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({"metaData": {
                "id": Uuid::new_v4().to_string(),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": DELTA_SCHEMA,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0,
            }}),
            add_data_file(table_dir, rows, encoding),
        ],
    );
}

/// Append `rows` to the Delta table at `table_dir` as `version`.
pub fn append_delta_version(
    table_dir: &Path,
    version: u64,
    rows: &[TimestampTestStruct],
    encoding: TimestampEncoding,
) {
    commit(
        table_dir,
        version,
        &[add_data_file(table_dir, rows, encoding)],
    );
}

/// Rows spanning the INT96 range: one inside the nanosecond window, one past
/// its 2262-04-11 upper bound, one before its 1677-09-21 lower bound, and a
/// null. The far-future value is the one a customer hit in a Databricks-written
/// table; decoded as nanoseconds it wraps to 2247-05-04T01:16:18.871345.
pub fn timestamp_test_data() -> Vec<TimestampTestStruct> {
    fn at(rfc3339: &str) -> Option<Timestamp> {
        Some(Timestamp::from_dateTime(
            chrono::DateTime::parse_from_rfc3339(rfc3339)
                .unwrap()
                .to_utc(),
        ))
    }

    vec![
        TimestampTestStruct {
            id: 1,
            ts: at("2024-06-01T12:00:00Z"),
        },
        TimestampTestStruct {
            id: 2,
            ts: at("4000-12-31T00:00:00Z"),
        },
        TimestampTestStruct {
            id: 3,
            ts: at("1600-01-01T00:00:00Z"),
        },
        TimestampTestStruct { id: 4, ts: None },
    ]
}
