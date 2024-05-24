use crate::catalog::InputCollectionHandle;
use crate::format::parquet::relation_to_arrow_fields;
use crate::format::parquet::test::load_parquet_file;
use crate::format::relation_to_parquet_schema;
use crate::integrated::delta_table::register_storage_handlers;
use crate::test::{
    file_to_zset, list_files_recursive, test_circuit, wait, DatabricksPeople, MockDeZSet,
    MockUpdate, TestStruct2,
};
use crate::{Controller, ControllerError, InputFormat};
use anyhow::anyhow;
use arrow::datatypes::Schema as ArrowSchema;
use chrono::{DateTime, NaiveDate};
use dbsp::typed_batch::TypedBatch;
use dbsp::utils::Tup2;
use dbsp::{DBData, OrdZSet, ZSet};
use deltalake::datafusion::dataframe::DataFrameWriteOptions;
use deltalake::datafusion::logical_expr::Literal;
use deltalake::datafusion::prelude::{col, SessionContext};
use deltalake::datafusion::sql::sqlparser::test_utils::table;
use deltalake::kernel::{BinaryOperator, DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder};
use env_logger::Env;
use log::{debug, info, trace};
use parquet::file::reader::Length;
use pipeline_types::config::PipelineConfig;
use pipeline_types::program_schema::{Field, Relation};
use pipeline_types::serde_with_context::serialize::SerializeWithContextWrapper;
use pipeline_types::serde_with_context::{
    DateFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig, TimeFormat,
    TimestampFormat,
};
use pipeline_types::transport::delta_table::DeltaTableIngestMode;
use proptest::collection::vec;
use proptest::prelude::{Arbitrary, ProptestConfig, Strategy};
use proptest::proptest;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_arrow::schema::SerdeArrowSchema;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Write};
use std::mem::forget;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::{tempdir, NamedTempFile, TempDir};
use tokio::time::sleep;
use uuid::Uuid;

fn delta_output_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        .with_date_format(DateFormat::String("%Y-%m-%d"))
        // DeltaLake only supports microsecond-based timestamp encoding, so we just
        // hardwire that for now.  See also `format/parquet/mod.rs`.
        .with_timestamp_format(TimestampFormat::MicrosSinceEpoch)
}

/// Read a snapshot of a delta table with records of type `T` to a temporary JSON file.
fn delta_table_snapshot_to_json<T>(
    table_uri: &str,
    config: &HashMap<String, String>,
) -> NamedTempFile
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    let start = Instant::now();
    let json_file = NamedTempFile::new().unwrap();
    println!(
        "delta_table_snapshot_to_json: writing output to {}",
        json_file.path().display()
    );

    let mut config = config.clone();
    config.insert("mode".to_string(), "snapshot".to_string());

    let input_pipeline = delta_table_input_pipeline::<T>(
        table_uri,
        &config,
        &json_file.path().display().to_string(),
    );
    input_pipeline.start();
    wait(|| input_pipeline.status().pipeline_complete(), 400_000).expect("timeout");
    input_pipeline.stop().unwrap();

    info!("Read delta snapshot in {:?}", start.elapsed());

    json_file
}

/// Wait until `table` contains exactly `expected_count` records.
async fn wait_for_count_records(
    table: &mut Arc<DeltaTable>,
    expected_count: usize,
    datafusion: &SessionContext,
) {
    let start = Instant::now();
    loop {
        // select count() output_table == len().
        Arc::get_mut(table)
            .unwrap()
            .update_incremental(None)
            .await
            .unwrap();

        let count = datafusion
            .read_table(table.clone())
            .unwrap()
            .count()
            .await
            .unwrap();

        info!("expected output table size: {expected_count}, current size: {count}");

        if count == expected_count {
            break;
        }

        if start.elapsed() > Duration::from_millis(20_000) {
            panic!("timeout");
        }

        sleep(Duration::from_millis(1_000)).await;
    }
}

/// Write `data` to `table`.
async fn write_data_to_table<T>(
    table: DeltaTable,
    arrow_schema: &ArrowSchema,
    data: &[T],
) -> DeltaTable
where
    T: DBData + SerializeWithContext<SqlSerdeConfig> + Sync,
{
    // Convert data to RecordBatch
    let batch = serde_arrow::to_record_batch(
        arrow_schema.fields(),
        &SerializeWithContextWrapper::new(&data.to_vec(), &delta_output_serde_config()),
    )
    .unwrap();

    // Write it to the input table.
    DeltaOps(table)
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

async fn create_table(
    table_uri: &str,
    storage_options: &HashMap<String, String>,
    fields: &[StructField],
) -> DeltaTable {
    info!("opening or creating delta table '{table_uri}'");

    let table = CreateBuilder::new()
        .with_location(table_uri)
        .with_save_mode(SaveMode::Ignore)
        .with_storage_options(storage_options.clone())
        .with_columns(fields.to_vec())
        .await
        .unwrap();
    info!("delta table '{table_uri}' successfully created");

    table
}

/// Build a pipeline that reads from a delta table and writes to a JSON file.
fn delta_table_input_pipeline<T>(
    table_uri: &str,
    config: &HashMap<String, String>,
    output_file_path: &str,
) -> Controller
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let mut storage_options = String::new();
    for (key, val) in config.iter() {
        storage_options += &format!("                {key}: \"{val}\"\n");
    }

    // Create controller.
    let config_str = format!(
        r#"
name: test
workers: 4
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: "{output_file_path}"
        format:
            name: json
            config:
                update_format: "insert_delete"
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: "delta_table_input"
            config:
                uri: "{table_uri}"
{}
"#,
        storage_options,
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    Controller::with_config(
        |workers| Ok(test_circuit::<T>(workers, &TestStruct2::schema())),
        &config,
        Box::new(move |e| panic!("delta_table_input_test: error: {e}")),
    )
    .unwrap()
}

/// Build a pipeline that reads from a delta table and writes to a delta table.
fn delta_to_delta_pipeline<T>(
    input_table_uri: &str,
    input_config: &HashMap<String, String>,
    output_table_uri: &str,
    output_config: &HashMap<String, String>,
    buffer_size: u64,
    buffer_timeout_ms: u64,
) -> Controller
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    info!("creating a pipeline to copy delta table '{input_table_uri}' to '{output_table_uri}'");

    let mut input_storage_options = String::new();
    for (key, val) in input_config.iter() {
        input_storage_options += &format!("                {key}: \"{val}\"\n");
    }

    let mut output_storage_options = String::new();
    for (key, val) in output_config.iter() {
        output_storage_options += &format!("                {key}: \"{val}\"\n");
    }

    // Create controller.
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: "delta_table_input"
            config:
                uri: "{input_table_uri}"
{}
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: "delta_table_output"
            config:
                uri: "{output_table_uri}"
{}
        enable_output_buffer: true
        max_output_buffer_size_records: {buffer_size}
        max_output_buffer_time_millis: {buffer_timeout_ms}
"#,
        input_storage_options, output_storage_options,
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    Controller::with_config(
        |workers| Ok(test_circuit::<T>(workers, &TestStruct2::schema())),
        &config,
        Box::new(move |e| panic!("delta_to_delta pipeline: error: {e}")),
    )
    .unwrap()
}

/// Test function that works for both local FS and remote object stores.
///
/// * `verify` - verify the final contents of the delta table is equivalent to
///   `data`.  Currently only works for tables in the local FS.
///
/// TODO: implement verification using the delta table API rather than
/// by reading parquet files directly.  I guess the best way to do this is
/// to build an input connector.
fn delta_table_output_test(
    data: Vec<TestStruct2>,
    table_uri: &str,
    object_store_config: &HashMap<String, String>,
    verify: bool,
) {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let buffer_size = 2000;

    let buffer_timeout_ms = 100;

    println!("delta_table_output_test: preparing input file");
    let mut input_file = NamedTempFile::new().unwrap();
    for v in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        v.serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        input_file
            .as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        input_file.write_all(b"\n").unwrap();
    }

    let mut storage_options = String::new();
    for (key, val) in object_store_config.iter() {
        storage_options += &format!("                {key}: \"{val}\"\n");
    }

    // Create controller.
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: "{}"
        format:
            name: json
            config:
                update_format: "raw"
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: "delta_table_output"
            config:
                uri: "{table_uri}"
                mode: "truncate"
{}
        enable_output_buffer: true
        max_output_buffer_size_records: {buffer_size}
        max_output_buffer_time_millis: {buffer_timeout_ms}
"#,
        input_file.path().display(),
        storage_options,
    );

    println!(
        "delta_table_output_test: {} records, input file: {}, table uri: {table_uri}",
        data.len(),
        input_file.path().display(),
    );
    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let controller = Controller::with_config(
        |workers| Ok(test_circuit::<TestStruct2>(workers, &TestStruct2::schema())),
        &config,
        Box::new(move |e| panic!("delta_table_output_test: error: {e}")),
    )
    .unwrap();

    controller.start();

    wait(|| controller.status().pipeline_complete(), 100_000).unwrap();

    if verify {
        let parquet_files =
            list_files_recursive(&Path::new(table_uri), OsStr::from_bytes(b"parquet")).unwrap();

        // // Uncomment to inspect the input JSON file.
        // std::mem::forget(input_file);

        let mut output_records = Vec::with_capacity(data.len());
        for parquet_file in parquet_files {
            let mut records: Vec<TestStruct2> = load_parquet_file(&parquet_file);
            output_records.append(&mut records);
        }

        output_records.sort();

        assert_eq!(output_records, data);
    }

    controller.stop().unwrap();
    println!("delta_table_output_test: success");
}

/// Read delta table in follow mode, write data to another delta table.
///
/// ```text
/// data --> input table in S3 ---> [pipeline] ---> output table in S3
/// ```
///
/// - When `snapshot` is `false`, runs the connector in `follow` mode,
///   consuming the dataset in 10 chunks.
/// - When `snapshot` is `true`, runs the connector in `snapshot_and_follow`
///   mode.  The dataset is split in halves; the first half is consumed as a
///   single snapshot and the second half is processed in follow mode.
async fn test_follow(
    schema: &[Field],
    input_table_uri: &str,
    output_table_uri: &str,
    storage_options: &HashMap<String, String>,
    data: Vec<TestStruct2>,
    snapshot: bool,
    buffer_size: u64,
    buffer_timeout_ms: u64,
) {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let datafusion = SessionContext::new();

    // Create arrow schema
    let arrow_fields = relation_to_arrow_fields(schema, true);
    info!("arrow_fields: {arrow_fields:?}");

    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));
    info!("arrow_schema: {arrow_schema:?}");

    let mut struct_fields: Vec<_> = vec![];

    for f in arrow_schema.fields.iter() {
        let data_type = DataType::try_from(f.data_type()).unwrap();
        struct_fields.push(StructField::new(f.name(), data_type, f.is_nullable()));
    }

    // Create delta table at `input_table_uri`.
    let mut input_table = create_table(input_table_uri, storage_options, &struct_fields).await;

    // If `snapshot` is true, write the first half of the dataset as initial snapshot.
    let split_at = if snapshot {
        let median = data.len() / 2;

        input_table = write_data_to_table(input_table, &arrow_schema, &data[..median]).await;

        median
    } else {
        0
    };

    // Start parquet-to-parquet pipeline.
    let mut input_config = storage_options.clone();

    if snapshot {
        input_config.insert("mode".to_string(), "snapshot_and_follow".to_string());
    } else {
        input_config.insert("mode".to_string(), "follow".to_string());
    }
    let output_config = storage_options.clone();

    let input_table_uri_clone = input_table_uri.to_string();
    let output_table_uri_clone = output_table_uri.to_string();

    let pipeline = tokio::task::spawn_blocking(move || {
        delta_to_delta_pipeline::<TestStruct2>(
            &input_table_uri_clone,
            &input_config,
            &output_table_uri_clone,
            &output_config,
            buffer_size,
            buffer_timeout_ms,
        )
    })
    .await
    .unwrap();

    pipeline.start();

    // Connect to `output_table_uri`.
    let mut output_table = Arc::new(
        DeltaTableBuilder::from_uri(&output_table_uri)
            .with_storage_options(storage_options.clone())
            .load()
            .await
            .unwrap(),
    );

    let mut total_count = split_at;

    // Write remaining data in 10 chunks, wait for it to show up in the output table.
    for chunk in data[split_at..].chunks(std::cmp::max(data[split_at..].len() / 10, 1)) {
        total_count += chunk.len();
        input_table = write_data_to_table(input_table, &arrow_schema, chunk).await;
        wait_for_count_records(&mut output_table, total_count, &datafusion).await;
    }

    // TODO: this does not currently work because our output delta connector doesn't support
    // deletions.
    // // Delete 10% of data, wait for the change to propagate.
    // for range in 0..10 {
    //     let cutoff = data.len() as i64 / (10 - range);
    //     data.retain(|x| x.field > cutoff);
    //
    //     // delete from output_table where ... ;
    //     (input_table, _) = DeltaOps(input_table)
    //         .delete()
    //         .with_predicate(col("id").lt_eq(cutoff.lit()))
    //         .await
    //         .unwrap();
    //
    //     let start = Instant::now();
    //
    //     loop {
    //         sleep(Duration::from_millis(1_000)).await;
    //
    //         Arc::get_mut(&mut output_table)
    //             .unwrap()
    //             .update_incremental(None)
    //             .await
    //             .unwrap();
    //
    //         let count = datafusion
    //             .read_table(output_table.clone())
    //             .unwrap()
    //             .count()
    //             .await
    //             .unwrap();
    //
    //         if count == total_count {
    //             break;
    //         }
    //
    //         if start.elapsed() > Duration::from_millis(20_000) {
    //             panic!("timeout");
    //         }
    //     }
    // }

    pipeline.stop().unwrap();
}

/// Generate up to `max_records` _unique_ records.
fn data(max_records: usize) -> impl Strategy<Value = Vec<TestStruct2>> {
    vec(TestStruct2::arbitrary(), 0..max_records).prop_map(|vec| {
        let mut idx = 0;
        vec.into_iter()
            .map(|mut x| {
                x.field = idx;
                idx += 1;
                x
            })
            .collect()
    })
}

async fn delta_table_follow_file_test_common(snapshot: bool) {
    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = data(100_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = TestStruct2::schema();

    let input_table_dir = TempDir::new().unwrap();
    let input_table_uri = input_table_dir.path().display().to_string();

    let output_table_dir = TempDir::new().unwrap();
    let output_table_uri = output_table_dir.path().display().to_string();

    test_follow(
        &relation_schema,
        &input_table_uri,
        &output_table_uri,
        &HashMap::new(),
        data,
        snapshot,
        1000,
        100,
    )
    .await;
}

#[tokio::test]
async fn delta_table_snapshot_and_follow_file_test() {
    delta_table_follow_file_test_common(true).await
}

#[tokio::test]
async fn delta_table_follow_file_test() {
    delta_table_follow_file_test_common(false).await
}

#[cfg(feature = "delta-s3-test")]
async fn delta_table_follow_s3_test_common(snapshot: bool) {
    register_storage_handlers();

    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = data(100_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = TestStruct2::schema();

    let input_uuid = uuid::Uuid::new_v4();
    let output_uuid = uuid::Uuid::new_v4();

    let object_store_config = [
        (
            "aws_access_key_id".to_string(),
            std::env::var("DELTA_TABLE_TEST_AWS_ACCESS_KEY_ID").unwrap(),
        ),
        (
            "aws_secret_access_key".to_string(),
            std::env::var("DELTA_TABLE_TEST_AWS_SECRET_ACCESS_KEY").unwrap(),
        ),
        // AWS region must be specified (see https://github.com/delta-io/delta-rs/issues/1095).
        ("aws_region".to_string(), "us-east-2".to_string()),
        ("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string()),
    ]
    .into_iter()
    .collect::<HashMap<_, _>>();

    let input_table_uri = format!("s3://feldera-delta-table-test/{input_uuid}/");
    let output_table_uri = format!("s3://feldera-delta-table-test/{output_uuid}/");

    test_follow(
        &relation_schema,
        &input_table_uri,
        &output_table_uri,
        &object_store_config,
        data,
        snapshot,
        1000,
        100,
    )
    .await;
}

#[cfg(feature = "delta-s3-test")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_follow_s3_test() {
    delta_table_follow_s3_test_common(false).await
}

#[cfg(feature = "delta-s3-test")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_snapshot_and_follow_s3_test() {
    delta_table_follow_s3_test_common(true).await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]

    /// ```text
    /// input.json --> [pipeline1]--->delta_table-->[pipeline2]-->output.json
    /// ```
    #[test]
    fn delta_table_file_output_proptest(data in data(100_000))
    {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        delta_table_output_test(data.clone(), &table_uri, &HashMap::new(), true);

        // // Uncomment to inspect output parquet files produced by the test.
        // forget(table_dir);

        // Read delta table unordered.
        let mut json_file = delta_table_snapshot_to_json::<TestStruct2>(
            &table_uri,
            &HashMap::new());

        let expected_zset = OrdZSet::from_tuples((), data.clone().into_iter().map(|x| Tup2(Tup2(x,()),1)).collect());
        let zset = file_to_zset::<TestStruct2>(json_file.as_file_mut(), "json", r#"update_format: "insert_delete""#);
        assert_eq!(zset, expected_zset);

        // Order delta table by `id` (which should be its natural order).
        let mut json_file_ordered_by_id = delta_table_snapshot_to_json::<TestStruct2>(
            &table_uri,
            &HashMap::from([("timestamp_column".to_string(), "id".to_string())]));

        let zset = file_to_zset::<TestStruct2>(json_file_ordered_by_id.as_file_mut(), "json", r#"update_format: "insert_delete""#);
        assert_eq!(zset, expected_zset);

        // Order delta table by `timestamp` (which is generated in random order, so this requires actual sorting).
        let mut json_file_ordered_by_ts = delta_table_snapshot_to_json::<TestStruct2>(
            &table_uri,
            &HashMap::from([("timestamp_column".to_string(), "ts".to_string())]));

        let zset = file_to_zset::<TestStruct2>(json_file_ordered_by_ts.as_file_mut(), "json", r#"update_format: "insert_delete""#);
        assert_eq!(zset, expected_zset);

        // Filter delta table by id
        let mut json_file_filtered_by_id = delta_table_snapshot_to_json::<TestStruct2>(
            &table_uri,
            &HashMap::from([("snapshot_filter".to_string(), "id >= 10000 and id <= 20000".to_string())]));

        let expected_filtered_zset = OrdZSet::from_tuples(
            (),
            data.clone().into_iter()
                .filter(|x| x.field >= 10000 && x.field <= 20000)
                .map(|x| Tup2(Tup2(x,()),1)).collect()
            );

        let zset = file_to_zset::<TestStruct2>(json_file_filtered_by_id.as_file_mut(), "json", r#"update_format: "insert_delete""#);
        assert_eq!(zset, expected_filtered_zset);

        // Filter delta table by timestamp.
        let mut json_file_filtered_by_ts = delta_table_snapshot_to_json::<TestStruct2>(
            &table_uri,
            &HashMap::from([("snapshot_filter".to_string(), "ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'".to_string())]));

        let start = NaiveDate::from_ymd_opt(2005, 1, 1)
                .unwrap()
                .and_hms_milli_opt(0, 0, 0, 0)
                .unwrap();

        let end = NaiveDate::from_ymd_opt(2010, 12, 31)
                .unwrap()
                .and_hms_milli_opt(23, 59, 59, 0)
                .unwrap();

        let expected_filtered_zset = OrdZSet::from_tuples(
            (),
            data.into_iter()
                .filter(|x| x.field_2.milliseconds() >= start.timestamp_millis() && x.field_2.milliseconds() <= end.timestamp_millis())
                .map(|x| Tup2(Tup2(x,()),1)).collect()
            );

        let zset = file_to_zset::<TestStruct2>(json_file_filtered_by_ts.as_file_mut(), "json", r#"update_format: "insert_delete""#);
        assert_eq!(zset, expected_filtered_zset);


        // // Uncomment to inspect one of the output json files produced by the test.
        // forget(json_file_filtered_by_id);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1))]

    /// Write to a Delta table in S3.
    #[cfg(feature = "delta-s3-test")]
    #[test]
    fn delta_table_s3_output_proptest(data in data(100_000))
    {
        let uuid = uuid::Uuid::new_v4();
        let object_store_config = [
            ("aws_access_key_id".to_string(), std::env::var("DELTA_TABLE_TEST_AWS_ACCESS_KEY_ID").unwrap()),
            ("aws_secret_access_key".to_string(), std::env::var("DELTA_TABLE_TEST_AWS_SECRET_ACCESS_KEY").unwrap()),
            // AWS region must be specified (see https://github.com/delta-io/delta-rs/issues/1095).
            ("aws_region".to_string(), "us-east-2".to_string()),
        ]
            .into_iter()
            .collect::<HashMap<_,_>>();

        let table_uri = format!("s3://feldera-delta-table-test/{uuid}/");
        // TODO: enable verification when it's supported for S3.
        delta_table_output_test(data.clone(), &table_uri, &object_store_config, false);
        //delta_table_output_test(data.clone(), &table_uri, &object_store_config, false);

        let mut json_file = delta_table_snapshot_to_json::<TestStruct2>(
            &table_uri,
            &object_store_config);

        let expected_zset = OrdZSet::from_tuples((), data.into_iter().map(|x| Tup2(Tup2(x,()),1)).collect());
        let zset = file_to_zset::<TestStruct2>(json_file.as_file_mut(), "json", r#"update_format: "insert_delete""#);
        assert_eq!(zset, expected_zset);
    }
}

/// Read a large (2M records) dataset created in S3 from Databricks.
/// This dataset was derived from
/// `/databricks-datasets/learning-spark-v2/people/people-10m.delta`; however the full dataset
/// is 200MB and can be slow to download, so we cut it down to 2M.
///
/// Here is the full notebook used to create it:
/// ```text
/// # Load the data from its source.
/// df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")
///
/// # Write the data to a table.
/// table_name = "people_2m"
/// df.limit(2000000).write.saveAsTable(table_name)
///
/// # Find location of the delta table in S3.
/// table_location = spark.sql("DESCRIBE DETAIL people_2m").collect()[0]['location']
/// print(table_location)
/// ```
#[cfg(feature = "delta-s3-test")]
#[test]
fn delta_table_s3_people_2m() {
    let uuid = uuid::Uuid::new_v4();
    let object_store_config = [
        (
            "aws_access_key_id".to_string(),
            std::env::var("DELTA_TABLE_TEST_AWS_ACCESS_KEY_ID").unwrap(),
        ),
        (
            "aws_secret_access_key".to_string(),
            std::env::var("DELTA_TABLE_TEST_AWS_SECRET_ACCESS_KEY").unwrap(),
        ),
        // AWS region must be specified (see https://github.com/delta-io/delta-rs/issues/1095).
        ("aws_region".to_string(), "us-west-1".to_string()),
        // Set long timeout for reading large files from S3.
        ("timeout".to_string(), "1000 secs".to_string()),
    ]
    .into_iter()
    .collect::<HashMap<_, _>>();

    //let table_uri = "s3://databricks-workspace-stack-d437e-bucket/unity-catalog/5496131495366467/__unitystorage/catalogs/d1e3a643-f243-4798-8554-d32bf5a7205a/tables/cee86cb4-a525-41b9-82fe-616ae62287fc/";
    let table_uri = "s3://databricks-workspace-stack-d437e-bucket/unity-catalog/5496131495366467/__unitystorage/catalogs/d1e3a643-f243-4798-8554-d32bf5a7205a/tables/90cc5aba-25cd-4ed7-a498-8d08f0ddaa21/";
    let mut json_file =
        delta_table_snapshot_to_json::<DatabricksPeople>(table_uri, &object_store_config);

    println!("reading output file");
    let zset = file_to_zset::<DatabricksPeople>(
        json_file.as_file_mut(),
        "json",
        r#"update_format: "insert_delete""#,
    );

    assert_eq!(zset.len(), 2_000_000);

    forget(json_file);
}
