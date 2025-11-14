use crate::format::parquet::relation_to_arrow_fields;
use crate::format::parquet::test::load_parquet_file;
use crate::integrated::delta_table::delta_input_serde_config;
use crate::test::data::DeltaTestKey;
use crate::test::{
    file_to_zset, list_files_recursive, test_circuit, test_circuit_with_index, wait,
    DeltaTestStruct,
};
use crate::Controller;
use arrow::datatypes::Schema as ArrowSchema;
use chrono::NaiveDate;
#[cfg(feature = "delta-s3-test")]
use dbsp::typed_batch::DynBatchReader;
use dbsp::utils::Tup2;
use dbsp::{DBData, OrdZSet};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder};
use feldera_adapterlib::utils::datafusion::execute_query_collect;
use feldera_types::config::PipelineConfig;
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::{Field, SqlIdentifier};
use feldera_types::serde_with_context::serde_config::DecimalFormat;
use feldera_types::serde_with_context::serialize::SerializeWithContextWrapper;
use feldera_types::serde_with_context::{
    DateFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig, TimestampFormat,
};
use feldera_types::transport::delta_table::DeltaTableTransactionMode;
use proptest::collection::vec;
use proptest::prelude::{Arbitrary, ProptestConfig, Strategy};
use proptest::proptest;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::{json, Value};
#[cfg(feature = "delta-s3-test")]
use serial_test::{parallel, serial};
use std::cmp::min;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Write;
use std::mem::forget;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tracing::{debug, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

fn delta_output_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        .with_date_format(DateFormat::String("%Y-%m-%d"))
        .with_decimal_format(DecimalFormat::String)
        // DeltaLake only supports microsecond-based timestamp encoding, so we just
        // hardwire that for now.  See also `format/parquet.rs`.
        .with_timestamp_format(TimestampFormat::MicrosSinceEpoch)
}

/// Read a snapshot of a delta table with records of type `T` to a temporary JSON file.
fn delta_table_snapshot_to_json<T>(
    table_uri: &str,
    schema: &[Field],
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
        schema,
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
async fn wait_for_output_records<T>(
    table: &mut Arc<DeltaTable>,
    expected_output: &[T],
    datafusion: &SessionContext,
    timeout_ms: u64,
) where
    T: for<'a> DeserializeWithContext<'a, SqlSerdeConfig> + DBData,
{
    let start = Instant::now();
    loop {
        // select count() output_table == len().
        Arc::get_mut(table)
            .unwrap()
            .update_incremental(None)
            .await
            .unwrap();

        let data = datafusion
            .read_table(table.clone())
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut result = Vec::new();

        for batch in data.iter() {
            let deserializer = serde_arrow::Deserializer::from_record_batch(batch).unwrap();
            let mut records =
                Vec::<T>::deserialize_with_context(deserializer, &delta_input_serde_config())
                    .unwrap();

            result.append(&mut records);
        }

        info!(
            "expected output table size: {}, current size: {}",
            expected_output.len(),
            result.len()
        );

        if result.len() == expected_output.len() {
            result.sort();
            let mut expected_output = expected_output.to_vec();
            expected_output.sort();
            assert_eq!(result, expected_output);
            break;
        }

        if timeout_ms == 0 {
            panic!("delta table contents does not match expected output");
        }

        if start.elapsed() >= Duration::from_millis(timeout_ms) {
            panic!("timeout");
        }

        sleep(Duration::from_millis(1_000)).await;
    }
}

/// Wait until materialized table contains exactly `expected_count` records.
async fn wait_for_records_materialized<T>(
    pipeline: &Controller,
    table: &SqlIdentifier,
    expected_output: &[T],
) where
    T: for<'a> DeserializeWithContext<'a, SqlSerdeConfig> + DBData,
{
    let start = Instant::now();
    loop {
        let data = execute_query_collect(
            &pipeline.session_context().unwrap(),
            &format!("select * from {table}"),
        )
        .await
        .unwrap();

        let mut result = Vec::new();

        for batch in data.iter() {
            let deserializer = serde_arrow::Deserializer::from_record_batch(batch).unwrap();
            let mut records =
                Vec::<T>::deserialize_with_context(deserializer, &delta_input_serde_config())
                    .unwrap();

            result.append(&mut records);
        }

        info!(
            "expected output table size: {}, current size: {}",
            expected_output.len(),
            result.len()
        );

        if result.len() == expected_output.len() {
            result.sort();
            let mut expected_output = expected_output.to_vec();
            expected_output.sort();
            if result == expected_output {
                break;
            }
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
    schema: &[Field],
    config: &HashMap<String, String>,
    output_file_path: &str,
) -> Controller
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    init_logging();

    let mut storage_options = config.clone();
    storage_options.insert("uri".into(), table_uri.into());

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": output_file_path
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete"
                    }
                }
            }
        },
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "delta_table_input",
                    "config": storage_options,
                }
            }
        }
    }))
    .unwrap();
    let schema = schema.to_vec();

    Controller::with_test_config(
        move |workers| Ok(test_circuit::<T>(workers, &schema, &[None])),
        &config,
        Box::new(move |e, _| {
            panic!("delta_table_input_test: error: {e}");
        }),
    )
    .unwrap()
}

/// Build a pipeline that reads from a delta table and writes to a delta table.
#[allow(clippy::too_many_arguments)]
fn delta_to_delta_pipeline<T>(
    input_table_uri: &str,
    skip_unused_columns: bool,
    input_config: &HashMap<String, Value>,
    output_table_uri: &str,
    output_config: &HashMap<String, String>,
    buffer_size: u64,
    buffer_timeout_ms: u64,
    storage_dir: &Path,
    paused: bool,
) -> Controller
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    info!("creating a pipeline to copy delta table '{input_table_uri}' to '{output_table_uri}'");

    let mut input_storage_options = input_config.clone();
    input_storage_options.insert("uri".into(), input_table_uri.into());
    input_storage_options.insert("skip_unused_columns".into(), skip_unused_columns.into());

    let mut output_storage_options = output_config.clone();
    output_storage_options.insert("uri".into(), output_table_uri.into());

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "inputs": {
            "test_input1": {
                "paused": paused,
                "stream": "test_input1",
                "transport": {
                    "name": "delta_table_input",
                    "config": input_storage_options,
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "delta_table_output",
                    "config": output_storage_options,
                },
                "enable_output_buffer": true,
                "max_output_buffer_size_records": buffer_size,
                "max_output_buffer_time_millis": buffer_timeout_ms,
            }
        }
    }))
    .unwrap();

    Controller::with_test_config(
        |workers| {
            Ok(test_circuit::<T>(
                workers,
                &DeltaTestStruct::schema(),
                &[Some("output")],
            ))
        },
        &config,
        Box::new(move |e, _| panic!("delta_to_delta pipeline: error: {e}")),
    )
    .unwrap()
}

/// Build a pipeline that reads from a delta table.
fn delta_read_pipeline<T, K, KF>(
    input_table_uri: &str,
    input_config: &HashMap<String, String>,
    key_fields: &[SqlIdentifier],
    key_func: KF,
    storage_dir: &Path,
) -> Controller
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
    K: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
    KF: Fn(&T) -> K + Clone + Send + Sync + 'static,
{
    info!("creating a pipeline to read delta table '{input_table_uri}'");

    let mut input_storage_options = input_config.clone();
    input_storage_options.insert("uri".into(), input_table_uri.into());

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "delta_table_input",
                    "config": input_storage_options,
                }
            }
        }
    }))
    .unwrap();

    let key_fields = key_fields.to_vec();

    Controller::with_test_config(
        move |workers| {
            Ok(test_circuit_with_index::<T, K, _>(
                workers,
                &DeltaTestStruct::schema(),
                &key_fields,
                key_func,
                &[Some("output")],
            ))
        },
        &config,
        Box::new(move |e, _| panic!("delta_read pipeline: error: {e}")),
    )
    .unwrap()
}

/// Build a pipeline that continuously follows a JSON file and writes changes
/// (both inserts and deletes) to a delta table.
fn delta_write_pipeline<T, K, KF>(
    input_file_path: &str,
    table_uri: &str,
    key_fields: &[SqlIdentifier],
    key_func: KF,
    index: bool,
    config: &HashMap<String, String>,
) -> Controller
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
    K: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
    KF: Fn(&T) -> K + Clone + Send + Sync + 'static,
{
    info!("creating a pipeline to write delta table '{table_uri}'");

    let mut storage_options = config.clone();
    storage_options.insert("uri".into(), table_uri.into());
    if index {
        storage_options.insert("index".into(), "idx1".into());
    }

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "test_intput1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": input_file_path,
                        "follow": true
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete"
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "enable_output_buffer": true,
                "max_output_buffer_time_millis": 1000,
                "transport": {
                    "name": "delta_table_output",
                    "config": storage_options,
                }
            }
        }
    }))
    .unwrap();
    let key_fields = key_fields.to_vec();

    Controller::with_test_config(
        move |workers| {
            Ok(test_circuit_with_index::<T, K, _>(
                workers,
                &DeltaTestStruct::schema(),
                &key_fields,
                key_func,
                &[Some("output")],
            ))
        },
        &config,
        Box::new(move |e, _| panic!("delta_write pipeline: error: {e}")),
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
    data: Vec<DeltaTestStruct>,
    table_uri: &str,
    object_store_config: &HashMap<String, String>,
    verify: bool,
) {
    init_logging();

    let buffer_size = 2000;

    let buffer_timeout_ms = 100;

    println!("delta_table_output_test: preparing input file");
    let mut input_file = NamedTempFile::new().unwrap();
    for v in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        v.serialize_with_context(
            &mut serializer,
            &SqlSerdeConfig::from(JsonFlavor::default()),
        )
        .unwrap();
        input_file
            .as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        input_file.write_all(b"\n").unwrap();
    }

    let mut storage_options = object_store_config.clone();
    storage_options.insert("uri".into(), table_uri.into());
    storage_options.insert("mode".into(), "truncate".into());

    println!(
        "delta_table_output_test: {} records, input file: {}, table uri: {table_uri}",
        data.len(),
        input_file.path().display(),
    );
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": input_file.path()
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw"
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "delta_table_output",
                    "config": storage_options,
                },
                "enable_output_buffer": true,
                "max_output_buffer_size_records": buffer_size,
                "max_output_buffer_time_millis": buffer_timeout_ms
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |workers| {
            Ok(test_circuit::<DeltaTestStruct>(
                workers,
                &DeltaTestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(move |e, _| panic!("delta_table_output_test: error: {e}")),
    )
    .unwrap();

    controller.start();

    wait(|| controller.status().pipeline_complete(), 100_000).unwrap();

    if verify {
        let parquet_files =
            list_files_recursive(Path::new(table_uri), OsStr::from_bytes(b"parquet")).unwrap();

        // // Uncomment to inspect the input JSON file.
        // std::mem::forget(input_file);

        let mut output_records = Vec::with_capacity(data.len());
        for parquet_file in parquet_files {
            let mut records: Vec<DeltaTestStruct> = load_parquet_file(&parquet_file);
            output_records.append(&mut records);
        }

        output_records.sort();

        if output_records != data {
            for i in 0..min(output_records.len(), data.len()) {
                if output_records[i] != data[i] {
                    println!("-{i}: {:?}", data[i]);
                    println!("+{i}: {:?}", output_records[i]);
                }
            }
            panic!("{} vs {}", output_records.len(), data.len());
        }
    }

    controller.stop().unwrap();
    println!("delta_table_output_test: success");
}

fn init_logging() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .try_init();
}

/// Read delta table in follow mode, write data to another delta table.
///
/// ```text
/// data --> input table in S3 ---> [pipeline] ---> output table in S3
/// ```
///
/// - `snapshot` flag
///   - When `snapshot` is `false`, runs the connector in `follow` mode,
///     consuming the dataset in 10 chunks.
///   - When `snapshot` is `true`, runs the connector in `snapshot_and_follow`
///     mode.  The dataset is split in halves; the first half is consumed as a
///     single snapshot and the second half is processed in follow mode.
/// - `suspend` flags: when `true`, suspends and resumes the pipeline after
///   every input chunk.
/// - `test_end_version`: when `true`, test that the connector respects the `end_version`
///   config property.
#[allow(clippy::too_many_arguments)]
async fn test_follow(
    schema: &[Field],
    input_table_uri: &str,
    output_table_uri: &str,
    storage_options: &HashMap<String, String>,
    data: Vec<DeltaTestStruct>,
    snapshot: bool,
    transaction_mode: DeltaTableTransactionMode,
    suspend: bool,
    test_end_version: bool,
    buffer_size: u64,
    buffer_timeout_ms: u64,
) {
    async fn suspend_pipeline(pipeline: Controller) {
        println!("start suspend");
        let (sender, mut receiver) = mpsc::channel(1);
        pipeline.start_suspend(Box::new(move |result| sender.try_send(result).unwrap()));

        timeout(Duration::from_secs(100), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        println!("pipeline suspended");

        pipeline.stop().unwrap();
        println!("pipeline stopped");
    }

    async fn start_pipeline(
        input_table_uri: &str,
        output_table_uri: &str,
        input_config: &HashMap<String, Value>,
        output_config: &HashMap<String, String>,
        storage_dir: &TempDir,
        buffer_size: u64,
        buffer_timeout_ms: u64,
    ) -> Controller {
        let input_table_uri_clone = input_table_uri.to_string();
        let output_table_uri_clone = output_table_uri.to_string();
        let input_config_clone = input_config.clone();
        let output_config_clone = output_config.clone();
        let storage_dir_path = storage_dir.path().to_path_buf();
        let pipeline = tokio::task::spawn_blocking(move || {
            delta_to_delta_pipeline::<DeltaTestStruct>(
                &input_table_uri_clone,
                true,
                &input_config_clone,
                &output_table_uri_clone,
                &output_config_clone,
                buffer_size,
                buffer_timeout_ms,
                &storage_dir_path,
                true,
            )
        })
        .await
        .unwrap();

        println!("Pipeline created");

        pipeline.start();
        pipeline
    }

    fn completed_version(pipeline: &Controller) -> Option<i64> {
        pipeline
            .status()
            .input_status()
            .values()
            .next()
            .unwrap()
            .completed_frontier
            .completed_watermark()
            .map(|w| w.metadata["version"].as_i64().unwrap())
    }

    init_logging();

    let storage_dir = TempDir::new().unwrap();

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

    println!("initial table version: {}", input_table.version());

    // Start parquet-to-parquet pipeline.
    let mut input_config = storage_options
        .iter()
        .map(|(k, v)| (k.into(), v.clone().into()))
        .collect::<HashMap<String, Value>>();
    let mode = if snapshot {
        "snapshot_and_follow"
    } else {
        "follow"
    };
    input_config.insert("mode".to_string(), mode.into());
    input_config.insert(
        "transaction_mode".to_string(),
        serde_json::to_value(transaction_mode).unwrap(),
    );

    let end_version = 5;
    if test_end_version {
        input_config.insert("end_version".to_string(), end_version.into());
    }

    input_config.insert("filter".to_string(), "bigint % 2 = 0".into());

    let mut pipeline = start_pipeline(
        input_table_uri,
        output_table_uri,
        &input_config,
        storage_options,
        &storage_dir,
        buffer_size,
        buffer_timeout_ms,
    )
    .await;

    // Connect to `output_table_uri`.
    let mut output_table = Arc::new(
        DeltaTableBuilder::from_uri(output_table_uri)
            .with_storage_options(storage_options.clone())
            .load()
            .await
            .unwrap(),
    );

    // The input connector is paused. Make sure that we can suspend the connector
    // before it started reading the checkpoint.
    if suspend {
        suspend_pipeline(pipeline).await;

        pipeline = start_pipeline(
            input_table_uri,
            output_table_uri,
            &input_config,
            storage_options,
            &storage_dir,
            buffer_size,
            buffer_timeout_ms,
        )
        .await;
    }

    pipeline.start_input_endpoint("test_input1").unwrap();

    let mut total_count = split_at;

    let mut expected_output = data[0..total_count]
        .iter()
        .filter_map(|x| {
            if x.bigint % 2 == 0 {
                let mut x = x.clone();
                x.unused = None;
                Some(x)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    wait_for_output_records::<DeltaTestStruct>(
        &mut output_table,
        &expected_output,
        &datafusion,
        20_000,
    )
    .await;

    println!("initial snapshot processed");

    // Write remaining data in 10 chunks, wait for it to show up in the output table.
    for chunk in data[split_at..].chunks(std::cmp::max(data[split_at..].len() / 10, 1)) {
        total_count += chunk.len();
        input_table = write_data_to_table(input_table, &arrow_schema, chunk).await;

        if !test_end_version || input_table.version() <= end_version {
            expected_output = data[0..total_count]
                .iter()
                .filter_map(|x| {
                    if x.bigint % 2 == 0 {
                        let mut x = x.clone();
                        x.unused = None;
                        Some(x)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
        };

        if suspend {
            suspend_pipeline(pipeline).await;

            pipeline = start_pipeline(
                input_table_uri,
                output_table_uri,
                &input_config,
                storage_options,
                &storage_dir,
                buffer_size,
                buffer_timeout_ms,
            )
            .await;
        }

        // Test the waterline tracking mechanism.
        if !suspend {
            wait(
                || {
                    if let Some(version) = completed_version(&pipeline) {
                        let expected = if test_end_version {
                            min(input_table.version(), end_version)
                        } else {
                            input_table.version()
                        };
                        debug!("pipeline completed version {version}, expected {expected}, waterlines: {:?}", pipeline.status().input_status().values().next().unwrap().completed_frontier.debug());
                        version == expected
                    } else {
                        debug!("pipeline completed version: None");
                        false
                    }
                },
                20_000,
            )
            .unwrap();
        }

        // Wait a bit to make sure the pipeline doesn't process data beyond end_version.
        if test_end_version && input_table.version() > end_version {
            sleep(Duration::from_millis(1000)).await;
        }

        // Use timeout of 0: once the pipeline indicates that the completed_version
        // reached the expected version (see above check), the output table should be up-to-date.
        wait_for_output_records::<DeltaTestStruct>(
            &mut output_table,
            &expected_output,
            &datafusion,
            if suspend { 200_000 } else { 0 },
        )
        .await;
    }

    if test_end_version && suspend {
        suspend_pipeline(pipeline).await;

        pipeline = start_pipeline(
            input_table_uri,
            output_table_uri,
            &input_config,
            storage_options,
            &storage_dir,
            buffer_size,
            buffer_timeout_ms,
        )
        .await;

        let status = pipeline.input_endpoint_status("test_input1").unwrap();

        assert!(status
            .as_object()
            .unwrap()
            .get("metrics")
            .unwrap()
            .as_object()
            .unwrap()
            .get("end_of_input")
            .unwrap()
            .as_bool()
            .unwrap());
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

    println!("Stopping pipeline");
    pipeline.stop().unwrap();
}

fn write_updates_as_json<T>(file: &mut File, data: &[T], polarity: bool)
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    let mut buffer = Vec::new();
    for v in data.iter() {
        let record_buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(record_buffer);
        v.serialize_with_context(
            &mut serializer,
            &SqlSerdeConfig::from(JsonFlavor::default()),
        )
        .unwrap();
        let record = serializer.into_inner();
        let update = if polarity {
            format!("{{\"insert\": {} }}\n", String::from_utf8(record).unwrap())
        } else {
            format!("{{\"delete\": {} }}\n", String::from_utf8(record).unwrap())
        };

        buffer.append(&mut update.into_bytes());
    }
    file.write_all(&buffer).unwrap();
}

/// Write and read delta table in cdc mode:
///
/// ```text
/// json data --> [pipeline 1] --> delta table --> [pipeline 2]
/// ```
///
/// pipeline 2 is configured in CDC mode to correctly interpret
/// __feldera_op and __feldera_ts fields output by pipelines 1.
#[allow(clippy::too_many_arguments)]
async fn test_cdc(
    _schema: &[Field],
    table_uri: &str,
    storage_options: &HashMap<String, String>,
    data: Vec<DeltaTestStruct>,
    suspend: bool,
    index: bool,
) {
    init_logging();

    let mut input_file = NamedTempFile::new().unwrap();
    let input_file_path = input_file.path().display().to_string();

    let table_uri_clone = table_uri.to_string();

    // Build pipeline 1.
    let mut output_config = storage_options.clone();

    output_config.insert("mode".to_string(), "truncate".to_string());

    let write_pipeline = tokio::task::spawn_blocking(move || {
        delta_write_pipeline::<DeltaTestStruct, DeltaTestKey, _>(
            &input_file_path,
            &table_uri_clone,
            &[SqlIdentifier::from("bigint")],
            |x: &DeltaTestStruct| DeltaTestKey { bigint: x.bigint },
            index,
            &output_config,
        )
    })
    .await
    .unwrap();

    write_pipeline.start();

    // Build pipeline 2.
    let mut input_config = storage_options.clone();

    input_config.insert("mode".to_string(), "cdc".to_string());
    input_config.insert("filter".to_string(), "bigint % 2 = 0".to_string());
    input_config.insert(
        "cdc_delete_filter".to_string(),
        "__feldera_op = 'd'".to_string(),
    );
    input_config.insert("cdc_order_by".to_string(), "__feldera_ts".to_string());

    let table_uri_clone = table_uri.to_string();

    let storage_dir = TempDir::new().unwrap();
    let storage_dir_path = storage_dir.path().to_path_buf();
    let input_config_clone: HashMap<String, String> = input_config.clone();
    let mut read_pipeline = tokio::task::spawn_blocking(move || {
        delta_read_pipeline::<DeltaTestStruct, DeltaTestKey, _>(
            &table_uri_clone,
            &input_config_clone,
            &[SqlIdentifier::from("bigint")],
            |x: &DeltaTestStruct| DeltaTestKey { bigint: x.bigint },
            &storage_dir_path,
        )
    })
    .await
    .unwrap();

    read_pipeline.start();

    let mut total_count = 0;

    let mut chunks = data.chunks(std::cmp::max(data.len() / 10, 1));

    // Write data in 10 chunks, wait for it to show up in the output view.
    loop {
        // Perform the first suspend while the table is empty to test that the delta lake connector
        // can successfully suspend when reading an empty table.
        if suspend {
            println!("start suspend");
            let (sender, mut receiver) = mpsc::channel(1);
            read_pipeline.start_suspend(Box::new(move |result| sender.try_send(result).unwrap()));

            // Suspend should not succeed, because of the barrier.
            timeout(Duration::from_secs(100), receiver.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            println!("pipeline suspended");

            read_pipeline.stop().unwrap();
            println!("pipeline stopped");

            let table_uri_clone = table_uri.to_string();
            let input_config_clone: HashMap<String, String> = input_config.clone();
            let storage_dir_path = storage_dir.path().to_path_buf();

            read_pipeline = tokio::task::spawn_blocking(move || {
                delta_read_pipeline::<DeltaTestStruct, DeltaTestKey, _>(
                    &table_uri_clone,
                    &input_config_clone,
                    &[SqlIdentifier::from("bigint")],
                    |x: &DeltaTestStruct| DeltaTestKey { bigint: x.bigint },
                    &storage_dir_path,
                )
            })
            .await
            .unwrap();

            read_pipeline.start();
        }

        wait_for_records_materialized(
            &read_pipeline,
            &SqlIdentifier::from("test_output1"),
            &data[0..total_count]
                .iter()
                .filter_map(|x| {
                    if x.bigint % 2 == 0 {
                        Some(x.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        )
        .await;

        let Some(chunk) = chunks.next() else {
            break;
        };

        write_updates_as_json(input_file.as_file_mut(), chunk, true);
        total_count += chunk.len();
    }

    // Modify all records by negating the `boolean` field.
    println!("Applying updates");
    let mut updated_count = 0;
    if index {
        for chunk in data.chunks(std::cmp::max(data.len() / 10, 1)) {
            let chunk = chunk
                .iter()
                .map(|x| {
                    let mut x = x.clone();
                    x.boolean = !x.boolean;
                    x
                })
                .collect::<Vec<_>>();
            write_updates_as_json(input_file.as_file_mut(), &chunk, true);
            updated_count += chunk.len();

            let expected = data[0..total_count]
                .iter()
                .enumerate()
                .flat_map(|(i, x)| {
                    let mut x = x.clone();
                    if x.bigint % 2 == 0 {
                        if i < updated_count {
                            x.boolean = !x.boolean;
                        }
                        Some(x)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            wait_for_records_materialized(
                &read_pipeline,
                &SqlIdentifier::from("test_output1"),
                &expected,
            )
            .await;
        }
    }

    let mut deleted_count = 0;
    // Delete data chunk by chunk.
    for chunk in data.chunks(std::cmp::max(data.len() / 10, 1)) {
        write_updates_as_json(input_file.as_file_mut(), chunk, false);

        deleted_count += chunk.len();

        wait_for_records_materialized(
            &read_pipeline,
            &SqlIdentifier::from("test_output1"),
            &data[deleted_count..total_count]
                .iter()
                .filter_map(|x| {
                    if x.bigint % 2 == 0 {
                        let mut x = x.clone();
                        if index {
                            x.boolean = !x.boolean;
                        }
                        Some(x.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        )
        .await;
    }

    read_pipeline.stop().unwrap();
    write_pipeline.stop().unwrap();
}

/// Generate up to `max_records` _unique_ records.
fn delta_data(max_records: usize) -> impl Strategy<Value = Vec<DeltaTestStruct>> {
    vec(DeltaTestStruct::arbitrary(), 0..max_records).prop_map(|vec| {
        let mut idx = 0;
        vec.into_iter()
            .map(|mut x| {
                x.bigint = idx;
                idx += 1;
                x
            })
            .collect()
    })
}

async fn delta_table_follow_file_test_common(
    snapshot: bool,
    transaction_mode: DeltaTableTransactionMode,
    suspend: bool,
    end_version: bool,
) {
    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data: Vec<DeltaTestStruct> = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema: Vec<Field> = DeltaTestStruct::schema();

    let input_table_dir = TempDir::new().unwrap();
    let input_table_uri = input_table_dir.path().display().to_string();

    let output_table_dir: TempDir = TempDir::new().unwrap();
    let output_table_uri = output_table_dir.path().display().to_string();

    test_follow(
        &relation_schema,
        &input_table_uri,
        &output_table_uri,
        &HashMap::new(),
        data,
        snapshot,
        transaction_mode,
        suspend,
        end_version,
        1000,
        100,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_file_test() {
    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = DeltaTestStruct::schema();

    let input_table_dir = TempDir::new().unwrap();
    let input_table_uri = input_table_dir.path().display().to_string();

    test_cdc(
        &relation_schema,
        &input_table_uri,
        &HashMap::new(),
        data,
        false,
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_file_indexed_test() {
    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = DeltaTestStruct::schema();

    let input_table_dir = TempDir::new().unwrap();
    let input_table_uri = input_table_dir.path().display().to_string();

    test_cdc(
        &relation_schema,
        &input_table_uri,
        &HashMap::new(),
        data,
        false,
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(target_arch = "aarch64", ignore = "flaky on aarch64")]
async fn delta_table_cdc_file_suspend_test() {
    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = DeltaTestStruct::schema();

    let input_table_dir = TempDir::new().unwrap();
    let input_table_uri = input_table_dir.path().display().to_string();

    test_cdc(
        &relation_schema,
        &input_table_uri,
        &HashMap::new(),
        data,
        true,
        false,
    )
    .await;
}

#[cfg(feature = "delta-s3-test")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[parallel(delta_s3)]
async fn delta_table_cdc_s3_test_suspend() {
    crate::integrated::delta_table::register_storage_handlers();

    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = DeltaTestStruct::schema();

    let input_uuid = uuid::Uuid::new_v4();

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

    test_cdc(
        &relation_schema,
        &input_table_uri,
        &object_store_config,
        data,
        false,
        true,
    )
    .await;
}

#[tokio::test]
async fn delta_table_snapshot_and_follow_file_test() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::None, false, false).await
}

#[tokio::test]
async fn delta_table_transactional_snapshot_and_follow_file_test() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::Snapshot, false, false)
        .await
}

#[tokio::test]
async fn delta_table_transactional_always_snapshot_and_follow_file_test() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::Always, false, false).await
}

#[tokio::test]
async fn delta_table_follow_file_test() {
    delta_table_follow_file_test_common(false, DeltaTableTransactionMode::None, false, false).await
}

#[tokio::test]
async fn delta_table_follow_file_test_end_version() {
    delta_table_follow_file_test_common(false, DeltaTableTransactionMode::None, false, true).await
}

#[tokio::test]
async fn delta_table_snapshot_and_follow_file_test_suspend() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::None, true, false).await
}

#[tokio::test]
async fn delta_table_transactional_snapshot_and_follow_file_test_suspend() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::Snapshot, true, false)
        .await
}

#[tokio::test]
async fn delta_table_transactional_always_snapshot_and_follow_file_test_suspend() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::Always, true, false).await
}

#[tokio::test]
async fn delta_table_snapshot_and_follow_file_test_suspend_end_version() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::None, true, true).await
}

#[cfg(feature = "delta-s3-test")]
async fn delta_table_follow_s3_test_common(snapshot: bool, suspend: bool) {
    crate::integrated::delta_table::register_storage_handlers();

    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = DeltaTestStruct::schema();

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
        suspend,
        false,
        1000,
        100,
    )
    .await;
}

#[cfg(feature = "delta-s3-test")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[parallel(delta_s3)]
async fn delta_table_follow_s3_test() {
    delta_table_follow_s3_test_common(false, false).await
}

#[cfg(feature = "delta-s3-test")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[parallel(delta_s3)]
async fn delta_table_snapshot_and_follow_s3_test() {
    delta_table_follow_s3_test_common(true, false).await
}

#[cfg(feature = "delta-s3-test")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[parallel(delta_s3)]
async fn delta_table_snapshot_and_follow_s3_test_suspend() {
    delta_table_follow_s3_test_common(true, true).await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1))]

    /// ```text
    /// input.json --> [pipeline1]--->delta_table-->[pipeline2]-->output.json
    /// ```
    #[test]
    fn delta_table_file_output_proptest(data in delta_data(20_000))
    {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        // Uncomment to inspect output parquet files produced by the test.
        forget(table_dir);

        delta_table_output_test(data.clone(), &table_uri, &HashMap::new(), true);


        // Read delta table unordered.
        let mut json_file = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema(),
            &HashMap::new());

        let expected_zset = OrdZSet::from_tuples((), data.clone().into_iter().map(|x| Tup2(Tup2(x,()),1)).collect());
        let zset = file_to_zset::<DeltaTestStruct>(json_file.as_file_mut());
        assert_eq!(zset, expected_zset);

        // Order delta table by `bigint` (which should be its natural order).
        let mut json_file_ordered_by_id = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema_with_lateness(),
            &HashMap::from([("timestamp_column".to_string(), "bigint".to_string())]));

        let zset = file_to_zset::<DeltaTestStruct>(json_file_ordered_by_id.as_file_mut());
        assert_eq!(zset, expected_zset);

        // Same as above, but commit a transaction after each input chunk.
        let mut json_file_ordered_by_id = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema_with_lateness(),
            &HashMap::from([("timestamp_column".to_string(), "bigint".to_string()), ("transaction_mode".to_string(), "always".to_string())]));

        let zset = file_to_zset::<DeltaTestStruct>(json_file_ordered_by_id.as_file_mut());
        assert_eq!(zset, expected_zset);

        // Order delta table by `bigint`, specify range in two different ways: using `snapshot_filter` and using `filter`.
        let mut json_file_ordered_filtered1 = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema_with_lateness(),
            &HashMap::from([("timestamp_column".to_string(), "bigint".to_string()), ("snapshot_filter".to_string(), "bigint >= 10000 ".to_string())]));

        let mut json_file_ordered_filtered2 = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema_with_lateness(),
            &HashMap::from([("timestamp_column".to_string(), "bigint".to_string()), ("filter".to_string(), "bigint >= 10000 ".to_string())]));

            let mut json_file_ordered_filtered3 = delta_table_snapshot_to_json::<DeltaTestStruct>(
                &table_uri,
                &DeltaTestStruct::schema_with_lateness(),
                &HashMap::from([("timestamp_column".to_string(), "bigint".to_string()), ("filter".to_string(), "bigint >= 10000 ".to_string()), ("transaction_mode".to_string(), "always".to_string())]));

        let expected_filtered_zset = OrdZSet::from_tuples(
                (),
                data.clone().into_iter()
                    .filter(|x| x.bigint >= 10000)
                    .map(|x| Tup2(Tup2(x,()),1)).collect()
                );

        let zset = file_to_zset::<DeltaTestStruct>(json_file_ordered_filtered1.as_file_mut());
        assert_eq!(zset, expected_filtered_zset);

        let zset = file_to_zset::<DeltaTestStruct>(json_file_ordered_filtered2.as_file_mut());
        assert_eq!(zset, expected_filtered_zset);

        let zset = file_to_zset::<DeltaTestStruct>(json_file_ordered_filtered3.as_file_mut());
        assert_eq!(zset, expected_filtered_zset);

        // Specify both `snapshot_filter` and `filter`.
        let mut json_file_filtered = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema_with_lateness(),
            &HashMap::from([("filter".to_string(), "bigint >= 5000 ".to_string()), ("snapshot_filter".to_string(), "bigint <= 10000 ".to_string())]));

        let expected_filtered_zset = OrdZSet::from_tuples(
                (),
                data.clone().into_iter()
                    .filter(|x| x.bigint >= 5000 && x.bigint <= 10000)
                    .map(|x| Tup2(Tup2(x,()),1)).collect()
                );

        let zset = file_to_zset::<DeltaTestStruct>(json_file_filtered.as_file_mut());
        assert_eq!(zset, expected_filtered_zset);

        // Order delta table by `timestamp`.
        let mut json_file_ordered_by_ts = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema_with_lateness(),
            &HashMap::from([("timestamp_column".to_string(), "timestamp_ntz".to_string())]));

        let zset = file_to_zset::<DeltaTestStruct>(json_file_ordered_by_ts.as_file_mut());
        assert_eq!(zset, expected_zset);

        // Order delta table by `timestamp`; specify an empty filter condition
        let mut json_file_ordered_by_ts = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema_with_lateness(),
            &HashMap::from([("timestamp_column".to_string(), "timestamp_ntz".to_string()), ("snapshot_filter".to_string(), "timestamp_ntz < timestamp '2005-01-01T00:00:00'".to_string())]));

        let zset = file_to_zset::<DeltaTestStruct>(json_file_ordered_by_ts.as_file_mut());
        assert_eq!(zset, OrdZSet::empty());

        // Filter delta table by id
        let mut json_file_filtered_by_id = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema(),
            &HashMap::from([("snapshot_filter".to_string(), "bigint >= 10000 ".to_string())]));

        let expected_filtered_zset = OrdZSet::from_tuples(
            (),
            data.clone().into_iter()
                .filter(|x| x.bigint >= 10000)
                .map(|x| Tup2(Tup2(x,()),1)).collect()
            );

        let zset = file_to_zset::<DeltaTestStruct>(json_file_filtered_by_id.as_file_mut());
        assert_eq!(zset, expected_filtered_zset);

        // Filter delta table by timestamp.
        let mut json_file_filtered_by_ts = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema(),
            &HashMap::from([("snapshot_filter".to_string(), "timestamp_ntz >= '2005-01-01 00:00:00'".to_string())]));

        let start = NaiveDate::from_ymd_opt(2005, 1, 1)
                .unwrap()
                .and_hms_milli_opt(0, 0, 0, 0)
                .unwrap();

        let expected_filtered_zset = OrdZSet::from_tuples(
            (),
            data.into_iter()
                .filter(|x| x.timestamp_ntz.milliseconds() >= start.and_utc().timestamp_millis())
                .map(|x| Tup2(Tup2(x,()),1)).collect()
            );

        let zset = file_to_zset::<DeltaTestStruct>(json_file_filtered_by_ts.as_file_mut());
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
    #[parallel(delta_s3)]
    fn delta_table_s3_output_proptest(data in delta_data(20_000))
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

        let mut json_file = delta_table_snapshot_to_json::<DeltaTestStruct>(
            &table_uri,
            &DeltaTestStruct::schema(),
            &object_store_config);

        let expected_zset = OrdZSet::from_tuples((), data.into_iter().map(|x| Tup2(Tup2(x,()),1)).collect());
        let zset = file_to_zset::<DeltaTestStruct>(json_file.as_file_mut());
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
#[parallel(delta_s3)]
fn delta_table_s3_people_2m() {
    use crate::test::DatabricksPeople;

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
    let mut json_file = delta_table_snapshot_to_json::<DatabricksPeople>(
        table_uri,
        &DatabricksPeople::schema(),
        &object_store_config,
    );

    println!("reading output file");
    let zset = file_to_zset::<DatabricksPeople>(json_file.as_file_mut());

    assert_eq!(zset.len(), 2_000_000);

    forget(json_file);
}

/// Read the same table using Unity Catalog path.
// I haven't investigated this in depth but it appears that, when running
// multiple delta connectors in parallel, some of which use unity catalog,
// and others use direct S3 URL to connect to the table, this confuses the
// AWS SDK into using the wrong authentication token in S3-based
// connectors. This causes authentication failures with errors like this:
//
// ```text
// 2025-10-29T17:59:09.906160Z  WARN dbsp_adapters::integrated::delta_table::output: delta_table test_output1: error creating or opening delta table 's3://feldera-delta-table-test/80007986-0e32-466a-ad60-4700013bc56e/' after 1 attempts (retrying in 1000 ms): ObjectStore { source: Generic { store: "S3", source: ListRequest { source: RetryError(RetryErrorImpl { method: GET, uri: Some(https://s3.us-east-2.amazonaws.com/feldera-delta-table-test?list-type=2&prefix=80007986-0e32-466a-ad60-4700013bc56e%2F_delta_log%2F), retries: 0, max_retries: 10, elapsed: 395.553916ms, retry_timeout: 180s, inner: Status { status: 400, body: Some("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InvalidToken</Code><Message>The provided token is malformed or otherwise invalid.</Message><Token-0>...skipped....</Token-0><RequestId>6A1SW4R71MWQFS8H</RequestId><HostId>XOF0VWtYySOJlF0t2R9roAqWM0qH4x+j6cbdkSIVep1l+0PxsAI3tq55nVIcsCj4brHzV9kzmGY=</HostId></Error>") } }) } } }
// ```
//
// Running the unity test sequentially with S3 tests seems to solve this
// reliably.

#[cfg(feature = "delta-s3-test")]
#[test]
#[serial(delta_s3)]
fn delta_table_unity_people_2m() {
    use crate::test::DatabricksPeople;

    let object_store_config = [
        (
            "unity_client_id".to_string(),
            std::env::var("DELTA_TABLE_TEST_UNITY_CLIENT_ID").unwrap(),
        ),
        (
            "unity_client_secret".to_string(),
            std::env::var("DELTA_TABLE_TEST_UNITY_CLIENT_SECRET").unwrap(),
        ),
        (
            "databricks_host".to_string(),
            std::env::var("DELTA_TABLE_TEST_UNITY_HOST").unwrap(),
        ),
        // AWS region must be specified (see https://github.com/delta-io/delta-rs/issues/1095).
        ("aws_region".to_string(), "us-west-1".to_string()),
        // Set long timeout for reading large files from S3.
        ("timeout".to_string(), "1000 secs".to_string()),
    ]
    .into_iter()
    .collect::<HashMap<_, _>>();

    let table_uri = "uc://feldera_experimental.default.people_2m";
    let mut json_file = delta_table_snapshot_to_json::<DatabricksPeople>(
        table_uri,
        &DatabricksPeople::schema(),
        &object_store_config,
    );

    println!("reading output file");
    let zset = file_to_zset::<DatabricksPeople>(json_file.as_file_mut());

    assert_eq!(zset.len(), 2_000_000);

    forget(json_file);
}
