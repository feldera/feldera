//! See crates/iceberg/srd/tests/README.md for a description of the Iceberg test harness.

use crate::{
    Controller,
    test::{file_to_zset, wait},
};
use crossbeam::channel::Receiver;
use dbsp::DBData;
use feldera_sqllib::Variant;
#[cfg(any(feature = "iceberg-tests-fs", feature = "iceberg-tests-follow"))]
use feldera_sqllib::{ByteArray, F32, F64, Timestamp, TimestampTz};
use feldera_types::{
    program_schema::Field,
    serde_with_context::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig},
};
use serde_json::json;

use std::collections::HashMap;
#[cfg(any(
    feature = "iceberg-tests-fs",
    feature = "iceberg-tests-glue",
    feature = "iceberg-tests-rest",
    feature = "iceberg-tests-s3tables"
))]
use std::time::Instant;
use tempfile::NamedTempFile;
#[cfg(any(
    feature = "iceberg-tests-fs",
    feature = "iceberg-tests-glue",
    feature = "iceberg-tests-rest",
    feature = "iceberg-tests-s3tables"
))]
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(any(feature = "iceberg-tests-fs", feature = "iceberg-tests-follow"))]
use std::io::Write;

#[cfg(feature = "iceberg-tests-fs")]
use super::IcebergSubsetTestStruct;
#[cfg(any(
    feature = "iceberg-tests-fs",
    feature = "iceberg-tests-glue",
    feature = "iceberg-tests-rest",
    feature = "iceberg-tests-follow"
))]
use super::IcebergTestStruct;
#[cfg(feature = "iceberg-tests-s3tables")]
use super::S3TablesTestStruct;
use super::test_circuit_with_properties;

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

#[cfg(any(feature = "iceberg-tests-fs", feature = "iceberg-tests-follow"))]
/// Store test dataset in an ndjson file
fn data_to_ndjson(data: Vec<IcebergTestStruct>) -> NamedTempFile {
    println!("delta_table_output_test: preparing input file");
    let mut file = NamedTempFile::new().unwrap();
    for v in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        v.serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        file.as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        file.write_all(b"\n").unwrap();
    }

    file
}

/// Read the Iceberg connector's custom metrics into a `name -> value` map.
fn iceberg_connector_metrics(pipeline: &Controller) -> HashMap<String, f64> {
    let endpoint_id = pipeline
        .input_endpoint_id_by_name("test_input1")
        .expect("iceberg input endpoint must exist");
    pipeline
        .status()
        .input_status()
        .get(&endpoint_id)
        .and_then(|status| status.custom_metrics.clone())
        .map(|metrics| {
            metrics
                .metrics()
                .into_iter()
                .map(|(name, _, _, value)| (name.to_string(), value))
                .collect()
        })
        .unwrap_or_default()
}

/// Read a snapshot of an Iceberg table with records of type `T` to a temporary JSON file.
///
/// `table_properties` are set on the input relation, the way table-level SQL
/// `WITH` properties (e.g., `skip_unused_columns`) reach the connector.
///
/// `config` is the connector's transport config as a JSON object. This function
/// forces `mode = snapshot`. Returns the output file and the connector's custom
/// metrics captured just before the pipeline is stopped.
#[cfg(any(
    feature = "iceberg-tests-fs",
    feature = "iceberg-tests-glue",
    feature = "iceberg-tests-rest",
    feature = "iceberg-tests-s3tables"
))]
fn iceberg_snapshot_to_json<T>(
    schema: &[Field],
    table_properties: &[(&str, &str)],
    config: serde_json::Value,
) -> (NamedTempFile, HashMap<String, f64>)
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Sync,
{
    let start = Instant::now();
    let json_file = NamedTempFile::new().unwrap();
    println!(
        "iceberg_snapshot_to_json: writing output to {}",
        json_file.path().display()
    );

    let mut config = config;
    config
        .as_object_mut()
        .expect("iceberg connector config must be a JSON object")
        .insert("mode".to_string(), json!("snapshot"));

    let (input_pipeline, err_receiver) = iceberg_input_pipeline::<T>(
        schema,
        table_properties,
        config,
        &json_file.path().display().to_string(),
    );
    input_pipeline.start();
    wait(
        || input_pipeline.status().pipeline_complete() || err_receiver.len() > 0,
        400_000,
    )
    .expect("timeout");

    assert!(err_receiver.is_empty());

    // Read metrics before stopping, while the connector status is still live.
    let metrics = iceberg_connector_metrics(&input_pipeline);

    input_pipeline.stop().unwrap();

    info!("Read Iceberg snapshot in {:?}", start.elapsed());

    (json_file, metrics)
}

/// Build a pipeline that reads from an Iceberg table and writes to a JSON file.
fn iceberg_input_pipeline<T>(
    schema: &[Field],
    table_properties: &[(&str, &str)],
    config: serde_json::Value,
    output_file_path: &str,
) -> (Controller, Receiver<String>)
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Sync,
{
    init_logging();

    // Create controller.
    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": {
            "name": "file_output",
            "config": {
              "path": output_file_path
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
      "inputs": {
        "test_input1": {
          "stream": "test_input1",
          "transport": {
              "name": "iceberg_input",
              "config": config
          }
        }
      }
    }))
    .unwrap();

    let schema = schema.to_vec();
    let table_properties: Vec<(String, String)> = table_properties
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();

    let controller = Controller::with_test_config(
        move |workers| {
            let table_properties: Vec<(&str, &str)> = table_properties
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            Ok(test_circuit_with_properties::<T>(
                workers,
                &schema,
                &table_properties,
                &[None],
            ))
        },
        &config,
        Box::new(move |e, _| {
            let msg = format!("iceberg_input_test: error: {e}");
            println!("{}", msg);
            err_sender.send(msg).unwrap()
        }),
    )
    .unwrap();

    (controller, err_receiver)
}

/// Generate up to `max_records` _unique_ records.
#[cfg(any(feature = "iceberg-tests-fs", feature = "iceberg-tests-follow"))]
fn data(n_records: usize) -> Vec<IcebergTestStruct> {
    let mut result = Vec::with_capacity(n_records);

    let mut time =
        chrono::NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    for i in 0..n_records {
        result.push(IcebergTestStruct {
            b: i % 2 != 0,
            i: i as i32,
            l: i as i64,
            r: F32::from(i as f32),
            d: F64::from(i as f64),
            dec: feldera_sqllib::SqlDecimal::<10, 3>::new(i as i128, 2).unwrap(),
            dt: feldera_sqllib::Date::from_date(time.date()),
            tm: feldera_sqllib::Time::from_time(time.time()),
            ts: feldera_sqllib::Timestamp::from_naiveDateTime(time),
            s: format!("s{i}"),
            // uuid: ByteArray::new([0u8; 16].as_slice()),
            fixed: ByteArray::new([0u8; 5].as_slice()),
            varbin: ByteArray::new([0u8; 5].as_slice()),
            tstz: TimestampTz::from(Timestamp::from_naiveDateTime(time)),
        });

        time += std::time::Duration::from_secs(1);
    }

    result
}

#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_unordered() {
    iceberg_localfs_input_test(1_000_000, json!({}), &|_| true);
}

#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_ordered() {
    iceberg_localfs_input_test(1_000_000, json!({ "timestamp_column": "ts" }), &|_| true);
}

#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_ordered_with_filter() {
    iceberg_localfs_input_test(
        1_000_000,
        json!({ "timestamp_column": "ts", "snapshot_filter": "i >= 10000" }),
        &|x| x.i >= 10000,
    );
}

/// A single parser task must ingest the whole snapshot correctly (the parallel
/// path defaults to 4 parsers and is covered by the tests above).
#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_single_parser() {
    iceberg_localfs_input_test(100_000, json!({ "num_parsers": 1 }), &|_| true);
}

/// `transaction_mode = snapshot` on an unordered read ingests the whole snapshot
/// in exactly one Feldera transaction; the ingested data must be identical to a
/// non-transactional read.
#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_transactional() {
    let metrics =
        iceberg_localfs_input_test(100_000, json!({ "transaction_mode": "snapshot" }), &|_| {
            true
        });
    // Unordered snapshot: exactly one transaction. (Reverting the transaction
    // wiring drops this to 0.)
    assert_eq!(
        metrics
            .get("input_connector_iceberg_snapshot_transaction_starts")
            .copied(),
        Some(1.0)
    );
}

/// `transaction_mode = snapshot` on an ordered read ingests one Feldera
/// transaction per lateness range; the ingested data must still be complete.
#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_ordered_transactional() {
    let metrics = iceberg_localfs_input_test(
        100_000,
        json!({ "timestamp_column": "ts", "transaction_mode": "snapshot" }),
        &|_| true,
    );
    // Ordered snapshot: one transaction per non-empty lateness range, so at
    // least one, and (with data spanning multiple ranges) typically several.
    let starts = metrics
        .get("input_connector_iceberg_snapshot_transaction_starts")
        .copied()
        .unwrap_or(0.0);
    assert!(
        starts >= 1.0,
        "expected >= 1 snapshot transaction, got {starts}"
    );
}

/// Create a local Iceberg table populated with `data` and return its metadata
/// location. With `extra_columns`, the table gets columns that no test SQL
/// schema declares (see `--extra-columns` in `create_test_table_s3.py`).
#[cfg(feature = "iceberg-tests-fs")]
fn create_localfs_table(data: &[IcebergTestStruct], extra_columns: bool) -> String {
    let table_dir = tempfile::TempDir::new().unwrap();
    let table_path = table_dir.path().display().to_string();

    let ndjson_file = data_to_ndjson(data.to_vec());
    println!("wrote test data to {}", ndjson_file.path().display());

    // Uncomment to inspect output parquet files produced by the test.
    std::mem::forget(table_dir);

    let script_path = "../iceberg/src/test/create_test_table_s3.py";

    // Run the Python script using the Python interpreter
    let mut command = std::process::Command::new("python3");
    command
        .arg(script_path)
        .arg("--catalog=sql")
        .arg(format!("--warehouse-path={table_path}"))
        .arg(format!("--json-file={}", ndjson_file.path().display()));
    if extra_columns {
        command.arg("--extra-columns");
    }
    let output = command
        .output()
        .map_err(|e| {
            format!("Error running '{script_path}' script to generate an Iceberg table: {e}")
        })
        .unwrap();

    if !output.status.success() {
        panic!(
            "'{script_path}' failed (status: {}), stdout:{}\nstderr:{}",
            output.status,
            &String::from_utf8(output.stdout).unwrap(),
            &String::from_utf8(output.stderr).unwrap()
        );
    }

    // The script should print table metadata location on the last line.
    String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .last()
        .unwrap()
        .to_string()
}

/// Ingest a local-FS Iceberg table in snapshot mode and assert the ingested
/// data matches `data(n_records)` filtered by `filter`. `extra_config` is
/// merged into the connector's transport config as JSON. Returns the
/// connector's custom metrics so callers can make mode-specific assertions.
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test(
    n_records: usize,
    extra_config: serde_json::Value,
    filter: &dyn Fn(&IcebergTestStruct) -> bool,
) -> HashMap<String, f64> {
    let data = data(n_records);

    let metadata_path = create_localfs_table(&data, false);

    let mut config = json!({ "metadata_location": metadata_path });
    let config_obj = config.as_object_mut().unwrap();
    for (key, value) in extra_config
        .as_object()
        .expect("extra_config must be a JSON object")
    {
        config_obj.insert(key.clone(), value.clone());
    }

    let (mut json_file, metrics) = iceberg_snapshot_to_json::<IcebergTestStruct>(
        &IcebergTestStruct::schema_with_lateness(),
        &[],
        config,
    );

    let expected_zset = dbsp::OrdZSet::from_tuples(
        (),
        data.clone()
            .into_iter()
            .filter(filter)
            .map(|x| dbsp::utils::Tup2(dbsp::utils::Tup2(x, ()), 1))
            .collect(),
    );
    let zset = file_to_zset::<IcebergTestStruct>(json_file.as_file_mut());

    assert_eq!(zset, expected_zset);

    // A snapshot-only connector must reach the completed phase (2).
    assert_eq!(
        metrics.get("input_connector_iceberg_phase").copied(),
        Some(2.0)
    );

    // The test table is built with a single append, i.e. the ingested snapshot
    // has sequence number 1. (An unset gauge would read -1.)
    assert_eq!(
        metrics
            .get("input_connector_iceberg_last_ingested_sequence_number")
            .copied(),
        Some(1.0)
    );

    metrics
}

/// Read a table through a SQL declaration that names only a few of its
/// columns, while the table also holds columns (including a `uuid` one, a
/// type no test struct models) that the connector must ignore because it
/// selects the declared columns instead of `*`.
///
/// With `skip_unused` (the `skip_unused_columns` table property), the
/// connector must additionally not read the nullable `l` column, which the
/// SQL schema marks unused, so `l` comes out NULL. This variant fails if the
/// connector falls back to reading all columns.
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_subset_test(skip_unused: bool) {
    let data = data(100_000);

    let metadata_path = create_localfs_table(&data, true);

    let table_properties: &[(&str, &str)] = if skip_unused {
        &[("skip_unused_columns", "true")]
    } else {
        &[]
    };

    let (mut json_file, _metrics) = iceberg_snapshot_to_json::<IcebergSubsetTestStruct>(
        &IcebergSubsetTestStruct::schema(),
        table_properties,
        json!({ "metadata_location": metadata_path }),
    );

    let expected_zset = dbsp::OrdZSet::from_tuples(
        (),
        data.into_iter()
            .map(|x| IcebergSubsetTestStruct {
                i: x.i,
                s: x.s,
                l: if skip_unused { None } else { Some(x.l) },
            })
            .map(|x| dbsp::utils::Tup2(dbsp::utils::Tup2(x, ()), 1))
            .collect(),
    );
    let zset = file_to_zset::<IcebergSubsetTestStruct>(json_file.as_file_mut());

    assert_eq!(zset, expected_zset);
}

/// The connector reads only the columns the SQL table declares.
#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_subset_schema() {
    iceberg_localfs_input_subset_test(false);
}

/// The `skip_unused_columns` table property also drops declared-but-unused
/// columns from the read.
#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_skip_unused_columns() {
    iceberg_localfs_input_subset_test(true);
}

/// Build an input-only pipeline that reads a local-FS Iceberg snapshot with
/// at-least-once fault tolerance, checkpointing to `storage_dir`. Rebuilding a
/// pipeline with the same `storage_dir` resumes from the latest checkpoint.
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_ft_pipeline(
    extra_config: serde_json::Value,
    storage_dir: &std::path::Path,
) -> Controller {
    init_logging();

    let mut config = json!({ "mode": "snapshot" });
    let config_obj = config.as_object_mut().unwrap();
    for (key, value) in extra_config
        .as_object()
        .expect("extra_config must be a JSON object")
    {
        config_obj.insert(key.clone(), value.clone());
    }

    let config: feldera_types::config::PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "fault_tolerance": { "model": "at_least_once" },
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "iceberg_input",
                    "config": config,
                }
            }
        }
    }))
    .unwrap();

    Controller::with_test_config(
        move |workers| {
            // A concrete persistent output id is required for checkpointing.
            Ok(test_circuit_with_properties::<IcebergTestStruct>(
                workers,
                &IcebergTestStruct::schema_with_lateness(),
                &[],
                &[Some("output")],
            ))
        },
        &config,
        Box::new(|e, _| panic!("iceberg ft pipeline: error: {e}")),
    )
    .unwrap()
}

/// Checkpoint-and-suspend the pipeline, then stop it.
#[cfg(feature = "iceberg-tests-fs")]
fn suspend_and_stop(pipeline: Controller) {
    let (sender, receiver) = std::sync::mpsc::channel();
    pipeline.start_suspend(Box::new(move |result| {
        let _ = sender.send(result.map(|_| ()).map_err(|e| e.to_string()));
    }));
    receiver
        .recv_timeout(std::time::Duration::from_secs(100))
        .expect("suspend timed out")
        .expect("suspend failed");
    pipeline.stop().unwrap();
}

/// A snapshot fully ingested before a checkpoint must not be re-read after a
/// suspend/resume: the resumed connector reaches the completed phase (2) and
/// reads zero records. This is what lets a large Iceberg table survive a
/// restart without re-ingesting all of its rows.
///
/// To confirm the assertion catches a regression, drop the terminal eoi
/// boundary (or the `resume_info.eoi` short-circuit) in `input.rs`: the resumed
/// run then re-reads the whole snapshot and `snapshot_records_total` is nonzero.
#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_resume_completed_snapshot() {
    let data = data(100_000);
    let metadata_path = create_localfs_table(&data, false);
    let storage_dir = tempfile::TempDir::new().unwrap();

    // Ordered snapshot so the read is resumable per lateness range.
    let config = json!({ "metadata_location": metadata_path, "timestamp_column": "ts" });

    // First run: ingest the whole snapshot, then checkpoint and suspend.
    let pipeline = iceberg_ft_pipeline(config.clone(), storage_dir.path());
    pipeline.start();
    wait(|| pipeline.pipeline_complete(), 400_000).expect("timeout waiting for snapshot");
    let first = iceberg_connector_metrics(&pipeline);
    assert!(
        first
            .get("input_connector_iceberg_snapshot_records_total")
            .copied()
            .unwrap_or(0.0)
            > 0.0,
        "the first run should ingest the snapshot"
    );
    suspend_and_stop(pipeline);

    // Second run: resume from the checkpoint. The snapshot is complete, so the
    // connector reaches the completed phase without reading any records.
    let pipeline = iceberg_ft_pipeline(config, storage_dir.path());
    pipeline.start();
    wait(|| pipeline.pipeline_complete(), 60_000).expect("timeout waiting for resume");
    let second = iceberg_connector_metrics(&pipeline);
    assert_eq!(
        second
            .get("input_connector_iceberg_snapshot_records_total")
            .copied(),
        Some(0.0),
        "a resumed, already-completed snapshot must not be re-read"
    );
    assert_eq!(
        second.get("input_connector_iceberg_phase").copied(),
        Some(2.0),
        "the resumed connector must reach the completed phase"
    );
    pipeline.stop().unwrap();
}

#[test]
#[cfg(feature = "iceberg-tests-glue")]
fn iceberg_glue_s3_input_test() {
    use dbsp::trace::BatchReader;
    // Read delta table unordered.
    let (mut json_file, _metrics) = iceberg_snapshot_to_json::<IcebergTestStruct>(
        &IcebergTestStruct::schema_with_lateness(),
        &[],
        json!({
            "catalog_type": "glue",
            "glue.warehouse": "s3://feldera-iceberg-test/",
            "table_name": "iceberg_test.test_table_v2",
            "glue.access-key-id": std::env::var("ICEBERG_TEST_AWS_ACCESS_KEY_ID").unwrap(),
            "glue.secret-access-key": std::env::var("ICEBERG_TEST_AWS_SECRET_ACCESS_KEY").unwrap(),
            "glue.region": "us-east-1",
            "s3.access-key-id": std::env::var("ICEBERG_TEST_AWS_ACCESS_KEY_ID").unwrap(),
            "s3.secret-access-key": std::env::var("ICEBERG_TEST_AWS_SECRET_ACCESS_KEY").unwrap(),
            "s3.region": "us-east-1",
        }),
    );

    let zset = file_to_zset::<IcebergTestStruct>(json_file.as_file_mut());

    // The data for this test is generated by the Python script, we don't know the
    // exact set of records in the dataset.
    assert_eq!(zset.len(), 2000000);
}

#[test]
#[cfg(feature = "iceberg-tests-s3tables")]
fn iceberg_s3tables_input_test() {
    use dbsp::trace::BatchReader;

    // Reads `dev.test_table` (schema `id BIGINT NOT NULL, name STRING,
    // created_at TIMESTAMP`, 100 rows) from an Amazon S3 Tables bucket.
    //
    // Credentials and region resolve from the ambient AWS provider chain
    // (environment variables, shared config file, or SSO profile), so no keys
    // are embedded in the connector config. The resolved identity must be
    // authorized for `s3tables:GetTable` (to locate the table metadata) and
    // `s3tables:GetTableData` (the FileIO reads the metadata and data files).
    // Run with AWS credentials configured, e.g. `AWS_PROFILE=<profile>` or
    // `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`(/`AWS_SESSION_TOKEN`) exported.
    let (mut json_file, _metrics) = iceberg_snapshot_to_json::<S3TablesTestStruct>(
        &S3TablesTestStruct::schema(),
        &[],
        json!({
            "catalog_type": "s3tables",
            "s3tables.table-bucket-arn": "arn:aws:s3tables:us-west-1:737834633458:bucket/iceberg-test",
            "table_name": "dev.test_table",
            "s3tables.region": "us-west-1",
            "s3.region": "us-west-1",
        }),
    );

    let zset = file_to_zset::<S3TablesTestStruct>(json_file.as_file_mut());

    assert_eq!(zset.len(), 100);
}

#[test]
#[cfg(feature = "iceberg-tests-rest")]
fn iceberg_rest_s3_input_test() {
    use dbsp::trace::BatchReader;

    // Read delta table unordered.
    let (mut json_file, _metrics) = iceberg_snapshot_to_json::<IcebergTestStruct>(
        &IcebergTestStruct::schema_with_lateness(),
        &[],
        json!({
            "catalog_type": "rest",
            "rest.uri": "http://localhost:8181",
            "rest.warehouse": "s3://feldera-iceberg-test/",
            "table_name": "iceberg_test.test_table_v2",
            "s3.access-key-id": std::env::var("ICEBERG_TEST_AWS_ACCESS_KEY_ID").unwrap(),
            "s3.secret-access-key": std::env::var("ICEBERG_TEST_AWS_SECRET_ACCESS_KEY").unwrap(),
            "s3.region": "us-east-1",
        }),
    );

    let zset = file_to_zset::<IcebergTestStruct>(json_file.as_file_mut());

    assert_eq!(zset.len(), 2000000);
    //assert_eq!(zset, expected_zset);
}

// ---------------------------------------------------------------------------
// Follow-mode tests (feature `iceberg-tests-follow`).
//
// These need a REST catalog and an S3 store that both the writer (pyiceberg)
// and the connector (iceberg-rust) can reach. Defaults target the local docker
// setup in crates/iceberg/src/test/README.md; override via FELDERA_ICEBERG_*.
// ---------------------------------------------------------------------------

#[cfg(feature = "iceberg-tests-follow")]
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Connector transport config for a REST-catalog table `mode` on the test store.
#[cfg(feature = "iceberg-tests-follow")]
fn rest_follow_config(table: &str, mode: &str) -> serde_json::Value {
    json!({
        "mode": mode,
        "catalog_type": "rest",
        "rest.uri": env_or("FELDERA_ICEBERG_REST_URI", "http://localhost:8181"),
        "rest.warehouse": env_or("FELDERA_ICEBERG_WAREHOUSE", "s3://test/iceberg-follow"),
        "table_name": table,
        "s3.endpoint": env_or("FELDERA_ICEBERG_S3_ENDPOINT", "http://localhost:9000"),
        "s3.access-key-id": env_or("FELDERA_ICEBERG_S3_KEY", "minio"),
        "s3.secret-access-key": env_or("FELDERA_ICEBERG_S3_SECRET", "miniopasswd"),
        "s3.region": env_or("FELDERA_ICEBERG_S3_REGION", "us-east-1"),
        // MinIO (and most non-AWS S3) serve path-style URLs; the opendal S3
        // backend defaults to virtual-host style, so opt out explicitly.
        "s3.path-style-access": env_or("FELDERA_ICEBERG_S3_PATH_STYLE", "true"),
    })
}

/// The follow connector config for `table` with an extra `key = value` option
/// merged in (e.g. a `snapshot_id` follow start point).
#[cfg(feature = "iceberg-tests-follow")]
fn rest_follow_config_with(
    table: &str,
    mode: &str,
    key: &str,
    value: serde_json::Value,
) -> serde_json::Value {
    let mut config = rest_follow_config(table, mode);
    config
        .as_object_mut()
        .expect("iceberg connector config must be a JSON object")
        .insert(key.to_string(), value);
    config
}

/// Create (`op = "create"`) or append to (`op = "append"`) the REST-catalog test
/// table with `chunk`, producing a new snapshot. Returns the new snapshot's id so
/// a test can pin it as a follow start point. Shells out to the pyiceberg helper,
/// since iceberg-rust cannot write tables.
#[cfg(feature = "iceberg-tests-follow")]
fn follow_table_op(op: &str, table: &str, chunk: &[IcebergTestStruct]) -> i64 {
    let ndjson = data_to_ndjson(chunk.to_vec());
    let script = "../iceberg/src/test/follow_table.py";
    let python = env_or("FELDERA_ICEBERG_PYTHON", "python3");
    let output = std::process::Command::new(python)
        .arg(script)
        .arg(format!("--op={op}"))
        .arg(format!("--table={table}"))
        .arg(format!("--json-file={}", ndjson.path().display()))
        .output()
        .unwrap_or_else(|e| panic!("failed to run '{script}': {e}"));
    if !output.status.success() {
        panic!(
            "'{script} --op={op}' failed (status {}):\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    // The writer prints the new snapshot id on its last stdout line.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let last = stdout.trim().lines().last().unwrap_or_default();
    last.parse::<i64>()
        .unwrap_or_else(|_| panic!("'{script} --op={op}' printed unexpected output: {last:?}"))
}

/// Sum of the records the connector has ingested so far (snapshot phase plus
/// follow phase), read from its custom metrics.
#[cfg(feature = "iceberg-tests-follow")]
fn ingested_records(pipeline: &Controller) -> u64 {
    let metrics = iceberg_connector_metrics(pipeline);
    let get = |name: &str| metrics.get(name).copied().unwrap_or(0.0) as u64;
    get("input_connector_iceberg_snapshot_records_total")
        + get("input_connector_iceberg_follow_records_total")
}

/// Run `body` against a running follow-mode pipeline, then stop it. The output
/// file receives `insert_delete` JSON.
#[cfg(feature = "iceberg-tests-follow")]
fn with_follow_pipeline<F>(table: &str, mode: &str, body: F)
where
    F: FnOnce(&Controller, &std::path::Path),
{
    with_follow_pipeline_cfg(rest_follow_config(table, mode), body)
}

/// Like [`with_follow_pipeline`], but takes a fully built connector config so a
/// test can add options such as a `snapshot_id` follow start point.
#[cfg(feature = "iceberg-tests-follow")]
fn with_follow_pipeline_cfg<F>(config: serde_json::Value, body: F)
where
    F: FnOnce(&Controller, &std::path::Path),
{
    let json_file = NamedTempFile::new().unwrap();
    let (pipeline, err_receiver) = iceberg_input_pipeline::<IcebergTestStruct>(
        &IcebergTestStruct::schema_with_lateness(),
        &[],
        config,
        &json_file.path().display().to_string(),
    );
    pipeline.start();

    // Surface connector errors promptly instead of hanging until a timeout.
    let watch = err_receiver.clone();
    let guard = std::thread::spawn(move || {
        if let Ok(msg) = watch.recv() {
            panic!("follow pipeline reported an error: {msg}");
        }
    });

    body(&pipeline, json_file.path());

    assert!(err_receiver.is_empty(), "follow pipeline reported errors");
    pipeline.stop().unwrap();
    drop(guard);
}

/// `snapshot_and_follow`: the initial snapshot is read, then a second snapshot
/// committed before startup is caught up via follow. Every row lands once.
#[test]
#[cfg(feature = "iceberg-tests-follow")]
fn iceberg_rest_follow_snapshot_and_follow() {
    use dbsp::trace::BatchReader;

    let all = data(10);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.snapshot_and_follow");

    // Two snapshots exist before the connector starts.
    follow_table_op("create", table, &all[..5]);
    follow_table_op("append", table, &all[5..]);

    with_follow_pipeline(table, "snapshot_and_follow", |pipeline, out_path| {
        wait(|| ingested_records(pipeline) >= 10, 120_000)
            .expect("timed out ingesting snapshot + follow");
        // Let the output connector flush the ingested rows.
        wait(|| output_record_count(out_path) >= 10, 60_000).expect("timed out writing output");

        let zset = output_zset(out_path);
        assert_eq!(zset.len(), 10);
        assert_eq!(zset, expected_zset(&all));
    });
}

/// `follow`: no initial snapshot; a snapshot committed after startup is tailed.
/// Rows present before the start snapshot are not ingested.
#[test]
#[cfg(feature = "iceberg-tests-follow")]
fn iceberg_rest_follow_live_append() {
    use dbsp::trace::BatchReader;

    let all = data(10);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.live_append");

    // The connector starts following after this snapshot, so these rows are
    // not ingested.
    follow_table_op("create", table, &all[..5]);

    with_follow_pipeline(table, "follow", |pipeline, out_path| {
        // Nothing to ingest until a new snapshot appears.
        follow_table_op("append", table, &all[5..]);

        wait(|| ingested_records(pipeline) >= 5, 120_000)
            .expect("timed out tailing the appended snapshot");
        wait(|| output_record_count(out_path) >= 5, 60_000).expect("timed out writing output");

        let zset = output_zset(out_path);
        assert_eq!(zset.len(), 5);
        assert_eq!(zset, expected_zset(&all[5..]));
    });
}

/// `follow` with an explicit `snapshot_id` start point. Three snapshots (A, B,
/// C) exist before the connector starts; following from B must ingest only C's
/// rows, never A's or B's, since `snapshots_after` walks the ancestry back to B.
#[test]
#[cfg(feature = "iceberg-tests-follow")]
fn iceberg_rest_follow_start_from_snapshot_id() {
    use dbsp::trace::BatchReader;

    let all = data(15);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.start_from_id");

    follow_table_op("create", table, &all[..5]); // snapshot A
    let snapshot_b = follow_table_op("append", table, &all[5..10]); // snapshot B
    follow_table_op("append", table, &all[10..]); // snapshot C

    let config = rest_follow_config_with(table, "follow", "snapshot_id", json!(snapshot_b));
    with_follow_pipeline_cfg(config, |pipeline, out_path| {
        wait(|| ingested_records(pipeline) >= 5, 120_000)
            .expect("timed out following from the pinned snapshot");
        wait(|| output_record_count(out_path) >= 5, 60_000).expect("timed out writing output");

        let zset = output_zset(out_path);
        assert_eq!(zset.len(), 5);
        assert_eq!(zset, expected_zset(&all[10..]));
    });
}

/// Number of complete `insert_delete` records written to the output file so
/// far (one JSON value per line).
#[cfg(feature = "iceberg-tests-follow")]
fn output_record_count(path: &std::path::Path) -> usize {
    std::fs::read(path)
        .map(|bytes| bytes.iter().filter(|&&b| b == b'\n').count())
        .unwrap_or(0)
}

/// The zset of `insert_delete` records currently in the output file.
#[cfg(feature = "iceberg-tests-follow")]
fn output_zset(path: &std::path::Path) -> dbsp::OrdZSet<IcebergTestStruct> {
    let mut file = std::fs::File::open(path).unwrap();
    file_to_zset::<IcebergTestStruct>(&mut file)
}

/// The all-`+1` zset the connector should produce for `data`.
#[cfg(feature = "iceberg-tests-follow")]
fn expected_zset(data: &[IcebergTestStruct]) -> dbsp::OrdZSet<IcebergTestStruct> {
    dbsp::OrdZSet::from_tuples(
        (),
        data.iter()
            .cloned()
            .map(|x| dbsp::utils::Tup2(dbsp::utils::Tup2(x, ()), 1))
            .collect(),
    )
}
