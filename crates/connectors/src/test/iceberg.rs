//! See crates/iceberg/srd/tests/README.md for a description of the Iceberg test harness.

use crate::{
    Controller,
    test::{
        engine_test, init_test_logger,
        pipeline::{PipelineTestEngine, TestCircuit},
        published_schema, read, relations,
    },
};
use chrono::{NaiveDateTime, TimeDelta};
use crossbeam::channel::Receiver;
use feldera_adapterlib::test::{Updates, ZSet, wait};
use feldera_types::{
    adapter_stats::TransactionStatus,
    program_schema::{ColumnType, Field, Relation},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    io::Write,
    path::Path,
    time::Instant,
};
use tempfile::NamedTempFile;
use tracing::info;

/// Store test dataset in an ndjson file
pub(super) fn data_to_ndjson(data: &[Value]) -> NamedTempFile {
    println!("delta_table_output_test: preparing input file");
    let mut file = NamedTempFile::new().unwrap();
    for v in data.iter() {
        file.write_all(v.to_string().as_bytes()).unwrap();
        file.write_all(b"\n").unwrap();
    }

    file
}

/// Read the Iceberg connector's custom metrics into a `name -> value` map.
pub(super) fn iceberg_connector_metrics(pipeline: &Controller) -> HashMap<String, f64> {
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

/// Read a snapshot of an Iceberg table into `relation`, and from there to a temporary JSON
/// file.
///
/// `table_properties` are set on the input relation, the way table-level SQL
/// `WITH` properties (e.g., `skip_unused_columns`) reach the connector.
///
/// `config` is the connector's transport config as a JSON object. This function
/// forces `mode = snapshot`. Returns the output file and the connector's custom
/// metrics captured just before the pipeline is stopped.
pub(super) fn iceberg_snapshot_to_json(
    engine: &dyn PipelineTestEngine,
    relation: &Relation,
    table_properties: &[(&str, &str)],
    config: Value,
) -> (NamedTempFile, HashMap<String, f64>) {
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

    let (input_pipeline, err_receiver) = iceberg_input_pipeline(
        engine,
        relation,
        table_properties,
        config,
        &json_file.path().display().to_string(),
    );
    input_pipeline.start();
    wait(
        || input_pipeline.status().pipeline_complete() || !err_receiver.is_empty(),
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

/// Build a pipeline that reads from an Iceberg table into `relation` and writes to a JSON
/// file.
pub(super) fn iceberg_input_pipeline(
    engine: &dyn PipelineTestEngine,
    relation: &Relation,
    table_properties: &[(&str, &str)],
    config: Value,
    output_file_path: &str,
) -> (Controller, Receiver<String>) {
    iceberg_input_pipeline_with_storage(
        engine,
        relation,
        table_properties,
        config,
        output_file_path,
        None,
        false,
    )
}

/// Like [`iceberg_input_pipeline`], with storage in `storage_dir`, if any, so
/// that the pipeline can checkpoint and resume, and with the input connector
/// `distributed` if requested.
pub(super) fn iceberg_input_pipeline_with_storage(
    engine: &dyn PipelineTestEngine,
    relation: &Relation,
    table_properties: &[(&str, &str)],
    config: Value,
    output_file_path: &str,
    storage_dir: Option<&Path>,
    distributed: bool,
) -> (Controller, Receiver<String>) {
    init_test_logger();

    // Create controller.
    let mut config = json!({
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
    });
    if let Some(storage_dir) = storage_dir {
        config["storage_config"] = json!({"path": storage_dir});
    }
    if distributed {
        config["inputs"]["test_input1"]["distributed"] = json!(true);
    }
    let config = serde_json::from_value(config).unwrap();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();

    // A checkpoint needs a persistent id for the output.
    let persistent_output_id = storage_dir.map(|_| "output".to_string());
    let circuit = TestCircuit::Identity {
        relation: relation.clone(),
        persistent_output_ids: vec![persistent_output_id],
        input_properties: table_properties
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        materialized: true,
        persistent_ids: true,
        table_version: 0,
        attribute_defaults: BTreeMap::new(),
    };
    let controller = engine
        .controller(
            &circuit,
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

/// Returns `cents / 100` as the default JSON flavor writes a DECIMAL: the shortest form.
fn decimal(cents: usize) -> Value {
    let text = format!("{}.{:02}", cents / 100, cents % 100);
    serde_json::from_str(text.trim_end_matches('0').trim_end_matches('.')).unwrap()
}

/// Returns the row of [`relations::basic`] with id `id` and the default other columns.
fn basic(id: usize) -> Value {
    json!({ "id": id, "b": false, "i": null, "s": "" })
}

/// Generate up to `max_records` unique records of [`relation`], in the default JSON flavor.
///
/// Keep the callers under ~200k: the nested columns cost the Python writer
/// 1.4 GiB there, and three of these tests run at once.
pub(super) fn data(n_records: usize) -> Vec<Value> {
    let mut result = Vec::with_capacity(n_records);

    let mut time =
        NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    for i in 0..n_records {
        result.push(json!({
            "b": i % 2 != 0,
            "i": i as i32,
            "l": i as i64,
            "r": i as f32,
            "d": i as f64,
            "dec": decimal(i),
            "dt": time.date().format("%Y-%m-%d").to_string(),
            "tm": time.time().format("%H:%M:%S%.f").to_string(),
            "ts": time.format("%F %T%.f").to_string(),
            "s": format!("s{i}"),
            "fixed": [0, 0, 0, 0, 0],
            "varbin": [0, 0, 0, 0, 0],
            "tstz": time.and_utc().format("%F %T%.f%:z").to_string(),
            "string_array": [format!("a{i}"), format!("b{i}")],
            "struct1": basic(i),
            "struct_array": [basic(i)],
            "string_string_map": { format!("k{i}"): format!("v{i}") },
            "string_struct_map": { format!("k{i}"): basic(i) },
        }));

        time += TimeDelta::seconds(1);
    }

    result
}

#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_unordered(engine: &dyn PipelineTestEngine) {
    iceberg_localfs_input_test(engine, 200_000, json!({}), &|_| true);
}

#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_ordered(engine: &dyn PipelineTestEngine) {
    iceberg_localfs_input_test(
        engine,
        200_000,
        json!({ "timestamp_column": "ts" }),
        &|_| true,
    );
}

#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_ordered_with_filter(engine: &dyn PipelineTestEngine) {
    iceberg_localfs_input_test(
        engine,
        200_000,
        json!({ "timestamp_column": "ts", "snapshot_filter": "i >= 10000" }),
        &|x| x["i"].as_i64().unwrap() >= 10000,
    );
}

/// A filter that keeps rows throughout the table's time range: an ordered read must apply it
/// to the rows of each range, not only to the bounds of the ranges.
#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_ordered_with_row_filter(engine: &dyn PipelineTestEngine) {
    iceberg_localfs_input_test(
        engine,
        100_000,
        json!({ "timestamp_column": "ts", "snapshot_filter": "b = true" }),
        &|x| x["b"].as_bool().unwrap(),
    );
}

/// A single parser task must ingest the whole snapshot correctly (the parallel
/// path defaults to 4 parsers and is covered by the tests above).
#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_single_parser(engine: &dyn PipelineTestEngine) {
    iceberg_localfs_input_test(engine, 100_000, json!({ "num_parsers": 1 }), &|_| true);
}

/// `transaction_mode = snapshot` on an unordered read ingests the whole snapshot
/// in exactly one Feldera transaction; the ingested data must be identical to a
/// non-transactional read.
#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_transactional(engine: &dyn PipelineTestEngine) {
    let metrics = iceberg_localfs_input_test(
        engine,
        100_000,
        json!({ "transaction_mode": "snapshot" }),
        &|_| true,
    );
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
#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_ordered_transactional(engine: &dyn PipelineTestEngine) {
    let metrics = iceberg_localfs_input_test(
        engine,
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
fn create_localfs_table(data: &[Value], extra_columns: bool) -> String {
    let table_dir = tempfile::TempDir::new().unwrap();
    let table_path = table_dir.path().display().to_string();

    let ndjson_file = data_to_ndjson(data);
    println!("wrote test data to {}", ndjson_file.path().display());

    // Uncomment to inspect output parquet files produced by the test.
    std::mem::forget(table_dir);

    let mut extra: Vec<&str> = Vec::new();
    if extra_columns {
        extra.push("--extra-columns");
    }
    run_table_script(&table_path, ndjson_file.path(), &extra)
}

/// A `datetime` read must land on the snapshot current at that time.
///
/// The connector picks the last snapshot whose `timestamp-ms` is at or before
/// the requested time, so the test reads the two timestamps back rather than
/// assuming what the writer stamped.
#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_datetime(engine: &dyn PipelineTestEngine) {
    let before = data(200);
    let after = data(400)[200..].to_vec();

    let table_dir = tempfile::TempDir::new().unwrap();
    let table_path = table_dir.path().display().to_string();
    std::mem::forget(table_dir);

    let first_ndjson = data_to_ndjson(&before);
    run_table_script(&table_path, first_ndjson.path(), &[]);
    let second_ndjson = data_to_ndjson(&after);
    let metadata_path = run_table_script(&table_path, second_ndjson.path(), &["--append"]);

    let metadata: Value =
        serde_json::from_str(&std::fs::read_to_string(&metadata_path).unwrap()).unwrap();
    let stamps: Vec<i64> = metadata["snapshot-log"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["timestamp-ms"].as_i64().unwrap())
        .collect();
    assert_eq!(stamps.len(), 2, "expected one snapshot per append");
    assert!(
        stamps[1] > stamps[0],
        "the two appends must land in different milliseconds, got {stamps:?}"
    );

    // Between the appends: the first is visible, the second is not.
    let read_at = chrono::DateTime::from_timestamp_millis((stamps[0] + stamps[1]) / 2)
        .unwrap()
        .to_rfc3339();
    let (json_file, _metrics) = iceberg_snapshot_to_json(
        engine,
        &relation(),
        &[],
        json!({ "metadata_location": metadata_path, "datetime": read_at }),
    );

    let expected = expected_zset(&relation(), &before);
    let zset = output_zset(&relation(), json_file.path());
    assert_eq!(
        zset.len(),
        200,
        "`datetime` must pin the table to the first snapshot"
    );
    assert_eq!(zset, expected);
}

/// Run the table-building script and return the metadata location it prints.
fn run_table_script(table_path: &str, ndjson_path: &Path, extra: &[&str]) -> String {
    let script_path = "../iceberg/src/test/create_test_table_s3.py";

    let mut command = std::process::Command::new("python3");
    command
        .arg(script_path)
        .arg("--catalog=sql")
        .arg(format!("--warehouse-path={table_path}"))
        .arg(format!("--json-file={}", ndjson_path.display()))
        .args(extra);
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
            String::from_utf8(output.stdout).unwrap(),
            String::from_utf8(output.stderr).unwrap()
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
fn iceberg_localfs_input_test(
    engine: &dyn PipelineTestEngine,
    n_records: usize,
    extra_config: Value,
    filter: &dyn Fn(&Value) -> bool,
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

    let (json_file, metrics) = iceberg_snapshot_to_json(engine, &lateness_relation(), &[], config);

    let expected: Vec<Value> = data.into_iter().filter(|row| filter(row)).collect();
    let expected_zset = expected_zset(&lateness_relation(), &expected);
    let zset = output_zset(&lateness_relation(), json_file.path());

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
/// type no test table models) that the connector must ignore because it
/// selects the declared columns instead of `*`.
///
/// With `skip_unused` (the `skip_unused_columns` table property), the
/// connector must additionally not read the nullable `l` column, which the
/// SQL schema marks unused, so `l` comes out NULL. This variant fails if the
/// connector falls back to reading all columns.
fn iceberg_localfs_input_subset_test(engine: &dyn PipelineTestEngine, skip_unused: bool) {
    let data = data(100_000);

    let metadata_path = create_localfs_table(&data, true);

    let table_properties: &[(&str, &str)] = if skip_unused {
        &[("skip_unused_columns", "true")]
    } else {
        &[]
    };

    let (json_file, _metrics) = iceberg_snapshot_to_json(
        engine,
        &subset_relation(),
        table_properties,
        json!({ "metadata_location": metadata_path }),
    );

    let expected: Vec<Value> = data
        .into_iter()
        .map(|x| {
            json!({
                "i": x["i"],
                "s": x["s"],
                "l": if skip_unused { Value::Null } else { x["l"].clone() },
            })
        })
        .collect();
    let expected_zset = expected_zset(&subset_relation(), &expected);
    let zset = output_zset(&subset_relation(), json_file.path());

    assert_eq!(zset, expected_zset);
}

/// The connector reads only the columns the SQL table declares.
#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_subset_schema(engine: &dyn PipelineTestEngine) {
    iceberg_localfs_input_subset_test(engine, false);
}

/// The `skip_unused_columns` table property also drops declared-but-unused
/// columns from the read.
#[engine_test(cfg(feature = "iceberg-tests-fs"))]
pub fn localfs_input_test_skip_unused_columns(engine: &dyn PipelineTestEngine) {
    iceberg_localfs_input_subset_test(engine, true);
}

/// Build an input-only pipeline that reads a local-FS Iceberg snapshot with
/// at-least-once fault tolerance, checkpointing to `storage_dir`. Rebuilding a
/// pipeline with the same `storage_dir` resumes from the latest checkpoint.
fn iceberg_ft_pipeline(
    engine: &dyn PipelineTestEngine,
    extra_config: Value,
    storage_dir: &Path,
) -> Controller {
    init_test_logger();

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

    // A concrete persistent output id is required for checkpointing.
    let circuit = TestCircuit::Identity {
        relation: lateness_relation(),
        persistent_output_ids: vec![Some("output".to_string())],
        input_properties: Vec::new(),
        materialized: true,
        persistent_ids: true,
        table_version: 0,
        attribute_defaults: BTreeMap::new(),
    };
    engine
        .controller(
            &circuit,
            &config,
            Box::new(|e, _| panic!("iceberg ft pipeline: error: {e}")),
        )
        .unwrap()
}

/// Checkpoint-and-suspend the pipeline, then stop it.
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
#[engine_test(
    cfg(feature = "iceberg-tests-fs"),
    requires(Storage, Checkpoints, SuspendResume, FaultTolerance)
)]
pub fn localfs_input_test_resume_completed_snapshot(engine: &dyn PipelineTestEngine) {
    let data = data(100_000);
    let metadata_path = create_localfs_table(&data, false);
    let storage_dir = tempfile::TempDir::new().unwrap();

    // Ordered snapshot so the read is resumable per lateness range.
    let config = json!({ "metadata_location": metadata_path, "timestamp_column": "ts" });

    // First run: ingest the whole snapshot, then checkpoint and suspend.
    let pipeline = iceberg_ft_pipeline(engine, config.clone(), storage_dir.path());
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
    let pipeline = iceberg_ft_pipeline(engine, config, storage_dir.path());
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

#[engine_test(cfg(feature = "iceberg-tests-glue"))]
pub fn glue_s3_input_test(engine: &dyn PipelineTestEngine) {
    // Read delta table unordered.
    let (json_file, _metrics) = iceberg_snapshot_to_json(
        engine,
        &lateness_relation(),
        &[],
        json!({
            "catalog_type": "glue",
            "glue.warehouse": "s3://feldera-ci-iceberg/",
            "table_name": "iceberg_test.test_table_v3",
            "glue.region": env_or("ICEBERG_TEST_REGION", "us-west-1"),
            "s3.region": env_or("ICEBERG_TEST_REGION", "us-west-1"),
        }),
    );

    let zset = output_zset(&lateness_relation(), json_file.path());

    // The data for this test is generated by the Python script, we don't know the
    // exact set of records in the dataset.
    assert_eq!(zset.len(), 2000000);
}

/// The columns of the test table `dev.test_table`.
pub fn s3tables_relation() -> Relation {
    Relation::new(
        "iceberg_s3tables".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("name".into(), ColumnType::varchar(true)),
            Field::new("created_at".into(), ColumnType::timestamp(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

#[engine_test(cfg(feature = "iceberg-tests-s3tables"))]
pub fn s3tables_input_test(engine: &dyn PipelineTestEngine) {
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
    let (json_file, _metrics) = iceberg_snapshot_to_json(
        engine,
        &s3tables_relation(),
        &[],
        json!({
            "catalog_type": "s3tables",
            "s3tables.table-bucket-arn": "arn:aws:s3tables:us-west-1:737834633458:bucket/iceberg-test",
            "table_name": "dev.test_table",
            "s3tables.region": "us-west-1",
            "s3.region": "us-west-1",
        }),
    );

    let zset = output_zset(&s3tables_relation(), json_file.path());

    assert_eq!(zset.len(), 100);
}

#[engine_test(cfg(feature = "iceberg-tests-rest"))]
pub fn rest_s3_input_test(engine: &dyn PipelineTestEngine) {
    // Read delta table unordered.
    let (json_file, _metrics) = iceberg_snapshot_to_json(
        engine,
        &lateness_relation(),
        &[],
        json!({
            "catalog_type": "rest",
            "rest.uri": "http://localhost:8181",
            "rest.warehouse": "s3://feldera-ci-iceberg/",
            "table_name": "iceberg_test.test_table_v3",
            "s3.region": env_or("ICEBERG_TEST_REGION", "us-west-1"),
        }),
    );

    let zset = output_zset(&lateness_relation(), json_file.path());

    assert_eq!(zset.len(), 2000000);
}

/// The columns of the test tables: a column of each type that the connector reads.
fn columns() -> Vec<Field> {
    let basic_struct = || ColumnType::structure(false, &relations::basic_columns());
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
        Field::new("fixed".into(), ColumnType::fixed(5, false)),
        Field::new("varbin".into(), ColumnType::varbinary(false)),
        Field::new("tstz".into(), ColumnType::timestamp_tz(false)),
        Field::new(
            "string_array".into(),
            ColumnType::array(false, ColumnType::varchar(false)),
        ),
        Field::new("struct1".into(), basic_struct()),
        Field::new(
            "struct_array".into(),
            ColumnType::array(false, basic_struct()),
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
            ColumnType::map(false, ColumnType::varchar(false), basic_struct()),
        ),
    ]
}

/// The table that reads a test table without ordering it.
pub fn relation() -> Relation {
    Relation::new("iceberg".into(), columns(), false, BTreeMap::new())
}

/// [`relation`] with a lateness on `ts`, which a `timestamp_column` of the connector needs.
pub fn lateness_relation() -> Relation {
    let fields = columns()
        .into_iter()
        .map(|field| match field.name.name().as_str() {
            "ts" => field.with_lateness("interval '10 days'"),
            _ => field,
        })
        .collect();
    Relation::new("iceberg_lateness".into(), fields, false, BTreeMap::new())
}

/// A table that declares only a few columns of a test table, one of them unused.
pub fn subset_relation() -> Relation {
    Relation::new(
        "iceberg_subset".into(),
        vec![
            Field::new("i".into(), ColumnType::int(false)),
            Field::new("s".into(), ColumnType::varchar(false)),
            Field::new("l".into(), ColumnType::bigint(true)).with_unused(true),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Returns the environment variable `key`, or `default` if it is not set.
pub(super) fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Connector transport config for a REST-catalog table `mode` on the test store.
fn rest_follow_config(table: &str, mode: &str) -> Value {
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
fn rest_follow_config_with(table: &str, mode: &str, key: &str, value: Value) -> Value {
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
fn follow_table_op(op: &str, table: &str, chunk: &[Value]) -> i64 {
    let ndjson = data_to_ndjson(chunk);
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

/// Create the REST-catalog test table `table` without any snapshot.
fn follow_table_create_empty(table: &str) {
    let script = "../iceberg/src/test/follow_table.py";
    let python = env_or("FELDERA_ICEBERG_PYTHON", "python3");
    let output = std::process::Command::new(python)
        .arg(script)
        .arg("--op=create-empty")
        .arg(format!("--table={table}"))
        .output()
        .unwrap_or_else(|e| panic!("failed to run '{script}': {e}"));
    if !output.status.success() {
        panic!(
            "'{script} --op=create-empty' failed (status {}):\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

/// Read a single custom metric of the Iceberg connector, or `0.0` if absent.
fn iceberg_metric(pipeline: &Controller, name: &str) -> f64 {
    iceberg_connector_metrics(pipeline)
        .get(name)
        .copied()
        .unwrap_or(0.0)
}

/// Wait for the catchup target gauge to reset. The connector clears it just
/// after it queues the commit, so it can lag the output.
fn wait_catchup_window_closed(pipeline: &Controller) {
    wait(
        || {
            iceberg_metric(
                pipeline,
                "input_connector_iceberg_catchup_target_sequence_number",
            ) == -1.0
        },
        10_000,
    )
    .expect("catchup target gauge never reset");
}

/// Sum of the records the connector has ingested so far (snapshot phase plus
/// follow phase), read from its custom metrics.
fn ingested_records(pipeline: &Controller) -> u64 {
    (iceberg_metric(pipeline, "input_connector_iceberg_snapshot_records_total")
        + iceberg_metric(pipeline, "input_connector_iceberg_follow_records_total")) as u64
}

/// Run `body` against a running follow-mode pipeline, then stop it. The output
/// file receives `insert_delete` JSON.
fn with_follow_pipeline<F>(engine: &dyn PipelineTestEngine, table: &str, mode: &str, body: F)
where
    F: FnOnce(&Controller, &Path),
{
    with_follow_pipeline_cfg(engine, rest_follow_config(table, mode), body)
}

/// Like [`with_follow_pipeline`], but takes a fully built connector config so a
/// test can add options such as a `snapshot_id` follow start point.
fn with_follow_pipeline_cfg<F>(engine: &dyn PipelineTestEngine, config: Value, body: F)
where
    F: FnOnce(&Controller, &Path),
{
    let json_file = NamedTempFile::new().unwrap();
    let (pipeline, err_receiver) = iceberg_input_pipeline(
        engine,
        &lateness_relation(),
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

/// The Z-set of the follow pipeline's output file, ignoring a partly written last line.
fn follow_output_zset(path: &Path) -> ZSet {
    output_zset(&lateness_relation(), path)
}

/// The all-`+1` Z-set the connector should produce for `data`.
fn follow_expected_zset(data: &[Value]) -> ZSet {
    expected_zset(&lateness_relation(), data)
}

/// `snapshot_and_follow`: the initial snapshot is read, then a second snapshot
/// committed before startup is caught up via follow. Every row lands once.
#[engine_test(cfg(feature = "iceberg-tests-follow"), services(S3))]
pub fn rest_follow_snapshot_and_follow(engine: &dyn PipelineTestEngine) {
    let all = data(10);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.snapshot_and_follow");

    // Two snapshots exist before the connector starts.
    follow_table_op("create", table, &all[..5]);
    follow_table_op("append", table, &all[5..]);

    with_follow_pipeline(
        engine,
        table,
        "snapshot_and_follow",
        |pipeline, out_path| {
            wait(|| ingested_records(pipeline) >= 10, 120_000)
                .expect("timed out ingesting snapshot + follow");
            // Let the output connector flush the ingested rows.
            wait(|| output_record_count(out_path) >= 10, 60_000).expect("timed out writing output");

            let zset = follow_output_zset(out_path);
            assert_eq!(zset.len(), 10);
            assert_eq!(zset, follow_expected_zset(&all));
        },
    );
}

/// `snapshot_and_follow`: a connector that resumes from a checkpoint taken
/// while it followed the table continues to follow it, without reading the
/// snapshot again.
#[engine_test(
    cfg(feature = "iceberg-tests-follow"),
    requires(Storage, Checkpoints, SuspendResume),
    services(S3)
)]
pub fn rest_follow_resume_does_not_reread_snapshot(engine: &dyn PipelineTestEngine) {
    let all = data(10);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.resume_follow");
    let storage_dir = tempfile::TempDir::new().unwrap();
    let run = |out: &NamedTempFile| {
        iceberg_input_pipeline_with_storage(
            engine,
            &lateness_relation(),
            &[],
            rest_follow_config(table, "snapshot_and_follow"),
            &out.path().display().to_string(),
            Some(storage_dir.path()),
            false,
        )
    };

    // Read the snapshot, then suspend while following.
    follow_table_op("create", table, &all[..5]);
    let first_out = NamedTempFile::new().unwrap();
    let (pipeline, _errors) = run(&first_out);
    pipeline.start();
    wait(|| output_record_count(first_out.path()) >= 5, 120_000)
        .expect("timed out reading the snapshot");
    let (sender, receiver) = std::sync::mpsc::channel();
    pipeline.start_suspend(Box::new(move |result| sender.send(result).unwrap()));
    receiver
        .recv_timeout(std::time::Duration::from_secs(100))
        .expect("timed out suspending")
        .expect("suspend failed");
    pipeline.stop().unwrap();

    // Resume after another snapshot.  Only its rows are new.
    follow_table_op("append", table, &all[5..]);
    let second_out = NamedTempFile::new().unwrap();
    let (pipeline, _errors) = run(&second_out);
    pipeline.start();
    wait(|| output_record_count(second_out.path()) >= 5, 120_000)
        .expect("timed out following the table");
    // Give a read of the snapshot time to show up.
    std::thread::sleep(std::time::Duration::from_secs(5));
    pipeline.stop().unwrap();

    let zset = follow_output_zset(second_out.path());
    assert_eq!(zset.len(), 5, "the snapshot was read again");
    assert_eq!(zset, follow_expected_zset(&all[5..]));
}

/// `snapshot_and_follow` on a table with no snapshot, read by a distributed
/// connector in a transaction: the empty snapshot's transaction commits, so it
/// does not hold back the pipeline until the table gets a snapshot.  A
/// connector that resumes then follows the table from its first snapshot.
#[engine_test(
    cfg(feature = "iceberg-tests-follow"),
    requires(Storage, Checkpoints, SuspendResume),
    services(S3)
)]
pub fn rest_follow_empty_table(engine: &dyn PipelineTestEngine) {
    let all = data(5);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.empty_follow");
    let storage_dir = tempfile::TempDir::new().unwrap();
    let run = |out: &NamedTempFile| {
        iceberg_input_pipeline_with_storage(
            engine,
            &lateness_relation(),
            &[],
            rest_follow_config_with(
                table,
                "snapshot_and_follow",
                "transaction_mode",
                json!("snapshot"),
            ),
            &out.path().display().to_string(),
            Some(storage_dir.path()),
            true,
        )
    };

    follow_table_create_empty(table);
    let first_out = NamedTempFile::new().unwrap();
    let (pipeline, _errors) = run(&first_out);
    pipeline.start();
    wait(
        || iceberg_metric(&pipeline, "input_connector_iceberg_phase") > 0.0,
        60_000,
    )
    .expect("timed out reaching follow mode");
    wait(
        || {
            pipeline.api_status(false).global_metrics.transaction_status
                == TransactionStatus::NoTransaction
        },
        10_000,
    )
    .expect("the empty snapshot's transaction stayed open");
    let (sender, receiver) = std::sync::mpsc::channel();
    pipeline.start_suspend(Box::new(move |result| sender.send(result).unwrap()));
    receiver
        .recv_timeout(std::time::Duration::from_secs(100))
        .expect("timed out suspending")
        .expect("suspend failed");
    pipeline.stop().unwrap();

    // The table gets its first snapshot.  The connector follows it.
    follow_table_op("append", table, &all);
    let second_out = NamedTempFile::new().unwrap();
    let (pipeline, _errors) = run(&second_out);
    pipeline.start();
    wait(|| output_record_count(second_out.path()) >= 5, 120_000)
        .expect("timed out following the table");
    assert_eq!(
        iceberg_metric(&pipeline, "input_connector_iceberg_follow_records_total"),
        5.0
    );
    pipeline.stop().unwrap();

    let zset = follow_output_zset(second_out.path());
    assert_eq!(zset.len(), 5);
    assert_eq!(zset, follow_expected_zset(&all));
}

/// `follow`: no initial snapshot; a snapshot committed after startup is tailed.
/// Rows present before the start snapshot are not ingested.
#[engine_test(cfg(feature = "iceberg-tests-follow"), services(S3))]
pub fn rest_follow_live_append(engine: &dyn PipelineTestEngine) {
    let all = data(10);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.live_append");

    // The connector starts following after this snapshot, so these rows are
    // not ingested.
    follow_table_op("create", table, &all[..5]);

    with_follow_pipeline(engine, table, "follow", |pipeline, out_path| {
        // Nothing to ingest until a new snapshot appears.
        follow_table_op("append", table, &all[5..]);

        wait(|| ingested_records(pipeline) >= 5, 120_000)
            .expect("timed out tailing the appended snapshot");
        wait(|| output_record_count(out_path) >= 5, 60_000).expect("timed out writing output");

        let zset = follow_output_zset(out_path);
        assert_eq!(zset.len(), 5);
        assert_eq!(zset, follow_expected_zset(&all[5..]));
    });
}

/// `follow` with an explicit `snapshot_id` start point. Three snapshots (A, B,
/// C) exist before the connector starts; following from B must ingest only C's
/// rows, never A's or B's, since `snapshots_after` walks the ancestry back to B.
#[engine_test(cfg(feature = "iceberg-tests-follow"), services(S3))]
pub fn rest_follow_start_from_snapshot_id(engine: &dyn PipelineTestEngine) {
    let all = data(15);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.start_from_id");

    follow_table_op("create", table, &all[..5]); // snapshot A
    let snapshot_b = follow_table_op("append", table, &all[5..10]); // snapshot B
    follow_table_op("append", table, &all[10..]); // snapshot C

    let config = rest_follow_config_with(table, "follow", "snapshot_id", json!(snapshot_b));
    with_follow_pipeline_cfg(engine, config, |pipeline, out_path| {
        wait(|| ingested_records(pipeline) >= 5, 120_000)
            .expect("timed out following from the pinned snapshot");
        wait(|| output_record_count(out_path) >= 5, 60_000).expect("timed out writing output");

        let zset = follow_output_zset(out_path);
        assert_eq!(zset.len(), 5);
        assert_eq!(zset, follow_expected_zset(&all[10..]));
    });
}

/// `follow`: `end_snapshot_id` stops the connector after fully ingesting the
/// named snapshot. Three snapshots (A, B, C) exist before startup; following
/// from A with `end_snapshot_id = B` must ingest only B's rows, reach the
/// completed phase, and never ingest C, even though C is in the same catch-up
/// batch as B.
#[engine_test(cfg(feature = "iceberg-tests-follow"), services(S3))]
pub fn rest_follow_end_snapshot_id(engine: &dyn PipelineTestEngine) {
    let all = data(15);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.end_snapshot");

    let snapshot_a = follow_table_op("create", table, &all[..5]); // snapshot A
    let snapshot_b = follow_table_op("append", table, &all[5..10]); // snapshot B
    follow_table_op("append", table, &all[10..]); // snapshot C

    let mut config = rest_follow_config_with(table, "follow", "snapshot_id", json!(snapshot_a));
    config
        .as_object_mut()
        .unwrap()
        .insert("end_snapshot_id".to_string(), json!(snapshot_b));

    with_follow_pipeline_cfg(engine, config, |pipeline, out_path| {
        // Phase 2 = completed: only set once the end snapshot is reached.
        wait(
            || iceberg_metric(pipeline, "input_connector_iceberg_phase") == 2.0,
            120_000,
        )
        .expect("timed out reaching the end snapshot");
        wait(|| output_record_count(out_path) >= 5, 60_000).expect("timed out writing output");

        // Stopped at B: C's rows are never ingested and exactly one follow
        // snapshot was processed.
        assert_eq!(ingested_records(pipeline), 5);
        assert_eq!(
            iceberg_metric(pipeline, "input_connector_iceberg_follow_snapshots_total"),
            1.0
        );
        let zset = follow_output_zset(out_path);
        assert_eq!(zset.len(), 5);
        assert_eq!(zset, follow_expected_zset(&all[5..10]));
    });
}

/// `transaction_mode = always`: each followed snapshot forms its own Feldera
/// transaction. Following from A over two pre-committed snapshots (B, C) starts
/// two follow transactions.
#[engine_test(cfg(feature = "iceberg-tests-follow"), services(S3))]
pub fn rest_follow_transaction_always(engine: &dyn PipelineTestEngine) {
    let all = data(15);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.txn_always");

    let snapshot_a = follow_table_op("create", table, &all[..5]); // A
    follow_table_op("append", table, &all[5..10]); // B
    follow_table_op("append", table, &all[10..]); // C

    let mut config = rest_follow_config_with(table, "follow", "snapshot_id", json!(snapshot_a));
    config
        .as_object_mut()
        .unwrap()
        .insert("transaction_mode".to_string(), json!("always"));

    with_follow_pipeline_cfg(engine, config, |pipeline, out_path| {
        wait(|| ingested_records(pipeline) >= 10, 120_000).expect("timed out following B and C");
        wait(|| output_record_count(out_path) >= 10, 60_000).expect("timed out writing output");

        // One transaction per followed snapshot: B and C. (Reverting the
        // per-snapshot allocation drops this below 2.)
        assert_eq!(
            iceberg_metric(
                pipeline,
                "input_connector_iceberg_follow_transaction_starts"
            ),
            2.0
        );
        let zset = follow_output_zset(out_path);
        assert_eq!(zset.len(), 10);
        assert_eq!(zset, follow_expected_zset(&all[5..]));
    });
}

/// `transaction_mode = catchup`: all snapshots caught up in one poll form a
/// single Feldera transaction. Following from A over two pre-committed snapshots
/// (B, C), both seen in the first poll, starts exactly one follow transaction.
#[engine_test(cfg(feature = "iceberg-tests-follow"), services(S3))]
pub fn rest_follow_transaction_catchup(engine: &dyn PipelineTestEngine) {
    let all = data(15);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.txn_catchup");

    let snapshot_a = follow_table_op("create", table, &all[..5]); // A
    follow_table_op("append", table, &all[5..10]); // B
    follow_table_op("append", table, &all[10..]); // C

    let mut config = rest_follow_config_with(table, "follow", "snapshot_id", json!(snapshot_a));
    config
        .as_object_mut()
        .unwrap()
        .insert("transaction_mode".to_string(), json!("catchup"));

    with_follow_pipeline_cfg(engine, config, |pipeline, out_path| {
        wait(|| ingested_records(pipeline) >= 10, 120_000).expect("timed out following B and C");
        wait(|| output_record_count(out_path) >= 10, 60_000).expect("timed out writing output");

        // B and C are committed before startup, so the first catch-up poll sees
        // both and groups them into one transaction. (In `always` this is 2;
        // that difference is the point of the mode.)
        assert_eq!(
            iceberg_metric(
                pipeline,
                "input_connector_iceberg_follow_transaction_starts"
            ),
            1.0
        );
        wait_catchup_window_closed(pipeline);
        let zset = follow_output_zset(out_path);
        assert_eq!(zset.len(), 10);
        assert_eq!(zset, follow_expected_zset(&all[5..]));
    });
}

/// `transaction_mode = catchup` with `end_snapshot_id` landing mid-batch: the
/// catchup window opens targeting the batch's last snapshot (C), but the end
/// bound B is reached first. The open catchup transaction (holding B's rows)
/// must be committed at the end bound, C must never be read, and the target
/// gauge must reset. This is the one interaction the plain `end_snapshot_id`
/// test (transaction_mode `none`) does not exercise.
#[engine_test(cfg(feature = "iceberg-tests-follow"), services(S3))]
pub fn rest_follow_transaction_catchup_end_snapshot_id(engine: &dyn PipelineTestEngine) {
    let all = data(15);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.txn_catchup_end");

    let snapshot_a = follow_table_op("create", table, &all[..5]); // A
    let snapshot_b = follow_table_op("append", table, &all[5..10]); // B
    follow_table_op("append", table, &all[10..]); // C

    let mut config = rest_follow_config_with(table, "follow", "snapshot_id", json!(snapshot_a));
    {
        let object = config.as_object_mut().unwrap();
        object.insert("transaction_mode".to_string(), json!("catchup"));
        object.insert("end_snapshot_id".to_string(), json!(snapshot_b));
    }

    with_follow_pipeline_cfg(engine, config, |pipeline, out_path| {
        // Phase 2 = completed: only set once the end snapshot is reached.
        wait(
            || iceberg_metric(pipeline, "input_connector_iceberg_phase") == 2.0,
            120_000,
        )
        .expect("timed out reaching the end snapshot");
        wait(|| output_record_count(out_path) >= 5, 60_000).expect("timed out writing output");

        // Only B ingested; C never read even though it shared the catch-up batch.
        assert_eq!(ingested_records(pipeline), 5);
        assert_eq!(
            iceberg_metric(pipeline, "input_connector_iceberg_follow_snapshots_total"),
            1.0
        );
        // The end bound committed the open catchup transaction: exactly one
        // follow transaction started, and the target gauge reset on close.
        assert_eq!(
            iceberg_metric(
                pipeline,
                "input_connector_iceberg_follow_transaction_starts"
            ),
            1.0
        );
        wait_catchup_window_closed(pipeline);
        let zset = follow_output_zset(out_path);
        assert_eq!(zset.len(), 5);
        assert_eq!(zset, follow_expected_zset(&all[5..10]));
    });
}

/// Covers the follow delete path (removed files), which the append-only follow
/// tests miss. A copy-on-write overwrite removes the old files and adds new
/// ones; drop one row and edit another, then expect the four survivors.
#[engine_test(cfg(feature = "iceberg-tests-follow"), services(S3))]
pub fn rest_follow_copy_on_write_delete(engine: &dyn PipelineTestEngine) {
    let all = data(5);
    let ns = env_or("FELDERA_ICEBERG_NAMESPACE", "follow_ns");
    let table = &format!("{ns}.cow_delete");

    follow_table_op("create", table, &all);

    let mut updated = all[..4].to_vec();
    updated[2]["s"] = json!("cow-updated");

    with_follow_pipeline(
        engine,
        table,
        "snapshot_and_follow",
        |pipeline, out_path| {
            wait(|| ingested_records(pipeline) >= 5, 120_000)
                .expect("timed out ingesting snapshot");
            follow_table_op("overwrite", table, &updated);
            // Overwrite reads 5 deletes + 4 inserts, so ingested reaches 14.
            wait(|| ingested_records(pipeline) >= 14, 120_000)
                .expect("timed out following the overwrite");
            // Deletes and inserts may reach the output in separate steps, so wait
            // for the final state, not a line count.
            let expected = follow_expected_zset(&updated);
            let _ = wait(|| follow_output_zset(out_path) == expected, 60_000);
            assert_eq!(follow_output_zset(out_path), expected);
        },
    );
}

/// Number of complete `insert_delete` records written to the output file so
/// far (one JSON value per line).
fn output_record_count(path: &Path) -> usize {
    std::fs::read(path)
        .map(|bytes| bytes.iter().filter(|&&b| b == b'\n').count())
        .unwrap_or(0)
}

/// Returns the rows of `relation` that the output file `path` holds, ignoring a partly
/// written last line.
pub(super) fn output_zset(relation: &Relation, path: &Path) -> ZSet {
    let bytes = std::fs::read(path).unwrap();
    let complete = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    read::insert_delete_updates(relation, &bytes[..complete])
        .unwrap()
        .zset()
        .unwrap()
}

/// Returns `rows`, rows of `relation` in the default JSON flavor, each inserted once.
pub(super) fn expected_zset(relation: &Relation, rows: &[Value]) -> ZSet {
    let rows: Vec<String> = rows
        .iter()
        .map(|row| read::arrow_form(relation, row.clone()).unwrap().to_string())
        .collect();
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
    Updates::from_json(published_schema(relation), &rows)
        .unwrap()
        .zset()
        .unwrap()
}
