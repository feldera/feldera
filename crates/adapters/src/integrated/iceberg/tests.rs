//! The Iceberg input connector reading local tables in pipelines: whole snapshots, ordered and
//! unordered, filtered, in transactions, at a point in time, through a subset of the columns,
//! and resumed from a checkpoint.
//!
//! The cases build their tables with `pyiceberg`, through the scripts in
//! `crates/iceberg/src/test`, so they need a CPython `python3` on the `PATH` with the packages
//! of `crates/iceberg/src/test/requirements.txt`. The README in that directory describes these
//! cases, which run with the `iceberg-tests-fs` feature, and `follow_tests`, `glue_tests`,
//! `rest_tests`, and `s3tables_tests`, which share the fixtures of this module.

use crate::test::engine_test;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use chrono::{NaiveDateTime, TimeDelta};
use crossbeam::channel::Receiver;
use feldera_adapterlib::test::{Updates, ZSet, wait};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use tracing::info;

use crate::Controller;
use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::{init_test_logger, published_schema, read, relations};

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
        attribute_defaults: Vec::new(),
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
        attribute_defaults: Vec::new(),
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
