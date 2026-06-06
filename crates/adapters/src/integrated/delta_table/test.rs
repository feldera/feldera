use crate::Controller;
use crate::adhoc::execute_sql;
use crate::format::parquet::relation_to_arrow_fields;
use crate::format::parquet::test::load_parquet_file;
use crate::integrated::delta_table::delta_input_serde_config;
use crate::test::data::DeltaTestKey;
use crate::test::{
    DeltaTestStruct, file_to_zset, list_files_recursive, test_circuit, test_circuit_with_index,
    wait,
};
use crate::{Catalog, CircuitCatalog};
use arrow::datatypes::Schema as ArrowSchema;
use chrono::NaiveDate;
use dbsp::circuit::CircuitConfig;
#[cfg(feature = "delta-s3-test")]
use dbsp::typed_batch::DynBatchReader;
use dbsp::utils::Tup2;
use dbsp::{DBData, DBSPHandle, OrdZSet, Runtime};
use delta_kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, DeltaTableBuilder, ensure_table_uri};
use feldera_sqllib::Variant;
use feldera_types::config::PipelineConfig;
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::{Field, Relation, SqlIdentifier};
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
use serde_json::{Value, json};
#[cfg(feature = "delta-s3-test")]
use serial_test::{parallel, serial};
use std::cmp::min;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::Write;
use std::mem::forget;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
    wait(|| input_pipeline.pipeline_complete(), 400_000).expect("timeout");
    input_pipeline.stop().unwrap();

    info!("Read delta snapshot in {:?}", start.elapsed());

    json_file
}

const DELTA_TEST_INPUT_ENDPOINT: &str = "test_input1";

/// Completed Delta table version reported by the input connector waterline.
fn pipeline_completed_version(pipeline: &Controller) -> Option<i64> {
    pipeline
        .status()
        .input_status()
        .values()
        .next()
        .and_then(|s| s.completed_frontier.completed_watermark())
        .and_then(|w| w.metadata["version"].as_i64())
}

/// One deterministic test row (even `bigint` so `bigint % 2 = 0` filters pass).
fn delta_test_record(bigint: i64) -> DeltaTestStruct {
    let mut runner = TestRunner::default();
    let mut record = DeltaTestStruct::arbitrary()
        .new_tree(&mut runner)
        .unwrap()
        .current();
    record.bigint = bigint;
    record
}

/// Append a single row, producing exactly one new Delta table version.
async fn append_table_version(
    table: DeltaTable,
    arrow_schema: &ArrowSchema,
    record: &DeltaTestStruct,
) -> DeltaTable {
    write_data_to_table(table, arrow_schema, std::slice::from_ref(record)).await
}

fn delta_connector_counter(pipeline: &Controller, metric_name: &str) -> u64 {
    delta_connector_gauge(pipeline, metric_name) as u64
}

fn delta_connector_gauge(pipeline: &Controller, metric_name: &str) -> f64 {
    let endpoint_id = pipeline
        .input_endpoint_id_by_name(DELTA_TEST_INPUT_ENDPOINT)
        .expect("delta input endpoint must exist");
    let custom_metrics = pipeline
        .status()
        .input_status()
        .get(&endpoint_id)
        .and_then(|status| status.custom_metrics.clone())
        .expect("delta input connector must expose custom metrics");
    custom_metrics
        .metrics()
        .iter()
        .find(|(name, ..)| *name == metric_name)
        .map(|(_, _, _, value)| *value)
        .unwrap_or(0.0)
}

fn delta_version_gauge(pipeline: &Controller, metric_name: &str) -> Option<i64> {
    let version = delta_connector_gauge(pipeline, metric_name);
    if version < 0.0 {
        None
    } else {
        Some(version as i64)
    }
}

/// Last Delta table version ingested by the connector (`None` if none yet).
fn delta_last_ingested_version(pipeline: &Controller) -> Option<i64> {
    delta_version_gauge(pipeline, "input_connector_delta_last_ingested_version")
}

/// Target Delta table version for the in-flight catchup window (`None` if none).
fn delta_catchup_target_version(pipeline: &Controller) -> Option<i64> {
    delta_version_gauge(pipeline, "input_connector_delta_catchup_target_version")
}

/// Number of Feldera follow transactions the Delta input connector started.
fn delta_follow_transaction_starts(pipeline: &Controller) -> u64 {
    delta_connector_counter(pipeline, "input_connector_delta_follow_transaction_starts")
}

/// Number of Feldera snapshot transactions the Delta input connector started.
fn delta_snapshot_transaction_starts(pipeline: &Controller) -> u64 {
    delta_connector_counter(
        pipeline,
        "input_connector_delta_snapshot_transaction_starts",
    )
}

/// Connector phase (`0` = loading snapshot, `1` = follow, `2` = completed).
fn delta_connector_phase(pipeline: &Controller) -> u64 {
    delta_connector_counter(pipeline, "input_connector_delta_phase")
}

async fn wait_for_delta_connector_phase(pipeline: &Controller, phase: u64, timeout_ms: u64) {
    let start = Instant::now();
    loop {
        if delta_connector_phase(pipeline) == phase {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_millis(timeout_ms),
            "timeout waiting for connector phase {phase} (current: {})",
            delta_connector_phase(pipeline),
        );
        sleep(Duration::from_millis(10)).await;
    }
}

/// Outcome of [`run_catchup_lag_experiment`].
struct CatchupLagExperimentResult {
    /// Total `input_connector_delta_snapshot_transaction_starts` when the experiment finishes.
    snapshot_transaction_starts: u64,
    /// Feldera follow transactions started in each pause/write/resume round.
    follow_transactions_per_round: Vec<u64>,
    /// Last input waterline Delta version observed (if any).
    completed_version: Option<i64>,
    /// Whether the input endpoint was unhealthy when the experiment ended.
    ended_unhealthy: bool,
}

/// Configuration for [`run_catchup_lag_experiment`].
struct CatchupLagExperimentOptions<'a> {
    transaction_mode: DeltaTableTransactionMode,
    versions_per_round: &'a [i64],
    end_version: Option<i64>,
    snapshot_rows: Option<i64>,
    max_retries: Option<u32>,
    /// Strip read permission on the input table root before resuming this round (Unix only).
    inject_read_failure_before_resume_round: Option<usize>,
    /// After injection, wait for unhealthy health and restore permissions before continuing.
    clear_read_failure_after_unhealthy: bool,
    /// When true, ingestion must not reach the round's target version; skips output validation.
    expect_ingest_failure: bool,
}

fn catchup_lag_options<'a>(
    transaction_mode: DeltaTableTransactionMode,
    versions_per_round: &'a [i64],
    end_version: Option<i64>,
    snapshot_rows: Option<i64>,
) -> CatchupLagExperimentOptions<'a> {
    CatchupLagExperimentOptions {
        transaction_mode,
        versions_per_round,
        end_version,
        snapshot_rows,
        max_retries: None,
        inject_read_failure_before_resume_round: None,
        clear_read_failure_after_unhealthy: false,
        expect_ingest_failure: false,
    }
}

fn input_endpoint_is_unhealthy(pipeline: &Controller) -> bool {
    pipeline
        .input_endpoint_status(DELTA_TEST_INPUT_ENDPOINT)
        .ok()
        .and_then(|s| s.health)
        .is_some_and(|h| {
            matches!(
                h.status,
                feldera_types::adapter_stats::ConnectorHealthStatus::Unhealthy
            )
        })
}

fn wait_input_endpoint_unhealthy(pipeline: &Controller, timeout_ms: u128) {
    wait(|| input_endpoint_is_unhealthy(pipeline), timeout_ms)
        .unwrap_or_else(|_| panic!("timeout waiting for input connector to become unhealthy"));
}

fn wait_input_endpoint_healthy(pipeline: &Controller, timeout_ms: u128) {
    wait(
        || {
            pipeline
                .input_endpoint_status(DELTA_TEST_INPUT_ENDPOINT)
                .ok()
                .and_then(|s| s.health)
                .is_some_and(|h| {
                    matches!(
                        h.status,
                        feldera_types::adapter_stats::ConnectorHealthStatus::Healthy
                    )
                })
        },
        timeout_ms,
    )
    .unwrap_or_else(|_| panic!("timeout waiting for input connector to become healthy"));
}

/// Build a Delta input pipeline and, for each entry in `versions_per_round`, pause the connector
/// (when needed), append that many Delta commits, resume, and wait for ingestion to catch up.
///
/// With `snapshot_rows`, uses `snapshot_and_follow`: writes that many rows before starting the
/// pipeline (unpaused), waits for the follow phase, and checks that the snapshot used exactly one
/// Feldera transaction before running follow rounds.
async fn run_catchup_lag_experiment(
    opts: CatchupLagExperimentOptions<'_>,
) -> CatchupLagExperimentResult {
    const DELTA_PHASE_FOLLOW: u64 = 1;
    init_logging();

    let CatchupLagExperimentOptions {
        transaction_mode,
        versions_per_round,
        end_version,
        snapshot_rows,
        max_retries,
        inject_read_failure_before_resume_round,
        clear_read_failure_after_unhealthy,
        expect_ingest_failure,
    } = opts;

    let relation_schema = DeltaTestStruct::schema();
    let arrow_fields = relation_to_arrow_fields(&relation_schema, true);
    let arrow_schema = ArrowSchema::new(arrow_fields);

    let input_table_dir = TempDir::new().unwrap();
    let input_table_uri = input_table_dir.path().display().to_string();
    let output_table_dir = TempDir::new().unwrap();
    let output_table_uri = output_table_dir.path().display().to_string();
    let storage_dir = TempDir::new().unwrap();

    let mut table = create_table(&input_table_uri, &HashMap::new(), &{
        let mut struct_fields = Vec::new();
        for f in arrow_schema.fields.iter() {
            let data_type = DataType::try_from_arrow(f.data_type()).unwrap();
            struct_fields.push(StructField::new(f.name(), data_type, f.is_nullable()));
        }
        struct_fields
    })
    .await;

    let snapshot_and_follow = snapshot_rows.is_some();
    let mut expected_output = Vec::new();
    let mut record_index: i64 = 0;

    if let Some(snapshot_rows) = snapshot_rows {
        let mut snapshot_records = Vec::with_capacity(snapshot_rows as usize);
        for i in 0..snapshot_rows {
            let mut record = delta_test_record(i * 2);
            record.unused = None;
            snapshot_records.push(record);
        }
        table = write_data_to_table(table, &arrow_schema, &snapshot_records).await;
        expected_output.extend(snapshot_records);
        record_index = snapshot_rows;
    }

    let mut input_config: HashMap<String, Value> = HashMap::new();
    input_config.insert(
        "mode".into(),
        if snapshot_and_follow {
            "snapshot_and_follow"
        } else {
            "follow"
        }
        .into(),
    );
    input_config.insert(
        "transaction_mode".into(),
        serde_json::to_value(transaction_mode).unwrap(),
    );
    input_config.insert("filter".into(), "bigint % 2 = 0".into());
    if let Some(end_version) = end_version {
        input_config.insert("end_version".into(), end_version.into());
    }
    if let Some(max_retries) = max_retries {
        input_config.insert("max_retries".into(), max_retries.into());
    }

    let input_table_root = input_table_dir.path().to_path_buf();

    // Follow-only: start paused so the first round can build lag. Snapshot-and-follow: start
    // running so the connector ingests the initial snapshot immediately.
    let start_paused = !snapshot_and_follow;

    let storage_dir_path = storage_dir.path().to_path_buf();
    let input_table_uri_clone = input_table_uri.clone();
    let output_table_uri_clone = output_table_uri.clone();
    let input_config_clone = input_config.clone();
    let pipeline = tokio::task::spawn_blocking(move || {
        delta_to_delta_pipeline::<DeltaTestStruct>(
            &input_table_uri_clone,
            true,
            &input_config_clone,
            &output_table_uri_clone,
            &HashMap::new(),
            1000,
            100,
            &storage_dir_path,
            start_paused,
        )
    })
    .await
    .unwrap();

    pipeline.start();

    if snapshot_and_follow {
        wait_for_delta_connector_phase(&pipeline, DELTA_PHASE_FOLLOW, 120_000).await;
        assert_eq!(
            delta_snapshot_transaction_starts(&pipeline),
            1,
            "catchup must ingest the initial snapshot in one Feldera transaction"
        );
        assert_eq!(
            delta_follow_transaction_starts(&pipeline),
            0,
            "follow transactions must not start until the follow phase ingests log commits"
        );
        assert_eq!(
            delta_last_ingested_version(&pipeline),
            Some(table.version().unwrap() as i64),
            "last_ingested_version must reflect the snapshot version"
        );
        assert_eq!(
            delta_catchup_target_version(&pipeline),
            None,
            "catchup_target_version must be unset before the first catchup window"
        );
    } else {
        // `connect_input` during controller construction blocks until the Delta worker finishes
        // `open_table` and sends init `Ok`, then parks on `wait_running` while still paused.
        assert!(
            pipeline
                .is_input_endpoint_paused(DELTA_TEST_INPUT_ENDPOINT)
                .unwrap(),
            "connector must start paused so we can build an exact log lag"
        );
    }

    let mut transactions_per_round = Vec::with_capacity(versions_per_round.len());
    let mut ingest_failed_as_expected = false;

    'rounds: for (round, &num_versions) in versions_per_round.iter().enumerate() {
        if snapshot_and_follow || round > 0 {
            pipeline
                .pause_input_endpoint(DELTA_TEST_INPUT_ENDPOINT)
                .unwrap();
            wait(
                || {
                    pipeline
                        .is_input_endpoint_paused(DELTA_TEST_INPUT_ENDPOINT)
                        .unwrap_or(false)
                },
                60_000,
            )
            .expect("timeout waiting for input endpoint to pause");
        }

        let metric_at_round_start = delta_follow_transaction_starts(&pipeline);
        let version_before_burst = table.version().unwrap() as i64;

        for _ in 0..num_versions {
            let record = delta_test_record(record_index * 2);
            record_index += 1;
            table = append_table_version(table, &arrow_schema, &record).await;
            let new_version = table.version().unwrap() as i64;
            if end_version.is_none() || new_version <= end_version.unwrap() {
                let mut record = record;
                record.unused = None;
                expected_output.push(record);
            }
        }

        let version_after_burst = table.version().unwrap() as i64;
        assert_eq!(
            version_after_burst,
            version_before_burst + num_versions,
            "round {round}: each append must add exactly one Delta commit"
        );

        assert!(
            pipeline_completed_version(&pipeline).is_none_or(|v| v < version_after_burst),
            "round {round}: connector must not ingest commits written while paused \
             (completed version: {:?}, table version: {version_after_burst})",
            pipeline_completed_version(&pipeline),
        );

        let inject_read_failure = inject_read_failure_before_resume_round == Some(round);
        #[cfg(unix)]
        let mut permission_restore: Option<Vec<(PathBuf, u32)>> = None;
        #[cfg(unix)]
        if inject_read_failure {
            let mut saved = Vec::new();
            strip_delta_input_table_read_permission(&input_table_root, &mut saved).unwrap_or_else(
                |e| panic!("round {round}: inject read failure on delta input table: {e}"),
            );
            permission_restore = Some(saved);
        }
        #[cfg(not(unix))]
        if inject_read_failure {
            panic!("read-failure injection requires a Unix platform");
        }

        pipeline
            .start_input_endpoint(DELTA_TEST_INPUT_ENDPOINT)
            .unwrap();

        let target_version = end_version
            .map(|end| min(version_after_burst, end))
            .unwrap_or(version_after_burst);

        if inject_read_failure && clear_read_failure_after_unhealthy {
            wait_input_endpoint_unhealthy(&pipeline, 60_000);
            #[cfg(unix)]
            if let Some(saved) = permission_restore.take() {
                restore_delta_input_table_read_permission(saved).unwrap_or_else(|e| {
                    panic!("round {round}: restore read permission on delta input table: {e}")
                });
            }
            wait_input_endpoint_healthy(&pipeline, 60_000);
        }

        let round_timeout = if expect_ingest_failure {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(120)
        };

        let start = Instant::now();
        loop {
            if let Some(catchup_target) = delta_catchup_target_version(&pipeline) {
                assert_eq!(
                    catchup_target, target_version,
                    "round {round}: catchup_target_version metric must match the active window"
                );
            }
            if pipeline_completed_version(&pipeline) == Some(target_version) {
                if expect_ingest_failure {
                    panic!(
                        "round {round}: connector reached version {target_version} but ingest failure was expected"
                    );
                }
                break;
            }
            if start.elapsed() >= round_timeout {
                if expect_ingest_failure {
                    assert!(
                        pipeline_completed_version(&pipeline).is_none_or(|v| v < target_version),
                        "round {round}: connector must not ingest version {target_version} after fatal error \
                         (completed: {:?})",
                        pipeline_completed_version(&pipeline),
                    );
                    ingest_failed_as_expected = true;
                    break 'rounds;
                }
                panic!(
                    "round {round}: timeout waiting for connector to reach version {target_version} \
                     (completed: {:?})",
                    pipeline_completed_version(&pipeline),
                );
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            delta_last_ingested_version(&pipeline),
            Some(target_version),
            "last_ingested_version metric must track the completed waterline"
        );
        assert_eq!(
            delta_catchup_target_version(&pipeline),
            None,
            "catchup_target_version metric must be cleared after the window closes"
        );

        let metric_at_round_end = delta_follow_transaction_starts(&pipeline);
        transactions_per_round.push(metric_at_round_end - metric_at_round_start);
    }

    if expect_ingest_failure && !ingest_failed_as_expected {
        panic!("expected ingest to fail but all rounds reached their target versions");
    }

    if !expect_ingest_failure {
        let datafusion = SessionContext::new();
        let mut output_table = Arc::new(
            DeltaTableBuilder::from_url(ensure_table_uri(&output_table_uri).unwrap())
                .unwrap()
                .load()
                .await
                .unwrap(),
        );
        wait_for_output_records::<DeltaTestStruct>(
            &mut output_table,
            &expected_output,
            &datafusion,
            60_000,
            false,
        )
        .await;
    }

    let snapshot_transaction_starts = delta_snapshot_transaction_starts(&pipeline);
    let completed_version = pipeline_completed_version(&pipeline);
    let ended_unhealthy = input_endpoint_is_unhealthy(&pipeline);
    pipeline.stop().unwrap();
    CatchupLagExperimentResult {
        snapshot_transaction_starts,
        follow_transactions_per_round: transactions_per_round,
        completed_version,
        ended_unhealthy,
    }
}

/// Wait until `table` contains exactly `expected_count` records.
async fn wait_for_output_records<T>(
    table: &mut Arc<DeltaTable>,
    expected_output: &[T],
    datafusion: &SessionContext,
    timeout_ms: u64,
    dedup: bool,
) where
    T: for<'a> DeserializeWithContext<'a, SqlSerdeConfig, Variant> + DBData,
{
    let start = Instant::now();
    // Scans address files by the table's `root_url()`, so register that store
    // before querying. Same as `register_snapshot_table` in input.rs.
    let log_store = table.log_store();
    datafusion
        .runtime_env()
        .register_object_store(log_store.root_url(), log_store.root_object_store(None));
    loop {
        // select count() output_table == len().
        Arc::get_mut(table)
            .unwrap()
            .update_incremental(None)
            .await
            .unwrap();

        let provider = table
            .table_provider()
            .await
            .expect("table_provider() failed while polling output table in test");
        let data = datafusion
            .read_table(provider)
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

        result.sort();
        if dedup {
            result.dedup();
        }

        if result.len() == expected_output.len() {
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
    T: for<'a> DeserializeWithContext<'a, SqlSerdeConfig, Variant> + DBData,
{
    let start = Instant::now();
    loop {
        let data = execute_sql(pipeline, &format!("select * from {table}"))
            .await
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
    table
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
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
    input_config: &HashMap<String, Value>,
    key_fields: &[SqlIdentifier],
    key_func: KF,
    storage_dir: &Path,
) -> Controller
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Sync,
    K: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
                false,
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
    config: &HashMap<String, Value>,
) -> Controller
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Sync,
    K: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
                false,
            ))
        },
        &config,
        Box::new(move |e, _| panic!("delta_write pipeline: error: {e}")),
    )
    .unwrap()
}

/// Build a circuit whose output index `idx1` keys records by `bigint` while the
/// underlying view is a plain Z-set.
///
/// Unlike [`test_circuit_with_index`], the input is an unkeyed Z-set, so two
/// records that share a `bigint` are *both* retained and the index `idx1` ends
/// up with multiple values for that key. This is exactly the non-unique-key
/// situation the Delta output connector must tolerate by skipping the offending
/// keys rather than failing the whole batch.
fn nonunique_index_circuit(config: CircuitConfig) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
    let schema = DeltaTestStruct::schema();

    let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
        let mut catalog = Catalog::new();

        let (input, hinput) = circuit.add_input_zset::<DeltaTestStruct>();

        let input_schema = serde_json::to_string(&Relation::new(
            "test_input1".into(),
            schema.clone(),
            false,
            BTreeMap::new(),
        ))
        .unwrap();
        let output_schema = serde_json::to_string(&Relation::new(
            "test_output1".into(),
            schema.clone(),
            false,
            BTreeMap::new(),
        ))
        .unwrap();

        catalog.register_materialized_input_zset(input.clone(), hinput, &input_schema);
        catalog
            .register_materialized_output_zset::<_, DeltaTestStruct>(input.clone(), &output_schema);

        let indexed = input.map_index(|r| (DeltaTestKey { bigint: r.bigint }, r.clone()));
        catalog
            .register_index::<DeltaTestKey, DeltaTestKey, DeltaTestStruct, DeltaTestStruct>(
                indexed,
                &SqlIdentifier::from("idx1"),
                &SqlIdentifier::from("test_output1"),
                &["bigint".to_string()],
            )
            .expect("failed to register index");

        Ok(catalog)
    })
    .unwrap();

    (circuit, Box::new(catalog))
}

/// Read the records referenced by the *current* Delta table snapshot, ignoring
/// the `__feldera_op`/`__feldera_ts` metadata columns and any orphaned parquet
/// files left behind by previous `truncate` commits.
fn read_delta_snapshot(table_uri: &str) -> Vec<DeltaTestStruct> {
    use dbsp::circuit::tokio::TOKIO;
    use deltalake::open_table;

    let url = url::Url::from_file_path(table_uri).unwrap();
    let Ok(table) = TOKIO.block_on(async move { open_table(url).await }) else {
        // The connector creates the table lazily; treat "not yet created" as empty.
        return Vec::new();
    };
    let base = Path::new(table_uri);
    let mut records = Vec::new();
    for uri in table.get_file_uris().unwrap() {
        let mut batch: Vec<DeltaTestStruct> = load_parquet_file(&base.join(&*uri));
        records.append(&mut batch);
    }
    records
}

/// A unique-key constraint violation in an indexed Delta output must skip only
/// the offending key, not the rest of the batch, and must report one error per
/// non-unique key.
///
/// The connector is configured with `index: idx1`, so every key is expected to
/// map to a single value. We feed a single batch in which most keys are unique
/// but three keys violate uniqueness: `bigint` 100 and 200 carry two distinct
/// values each, and `bigint` 300 carries eleven. The connector must (a) write
/// every well-formed record and (b) report a uniqueness-violation error for
/// each of the three offending keys while leaving their records out of the
/// table.
///
/// The eleven-value key is not redundant with the two-value keys: it guards a
/// distinct failure mode. When a single key carries MORE than ten distinct
/// values, the diagnostic builder (`non_unique_key_error`) stops after ten
/// values, so it cannot be relied on to drain the value cursor. The skip path
/// must therefore advance the key cursor itself. Without that advance the
/// encode thread spins forever on the offending key: the output batch never
/// commits and every well-formed record is stranded. Eleven-plus values is the
/// smallest case that exposes this; keys 100 and 200 cover the two-value path,
/// which the diagnostic happens to drain on its own.
///
/// Output buffering is sized so the whole input collapses into one output
/// batch; this guarantees that all values of each non-unique key are encoded
/// together (otherwise the conflict would not be observable within a single
/// batch).
#[test]
fn delta_table_output_non_unique_keys_skipped_test() {
    init_logging();

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // Ten well-formed keys, one value each.
    let unique: Vec<DeltaTestStruct> = (0..10).map(delta_test_record).collect();

    // Two non-unique keys, each with two distinct values inserted in the same
    // batch. `delta_test_record` is deterministic, so we mutate a field to make
    // the second value differ from the first.
    let make_conflict = |bigint: i64| {
        let first = delta_test_record(bigint);
        let second = DeltaTestStruct {
            int: first.int.wrapping_add(1),
            ..first.clone()
        };
        assert_eq!(first.bigint, second.bigint);
        assert_ne!(first, second);
        [first, second]
    };
    let conflict_a = make_conflict(100);
    let conflict_b = make_conflict(200);

    // A non-unique key carrying eleven distinct values. This exceeds the
    // ten-value cap of `non_unique_key_error`, so the diagnostic builder leaves
    // the value cursor partway through. The skip path must advance the key
    // cursor on its own; otherwise the encode thread loops forever on this key.
    let make_conflict_n = |bigint: i64, n: i32| -> Vec<DeltaTestStruct> {
        let base = delta_test_record(bigint);
        let records: Vec<DeltaTestStruct> = (0..n)
            .map(|i| DeltaTestStruct {
                int: base.int.wrapping_add(i),
                ..base.clone()
            })
            .collect();
        assert!(records.iter().all(|r| r.bigint == bigint));
        assert_eq!(
            records.iter().collect::<HashSet<_>>().len(),
            n as usize,
            "the values for a non-unique key must be pairwise distinct"
        );
        records
    };
    let conflict_c = make_conflict_n(300, 11);

    let all_records: Vec<DeltaTestStruct> = unique
        .iter()
        .cloned()
        .chain(conflict_a.iter().cloned())
        .chain(conflict_b.iter().cloned())
        .chain(conflict_c.iter().cloned())
        .collect();

    // Write all records (as raw inserts) to the input file.
    let mut input_file = NamedTempFile::new().unwrap();
    for record in &all_records {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        record
            .serialize_with_context(
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

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": input_file.path() }
                },
                "format": {
                    "name": "json",
                    "config": { "update_format": "raw" }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "index": "idx1",
                "enable_output_buffer": true,
                "max_output_buffer_size_records": 1_000_000,
                "max_output_buffer_time_millis": 2_000,
                "transport": {
                    "name": "delta_table_output",
                    "config": { "uri": table_uri.clone(), "mode": "truncate" }
                }
            }
        }
    }))
    .unwrap();

    // Capture every error the controller reports instead of panicking, so we
    // can assert on the uniqueness violations.
    let errors: Arc<Mutex<Vec<(String, Option<String>)>>> = Arc::new(Mutex::new(Vec::new()));
    let errors_clone = errors.clone();

    let controller = Controller::with_test_config(
        move |workers| Ok(nonunique_index_circuit(workers)),
        &config,
        Box::new(move |e, tag| {
            errors_clone.lock().unwrap().push((e.to_string(), tag));
        }),
    )
    .unwrap();

    controller.start();

    // The unique records become visible only after the (single) output batch
    // commits, which happens after the offending keys have been skipped and
    // their errors reported.
    wait(
        || read_delta_snapshot(&table_uri).len() == unique.len(),
        60_000,
    )
    .expect("timeout waiting for the well-formed records to reach the Delta table");

    controller.stop().unwrap();

    // (a) Exactly the well-formed records are written; the non-unique keys are
    //     dropped entirely.
    let mut written = read_delta_snapshot(&table_uri);
    written.sort();
    let mut expected = unique.clone();
    expected.sort();
    assert_eq!(
        written, expected,
        "non-unique keys must be skipped while every unique key is preserved"
    );
    assert!(
        written
            .iter()
            .all(|r| r.bigint != 100 && r.bigint != 200 && r.bigint != 300),
        "no record for a non-unique key should leak into the output"
    );

    // (b) One uniqueness-violation error is reported per non-unique key, and
    //     each names its offending key.
    let errors = errors.lock().unwrap();
    let violations: Vec<&(String, Option<String>)> = errors
        .iter()
        .filter(|(_, tag)| {
            tag.as_deref()
                .is_some_and(|tag| tag.contains("delta_uniqueness_violation"))
        })
        .collect();
    assert_eq!(
        violations.len(),
        3,
        "expected exactly one uniqueness-violation error per non-unique key, got: {errors:?}"
    );
    assert!(
        violations.iter().any(|(msg, _)| msg.contains("100")),
        "a uniqueness-violation error should name key 100: {violations:?}"
    );
    assert!(
        violations.iter().any(|(msg, _)| msg.contains("200")),
        "a uniqueness-violation error should name key 200: {violations:?}"
    );
    assert!(
        violations.iter().any(|(msg, _)| msg.contains("300")),
        "a uniqueness-violation error should name the eleven-value key 300: {violations:?}"
    );
}

/// Test function that works for both local FS and remote object stores.
///
/// * `verify` - verify the final contents of the delta table is equivalent to
///   `data`.  Currently only works for tables in the local FS.
/// * `index_is_alias` - if true, uses catalog.register_materialized_output_map_persistent with
///    index_is_alias=true to register index; otherwise, uses catalog.register_index_persistent.
///
/// TODO: implement verification using the delta table API rather than
/// by reading parquet files directly.  I guess the best way to do this is
/// to build an input connector.
fn delta_table_output_test(
    data: Vec<DeltaTestStruct>,
    table_uri: &str,
    object_store_config: &HashMap<String, String>,
    verify: bool,
    threads: Option<usize>,
    index_is_alias: bool,
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

    let mut storage_options: serde_json::Value = serde_json::to_value(object_store_config).unwrap();
    storage_options["uri"] = json!(table_uri);
    storage_options["mode"] = json!("truncate");
    if let Some(threads) = threads {
        storage_options["threads"] = json!(threads);
    }

    println!(
        "delta_table_output_test: {} records, input file: {}, table uri: {table_uri}, threads: {threads:?}",
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
        move |workers| {
            Ok(test_circuit_with_index::<DeltaTestStruct, DeltaTestKey, _>(
                workers,
                &DeltaTestStruct::schema(),
                &[SqlIdentifier::from("bigint")],
                |x: &DeltaTestStruct| DeltaTestKey { bigint: x.bigint },
                &[None],
                index_is_alias,
            ))
        },
        &config,
        Box::new(move |e, _| panic!("delta_table_output_test: error: {e}")),
    )
    .unwrap();

    controller.start();

    wait(|| controller.pipeline_complete(), 100_000).unwrap();

    if verify {
        let parquet_files =
            list_files_recursive(Path::new(table_uri), OsStr::from_bytes(b"parquet")).unwrap();

        // // Uncomment to inspect the input JSON file.
        // std::mem::forget(input_file);

        let mut output_records = Vec::with_capacity(data.len());
        for parquet_file in parquet_files {
            if !parquet_file
                .display()
                .to_string()
                .ends_with(".checkpoint.parquet")
            {
                let mut records: Vec<DeltaTestStruct> = load_parquet_file(&parquet_file);
                output_records.append(&mut records);
            }
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
            // Default to `info`, but quiet `object_store` and `buoyant_kernel`,
            // whose per-request logs drown out the test output. `RUST_LOG` overrides.
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("object_store=warn,buoyant_kernel=warn,info"))
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
    inject_failure: Option<Box<dyn Fn()>>,
    clear_failure: Option<Box<dyn Fn()>>,
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
        let data_type = DataType::try_from_arrow(f.data_type()).unwrap();
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

    println!("initial table version: {}", input_table.version().unwrap());

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

    let end_version: i64 = 5;
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
    println!("connecting to output table: {output_table_uri}");
    let mut output_table = Arc::new(
        DeltaTableBuilder::from_url(ensure_table_uri(output_table_uri).unwrap())
            .unwrap()
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
        false,
    )
    .await;

    println!("initial snapshot processed");

    // The connector should report the initial version as the completed version (issue 5447).
    wait(
        || {
            if let Some(version) = completed_version(&pipeline) {
                let expected = input_table.version().unwrap() as i64;
                debug!(
                    "pipeline completed version {version}, expected (initial version) {expected}, waterlines: {:?}",
                    pipeline
                        .status()
                        .input_status()
                        .values()
                        .next()
                        .unwrap()
                        .completed_frontier
                        .debug()
                );
                version == expected
            } else {
                debug!("pipeline completed version: None");
                false
            }
        },
        20_000,
    )
    .unwrap();

    // Write remaining data in 10 chunks, wait for it to show up in the output table.
    for chunk in data[split_at..].chunks(std::cmp::max(data[split_at..].len() / 10, 1)) {
        total_count += chunk.len();
        input_table = write_data_to_table(input_table, &arrow_schema, chunk).await;

        if !test_end_version || input_table.version().unwrap() as i64 <= end_version {
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

        // Run after the write so the test process can still update the table; the pipeline
        // then fails to read the new snapshot until permissions are restored.
        if let Some(inject_failure) = &inject_failure {
            inject_failure();
        }

        if inject_failure.is_some() {
            wait(
                || {
                    pipeline
                        .input_endpoint_status("test_input1")
                        .ok()
                        .and_then(|s| s.health)
                        .is_some_and(|h| {
                            let unhealthy = matches!(
                                h.status,
                                feldera_types::adapter_stats::ConnectorHealthStatus::Unhealthy
                            );
                            if unhealthy {
                                println!("unhealthy: {:?}", h);
                            }
                            unhealthy
                        })
                },
                20_000,
            )
            .expect("timeout waiting for input connector health to become unhealthy");
        }

        if let Some(clear_failure) = &clear_failure {
            clear_failure();
        }

        if clear_failure.is_some() {
            wait(
                || {
                    pipeline
                        .input_endpoint_status("test_input1")
                        .ok()
                        .and_then(|s| s.health)
                        .is_some_and(|h| {
                            matches!(
                                h.status,
                                feldera_types::adapter_stats::ConnectorHealthStatus::Healthy
                            )
                        })
                },
                20_000,
            )
            .expect("timeout waiting for input connector health to become healthy");
        }

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
        wait(
                || {
                    if let Some(version) = completed_version(&pipeline) {
                        let expected = if test_end_version {
                            min(input_table.version().unwrap() as i64, end_version)
                        } else {
                            input_table.version().unwrap() as i64
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

        // Wait a bit to make sure the pipeline doesn't process data beyond end_version.
        if test_end_version && input_table.version().unwrap() as i64 > end_version {
            sleep(Duration::from_millis(1000)).await;
        }

        // Use timeout of 0: once the pipeline indicates that the completed_version
        // reached the expected version (see above check), the output table should be up-to-date.
        wait_for_output_records::<DeltaTestStruct>(
            &mut output_table,
            &expected_output,
            &datafusion,
            if suspend { 200_000 } else { 0 },
            inject_failure.is_some(),
        )
        .await;
    }

    // If the table has reached the end_version, make sure the eoi status survives the suspend.
    if test_end_version && suspend && input_table.version().unwrap() as i64 >= end_version {
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
        assert!(status.metrics.end_of_input);
        println!("eoi status survived suspend");
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
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
    storage_options: &HashMap<String, Value>,
    data: Vec<DeltaTestStruct>,
    suspend: bool,
    index: bool,
    transaction_mode: DeltaTableTransactionMode,
    skip_unused_columns: bool,
) {
    init_logging();

    // The record the connector is expected to emit. With `skip_unused_columns`,
    // the connector never reads the `unused` column (nullable, marked unused in
    // `DeltaTestStruct::schema`), so the `unused` column must contain NULL
    // regardless of what was written to the Delta table.
    let expected_record = |record: &DeltaTestStruct| -> DeltaTestStruct {
        let mut record = record.clone();
        if skip_unused_columns {
            record.unused = None;
        }
        record
    };

    let mut input_file = NamedTempFile::new().unwrap();
    let input_file_path = input_file.path().display().to_string();

    let table_uri_clone = table_uri.to_string();

    // Build pipeline 1.
    let mut output_config = storage_options.clone();

    output_config.insert("mode".to_string(), "truncate".into());

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

    input_config.insert("mode".to_string(), "cdc".into());
    input_config.insert("filter".to_string(), "bigint % 2 = 0".into());
    input_config.insert("cdc_delete_filter".to_string(), "__feldera_op = 'd'".into());
    input_config.insert("cdc_order_by".to_string(), "__feldera_ts".into());
    input_config.insert(
        "skip_unused_columns".to_string(),
        skip_unused_columns.into(),
    );
    input_config.insert(
        "transaction_mode".to_string(),
        serde_json::to_value(transaction_mode).unwrap(),
    );

    let table_uri_clone = table_uri.to_string();

    let storage_dir = TempDir::new().unwrap();
    let storage_dir_path = storage_dir.path().to_path_buf();
    let input_config_clone: HashMap<String, Value> = input_config.clone();
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
            let input_config_clone: HashMap<String, Value> = input_config.clone();
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
                        Some(expected_record(x))
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
                    let mut x = expected_record(x);
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
                        let mut x = expected_record(x);
                        if index {
                            x.boolean = !x.boolean;
                        }
                        Some(x)
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
    vec(DeltaTestStruct::arbitrary(), 10..max_records).prop_map(|vec| {
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

/// Remove owner read and execute on the delta table **root directory only**, and push that path
/// and its original mode onto `saved` for [`restore_delta_input_table_read_permission`].
///
/// Without `r` and `x` on the root, the process cannot traverse into `_delta_log` or data paths
/// even if inner files still have permissive modes.
#[cfg(unix)]
fn strip_delta_input_table_read_permission(
    table_root: &Path,
    saved: &mut Vec<(PathBuf, u32)>,
) -> std::io::Result<()> {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let meta = fs::metadata(table_root)?;
    if !meta.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "delta input table path must be a directory",
        ));
    }

    let mode = meta.permissions().mode();
    saved.push((table_root.to_path_buf(), mode));

    let new_mode = mode & !0o500;
    let mut perms = meta.permissions();
    perms.set_mode(new_mode);
    fs::set_permissions(table_root, perms)?;
    Ok(())
}

#[cfg(unix)]
fn restore_delta_input_table_read_permission(saved: Vec<(PathBuf, u32)>) -> std::io::Result<()> {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    for (path, mode) in saved.into_iter().rev() {
        fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
    }
    Ok(())
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

    // With `end_version`, the connector stops tailing the log before new versions appear, so
    // stripping read permission would not drive the connector unhealthy (wait would time out).
    #[cfg(unix)]
    type FailureHook = Option<Box<dyn Fn()>>;
    #[cfg(unix)]
    let (inject_failure, clear_failure): (FailureHook, FailureHook) = if end_version {
        (None, None)
    } else {
        let saved_modes: Arc<Mutex<Vec<(PathBuf, u32)>>> = Arc::new(Mutex::new(Vec::new()));
        let input_root = input_table_dir.path().to_path_buf();

        let inject_failure: Box<dyn Fn()> = {
            let saved_modes = Arc::clone(&saved_modes);
            let input_root = input_root.clone();
            Box::new(move || {
                let mut guard = saved_modes.lock().unwrap();
                guard.clear();
                strip_delta_input_table_read_permission(&input_root, &mut guard).unwrap_or_else(
                    |e| panic!("inject_failure (strip read permission on input table): {e}"),
                );
            })
        };

        let clear_failure: Box<dyn Fn()> = {
            let saved_modes = Arc::clone(&saved_modes);
            Box::new(move || {
                let entries = std::mem::take(&mut *saved_modes.lock().unwrap());
                restore_delta_input_table_read_permission(entries).unwrap_or_else(|e| {
                    panic!("clear_failure (restore read permission on input table): {e}")
                });
            })
        };

        (Some(inject_failure), Some(clear_failure))
    };

    #[cfg(not(unix))]
    let (inject_failure, clear_failure) = (None, None);

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
        inject_failure,
        clear_failure,
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
        DeltaTableTransactionMode::None,
        false,
    )
    .await;
}

/// CDC mode must honor `skip_unused_columns`: the connector should not read the
/// `unused` column (nullable, marked unused in the SQL schema), even though the
/// Delta table stores a non-null value for it. Regression test for issue #6113.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_skip_unused_columns_test() {
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
        DeltaTableTransactionMode::None,
        true,
    )
    .await;
}

/// Same as [`delta_table_cdc_skip_unused_columns_test`] but in `Catchup`
/// transaction mode, so projection is exercised across the catchup
/// transaction-batching path as well. Regression test for issue #6113.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_skip_unused_columns_catchup_test() {
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
        DeltaTableTransactionMode::Catchup,
        true,
    )
    .await;
}

/// Same as [`delta_table_cdc_skip_unused_columns_catchup_test`] but with
/// suspend/resume, so projection is exercised across the catchup re-read from a
/// resumed version. Regression test for issue #6113.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_skip_unused_columns_catchup_suspend_test() {
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
        DeltaTableTransactionMode::Catchup,
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_file_catchup_test() {
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
        DeltaTableTransactionMode::Catchup,
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_file_catchup_suspend_test() {
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
        DeltaTableTransactionMode::Catchup,
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
        DeltaTableTransactionMode::None,
        false,
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
        DeltaTableTransactionMode::None,
        false,
    )
    .await;
}

/// CDC connector must not re-emit rows when a Delta operation rewrites a
/// file without changing its logical contents. Covers:
/// - `OPTIMIZE` (every action has `data_change=false` -> early return).
/// - No-op `UPDATE` (paired Add+Remove with identical rows -> EXCEPT ALL
///   cancels them).
/// - Real `UPDATE` (genuine row change is propagated).
///
/// Each step is followed by a sentinel-row append. Because the connector
/// processes Delta versions in order, the sentinel can appear in the
/// materialized output only after the prior step has been processed; that
/// turns each `wait_for_records_materialized` into a deterministic check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_rewrite_test() {
    use crate::test::TestStruct;
    use arrow::array::{
        Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
    };
    use deltalake::datafusion::prelude::{col, lit};
    use deltalake::kernel::{DataType as KernelDataType, PrimitiveType, StructField};
    use std::num::NonZeroU64;

    init_logging();

    let row = |id: u32, label: &str| TestStruct {
        id,
        b: false,
        i: None,
        s: label.to_string(),
    };

    // Two batches => two parquet files, so OPTIMIZE has work to do.
    // Filter `id % 2 = 0` keeps half of them.
    let batch1: Vec<TestStruct> = (0..6).map(|id| row(id, &format!("row-{id}"))).collect();
    let batch2: Vec<TestStruct> = (6..12).map(|id| row(id, &format!("row-{id}"))).collect();
    let all: Vec<TestStruct> = batch1.iter().chain(batch2.iter()).cloned().collect();
    let expected_initial: Vec<TestStruct> = all.iter().filter(|r| r.id % 2 == 0).cloned().collect();

    // TestStruct columns + the CDC bookkeeping columns the connector expects.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("__feldera_op", ArrowDataType::Utf8, false),
        ArrowField::new(
            "__feldera_ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let struct_fields = vec![
        StructField::new("id", KernelDataType::Primitive(PrimitiveType::Long), false),
        StructField::new(
            "b",
            KernelDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        StructField::new("s", KernelDataType::Primitive(PrimitiveType::String), false),
        StructField::new(
            "__feldera_op",
            KernelDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "__feldera_ts",
            KernelDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
    ];

    let make_batch = |rows: &[TestStruct], op: &str, ts: i64| -> RecordBatch {
        let n = rows.len();
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|r| r.id as i64),
                )) as Arc<dyn Array>,
                Arc::new(rows.iter().map(|r| Some(r.b)).collect::<BooleanArray>()),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| r.s.clone()),
                )),
                Arc::new(StringArray::from_iter_values((0..n).map(|_| op))),
                Arc::new(TimestampMicrosecondArray::from_iter_values(
                    (0..n as i64).map(|i| ts + i),
                )),
            ],
        )
        .unwrap()
    };

    async fn append(delta: DeltaTable, batches: Vec<RecordBatch>) -> DeltaTable {
        delta
            .write(batches)
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap()
    }

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let mut delta = create_table(&table_uri, &HashMap::new(), &struct_fields).await;

    let storage_dir = TempDir::new().unwrap();
    let read_pipeline = {
        let table_uri = table_uri.clone();
        let storage_dir = storage_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let pipeline_config: PipelineConfig = serde_json::from_value(json!({
                "name": "test",
                "workers": 4,
                "storage_config": { "path": storage_dir },
                "inputs": {
                    "test_input1": {
                        "stream": "test_input1",
                        "transport": {
                            "name": "delta_table_input",
                            "config": {
                                "uri": table_uri,
                                "mode": "cdc",
                                "filter": "id % 2 = 0",
                                "cdc_delete_filter": "__feldera_op = 'd'",
                                "cdc_order_by": "__feldera_ts",
                            }
                        }
                    }
                }
            }))
            .unwrap();
            Controller::with_test_config(
                move |workers| {
                    Ok(test_circuit::<TestStruct>(
                        workers,
                        &TestStruct::schema(),
                        &[Some("output")],
                    ))
                },
                &pipeline_config,
                Box::new(move |e, _| panic!("cdc rewrite test: {e}")),
            )
            .unwrap()
        })
        .await
        .unwrap()
    };
    read_pipeline.start();
    let output = SqlIdentifier::from("test_output1");

    // Step 1: two appends => two parquet files, one Delta version each.
    delta = append(delta, vec![make_batch(&batch1, "i", 1_000)]).await;
    delta = append(delta, vec![make_batch(&batch2, "i", 2_000)]).await;
    wait_for_records_materialized(&read_pipeline, &output, &expected_initial).await;

    // Step 2: OPTIMIZE coalesces the two files into one. Every action has
    // `data_change=false`, so the connector early-returns. Any spurious
    // emission would be exposed by the size check below.
    let v_before = delta.version().unwrap();
    let (optimized, _) = delta
        .optimize()
        .with_target_size(NonZeroU64::new(128 * 1024 * 1024).unwrap())
        .await
        .unwrap();
    assert!(
        optimized.version().unwrap() > v_before,
        "OPTIMIZE should produce a new commit"
    );
    let sentinel1 = row(100, "sentinel-after-optimize");
    delta = append(
        optimized,
        vec![make_batch(std::slice::from_ref(&sentinel1), "i", 2_500)],
    )
    .await;
    let mut expected = expected_initial.clone();
    expected.push(sentinel1);
    wait_for_records_materialized(&read_pipeline, &output, &expected).await;

    // Step 3: no-op UPDATE (`b = b`). delta-rs rewrites every matching file
    // with paired Add+Remove actions over identical rows; EXCEPT ALL must
    // cancel them so no rows are re-ingested.
    let v_before = delta.version().unwrap();
    let (updated, _) = delta
        .update()
        .with_predicate(col("id").gt_eq(lit(0i64)))
        .with_update("b", col("b"))
        .await
        .unwrap();
    assert!(
        updated.version().unwrap() > v_before,
        "no-op UPDATE should produce a new commit"
    );
    let sentinel2 = row(200, "sentinel-after-noop-update");
    delta = append(
        updated,
        vec![make_batch(std::slice::from_ref(&sentinel2), "i", 2_700)],
    )
    .await;
    expected.push(sentinel2);
    wait_for_records_materialized(&read_pipeline, &output, &expected).await;

    // Step 4: real change. CDC tables encode an in-place row update as a
    // `'d'` row (old value) followed by an `'i'` row (new value), ordered
    // by `cdc_order_by`. The connector should turn that into delete+insert.
    let target_id: u32 = 0;
    let original = all.iter().find(|r| r.id == target_id).unwrap().clone();
    let mut modified = original.clone();
    modified.b = true;
    let _ = append(
        delta,
        vec![
            make_batch(std::slice::from_ref(&original), "d", 3_000_000),
            make_batch(std::slice::from_ref(&modified), "i", 3_000_001),
        ],
    )
    .await;
    let mut expected_after = expected.clone();
    expected_after
        .iter_mut()
        .find(|r| r.id == target_id)
        .unwrap()
        .b = true;
    wait_for_records_materialized(&read_pipeline, &output, &expected_after).await;

    read_pipeline.stop().unwrap();
}

/// A multi-key `cdc_order_by`: "__feldera_ts asc, lsn asc`
///
/// One transaction deletes the old value and inserts a new one for the same id,
/// and the connector applies both, leaving the updated row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_table_cdc_multi_key_order_by_test() {
    use crate::test::TestStruct;
    use arrow::array::{
        Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
    };
    use deltalake::kernel::{DataType as KernelDataType, PrimitiveType, StructField};

    init_logging();

    let expect = |id: u32, label: &str| TestStruct {
        id,
        b: false,
        i: None,
        s: label.to_string(),
    };

    // TestStruct columns + CDC bookkeeping columns. `lsn` is the order
    // tiebreaker; it is not a TestStruct field, so it never reaches the table.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("__feldera_op", ArrowDataType::Utf8, false),
        ArrowField::new(
            "__feldera_ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        ArrowField::new("lsn", ArrowDataType::Int64, false),
    ]));
    let struct_fields = vec![
        StructField::new("id", KernelDataType::Primitive(PrimitiveType::Long), false),
        StructField::new(
            "b",
            KernelDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        StructField::new("s", KernelDataType::Primitive(PrimitiveType::String), false),
        StructField::new(
            "__feldera_op",
            KernelDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "__feldera_ts",
            KernelDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
        StructField::new("lsn", KernelDataType::Primitive(PrimitiveType::Long), false),
    ];

    // Each tuple is (id, op, ts, lsn, s).
    let make_batch = |rows: &[(i64, &str, i64, i64, &str)]| -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as Arc<dyn Array>,
                Arc::new(rows.iter().map(|_| Some(false)).collect::<BooleanArray>()),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.4))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|r| r.2),
                )),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3))),
            ],
        )
        .unwrap()
    };

    async fn append(delta: DeltaTable, batch: RecordBatch) -> DeltaTable {
        delta
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap()
    }

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let mut delta = create_table(&table_uri, &HashMap::new(), &struct_fields).await;

    let storage_dir = TempDir::new().unwrap();
    let read_pipeline = {
        let table_uri = table_uri.clone();
        let storage_dir = storage_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let pipeline_config: PipelineConfig = serde_json::from_value(json!({
                "name": "test",
                "workers": 4,
                "storage_config": { "path": storage_dir },
                "inputs": {
                    "test_input1": {
                        "stream": "test_input1",
                        "transport": {
                            "name": "delta_table_input",
                            "config": {
                                "uri": table_uri,
                                "mode": "cdc",
                                "filter": "id % 2 = 0",
                                "cdc_delete_filter": "__feldera_op = 'd'",
                                "cdc_order_by": "__feldera_ts asc, lsn asc",
                            }
                        }
                    }
                }
            }))
            .unwrap();
            Controller::with_test_config(
                move |workers| {
                    Ok(test_circuit::<TestStruct>(
                        workers,
                        &TestStruct::schema(),
                        &[Some("output")],
                    ))
                },
                &pipeline_config,
                Box::new(move |e, _| panic!("cdc multi-key order_by test: {e}")),
            )
            .unwrap()
        })
        .await
        .unwrap()
    };
    read_pipeline.start();
    let output = SqlIdentifier::from("test_output1");

    // Seed id 0 (kept by `id % 2 = 0`).
    delta = append(delta, make_batch(&[(0, "i", 1_000, 0, "orig")])).await;
    wait_for_records_materialized(&read_pipeline, &output, &[expect(0, "orig")]).await;

    // One transaction updates id 0: delete the old value, insert the new one.
    // The multi-key `cdc_order_by` must parse for this transaction to run.
    delta = append(
        delta,
        make_batch(&[(0, "d", 2_000, 1, "orig"), (0, "i", 2_000, 2, "updated")]),
    )
    .await;
    wait_for_records_materialized(&read_pipeline, &output, &[expect(0, "updated")]).await;

    let _ = delta;
    read_pipeline.stop().unwrap();
}

/// `cdc_delete_filter` is parsed against the snapshot schema at
/// connector startup, so an expression that references a column that
/// does not exist on the Delta table must fail before any row flows
/// through the pipeline, with an error that names the missing column
/// and the `cdc_delete_filter` setting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_cdc_invalid_delete_filter_test() {
    use crate::test::TestStruct;
    use deltalake::kernel::{DataType as KernelDataType, PrimitiveType, StructField};

    init_logging();

    let struct_fields = vec![
        StructField::new("id", KernelDataType::Primitive(PrimitiveType::Long), false),
        StructField::new(
            "b",
            KernelDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        StructField::new("s", KernelDataType::Primitive(PrimitiveType::String), false),
        StructField::new(
            "__feldera_op",
            KernelDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "__feldera_ts",
            KernelDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
    ];

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let _ = create_table(&table_uri, &HashMap::new(), &struct_fields).await;

    let storage_dir = TempDir::new().unwrap();
    let result = {
        let table_uri = table_uri.clone();
        let storage_dir = storage_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let pipeline_config: PipelineConfig = serde_json::from_value(json!({
                "name": "test",
                "workers": 4,
                "storage_config": { "path": storage_dir },
                "inputs": {
                    "test_input1": {
                        "stream": "test_input1",
                        "transport": {
                            "name": "delta_table_input",
                            "config": {
                                "uri": table_uri,
                                "mode": "cdc",
                                "cdc_delete_filter": "no_such_column = 'd'",
                                "cdc_order_by": "__feldera_ts",
                            }
                        }
                    }
                }
            }))
            .unwrap();
            Controller::with_test_config(
                move |workers| {
                    Ok(test_circuit::<TestStruct>(
                        workers,
                        &TestStruct::schema(),
                        &[Some("output")],
                    ))
                },
                &pipeline_config,
                Box::new(move |_, _| {}),
            )
        })
        .await
        .unwrap()
    };

    let err = match result {
        Ok(_) => panic!("controller should fail to start with an invalid cdc_delete_filter"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("no_such_column"),
        "expected error to name the missing column 'no_such_column', got: {msg}",
    );
    assert!(
        msg.contains("cdc_delete_filter"),
        "expected error to mention 'cdc_delete_filter', got: {msg}",
    );
}

/// Assert the logical-plan shape of the CDC `DataFrame`: filter on
/// both sides of an `EXCEPT ALL`, then a sort by `cdc_order_by`. Also
/// checks that an `order_by` referencing an unknown column fails at
/// build time.
#[tokio::test]
async fn build_cdc_dataframe_plan_structure() {
    use crate::integrated::delta_table::input::build_cdc_dataframe;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
    use datafusion::datasource::MemTable;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("__feldera_op", ArrowDataType::Utf8, false),
        ArrowField::new(
            "__feldera_ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    // Two independently-registered tables so the filter expression has
    // to resolve against each side of the set difference.
    let ctx = SessionContext::new();
    let empty = || Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap());
    ctx.register_table("cdc_adds", empty()).unwrap();
    ctx.register_table("cdc_removes", empty()).unwrap();

    // Full path: filter + removes + order_by.
    let df = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        Some(ctx.table("cdc_removes").await.unwrap()),
        Some("id % 2 = 0"),
        "__feldera_ts",
        "unit-test",
    )
    .unwrap();
    let plan = format!("{}", df.logical_plan().display_indent());

    assert!(
        plan.contains("Sort: "),
        "expected a Sort node; plan was:\n{plan}"
    );
    assert!(
        plan.contains("__feldera_ts"),
        "expected sort key '__feldera_ts'; plan was:\n{plan}"
    );
    assert!(
        plan.contains("LeftAnti"),
        "expected a LeftAnti join (DataFusion encoding of EXCEPT); plan was:\n{plan}"
    );
    // EXCEPT ALL uses a LeftAnti join without a Distinct wrapper;
    // EXCEPT DISTINCT inserts a `Distinct:` node above the join.
    assert!(
        !plan.contains("Distinct:"),
        "expected EXCEPT ALL (no Distinct node); plan was:\n{plan}"
    );
    let filter_lines = plan
        .lines()
        .filter(|line| line.trim_start().starts_with("Filter:"))
        .count();
    assert_eq!(
        filter_lines, 2,
        "expected Filter applied to both adds and removes; plan was:\n{plan}"
    );

    // No removes: no set-difference in the plan.
    let df = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        None,
        Some("id % 2 = 0"),
        "__feldera_ts",
        "unit-test",
    )
    .unwrap();
    let plan = format!("{}", df.logical_plan().display_indent());
    assert!(
        plan.contains("Sort: "),
        "expected Sort even without removes; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("LeftAnti"),
        "no removes -> no LeftAnti join; plan was:\n{plan}"
    );
    let filter_lines = plan
        .lines()
        .filter(|line| line.trim_start().starts_with("Filter:"))
        .count();
    assert_eq!(
        filter_lines, 1,
        "expected one Filter (adds only); plan was:\n{plan}"
    );

    // No filter: set difference and sort, no Filter node.
    let df = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        Some(ctx.table("cdc_removes").await.unwrap()),
        None,
        "__feldera_ts",
        "unit-test",
    )
    .unwrap();
    let plan = format!("{}", df.logical_plan().display_indent());
    assert!(plan.contains("Sort: "), "expected Sort; plan was:\n{plan}");
    assert!(
        plan.contains("LeftAnti"),
        "expected LeftAnti join; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Filter:"),
        "no filter configured -> no Filter node; plan was:\n{plan}"
    );

    // Invalid `order_by`: error surfaces at build time.
    let err = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        None,
        None,
        "no_such_column",
        "unit-test",
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("no_such_column"),
        "expected error to name the missing column; got: {err}"
    );
    assert!(
        err.contains("cdc_order_by"),
        "expected error to mention 'cdc_order_by'; got: {err}"
    );
}

/// A `cdc_order_by` with several keys and explicit ASC/DESC directions
/// (a real ORDER BY clause body) must build a multi-key Sort. Guards a
/// regression where the clause was parsed as a single scalar expression.
#[tokio::test]
async fn build_cdc_dataframe_multi_key_order_by() {
    use crate::integrated::delta_table::input::build_cdc_dataframe;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
    use datafusion::datasource::MemTable;
    use feldera_adapterlib::utils::datafusion::validate_sql_order_by;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new(
            "__feldera_ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    let ctx = SessionContext::new();
    ctx.register_table(
        "cdc_adds",
        Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap()),
    )
    .unwrap();

    let df = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        None,
        None,
        "__feldera_ts asc, id desc",
        "unit-test",
    )
    .unwrap();
    let plan = format!("{}", df.logical_plan().display_indent());

    // One Sort node carrying both keys with their directions.
    assert!(
        plan.contains("__feldera_ts ASC") && plan.contains("id DESC"),
        "expected both sort keys with directions; plan was:\n{plan}"
    );

    // The validator accepts the same multi-key clause and rejects garbage.
    assert!(validate_sql_order_by("__feldera_ts asc, id desc").is_ok());
    assert!(validate_sql_order_by("ts asc !! lsn").is_err());
}

/// `parse_cdc_order_by` applies DataFusion's SQL planner defaults: a missing
/// direction is `ASC`, a missing `NULLS` placement is nulls-last for `ASC` and
/// nulls-first for `DESC` (the `nulls_max` convention), and explicit `ASC` /
/// `DESC` / `NULLS FIRST` / `NULLS LAST` override those defaults.
#[tokio::test]
async fn parse_cdc_order_by_defaults() {
    use crate::integrated::delta_table::input::parse_cdc_order_by;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::datasource::MemTable;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("a", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Int64, false),
    ]));
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap()),
    )
    .unwrap();
    let df = ctx.table("t").await.unwrap();

    // (clause, expected [(asc, nulls_first), ...] per key).
    let cases: &[(&str, &[(bool, bool)])] = &[
        // No direction => ASC; ASC default nulls placement is last.
        ("a", &[(true, false)]),
        ("a asc", &[(true, false)]),
        // DESC default nulls placement is first.
        ("a desc", &[(false, true)]),
        // Explicit NULLS overrides the default for either direction.
        ("a nulls first", &[(true, true)]),
        ("a asc nulls first", &[(true, true)]),
        ("a desc nulls last", &[(false, false)]),
        // Each key carries its own direction and default.
        ("a asc, b desc", &[(true, false), (false, true)]),
    ];

    for (clause, expected) in cases {
        let sort_exprs = parse_cdc_order_by(&df, clause).unwrap();
        let got: Vec<(bool, bool)> = sort_exprs.iter().map(|s| (s.asc, s.nulls_first)).collect();
        assert_eq!(
            got.as_slice(),
            *expected,
            "clause '{clause}': expected {expected:?}, got {got:?}"
        );
    }
}

/// Pin the exact set of arrow types that the `EXCEPT ALL` cancellation
/// step in `do_process_cdc_transaction` cannot handle.
///
/// `EXCEPT ALL` is planned via `RowConverter`, so any column type for
/// which `RowConverter::supports_fields` returns `false` will make the
/// CDC rewrite path fail at runtime once the transaction contains
/// Removes. Locking that list down here means a future arrow upgrade
/// that widens (or narrows) support is caught by this test instead of
/// silently changing user-visible behavior, and gives us a concrete
/// reference for the caveat documented at the call site.
#[test]
fn except_all_unsupported_types_are_only_map() {
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use arrow::row::{RowConverter, SortField};
    use std::sync::Arc;

    let supports = |dt: DataType| -> bool { RowConverter::supports_fields(&[SortField::new(dt)]) };

    // Every Delta-mappable scalar / nested type that previous review
    // comments and code comments listed as "unsupported" is in fact
    // supported by `arrow-row` 57.x.
    assert!(supports(DataType::Binary), "Binary must be supported");
    assert!(
        supports(DataType::LargeBinary),
        "LargeBinary must be supported",
    );
    assert!(
        supports(DataType::FixedSizeBinary(16)),
        "FixedSizeBinary must be supported",
    );
    assert!(
        supports(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        )))),
        "List<Utf8> must be supported",
    );
    assert!(
        supports(DataType::LargeList(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        )))),
        "LargeList<Utf8> must be supported",
    );
    assert!(
        supports(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, true)),
            4,
        )),
        "FixedSizeList<Int32> must be supported",
    );
    assert!(
        supports(DataType::Struct(
            vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        )),
        "Struct must be supported",
    );

    // Spot-check that the broader scalar surface area used by
    // `DeltaTestStruct` is supported too — these are the types a CDC
    // rewrite is most likely to encounter in practice.
    for dt in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Date32,
        DataType::Date64,
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Decimal128(10, 3),
    ] {
        assert!(supports(dt.clone()), "{dt:?} must be supported");
    }

    // `Map` is the one Delta-mappable type that is genuinely unsupported.
    let map_type = Field::new_map(
        "map",
        "entries",
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
        false,
        true,
    )
    .data_type()
    .clone();
    assert!(
        !supports(map_type),
        "Map is expected to be unsupported by RowConverter; if this \
         changes, update the caveat in `do_process_cdc_transaction`",
    );
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
        DeltaTableTransactionMode::None,
        false,
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

/// With the input endpoint paused between rounds, verify catchup batches each round's Delta
/// commits into a single Feldera transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_catchup_batches_multiple_versions_test() {
    let result = run_catchup_lag_experiment(catchup_lag_options(
        DeltaTableTransactionMode::Catchup,
        &[3, 2],
        None,
        None,
    ))
    .await;
    assert_eq!(
        result.follow_transactions_per_round,
        vec![1, 1],
        "catchup must use one Feldera transaction per pause/write/resume round"
    );
}

/// Control for [`delta_table_catchup_batches_multiple_versions_test`]: `always` mode commits once
/// per Delta log version in each round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_always_one_transaction_per_version_test() {
    let result = run_catchup_lag_experiment(catchup_lag_options(
        DeltaTableTransactionMode::Always,
        &[2, 2],
        None,
        None,
    ))
    .await;
    assert_eq!(
        result.follow_transactions_per_round,
        vec![2, 2],
        "always mode must commit each Delta version in its own Feldera transaction"
    );
}

/// `end_version` caps the catchup window target at startup of the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_catchup_end_version_caps_window_test() {
    const NUM_VERSIONS: i64 = 8;
    const END_VERSION: i64 = 5;
    let result = run_catchup_lag_experiment(catchup_lag_options(
        DeltaTableTransactionMode::Catchup,
        &[NUM_VERSIONS],
        Some(END_VERSION),
        None,
    ))
    .await;
    assert_eq!(
        result.follow_transactions_per_round,
        vec![1],
        "catchup with end_version must still batch versions 1..=end_version in one transaction"
    );
}

/// Large bursts across two rounds: each round still uses one Feldera transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_catchup_rapid_versions_test() {
    let result = run_catchup_lag_experiment(catchup_lag_options(
        DeltaTableTransactionMode::Catchup,
        &[6, 6],
        None,
        None,
    ))
    .await;
    assert_eq!(
        result.follow_transactions_per_round,
        vec![1, 1],
        "catchup must batch a large pre-existing commit backlog in one transaction per round"
    );
}

/// Catchup survives a transient unreadable table (chmod on input root): unhealthy, then healthy,
/// one Feldera transaction per catch-up round.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_catchup_transient_read_failure_test() {
    let result = run_catchup_lag_experiment(CatchupLagExperimentOptions {
        transaction_mode: DeltaTableTransactionMode::Catchup,
        versions_per_round: &[2, 2],
        end_version: None,
        snapshot_rows: None,
        max_retries: None,
        inject_read_failure_before_resume_round: Some(0),
        clear_read_failure_after_unhealthy: true,
        expect_ingest_failure: false,
    })
    .await;
    assert_eq!(
        result.follow_transactions_per_round,
        vec![1, 1],
        "catchup must still batch each round in one Feldera transaction after recovery"
    );
    assert!(
        !result.ended_unhealthy,
        "connector must be healthy after permissions are restored"
    );
    assert_eq!(
        result.completed_version,
        Some(4),
        "all four follow commits (two per round) must be ingested"
    );
}

/// With `max_retries = 0`, a read failure is fatal: connector stays unhealthy and does not advance.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_catchup_max_retries_fatal_test() {
    let result = run_catchup_lag_experiment(CatchupLagExperimentOptions {
        transaction_mode: DeltaTableTransactionMode::Catchup,
        versions_per_round: &[1],
        end_version: None,
        snapshot_rows: None,
        max_retries: Some(0),
        inject_read_failure_before_resume_round: Some(0),
        clear_read_failure_after_unhealthy: false,
        expect_ingest_failure: true,
    })
    .await;
    assert!(
        result.ended_unhealthy,
        "connector must remain unhealthy after retries are exhausted"
    );
    assert!(
        result.completed_version.is_none_or(|v| v < 1),
        "connector must not ingest the commit written while paused (completed: {:?})",
        result.completed_version
    );
    assert!(
        result.follow_transactions_per_round.iter().all(|&n| n == 0),
        "no follow transaction should complete when ingestion fails immediately: {:?}",
        result.follow_transactions_per_round
    );
}

#[tokio::test]
async fn delta_table_follow_file_catchup_test() {
    delta_table_follow_file_test_common(false, DeltaTableTransactionMode::Catchup, false, false)
        .await
}

/// In `snapshot_and_follow` + `catchup`, the initial snapshot uses one Feldera transaction; each
/// follow catch-up window still uses one transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_snapshot_and_follow_catchup_snapshot_transaction_test() {
    const SNAPSHOT_ROWS: i64 = 10;
    let result = run_catchup_lag_experiment(catchup_lag_options(
        DeltaTableTransactionMode::Catchup,
        &[2],
        None,
        Some(SNAPSHOT_ROWS),
    ))
    .await;
    assert_eq!(
        result.snapshot_transaction_starts, 1,
        "catchup must ingest the initial snapshot in one Feldera transaction"
    );
    assert_eq!(
        result.follow_transactions_per_round,
        vec![1],
        "catchup must batch follow commits in one Feldera transaction per round"
    );
}

/// Regression test for a bug where `snapshot` transaction mode ingested the initial snapshot
/// without starting a Feldera transaction: `follow_start_transaction` dropped the `snapshot-*`
/// label instead of propagating it. The `snapshot_transaction_starts` metric counts transactions
/// actually started (the label reaching a queue entry), so it reads 0 when the bug is present.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_table_snapshot_mode_snapshot_transaction_test() {
    const SNAPSHOT_ROWS: i64 = 10;
    let result = run_catchup_lag_experiment(catchup_lag_options(
        DeltaTableTransactionMode::Snapshot,
        &[2],
        None,
        Some(SNAPSHOT_ROWS),
    ))
    .await;
    assert_eq!(
        result.snapshot_transaction_starts, 1,
        "snapshot mode must ingest the initial snapshot in one Feldera transaction"
    );
    assert_eq!(
        result.follow_transactions_per_round,
        vec![0],
        "snapshot mode must not start a Feldera transaction for follow commits"
    );
}

#[tokio::test]
async fn delta_table_snapshot_and_follow_file_catchup_test() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::Catchup, false, false)
        .await
}

#[tokio::test]
async fn delta_table_follow_file_catchup_end_version_test() {
    delta_table_follow_file_test_common(false, DeltaTableTransactionMode::Catchup, false, true)
        .await
}

#[tokio::test]
async fn delta_table_follow_file_catchup_suspend_test() {
    delta_table_follow_file_test_common(false, DeltaTableTransactionMode::Catchup, true, false)
        .await
}

#[tokio::test]
async fn delta_table_snapshot_and_follow_file_catchup_suspend_test() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::Catchup, true, false).await
}

#[tokio::test]
async fn delta_table_snapshot_and_follow_file_catchup_end_version_test() {
    delta_table_follow_file_test_common(true, DeltaTableTransactionMode::Catchup, false, true).await
}

#[tokio::test]
async fn delta_table_follow_file_test() {
    delta_table_follow_file_test_common(false, DeltaTableTransactionMode::None, false, false).await
}

#[tokio::test]
async fn delta_table_follow_file_test_suspend() {
    delta_table_follow_file_test_common(false, DeltaTableTransactionMode::None, true, false).await
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
        DeltaTableTransactionMode::None,
        suspend,
        false,
        1000,
        100,
        None,
        None,
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

        delta_table_output_test(data.clone(), &table_uri, &HashMap::new(), true, None, false);

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

    /// ```text
    /// input.json --> [pipeline1]--->delta_table
    /// ```
    #[test]
    fn delta_table_file_output_proptest_index_alias(data in delta_data(20_000))
    {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        // Uncomment to inspect output parquet files produced by the test.
        forget(table_dir);

        delta_table_output_test(data.clone(), &table_uri, &HashMap::new(), true, None, true);

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
        delta_table_output_test(data.clone(), &table_uri, &object_store_config, false, None, false);
        //delta_table_output_test(data.clone(), &table_uri, &object_store_config, false, None);

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
