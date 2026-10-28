//! Tests of the Delta Lake connectors: the cases of the `delta_table` connector suite, which run
//! pipelines on an engine, and tests that reach no engine.

use crate::{
    Controller, PipelineState,
    adhoc::execute_sql,
    format::parquet::{ArrowSchemaOptions, relation_to_arrow_fields},
    integrated::delta_table::output::delta_variant_types,
    test::{
        delta, engine_test, off_runtime,
        pipeline::{PipelineTestEngine, TestCircuit},
        published_schema, read, relations, render, rows, run_async, run_async_on,
    },
};
use arrow::{
    array::{AsArray, RecordBatch},
    compute::cast,
    datatypes::{DataType as ArrowDataType, FieldRef, Int64Type, Schema as ArrowSchema},
};
use chrono::{DateTime, NaiveDate};
use delta_kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::{
    DeltaTable, DeltaTableBuilder,
    datafusion::prelude::{SessionContext, col, concat, lit, substr},
    ensure_table_uri,
    kernel::{DataType, StructField, StructType},
    operations::create::CreateBuilder,
    protocol::SaveMode,
    table::config::TableProperty,
};
use feldera_adapterlib::{
    errors::controller::ControllerError,
    test::{Capability, Updates, ZSet, default_timeout_ms, wait},
};
#[cfg(feature = "delta-unity-test")]
use feldera_storage::tokio::TOKIO;
use feldera_types::{
    config::{PipelineConfig, TransportConfig},
    constants::DATAFUSION_TEMP_DIR,
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
    transport::delta_table::{DeltaTableChangeFeed, DeltaTableTransactionMode},
};
use proptest::{
    collection::vec,
    prelude::{ProptestConfig, Strategy},
    proptest,
    strategy::ValueTree,
    test_runner::TestRunner,
};
use roaring::RoaringTreemap;
use serde_json::{Map, Value, json};
use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsStr,
    fs::File,
    io::Write,
    mem::forget,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tempfile::{NamedTempFile, TempDir};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};
use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Returns the rows that a file output connector wrote to `file` in the `insert_delete`
/// format, as a Z-set of rows of `relation`.
pub(super) fn file_zset(relation: &Relation, file: &NamedTempFile) -> ZSet {
    read::insert_delete_updates(relation, &std::fs::read(file.path()).unwrap())
        .unwrap()
        .zset()
        .unwrap()
}

/// Read a snapshot of the delta table at `table_uri`, with the columns of `relation`, to a
/// temporary JSON file.
pub(super) fn delta_table_snapshot_to_json(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    relation: &Relation,
    config: &HashMap<String, String>,
) -> NamedTempFile {
    delta_table_snapshot_to_json_values(
        engine,
        table_uri,
        relation,
        &config
            .iter()
            .map(|(key, value)| (key.clone(), Value::from(value.clone())))
            .collect(),
    )
}

/// As [`delta_table_snapshot_to_json`], for settings the connector reads as a
/// number rather than a string.
fn delta_table_snapshot_to_json_values(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    relation: &Relation,
    config: &HashMap<String, Value>,
) -> NamedTempFile {
    let start = Instant::now();
    let json_file = NamedTempFile::new().unwrap();
    println!(
        "delta_table_snapshot_to_json: writing output to {}",
        json_file.path().display()
    );

    let mut config = config.clone();
    config.insert("mode".to_string(), "snapshot".into());

    let input_pipeline = delta_table_input_pipeline(
        engine,
        table_uri,
        relation,
        &config,
        &json_file.path().display().to_string(),
    );
    input_pipeline.start();
    wait(|| input_pipeline.pipeline_complete(), 400_000).expect("timeout");
    input_pipeline.stop().unwrap();

    info!("Read delta snapshot in {:?}", start.elapsed());

    json_file
}

/// Arrow schema options matching what the Delta output connector writes by
/// default, so a table the tests create by hand lines up with one the connector
/// would have created.
pub fn delta_schema_options() -> ArrowSchemaOptions {
    ArrowSchemaOptions::new(true).with_parquet_variant(true)
}

const DELTA_TEST_INPUT_ENDPOINT: &str = "test_input1";

/// Watermark metadata reported by the input connector's completed frontier.
fn completed_frontier_metadata(pipeline: &Controller) -> Option<Value> {
    pipeline
        .status()
        .input_status()
        .values()
        .next()
        .and_then(|s| s.completed_frontier.completed_watermark())
        .map(|w| w.metadata)
}

/// Completed Delta table version reported by the input connector waterline.
///
/// The connector reports `version: null` until it has processed a table version,
/// which this reports as `None` rather than mistaking it for a number (issue 7174).
fn pipeline_completed_version(pipeline: &Controller) -> Option<i64> {
    completed_frontier_metadata(pipeline).and_then(|metadata| metadata["version"].as_i64())
}

/// How long to wait for `pipeline_completed_version` to reach a table version.
///
/// The version completes only once the output connector commits it, and a
/// single commit to GCS has taken 24 s (issue 7260).
const COMPLETED_VERSION_TIMEOUT_MS: u64 = 200_000;

/// One deterministic test row (even `bigint` so `bigint % 2 = 0` filters pass).
fn delta_test_record(bigint: i64) -> Value {
    let mut runner = TestRunner::default();
    let mut record = rows::row(&relation())
        .new_tree(&mut runner)
        .unwrap()
        .current();
    record.insert("bigint".to_string(), Value::from(bigint));
    // `unused` always holds a value, so that a read that skips the column shows as NULL.
    record.insert(
        "unused".to_string(),
        Value::from(format!("unused {bigint}")),
    );
    Value::Object(record)
}

/// Append a single row, producing exactly one new Delta table version.
async fn append_table_version(
    table: DeltaTable,
    relation: &Relation,
    record: &Value,
) -> DeltaTable {
    write_data_to_table(table, relation, std::slice::from_ref(record)).await
}

pub(super) fn delta_connector_counter(pipeline: &Controller, metric_name: &str) -> u64 {
    delta_connector_gauge(pipeline, metric_name) as u64
}

/// The `change_feed` the connector resolved from its configuration.
///
/// Use it where no observable distinguishes the settings: `auto` on a table that
/// records no change feed reads exactly what `off` reads.
fn delta_change_feed(pipeline: &Controller) -> DeltaTableChangeFeed {
    let endpoint_id = pipeline
        .input_endpoint_id_by_name(DELTA_TEST_INPUT_ENDPOINT)
        .expect("delta input endpoint must exist");
    let status = pipeline.status();
    let input_status = status.input_status();
    let config = &input_status
        .get(&endpoint_id)
        .expect("delta input endpoint must have status")
        .config
        .connector_config
        .transport;
    match config {
        TransportConfig::DeltaTableInput(config) => config.change_feed(),
        other => panic!("expected a delta input connector, got {other:?}"),
    }
}

/// Fail unless the connector read at least one commit from change data.
///
/// A change feed test whose config omits `change_feed` reads every commit from
/// file actions instead, which reaches the same rows and so passes every
/// assertion about contents. This asserts the path the test means to cover was
/// the one taken.
fn assert_read_change_data(pipeline: &Controller) {
    let from_change_data =
        delta_connector_counter(pipeline, "input_connector_delta_commits_from_change_data");
    assert!(
        from_change_data > 0,
        "no commit was read from change data, so this test covered the \
         file-action path; is `change_feed` set on the connector?"
    );
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
    /// In this round, append the first version while the endpoint runs, with listing of the
    /// input table's `_delta_log` blocked (Unix only), so that the connector reads the version but
    /// keeps retrying the catchup target listing. Pause during those retries, unblock the listing,
    /// and write the round's other versions while paused.
    pause_during_listing_round: Option<usize>,
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
        pause_during_listing_round: None,
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

fn wait_input_endpoint_unhealthy(pipeline: &Controller, timeout_ms: u64) {
    wait(|| input_endpoint_is_unhealthy(pipeline), timeout_ms)
        .unwrap_or_else(|_| panic!("timeout waiting for input connector to become unhealthy"));
}

fn wait_input_endpoint_healthy(pipeline: &Controller, timeout_ms: u64) {
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

/// With `max_retries = 0`, a read failure is fatal: connector stays unhealthy and does not advance.
#[cfg(unix)]
#[engine_test(requires(Storage))]
pub fn catchup_max_retries_fatal_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        if running_as_root() {
            eprintln!("skipping: root ignores directory permissions");
            return;
        }
        let result = run_catchup_lag_experiment(
            engine,
            CatchupLagExperimentOptions {
                transaction_mode: DeltaTableTransactionMode::Catchup,
                versions_per_round: &[1],
                end_version: None,
                snapshot_rows: None,
                max_retries: Some(0),
                inject_read_failure_before_resume_round: Some(0),
                clear_read_failure_after_unhealthy: false,
                expect_ingest_failure: true,
                pause_during_listing_round: None,
            },
        )
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
    })
}

#[engine_test(requires(Storage))]
pub fn follow_file_catchup_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            false,
            DeltaTableTransactionMode::Catchup,
            false,
            false,
        )
        .await
    })
}

/// In `snapshot_and_follow` + `catchup`, the initial snapshot uses one Feldera transaction; each
/// follow catch-up window still uses one transaction.
#[engine_test(requires(Storage))]
pub fn snapshot_and_follow_catchup_snapshot_transaction_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        const SNAPSHOT_ROWS: i64 = 10;
        let result = run_catchup_lag_experiment(
            engine,
            catchup_lag_options(
                DeltaTableTransactionMode::Catchup,
                &[2],
                None,
                Some(SNAPSHOT_ROWS),
            ),
        )
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
    })
}

/// Regression test for a bug where `snapshot` transaction mode ingested the initial snapshot
/// without starting a Feldera transaction: `follow_start_transaction` dropped the `snapshot-*`
/// label instead of propagating it. The `snapshot_transaction_starts` metric counts transactions
/// actually started (the label reaching a queue entry), so it reads 0 when the bug is present.
#[engine_test(requires(Storage))]
pub fn snapshot_mode_snapshot_transaction_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        const SNAPSHOT_ROWS: i64 = 10;
        let result = run_catchup_lag_experiment(
            engine,
            catchup_lag_options(
                DeltaTableTransactionMode::Snapshot,
                &[2],
                None,
                Some(SNAPSHOT_ROWS),
            ),
        )
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
    })
}

#[engine_test(requires(Storage))]
pub fn snapshot_and_follow_file_catchup_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::Catchup,
            false,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage))]
pub fn follow_file_catchup_end_version_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            false,
            DeltaTableTransactionMode::Catchup,
            false,
            true,
        )
        .await
    })
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn follow_file_catchup_suspend_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            false,
            DeltaTableTransactionMode::Catchup,
            true,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn snapshot_and_follow_file_catchup_suspend_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::Catchup,
            true,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage))]
pub fn snapshot_and_follow_file_catchup_end_version_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::Catchup,
            false,
            true,
        )
        .await
    })
}

#[engine_test(requires(Storage))]
pub fn follow_file_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            false,
            DeltaTableTransactionMode::None,
            false,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn follow_file_test_suspend(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            false,
            DeltaTableTransactionMode::None,
            true,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage))]
pub fn follow_file_test_end_version(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            false,
            DeltaTableTransactionMode::None,
            false,
            true,
        )
        .await
    })
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn snapshot_and_follow_file_test_suspend(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::None,
            true,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn transactional_snapshot_and_follow_file_test_suspend(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::Snapshot,
            true,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn transactional_always_snapshot_and_follow_file_test_suspend(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::Always,
            true,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn snapshot_and_follow_file_test_suspend_end_version(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::None,
            true,
            true,
        )
        .await
    })
}

/// Build a Delta input pipeline and, for each entry in `versions_per_round`, pause the connector
/// (when needed), append that many Delta commits, resume, and wait for ingestion to catch up.
///
/// With `snapshot_rows`, uses `snapshot_and_follow`: writes that many rows before starting the
/// pipeline (unpaused), waits for the follow phase, and checks that the snapshot used exactly one
/// Feldera transaction before running follow rounds.
async fn run_catchup_lag_experiment(
    engine: &dyn PipelineTestEngine,
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
        pause_during_listing_round,
    } = opts;

    let relation_schema = relation();
    let arrow_fields = relation_to_arrow_fields(&relation_schema.fields, delta_schema_options());
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
            record["unused"] = Value::Null;
            snapshot_records.push(record);
        }
        table = write_data_to_table(table, &relation_schema, &snapshot_records).await;
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
        serde_json::to_value(transaction_mode.clone()).unwrap(),
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
    let pipeline = off_runtime(move || {
        delta_to_delta_pipeline(
            engine,
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
    });

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
        let metric_at_round_start = delta_follow_transaction_starts(&pipeline);
        let version_before_burst = table.version().unwrap() as i64;
        let pause_during_listing = pause_during_listing_round == Some(round);
        #[cfg(unix)]
        let mut listing_restore = Vec::new();
        let mut written_while_running = 0;
        if pause_during_listing {
            assert!(
                snapshot_and_follow || round > 0,
                "the endpoint must be running at the start of round {round}"
            );
            #[cfg(not(unix))]
            panic!("blocking the catchup target listing requires a Unix platform");
            let record = delta_test_record(record_index * 2);
            record_index += 1;
            // Reading a commit by name still works, but listing the log fails and is retried.
            #[cfg(unix)]
            append_table_version_with_listing_blocked(
                &table,
                &input_table_root,
                &relation_schema,
                &record,
                &mut listing_restore,
            )
            .await;
            let mut record = record;
            record["unused"] = Value::Null;
            expected_output.push(record);
            written_while_running = 1;
            // The connector read the new version and failed to list the log.
            wait_input_endpoint_unhealthy(&pipeline, 60_000);
        }
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
        #[cfg(unix)]
        if pause_during_listing {
            // The pause reaches the connector asynchronously. Wait for it, then let the listing
            // succeed while paused.
            let reader = pipeline
                .get_input_endpoint(DELTA_TEST_INPUT_ENDPOINT)
                .unwrap()
                .as_any()
                .downcast::<super::input::DeltaTableInputReader>()
                .unwrap();
            wait(|| reader.state() == PipelineState::Paused, 60_000)
                .expect("timeout waiting for the pause to reach the connector");
            restore_delta_input_table_read_permission(std::mem::take(&mut listing_restore))
                .unwrap_or_else(|e| panic!("round {round}: unblock listing of the delta log: {e}"));
            wait_input_endpoint_healthy(&pipeline, 60_000);
            table.update_incremental(None).await.unwrap();
        }

        for _ in written_while_running..num_versions {
            let record = delta_test_record(record_index * 2);
            record_index += 1;
            table = append_table_version(table, &relation_schema, &record).await;
            let new_version = table.version().unwrap() as i64;
            if end_version.is_none() || new_version <= end_version.unwrap() {
                let mut record = record;
                record["unused"] = Value::Null;
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

        if transaction_mode == DeltaTableTransactionMode::Catchup {
            assert_eq!(
                delta_catchup_target_version(&pipeline),
                None,
                "round {round}: no catchup window may be open while the endpoint is paused"
            );
        }

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

        // The connector sets these just after it queues the commit, so they can lag the
        // completed version.
        let start = Instant::now();
        while delta_last_ingested_version(&pipeline) != Some(target_version)
            || delta_catchup_target_version(&pipeline).is_some()
        {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "round {round}: last_ingested_version must reach {target_version} and \
                 catchup_target_version must clear (got {:?} and {:?})",
                delta_last_ingested_version(&pipeline),
                delta_catchup_target_version(&pipeline),
            );
            sleep(Duration::from_millis(10)).await;
        }

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
        wait_for_output_records(
            &mut output_table,
            &relation_schema,
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

/// Suspending a pipeline before its Delta connector has read anything checkpoints
/// `version: null`.  The connector republishes that state as a watermark when it
/// resumes, so its completed frontier reports a null version until the connector
/// processes a table version (issue 7174).
#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn completed_frontier_null_version_after_resume(engine: &dyn PipelineTestEngine) {
    run_async(async {
        init_logging();

        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();
        let output_table_dir = TempDir::new().unwrap();
        let output_table_uri = output_table_dir.path().display().to_string();
        let storage_dir = TempDir::new().unwrap();

        let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
            &relation().fields,
            delta_schema_options(),
        ));
        let input_table = create_table_from_arrow(&input_table_uri, &arrow_schema, &[]).await;
        write_data_to_table(input_table, &relation(), &[delta_test_record(0)]).await;

        // In `snapshot_and_follow` mode the connector reports no table version until it
        // reads the snapshot, which it cannot do while its endpoint is paused.
        let input_config =
            HashMap::from([("mode".to_string(), Value::from("snapshot_and_follow"))]);
        let start = async || {
            start_delta_to_delta_pipeline(
                engine,
                &input_table_uri,
                &output_table_uri,
                &input_config,
                &HashMap::new(),
                storage_dir.path(),
                1000,
                100,
            )
            .await
        };

        // Checkpoint the connector in its initial state, then resume from it.
        suspend_pipeline(start().await).await;
        let pipeline = start().await;

        wait(|| completed_frontier_metadata(&pipeline).is_some(), 20_000)
            .expect("timeout waiting for the resumed connector to report its completed frontier");

        let metadata = completed_frontier_metadata(&pipeline).unwrap();
        assert!(
            metadata["version"].is_null(),
            "a connector that has read nothing must report no table version, got {metadata}"
        );
        assert_eq!(pipeline_completed_version(&pipeline), None);

        pipeline.stop().unwrap();
    })
}

/// Wait until `table` contains exactly the rows `expected_output` of `relation`.
async fn wait_for_output_records(
    table: &mut Arc<DeltaTable>,
    relation: &Relation,
    expected_output: &[Value],
    datafusion: &SessionContext,
    timeout_ms: u64,
    dedup: bool,
) {
    let expected = rows_zset(relation, expected_output);
    let start = Instant::now();
    // Scans address files by the table's `root_url()`, so register that store
    // before querying. Same as `register_snapshot_table` in input.rs.
    let log_store = table.log_store();
    datafusion
        .runtime_env()
        .register_object_store(log_store.root_url(), log_store.root_object_store());
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

        let mut result = delta::published_rows(relation, &data)
            .unwrap()
            .zset()
            .unwrap();
        if dedup {
            result = distinct(&result);
        }

        info!(
            "expected output table size: {}, current size: {}",
            expected_output.len(),
            row_count(&result)
        );

        if row_count(&result) == expected_output.len() as i64 {
            assert_zset_eq(&result, &expected, "the delta table");
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

/// Returns the rows of the materialized `table`, which has the columns of `relation`.
async fn materialized_rows(
    pipeline: &Controller,
    table: &SqlIdentifier,
    relation: &Relation,
) -> ZSet {
    let data = execute_sql(pipeline, &format!("select * from {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    delta::published_rows(relation, &data)
        .unwrap()
        .zset()
        .unwrap()
}

/// Wait until the materialized `table` holds exactly the rows `expected_output` of
/// `relation`.
async fn wait_for_records_materialized(
    pipeline: &Controller,
    table: &SqlIdentifier,
    relation: &Relation,
    expected_output: &[Value],
) {
    let expected = rows_zset(relation, expected_output);
    let start = Instant::now();
    loop {
        let result = materialized_rows(pipeline, table, relation).await;

        info!(
            "expected output table size: {}, current size: {}",
            expected_output.len(),
            row_count(&result)
        );

        if result == expected {
            break;
        }

        if start.elapsed() > Duration::from_millis(default_timeout_ms()) {
            panic!("timeout");
        }

        sleep(Duration::from_millis(1_000)).await;
    }
}

/// Write `data`, rows of `relation`, to `table`.
async fn write_data_to_table(table: DeltaTable, relation: &Relation, data: &[Value]) -> DeltaTable {
    let batch = delta::table_batch(relation, updates(relation, data).rows()).unwrap();

    // Write it to the input table.
    table
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

pub(super) async fn create_table(
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
fn delta_table_input_pipeline(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    relation: &Relation,
    config: &HashMap<String, Value>,
    output_file_path: &str,
) -> Controller {
    try_delta_table_input_pipeline(
        engine,
        table_uri,
        relation,
        config,
        output_file_path,
        Box::new(move |e, _| {
            panic!("delta_table_input_test: error: {e}");
        }),
    )
    .unwrap()
}

/// [`delta_table_input_pipeline`], returning a start-up error and passing later errors to `error_cb`.
fn try_delta_table_input_pipeline(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    relation: &Relation,
    config: &HashMap<String, Value>,
    output_file_path: &str,
    error_cb: Box<dyn Fn(Arc<ControllerError>, Option<String>) + Send + Sync>,
) -> Result<Controller, ControllerError> {
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

    engine.controller(&identity(relation, &[None]), &config, error_cb)
}

/// `cdc_delete_filter` is parsed against the snapshot schema at
/// connector startup, so an expression that references a column that
/// does not exist on the Delta table must fail before any row flows
/// through the pipeline, with an error that names the missing column
/// and the `cdc_delete_filter` setting.
#[engine_test(requires(Storage))]
pub fn cdc_invalid_delete_filter_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
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
            off_runtime(move || {
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
                engine.controller(
                    &identity(&basic_relation(), &[Some("output")]),
                    &pipeline_config,
                    Box::new(move |_, _| {}),
                )
            })
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
    })
}

/// Build a pipeline that reads from a delta table and writes to a delta table.
#[allow(clippy::too_many_arguments)]
fn delta_to_delta_pipeline(
    engine: &dyn PipelineTestEngine,
    input_table_uri: &str,
    skip_unused_columns: bool,
    input_config: &HashMap<String, Value>,
    output_table_uri: &str,
    output_config: &HashMap<String, String>,
    buffer_size: u64,
    buffer_timeout_ms: u64,
    storage_dir: &Path,
    paused: bool,
) -> Controller {
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

    engine
        .controller(
            &identity(&relation(), &[Some("output")]),
            &config,
            Box::new(move |e, _| panic!("delta_to_delta pipeline: error: {e}")),
        )
        .unwrap()
}

/// Build a pipeline that reads from a delta table into a table keyed on `bigint`.
fn delta_read_pipeline(
    engine: &dyn PipelineTestEngine,
    input_table_uri: &str,
    input_config: &HashMap<String, Value>,
    storage_dir: &Path,
) -> Controller {
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

    engine
        .controller(
            &keyed(Some("output"), false),
            &config,
            Box::new(move |e, _| panic!("delta_read pipeline: error: {e}")),
        )
        .unwrap()
}

/// Build a pipeline that continuously follows a JSON file and writes changes
/// (both inserts and deletes) to a delta table.
fn delta_write_pipeline(
    engine: &dyn PipelineTestEngine,
    input_file_path: &str,
    table_uri: &str,
    index: bool,
    config: &HashMap<String, Value>,
) -> Controller {
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

    engine
        .controller(
            &keyed(Some("output"), false),
            &config,
            Box::new(move |e, _| panic!("delta_write pipeline: error: {e}")),
        )
        .unwrap()
}

/// Read the rows of [`relation`] that the current Delta table snapshot holds,
/// ignoring the `__feldera_op`/`__feldera_ts` metadata columns and any orphaned parquet
/// files left behind by previous `truncate` commits.
fn read_delta_snapshot(table_uri: &str) -> ZSet {
    // The connector creates the table lazily; treat "not yet created" as empty.
    delta::table_rows(&relation(), table_uri)
        .map(|rows| rows.zset().unwrap())
        .unwrap_or_else(|_| ZSet::empty(published_schema(&relation())).unwrap())
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
#[engine_test]
pub fn output_non_unique_keys_skipped_test(engine: &dyn PipelineTestEngine) {
    init_logging();

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // Ten well-formed keys, one value each.
    let unique: Vec<Value> = (0..10).map(delta_test_record).collect();

    // Two non-unique keys, each with two distinct values inserted in the same
    // batch. `delta_test_record` is deterministic, so we mutate a field to make
    // the second value differ from the first.
    let make_conflict = |key: i64| {
        let first = delta_test_record(key);
        let mut second = first.clone();
        second["int"] = Value::from(int(&first).wrapping_add(1));
        assert_eq!(bigint(&first), bigint(&second));
        assert_ne!(first, second);
        [first, second]
    };
    let conflict_a = make_conflict(100);
    let conflict_b = make_conflict(200);

    // A non-unique key carrying eleven distinct values. This exceeds the
    // ten-value cap of `non_unique_key_error`, so the diagnostic builder leaves
    // the value cursor partway through. The skip path must advance the key
    // cursor on its own; otherwise the encode thread loops forever on this key.
    let make_conflict_n = |key: i64, n: i32| -> Vec<Value> {
        let base = delta_test_record(key);
        let records: Vec<Value> = (0..n)
            .map(|i| {
                let mut record = base.clone();
                record["int"] = Value::from(int(&base).wrapping_add(i));
                record
            })
            .collect();
        assert!(records.iter().all(|r| bigint(r) == key));
        assert_eq!(
            records
                .iter()
                .map(Value::to_string)
                .collect::<HashSet<_>>()
                .len(),
            n as usize,
            "the values for a non-unique key must be pairwise distinct"
        );
        records
    };
    let conflict_c = make_conflict_n(300, 11);

    let all_records: Vec<Value> = unique
        .iter()
        .cloned()
        .chain(conflict_a.iter().cloned())
        .chain(conflict_b.iter().cloned())
        .chain(conflict_c.iter().cloned())
        .collect();

    // Write all records (as raw inserts) to the input file.
    let mut input_file = NamedTempFile::new().unwrap();
    input_file
        .write_all(
            render::json_lines(&relation(), updates(&relation(), &all_records).rows()).as_bytes(),
        )
        .unwrap();

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
    let errors = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
    let errors_clone = errors.clone();

    // The view holds every record, so the index on it ends up with several values for
    // a non-unique key.
    let controller = engine
        .controller(
            &TestCircuit::IndexedView {
                key: key_relation(),
                relation: relation(),
                index: "idx1".to_string(),
            },
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
        || row_count(&read_delta_snapshot(&table_uri)) == unique.len() as i64,
        60_000,
    )
    .expect("timeout waiting for the well-formed records to reach the Delta table");

    controller.stop().unwrap();

    // (a) Exactly the well-formed records are written; the non-unique keys are
    //     dropped entirely.
    let written = read_delta_snapshot(&table_uri);
    assert_eq!(
        written,
        rows_zset(&relation(), &unique),
        "non-unique keys must be skipped while every unique key is preserved"
    );
    assert!(
        zset_bigints(&written)
            .iter()
            .all(|bigint| *bigint != 100 && *bigint != 200 && *bigint != 300),
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
/// * `index_is_alias` - if true, the index is an alias of the view, as the SQL compiler
///   registers the first index of a materialized view; otherwise, the index is a stream of
///   its own.
pub(super) fn delta_table_output_test(
    engine: &dyn PipelineTestEngine,
    data: Vec<Value>,
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
    input_file
        .write_all(render::json_lines(&relation(), updates(&relation(), &data).rows()).as_bytes())
        .unwrap();

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

    let controller = engine
        .controller(
            &keyed(None, index_is_alias),
            &config,
            Box::new(move |e, _| panic!("delta_table_output_test: error: {e}")),
        )
        .unwrap();

    controller.start();

    wait(|| controller.pipeline_complete(), 100_000).unwrap();

    if verify {
        let parquet_files: Vec<PathBuf> = delta::parquet_files(Path::new(table_uri))
            .into_iter()
            .filter(|path| !path.display().to_string().ends_with(".checkpoint.parquet"))
            .collect();

        // // Uncomment to inspect the input JSON file.
        // std::mem::forget(input_file);

        let output = delta::file_rows(&relation(), &parquet_files)
            .unwrap()
            .zset()
            .unwrap();
        assert_zset_eq(&output, &rows_zset(&relation(), &data), "the delta table");
    }

    controller.stop().unwrap();
    println!("delta_table_output_test: success");
}

pub fn init_logging() {
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

/// Checkpoint a running pipeline and stop it.
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

/// Suspending and resuming a `cdf` connector ingests the commits made while it
/// was down exactly once.
///
/// Suspension leaves the connector on a Delta version boundary, so the ad hoc
/// query after the resume returns each row once.
#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn change_feed_suspend_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        init_logging();

        let relation = basic_relation();
        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;

        let initial: Vec<Value> = (0..4).map(|id| change_feed_row(id, "initial")).collect();
        delta = write_data_to_table(delta, &basic_relation(), &initial).await;

        // The connector resumes from the checkpoint this directory holds, so both
        // runs share it.
        let storage_dir = TempDir::new().unwrap();
        let output = SqlIdentifier::from("test_output1");
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto" }),
            &relation,
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();
        wait_or_connector_error(&pipeline, &output, &relation, &initial, &errors).await;

        // One change feed commit before the checkpoint.
        delta = delta
            .update()
            .with_predicate(col("id").eq(lit(0i64)))
            .with_update("s", lit("before suspend"))
            .await
            .unwrap()
            .0;
        let mut expected = initial.clone();
        expected[0]["s"] = Value::from("before suspend");
        wait_or_connector_error(&pipeline, &output, &relation, &expected, &errors).await;

        let (sender, mut receiver) = mpsc::channel(1);
        pipeline.start_suspend(Box::new(move |result| sender.try_send(result).unwrap()));
        timeout(Duration::from_secs(100), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        pipeline.stop().unwrap();

        // Two more commits while the connector is down: one recorded in the change
        // feed, one an append that is not.
        delta = delta
            .update()
            .with_predicate(col("id").eq(lit(1i64)))
            .with_update("s", lit("while down"))
            .await
            .unwrap()
            .0;
        expected[1]["s"] = Value::from("while down");
        let appended = vec![change_feed_row(4, "after restart")];
        write_data_to_table(delta, &basic_relation(), &appended).await;
        expected.extend(appended.iter().cloned());

        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto" }),
            &relation,
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();
        wait_or_connector_error(&pipeline, &output, &relation, &expected, &errors).await;

        assert_read_change_data(&pipeline);
        pipeline.stop().unwrap();
    })
}

/// Start a pipeline that copies one delta table to another, with its input
/// endpoint paused.
#[allow(clippy::too_many_arguments)]
async fn start_delta_to_delta_pipeline(
    engine: &dyn PipelineTestEngine,
    input_table_uri: &str,
    output_table_uri: &str,
    input_config: &HashMap<String, Value>,
    output_config: &HashMap<String, String>,
    storage_dir: &Path,
    buffer_size: u64,
    buffer_timeout_ms: u64,
) -> Controller {
    let input_table_uri_clone = input_table_uri.to_string();
    let output_table_uri_clone = output_table_uri.to_string();
    let input_config_clone = input_config.clone();
    let output_config_clone = output_config.clone();
    let storage_dir_path = storage_dir.to_path_buf();
    let pipeline = off_runtime(move || {
        delta_to_delta_pipeline(
            engine,
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
    });

    println!("Pipeline created");

    pipeline.start();
    pipeline
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
pub(super) async fn test_follow(
    engine: &dyn PipelineTestEngine,
    relation: &Relation,
    input_table_uri: &str,
    output_table_uri: &str,
    storage_options: &HashMap<String, String>,
    data: Vec<Value>,
    snapshot: bool,
    transaction_mode: DeltaTableTransactionMode,
    suspend: bool,
    test_end_version: bool,
    buffer_size: u64,
    buffer_timeout_ms: u64,
    inject_failure: Option<Box<dyn Fn()>>,
    clear_failure: Option<Box<dyn Fn()>>,
) {
    init_logging();

    let storage_dir = TempDir::new().unwrap();

    let datafusion = SessionContext::new();

    // Create arrow schema
    let arrow_fields = relation_to_arrow_fields(&relation.fields, delta_schema_options());
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

        input_table = write_data_to_table(input_table, relation, &data[..median]).await;
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

    let mut pipeline = start_delta_to_delta_pipeline(
        engine,
        input_table_uri,
        output_table_uri,
        &input_config,
        storage_options,
        storage_dir.path(),
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

        pipeline = start_delta_to_delta_pipeline(
            engine,
            input_table_uri,
            output_table_uri,
            &input_config,
            storage_options,
            storage_dir.path(),
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
            if bigint(x) % 2 == 0 {
                let mut x = x.clone();
                x["unused"] = Value::Null;
                Some(x)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    wait_for_output_records(
        &mut output_table,
        relation,
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
            if let Some(version) = pipeline_completed_version(&pipeline) {
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
        COMPLETED_VERSION_TIMEOUT_MS,
    )
    .unwrap();

    // Write remaining data in 10 chunks, wait for it to show up in the output table.
    for chunk in data[split_at..].chunks(std::cmp::max(data[split_at..].len() / 10, 1)) {
        total_count += chunk.len();
        input_table = write_data_to_table(input_table, relation, chunk).await;

        if !test_end_version || input_table.version().unwrap() as i64 <= end_version {
            expected_output = data[0..total_count]
                .iter()
                .filter_map(|x| {
                    if bigint(x) % 2 == 0 {
                        let mut x = x.clone();
                        x["unused"] = Value::Null;
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

            pipeline = start_delta_to_delta_pipeline(
                engine,
                input_table_uri,
                output_table_uri,
                &input_config,
                storage_options,
                storage_dir.path(),
                buffer_size,
                buffer_timeout_ms,
            )
            .await;
        }

        // Test the waterline tracking mechanism.
        wait(
                || {
                    if let Some(version) = pipeline_completed_version(&pipeline) {
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
                COMPLETED_VERSION_TIMEOUT_MS,
            )
            .unwrap();

        // Wait a bit to make sure the pipeline doesn't process data beyond end_version.
        if test_end_version && input_table.version().unwrap() as i64 > end_version {
            sleep(Duration::from_millis(1000)).await;
        }

        // Use timeout of 0: once the pipeline indicates that the completed_version
        // reached the expected version (see above check), the output table should be up-to-date.
        wait_for_output_records(
            &mut output_table,
            relation,
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

        pipeline = start_delta_to_delta_pipeline(
            engine,
            input_table_uri,
            output_table_uri,
            &input_config,
            storage_options,
            storage_dir.path(),
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

/// Writes `data`, rows of `relation`, to `file` as insertions when `polarity` is true and as
/// deletions otherwise, in the `insert_delete` format.
fn write_updates_as_json(file: &mut File, relation: &Relation, data: &[Value], polarity: bool) {
    let rows = updates(relation, data);
    let operation = if polarity { "insert" } else { "delete" };
    let mut buffer = String::new();
    for index in 0..rows.len() {
        let mut update = Map::new();
        update.insert(
            operation.to_string(),
            render::json_object(relation, rows.rows(), index),
        );
        buffer.push_str(&Value::Object(update).to_string());
        buffer.push('\n');
    }
    file.write_all(buffer.as_bytes()).unwrap();
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
pub(super) async fn test_cdc(
    engine: &dyn PipelineTestEngine,
    _relation: &Relation,
    table_uri: &str,
    storage_options: &HashMap<String, Value>,
    data: Vec<Value>,
    suspend: bool,
    index: bool,
    transaction_mode: DeltaTableTransactionMode,
    skip_unused_columns: bool,
) {
    init_logging();

    // The record the connector is expected to emit. With `skip_unused_columns`,
    // the connector never reads the `unused` column (nullable, marked unused in
    // [`relation`]), so the `unused` column must contain NULL
    // regardless of what was written to the Delta table.
    let expected_record = |record: &Value| -> Value {
        let mut record = record.clone();
        if skip_unused_columns {
            record["unused"] = Value::Null;
        }
        record
    };

    let mut input_file = NamedTempFile::new().unwrap();
    let input_file_path = input_file.path().display().to_string();

    let table_uri_clone = table_uri.to_string();

    // Build pipeline 1.
    let mut output_config = storage_options.clone();

    output_config.insert("mode".to_string(), "truncate".into());

    let write_pipeline = off_runtime(move || {
        delta_write_pipeline(
            engine,
            &input_file_path,
            &table_uri_clone,
            index,
            &output_config,
        )
    });

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
    let mut read_pipeline = off_runtime(move || {
        delta_read_pipeline(
            engine,
            &table_uri_clone,
            &input_config_clone,
            &storage_dir_path,
        )
    });

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

            read_pipeline = off_runtime(move || {
                delta_read_pipeline(
                    engine,
                    &table_uri_clone,
                    &input_config_clone,
                    &storage_dir_path,
                )
            });

            read_pipeline.start();
        }

        wait_for_records_materialized(
            &read_pipeline,
            &SqlIdentifier::from("test_output1"),
            &relation(),
            &data[0..total_count]
                .iter()
                .filter_map(|x| {
                    if bigint(x) % 2 == 0 {
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

        write_updates_as_json(input_file.as_file_mut(), &relation(), chunk, true);
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
                    x["boolean"] = Value::from(!x["boolean"].as_bool().unwrap());
                    x
                })
                .collect::<Vec<_>>();
            write_updates_as_json(input_file.as_file_mut(), &relation(), &chunk, true);
            updated_count += chunk.len();

            let expected = data[0..total_count]
                .iter()
                .enumerate()
                .flat_map(|(i, x)| {
                    let mut x = expected_record(x);
                    if bigint(&x) % 2 == 0 {
                        if i < updated_count {
                            x["boolean"] = Value::from(!x["boolean"].as_bool().unwrap());
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
                &relation(),
                &expected,
            )
            .await;
        }
    }

    let mut deleted_count = 0;
    // Delete data chunk by chunk.
    for chunk in data.chunks(std::cmp::max(data.len() / 10, 1)) {
        write_updates_as_json(input_file.as_file_mut(), &relation(), chunk, false);

        deleted_count += chunk.len();

        wait_for_records_materialized(
            &read_pipeline,
            &SqlIdentifier::from("test_output1"),
            &relation(),
            &data[deleted_count..total_count]
                .iter()
                .filter_map(|x| {
                    if bigint(x) % 2 == 0 {
                        let mut x = expected_record(x);
                        if index {
                            x["boolean"] = Value::from(!x["boolean"].as_bool().unwrap());
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

/// The table that most cases read and write: a column of each type that a Delta table
/// holds, with `bigint` numbering the rows.
pub fn relation() -> Relation {
    relations::every_type("delta_table")
}

/// [`relation`] with the lateness that orders a snapshot: on `bigint` and on
/// `timestamp_ntz`.
pub fn lateness_relation() -> Relation {
    let mut relation = relations::every_type("delta_table_lateness");
    for field in &mut relation.fields {
        let lateness = match field.name.name().as_str() {
            "bigint" => "1000",
            "timestamp_ntz" => "interval '10 days'",
            _ => continue,
        };
        *field = field.clone().with_lateness(lateness);
    }
    relation
}

/// [`lateness_relation`] with a lateness of 1000 days on `timestamp_ntz`. The test rows have
/// timestamps from 1970 to 2100, so a read ordered by `timestamp_ntz` with a 10-day lateness
/// runs about 4,750 range queries, and each one decodes most of the table. A 1000-day lateness
/// runs about 48.
pub fn coarse_lateness_relation() -> Relation {
    let mut relation = lateness_relation();
    relation.name = "delta_table_coarse_lateness".into();
    for field in &mut relation.fields {
        if field.name.name() == "timestamp_ntz" {
            *field = field.clone().with_lateness("interval '1000 days'");
        }
    }
    relation
}

/// The index of [`relation`] on `bigint`.
pub fn key_relation() -> Relation {
    Relation::new(
        "delta_table_key".into(),
        vec![Field::new("bigint".into(), ColumnType::bigint(false))],
        false,
        BTreeMap::new(),
    )
}

/// A table with the columns of [`relations::basic`].
pub fn basic_relation() -> Relation {
    relations::basic("delta_table_basic")
}

/// [`basic_relation`] with its nullable column `i` marked unused, so that a connector may
/// skip it.
pub fn basic_unused_i_relation() -> Relation {
    let mut relation = relations::basic("delta_table_basic_unused_i");
    for field in &mut relation.fields {
        field.unused = field.name.name() == "i";
    }
    relation
}

/// The index of [`basic_relation`] on `id`.
pub fn basic_key_relation() -> Relation {
    relations::basic_key("delta_table_basic_key")
}

/// The table that the soft delete case reads into.
pub fn soft_delete_relation() -> Relation {
    relations::soft_delete("delta_table_soft_delete")
}

/// A table whose column names are quoted uppercase SQL identifiers, as a table imported from
/// a system that folds identifiers to upper case carries.
pub fn upper_case_relation() -> Relation {
    Relation::new(
        "delta_table_upper_case".into(),
        vec![
            Field::new(SqlIdentifier::new("ID", true), ColumnType::bigint(false)),
            Field::new(
                SqlIdentifier::new("PAYLOAD", true),
                ColumnType::varchar(false),
            ),
            Field::new(
                SqlIdentifier::new("REGION", true),
                ColumnType::varchar(false),
            ),
        ],
        false,
        BTreeMap::new(),
    )
}

/// [`upper_case_relation`] with the LATENESS on `ID` that an ordered read needs.
pub fn upper_case_lateness_relation() -> Relation {
    let mut relation = upper_case_relation();
    relation.name = "delta_table_upper_case_lateness".into();
    relation.fields[0] = relation.fields[0].clone().with_lateness("2");
    relation
}

/// The table of [`upper_case_relation`] declared in SQL without quotes, so that the two sides
/// spell the columns differently.
pub fn unquoted_relation() -> Relation {
    Relation::new(
        "delta_table_unquoted".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("payload".into(), ColumnType::varchar(false)),
            Field::new("region".into(), ColumnType::varchar(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The table that the VARIANT cases read: one column stored as a Delta `variant`, one stored
/// as a Delta `string` holding JSON text.
pub fn variant_relation() -> Relation {
    Relation::new(
        "delta_table_variant".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("v".into(), ColumnType::variant(true)),
            Field::new("json_v".into(), ColumnType::variant(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The table that the Uniform cases read.
pub fn uniform_relation() -> Relation {
    Relation::new(
        "delta_table_uniform".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(true)),
            Field::new("label".into(), ColumnType::varchar(true)),
            Field::new(
                "after".into(),
                ColumnType::structure(true, &uniform_nested_columns()),
            ),
            Field::new(
                "history".into(),
                ColumnType::array(true, ColumnType::structure(true, &uniform_nested_columns())),
            ),
            Field::new(
                "tags".into(),
                ColumnType::map(
                    true,
                    ColumnType::varchar(false),
                    ColumnType::structure(true, &uniform_nested_columns()),
                ),
            ),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The columns of the nested values of [`uniform_relation`]. They are column-mapped too, so
/// reading them needs field-id matching inside the struct.
fn uniform_nested_columns() -> Vec<Field> {
    vec![
        Field::new("merchant".into(), ColumnType::varchar(true)),
        Field::new("status".into(), ColumnType::varchar(true)),
    ]
}

/// [`uniform_relation`] with the `op` and `ts` columns that the CDC configuration names but
/// the SQL table never declares.
fn uniform_cdc_relation() -> Relation {
    let mut relation = uniform_relation();
    relation.name = "delta_table_uniform_cdc".into();
    relation
        .fields
        .push(Field::new("op".into(), ColumnType::varchar(true)));
    relation
        .fields
        .push(Field::new("ts".into(), ColumnType::bigint(true)));
    relation
}

/// A row of [`uniform_relation`] whose nested values all hold `merchant` and `status`.
fn uniform_row(id: i64, label: &str, merchant: &str, status: &str) -> Value {
    let nested = json!({ "merchant": merchant, "status": status });
    let mut tags = Map::new();
    tags.insert(label.to_string(), nested.clone());
    json!({
        "id": id,
        "label": label,
        "after": nested,
        "history": [nested],
        "tags": tags,
    })
}

/// A row of [`uniform_cdc_relation`].
fn uniform_cdc_row(id: i64, label: &str, merchant: &str, status: &str, op: &str, ts: i64) -> Value {
    let mut row = uniform_row(id, label, merchant, status);
    row["op"] = Value::from(op);
    row["ts"] = Value::from(ts);
    row
}

/// The table of the people dataset in the CI object store.
#[cfg(any(feature = "delta-s3-test", feature = "delta-unity-test"))]
pub fn people_relation() -> Relation {
    Relation::new(
        "delta_table_people".into(),
        vec![
            Field::new("id".into(), ColumnType::int(false)),
            Field::new("firstName".into(), ColumnType::varchar(true)),
            Field::new("middleName".into(), ColumnType::varchar(true)),
            Field::new("lastName".into(), ColumnType::varchar(true)),
            Field::new("gender".into(), ColumnType::varchar(true)),
            Field::new("birthDate".into(), ColumnType::timestamp(true)),
            Field::new("ssn".into(), ColumnType::varchar(true)),
            Field::new("salary".into(), ColumnType::int(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Returns the circuit that copies the table `test_input1`, with the columns of `relation`, to
/// the view `test_output1`, whose persistent ids are `persistent_output_ids`.
fn identity(relation: &Relation, persistent_output_ids: &[Option<&str>]) -> TestCircuit {
    TestCircuit::Identity {
        relation: relation.clone(),
        persistent_output_ids: persistent_output_ids
            .iter()
            .map(|id| id.map(str::to_string))
            .collect(),
        input_properties: Vec::new(),
        materialized: true,
        persistent_ids: true,
        table_version: 0,
        attribute_defaults: BTreeMap::new(),
    }
}

/// Returns the circuit whose table `test_input1` has the columns of [`relation`] and the
/// primary key `bigint`, and whose view `test_output1`, with the persistent id `output`
/// unless `persistent_output_id` is `None`, has the index `idx1` on `bigint`.
fn keyed(persistent_output_id: Option<&str>, aliased: bool) -> TestCircuit {
    TestCircuit::Keyed {
        key: key_relation(),
        relation: relation(),
        persistent_output_ids: vec![persistent_output_id.map(str::to_string)],
        aliased,
    }
}

/// Returns `rows`, rows of `relation` as arrow-json reads them, each inserted once.
fn updates(relation: &Relation, rows: &[Value]) -> Updates {
    let rows: Vec<String> = rows.iter().map(Value::to_string).collect();
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
    Updates::from_json(published_schema(relation), &rows).unwrap()
}

/// Returns the Z-set of `rows`, rows of `relation` as arrow-json reads them.
pub(super) fn rows_zset(relation: &Relation, rows: &[Value]) -> ZSet {
    updates(relation, rows).zset().unwrap()
}

/// Returns the number of rows of `zset`, counted with their weights.
fn row_count(zset: &ZSet) -> i64 {
    zset.to_batch().unwrap().1.iter().sum()
}

/// Returns the distinct rows of `zset`, each with weight 1.
fn distinct(zset: &ZSet) -> ZSet {
    let (rows, weights) = zset.to_batch().unwrap();
    ZSet::from_batch(&rows, &vec![1; weights.len()]).unwrap()
}

/// Fails unless `actual` equals `expected`, naming only the rows in which they differ.
fn assert_zset_eq(actual: &ZSet, expected: &ZSet, context: &str) {
    let mut difference = actual.clone();
    difference.add(&expected.negate()).unwrap();
    assert!(
        difference.is_empty(),
        "{context}: the rows differ by (actual minus expected) {difference:?}"
    );
}

/// Returns the `bigint` of `row`, a row of [`relation`].
fn bigint(row: &Value) -> i64 {
    row["bigint"].as_i64().unwrap()
}

/// Returns the TIMESTAMP `millis` milliseconds after the epoch, as arrow-json reads it.
fn timestamp_value(millis: i64) -> Value {
    let timestamp = DateTime::from_timestamp_millis(millis).unwrap().naive_utc();
    Value::from(timestamp.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
}

/// Returns the `timestamp_ntz` of `row`, a row of [`relation`], in milliseconds after the epoch.
fn timestamp_millis(row: &Value) -> i64 {
    let text = row["timestamp_ntz"].as_str().unwrap();
    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_millis()
}

/// Returns the `id` of `row`, a row of [`basic_relation`].
fn id(row: &Value) -> i64 {
    row["id"].as_i64().unwrap()
}

/// Returns the `int` of `row`, a row of [`relation`].
fn int(row: &Value) -> i32 {
    row["int"].as_i64().unwrap() as i32
}

/// Returns the `bigint` of each distinct row of `zset`, a Z-set of rows of [`relation`].
fn zset_bigints(zset: &ZSet) -> Vec<i64> {
    let (rows, _) = zset.to_batch().unwrap();
    rows.column_by_name("bigint")
        .unwrap()
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

/// Generate up to `max_records` distinct records, with `bigint` numbering them from 0.
pub(super) fn delta_data(max_records: usize) -> impl Strategy<Value = Vec<Value>> {
    vec(rows::row(&relation()), 10..max_records).prop_map(|rows| {
        rows.into_iter()
            .enumerate()
            .map(|(index, mut row)| {
                row.insert("bigint".to_string(), Value::from(index as i64));
                Value::Object(row)
            })
            .collect()
    })
}

/// Whether the tests run as root, which ignores the directory permissions that the read failure
/// tests remove.
#[cfg(unix)]
fn running_as_root() -> bool {
    // SAFETY: `geteuid` cannot fail and has no side effects.
    unsafe { libc::geteuid() == 0 }
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

/// Append `record` as one new version of the table at `table_root`, with read permission on its
/// `_delta_log` directory removed first, and push the directory's original mode onto `saved` for
/// [`restore_delta_input_table_read_permission`]. Commits can still be read by name, but listing
/// the log fails. `table` is not updated.
///
/// The delta-rs writer lists the log, so the commit is written in a copy of the table and then
/// moved into place.
#[cfg(unix)]
async fn append_table_version_with_listing_blocked(
    table: &DeltaTable,
    table_root: &Path,
    relation: &Relation,
    record: &Value,
    saved: &mut Vec<(PathBuf, u32)>,
) {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    // Write the new version in a copy of the table.
    let copy_dir = TempDir::new().unwrap();
    copy_dir_recursive(table_root, copy_dir.path()).unwrap();
    let copy =
        DeltaTableBuilder::from_url(ensure_table_uri(copy_dir.path().to_str().unwrap()).unwrap())
            .unwrap()
            .load()
            .await
            .unwrap();
    let copy = append_table_version(copy, relation, record).await;
    let version = copy.version().unwrap();
    assert_eq!(version, table.version().unwrap() + 1);

    // Copy the new data files, then block listing and move the commit into place.
    copy_new_files(copy_dir.path(), table_root).unwrap();
    let log_dir = table_root.join("_delta_log");
    let mode = fs::metadata(&log_dir).unwrap().permissions().mode();
    saved.push((log_dir.clone(), mode));
    fs::set_permissions(&log_dir, fs::Permissions::from_mode(mode & !0o444)).unwrap();
    let commit = format!("{version:020}.json");
    let staged = table_root.join(format!("{commit}.tmp"));
    fs::copy(copy_dir.path().join("_delta_log").join(&commit), &staged).unwrap();
    fs::rename(&staged, log_dir.join(&commit)).unwrap();
}

fn copy_dir_recursive(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// Copy the files under `from`, outside `_delta_log`, that do not exist under `to`.
fn copy_new_files(from: &Path, to: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            if entry.file_name() != "_delta_log" {
                std::fs::create_dir_all(&target)?;
                copy_new_files(&entry.path(), &target)?;
            }
        } else if !target.exists() {
            std::fs::copy(entry.path(), target)?;
        }
    }
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
    engine: &dyn PipelineTestEngine,
    snapshot: bool,
    transaction_mode: DeltaTableTransactionMode,
    suspend: bool,
    end_version: bool,
) {
    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data: Vec<Value> = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = relation();

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
        engine,
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

#[engine_test(requires(Storage))]
pub fn cdc_file_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();

        test_cdc(
            engine,
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
    })
}

/// A connector configured with `soft_delete` ingests every CDC event as a row,
/// reporting the polarity of the event in the `is_delete` column.
///
/// The Delta Lake connector evaluates the delete filter per row, so one batch
/// can mix insertions and deletions; this covers that path end to end.
#[engine_test(requires(Storage))]
pub fn cdc_soft_delete_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
        };
        use deltalake::kernel::{DataType as KernelDataType, PrimitiveType, StructField};

        init_logging();

        // The Delta table carries the CDC bookkeeping columns.  The SQL table
        // declares `is_delete` instead, which the connector fills in from metadata.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("s", ArrowDataType::Utf8, false),
            ArrowField::new("__feldera_op", ArrowDataType::Utf8, false),
            ArrowField::new("__feldera_ts", ArrowDataType::Int64, false),
        ]));
        let struct_fields = vec![
            StructField::new("id", KernelDataType::Primitive(PrimitiveType::Long), false),
            StructField::new("s", KernelDataType::Primitive(PrimitiveType::String), false),
            StructField::new(
                "__feldera_op",
                KernelDataType::Primitive(PrimitiveType::String),
                false,
            ),
            StructField::new(
                "__feldera_ts",
                KernelDataType::Primitive(PrimitiveType::Long),
                false,
            ),
        ];

        // One batch with both polarities: rows 1 and 3 inserted, row 2 deleted.
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
                Arc::new(StringArray::from(vec!["one", "two", "three"])),
                Arc::new(StringArray::from(vec!["i", "d", "i"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let delta = create_table(&table_uri, &HashMap::new(), &struct_fields).await;

        let storage_dir = TempDir::new().unwrap();
        let pipeline = {
            let table_uri = table_uri.clone();
            let storage_dir = storage_dir.path().to_path_buf();
            off_runtime(move || {
                let pipeline_config: PipelineConfig = serde_json::from_value(json!({
                    "name": "test",
                    "workers": 4,
                    "storage_config": { "path": storage_dir },
                    "inputs": {
                        "test_input1": {
                            "stream": "test_input1",
                            "soft_delete": true,
                            "transport": {
                                "name": "delta_table_input",
                                "config": {
                                    "uri": table_uri,
                                    "mode": "cdc",
                                    "cdc_delete_filter": "__feldera_op = 'd'",
                                    "cdc_order_by": "__feldera_ts",
                                }
                            }
                        }
                    }
                }))
                .unwrap();
                engine
                    .controller(
                        &identity(&soft_delete_relation(), &[Some("output")])
                            .with_attribute_defaults(
                                relations::soft_delete_options().attribute_defaults,
                            ),
                        &pipeline_config,
                        Box::new(move |e, _| panic!("cdc soft delete test: {e}")),
                    )
                    .unwrap()
            })
        };
        pipeline.start();

        delta
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        wait_for_records_materialized(
            &pipeline,
            &SqlIdentifier::from("test_output1"),
            &soft_delete_relation(),
            &[
                json!({ "id": 1, "s": "one", "is_delete": null }),
                json!({ "id": 2, "s": "two", "is_delete": true }),
                json!({ "id": 3, "s": "three", "is_delete": null }),
            ],
        )
        .await;

        pipeline.stop().unwrap();
    })
}

/// CDC mode must honor `skip_unused_columns`: the connector should not read the
/// `unused` column (nullable, marked unused in the SQL schema), even though the
/// Delta table stores a non-null value for it. Regression test for issue #6113.
#[engine_test(requires(Storage))]
pub fn cdc_skip_unused_columns_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();

        test_cdc(
            engine,
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
    })
}

/// Same as [`cdc_skip_unused_columns_test`] but in `Catchup`
/// transaction mode, so projection is exercised across the catchup
/// transaction-batching path as well. Regression test for issue #6113.
#[engine_test(requires(Storage))]
pub fn cdc_skip_unused_columns_catchup_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();

        test_cdc(
            engine,
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
    })
}

/// Same as [`cdc_skip_unused_columns_catchup_test`] but with
/// suspend/resume, so projection is exercised across the catchup re-read from a
/// resumed version. Regression test for issue #6113.
#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn cdc_skip_unused_columns_catchup_suspend_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();

        test_cdc(
            engine,
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
    })
}

#[engine_test(requires(Storage))]
pub fn cdc_file_catchup_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();

        test_cdc(
            engine,
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
    })
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn cdc_file_catchup_suspend_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();
        let relation_schema = relation();
        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();
        test_cdc(
            engine,
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
    })
}

#[engine_test(requires(Storage))]
pub fn cdc_file_indexed_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();

        test_cdc(
            engine,
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
    })
}

#[engine_test(
    ignore_if(target_arch = "aarch64", "flaky on aarch64"),
    requires(Storage, Checkpoints, SuspendResume)
)]
pub fn cdc_file_suspend_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_table_dir = TempDir::new().unwrap();
        let input_table_uri = input_table_dir.path().display().to_string();

        test_cdc(
            engine,
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
    })
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
#[engine_test(requires(Storage))]
pub fn cdc_rewrite_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
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

        let row = |id: u32, label: &str| json!({ "id": id, "b": false, "i": null, "s": label.to_string() });

        // Two batches => two parquet files, so OPTIMIZE has work to do.
        // Filter `id % 2 = 0` keeps half of them.
        let batch1: Vec<Value> = (0..6).map(|id| row(id, &format!("row-{id}"))).collect();
        let batch2: Vec<Value> = (6..12).map(|id| row(id, &format!("row-{id}"))).collect();
        let all: Vec<Value> = batch1.iter().chain(batch2.iter()).cloned().collect();
        let expected_initial: Vec<Value> = all.iter().filter(|r| id(r) % 2 == 0).cloned().collect();

        // The columns of `basic_relation` and the CDC bookkeeping columns the connector expects.
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

        let make_batch = |rows: &[Value], op: &str, ts: i64| -> RecordBatch {
            let n = rows.len();
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(id))) as Arc<dyn Array>,
                    Arc::new(
                        rows.iter()
                            .map(|r| r["b"].as_bool())
                            .collect::<BooleanArray>(),
                    ),
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|r| r["s"].as_str().unwrap().to_string()),
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
            off_runtime(move || {
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
                engine
                    .controller(
                        &identity(&basic_relation(), &[Some("output")]),
                        &pipeline_config,
                        Box::new(move |e, _| panic!("cdc rewrite test: {e}")),
                    )
                    .unwrap()
            })
        };
        read_pipeline.start();
        let output = SqlIdentifier::from("test_output1");

        // Step 1: two appends => two parquet files, one Delta version each.
        delta = append(delta, vec![make_batch(&batch1, "i", 1_000)]).await;
        delta = append(delta, vec![make_batch(&batch2, "i", 2_000)]).await;
        wait_for_records_materialized(
            &read_pipeline,
            &output,
            &basic_relation(),
            &expected_initial,
        )
        .await;

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
        wait_for_records_materialized(&read_pipeline, &output, &basic_relation(), &expected).await;

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
        wait_for_records_materialized(&read_pipeline, &output, &basic_relation(), &expected).await;

        // Step 4: real change. CDC tables encode an in-place row update as a
        // `'d'` row (old value) followed by an `'i'` row (new value), ordered
        // by `cdc_order_by`. The connector should turn that into delete+insert.
        let target_id: i64 = 0;
        let original = all.iter().find(|r| id(r) == target_id).unwrap().clone();
        let mut modified = original.clone();
        modified["b"] = Value::from(true);
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
            .find(|r| id(r) == target_id)
            .unwrap()["b"] = Value::from(true);
        wait_for_records_materialized(&read_pipeline, &output, &basic_relation(), &expected_after)
            .await;

        read_pipeline.stop().unwrap();
    })
}

/// A multi-key `cdc_order_by`: "__feldera_ts asc, lsn asc`
///
/// One transaction deletes the old value and inserts a new one for the same id,
/// and the connector applies both, leaving the updated row.
#[engine_test(requires(Storage))]
pub fn cdc_multi_key_order_by_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        use arrow::array::{
            Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
        };
        use arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
        };
        use deltalake::kernel::{DataType as KernelDataType, PrimitiveType, StructField};

        init_logging();

        let expect = |id: u32, label: &str| json!({ "id": id, "b": false, "i": null, "s": label.to_string() });

        // The columns of `basic_relation` and the CDC bookkeeping columns. `lsn` is the order
        // tiebreaker; it is not a column of `basic_relation`, so it never reaches the table.
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
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0)))
                        as Arc<dyn Array>,
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
            off_runtime(move || {
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
                engine
                    .controller(
                        &identity(&basic_relation(), &[Some("output")]),
                        &pipeline_config,
                        Box::new(move |e, _| panic!("cdc multi-key order_by test: {e}")),
                    )
                    .unwrap()
            })
        };
        read_pipeline.start();
        let output = SqlIdentifier::from("test_output1");

        // Seed id 0 (kept by `id % 2 = 0`).
        delta = append(delta, make_batch(&[(0, "i", 1_000, 0, "orig")])).await;
        wait_for_records_materialized(
            &read_pipeline,
            &output,
            &basic_relation(),
            &[expect(0, "orig")],
        )
        .await;

        // One transaction updates id 0: delete the old value, insert the new one.
        // The multi-key `cdc_order_by` must parse for this transaction to run.
        delta = append(
            delta,
            make_batch(&[(0, "d", 2_000, 1, "orig"), (0, "i", 2_000, 2, "updated")]),
        )
        .await;
        wait_for_records_materialized(
            &read_pipeline,
            &output,
            &basic_relation(),
            &[expect(0, "updated")],
        )
        .await;

        let _ = delta;
        read_pipeline.stop().unwrap();
    })
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

/// Runs the real CDC `EXCEPT ALL` over a `Map` column; the rewritten row must cancel.
#[tokio::test]
async fn except_all_cancels_a_rewrite_on_a_map_column() {
    use crate::integrated::delta_table::input::build_cdc_dataframe;
    use arrow::array::{Array, ArrayRef, Int64Array, MapBuilder, RecordBatch, StringBuilder};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::datasource::MemTable;

    // The schema must repeat the builder's entry field names exactly.
    let map_column = |keys: &[&str]| -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for key in keys {
            builder.keys().append_value(*key);
            builder.values().append_value(format!("v-{key}"));
            builder.append(true).unwrap();
        }
        Arc::new(builder.finish())
    };

    let probe = map_column(&["a"]);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("m", probe.data_type().clone(), false),
    ]));

    let batch = |ids: &[i64], keys: &[&str]| {
        vec![vec![
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
                    map_column(keys),
                ],
            )
            .unwrap(),
        ]]
    };

    let ctx = SessionContext::new();
    // Row 1 is rewritten unchanged, so its Add and Remove must cancel.
    ctx.register_table(
        "cdc_adds",
        Arc::new(MemTable::try_new(arrow_schema.clone(), batch(&[1, 2], &["a", "b"])).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        "cdc_removes",
        Arc::new(MemTable::try_new(arrow_schema.clone(), batch(&[1], &["a"])).unwrap()),
    )
    .unwrap();

    let rows = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        Some(ctx.table("cdc_removes").await.unwrap()),
        None,
        "id",
        "unit-test",
    )
    .unwrap()
    .collect()
    .await
    .unwrap();

    let ids: Vec<i64> = rows
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(
        ids,
        vec![2],
        "the unchanged row must cancel against its Remove"
    );
}

/// CDC `EXCEPT ALL` is an anti join on every column, so a rewritten row must cancel
/// whatever its column types.
#[engine_test]
pub fn except_all_cancels_a_rewrite_across_all_types(_engine: &dyn PipelineTestEngine) {
    run_async(async {
        use crate::integrated::delta_table::input::build_cdc_dataframe;
        use datafusion::datasource::MemTable;

        let schema = delta::table_schema(&relation());
        let table = |rows: Vec<Value>| {
            let batch =
                delta::table_batch(&relation(), updates(&relation(), &rows).rows()).unwrap();
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap())
        };

        let rows: Vec<Value> = (0..3).map(delta_test_record).collect();
        let ctx = SessionContext::new();
        ctx.register_table("cdc_adds", table(rows.clone())).unwrap();
        ctx.register_table("cdc_removes", table(rows[..2].to_vec()))
            .unwrap();

        let out = build_cdc_dataframe(
            ctx.table("cdc_adds").await.unwrap(),
            Some(ctx.table("cdc_removes").await.unwrap()),
            None,
            "bigint",
            "unit-test",
        )
        .unwrap()
        .collect()
        .await
        .unwrap();

        let bigints: Vec<i64> = out
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("bigint")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            bigints,
            vec![2],
            "rows 0 and 1 must cancel against their Removes"
        );
    })
}

/// Value of `name`, or a panic naming the variable that has to be set.
///
/// `env::var(..).unwrap()` reports only `NotPresent`, which does not say what
/// to set. Mirrors `required_env` in `python/tests/utils.py`.
#[cfg(feature = "delta-s3-test")]
fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set to run the delta-s3-test tests"))
}

/// Object-store options for the CI buckets.
///
/// Empty: the buckets are GCS, and both the test process and the pipeline
/// authenticate with Workload Identity in CI and application-default
/// credentials locally, so no key ever reaches a connector config. Same
/// reasoning as `delta_storage_options` in `python/tests/utils.py`.
#[cfg(feature = "delta-s3-test")]
fn ci_object_store_config() -> Vec<(String, String)> {
    Vec::new()
}

/// Store for per-run test data, as `gs://<bucket>[/<prefix>]`. Everything in it
/// is deleted after 7 days.
#[cfg(feature = "delta-s3-test")]
fn ci_store_uri() -> String {
    required_env("CI_OBJECT_STORE_URI")
        .trim_end_matches('/')
        .to_string()
}

/// Store for read-only input data, which has no expiry rule. Fixtures that
/// cannot be regenerated from this repo have to live here: the per-run store
/// deletes them a week after upload.
#[cfg(feature = "delta-s3-test")]
fn ci_inputs_store_uri() -> String {
    required_env("CI_OBJECT_STORE_INPUTS_URI")
        .trim_end_matches('/')
        .to_string()
}

/// Location for a table the test writes. No test removes the table it creates;
/// the bucket's lifecycle rule deletes objects after 7 days instead.
#[cfg(feature = "delta-s3-test")]
fn ci_scratch_table_uri(uuid: uuid::Uuid) -> String {
    format!("{}/rust-delta-tests/{uuid}/", ci_store_uri())
}

#[cfg(feature = "delta-s3-test")]
#[engine_test(parallel(delta_s3), requires(Storage))]
pub fn cdc_s3_test_suspend(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        crate::integrated::delta_table::register_storage_handlers();

        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_uuid = uuid::Uuid::new_v4();

        let object_store_config = ci_object_store_config()
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect::<HashMap<String, Value>>();

        let input_table_uri = ci_scratch_table_uri(input_uuid);

        test_cdc(
            engine,
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
    })
}

#[cfg(feature = "delta-s3-test")]
async fn follow_s3_test_common(engine: &dyn PipelineTestEngine, snapshot: bool, suspend: bool) {
    crate::integrated::delta_table::register_storage_handlers();

    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = relation();

    let input_uuid = uuid::Uuid::new_v4();
    let output_uuid = uuid::Uuid::new_v4();

    let object_store_config = ci_object_store_config()
        .into_iter()
        .collect::<HashMap<_, _>>();

    let input_table_uri = ci_scratch_table_uri(input_uuid);
    let output_table_uri = ci_scratch_table_uri(output_uuid);

    test_follow(
        engine,
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
#[engine_test(parallel(delta_s3), requires(Storage))]
pub fn follow_s3_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        follow_s3_test_common(engine, false, false).await
    })
}

#[cfg(feature = "delta-s3-test")]
#[engine_test(parallel(delta_s3), requires(Storage))]
pub fn snapshot_and_follow_s3_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        follow_s3_test_common(engine, true, false).await
    })
}

#[cfg(feature = "delta-s3-test")]
#[engine_test(parallel(delta_s3), requires(Storage, Checkpoints, SuspendResume))]
pub fn snapshot_and_follow_s3_test_suspend(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async { follow_s3_test_common(engine, true, true).await })
}

/// Write to a Delta table in S3.
#[cfg(feature = "delta-s3-test")]
#[engine_test(parallel(delta_s3))]
pub fn s3_output_proptest(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(1), |(data in delta_data(20_000))| {
    let uuid = uuid::Uuid::new_v4();
    let object_store_config = ci_object_store_config().into_iter().collect::<HashMap<_,_>>();

    let table_uri = ci_scratch_table_uri(uuid);
    // TODO: enable verification when it's supported for S3.
    delta_table_output_test(engine, data.clone(), &table_uri, &object_store_config, false, None, false);
    //delta_table_output_test(engine, data.clone(), &table_uri, &object_store_config, false, None);

    let json_file = delta_table_snapshot_to_json(engine,
        &table_uri,
        &relation(),
        &object_store_config);

    let expected_zset = rows_zset(&relation(), &data);
    let zset = file_zset(&relation(), &json_file);
    assert_eq!(zset, expected_zset);
    });
}

/// Read a large (2M records) dataset from the CI object store.
///
/// Copied there from the Databricks workspace bucket it was created in, so CI
/// can read it without credentials of its own. It sits in the inputs store
/// rather than the per-run one because nothing in this repo can rebuild it --
/// it came from a Databricks notebook (below) -- and the per-run store deletes
/// everything after 7 days.
///
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
#[engine_test(parallel(delta_s3))]
pub fn s3_people_2m(engine: &dyn PipelineTestEngine) {
    let object_store_config = ci_object_store_config()
        .into_iter()
        // Set long timeout for reading large files.
        .chain([("timeout".to_string(), "1000 secs".to_string())])
        .collect::<HashMap<_, _>>();

    let table_uri = format!("{}/delta-connector-tests/people_2m/", ci_inputs_store_uri());
    let table_uri = table_uri.as_str();
    let json_file =
        delta_table_snapshot_to_json(engine, table_uri, &people_relation(), &object_store_config);

    println!("reading output file");
    let zset = file_zset(&people_relation(), &json_file);

    assert_eq!(zset.len(), 2_000_000);

    forget(json_file);
}

#[engine_test(requires(Storage))]
pub fn snapshot_and_follow_file_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::None,
            false,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage))]
pub fn transactional_snapshot_and_follow_file_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::Snapshot,
            false,
            false,
        )
        .await
    })
}

#[engine_test(requires(Storage))]
pub fn transactional_always_snapshot_and_follow_file_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        delta_table_follow_file_test_common(
            engine,
            true,
            DeltaTableTransactionMode::Always,
            false,
            false,
        )
        .await
    })
}

/// With the input endpoint paused between rounds, verify catchup batches each round's Delta
/// commits into a single Feldera transaction.
#[engine_test(requires(Storage))]
pub fn catchup_batches_multiple_versions_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let result = run_catchup_lag_experiment(
            engine,
            catchup_lag_options(DeltaTableTransactionMode::Catchup, &[3, 2], None, None),
        )
        .await;
        assert_eq!(
            result.follow_transactions_per_round,
            vec![1, 1],
            "catchup must use one Feldera transaction per pause/write/resume round"
        );
    })
}

/// A pause that lands while the connector lists the table to choose a catchup target must not
/// latch a target from before the pause: the commits written during the pause belong to the
/// same window as the one that started the listing.
#[cfg(unix)]
#[engine_test(requires(Storage))]
pub fn catchup_pause_during_target_listing_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        if running_as_root() {
            eprintln!("skipping: root ignores directory permissions");
            return;
        }
        let mut options =
            catchup_lag_options(DeltaTableTransactionMode::Catchup, &[2, 3], None, Some(4));
        options.pause_during_listing_round = Some(1);
        let result = run_catchup_lag_experiment(engine, options).await;
        assert_eq!(
            result.follow_transactions_per_round,
            vec![1, 1],
            "a pause during the catchup target listing must not split the round into two windows"
        );
    })
}

/// Control for [`catchup_batches_multiple_versions_test`]: `always` mode commits once
/// per Delta log version in each round.
#[engine_test(requires(Storage))]
pub fn always_one_transaction_per_version_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let result = run_catchup_lag_experiment(
            engine,
            catchup_lag_options(DeltaTableTransactionMode::Always, &[2, 2], None, None),
        )
        .await;
        assert_eq!(
            result.follow_transactions_per_round,
            vec![2, 2],
            "always mode must commit each Delta version in its own Feldera transaction"
        );
    })
}

/// `end_version` caps the catchup window target at startup of the window.
#[engine_test(requires(Storage))]
pub fn catchup_end_version_caps_window_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        const NUM_VERSIONS: i64 = 8;
        const END_VERSION: i64 = 5;
        let result = run_catchup_lag_experiment(
            engine,
            catchup_lag_options(
                DeltaTableTransactionMode::Catchup,
                &[NUM_VERSIONS],
                Some(END_VERSION),
                None,
            ),
        )
        .await;
        assert_eq!(
            result.follow_transactions_per_round,
            vec![1],
            "catchup with end_version must still batch versions 1..=end_version in one transaction"
        );
    })
}

/// Large bursts across two rounds: each round still uses one Feldera transaction.
#[engine_test(requires(Storage))]
pub fn catchup_rapid_versions_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let result = run_catchup_lag_experiment(
            engine,
            catchup_lag_options(DeltaTableTransactionMode::Catchup, &[6, 6], None, None),
        )
        .await;
        assert_eq!(
            result.follow_transactions_per_round,
            vec![1, 1],
            "catchup must batch a large pre-existing commit backlog in one transaction per round"
        );
    })
}

/// Catchup survives a transient unreadable table (chmod on input root): unhealthy, then healthy,
/// one Feldera transaction per catch-up round.
#[cfg(unix)]
#[engine_test(requires(Storage))]
pub fn catchup_transient_read_failure_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        if running_as_root() {
            eprintln!("skipping: root ignores directory permissions");
            return;
        }
        let result = run_catchup_lag_experiment(
            engine,
            CatchupLagExperimentOptions {
                transaction_mode: DeltaTableTransactionMode::Catchup,
                versions_per_round: &[2, 2],
                end_version: None,
                snapshot_rows: None,
                max_retries: None,
                inject_read_failure_before_resume_round: Some(0),
                clear_read_failure_after_unhealthy: true,
                expect_ingest_failure: false,
                pause_during_listing_round: None,
            },
        )
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
    })
}

/// An ordered snapshot suspended between slices resumes from its checkpoint and
/// reads the rest of the table.
#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn ordered_snapshot_resume_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        run_ordered_snapshot_resume_test(engine, false).await;
    })
}

/// The same from a checkpoint that holds the resume point as a SQL expression,
/// as older connectors wrote it.
#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn ordered_snapshot_resume_legacy_checkpoint_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        run_ordered_snapshot_resume_test(engine, true).await;
    })
}

/// A `datetime` read must land on the version current at that time.
///
/// delta-rs resolves `datetime` from each commit file's modification time, so
/// the test sets those rather than racing the clock.
#[engine_test]
pub fn snapshot_datetime_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        use std::fs::{File, FileTimes};
        use std::time::{Duration, SystemTime};

        const FIRST_COMMIT_SECS: u64 = 1_700_000_000;
        const COMMIT_INTERVAL_SECS: u64 = 100;
        // Halfway between the two appends, so neither boundary is a tie.
        const READ_AT_SECS: u64 = FIRST_COMMIT_SECS + COMMIT_INTERVAL_SECS * 3 / 2;

        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let arrow_schema = Arc::new(ArrowSchema::new(relation_to_arrow_fields(
            &basic_relation().fields,
            delta_schema_options(),
        )));
        let struct_fields: Vec<_> = arrow_schema
            .fields
            .iter()
            .map(|f| {
                StructField::new(
                    f.name(),
                    DataType::try_from_arrow(f.data_type()).unwrap(),
                    f.is_nullable(),
                )
            })
            .collect();

        let before: Vec<Value> = (0..3).map(|id| change_feed_row(id, "")).collect();
        let after: Vec<Value> = (3..6).map(|id| change_feed_row(id, "")).collect();

        let table = create_table(&table_uri, &HashMap::new(), &struct_fields).await;
        let table = write_data_to_table(table, &basic_relation(), &before).await;
        let table = write_data_to_table(table, &basic_relation(), &after).await;
        assert_eq!(table.version(), Some(2), "expected create + two appends");

        let log_dir = table_dir.path().join("_delta_log");
        for version in 0..=2u64 {
            let times = FileTimes::new().set_modified(
                SystemTime::UNIX_EPOCH
                    + Duration::from_secs(FIRST_COMMIT_SECS + version * COMMIT_INTERVAL_SECS),
            );
            File::options()
                .write(true)
                .open(log_dir.join(format!("{version:020}.json")))
                .unwrap()
                .set_times(times)
                .unwrap();
        }

        // The first append is visible at that point, the second is not.
        let read_at = DateTime::from_timestamp(READ_AT_SECS as i64, 0)
            .unwrap()
            .to_rfc3339();
        let read = off_runtime(move || {
            let json_file = delta_table_snapshot_to_json(
                engine,
                &table_uri,
                &basic_relation(),
                &HashMap::from([("datetime".to_string(), read_at)]),
            );
            file_zset(&basic_relation(), &json_file)
        });

        let expected = rows_zset(&basic_relation(), &before);
        assert_eq!(read, expected, "`datetime` must pin the table to version 1");
    })
}

/// Calls `f` on every `snapshot_timestamp` in the checkpoints under
/// `storage_dir`, saving what it changes, and returns how many it found.
fn visit_checkpointed_snapshot_timestamps(
    storage_dir: &Path,
    f: &mut dyn FnMut(&mut String),
) -> usize {
    fn visit(value: &mut Value, f: &mut dyn FnMut(&mut String)) -> usize {
        match value {
            Value::Object(map) => map
                .iter_mut()
                .map(|(key, value)| match value {
                    Value::String(ts) if key == "snapshot_timestamp" => {
                        f(ts);
                        1
                    }
                    _ => visit(value, f),
                })
                .sum(),
            Value::Array(values) => values.iter_mut().map(|value| visit(value, f)).sum(),
            _ => 0,
        }
    }

    let mut found = 0;
    let mut dirs = vec![storage_dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            // DataFusion deletes its scratch dirs after the pipeline stops.
            if path.is_dir() && path.file_name() != Some(OsStr::new(DATAFUSION_TEMP_DIR)) {
                dirs.push(path);
            } else if path.file_name() == Some(OsStr::new("state.json")) {
                let mut state: Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                found += visit(&mut state, f);
                std::fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();
            }
        }
    }
    found
}

/// Suspends an ordered snapshot after its first slice and resumes it, wrapping
/// the checkpointed resume point first if `legacy_checkpoint`.
async fn run_ordered_snapshot_resume_test(
    engine: &dyn PipelineTestEngine,
    legacy_checkpoint: bool,
) {
    init_logging();

    // One row per day and a 10-day lateness give 60 slices.
    const ROWS: i64 = 600;
    const DAY_MS: i64 = 86_400_000;
    const JAN_1_2024_MS: i64 = 1_704_067_200_000;

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let storage_dir = TempDir::new().unwrap();

    let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
        &relation().fields,
        delta_schema_options(),
    ));
    let data: Vec<Value> = (0..ROWS)
        .map(|i| {
            let mut record = delta_test_record(i);
            record["timestamp_ntz"] = timestamp_value(JAN_1_2024_MS + i * DAY_MS);
            record
        })
        .collect();
    let table = create_table_from_arrow(&table_uri, &arrow_schema, &[]).await;
    write_data_to_table(table, &relation(), &data).await;

    // One transaction per slice, so a checkpoint lands on a slice boundary.
    let config = json!({
        "mode": "snapshot",
        "timestamp_column": "timestamp_ntz",
        "transaction_mode": "snapshot",
    });
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let start = async || {
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            config.clone(),
            &lateness_relation(),
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();
        pipeline
    };
    let snapshot_records = |pipeline: &Controller| {
        delta_connector_counter(pipeline, "input_connector_delta_snapshot_records_total")
    };

    // Wait until the circuit has taken in a slice, not just the reader.
    let pipeline = start().await;
    wait(
        || {
            completed_frontier_metadata(&pipeline)
                .is_some_and(|metadata| metadata["snapshot_timestamp"].is_string())
        },
        20_000,
    )
    .expect("timeout waiting for the first snapshot slice");
    suspend_pipeline(pipeline).await;
    let found = visit_checkpointed_snapshot_timestamps(storage_dir.path(), &mut |ts| {
        assert!(
            !ts.starts_with("timestamp '"),
            "resume point stored as SQL: {ts}"
        );
        if legacy_checkpoint {
            *ts = format!("timestamp '{ts}'");
        }
    });
    assert!(found > 0, "the checkpoint must be taken mid-snapshot");

    let pipeline = start().await;
    let output = SqlIdentifier::from("test_output1");
    wait_or_connector_error(&pipeline, &output, &lateness_relation(), &data, &errors).await;

    let resumed = snapshot_records(&pipeline);
    assert!(
        resumed > 0 && resumed < ROWS as u64,
        "the resumed run must read only the rows after the checkpoint, read {resumed} of {ROWS}"
    );
    pipeline.stop().unwrap();
}

/// A `filter` is a `where` clause over the Delta table, so it may name a column
/// the SQL table does not declare; follow mode used to project those away first.
async fn run_follow_filter_undeclared_column_test(
    engine: &dyn PipelineTestEngine,
    filter_expr: &str,
) {
    use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
        Schema as ArrowSchema,
    };
    use deltalake::kernel::{DataType as KernelDataType, PrimitiveType, StructField};

    init_logging();

    // `region` and `meta` exist only in the Delta table. `meta.tag` mirrors
    // `region`, so both filter forms select the same rows.
    let meta_fields: ArrowFields =
        vec![Arc::new(ArrowField::new("tag", ArrowDataType::Utf8, false))].into();
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("region", ArrowDataType::Utf8, false),
        ArrowField::new("meta", ArrowDataType::Struct(meta_fields.clone()), false),
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
            "region",
            KernelDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "meta",
            KernelDataType::try_from_arrow(&ArrowDataType::Struct(meta_fields.clone())).unwrap(),
            false,
        ),
    ];

    // Even ids are in region 'us' (kept), odd ids in 'eu' (filtered out).
    let region = |id: u32| if id.is_multiple_of(2) { "us" } else { "eu" };
    let row = |id: u32| json!({ "id": id, "b": false, "i": null, "s": format!("row-{id}") });
    let make_batch = |ids: &[u32]| -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    ids.iter().map(|id| *id as i64),
                )) as Arc<dyn Array>,
                Arc::new(ids.iter().map(|_| Some(false)).collect::<BooleanArray>()),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| format!("row-{id}")),
                )),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| region(*id)),
                )),
                Arc::new(StructArray::new(
                    meta_fields.clone(),
                    vec![Arc::new(StringArray::from_iter_values(
                        ids.iter().map(|id| region(*id)),
                    ))],
                    None,
                )),
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
    // The error callback runs on a worker thread, where a panic is swallowed:
    // record the error so the wait below can fail on it instead of timing out.
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let read_pipeline = {
        let errors = errors.clone();
        let table_uri = table_uri.clone();
        let storage_dir = storage_dir.path().to_path_buf();
        let filter_expr = filter_expr.to_string();
        off_runtime(move || {
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
                                "mode": "follow",
                                "filter": filter_expr,
                                "skip_unused_columns": true,
                            }
                        }
                    }
                }
            }))
            .unwrap();
            engine
                .controller(
                    &identity(&basic_relation(), &[Some("output")]),
                    &pipeline_config,
                    Box::new(move |e, _| errors.lock().unwrap().push(e.to_string())),
                )
                .unwrap()
        })
    };
    read_pipeline.start();
    let output = SqlIdentifier::from("test_output1");

    let batch1: Vec<u32> = (0..6).collect();
    delta = append(delta, make_batch(&batch1)).await;
    let mut expected: Vec<Value> = batch1
        .iter()
        .filter(|id| region(**id) == "us")
        .map(|id| row(*id))
        .collect();
    wait_or_connector_error(
        &read_pipeline,
        &output,
        &basic_relation(),
        &expected,
        &errors,
    )
    .await;

    // A second commit: the filter must still resolve past the first log entry.
    let batch2: Vec<u32> = (6..12).collect();
    let _ = append(delta, make_batch(&batch2)).await;
    expected.extend(
        batch2
            .iter()
            .filter(|id| region(**id) == "us")
            .map(|id| row(*id)),
    );
    wait_or_connector_error(
        &read_pipeline,
        &output,
        &basic_relation(),
        &expected,
        &errors,
    )
    .await;

    read_pipeline.stop().unwrap();
}

/// A Delta table with `arrow_schema`'s columns, partitioned by `partition_columns`.
async fn create_table_from_arrow(
    table_uri: &str,
    arrow_schema: &ArrowSchema,
    partition_columns: &[&str],
) -> DeltaTable {
    let struct_fields: Vec<StructField> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            StructField::new(
                f.name(),
                DataType::try_from_arrow(f.data_type()).unwrap(),
                f.is_nullable(),
            )
        })
        .collect();

    CreateBuilder::new()
        .with_location(table_uri)
        .with_save_mode(SaveMode::Ignore)
        .with_columns(struct_fields)
        .with_partition_columns(partition_columns.iter().copied())
        .await
        .unwrap()
}

/// Start a pipeline whose only input reads `table_uri` with `transport_config`, into a table
/// with the columns of `relation`.
///
/// The error callback runs on a worker thread, where a panic is swallowed, so
/// errors go to `errors` for [`wait_or_connector_error`] to report.
async fn delta_input_controller(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    mut transport_config: Value,
    relation: &Relation,
    storage_dir: &Path,
    errors: &Arc<Mutex<Vec<String>>>,
) -> Result<Controller, ControllerError> {
    transport_config["uri"] = table_uri.into();
    let pipeline_config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "delta_table_input",
                    "config": transport_config,
                }
            }
        }
    }))
    .unwrap();

    let errors = errors.clone();
    off_runtime(move || {
        engine.controller(
            &identity(relation, &[Some("output")]),
            &pipeline_config,
            Box::new(move |e, _| errors.lock().unwrap().push(e.to_string())),
        )
    })
}

/// Write `rows`, rows of `relation`, to a table partitioned by `partition_columns` and check
/// that a follow read gets every column back.
async fn run_follow_partition_test(
    engine: &dyn PipelineTestEngine,
    relation: &Relation,
    rows: &[Value],
    partition_columns: &[&str],
) {
    run_follow_partition_test_on(engine, relation, relation, rows, rows, partition_columns).await
}

/// [`run_follow_partition_test`] with the Delta table's columns named apart from
/// the SQL table's, for a test whose two sides spell them differently.
///
/// Hence the two relations: `written` names the columns of the Delta table, which is what
/// the writer needs, and `relation` those of the SQL table, which is what the ad hoc query
/// reading the output back needs. `written_rows` and `expected_rows` hold the same rows.
async fn run_follow_partition_test_on(
    engine: &dyn PipelineTestEngine,
    written: &Relation,
    relation: &Relation,
    written_rows: &[Value],
    expected_rows: &[Value],
    partition_columns: &[&str],
) {
    init_logging();

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let table =
        create_table_from_arrow(&table_uri, &delta::table_schema(written), partition_columns).await;

    let storage_dir = TempDir::new().unwrap();
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let read_pipeline = delta_input_controller(
        engine,
        &table_uri,
        json!({ "mode": "follow" }),
        relation,
        storage_dir.path(),
        &errors,
    )
    .await
    .unwrap();
    read_pipeline.start();

    write_data_to_table(table, written, written_rows).await;
    wait_or_connector_error(
        &read_pipeline,
        &SqlIdentifier::from("test_output1"),
        relation,
        expected_rows,
        &errors,
    )
    .await;

    read_pipeline.stop().unwrap();
}

/// CDC mode over a partitioned table: the adds side reads files that span two
/// partitions, so each file's value must come back with its own rows.
#[engine_test(requires(Storage))]
pub fn cdc_partition_column_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        use arrow::array::{
            Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};

        init_logging();

        let expect =
            |id: u32, s: &str| json!({ "id": id, "b": false, "i": null, "s": s.to_string() });

        // The CDC marker columns are not part of the SQL relation, so the table's
        // schema is built by hand rather than from `basic_relation()`.
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

        // Each tuple is (id, op, ts, s); `s` is the partition column.
        let make_batch = |rows: &[(i64, &str, i64, &str)]| -> RecordBatch {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0)))
                        as Arc<dyn Array>,
                    Arc::new(rows.iter().map(|_| Some(false)).collect::<BooleanArray>()),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                    Arc::new(TimestampMicrosecondArray::from_iter_values(
                        rows.iter().map(|r| r.2),
                    )),
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
        let mut delta = create_table_from_arrow(&table_uri, &arrow_schema, &["s"]).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let read_pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({
                "mode": "cdc",
                "cdc_delete_filter": "__feldera_op = 'd'",
                "cdc_order_by": "__feldera_ts asc",
            }),
            &basic_relation(),
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        read_pipeline.start();
        let output = SqlIdentifier::from("test_output1");

        // One commit, two partitions.
        delta = append(
            delta,
            make_batch(&[(0, "i", 1_000, "a"), (1, "i", 1_000, "b")]),
        )
        .await;
        wait_or_connector_error(
            &read_pipeline,
            &output,
            &basic_relation(),
            &[expect(0, "a"), expect(1, "b")],
            &errors,
        )
        .await;

        // Deleting id 0 must match the row ingested from partition "a".
        let _ = append(delta, make_batch(&[(0, "d", 2_000, "a")])).await;
        wait_or_connector_error(
            &read_pipeline,
            &output,
            &basic_relation(),
            &[expect(1, "b")],
            &errors,
        )
        .await;

        read_pipeline.stop().unwrap();
    })
}

/// A Delta table like [`create_table_from_arrow`], recording a change data feed.
async fn create_change_feed_table(
    table_uri: &str,
    arrow_schema: &ArrowSchema,
    partition_columns: &[&str],
) -> DeltaTable {
    let struct_fields: Vec<StructField> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            StructField::new(
                f.name(),
                DataType::try_from_arrow(f.data_type()).unwrap(),
                f.is_nullable(),
            )
        })
        .collect();

    CreateBuilder::new()
        .with_location(table_uri)
        .with_save_mode(SaveMode::Ignore)
        .with_columns(struct_fields)
        .with_partition_columns(partition_columns.iter().copied())
        .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
        .await
        .unwrap()
}

/// Rows of [`basic_relation`], for the change-feed tests.
fn change_feed_row(id: u32, s: &str) -> Value {
    json!({ "id": id, "b": false, "i": null, "s": s })
}

/// The Arrow schema of [`basic_relation`], as the Delta writer sees it.
fn change_feed_arrow_schema() -> Arc<ArrowSchema> {
    arrow_schema_of(&basic_relation().fields)
}

/// The Arrow schema of a SQL relation, as the Delta writer sees it.
fn arrow_schema_of(relation: &[Field]) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(relation_to_arrow_fields(
        relation,
        delta_schema_options(),
    )))
}

/// Start a pipeline whose input relation, [`basic_relation`], has a primary key, so the
/// connector feeds an upsert handle.
pub(super) async fn delta_input_controller_keyed(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    mut transport_config: Value,
    relation: &Relation,
    storage_dir: &Path,
    errors: &Arc<Mutex<Vec<String>>>,
) -> Controller {
    transport_config["uri"] = table_uri.into();
    let pipeline_config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "delta_table_input",
                    "config": transport_config,
                }
            }
        }
    }))
    .unwrap();

    let errors = errors.clone();
    off_runtime(move || {
        engine.controller(
            &TestCircuit::Keyed {
                key: basic_key_relation(),
                relation: relation.clone(),
                persistent_output_ids: vec![Some("output".to_string())],
                aliased: false,
            },
            &pipeline_config,
            Box::new(move |e, _| errors.lock().unwrap().push(e.to_string())),
        )
    })
    .unwrap()
}

/// An `UPDATE` that moves every row between partitions leaves each row present
/// once, at its new value, on a table with a primary key.
///
/// On a keyed relation a retraction is a delete *by key*, and `input_upsert`
/// applies same-key updates in the order they arrive, so an update's two images
/// do not commute: the retraction has to reach the circuit first.
///
/// `delta_unity::unity_change_feed` pins the same property against a live
/// Databricks table, where Spark rather than delta-rs writes the change data.
#[engine_test(requires(Storage))]
pub fn change_feed_keyed_update_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        // Partitioned by `s`, the column the update below moves.
        let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &["s"]).await;

        // Every row sits in its own partition, and the update below moves each into
        // a sibling that sorts before it. The reader groups change data files by
        // partition value in a `BTreeMap`, so every post-image group precedes its
        // pre-image group. One such row would be a coin flip (the groups become
        // parallel scan partitions, so their batches race), but forty of them lose
        // rows on any scheduling.
        const ROWS: u32 = 40;
        let before = |id: u32| format!("b{id:02}");
        let after = |id: u32| format!("a{id:02}");

        let initial: Vec<Value> = (0..ROWS)
            .map(|id| change_feed_row(id, &before(id)))
            .collect();
        delta = write_data_to_table(delta, &basic_relation(), &initial).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller_keyed(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto" }),
            &basic_relation(),
            storage_dir.path(),
            &errors,
        )
        .await;
        pipeline.start();

        let output = SqlIdentifier::from("test_output1");
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &initial, &errors).await;

        // One commit that moves every row into its earlier-sorting partition.
        let _ = delta
            .update()
            .with_update("s", concat(vec![lit("a"), substr(col("s"), lit(2i64))]))
            .await
            .unwrap();

        let expected: Vec<Value> = (0..ROWS)
            .map(|id| change_feed_row(id, &after(id)))
            .collect();
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

        assert_read_change_data(&pipeline);
        pipeline.stop().unwrap();
    })
}

/// Read a change feed under a `filter` that names columns the SQL table does
/// not declare, optionally with the filter's column as the partition key.
///
/// The change data path resolves a filter the same way follow mode does (read
/// every column, filter, then project to the SQL columns), with the
/// physical-to-logical rename, the partition literals and a union in between. An
/// unresolved column fails loudly ("No field named region"), so a clean run is
/// itself the resolution assertion; the rows then say the filter was applied and
/// not merely parsed.
async fn run_change_feed_filter_test(
    engine: &dyn PipelineTestEngine,
    filter_expr: &str,
    partition_by_region: bool,
) {
    use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields};

    init_logging();

    // `region` and `meta` exist only in the Delta table; `meta.tag` mirrors
    // `region`, so both filter forms select the same rows.
    let meta_fields: ArrowFields =
        vec![Arc::new(ArrowField::new("tag", ArrowDataType::Utf8, false))].into();
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("region", ArrowDataType::Utf8, false),
        ArrowField::new("meta", ArrowDataType::Struct(meta_fields.clone()), false),
    ]));

    // Even ids are in region 'us' (kept), odd ids in 'eu' (filtered out).
    let region = |id: u32| if id.is_multiple_of(2) { "us" } else { "eu" };
    let row = |id: u32, s: &str| json!({ "id": id, "b": false, "i": null, "s": s.to_string() });
    let make_batch = |ids: &[u32]| -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    ids.iter().map(|id| *id as i64),
                )) as Arc<dyn Array>,
                Arc::new(ids.iter().map(|_| Some(false)).collect::<BooleanArray>()),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| format!("row-{id}")),
                )),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| region(*id)),
                )),
                Arc::new(StructArray::new(
                    meta_fields.clone(),
                    vec![Arc::new(StringArray::from_iter_values(
                        ids.iter().map(|id| region(*id)),
                    ))],
                    None,
                )),
            ],
        )
        .unwrap()
    };

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let partitions: &[&str] = if partition_by_region {
        &["region"]
    } else {
        &[]
    };
    let mut delta = create_change_feed_table(&table_uri, &arrow_schema, partitions).await;

    let storage_dir = TempDir::new().unwrap();
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let pipeline = delta_input_controller(
        engine,
        &table_uri,
        json!({
            "mode": "snapshot_and_follow",
            "change_feed": "auto",
            "filter": filter_expr,
            "skip_unused_columns": true,
        }),
        &basic_relation(),
        storage_dir.path(),
        &errors,
    )
    .await
    .unwrap();
    pipeline.start();
    let output = SqlIdentifier::from("test_output1");

    // An append records no change data, so this half is the fallback path.
    let ids: Vec<u32> = (0..6).collect();
    delta = delta
        .write(vec![make_batch(&ids)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    let mut expected: Vec<Value> = ids
        .iter()
        .filter(|id| region(**id) == "us")
        .map(|id| row(*id, &format!("row-{id}")))
        .collect();
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

    // An UPDATE does record it, so this half is the change data path. Both
    // images of id 0 pass the filter: the pre-image is retracted, the
    // post-image inserted.
    delta = delta
        .update()
        .with_predicate(col("id").eq(lit(0i64)))
        .with_update("s", lit("updated"))
        .await
        .unwrap()
        .0;
    expected[0] = row(0, "updated");
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

    // id 1 is in the filtered-out region. Its change data must be dropped, not
    // merely resolved: a filter that parses but never applies leaks it in.
    delta = delta
        .update()
        .with_predicate(col("id").eq(lit(1i64)))
        .with_update("s", lit("leaked"))
        .await
        .unwrap()
        .0;
    // Nothing changes, so give the commit a chance to arrive wrongly before
    // asserting that it did not.
    let appended: Vec<u32> = (6..8).collect();
    let _ = delta
        .write(vec![make_batch(&appended)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    expected.extend(
        appended
            .iter()
            .filter(|id| region(**id) == "us")
            .map(|id| row(*id, &format!("row-{id}"))),
    );
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

    assert_read_change_data(&pipeline);
    pipeline.stop().unwrap();
}

/// A change feed read under a `filter` over a Delta column the SQL table never
/// declares admits the rows the predicate selects.
#[engine_test(requires(Storage))]
pub fn change_feed_filter_undeclared_column_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        run_change_feed_filter_test(engine, "region = 'us'", false).await;
    })
}

/// The same with the filter naming a field of an undeclared struct column, which
/// parses as a compound identifier, so the read set keeps `meta` rather than
/// `meta.tag`.
#[engine_test(requires(Storage))]
pub fn change_feed_filter_struct_field_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        run_change_feed_filter_test(engine, "meta.tag = 'us'", false).await;
    })
}

/// The same filter with its column as the partition key.
///
/// Delta keeps a partition value in the log rather than the file, so the
/// partition columns are supplied as literals before the filter runs.
#[engine_test(requires(Storage))]
pub fn change_feed_filter_partition_column_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        run_change_feed_filter_test(engine, "region = 'us'", true).await;
    })
}

/// A `MERGE` that updates one row, deletes another and inserts a third leaves the
/// table with each of those changes applied once.
///
/// One `MERGE` clause matches each row, so the commit records all four change
/// types (an update's two images, a delete, and an insert), which no other
/// operation does in a single commit.
///
/// `test_delta_input_change_feed_merge` in
/// `python/tests/platform/test_delta_input_change_feed.py` drives the same
/// commit shape written by Spark, and asserts how much of it was read.
#[engine_test(requires(Storage))]
pub fn change_feed_merge_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        use deltalake::datafusion::prelude::SessionContext;

        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;

        let initial: Vec<Value> = (0..4).map(|id| change_feed_row(id, "initial")).collect();
        delta = write_data_to_table(delta, &basic_relation(), &initial).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto" }),
            &basic_relation(),
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();

        let output = SqlIdentifier::from("test_output1");
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &initial, &errors).await;

        // Source rows: id 1 updates, id 2 deletes, id 9 inserts. Each `MERGE` clause
        // matches exactly one, so the commit records one of every change type.
        let source = SessionContext::new()
            .read_batch(
                delta::table_batch(
                    &basic_relation(),
                    updates(
                        &basic_relation(),
                        &[
                            change_feed_row(1, "merged"),
                            change_feed_row(2, "doomed"),
                            change_feed_row(9, "inserted"),
                        ],
                    )
                    .rows(),
                )
                .unwrap(),
            )
            .unwrap();

        let _ = delta
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|u| {
                u.predicate(col("source.s").eq(lit("merged")))
                    .update("s", col("source.s"))
            })
            .unwrap()
            .when_matched_delete(|d| d.predicate(col("source.s").eq(lit("doomed"))))
            .unwrap()
            .when_not_matched_insert(|i| {
                i.set("id", col("source.id"))
                    .set("b", col("source.b"))
                    .set("i", col("source.i"))
                    .set("s", col("source.s"))
            })
            .unwrap()
            .await
            .unwrap();

        // id 1 renamed, id 2 gone, id 9 added; ids 0 and 3 untouched.
        let expected = vec![
            change_feed_row(0, "initial"),
            change_feed_row(1, "merged"),
            change_feed_row(3, "initial"),
            change_feed_row(9, "inserted"),
        ];
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

        assert_eq!(
            delta_connector_counter(&pipeline, "input_connector_delta_commits_from_change_data"),
            1,
            "the merge must be read from the change data it recorded, not from the \
         files it rewrote"
        );
        pipeline.stop().unwrap();
    })
}

/// Records the connector has fed into the circuit so far.
fn delta_input_records(pipeline: &Controller) -> u64 {
    pipeline
        .status()
        .input_status()
        .values()
        .next()
        .unwrap()
        .metrics
        .total_records
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// Ingest a snapshot and one whole-file `UPDATE` under `change_feed`, and report
/// the resulting rows and the records it took to produce them.
async fn run_change_feed_option(
    engine: &dyn PipelineTestEngine,
    change_feed: &str,
    rows: u32,
) -> (Vec<Value>, u64, u64, u64) {
    init_logging();

    let arrow_schema = change_feed_arrow_schema();
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;

    let initial: Vec<Value> = (0..rows).map(|id| change_feed_row(id, "initial")).collect();
    delta = write_data_to_table(delta, &basic_relation(), &initial).await;

    let storage_dir = TempDir::new().unwrap();
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let pipeline = delta_input_controller(
        engine,
        &table_uri,
        json!({ "mode": "snapshot_and_follow", "change_feed": change_feed }),
        &basic_relation(),
        storage_dir.path(),
        &errors,
    )
    .await
    .unwrap();
    pipeline.start();

    let output = SqlIdentifier::from("test_output1");
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &initial, &errors).await;

    // One row of one file changes, so the file-action read rewrites the lot.
    let _ = delta
        .update()
        .with_predicate(col("id").eq(lit(0i64)))
        .with_update("s", lit("updated"))
        .await
        .unwrap();
    let mut expected = initial;
    expected[0]["s"] = Value::from("updated");
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

    let records = delta_input_records(&pipeline);
    let from_change_data =
        delta_connector_counter(&pipeline, "input_connector_delta_commits_from_change_data");
    let from_file_actions =
        delta_connector_counter(&pipeline, "input_connector_delta_commits_from_file_actions");
    pipeline.stop().unwrap();
    (expected, records, from_change_data, from_file_actions)
}

/// Ingest a burst of change feed commits under `transaction_mode`, and report
/// how many Feldera transactions the connector opened for it.
///
/// The connector is paused while the commits are written, so the burst is one
/// backlog when it resumes rather than a race between the writer and the
/// reader. That backlog is what `catchup` exists to batch.
async fn run_change_feed_transaction_burst(
    engine: &dyn PipelineTestEngine,
    transaction_mode: &str,
) -> (u64, u64) {
    init_logging();

    let arrow_schema = change_feed_arrow_schema();
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;

    let initial: Vec<Value> = (0..4).map(|id| change_feed_row(id, "initial")).collect();
    delta = write_data_to_table(delta, &basic_relation(), &initial).await;

    let storage_dir = TempDir::new().unwrap();
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let pipeline = delta_input_controller(
        engine,
        &table_uri,
        json!({
            "mode": "snapshot_and_follow",
            "change_feed": "auto",
            "transaction_mode": transaction_mode,
        }),
        &basic_relation(),
        storage_dir.path(),
        &errors,
    )
    .await
    .unwrap();
    pipeline.start();

    let output = SqlIdentifier::from("test_output1");
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &initial, &errors).await;

    // Pause, so the three commits below become one backlog.
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
    .expect("timeout waiting for the input endpoint to pause");
    let transactions_before = delta_follow_transaction_starts(&pipeline);

    // Three commits, two of which record change data and one of which does not.
    delta = delta
        .update()
        .with_predicate(col("id").eq(lit(0i64)))
        .with_update("s", lit("updated"))
        .await
        .unwrap()
        .0;
    delta = delta
        .delete()
        .with_predicate(col("id").eq(lit(3i64)))
        .await
        .unwrap()
        .0;
    let appended = vec![change_feed_row(4, "appended")];
    write_data_to_table(delta, &basic_relation(), &appended).await;

    let mut expected: Vec<Value> = initial.iter().filter(|row| id(row) != 3).cloned().collect();
    expected[0]["s"] = Value::from("updated");
    expected.extend(appended);

    pipeline
        .start_input_endpoint(DELTA_TEST_INPUT_ENDPOINT)
        .unwrap();
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

    let transactions = delta_follow_transaction_starts(&pipeline) - transactions_before;
    let snapshot_transactions = delta_snapshot_transaction_starts(&pipeline);
    assert_read_change_data(&pipeline);
    pipeline.stop().unwrap();
    (transactions, snapshot_transactions)
}

/// `change_feed = off` and `auto` reach the same contents from the same table,
/// and read different amounts of data doing it.
///
/// `off` reconstructs every change from file actions; `auto` reads the rows the
/// commit recorded. Both halves are asserted: the results agree, the record
/// counts do not.
#[engine_test(requires(Storage))]
pub fn change_feed_off_reads_file_actions_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        const ROWS: u32 = 50;

        let (auto_rows, auto_records, auto_change_data, _) =
            run_change_feed_option(engine, "auto", ROWS).await;
        let (off_rows, off_records, off_change_data, _) =
            run_change_feed_option(engine, "off", ROWS).await;

        assert_eq!(
            auto_rows, off_rows,
            "the option chooses how a change is read, never what it means"
        );
        assert_eq!(
            auto_change_data, 1,
            "auto must read the UPDATE from the change data it recorded"
        );
        assert_eq!(
            off_change_data, 0,
            "off must not look at change data at all"
        );

        // snapshot + 2 rows against snapshot + the file retracted and re-inserted.
        assert_eq!(
            auto_records,
            u64::from(ROWS) + 2,
            "auto must read the two rows the UPDATE recorded"
        );
        assert_eq!(
            off_records,
            u64::from(ROWS) * 3,
            "off must retract and re-insert the whole rewritten file"
        );
    })
}

/// `catchup` batches a backlog of change feed commits into one Feldera
/// transaction, so no downstream view sees a half-applied backlog.
///
/// The burst mixes the two read paths: an `UPDATE` and a `DELETE` that record
/// change data, and an append that records none and falls back to its `add`
/// action. One transaction spans all three.
///
/// The metric counts transaction labels the commit loop allocates, not labels
/// the reader attached to its queue entries. That loop is shared with follow
/// mode, so this covers `cdf` mode taking part in it and ingesting the backlog,
/// not the label reaching the data. The follow-mode catchup tests above measure
/// the same way.
#[engine_test(requires(Storage))]
pub fn change_feed_catchup_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        let (transactions, snapshot_transactions) =
            run_change_feed_transaction_burst(engine, "catchup").await;

        assert_eq!(
            snapshot_transactions, 1,
            "catchup must ingest the initial snapshot in one Feldera transaction"
        );
        assert_eq!(
            transactions, 1,
            "catchup must ingest a three-commit backlog in one Feldera transaction; \
         {transactions} means it opened one per commit instead of batching them"
        );
    })
}

/// `always` gives each Delta commit its own Feldera transaction, one for one,
/// whichever path the commit is read through.
///
/// Counted the same way as [`change_feed_catchup_test`], with the
/// same caveat about what the metric measures.
#[engine_test(requires(Storage))]
pub fn change_feed_always_transaction_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        let (transactions, _) = run_change_feed_transaction_burst(engine, "always").await;

        assert_eq!(
            transactions, 3,
            "always must open one Feldera transaction per Delta commit; the burst \
         has three, so {transactions} means commits were batched or dropped"
        );
    })
}

/// A change feed read and a file-level follow read of the same table history
/// land on the same ZSet.
///
/// The mutations cover every commit shape the reader distinguishes: an append,
/// which records no change data file and falls back to the `add` actions; an
/// `UPDATE`, which records pre-image and post-image rows; and a `DELETE`.
///
/// Read over [`relation`], whose 21 columns carry the types the change
/// data path handles with machinery of its own: `change_data_read_schema`,
/// `project_physical_to_logical` with `relabel_nested_columns`, and
/// `add_partition_columns`. Binary, decimal, arrays, nested rows, maps of rows,
/// `VARIANT` and `UUID` are where that machinery breaks, and none of them is
/// reached by four scalar columns. The oracle is the follow-mode connector
/// reading the same history, so the wider row type costs no new assertions.
#[engine_test(requires(Storage))]
pub fn change_feed_matches_file_actions_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        init_logging();

        let mut runner = TestRunner::default();
        let relation = relation();
        let arrow_schema = arrow_schema_of(&relation.fields);
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;

        // Snapshot contents, in the table before either connector starts. Proptest
        // fills every column; `delta_data` numbers `bigint` from zero, which the
        // predicates below select on.
        let generated: Vec<Value> = delta_data(40).new_tree(&mut runner).unwrap().current();
        let split = generated.len() / 2;
        let initial: Vec<Value> = generated[..split].to_vec();
        let appended: Vec<Value> = generated[split..].to_vec();
        delta = write_data_to_table(delta, &relation, &initial).await;

        let cdf_storage = TempDir::new().unwrap();
        let follow_storage = TempDir::new().unwrap();
        let cdf_errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let follow_errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let cdf_pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto" }),
            &relation,
            cdf_storage.path(),
            &cdf_errors,
        )
        .await
        .unwrap();
        let follow_pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "off" }),
            &relation,
            follow_storage.path(),
            &follow_errors,
        )
        .await
        .unwrap();
        cdf_pipeline.start();
        follow_pipeline.start();

        let output = SqlIdentifier::from("test_output1");
        let expect_both = async |expected: &[Value]| {
            wait_or_connector_error(&cdf_pipeline, &output, &relation, expected, &cdf_errors).await;
            wait_or_connector_error(
                &follow_pipeline,
                &output,
                &relation,
                expected,
                &follow_errors,
            )
            .await;
        };

        expect_both(&initial).await;

        // Append: no change data file, so the change feed reads the added file.
        delta = write_data_to_table(delta, &relation, &appended).await;
        let mut expected = initial.clone();
        expected.extend(appended.iter().cloned());
        expect_both(&expected).await;

        // Update: pre-image and post-image rows. Only `string` changes, so every
        // other column has to survive the round trip through the change data file
        // for the two readers to agree.
        let updated_below = split as i64 / 2;
        delta = delta
            .update()
            .with_predicate(col("bigint").lt(lit(updated_below)))
            .with_update("string", lit("updated"))
            .await
            .unwrap()
            .0;
        for row in expected
            .iter_mut()
            .filter(|row| bigint(row) < updated_below)
        {
            row["string"] = Value::from("updated");
        }
        expect_both(&expected).await;

        // Delete: rows leave through the change feed, not through a removed file.
        let deleted_from = split as i64;
        let _ = delta
            .delete()
            .with_predicate(col("bigint").gt_eq(lit(deleted_from)))
            .await
            .unwrap();
        expected.retain(|row| bigint(row) < deleted_from);
        expect_both(&expected).await;

        assert_read_change_data(&cdf_pipeline);
        cdf_pipeline.stop().unwrap();
        follow_pipeline.stop().unwrap();
    })
}

/// An `UPDATE` that moves a row across a `filter` boundary retracts it on the way
/// out and inserts it on the way in.
///
/// `filter` decides which rows the connector ingested at all, and the change feed
/// carries both images, so each is judged by the predicate the snapshot used.
#[engine_test(requires(Storage))]
pub fn change_feed_filter_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;

        let initial = vec![
            change_feed_row(0, "keep"),
            change_feed_row(1, "drop"),
            change_feed_row(2, "keep"),
        ];
        delta = write_data_to_table(delta, &basic_relation(), &initial).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto", "filter": "s = 'keep'" }),
            &basic_relation(),
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();

        let output = SqlIdentifier::from("test_output1");
        wait_or_connector_error(
            &pipeline,
            &output,
            &basic_relation(),
            &[change_feed_row(0, "keep"), change_feed_row(2, "keep")],
            &errors,
        )
        .await;

        // Out of the filter: the pre-image passed it and was ingested, so it must be
        // retracted; the post-image does not and is dropped.
        delta = delta
            .update()
            .with_predicate(col("id").eq(lit(0i64)))
            .with_update("s", lit("drop"))
            .await
            .unwrap()
            .0;
        wait_or_connector_error(
            &pipeline,
            &output,
            &basic_relation(),
            &[change_feed_row(2, "keep")],
            &errors,
        )
        .await;

        // Into the filter: only the post-image passes, so the row appears with no
        // retraction to match.
        let _ = delta
            .update()
            .with_predicate(col("id").eq(lit(1i64)))
            .with_update("s", lit("keep"))
            .await
            .unwrap();
        wait_or_connector_error(
            &pipeline,
            &output,
            &basic_relation(),
            &[change_feed_row(1, "keep"), change_feed_row(2, "keep")],
            &errors,
        )
        .await;

        assert_read_change_data(&pipeline);

        pipeline.stop().unwrap();
    })
}

/// `auto` reads a table that records no change feed, falling back to its file
/// actions.
#[engine_test(requires(Storage))]
pub fn change_feed_auto_without_feed_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        // Deliberately not a change-feed table.
        let delta = create_table_from_arrow(&table_uri, &arrow_schema, &[]).await;

        let initial: Vec<Value> = (0..4).map(|id| change_feed_row(id, "initial")).collect();
        let delta = write_data_to_table(delta, &basic_relation(), &initial).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto" }),
            &basic_relation(),
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();

        let output = SqlIdentifier::from("test_output1");
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &initial, &errors).await;

        let appended = vec![change_feed_row(4, "appended")];
        write_data_to_table(delta, &basic_relation(), &appended).await;
        let mut expected = initial;
        expected.extend(appended.iter().cloned());
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

        // The counter cannot tell `auto` from `off` here, since neither reads
        // change data from a table that records none; the config can.
        assert_eq!(delta_change_feed(&pipeline), DeltaTableChangeFeed::Auto);
        assert_eq!(
            delta_connector_counter(&pipeline, "input_connector_delta_commits_from_change_data"),
            0,
            "the table records no change feed, so every commit must fall back to \
         its file actions"
        );
        pipeline.stop().unwrap();
    })
}

/// A commit that records change data is counted as read from change data, and an
/// append, which records none, is counted against the file-action fallback.
#[engine_test(requires(Storage))]
pub fn change_feed_reads_change_data_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;

        let initial: Vec<Value> = (0..4).map(|id| change_feed_row(id, "initial")).collect();
        delta = write_data_to_table(delta, &basic_relation(), &initial).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto" }),
            &basic_relation(),
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();

        let output = SqlIdentifier::from("test_output1");
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &initial, &errors).await;

        const FROM_CHANGE_DATA: &str = "input_connector_delta_commits_from_change_data";
        const FROM_FILE_ACTIONS: &str = "input_connector_delta_commits_from_file_actions";

        // An append records no change data file: the Delta protocol lets a writer
        // skip one when a commit only adds rows.
        let appended = vec![change_feed_row(4, "appended")];
        delta = write_data_to_table(delta, &basic_relation(), &appended).await;
        let mut expected = initial.clone();
        expected.extend(appended.iter().cloned());
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;
        assert_eq!(delta_connector_counter(&pipeline, FROM_FILE_ACTIONS), 1);
        assert_eq!(delta_connector_counter(&pipeline, FROM_CHANGE_DATA), 0);

        // An update records one, and this is the assertion that fails if the reader
        // silently ignores `cdc` actions and re-reads the rewritten files.
        let _ = delta
            .update()
            .with_predicate(col("id").lt(lit(2i64)))
            .with_update("s", lit("updated"))
            .await
            .unwrap();
        for row in expected.iter_mut().filter(|row| id(row) < 2) {
            row["s"] = Value::from("updated");
        }
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;
        assert_eq!(delta_connector_counter(&pipeline, FROM_FILE_ACTIONS), 1);
        assert_eq!(delta_connector_counter(&pipeline, FROM_CHANGE_DATA), 1);

        pipeline.stop().unwrap();
    })
}

/// An `UPDATE` that moves a row between partitions retracts it under its old
/// partition value and inserts it under the new one.
///
/// A change data file keeps its partition column in the path, like any other
/// Delta data file, so the pre-image is written under the old partition and the
/// post-image under the new one.
#[engine_test(requires(Storage))]
pub fn change_feed_partition_column_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        // The partition values need escaping in the path, as in the follow tests.
        let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &["s"]).await;

        let initial: Vec<Value> = (0..4)
            .map(|id| {
                change_feed_row(
                    id,
                    if id.is_multiple_of(2) {
                        "us east"
                    } else {
                        "us/west"
                    },
                )
            })
            .collect();
        delta = write_data_to_table(delta, &basic_relation(), &initial).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "auto" }),
            &basic_relation(),
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();

        let output = SqlIdentifier::from("test_output1");
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &initial, &errors).await;

        // Move id 0 from "us east" to "us/west": its pre-image and post-image land
        // in different partitions of `_change_data`.
        let _ = delta
            .update()
            .with_predicate(col("id").eq(lit(0i64)))
            .with_update("s", lit("us/west"))
            .await
            .unwrap();
        let mut expected = initial.clone();
        expected[0]["s"] = Value::from("us/west");
        wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

        assert_read_change_data(&pipeline);
        pipeline.stop().unwrap();
    })
}

/// `change_feed = require` against a table that records no feed fails at startup,
/// with an error naming the table property that is missing.
#[engine_test(requires(Storage))]
pub fn change_feed_require_not_enabled_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let delta = create_table_from_arrow(&table_uri, &arrow_schema, &[]).await;
        write_data_to_table(delta, &basic_relation(), &[change_feed_row(0, "row")]).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let error = match delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot_and_follow", "change_feed": "require" }),
            &basic_relation(),
            storage_dir.path(),
            &errors,
        )
        .await
        {
            Err(error) => error.to_string(),
            Ok(pipeline) => {
                pipeline.stop().unwrap();
                panic!(
                    "change_feed = require must not start against a table without a change feed"
                );
            }
        };

        assert!(
            error.contains("delta.enableChangeDataFeed"),
            "the error must name the table property; got: {error}"
        );
    })
}

/// `change_feed` is rejected in a mode that never reads one, rather than
/// silently dropped.
///
/// `require` is the reason it must fail: its whole purpose is to fail loudly
/// when a table records no change data, so a connector that starts and reads
/// nothing from the feed is the one outcome it exists to rule out. `snapshot`
/// never follows the log, and `cdc` reads a change log the user encoded as
/// table rows.
#[engine_test(requires(Storage))]
pub fn change_feed_rejected_outside_follow_modes_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;
        write_data_to_table(delta, &basic_relation(), &[change_feed_row(0, "row")]).await;

        for (mode, change_feed) in [
            ("snapshot", "auto"),
            ("snapshot", "require"),
            // Rejected even where it names the default, so the rule is "not valid
            // in this mode" rather than "not valid unless it happens to agree".
            ("snapshot", "off"),
            ("cdc", "auto"),
            ("cdc", "require"),
            ("cdc", "off"),
        ] {
            let storage_dir = TempDir::new().unwrap();
            let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let mut config = json!({ "mode": mode, "change_feed": change_feed });
            if mode == "cdc" {
                config["cdc_order_by"] = "id".into();
                config["cdc_delete_filter"] = "b".into();
            }
            let error = match delta_input_controller(
                engine,
                &table_uri,
                config,
                &basic_relation(),
                storage_dir.path(),
                &errors,
            )
            .await
            {
                Err(error) => error.to_string(),
                Ok(pipeline) => {
                    pipeline.stop().unwrap();
                    panic!("'change_feed = {change_feed}' must not start in '{mode}' mode");
                }
            };
            assert!(
                error.contains("'change_feed'") && error.contains(mode),
                "the error must name the property and the mode; got: {error}"
            );
        }
    })
}

/// A `follow` mode connector accepts `change_feed`, so the rejection above is
/// about the mode and not about the property.
#[engine_test(requires(Storage))]
pub fn change_feed_accepted_in_follow_modes_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;
        write_data_to_table(delta, &basic_relation(), &[change_feed_row(0, "row")]).await;

        for mode in ["follow", "snapshot_and_follow"] {
            let storage_dir = TempDir::new().unwrap();
            let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let pipeline = delta_input_controller(
                engine,
                &table_uri,
                json!({ "mode": mode, "change_feed": "require" }),
                &basic_relation(),
                storage_dir.path(),
                &errors,
            )
            .await
            .unwrap_or_else(|e| panic!("'change_feed' must be valid in '{mode}' mode: {e}"));
            pipeline.stop().unwrap();
        }
    })
}

/// `skip_unused_columns` applies to a change feed read, and the columns it drops
/// leave `_change_type` in place.
#[engine_test(requires(Storage))]
pub fn change_feed_skip_unused_columns_test(engine: &dyn PipelineTestEngine) {
    run_async_on(4, async {
        init_logging();

        let arrow_schema = change_feed_arrow_schema();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let mut delta = create_change_feed_table(&table_uri, &arrow_schema, &[]).await;

        let initial: Vec<Value> = (0..3).map(|id| change_feed_row(id, "initial")).collect();
        delta = write_data_to_table(delta, &basic_relation(), &initial).await;

        // `i` is nullable and unused by the circuit, so it can be skipped.
        let relation = basic_unused_i_relation();

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({
                "mode": "snapshot_and_follow",
                "change_feed": "auto",
                "skip_unused_columns": true,
            }),
            &relation,
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();

        let output = SqlIdentifier::from("test_output1");
        wait_or_connector_error(&pipeline, &output, &relation, &initial, &errors).await;

        let _ = delta
            .delete()
            .with_predicate(col("id").eq(lit(0i64)))
            .await
            .unwrap();
        wait_or_connector_error(&pipeline, &output, &relation, &initial[1..], &errors).await;

        assert_read_change_data(&pipeline);
        pipeline.stop().unwrap();
    })
}

/// [`wait_for_records_materialized`], but report a connector error as soon as
/// one arrives rather than waiting out the materialization timeout.
pub(super) async fn wait_or_connector_error(
    pipeline: &Controller,
    table: &SqlIdentifier,
    relation: &Relation,
    expected_output: &[Value],
    errors: &Arc<Mutex<Vec<String>>>,
) {
    let wait = wait_for_records_materialized(pipeline, table, relation, expected_output);
    tokio::pin!(wait);
    loop {
        tokio::select! {
            _ = &mut wait => return,
            _ = sleep(Duration::from_millis(250)) => {
                if let Some(error) = errors.lock().unwrap().first() {
                    panic!("connector reported an error: {error}");
                }
            }
        }
    }
}

/// A follow read under a `filter` over a Delta column the SQL table never
/// declares admits the rows the predicate selects.
#[engine_test(requires(Storage))]
pub fn follow_filter_undeclared_column_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        run_follow_filter_undeclared_column_test(engine, "region = 'us'").await;
    })
}

/// The same over a field of an undeclared struct column, which parses as a
/// compound identifier, so the read set keeps `meta`.
#[engine_test(requires(Storage))]
pub fn follow_filter_undeclared_struct_field_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        run_follow_filter_undeclared_column_test(engine, "meta.tag = 'us'").await;
    })
}

/// A CDC read projects before it filters, so the read set has to keep the
/// columns the `filter` names. `filter_expr` selects the rows in region 'us'.
async fn run_cdc_filter_undeclared_column_test(engine: &dyn PipelineTestEngine, filter_expr: &str) {
    use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
        Schema as ArrowSchema,
    };

    init_logging();

    // Only `id`, `b` and `s` are declared in SQL. `region` and `meta` back the
    // two filter forms, the `__feldera_*` pair drives CDC, and `junk` is named
    // nowhere, so no CDC read may decode it.
    let meta_fields: ArrowFields =
        vec![Arc::new(ArrowField::new("tag", ArrowDataType::Utf8, false))].into();
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("region", ArrowDataType::Utf8, false),
        ArrowField::new("meta", ArrowDataType::Struct(meta_fields.clone()), false),
        ArrowField::new("junk", ArrowDataType::Utf8, false),
        ArrowField::new("__feldera_op", ArrowDataType::Utf8, false),
        ArrowField::new("__feldera_ts", ArrowDataType::Int64, false),
    ]));

    // Even ids are in region 'us' (kept), odd ids in 'eu' (filtered out).
    let region = |id: u32| if id.is_multiple_of(2) { "us" } else { "eu" };
    let row = |id: u32| json!({ "id": id, "b": false, "i": null, "s": format!("row-{id}") });
    // One CDC event per id: `op` 'i' inserts the row, 'd' deletes it.
    let make_batch = |ids: &[u32], op: &str, ts: i64| -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    ids.iter().map(|id| *id as i64),
                )) as Arc<dyn Array>,
                Arc::new(ids.iter().map(|_| Some(false)).collect::<BooleanArray>()),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| format!("row-{id}")),
                )),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| region(*id)),
                )),
                Arc::new(StructArray::new(
                    meta_fields.clone(),
                    vec![Arc::new(StringArray::from_iter_values(
                        ids.iter().map(|id| region(*id)),
                    ))],
                    None,
                )),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| format!("junk-{id}")),
                )),
                Arc::new(StringArray::from_iter_values(ids.iter().map(|_| op))),
                Arc::new(Int64Array::from_iter_values(ids.iter().map(|_| ts))),
            ],
        )
        .unwrap()
    };

    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let mut delta = create_table_from_arrow(&table_uri, &arrow_schema, &[]).await;

    let storage_dir = TempDir::new().unwrap();
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let pipeline = delta_input_controller(
        engine,
        &table_uri,
        json!({
            "mode": "cdc",
            "filter": filter_expr,
            "cdc_delete_filter": "__feldera_op = 'd'",
            "cdc_order_by": "__feldera_ts",
        }),
        &basic_relation(),
        storage_dir.path(),
        &errors,
    )
    .await
    .unwrap();
    pipeline.start();
    let output = SqlIdentifier::from("test_output1");

    let ids: Vec<u32> = (0..6).collect();
    delta = delta
        .write(vec![make_batch(&ids, "i", 1)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    let mut expected: Vec<Value> = ids
        .iter()
        .filter(|id| region(**id) == "us")
        .map(|id| row(*id))
        .collect();
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

    // A delete event retracts the row it repeats, which only cancels if both
    // commits were read through the same projection.
    delta = delta
        .write(vec![make_batch(&[0], "d", 2)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    expected.remove(0);
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

    // An UPDATE rewrites the file, so this commit pairs `remove` and `add`
    // actions for `EXCEPT ALL` to reconcile. It touches only `junk`, which the
    // projection drops, so the two sides match and the commit cancels.
    delta = delta
        .update()
        .with_predicate(col("junk").eq(lit("junk-2")))
        .with_update("junk", lit("junk-2-rewritten"))
        .await
        .unwrap()
        .0;
    delta = delta
        .write(vec![make_batch(&[2], "d", 3)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    // Versions are read in order, so the sentinel appears only once the rewrite
    // and the delete are both in: a row the rewrite re-emitted would be here
    // still, since the delete retracts one copy, not two.
    let _ = delta
        .write(vec![make_batch(&[100], "i", 4)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    expected.remove(0);
    expected.push(row(100));
    wait_or_connector_error(&pipeline, &output, &basic_relation(), &expected, &errors).await;

    pipeline.stop().unwrap();
}

/// A change feed read prunes from the scan a column that neither the filter nor
/// the projection names, even though the declared read schema lists it.
///
/// `skip_unused_columns` on the change feed path rests on DataFusion's
/// projection pushdown rather than on the declared read schema:
/// `change_data_read_schema` declares every column, and the projection to
/// `used_columns` comes several nodes later, after the physical-to-logical
/// rename, the partition literals, and a union across partition groups.
/// [`follow_filter_before_projection_prunes_scan`] asserts the same for a follow
/// read, which stacks fewer nodes between the scan and the projection.
#[tokio::test]
async fn change_feed_projection_prunes_scan() {
    use crate::integrated::delta_table::input::apply_filter;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    // A change data file's columns: the table's own, minus the partition column
    // that lives in the log, plus `_change_type`. `region` is filtered on but
    // not projected; `junk` is neither.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("region", ArrowDataType::Utf8, false),
        ArrowField::new("junk", ArrowDataType::Utf8, false),
        ArrowField::new("_change_type", ArrowDataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let mut groups = Vec::new();
    for (i, grp) in ["0", "1"].iter().enumerate() {
        let name = format!("cdc_{i}");
        ctx.register_table(
            name.as_str(),
            Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap()),
        )
        .unwrap();
        // What `add_partition_columns` produces: the table's columns in schema
        // order, the partition column supplied as a literal from the log, and
        // the extra column carried through.
        groups.push(
            ctx.table(name.as_str())
                .await
                .unwrap()
                .select(vec![
                    col("id"),
                    col("s"),
                    col("region"),
                    col("junk"),
                    lit(*grp).alias("grp"),
                    col("_change_type"),
                ])
                .unwrap(),
        );
    }

    let mut groups = groups.into_iter();
    let df = groups.next().unwrap();
    let df = groups.try_fold(df, |acc, g| acc.union(g)).unwrap();
    let df = apply_filter(df, Some("region = 'us'"), "unit-test", None).unwrap();
    let df = df
        .select_columns(&["id", "s", "grp", "_change_type"])
        .unwrap();

    let plan = format!("{}", df.into_optimized_plan().unwrap().display_indent());
    assert!(
        plan.contains("projection=") && !plan.contains("junk"),
        "the scan must read only the projected columns plus the filter's, so \
         'junk' must be pruned through the partition literals and the union; \
         plan was:\n{plan}"
    );
}

#[tokio::test]
async fn follow_filter_before_projection_prunes_scan() {
    use crate::integrated::delta_table::input::apply_filter;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    // `region` is filtered on but not projected; `junk` is neither.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("region", ArrowDataType::Utf8, false),
        ArrowField::new("junk", ArrowDataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap()),
    )
    .unwrap();

    let df = apply_filter(
        ctx.table("t").await.unwrap(),
        Some("region = 'us'"),
        "unit-test",
        None,
    )
    .unwrap();
    let df = df.select_columns(&["id", "b", "s"]).unwrap();

    let plan = format!("{}", df.into_optimized_plan().unwrap().display_indent());
    assert!(
        plan.contains("projection=") && !plan.contains("junk"),
        "the scan must read only the projected columns plus the filter's, so \
         'junk' must be pruned; plan was:\n{plan}"
    );
}

/// A CDC read under a `filter` over a Delta column the SQL table never declares
/// admits the rows the predicate selects.
#[engine_test(requires(Storage))]
pub fn cdc_filter_undeclared_column_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        run_cdc_filter_undeclared_column_test(engine, "region = 'us'").await;
    })
}

/// The same over a field of an undeclared struct column, which parses as a
/// compound identifier, so the read set keeps `meta`.
#[engine_test(requires(Storage))]
pub fn cdc_filter_undeclared_struct_field_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        run_cdc_filter_undeclared_column_test(engine, "meta.tag = 'us'").await;
    })
}

/// A `filter` that does not parse fails the connector at startup rather than on
/// the first Delta commit.
///
/// The directory holds no Delta table, so opening it would fail with a different
/// error; validation happens before the connector opens it.
#[engine_test(requires(Storage))]
pub fn follow_invalid_filter_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        init_logging();

        let relation = basic_relation();
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let result = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "follow", "filter": "s =" }),
            &relation,
            storage_dir.path(),
            &errors,
        )
        .await;

        let error = match result {
            Ok(_) => panic!("the controller must not start with an invalid filter"),
            Err(e) => e.to_string(),
        };

        assert!(
            error.contains("'filter'"),
            "expected the error to name 'filter', got: {error}"
        );
    })
}

/// Create the VARIANT test table: one Delta `variant` column and one Delta
/// `string` column holding JSON, both declared `VARIANT` in SQL.
async fn create_variant_test_table(table_uri: &str) -> DeltaTable {
    create_table(
        table_uri,
        &HashMap::new(),
        &[
            StructField::new("id", DataType::LONG, false),
            StructField::new("v", DataType::unshredded_variant(), true),
            StructField::new("json_v", DataType::STRING, true),
        ],
    )
    .await
}

/// Append the two test rows: one carrying a variant of every type the mapping
/// covers, one whose VARIANT columns are SQL NULL.
async fn write_variant_test_rows(table: DeltaTable) -> DeltaTable {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::DataType as ArrowDataType;
    use chrono::NaiveDate;
    use parquet_variant::VariantBuilderExt;
    use parquet_variant_compute::VariantArrayBuilder;

    // Build the batch against the table's own Arrow schema so the column types
    // and nullability match what Delta declared.
    let arrow_schema = table
        .snapshot()
        .unwrap()
        .snapshot()
        .arrow_schema()
        .as_ref()
        .clone();

    let mut builder = VariantArrayBuilder::new(2);
    {
        let mut object = builder.new_object();
        object.insert("int", 42i64);
        object.insert("str", "hello");
        object.insert("bool", true);
        object.insert("nul", parquet_variant::Variant::Null);
        object.insert(
            "dec",
            parquet_variant::Variant::Decimal4(
                parquet_variant::VariantDecimal4::try_new(12_345i32, 2u8).unwrap(),
            ),
        );
        object.insert(
            "date",
            parquet_variant::Variant::Date(NaiveDate::from_ymd_opt(2026, 9, 1).unwrap()),
        );
        object.insert("bin", parquet_variant::Variant::Binary(&[1u8, 2, 3]));
        {
            let mut list = object.new_list("list");
            list.append_value(1i64);
            list.append_value("two");
            list.finish();
        }
        object.finish();
    }
    builder.append_null();

    // The Parquet variant builder produces `BinaryView` sub-fields; Delta
    // stores them as `binary`.
    let variant_type = arrow_schema.field_with_name("v").unwrap().data_type();
    let variant_column =
        arrow::compute::cast(&ArrayRef::from(builder.build()), variant_type).unwrap();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1i64, 2])),
        variant_column,
        Arc::new(StringArray::from(vec![Some(r#"{"legacy": true}"#), None])),
    ];
    assert_eq!(
        arrow_schema.fields().len(),
        columns.len(),
        "test batch must cover every column"
    );
    assert!(matches!(variant_type, ArrowDataType::Struct(_)));

    let batch = RecordBatch::try_new(Arc::new(arrow_schema), columns).unwrap();

    table
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

/// The `insert` payloads the pipeline wrote, sorted by `id`.
fn variant_output_records(path: &Path) -> Vec<Value> {
    let contents = std::fs::read_to_string(path).unwrap_or_default();
    let mut records: Vec<Value> = contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<Value>(line)
                .unwrap_or_else(|e| panic!("output line is not JSON: {line}: {e}"))["insert"]
                .clone()
        })
        .collect();
    records.sort_by_key(|record| record["id"].as_i64().unwrap());
    records
}

/// What the two rows must look like once ingested.
///
/// The typed values are the point: a date arrives as a date and a decimal as a
/// number because the connector decoded the Parquet variant, which a detour
/// through JSON text could not have produced.
fn expected_variant_records(engine: &dyn PipelineTestEngine) -> Vec<Value> {
    // A binary value reads back as an array of bytes on an engine with
    // `VariantBinaryAsArray`, and as base64 text on any other.
    let bin = if engine.has(Capability::VariantBinaryAsArray) {
        json!([1, 2, 3])
    } else {
        json!("AQID")
    };
    vec![
        json!({
            "id": 1,
            "v": {
                "bin": bin,
                "bool": true,
                "date": "2026-09-01",
                "dec": 123.45,
                "int": 42,
                "list": [1, "two"],
                "nul": null,
                "str": "hello",
            },
            "json_v": {"legacy": true},
        }),
        json!({"id": 2, "v": null, "json_v": null}),
    ]
}

/// A Delta `variant` column written by something other than Feldera.
///
/// The round trips of [`relation`] cover Feldera writing and reading its own
/// variants. This covers what they structurally cannot: an encoding produced by
/// another engine, values of types that the rows of [`relation`] never hold,
/// and a JSON-string `VARIANT` column beside a binary one, which is the
/// per-column choice the reader makes from the data.
#[engine_test]
pub fn foreign_variant_test(engine: &dyn PipelineTestEngine) {
    run_async(async {
        init_logging();

        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let table = create_variant_test_table(&table_uri).await;
        write_variant_test_rows(table).await;

        let output_file = NamedTempFile::new().unwrap();
        let pipeline = delta_table_input_pipeline(
            engine,
            &table_uri,
            &variant_relation(),
            &HashMap::from([("mode".to_string(), Value::from("snapshot"))]),
            &output_file.path().display().to_string(),
        );
        pipeline.start();
        wait(|| pipeline.pipeline_complete(), 60_000).expect("timeout ingesting the snapshot");
        pipeline.stop().unwrap();

        assert_eq!(
            variant_output_records(output_file.path()),
            expected_variant_records(engine)
        );
    })
}

/// Whether the table's protocol advertises the `variantType` writer feature,
/// which a table holding a `variant` column requires.
fn delta_has_variant_writer_feature(table_uri: &str) -> bool {
    use deltalake::open_table;
    use feldera_storage::tokio::TOKIO;

    let url = url::Url::from_file_path(table_uri).unwrap();
    let table = TOKIO
        .block_on(async move { open_table(url).await })
        .unwrap();
    table
        .snapshot()
        .unwrap()
        .protocol()
        .writer_features()
        .is_some_and(|features| features.iter().any(|f| f.as_ref() == "variantType"))
}

/// The declared Delta type of `column`, as the table's schema JSON spells it.
fn delta_column_type(table_uri: &str, column: &str) -> String {
    use deltalake::open_table;
    use feldera_storage::tokio::TOKIO;

    let url = url::Url::from_file_path(table_uri).unwrap();
    let table = TOKIO
        .block_on(async move { open_table(url).await })
        .unwrap();
    let schema = table.snapshot().unwrap().schema();
    serde_json::to_value(schema.field(column).unwrap().data_type())
        .unwrap()
        .to_string()
}

/// A `VARIANT` nested in a `ROW`, an `ARRAY` and a `MAP` must be declared as a
/// Delta `variant`, not as the struct of two binary buffers it is encoded as.
///
/// The data is written as a variant either way, so a Feldera-to-Feldera round
/// trip cannot see the difference: the reader recognizes the buffers by name.
/// Only the declared type tells another engine what the column holds.
#[test]
fn delta_output_nested_variant_type_test() {
    use crate::integrated::delta_table::output::delta_variant_types;
    use delta_kernel::engine::arrow_conversion::TryFromArrow;
    use deltalake::kernel::DataType;
    use feldera_types::program_schema::ColumnType;

    let variant = || ColumnType::variant(true);
    let cases = [
        ("plain", variant()),
        (
            "row",
            ColumnType::structure(true, &[Field::new("v".into(), variant())]),
        ),
        ("array", ColumnType::array(true, variant())),
        (
            "map",
            ColumnType::map(true, ColumnType::varchar(false), variant()),
        ),
        (
            "row_of_array",
            ColumnType::structure(
                true,
                &[Field::new("a".into(), ColumnType::array(true, variant()))],
            ),
        ),
    ];

    for (name, sql_type) in cases {
        let arrow = relation_to_arrow_fields(
            &[Field::new(name.into(), sql_type.clone())],
            delta_schema_options(),
        );
        let from_arrow = DataType::try_from_arrow(arrow[0].data_type()).unwrap();

        // What the Arrow type alone says: the storage struct, at every depth.
        let raw = serde_json::to_string(&from_arrow).unwrap();
        assert!(
            raw.contains("\"name\":\"metadata\""),
            "{name} was expected to arrive as the storage struct: {raw}"
        );

        let json =
            serde_json::to_string(&delta_variant_types(&sql_type, from_arrow).unwrap()).unwrap();
        assert!(
            json.contains("\"variant\""),
            "{name} declares no variant: {json}"
        );
        assert!(
            !json.contains("\"name\":\"metadata\""),
            "{name} still declares the storage struct: {json}"
        );
    }
}

/// Appending to a table an earlier Feldera created is the likeliest upgrade
/// failure of the default flip, and nothing downstream catches it: `append`
/// opens the existing table with `SaveMode::Ignore`, which keeps its schema,
/// the writer validates each batch against the schema the connector computed
/// itself, and the commit checks no schema at all. Without this guard the
/// connector writes variant buffers into a column the table declares a
/// `string`, with no error.
#[tokio::test]
async fn delta_output_variant_encoding_mismatch_test() {
    use crate::integrated::delta_table::output::check_variant_encoding;
    use feldera_types::transport::delta_table::DeltaVariantEncoding;

    init_logging();

    let string_column = StructField::new("v", DataType::STRING, true);
    let variant_column = StructField::new("v", DataType::unshredded_variant(), true);

    for (existing, configured, encoding, expected) in [
        (
            &string_column,
            &variant_column,
            DeltaVariantEncoding::Variant,
            "'json_string'",
        ),
        (
            &variant_column,
            &string_column,
            DeltaVariantEncoding::JsonString,
            "'variant'",
        ),
    ] {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let table = create_table(&table_uri, &HashMap::new(), std::slice::from_ref(existing)).await;

        let error = check_variant_encoding(&table, std::slice::from_ref(configured), encoding)
            .unwrap_err()
            .to_string();
        assert!(error.contains("column 'v'"), "{error}");
        assert!(error.contains(expected), "{error}");

        // The matching encoding is accepted.
        check_variant_encoding(&table, std::slice::from_ref(existing), encoding).unwrap();
    }
}

/// Run a `mode = 'id'` table through the input connector and return the net rows
/// it ingested, rows of `relation`. Following stops at `end_version`, so the pipeline
/// completes on its own.
fn read_id_mapped_table(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    relation: &Relation,
    end_version: u64,
    config: &[(&str, Value)],
) -> ZSet {
    read_id_mapped_table_counting(engine, table_uri, relation, end_version, config, &mut 0)
}

/// [`read_id_mapped_table`], also reporting how many commits the connector read
/// from the Change Data Feed rather than from file actions.
fn read_id_mapped_table_counting(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    relation: &Relation,
    end_version: u64,
    config: &[(&str, Value)],
    change_data_commits: &mut u64,
) -> ZSet {
    let mut connector_config: HashMap<String, Value> = HashMap::from([
        ("version".to_string(), Value::from(0)),
        ("end_version".to_string(), Value::from(end_version)),
    ]);
    connector_config.extend(
        config
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone())),
    );

    let output_file = NamedTempFile::new().unwrap();
    let pipeline = delta_table_input_pipeline(
        engine,
        table_uri,
        relation,
        &connector_config,
        &output_file.path().display().to_string(),
    );
    pipeline.start();
    wait(|| pipeline.pipeline_complete(), 60_000).expect("timeout ingesting the table");
    *change_data_commits =
        delta_connector_counter(&pipeline, "input_connector_delta_commits_from_change_data");
    pipeline.stop().unwrap();

    let zset = file_zset(relation, &output_file);
    let (_, weights) = zset.to_batch().unwrap();
    assert!(
        weights.iter().all(|weight| *weight == 1),
        "each row must be ingested once: {zset:?}"
    );
    zset
}

/// Read a Uniform table declared as [`uniform_relation`].
fn read_uniform_table(
    engine: &dyn PipelineTestEngine,
    table_uri: &str,
    end_version: u64,
    config: &[(&str, Value)],
) -> ZSet {
    read_id_mapped_table(engine, table_uri, &uniform_relation(), end_version, config)
}

/// CDC mode resolves a Uniform file's columns by field id too, including the
/// undeclared `op` and `ts` its own configuration names. The second commit's
/// delete cancels the first commit's row only if both reads resolved alike.
#[engine_test]
pub fn cdc_uniform_field_id_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        write_id_mapped_table(
            table_dir.path(),
            &uniform_cdc_relation(),
            &[
                &[
                    uniform_cdc_row(1, "alpha", "Coffee Shop", "settled", "c", 1),
                    uniform_cdc_row(2, "beta", "Gas Station", "pending", "c", 2),
                ],
                &[uniform_cdc_row(
                    1,
                    "alpha",
                    "Coffee Shop",
                    "settled",
                    "d",
                    3,
                )],
            ],
        );
        let table_uri = table_dir.path().display().to_string();

        let rows = off_runtime(move || {
            read_uniform_table(
                engine,
                &table_uri,
                2,
                &[
                    ("mode", json!("cdc")),
                    ("cdc_delete_filter", json!("op = 'd'")),
                    ("cdc_order_by", json!("ts")),
                ],
            )
        });

        assert_eq!(
            rows,
            rows_zset(
                &uniform_relation(),
                &[uniform_row(2, "beta", "Gas Station", "pending")]
            ),
            "the CDC delete must cancel row 1, leaving row 2"
        );
    })
}

/// Stamp `field` and every struct field nested inside it with a Parquet field
/// id, numbering depth-first from `next_id`.
fn stamp_field_ids(field: &arrow::datatypes::Field, next_id: &mut i64) -> arrow::datatypes::Field {
    let id = *next_id;
    *next_id += 1;
    let mut metadata = field.metadata().clone();
    metadata.insert("PARQUET:field_id".to_string(), id.to_string());
    field
        .clone()
        .with_data_type(stamp_nested_field_ids(field.data_type(), next_id))
        .with_metadata(metadata)
}

/// Stamp the struct fields nested inside `data_type`, leaving every other type
/// as it is. A list's element and a map's entries are unnamed in Delta, so they
/// take no id of their own; their struct fields still do.
fn stamp_nested_field_ids(data_type: &ArrowDataType, next_id: &mut i64) -> ArrowDataType {
    fn nested(element: &FieldRef, next_id: &mut i64) -> FieldRef {
        Arc::new(
            element
                .as_ref()
                .clone()
                .with_data_type(stamp_nested_field_ids(element.data_type(), next_id)),
        )
    }
    match data_type {
        ArrowDataType::Struct(fields) => ArrowDataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(stamp_field_ids(field, next_id)))
                .collect(),
        ),
        ArrowDataType::List(element) => ArrowDataType::List(nested(element, next_id)),
        ArrowDataType::LargeList(element) => ArrowDataType::LargeList(nested(element, next_id)),
        ArrowDataType::Map(entries, sorted) => {
            ArrowDataType::Map(nested(entries, next_id), *sorted)
        }
        other => other.clone(),
    }
}

/// Rename a field to the physical name the log gives it, recursively, keeping
/// its field id: this is how Delta writes a mapped table's data files. An
/// Iceberg writer leaves the logical names, which [`write_id_mapped_table`]
/// does. Delta spells its physical names `col-<uuid>` and these `col-<id>`;
/// only the agreement with the log matters.
fn to_physical_field(field: &arrow::datatypes::Field) -> arrow::datatypes::Field {
    let name = match field.metadata().get("PARQUET:field_id") {
        Some(id) => format!("col-{id}"),
        None => field.name().clone(),
    };
    arrow::datatypes::Field::new(
        name,
        to_physical_type(field.data_type()),
        field.is_nullable(),
    )
    .with_metadata(field.metadata().clone())
}

/// [`to_physical_field`] for the struct fields nested inside `data_type`.
fn to_physical_type(data_type: &ArrowDataType) -> ArrowDataType {
    let nested = |field: &FieldRef| Arc::new(to_physical_field(field));
    match data_type {
        ArrowDataType::Struct(fields) => ArrowDataType::Struct(fields.iter().map(nested).collect()),
        ArrowDataType::List(element) => ArrowDataType::List(nested(element)),
        ArrowDataType::LargeList(element) => ArrowDataType::LargeList(nested(element)),
        // A map's `key` and `value` are named by the Parquet encoding, not by the
        // table, so they keep their names; a struct inside them does not.
        ArrowDataType::Map(entries, sorted) => {
            let keep_name = |field: &FieldRef| {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(to_physical_type(field.data_type()))
                        .with_metadata(Default::default()),
                )
            };
            let entries = match entries.data_type() {
                ArrowDataType::Struct(fields) => Arc::new(entries.as_ref().clone().with_data_type(
                    ArrowDataType::Struct(fields.iter().map(keep_name).collect()),
                )),
                _ => Arc::clone(entries),
            };
            ArrowDataType::Map(entries, *sorted)
        }
        other => other.clone(),
    }
}

/// Rewrite `batch` under the physical names [`to_physical_field`] gives its
/// columns. Only field names change, so the buffers move across as they are.
fn to_physical_batch(batch: &arrow::array::RecordBatch) -> arrow::array::RecordBatch {
    /// Relabel `data` and its children to `target`, which must differ from the
    /// array's own type in field names alone.
    fn relabel(data: arrow::array::ArrayData, target: &ArrowDataType) -> arrow::array::ArrayData {
        let child_types: Vec<&ArrowDataType> = match target {
            ArrowDataType::Struct(fields) => fields.iter().map(|f| f.data_type()).collect(),
            ArrowDataType::List(field)
            | ArrowDataType::LargeList(field)
            | ArrowDataType::Map(field, _) => vec![field.data_type()],
            _ => vec![],
        };
        let children = data
            .child_data()
            .iter()
            .enumerate()
            .map(|(index, child)| match child_types.get(index) {
                Some(target) => relabel(child.clone(), target),
                None => child.clone(),
            })
            .collect();
        data.into_builder()
            .data_type(target.clone())
            .child_data(children)
            .build()
            .unwrap()
    }

    let fields: Vec<arrow::datatypes::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| to_physical_field(field))
        .collect();
    let columns = batch
        .columns()
        .iter()
        .zip(&fields)
        .map(|(column, field)| {
            arrow::array::make_array(relabel(column.to_data(), field.data_type()))
        })
        .collect();
    arrow::array::RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
}

/// Rewrite the `parquet.field.id` metadata the Arrow conversion carries over
/// into the column mapping metadata a `mode = 'id'` log holds. Every column
/// gets a physical `col-<id>` name, which is never the logical name its data
/// file uses.
fn to_column_mapping_metadata(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if let Some(Value::Object(metadata)) = object.get_mut("metadata")
                && let Some(id) = metadata.remove("parquet.field.id")
            {
                metadata.insert(
                    "delta.columnMapping.physicalName".to_string(),
                    json!(format!("col-{id}")),
                );
                metadata.insert("delta.columnMapping.id".to_string(), id);
            }
            object.values_mut().for_each(to_column_mapping_metadata);
        }
        Value::Array(items) => items.iter_mut().for_each(to_column_mapping_metadata),
        _ => {}
    }
}

/// Reverse the child order of every struct nested in `schema`, which is what an
/// Iceberg field reorder leaves behind: the log and the data file agree on field
/// ids but not on position, so pairing children by position swaps their values.
fn reverse_nested_struct_fields(schema: &mut Value) {
    fn walk(value: &mut Value, nested: bool) {
        match value {
            Value::Object(object) => {
                if let Some(Value::Array(fields)) = object.get_mut("fields") {
                    if nested {
                        fields.reverse();
                    }
                    fields.iter_mut().for_each(|field| walk(field, true));
                } else {
                    object.values_mut().for_each(|v| walk(v, nested));
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|v| walk(v, nested)),
            _ => {}
        }
    }
    walk(schema, false);
}

/// The largest column mapping id a `schemaString` assigns, which the table's
/// `maxColumnId` must be at least.
fn max_column_mapping_id(schema_string: &str) -> i64 {
    fn walk(value: &Value, max: &mut i64) {
        match value {
            Value::Object(object) => {
                if let Some(id) = object.get("delta.columnMapping.id").and_then(Value::as_i64) {
                    *max = (*max).max(id);
                }
                object.values().for_each(|v| walk(v, max));
            }
            Value::Array(items) => items.iter().for_each(|v| walk(v, max)),
            _ => {}
        }
    }
    let mut max = 0;
    walk(&serde_json::from_str(schema_string).unwrap(), &mut max);
    max
}

/// Write the log of a `columnMapping.mode = 'id'` table: version 0 carries the
/// protocol and metadata alone, then one version per commit, each adding the
/// files `write_files` wrote for it.
///
/// The log is written by hand because delta-rs refuses to create or write a
/// column-mapped table.
/// Write `batch` to `name` under the table root, creating its parent directory.
fn write_parquet(table_dir: &Path, name: &str, batch: &arrow::array::RecordBatch) {
    let path = table_dir.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let file = File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// One data file a commit of a `mode = 'id'` table writes.
struct IdMappedFile {
    /// Path relative to the table root.
    path: String,
    /// The `partitionValues` its action records.
    partition_values: Value,
    /// Record it as a `cdc` action (a change data file) rather than an `add`.
    change_data: bool,
    /// Rows of this file an inline deletion vector masks, by index. Empty
    /// records no deletion vector at all.
    deleted_rows: Vec<u32>,
}

/// A Delta inline deletion vector masking `rows`, by index into its data file:
/// the `pathOrInlineDv` string and the byte count `sizeInBytes` records.
///
/// Inline, so the bitmap rides in the log and the fixture writes no file of its
/// own for it.
fn inline_deletion_vector(rows: &[u32]) -> (String, usize) {
    /// Delta marks a deletion vector's bitmap with this, in little-endian bytes.
    const PORTABLE_ROARING_MAGIC: u32 = 1681511377;

    let bitmap: RoaringTreemap = rows.iter().map(|&row| row as u64).collect();
    let mut bytes = PORTABLE_ROARING_MAGIC.to_le_bytes().to_vec();
    bitmap.serialize_into(&mut bytes).unwrap();
    let size_in_bytes = bytes.len();
    // z85 encodes four bytes at a time; a reader stops at the bitmap's end, so
    // padding it out is invisible.
    bytes.resize(size_in_bytes.next_multiple_of(4), 0);
    (z85_encode(&bytes), size_in_bytes)
}

/// Encode `bytes` as z85, which is how a Delta log carries binary in JSON.
///
/// Each four bytes become five base-85 digits, most significant first.
pub(super) fn z85_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 85] =
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

    let mut encoded = String::with_capacity(bytes.len() / 4 * 5);
    for chunk in bytes.as_chunks::<4>().0 {
        let mut value = u32::from_be_bytes(*chunk);
        let mut group = [0u8; 5];
        for digit in group.iter_mut().rev() {
            *digit = DIGITS[(value % 85) as usize];
            value /= 85;
        }
        encoded.push_str(std::str::from_utf8(&group).unwrap());
    }
    encoded
}

fn write_id_mapped_log(
    table_dir: &Path,
    schema_string: &str,
    partition_columns: &[&str],
    change_data_feed: bool,
    commits: usize,
    mut write_files: impl FnMut(u64) -> Vec<IdMappedFile>,
) {
    let max_column_id = max_column_mapping_id(schema_string);
    let log_dir = table_dir.join("_delta_log");
    std::fs::create_dir_all(&log_dir).unwrap();

    let write_commit = |version: u64, actions: &[Value]| {
        let mut file = File::create(log_dir.join(format!("{version:020}.json"))).unwrap();
        for action in actions {
            writeln!(file, "{action}").unwrap();
        }
    };

    // Column mapping and `variant` are both table features, so the protocol has
    // to declare them that way rather than by a minimum reader version.
    write_commit(
        0,
        &[
            json!({"protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping", "variantType", "deletionVectors"],
                "writerFeatures": ["columnMapping", "variantType", "deletionVectors"],
            }}),
            json!({"metaData": {
                "id": uuid::Uuid::new_v4().to_string(),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": partition_columns,
                "configuration": {
                    "delta.columnMapping.mode": "id",
                    "delta.columnMapping.maxColumnId": max_column_id.to_string(),
                    "delta.enableChangeDataFeed": change_data_feed.to_string(),
                },
                "createdTime": 0,
            }}),
        ],
    );

    for version in 1..=commits as u64 {
        let actions: Vec<Value> = write_files(version)
            .into_iter()
            .map(|file| {
                let size = std::fs::metadata(table_dir.join(&file.path)).unwrap().len();
                let mut action = json!({
                    "path": file.path,
                    "partitionValues": file.partition_values,
                    "size": size,
                    "modificationTime": version as i64 * 1000,
                    "dataChange": true,
                });
                if !file.deleted_rows.is_empty() {
                    let (inline, size_in_bytes) = inline_deletion_vector(&file.deleted_rows);
                    action["deletionVector"] = json!({
                        "storageType": "i",
                        "pathOrInlineDv": inline,
                        "sizeInBytes": size_in_bytes,
                        "cardinality": file.deleted_rows.len(),
                    });
                }
                match file.change_data {
                    true => json!({"cdc": action}),
                    false => json!({"add": action}),
                }
            })
            .collect();
        write_commit(version, &actions);
    }
}

/// Build a `columnMapping.mode = 'id'` table from `commits` of `T`, one version
/// each, as Unity Catalog Uniform leaves a table an Iceberg writer produced: the
/// data files name their columns logically whereas the log maps every column to a
/// physical `col-<id>` name no file uses.
///
/// Derived from `relation`, so it covers any SQL type. Its nested structs list
/// their children in the opposite order to the data file, as an Iceberg field
/// reorder leaves them.
fn write_id_mapped_table(table_dir: &Path, relation: &Relation, commits: &[&[Value]]) {
    write_id_mapped_table_with(
        table_dir,
        relation,
        IdMappedLayout {
            reverse_nested: true,
            ..IdMappedLayout::default()
        },
        commits,
    )
}

/// As [`write_id_mapped_table`], but with the log's nested children in the data
/// file's order, so a read that ignored their ids would still come out right.
fn write_id_mapped_table_in_file_order(
    table_dir: &Path,
    relation: &Relation,
    commits: &[&[Value]],
) {
    write_id_mapped_table_with(table_dir, relation, IdMappedLayout::default(), commits)
}

/// As [`write_id_mapped_table`], but `partition` names one column Delta keeps in
/// the log instead of the data file, with the value each commit's file carries.
fn write_id_mapped_table_partitioned(
    table_dir: &Path,
    relation: &Relation,
    partition: (&str, &[&str]),
    commits: &[&[Value]],
) {
    write_id_mapped_table_with(
        table_dir,
        relation,
        IdMappedLayout {
            partition: Some(partition),
            reverse_nested: true,
            ..IdMappedLayout::default()
        },
        commits,
    )
}

/// As [`write_id_mapped_table`], but the data files name their columns
/// physically and carry no field ids, as Delta itself writes a mapped table.
fn write_id_mapped_table_physical_names(
    table_dir: &Path,
    relation: &Relation,
    commits: &[&[Value]],
) {
    write_id_mapped_table_with(
        table_dir,
        relation,
        IdMappedLayout {
            physical_file_names: true,
            ..IdMappedLayout::default()
        },
        commits,
    )
}

/// How [`write_id_mapped_table_with`] lays out a `mode = 'id'` table.
#[derive(Default, Clone, Copy)]
struct IdMappedLayout<'a> {
    /// Column Delta keeps in the log rather than the data file, with the value
    /// each commit's file carries.
    partition: Option<(&'a str, &'a [&'a str])>,
    /// List the log's nested struct children in the opposite order to the data
    /// file, as an Iceberg field reorder leaves them.
    reverse_nested: bool,
    /// Name the data file's columns the way the log does, as Delta writes them;
    /// an Iceberg writer leaves the logical names instead. Field ids are kept
    /// either way, as both writers stamp them.
    physical_file_names: bool,
}

/// The Arrow schema of a `mode = 'id'` table, field ids stamped, paired with the
/// Delta `schemaString` its log declares.
fn id_mapped_schema(relation: &Relation, reverse_nested: bool) -> (Arc<ArrowSchema>, String) {
    let mut next_id = 100;
    let fields: Vec<arrow::datatypes::Field> =
        relation_to_arrow_fields(&relation.fields, delta_schema_options())
            .iter()
            .map(|field| stamp_field_ids(field, &mut next_id))
            .collect();
    let table_schema = Arc::new(ArrowSchema::new(fields));

    // The Arrow conversion sees a Parquet variant as the struct of two binary
    // buffers it is encoded as; column mapping would then demand a physical name
    // for each, which a real table never has.
    let struct_type = StructType::try_from_arrow(table_schema.as_ref())
        .unwrap()
        .fields()
        .zip(&relation.fields)
        .map(|(field, column)| {
            StructField::new(
                field.name(),
                delta_variant_types(&column.columntype, field.data_type().clone()).unwrap(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone())
        })
        .collect::<Vec<_>>();
    let mut schema = serde_json::to_value(StructType::try_new(struct_type).unwrap()).unwrap();
    to_column_mapping_metadata(&mut schema);
    if reverse_nested {
        reverse_nested_struct_fields(&mut schema);
    }
    (table_schema, schema.to_string())
}

/// Returns `rows`, rows of `relation`, as a batch of `table_schema`: the form of the Delta
/// table, with the field ids that `table_schema` stamps.
fn id_mapped_batch(relation: &Relation, table_schema: &ArrowSchema, rows: &[Value]) -> RecordBatch {
    let batch = delta::table_batch(relation, updates(relation, rows).rows()).unwrap();
    let columns = batch
        .columns()
        .iter()
        .zip(table_schema.fields())
        .map(|(column, field)| cast(column, field.data_type()).unwrap())
        .collect();
    RecordBatch::try_new(Arc::new(table_schema.clone()), columns).unwrap()
}

/// Write `rows` as one data file of an id-mapped table.
///
/// `stored` picks the columns the file carries, leaving out any partition column
/// Delta keeps in the log. `physical_file_names` picks the writer: Delta names a
/// column as the log does, an Iceberg writer names it logically.
#[allow(clippy::too_many_arguments)]
fn write_id_mapped_data_file(
    table_dir: &Path,
    relation: &Relation,
    table_schema: &ArrowSchema,
    stored: &[usize],
    physical_file_names: bool,
    name: &str,
    rows: &[Value],
) {
    let batch = id_mapped_batch(relation, table_schema, rows)
        .project(stored)
        .unwrap();

    let batch = match physical_file_names {
        true => to_physical_batch(&batch),
        false => batch,
    };
    write_parquet(table_dir, name, &batch);
}

/// A `mode = 'id'` table whose single commit holds one file per entry of
/// `files`, the last of them carrying a deletion vector that masks its rows at
/// the indexes `deleted` gives. The log lists nested children in the opposite
/// order to the data file, as [`write_id_mapped_table`] does.
///
/// A file with a deletion vector is read through the object store rather than
/// through a listing, the route an unlistable `uc://` location takes for every
/// file. A commit holding both kinds is what makes a read union the two.
fn write_id_mapped_table_with_deletion_vector(
    table_dir: &Path,
    relation: &Relation,
    files: &[&[Value]],
    deleted: &[u32],
) {
    let (table_schema, schema_string) = id_mapped_schema(relation, true);
    let stored: Vec<usize> = (0..table_schema.fields().len()).collect();

    write_id_mapped_log(table_dir, &schema_string, &[], false, 1, |version| {
        files
            .iter()
            .enumerate()
            .map(|(index, rows)| {
                let name = format!("part-{version:05}-{index}.parquet");
                write_id_mapped_data_file(
                    table_dir,
                    relation,
                    &table_schema,
                    &stored,
                    false,
                    &name,
                    rows,
                );
                IdMappedFile {
                    path: name,
                    partition_values: json!({}),
                    change_data: false,
                    deleted_rows: match index == files.len() - 1 {
                        true => deleted.to_vec(),
                        false => Vec::new(),
                    },
                }
            })
            .collect()
    });
}

fn write_id_mapped_table_with(
    table_dir: &Path,
    relation: &Relation,
    layout: IdMappedLayout,
    commits: &[&[Value]],
) {
    let (table_schema, schema_string) = id_mapped_schema(relation, layout.reverse_nested);

    // A partition column is not stored in the data file, and the log keys its
    // value by physical name.
    let partition_column = layout.partition.map(|(name, _)| name);
    let stored: Vec<usize> = table_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| Some(field.name().as_str()) != partition_column)
        .map(|(index, _)| index)
        .collect();
    let partition_key = partition_column.map(|name| {
        let field = table_schema.field_with_name(name).unwrap();
        format!("col-{}", field.metadata()["PARQUET:field_id"])
    });

    write_id_mapped_log(
        table_dir,
        &schema_string,
        partition_column.as_slice(),
        false,
        commits.len(),
        |version| {
            let rows = commits[version as usize - 1];
            let values = match (&partition_key, layout.partition) {
                (Some(key), Some((_, values))) => json!({key: values[version as usize - 1]}),
                _ => json!({}),
            };

            // Two files per commit where the rows allow it, so a read plans
            // several files and their columns have to line up.
            let per_file = rows.len().div_ceil(2).max(1);
            rows.chunks(per_file)
                .enumerate()
                .map(|(index, chunk)| {
                    let name = format!("part-{version:05}-{index}.parquet");
                    write_id_mapped_data_file(
                        table_dir,
                        relation,
                        &table_schema,
                        &stored,
                        layout.physical_file_names,
                        &name,
                        chunk,
                    );
                    IdMappedFile {
                        path: name,
                        partition_values: values.clone(),
                        change_data: false,
                        deleted_rows: Vec::new(),
                    }
                })
                .collect()
        },
    );
}

/// A `mode = 'id'` table with the Change Data Feed on: commit 1 adds `rows`,
/// commit 2 records `changes` in a `cdc` action with their `_change_type`.
///
/// `physical_file_names` picks the writer: Delta's names or Iceberg's.
fn write_id_mapped_change_data_table(
    table_dir: &Path,
    relation: &Relation,
    rows: &[Value],
    changes: &[(Value, &str)],
    physical_file_names: bool,
) {
    let (table_schema, schema_string) = id_mapped_schema(relation, false);

    let to_batch = |rows: Vec<Value>| {
        let batch = id_mapped_batch(relation, &table_schema, &rows);
        match physical_file_names {
            true => to_physical_batch(&batch),
            false => batch,
        }
    };

    write_id_mapped_log(
        table_dir,
        &schema_string,
        &[],
        true,
        2,
        |version| match version {
            1 => {
                let batch = to_batch(rows.to_vec());
                let name = "part-00001-0.parquet".to_string();
                write_parquet(table_dir, &name, &batch);
                vec![IdMappedFile {
                    path: name,
                    partition_values: json!({}),
                    change_data: false,
                    deleted_rows: Vec::new(),
                }]
            }
            _ => {
                let batch = to_batch(changes.iter().map(|(row, _)| row.clone()).collect());
                // Delta does not map `_change_type`: it is named the same in
                // every file, whatever the table's column mapping.
                let mut fields = batch.schema().fields().to_vec();
                fields.push(Arc::new(arrow::datatypes::Field::new(
                    "_change_type",
                    ArrowDataType::Utf8,
                    true,
                )));
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(arrow::array::StringArray::from(
                    changes.iter().map(|(_, kind)| *kind).collect::<Vec<_>>(),
                )));
                let batch =
                    arrow::array::RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
                        .unwrap();

                let name = "_change_data/cdc-00002-0.parquet".to_string();
                write_parquet(table_dir, &name, &batch);
                vec![IdMappedFile {
                    path: name,
                    partition_values: json!({}),
                    change_data: true,
                    deleted_rows: Vec::new(),
                }]
            }
        },
    );
}

/// A `mode = 'id'` file named the log's way, which is how Delta writes one,
/// reads correctly. It needs no field-id projection, so it must keep the
/// listing and the row-group pruning that comes with it.
#[engine_test]
pub fn follow_id_mapped_physical_names_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        let rows = [
            uniform_row(1, "alpha", "Coffee Shop", "settled"),
            uniform_row(2, "beta", "Gas Station", "pending"),
        ];
        write_id_mapped_table_physical_names(table_dir.path(), &uniform_relation(), &[&rows]);
        let table_uri = table_dir.path().display().to_string();

        let read = off_runtime(move || {
            read_uniform_table(engine, &table_uri, 1, &[("mode", json!("follow"))])
        });

        assert_eq!(
            read,
            rows_zset(&uniform_relation(), &rows),
            "a physically named file must match by name, nested children included"
        );
    })
}

/// A commit's change data file is read, and its columns resolved, under either
/// writer's naming. These files also carry `_change_type`, which no column
/// mapping covers.
async fn run_id_mapped_change_data_test(
    engine: &dyn PipelineTestEngine,
    physical_file_names: bool,
) {
    let table_dir = TempDir::new().unwrap();
    let rows = [
        uniform_cdc_row(1, "alpha", "Coffee Shop", "settled", "c", 1),
        uniform_cdc_row(2, "beta", "Gas Station", "pending", "c", 2),
    ];
    // The change data commit inserts a row and deletes row 1.
    let changes = [
        (
            uniform_cdc_row(3, "gamma", "Book Store", "settled", "c", 3),
            "insert",
        ),
        (
            uniform_cdc_row(1, "alpha", "Coffee Shop", "settled", "d", 4),
            "delete",
        ),
    ];
    write_id_mapped_change_data_table(
        table_dir.path(),
        &uniform_cdc_relation(),
        &rows,
        &changes,
        physical_file_names,
    );
    let table_uri = table_dir.path().display().to_string();

    let (read, change_data_commits) = off_runtime(move || {
        let mut commits = 0;
        let read = read_id_mapped_table_counting(
            engine,
            &table_uri,
            &uniform_relation(),
            2,
            &[("mode", json!("follow")), ("change_feed", json!("require"))],
            &mut commits,
        );
        (read, commits)
    });

    assert_eq!(
        change_data_commits, 1,
        "the second commit must be read from its change data file, not its file actions"
    );
    assert_eq!(
        read,
        rows_zset(
            &uniform_relation(),
            &[
                uniform_row(2, "beta", "Gas Station", "pending"),
                uniform_row(3, "gamma", "Book Store", "settled"),
            ]
        ),
        "the insert must arrive and the delete must cancel row 1, which happens only if \
         both commits resolved their columns alike"
    );
}

/// Delta names a change data file's columns the way the log does.
#[engine_test]
pub fn follow_id_mapped_change_data_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        run_id_mapped_change_data_test(engine, true).await;
    })
}

/// Iceberg leaves the logical names, so the read resolves them by field id.
#[engine_test]
pub fn follow_id_mapped_change_data_logical_names_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        run_id_mapped_change_data_test(engine, false).await;
    })
}

/// A NULL or empty nested value survives a `mode = 'id'` read.
///
/// `realign_container` rebuilds a list or a map by hand, carrying the
/// container's own null buffer and offsets across; dropping either shows up
/// only on a row whose nested columns are NULL or empty.
#[engine_test]
pub fn follow_id_mapped_nulls_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        let null_nested = json!({ "merchant": null, "status": null });
        let rows = [
            uniform_row(1, "alpha", "Coffee Shop", "settled"),
            // Every nested column NULL.
            json!({ "id": 2, "label": null, "after": null, "history": null, "tags": null }),
            // Present but empty, and a struct whose own children are NULL.
            json!({
                "id": 3,
                "label": "gamma",
                "after": null_nested.clone(),
                "history": [],
                "tags": {},
            }),
            // A NULL-valued element inside each container.
            json!({
                "id": 4,
                "label": "delta",
                "after": null,
                "history": [null_nested.clone()],
                "tags": { "k": null_nested },
            }),
        ];
        write_id_mapped_table(table_dir.path(), &uniform_relation(), &[&rows]);
        let table_uri = table_dir.path().display().to_string();

        let read = off_runtime(move || {
            read_uniform_table(engine, &table_uri, 1, &[("mode", json!("follow"))])
        });

        assert_eq!(
            read,
            rows_zset(&uniform_relation(), &rows),
            "a NULL or empty nested value must survive the field-id projection"
        );
    })
}

/// Follow mode resolves a Uniform file's columns by Parquet field id.
///
/// The file names its columns logically whereas the log maps them to `col-<id>`,
/// so matching by name finds nothing and null-fills every column, silently.
/// Snapshot mode resolves this inside delta-rs; follow reads the file itself.
#[engine_test]
pub fn follow_uniform_field_id_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        write_id_mapped_table(
            table_dir.path(),
            &uniform_cdc_relation(),
            &[&[
                uniform_cdc_row(1, "alpha", "Coffee Shop", "settled", "c", 1),
                uniform_cdc_row(2, "beta", "Gas Station", "pending", "c", 2),
            ]],
        );
        let table_uri = table_dir.path().display().to_string();

        let rows = off_runtime(move || {
            read_uniform_table(engine, &table_uri, 1, &[("mode", json!("follow"))])
        });

        assert_eq!(
            rows,
            rows_zset(
                &uniform_relation(),
                &[
                    uniform_row(1, "alpha", "Coffee Shop", "settled"),
                    uniform_row(2, "beta", "Gas Station", "pending")
                ]
            ),
            "follow mode must resolve the file's logically-named columns by field id, \
         inside the nested struct as well as at the top level"
        );
    })
}

/// `filter` still selects rows on the direct path, including on a column the
/// SQL table does not declare, which the connector reads for the filter alone.
#[engine_test]
pub fn follow_id_mapped_filter_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        write_id_mapped_table(
            table_dir.path(),
            &uniform_cdc_relation(),
            &[&[
                uniform_cdc_row(1, "alpha", "Coffee Shop", "settled", "c", 1),
                uniform_cdc_row(2, "beta", "Gas Station", "pending", "c", 2),
                uniform_cdc_row(3, "gamma", "Book Store", "settled", "c", 3),
            ]],
        );
        let table_uri = table_dir.path().display().to_string();

        let rows = off_runtime(move || {
            read_uniform_table(
                engine,
                &table_uri,
                1,
                &[("mode", json!("follow")), ("filter", json!("ts > 1"))],
            )
        });

        assert_eq!(
            rows,
            rows_zset(
                &uniform_relation(),
                &[
                    uniform_row(2, "beta", "Gas Station", "pending"),
                    uniform_row(3, "gamma", "Book Store", "settled"),
                ]
            ),
            "the filter must drop row 1 and keep the rest"
        );
    })
}

/// Every SQL type the connector supports survives a `mode = 'id'` follow read.
///
/// `mode = 'id'` routes every file through `project_to_logical` instead of
/// delta-rs's reader, so every column's Arrow type passes through
/// `realign_array`.
#[engine_test]
pub fn follow_id_mapped_all_types_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        let rows: Vec<Value> = (0..10).map(delta_test_record).collect();
        write_id_mapped_table(table_dir.path(), &relation(), &[&rows]);
        let table_uri = table_dir.path().display().to_string();

        let read = off_runtime(move || {
            read_id_mapped_table(
                engine,
                &table_uri,
                &relation(),
                1,
                &[("mode", json!("follow"))],
            )
        });

        assert_eq!(
            read,
            rows_zset(&relation(), &rows),
            "every column type must survive the field-id projection"
        );
    })
}

/// A partitioned `mode = 'id'` table reads its partition column back.
///
/// Delta stores a partition column's value in the log, never in the data file,
/// and the direct read this mode forces bypasses the `ListingTable` that would
/// otherwise supply it, leaving `add_partition_columns` as the only source.
#[engine_test]
pub fn follow_id_mapped_partitioned_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        // `label` is the partition column, so each commit's rows share its value.
        let first = [uniform_row(1, "alpha", "Coffee Shop", "settled")];
        let second = [uniform_row(2, "beta", "Gas Station", "pending")];
        write_id_mapped_table_partitioned(
            table_dir.path(),
            &uniform_relation(),
            ("label", &["alpha", "beta"]),
            &[&first, &second],
        );
        let table_uri = table_dir.path().display().to_string();

        let read = off_runtime(move || {
            read_uniform_table(engine, &table_uri, 2, &[("mode", json!("follow"))])
        });

        let expected = [first.to_vec(), second.to_vec()].concat();
        assert_eq!(
            read,
            rows_zset(&uniform_relation(), &expected),
            "the partition column must come back from the log, not as NULL"
        );
    })
}

/// Every SQL type survives a `mode = 'id'` CDC read too.
///
/// CDC plans each data file on its own and joins them with `UNION ALL`, so every
/// column's Arrow type meets `realign_array` on a second path, and the files'
/// columns have to line up. No row is a delete, so the read is every row.
#[engine_test]
pub fn cdc_id_mapped_all_types_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        // `string` is the delete flag and the ordering key. The random strings of the rows
        // are not 'deleted', so the read deletes nothing.
        let rows: Vec<Value> = (0..10).map(delta_test_record).collect();
        write_id_mapped_table(table_dir.path(), &relation(), &[&rows[..5], &rows[5..]]);
        let table_uri = table_dir.path().display().to_string();

        let read = off_runtime(move || {
            read_id_mapped_table(
                engine,
                &table_uri,
                &relation(),
                2,
                &[
                    ("mode", json!("cdc")),
                    ("cdc_delete_filter", json!("string = 'deleted'")),
                    ("cdc_order_by", json!("string")),
                ],
            )
        });

        assert_eq!(
            read,
            rows_zset(&relation(), &rows),
            "every column type must survive the CDC field-id projection"
        );
    })
}

/// A CDC read of an id-mapped table whose commit mixes a plain file with one a
/// deletion vector masks rows of.
///
/// A listing reads the first and the object store reads the second, and
/// `cdc_side_dataframe` unions the two frames before the `EXCEPT ALL` that pairs
/// the transaction's adds with its removes. The union lines up only if both
/// frames carry the same columns in the same order, which the listing side
/// reaches by field id on its own. The object-store route is also the one an
/// unlistable `uc://` location takes for every file, which no local table can
/// otherwise reach.
#[engine_test]
pub fn cdc_id_mapped_deletion_vector_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        let listed = [
            uniform_row(1, "alpha", "Coffee Shop", "settled"),
            uniform_row(2, "beta", "Gas Station", "pending"),
        ];
        let masked = [
            uniform_row(3, "gamma", "Book Store", "settled"),
            uniform_row(4, "delta", "Bakery", "reversed"),
        ];
        // The second row of the second file never made it into the table.
        write_id_mapped_table_with_deletion_vector(
            table_dir.path(),
            &uniform_relation(),
            &[&listed, &masked],
            &[1],
        );
        let table_uri = table_dir.path().display().to_string();

        let read = off_runtime(move || {
            read_uniform_table(
                engine,
                &table_uri,
                1,
                &[
                    ("mode", json!("cdc")),
                    ("cdc_delete_filter", json!("label = 'none'")),
                    ("cdc_order_by", json!("id asc")),
                ],
            )
        });

        let expected = [listed.to_vec(), masked[..1].to_vec()].concat();
        assert_eq!(
            read,
            rows_zset(&uniform_relation(), &expected),
            "a CDC read must resolve the columns of a listed file and of one a \
         deletion vector masks the same way, and drop the masked row"
        );
    })
}

/// A snapshot of an id-mapped table reads a struct nested in an `ARRAY` or a
/// `MAP` under the right field names, even after the table reordered them.
///
/// The log's `col-<id>` children had to be relabeled to the file's names before
/// the struct cast paired them, and neither container reached that relabeling:
/// a list matched only when the log and the file agreed on the list flavor, and
/// a map's `entries`, `key` and `value` carry no column mapping id to pair on.
/// Both left every child named `col-<id>`, nothing overlapped the file's names,
/// and the cast fell back to pairing by position, so the two fields came back
/// exchanged with no error. Fixed in the delta-rs fork by `0ae918e3`; #7279.
///
/// `after` is the control: a struct outside a container was always paired by
/// name, so it read correctly even while the other two did not.
#[engine_test]
pub fn snapshot_id_mapped_reordered_nested_fields_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        /// `snapshot` mode rejects `end_version`, so this cannot use
        /// [`read_uniform_table`].
        fn snapshot_read(engine: &dyn PipelineTestEngine, table_uri: &str) -> ZSet {
            let output_file = NamedTempFile::new().unwrap();
            let pipeline = delta_table_input_pipeline(
                engine,
                table_uri,
                &uniform_relation(),
                &HashMap::from([("mode".to_string(), json!("snapshot"))]),
                &output_file.path().display().to_string(),
            );
            pipeline.start();
            wait(|| pipeline.pipeline_complete(), 60_000).expect("timeout ingesting the table");
            pipeline.stop().unwrap();
            distinct(&file_zset(&uniform_relation(), &output_file))
        }

        async fn snapshot_of(engine: &dyn PipelineTestEngine, table_dir: &Path) -> ZSet {
            let uri = table_dir.display().to_string();
            off_runtime(move || snapshot_read(engine, &uri))
        }

        let rows = [uniform_row(1, "alpha", "Coffee Shop", "settled")];
        let schema = uniform_relation();

        // The log lists the nested struct's fields in the opposite order to the
        // data file, which is what a field reorder leaves behind.
        let reordered = TempDir::new().unwrap();
        write_id_mapped_table(reordered.path(), &schema, &[&rows]);
        assert_eq!(
            snapshot_of(engine, reordered.path()).await,
            rows_zset(&schema, &rows),
            "a reordered log must not exchange the fields of the struct inside \
         `history` (an ARRAY) or `tags` (a MAP)"
        );

        // The same rows with the log in the file's order, which never depended on
        // the relabeling and must stay readable.
        let in_order = TempDir::new().unwrap();
        write_id_mapped_table_in_file_order(in_order.path(), &schema, &[&rows]);
        assert_eq!(
            snapshot_of(engine, in_order.path()).await,
            rows_zset(&schema, &rows)
        );
    })
}

/// `variant_encoding` picks how the output connector writes a `VARIANT`
/// column, and the choice must reach the Delta schema: `variant` by default,
/// `string` under the override.
///
/// The values must survive either way. The default is what the rest of these
/// tests exercise; `json_string` is the encoding Feldera wrote before it
/// supported the Delta `variant` type, and nothing else covers it now.
#[engine_test]
pub fn output_variant_encoding_test(engine: &dyn PipelineTestEngine) {
    let data: Vec<Value> = (0..10).map(|i| delta_test_record(i * 2)).collect();

    for (encoding, delta_type) in [(None, r#""variant""#), (Some("json_string"), r#""string""#)] {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let mut config = HashMap::new();
        if let Some(encoding) = encoding {
            config.insert("variant_encoding".to_string(), encoding.to_string());
        }

        delta_table_output_test(engine, data.clone(), &table_uri, &config, true, None, false);

        assert_eq!(
            delta_column_type(&table_uri, "variant"),
            delta_type,
            "unexpected Delta type for encoding {encoding:?}"
        );

        // A `variant` column is only legible to other engines if the table
        // says so in its protocol.
        assert_eq!(
            delta_has_variant_writer_feature(&table_uri),
            encoding.is_none(),
            "unexpected variantType writer feature for encoding {encoding:?}"
        );
    }
}

/// A date statistic Arrow cannot parse must not fail the read.
///
/// The kernel parses `maxValues` when it opens the table, so one row Spark
/// wrote with a long year makes the whole table unreadable.
///
/// arrow-json 58 rejects a 7-digit year; 59 accepts it.
#[engine_test]
pub fn snapshot_out_of_range_date_stats_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        init_logging();

        let relation_schema = relation();
        let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
            &relation_schema.fields,
            delta_schema_options(),
        ));
        let struct_fields: Vec<StructField> = arrow_schema
            .fields
            .iter()
            .map(|f| {
                StructField::new(
                    f.name(),
                    DataType::try_from_arrow(f.data_type()).unwrap(),
                    f.is_nullable(),
                )
            })
            .collect();

        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let table = create_table(&table_uri, &HashMap::new(), &struct_fields).await;
        let records: Vec<Value> = (0..10).map(|i| delta_test_record(i * 2)).collect();
        write_data_to_table(table, &relation(), &records).await;

        // Rewrite the statistic delta-rs wrote with the one Spark leaves behind.
        let log_dir = table_dir.path().join("_delta_log");
        for entry in std::fs::read_dir(&log_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension() != Some(OsStr::new("json")) {
                continue;
            }
            let rewritten: Vec<String> = std::fs::read_to_string(&path)
                .unwrap()
                .lines()
                .map(|line| {
                    let mut action: Value = serde_json::from_str(line).unwrap();
                    if let Some(add) = action.get_mut("add") {
                        add["stats"] = json!({
                            "numRecords": records.len(),
                            "minValues": {"date": "1970-01-01"},
                            "maxValues": {"date": "+2012017-10-26"},
                            "nullCount": {},
                        })
                        .to_string()
                        .into();
                    }
                    action.to_string()
                })
                .collect();
            std::fs::write(&path, rewritten.join("\n") + "\n").unwrap();
        }

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().display().to_string();
        let rows = off_runtime(move || {
            let pipeline = delta_table_input_pipeline(
                engine,
                &table_uri,
                &relation_schema,
                &HashMap::from([("mode".to_string(), json!("snapshot"))]),
                &output_path,
            );
            pipeline.start();
            wait(|| pipeline.pipeline_complete(), 60_000).expect("timeout ingesting the table");
            pipeline.stop().unwrap();
            distinct(&file_zset(&relation(), &output_file))
        });

        assert_eq!(
            rows,
            rows_zset(&relation(), &records),
            "a `maxValues` date Arrow cannot represent must not fail the read"
        );
    })
}

/// CRC-32/ISO-HDLC, the trailer of a deletion-vector file.
///
/// Written out here because the workspace's `crc32c` uses a different
/// polynomial.
#[cfg(test)]
fn crc32_iso_hdlc(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

/// Build one deletion vector file in the layout the Delta protocol specifies,
/// and return it with the `sizeInBytes` its descriptor must carry.
///
/// ```text
/// 1 byte    version (1)
/// 4 bytes   dv_size, big endian      <- the descriptor's offset points here
/// 4 bytes   magic, little endian
/// n bytes   portable Roaring bitmap
/// 4 bytes   CRC-32/ISO-HDLC over magic..=bitmap, big endian
/// ```
#[cfg(test)]
fn deletion_vector_file(deleted_rows: &[u64]) -> (Vec<u8>, i32) {
    /// Delta marks a deletion vector's bitmap with this, in little-endian bytes.
    const PORTABLE_ROARING_MAGIC: u32 = 1681511377;

    let bitmap: RoaringTreemap = deleted_rows.iter().copied().collect();
    let mut bitmap_bytes = Vec::new();
    bitmap.serialize_into(&mut bitmap_bytes).unwrap();

    // `dv_size` covers the magic and the bitmap, but not the CRC.
    let mut checksummed = PORTABLE_ROARING_MAGIC.to_le_bytes().to_vec();
    checksummed.extend_from_slice(&bitmap_bytes);
    let dv_size = checksummed.len() as u32;

    let mut bytes = vec![1u8];
    bytes.extend_from_slice(&dv_size.to_be_bytes());
    bytes.extend_from_slice(&checksummed);
    bytes.extend_from_slice(&crc32_iso_hdlc(&checksummed).to_be_bytes());

    (bytes, dv_size as i32)
}

/// Scan planning must not need a blocking thread per deletion vector.
///
/// Loading a vector held a blocking thread while it waited on the kernel's
/// sync-over-async bridge, which needs a second one. Once the loads reached
/// `max_blocking_threads` the pool deadlocked, with no error. The fork's
/// `943cf48b` fixes it; eight vectors against four threads reproduce it.
/// The scan reports through a channel, so a regression fails here rather than
/// hanging the suite.
#[test]
fn delta_table_deletion_vector_scan_without_blocking_pool_test() {
    use deltalake::kernel::{Action, Add, DeletionVectorDescriptor, PrimitiveType, StorageType};

    const FILES: usize = 8;
    const ROWS_PER_FILE: i64 = 4;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        arrow::datatypes::Field::new("id", ArrowDataType::Int64, false),
        arrow::datatypes::Field::new("val", ArrowDataType::Utf8, true),
    ]));

    // Written by hand: delta-rs reads deletion vectors but cannot write them.
    // Each file hides its second row.
    let mut adds = Vec::with_capacity(FILES);
    for index in 0..FILES {
        let file_name = format!("part-{index:05}.parquet");
        let first_id = index as i64 * ROWS_PER_FILE;
        let ids: Vec<i64> = (first_id..first_id + ROWS_PER_FILE).collect();
        let vals: Vec<String> = ids.iter().map(|id| format!("v{id}")).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(ids)) as Arc<dyn arrow::array::Array>,
                Arc::new(arrow::array::StringArray::from(vals)),
            ],
        )
        .unwrap();
        let path = root.join(&file_name);
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            arrow_schema.clone(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uuid = uuid::Uuid::new_v4();
        let (dv_bytes, size_in_bytes) = deletion_vector_file(&[1]);
        std::fs::write(root.join(format!("deletion_vector_{uuid}.bin")), dv_bytes).unwrap();

        adds.push(Add {
            path: file_name,
            size: std::fs::metadata(&path).unwrap().len() as i64,
            modification_time: 1_700_000_000_000,
            data_change: true,
            stats: Some(json!({ "numRecords": ROWS_PER_FILE }).to_string()),
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: StorageType::UuidRelativePath,
                path_or_inline_dv: z85_encode(uuid.as_bytes()),
                offset: Some(1),
                size_in_bytes,
                cardinality: 1,
            }),
            ..Default::default()
        });
    }

    let table_uri = root.to_str().unwrap().to_string();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        CreateBuilder::new()
            .with_location(&table_uri)
            .with_columns([
                StructField::new("id", DataType::Primitive(PrimitiveType::Long), false),
                StructField::new("val", DataType::Primitive(PrimitiveType::String), true),
            ])
            .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
            .with_actions(adds.into_iter().map(Action::Add))
            .await
            .unwrap();
    });

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        // Snapshot loading blocks a pooled thread and then waits on the same
        // pool, so four leaves room for that but not for eight vectors.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let rows = runtime.block_on(async {
            let table: DeltaTable =
                DeltaTableBuilder::from_url(ensure_table_uri(&table_uri).unwrap())
                    .unwrap()
                    .load()
                    .await
                    .unwrap();
            let ctx = SessionContext::new();
            let log_store = table.log_store();
            ctx.runtime_env()
                .register_object_store(log_store.root_url(), log_store.root_object_store());
            ctx.register_table("snapshot", table.table_provider().await.unwrap())
                .unwrap();
            ctx.sql("select \"id\" from snapshot")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>()
        });
        let _ = tx.send(rows);
        // Dropping a runtime joins its blocking threads. A regression leaves
        // one wedged, which would hang the suite's exit instead of failing.
        forget(runtime);
    });

    let rows = rx
        .recv_timeout(Duration::from_secs(120))
        .expect("scan planning wedged: the deletion vectors exhausted the blocking pool");
    assert_eq!(
        rows,
        FILES * (ROWS_PER_FILE as usize - 1),
        "every file's second row is deleted by its vector"
    );
}

/// A reader feature the kernel implements but delta-rs does not list must not
/// make a table unreadable.
///
/// Delta 3.x stamps `vacuumProtocolCheck` on ordinary tables, and delta-rs
/// rejects any reader feature it does not list. The fork's `7f8bb445`
/// downgrades that rejection to a warning.
#[engine_test]
pub fn snapshot_unlisted_reader_feature_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        init_logging();

        let relation_schema = relation();
        let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
            &relation_schema.fields,
            delta_schema_options(),
        ));
        let struct_fields: Vec<StructField> = arrow_schema
            .fields
            .iter()
            .map(|f| {
                StructField::new(
                    f.name(),
                    DataType::try_from_arrow(f.data_type()).unwrap(),
                    f.is_nullable(),
                )
            })
            .collect();

        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let table = create_table(&table_uri, &HashMap::new(), &struct_fields).await;

        let records: Vec<Value> = (0..10).map(|i| delta_test_record(i * 2)).collect();
        write_data_to_table(table, &relation(), &records).await;

        // Stamped last: delta-rs refuses to write through a protocol it cannot
        // read.
        stamp_protocol_features(table_dir.path(), &["vacuumProtocolCheck"]);

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().display().to_string();
        let rows = off_runtime(move || {
            let pipeline = delta_table_input_pipeline(
                engine,
                &table_uri,
                &relation_schema,
                &HashMap::from([("mode".to_string(), json!("snapshot"))]),
                &output_path,
            );
            pipeline.start();
            wait(|| pipeline.pipeline_complete(), 60_000).expect("timeout ingesting the table");
            pipeline.stop().unwrap();
            distinct(&file_zset(&relation(), &output_file))
        });

        assert_eq!(
            rows,
            rows_zset(&relation(), &records),
            "a table declaring a reader feature delta-rs does not list must still \
         read; without the fork patch delta-rs refuses the whole table"
        );
    })
}

/// Commits a protocol action that declares `features` as reader and writer features.
fn stamp_protocol_features(table_dir: &Path, features: &[&str]) {
    let log_dir = table_dir.join("_delta_log");
    let next_version = std::fs::read_dir(&log_dir)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            (path.extension()? == "json")
                .then(|| path.file_stem()?.to_str()?.parse::<u64>().ok())
                .flatten()
        })
        .max()
        .unwrap()
        + 1;
    // Renamed into place, as a real commit is atomic: a follower must never read a half-written file.
    let tmp = log_dir.join(format!(".{next_version:020}.json.tmp"));
    let mut commit = File::create(&tmp).unwrap();
    writeln!(
        commit,
        "{}",
        json!({"protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": features,
            "writerFeatures": features,
        }})
    )
    .unwrap();
    drop(commit);
    std::fs::rename(&tmp, log_dir.join(format!("{next_version:020}.json"))).unwrap();
}

/// ```text
/// input.json --> [pipeline1]--->delta_table-->[pipeline2]-->output.json
/// ```
#[engine_test]
pub fn file_output_proptest(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(1), |(data in delta_data(20_000))| {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // Uncomment to inspect output parquet files produced by the test.
    forget(table_dir);

    delta_table_output_test(engine, data.clone(), &table_uri, &HashMap::new(), true, None, false);

    // Read delta table unordered.
    let json_file = delta_table_snapshot_to_json(engine,
        &table_uri,
        &relation(),
        &HashMap::new());

    let expected_zset = rows_zset(&relation(), &data);
    let zset = file_zset(&relation(), &json_file);
    assert_eq!(zset, expected_zset);

    // The same read done serially, in many small batches. Not
    // `max_concurrent_readers`: it is process-global and never reset, so a
    // test that sets it changes every later test in the same binary.
    let json_file_serial = delta_table_snapshot_to_json_values(engine,
        &table_uri,
        &relation(),
        &HashMap::from([
            ("num_parsers".to_string(), Value::from(1)),
            ("scan_parallelism".to_string(), Value::from(1)),
            ("batch_size".to_string(), Value::from(100)),
        ]));

    let zset = file_zset(&relation(), &json_file_serial);
    assert_eq!(zset, expected_zset, "a serial read must return the same data");

    // Order delta table by `bigint` (which should be its natural order).
    let json_file_ordered_by_id = delta_table_snapshot_to_json(engine,
        &table_uri,
        &lateness_relation(),
        &HashMap::from([("timestamp_column".to_string(), "bigint".to_string())]));

    let zset = file_zset(&relation(), &json_file_ordered_by_id);
    assert_eq!(zset, expected_zset);

    // Same as above, but commit a transaction after each input chunk.
    let json_file_ordered_by_id = delta_table_snapshot_to_json(engine,
        &table_uri,
        &lateness_relation(),
        &HashMap::from([("timestamp_column".to_string(), "bigint".to_string()), ("transaction_mode".to_string(), "always".to_string())]));

    let zset = file_zset(&relation(), &json_file_ordered_by_id);
    assert_eq!(zset, expected_zset);

    // Order delta table by `bigint`, specify range in two different ways: using `snapshot_filter` and using `filter`.
    let json_file_ordered_filtered1 = delta_table_snapshot_to_json(engine,
        &table_uri,
        &lateness_relation(),
        &HashMap::from([("timestamp_column".to_string(), "bigint".to_string()), ("snapshot_filter".to_string(), "bigint >= 10000 ".to_string())]));

    let json_file_ordered_filtered2 = delta_table_snapshot_to_json(engine,
        &table_uri,
        &lateness_relation(),
        &HashMap::from([("timestamp_column".to_string(), "bigint".to_string()), ("filter".to_string(), "bigint >= 10000 ".to_string())]));

        let json_file_ordered_filtered3 = delta_table_snapshot_to_json(engine,
            &table_uri,
            &lateness_relation(),
            &HashMap::from([("timestamp_column".to_string(), "bigint".to_string()), ("filter".to_string(), "bigint >= 10000 ".to_string()), ("transaction_mode".to_string(), "always".to_string())]));

    let expected_filtered_zset = rows_zset(&relation(), &data.clone().into_iter().filter(|x| bigint(x) >= 10000).collect::<Vec<_>>());

    let zset = file_zset(&relation(), &json_file_ordered_filtered1);
    assert_eq!(zset, expected_filtered_zset);

    let zset = file_zset(&relation(), &json_file_ordered_filtered2);
    assert_eq!(zset, expected_filtered_zset);

    let zset = file_zset(&relation(), &json_file_ordered_filtered3);
    assert_eq!(zset, expected_filtered_zset);

    // Specify both `snapshot_filter` and `filter`.
    let json_file_filtered = delta_table_snapshot_to_json(engine,
        &table_uri,
        &lateness_relation(),
        &HashMap::from([("filter".to_string(), "bigint >= 5000 ".to_string()), ("snapshot_filter".to_string(), "bigint <= 10000 ".to_string())]));

    let expected_filtered_zset = rows_zset(&relation(), &data.clone().into_iter().filter(|x| bigint(x) >= 5000 && bigint(x) <= 10000).collect::<Vec<_>>());

    let zset = file_zset(&relation(), &json_file_filtered);
    assert_eq!(zset, expected_filtered_zset);

    // Order delta table by `timestamp`, in 1000-day ranges.
    let json_file_ordered_by_ts = delta_table_snapshot_to_json(engine,
        &table_uri,
        &coarse_lateness_relation(),
        &HashMap::from([("timestamp_column".to_string(), "timestamp_ntz".to_string())]));

    let zset = file_zset(&relation(), &json_file_ordered_by_ts);
    assert_eq!(zset, expected_zset);

    // Order delta table by `timestamp`; specify an empty filter condition
    let json_file_ordered_by_ts = delta_table_snapshot_to_json(engine,
        &table_uri,
        &coarse_lateness_relation(),
        &HashMap::from([("timestamp_column".to_string(), "timestamp_ntz".to_string()), ("snapshot_filter".to_string(), "timestamp_ntz < timestamp '1970-01-01T00:00:00'".to_string())]));

    let zset = file_zset(&relation(), &json_file_ordered_by_ts);
    assert!(zset.is_empty(), "no row is older than 1970: {zset:?}");

    // Filter delta table by id
    let json_file_filtered_by_id = delta_table_snapshot_to_json(engine,
        &table_uri,
        &relation(),
        &HashMap::from([("snapshot_filter".to_string(), "bigint >= 10000 ".to_string())]));

    let expected_filtered_zset = rows_zset(&relation(), &data.clone().into_iter().filter(|x| bigint(x) >= 10000).collect::<Vec<_>>());

    let zset = file_zset(&relation(), &json_file_filtered_by_id);
    assert_eq!(zset, expected_filtered_zset);

    // Filter delta table by timestamp.
    let json_file_filtered_by_ts = delta_table_snapshot_to_json(engine,
        &table_uri,
        &relation(),
        &HashMap::from([("snapshot_filter".to_string(), "timestamp_ntz >= '2005-01-01 00:00:00'".to_string())]));

    let start = NaiveDate::from_ymd_opt(2005, 1, 1)
            .unwrap()
            .and_hms_milli_opt(0, 0, 0, 0)
            .unwrap();

    let expected_filtered_zset = rows_zset(&relation(), &data.into_iter().filter(|x| timestamp_millis(x) >= start.and_utc().timestamp_millis()).collect::<Vec<_>>());

    let zset = file_zset(&relation(), &json_file_filtered_by_ts);
    assert_eq!(zset, expected_filtered_zset);


    // // Uncomment to inspect one of the output json files produced by the test.
    // forget(json_file_filtered_by_id);
    });
}

/// ```text
/// input.json --> [pipeline1]--->delta_table
/// ```
#[engine_test]
pub fn file_output_proptest_index_alias(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(1), |(data in delta_data(20_000))| {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // Uncomment to inspect output parquet files produced by the test.
    forget(table_dir);

    delta_table_output_test(engine, data.clone(), &table_uri, &HashMap::new(), true, None, true);

    // forget(json_file_filtered_by_id);
    });
}

/// A table that enables VARIANT shredding is refused: delta-rs would read only the unshredded
/// part of each value and drop the shredded fields without an error.
#[engine_test]
pub fn variant_shredding_is_refused_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let relation_schema = relation();
        let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
            &relation_schema.fields,
            delta_schema_options(),
        ));
        let struct_fields: Vec<StructField> = arrow_schema
            .fields
            .iter()
            .map(|f| {
                StructField::new(
                    f.name(),
                    DataType::try_from_arrow(f.data_type()).unwrap(),
                    f.is_nullable(),
                )
            })
            .collect();

        for feature in ["variantShredding-preview", "variantShredding"] {
            let table_dir = TempDir::new().unwrap();
            let table_uri = table_dir.path().display().to_string();
            let table = create_table(&table_uri, &HashMap::new(), &struct_fields).await;
            write_data_to_table(table, &relation(), &[delta_test_record(0)]).await;
            stamp_protocol_features(table_dir.path(), &["variantType", feature]);

            let output_file = NamedTempFile::new().unwrap();
            let output_path = output_file.path().display().to_string();
            let schema = relation_schema.clone();
            let error = off_runtime(move || {
                try_delta_table_input_pipeline(
                    engine,
                    &table_uri,
                    &schema,
                    &HashMap::from([("mode".to_string(), json!("snapshot"))]),
                    &output_path,
                    Box::new(|e, _| panic!("unexpected error: {e}")),
                )
                .err()
                .expect("a table with VARIANT shredding must be refused")
                .to_string()
            });
            assert!(error.contains(feature), "{error}");
        }
    })
}

/// Delta keeps a partition column's value in the file path and the log, never in
/// the data file, so follow mode must reconstruct it the way the snapshot read
/// (which delegates to delta-rs) already does.
#[engine_test(requires(Storage))]
pub fn follow_partition_column_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        // `s` is the partition column. Its values need escaping in the file path, so
        // this also covers the encoding round trip: Delta escapes the value into the
        // directory name and the log stores that name URL-encoded again.
        let rows: Vec<Value> = (0..6u32)
            .map(|id| {
                json!({ "id": id, "b": false, "i": null, "s": if id.is_multiple_of(2) {
                    "us east"
                } else {
                    "us/west"
                }
                .to_string() })
            })
            .collect();

        run_follow_partition_test(engine, &basic_relation(), &rows, &["s"]).await;
    })
}

/// A partition value reaches the connector as the string Delta wrote into the
/// path, so every SQL type Delta can partition on must survive the cast back.
#[engine_test(requires(Storage))]
pub fn follow_partition_column_types_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        // Every scalar type in `relation` except `binary`: delta-rs writes
        // such a partition value as literal `\uXXXX` text (the byte 0xFE becomes
        // six characters), which does not read back as the original bytes.
        // `unused` is nullable and covers the NULL partition value.
        const PARTITION_COLUMNS: &[&str] = &[
            "bigint",
            "boolean",
            "date",
            "decimal_10_3",
            "double",
            "float",
            "int",
            "smallint",
            "string",
            "timestamp_ntz",
            "tinyint",
            "unused",
            "uuid",
        ];

        let mut rows: Vec<Value> = (0..2).map(delta_test_record).collect();
        rows[0]["unused"] = Value::Null;
        rows[1]["unused"] = Value::from("present");

        run_follow_partition_test(engine, &relation(), &rows, PARTITION_COLUMNS).await;
    })
}

/// Follow a partitioned table whose column names are not lower case.
///
/// A partitioned read names every other column in a projection, and a name read
/// as SQL rather than taken verbatim would arrive lowercased, matching no column
/// of such a table.
#[engine_test(requires(Storage))]
pub fn follow_partition_uppercase_columns_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let rows: Vec<Value> = (0..6)
            .map(|id| json!({ "ID": id, "PAYLOAD": format!("row-{id}"), "REGION": if id % 2 == 0 { "EAST" } else { "WEST" }.to_string() }))
            .collect();

        run_follow_partition_test(engine, &upper_case_relation(), &rows, &["REGION"]).await;
    })
}

/// Follow the same uppercase partitioned table through a SQL table that declares
/// its columns unquoted.
///
/// The connector matches the two schemas ignoring case, so the SQL side resolves
/// whatever it is spelled; the projection still has to name each column the way
/// the Delta table does.
#[engine_test(requires(Storage))]
pub fn follow_partition_unquoted_sql_columns_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let written_rows: Vec<Value> = (0..6)
            .map(|id| json!({ "ID": id, "PAYLOAD": format!("row-{id}"), "REGION": if id % 2 == 0 { "EAST" } else { "WEST" }.to_string() }))
            .collect();
        let expected_rows: Vec<Value> = written_rows
            .iter()
            .map(|row| json!({ "id": row["ID"], "payload": row["PAYLOAD"], "region": row["REGION"] }))
            .collect();

        run_follow_partition_test_on(
            engine,
            &upper_case_relation(),
            &unquoted_relation(),
            &written_rows,
            &expected_rows,
            &["REGION"],
        )
        .await;
    })
}

/// An ordered snapshot read of a table whose column names are case sensitive.
///
/// `timestamp_column` holds an SQL identifier rather than a bare column name,
/// so a case-sensitive column has to be quoted. Unquoted, it folds to lower
/// case and the connector rejects it at startup as missing from the table.
#[engine_test(requires(Storage))]
pub fn snapshot_uppercase_timestamp_column_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        init_logging();

        // An ordered read needs LATENESS on the timestamp column.
        let relation = upper_case_lateness_relation();
        let rows: Vec<Value> = (0..6)
            .map(|id| json!({ "ID": id, "PAYLOAD": format!("row-{id}"), "REGION": "EAST".to_string() }))
            .collect();

        let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
            &relation.fields,
            delta_schema_options(),
        ));
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let table = create_table_from_arrow(&table_uri, &arrow_schema, &[]).await;
        write_data_to_table(table, &relation, &rows).await;

        let storage_dir = TempDir::new().unwrap();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let pipeline = delta_input_controller(
            engine,
            &table_uri,
            json!({ "mode": "snapshot", "timestamp_column": "\"ID\"" }),
            &relation,
            storage_dir.path(),
            &errors,
        )
        .await
        .unwrap();
        pipeline.start();

        wait_or_connector_error(
            &pipeline,
            &SqlIdentifier::from("test_output1"),
            &relation,
            &rows,
            &errors,
        )
        .await;
        pipeline.stop().unwrap();
    })
}

/// A commit that turns VARIANT shredding on while the connector follows the table stops it.
#[engine_test]
pub fn variant_shredding_turned_on_while_following_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let relation_schema = relation();
        let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
            &relation_schema.fields,
            delta_schema_options(),
        ));
        let struct_fields: Vec<StructField> = arrow_schema
            .fields
            .iter()
            .map(|f| {
                StructField::new(
                    f.name(),
                    DataType::try_from_arrow(f.data_type()).unwrap(),
                    f.is_nullable(),
                )
            })
            .collect();

        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let table = create_table(&table_uri, &HashMap::new(), &struct_fields).await;
        write_data_to_table(table, &relation(), &[delta_test_record(0)]).await;

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().display().to_string();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let errors_clone = errors.clone();
        let table_path = table_dir.path().to_path_buf();
        off_runtime(move || {
            // Start-up opens the table, so the feature below arrives through the follow path.
            let pipeline = try_delta_table_input_pipeline(
                engine,
                &table_uri,
                &relation_schema,
                &HashMap::from([("mode".to_string(), json!("follow"))]),
                &output_path,
                Box::new(move |e, _| errors_clone.lock().unwrap().push(e.to_string())),
            )
            .unwrap();
            pipeline.start();

            stamp_protocol_features(&table_path, &["variantType", "variantShredding"]);
            wait(
                || {
                    errors
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|e| e.contains("variantShredding"))
                },
                60_000,
            )
            .unwrap_or_else(|_| {
                panic!(
                    "expected a VARIANT shredding error, got {:?}",
                    errors.lock().unwrap()
                )
            });
            pipeline.stop().unwrap();
        });
    })
}

/// The two readers a `snapshot_and_follow` read uses must agree.
///
/// The snapshot half goes through delta-rs and the follow half through
/// `project_to_logical`; #7076 is what happens when the two disagree. The
/// snapshot covers version 1 and the follow covers version 2, so a
/// disagreement shows up as one commit's rows missing or nulled.
///
/// [`uniform_relation`] rather than [`relation`] because delta-rs cannot
/// read a `VARIANT` column of a column-mapped table at all: it reads the binary
/// fields a variant is stored in as `BinaryView`, which its own schema rejects.
///
/// The follow half also pins `collect_statistics = false`: with statistics on,
/// DataFusion pairs them to the wrong field and the follow reads all NULL.
#[engine_test]
pub fn snapshot_and_follow_id_mapped_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        let table_dir = TempDir::new().unwrap();
        let snapshot = [
            uniform_row(1, "alpha", "Coffee Shop", "settled"),
            uniform_row(2, "beta", "Gas Station", "pending"),
        ];
        let followed = [uniform_row(3, "gamma", "Book Store", "settled")];
        write_id_mapped_table_in_file_order(
            table_dir.path(),
            &uniform_relation(),
            &[&snapshot, &followed],
        );
        let table_uri = table_dir.path().display().to_string();

        let read = off_runtime(move || {
            read_uniform_table(
                engine,
                &table_uri,
                2,
                &[
                    ("mode", json!("snapshot_and_follow")),
                    ("version", json!(1)),
                ],
            )
        });

        let expected = [snapshot.to_vec(), followed.to_vec()].concat();
        assert_eq!(
            read,
            rows_zset(&uniform_relation(), &expected),
            "the snapshot of version 1 and the follow of version 2 must resolve \
         the same columns"
        );
    })
}

/// Read a Unity Catalog table's change feed over a keyed relation whose rows all
/// move between partitions, and check that each row arrives once, at its new key.
///
/// Each row starts in partition `b<nn>` and the merge moves it to `a<nn>`. The two
/// images of a row therefore land in different change data files.
///
/// NOTE: this test requires a `uc://` table, which exercises the code path that
/// reads files through the object store rather than via a `ListingTable`.
///
/// `python/tests/platform/fixtures/unity_change_feed.py` builds the table; the
/// test is inert until `DELTA_TABLE_TEST_UNITY_CDF_TABLE` names it.
#[cfg(feature = "delta-unity-test")]
#[engine_test(serial(delta_s3), requires(Storage))]
pub fn unity_change_feed(engine: &dyn PipelineTestEngine) {
    let Ok(table_uri) = std::env::var("DELTA_TABLE_TEST_UNITY_CDF_TABLE") else {
        println!("DELTA_TABLE_TEST_UNITY_CDF_TABLE is unset; skipping");
        return;
    };

    init_logging();

    // Matches `unity_change_feed.py`: v0 creates, v1 inserts ROWS rows in
    // partitions `b<nn>`, v2 merges every one into `a<nn>`.
    const ROWS: u32 = 40;
    const CREATE_VERSION: i64 = 0;
    const MERGE_VERSION: i64 = 2;

    let mut config: HashMap<String, Value> = [
        // feldera-ci has no OAuth secret; CI mints this token per run via
        // Databricks workload identity federation (see test-adapters.yml),
        // so there is nothing here to rotate or leak.
        (
            "databricks_token",
            std::env::var("DELTA_TABLE_TEST_UNITY_ACCESS_TOKEN").unwrap(),
        ),
        (
            "databricks_host",
            std::env::var("DELTA_TABLE_TEST_UNITY_HOST").unwrap(),
        ),
        // Must be set explicitly (delta-rs #1095), and long enough for a cold
        // read of files the catalog vends credentials for.
        ("aws_region", "us-west-1".to_string()),
        ("timeout", "1000 secs".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), Value::from(v)))
    .collect();
    config.insert("mode".into(), "snapshot_and_follow".into());
    // `require` fails the connector if the table records no change feed, so a
    // misconfigured fixture cannot pass through the file-action path.
    config.insert("change_feed".into(), "require".into());
    config.insert("version".into(), CREATE_VERSION.into());
    config.insert("end_version".into(), MERGE_VERSION.into());

    let storage_dir = TempDir::new().unwrap();
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let errors_clone = errors.clone();
    let pipeline = TOKIO.block_on(async move {
        delta_input_controller_keyed(
            engine,
            &table_uri,
            Value::Object(config.into_iter().collect()),
            &basic_relation(),
            storage_dir.path(),
            &errors_clone,
        )
        .await
    });
    pipeline.start();

    let expected: Vec<Value> = (0..ROWS)
        .map(|id| json!({ "id": id, "b": false, "i": null, "s": format!("a{id:02}") }))
        .collect();
    TOKIO.block_on(async {
        wait_or_connector_error(
            &pipeline,
            &SqlIdentifier::from("test_output1"),
            &basic_relation(),
            &expected,
            &errors,
        )
        .await
    });

    let from_change_data =
        delta_connector_counter(&pipeline, "input_connector_delta_commits_from_change_data");
    pipeline.stop().unwrap();

    // The merge must have been read from the change data it recorded. Reading
    // it from the files the commit rewrote would give the same rows, so only
    // this counter tells the two apart.
    assert_eq!(
        from_change_data, 1,
        "expected the merge at v{MERGE_VERSION} to be read from its change data"
    );
}

// I haven't investigated this in depth but it appears that, when running
// multiple delta connectors in parallel, some of which use unity catalog,
// and others use direct S3 URL to connect to the table, this confuses the
// AWS SDK into using the wrong authentication token in S3-based
// connectors. This causes authentication failures with errors like this:
//
// ```text
// 2025-10-29T17:59:09.906160Z  WARN dbsp_connectors::integrated::delta_table::output: delta_table test_output1: error creating or opening delta table 's3://feldera-delta-table-test/80007986-0e32-466a-ad60-4700013bc56e/' after 1 attempts (retrying in 1000 ms): ObjectStore { source: Generic { store: "S3", source: ListRequest { source: RetryError(RetryErrorImpl { method: GET, uri: Some(https://s3.us-east-2.amazonaws.com/feldera-delta-table-test?list-type=2&prefix=80007986-0e32-466a-ad60-4700013bc56e%2F_delta_log%2F), retries: 0, max_retries: 10, elapsed: 395.553916ms, retry_timeout: 180s, inner: Status { status: 400, body: Some("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InvalidToken</Code><Message>The provided token is malformed or otherwise invalid.</Message><Token-0>...skipped....</Token-0><RequestId>6A1SW4R71MWQFS8H</RequestId><HostId>XOF0VWtYySOJlF0t2R9roAqWM0qH4x+j6cbdkSIVep1l+0PxsAI3tq55nVIcsCj4brHzV9kzmGY=</HostId></Error>") } }) } } }
// ```
//
// Running the unity test sequentially with S3 tests seems to solve this
// reliably.
/// Read the 2M-record dataset over a Unity Catalog path.
///
/// Behind `delta-unity-test`, not `delta-s3-test`: this one needs a Databricks
/// workspace, so it stays out of CI while the rest of the S3 tests run there.
#[cfg(feature = "delta-unity-test")]
#[engine_test(serial(delta_s3))]
pub fn unity_people_2m(engine: &dyn PipelineTestEngine) {
    let object_store_config = [
        // feldera-ci has no OAuth secret; CI mints this token per run via
        // Databricks workload identity federation (see test-adapters.yml),
        // so there is nothing here to rotate or leak.
        (
            "databricks_token".to_string(),
            std::env::var("DELTA_TABLE_TEST_UNITY_ACCESS_TOKEN").unwrap(),
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
    let json_file =
        delta_table_snapshot_to_json(engine, table_uri, &people_relation(), &object_store_config);

    println!("reading output file");
    let zset = file_zset(&people_relation(), &json_file);

    assert_eq!(zset.len(), 2_000_000);

    forget(json_file);
}
