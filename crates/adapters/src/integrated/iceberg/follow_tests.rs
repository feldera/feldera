//! The Iceberg input connector following a table: snapshots that a writer commits before and
//! after the connector starts, start and end snapshots, the transaction modes, a resume from
//! a checkpoint, and the deletes of a copy-on-write overwrite.
//!
//! These need a REST catalog and an S3 store that both the writer (pyiceberg)
//! and the connector (iceberg-rust) can reach. Defaults target the local docker
//! setup in crates/iceberg/src/test/README.md; override via FELDERA_ICEBERG_*.
//! The cases run with the `iceberg-tests-follow` feature.

use crate::test::engine_test;
use std::path::Path;

use feldera_adapterlib::test::{ZSet, wait};
use feldera_types::adapter_stats::TransactionStatus;
use serde_json::{Value, json};
use tempfile::NamedTempFile;

use super::tests::{
    data, data_to_ndjson, env_or, expected_zset, iceberg_connector_metrics, iceberg_input_pipeline,
    iceberg_input_pipeline_with_storage, lateness_relation, output_zset,
};
use crate::Controller;
use crate::test::pipeline::PipelineTestEngine;

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

/// Number of complete `insert_delete` records written to the output file so
/// far (one JSON value per line).
fn output_record_count(path: &Path) -> usize {
    std::fs::read(path)
        .map(|bytes| bytes.iter().filter(|&&b| b == b'\n').count())
        .unwrap_or(0)
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
