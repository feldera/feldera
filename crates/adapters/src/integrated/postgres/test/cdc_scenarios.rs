//! Postgres CDC crash and restart scenarios.
//!
//! Each test drives the connector through the full `Controller` with fault
//! tolerance enabled and checks the delivery guarantee the connector claims:
//! at-least-once, with the replication slot never advancing past a Feldera
//! checkpoint. Every test names the GitHub issue it exercises.
//!
//! The tests assert the *desired* behavior. A failing test points at an open
//! issue, not at a broken test. `test_etl_state_lives_in_source_database` is
//! the exception: it pins the current behavior discussed in #6122 so a change
//! there is noticed.
//!
//! Requirements, same as [`super::cdc_tests`]: `POSTGRES_URL` pointing at a
//! server with `wal_level=logical`, a user with `REPLICATION` privilege, and
//! the `with-postgres-cdc` feature.
//!
//! Run with:
//!
//! ```text
//! POSTGRES_URL=postgres://postgres:postgres@localhost:5435/postgres \
//!   cargo test -p dbsp_adapters --features with-postgres-cdc \
//!   cdc_scenarios -- --ignored --test-threads=1 --nocapture
//! ```

use super::cdc_tests::{CdcTestTable, cdc_connector_url, count_inserts, read_output_json};
use super::*;
use crossbeam::channel::Receiver;
use feldera_types::config::PipelineConfig;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;
use tempfile::{NamedTempFile, TempDir};

/// Timeout for waiting on rows to reach the output file.
const WAIT_MS: u128 = 60_000;

/// One pipeline run: a controller plus its error channel and output file.
struct Run {
    controller: Controller,
    errors: Receiver<String>,
    output: NamedTempFile,
}

impl Run {
    /// Start a fault-tolerant CDC pipeline on `storage`. Checkpoints only
    /// happen when the test asks for them (interval set to one hour).
    fn start(table: &CdcTestTable, storage: &Path) -> Self {
        let output = NamedTempFile::new().unwrap();
        let config: PipelineConfig = serde_json::from_value(json!({
            "name": "cdc_scenario",
            "workers": 1,
            "storage_config": { "path": storage },
            "storage": true,
            "fault_tolerance": { "model": "at_least_once", "checkpoint_interval_secs": 3600 },
            "inputs": {
                "cdc_in": {
                    "stream": "test_input1",
                    "transport": {
                        "name": "postgres_cdc_input",
                        "config": {
                            "uri": cdc_connector_url(&table.url),
                            "publication": table.publication_name,
                            "source_table": format!("public.{}", table.table_name),
                        },
                    },
                },
            },
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": { "path": output.path() },
                    },
                    "format": {
                        "name": "json",
                        "config": { "update_format": "insert_delete", "array": false },
                    },
                },
            },
        }))
        .unwrap();

        let (err_sender, errors) = crossbeam::channel::unbounded();
        // `test_circuit` assigns persistent ids to every operator, which a
        // checkpoint needs. A hand-built circuit without them makes the
        // checkpoint fail, and the failure path hangs the circuit thread
        // (see the `NoPersistentId` note in the module docs).
        let controller = Controller::with_test_config(
            |circuit_config| {
                Ok(crate::test::test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some("output")],
                ))
            },
            &config,
            Box::new(move |e, _| {
                let msg = format!("cdc_scenario: error: {e}");
                println!("{msg}");
                let _ = err_sender.send(msg);
            }),
        )
        .unwrap();
        controller.start();

        Run {
            controller,
            errors,
            output,
        }
    }

    fn rows(&self) -> Vec<serde_json::Value> {
        read_output_json(self.output.path())
    }

    fn inserted_ids(&self) -> Vec<i64> {
        inserted_ids(&self.rows())
    }

    /// Wait until the output holds at least `n` inserts. Panics on a
    /// connector error or a timeout.
    fn wait_for_inserts(&self, n: usize, what: &str) {
        wait(
            || count_inserts(&self.rows()) >= n || !self.errors.is_empty(),
            WAIT_MS,
        )
        .unwrap_or_else(|_| {
            panic!(
                "timeout waiting for {what}: expected {n} inserts, got {}",
                count_inserts(&self.rows())
            )
        });
        self.assert_no_errors(what);
    }

    fn assert_no_errors(&self, what: &str) {
        if let Ok(e) = self.errors.try_recv() {
            panic!("connector error while waiting for {what}: {e}");
        }
    }

    /// Stop the pipeline without checkpointing, as a crash would.
    fn stop(self) -> NamedTempFile {
        self.controller.stop().unwrap();
        // Let etl's replication connection detach so the next run can reuse
        // the slot.
        std::thread::sleep(Duration::from_secs(1));
        self.output
    }
}

fn inserted_ids(rows: &[serde_json::Value]) -> Vec<i64> {
    rows.iter()
        .filter_map(|r| r.get("insert")?.get("id")?.as_i64())
        .collect()
}

/// Count how many times each id was inserted across `runs`.
fn insert_histogram(ids: impl IntoIterator<Item = i64>) -> BTreeMap<i64, usize> {
    let mut h = BTreeMap::new();
    for id in ids {
        *h.entry(id).or_insert(0) += 1;
    }
    h
}

/// Assert that `ids` holds each of `1..=n` exactly once. Reports both missing
/// ids (data loss) and repeated ids (duplicates) in one message.
fn assert_exactly_once(ids: &[i64], n: i64, what: &str) {
    let h = insert_histogram(ids.iter().copied());
    let missing: Vec<i64> = (1..=n).filter(|id| !h.contains_key(id)).collect();
    let dups: Vec<(i64, usize)> = h
        .iter()
        .filter(|(_, c)| **c > 1)
        .map(|(i, c)| (*i, *c))
        .collect();
    let extra: Vec<i64> = h.keys().copied().filter(|id| *id < 1 || *id > n).collect();
    assert!(
        missing.is_empty() && dups.is_empty() && extra.is_empty(),
        "{what}: expected ids 1..={n} exactly once; missing {} ids {:?}, duplicated {} ids {:?}, unexpected {:?}",
        missing.len(),
        preview(&missing),
        dups.len(),
        preview(&dups),
        preview(&extra),
    );
}

fn preview<T: std::fmt::Debug>(v: &[T]) -> String {
    if v.len() <= 10 {
        format!("{v:?}")
    } else {
        format!("{:?} ... (+{})", &v[..10], v.len() - 10)
    }
}

fn scenario_table(name: &str) -> CdcTestTable {
    let url = postgres_url();
    CdcTestTable::new_simple(&unique_pg_name(name), &unique_pg_name("cdc_pub"), &url)
}

/// Bulk insert ids `from..=to` in one statement.
fn insert_range(table: &mut CdcTestTable, from: i64, to: i64) {
    table.execute(&format!(
        "INSERT INTO {} SELECT g, g % 2 = 0, g * 10, 'row-' || g FROM generate_series({from}, {to}) g",
        table.table_name
    ));
}

// ---------------------------------------------------------------------------
// Scenario 1: snapshot rows must survive a crash before the first checkpoint.
// ---------------------------------------------------------------------------

/// Issue #6121 (comment of 2026-05-24) and PR #6652.
///
/// Run 1 ingests the initial snapshot and stops before any checkpoint. Run 2
/// resumes on the same storage, which holds no checkpoint, so the circuit is
/// empty. The connector must deliver the snapshot again: etl may only record
/// the copy as finished once Feldera has checkpointed the rows.
#[test]
#[serial]
#[ignore]
fn test_snapshot_replayed_when_stopped_before_checkpoint() {
    let mut table = scenario_table("cdc_sc_snap_replay");
    insert_range(&mut table, 1, 3);
    let storage = TempDir::new().unwrap();

    let run1 = Run::start(&table, storage.path());
    run1.wait_for_inserts(3, "run 1 snapshot");
    // Stop only once etl has persisted the copy as complete. Stopping earlier
    // leaves etl in `data_sync`, and it redoes the copy on restart, which
    // hides the bug behind timing.
    wait_for_etl_state(&table, "ready");
    run1.stop();

    let run2 = Run::start(&table, storage.path());
    // A new streamed row proves replication is live even if the snapshot
    // is not replayed, so the timeout below fires only on a total stall.
    insert_range(&mut table, 4, 4);
    run2.wait_for_inserts(1, "run 2 first row");
    // Give the snapshot replay a moment to land after the streamed row.
    let _ = wait(|| count_inserts(&run2.rows()) >= 4, 10_000);

    let ids = run2.inserted_ids();
    run2.stop();
    assert_exactly_once(
        &ids,
        4,
        "run 2 output (snapshot 1..3 must be replayed, 4 streamed)",
    );
}

// ---------------------------------------------------------------------------
// Scenario 2: a checkpoint taken mid-copy must not lose rows or wedge restarts.
// ---------------------------------------------------------------------------

/// Issue #6121 (comment of 2026-05-24) and PR #6652.
///
/// The table is large enough that the initial copy spans several etl batches.
/// Run 1 pauses input as soon as the first rows come out, checkpoints, and
/// stops. Pausing stops Feldera from stepping but does not stop etl, which
/// keeps copying into the connector's queue and may mark the copy finished.
/// Run 2 resumes from that checkpoint. Across both runs every row must appear
/// at least once, and the stop must not leave etl with a persisted error.
///
/// Rows checkpointed by run 1 may be delivered again by run 2: at-least-once
/// allows it, and a primary key on the Feldera table folds them. The test
/// reports how many were repeated.
#[test]
#[serial]
#[ignore]
fn test_checkpoint_mid_snapshot_loses_nothing() {
    const N: i64 = 300_000;
    let mut table = scenario_table("cdc_sc_mid_snap");
    insert_range(&mut table, 1, N);
    let storage = TempDir::new().unwrap();

    let run1 = Run::start(&table, storage.path());
    run1.wait_for_inserts(1, "run 1 first snapshot rows");
    run1.controller.pause();
    run1.controller.checkpoint().unwrap();
    let checkpointed = run1.inserted_ids();
    println!(
        "run 1: checkpointed {} of {N} snapshot rows before stopping",
        checkpointed.len()
    );
    run1.stop();

    // Stopping while paused ends the etl batch that was waiting in
    // `wait_unpaused`. etl records that as a table error; the connector must
    // recover from it on the next start instead of failing (PR #6652).
    let states = etl_table_states(&table);
    println!("etl replication_state after stop: {states:?}");

    let run2 = Run::start(&table, storage.path());
    wait(
        || {
            let mut seen: BTreeSet<i64> = checkpointed.iter().copied().collect();
            seen.extend(run2.inserted_ids());
            seen.len() >= N as usize || !run2.errors.is_empty()
        },
        WAIT_MS * 3,
    )
    .unwrap_or_else(|_| {
        panic!(
            "timeout: run 2 delivered {} rows, run 1 checkpointed {}, expected {N} distinct ids; \
             etl states after run 1: {states:?}",
            count_inserts(&run2.rows()),
            checkpointed.len()
        )
    });
    run2.assert_no_errors("run 2");

    let mut all = checkpointed;
    all.extend(run2.inserted_ids());
    run2.stop();
    let h = insert_histogram(all);
    let missing: Vec<i64> = (1..=N).filter(|id| !h.contains_key(id)).collect();
    let repeated = h.values().filter(|c| **c > 1).count();
    println!("run 2: {repeated} rows already in the checkpoint were delivered again");
    assert!(
        missing.is_empty(),
        "snapshot rows lost across restart: {} missing, e.g. {}",
        missing.len(),
        preview(&missing)
    );
}

// ---------------------------------------------------------------------------
// Scenario 3: streamed events after the last checkpoint must be redelivered.
// ---------------------------------------------------------------------------

/// Issue #6121 (core case, fixed in 157254596).
///
/// Run 1 checkpoints after the snapshot, then streams rows 2 and 3 and stops
/// without another checkpoint. Run 2 resumes from the checkpoint, which does
/// not contain 2 and 3, so the slot must not have moved past them: both must
/// be redelivered. Row 1 is in the checkpoint and must not come back.
#[test]
#[serial]
#[ignore]
fn test_streamed_rows_after_checkpoint_are_redelivered() {
    let mut table = scenario_table("cdc_sc_stream_replay");
    insert_range(&mut table, 1, 1);
    let storage = TempDir::new().unwrap();

    let run1 = Run::start(&table, storage.path());
    run1.wait_for_inserts(1, "run 1 snapshot");
    // Let etl record the copy as finished and the connector report that in
    // its resume metadata, so the checkpoint below covers the snapshot.
    wait_for_etl_state(&table, "ready");
    std::thread::sleep(Duration::from_secs(2));
    run1.controller.checkpoint().unwrap();

    insert_range(&mut table, 2, 3);
    run1.wait_for_inserts(3, "run 1 streamed rows");
    run1.stop();

    let run2 = Run::start(&table, storage.path());
    insert_range(&mut table, 4, 4);
    // 2, 3 replayed plus 4 streamed.
    run2.wait_for_inserts(3, "run 2 replay and new row");
    std::thread::sleep(Duration::from_secs(2));

    let ids = run2.inserted_ids();
    run2.stop();
    let h = insert_histogram(ids.iter().copied());
    assert!(
        !h.contains_key(&1),
        "row 1 was in the checkpoint and must not be redelivered; got {ids:?}"
    );
    for id in 2..=4 {
        assert_eq!(
            h.get(&id).copied().unwrap_or(0),
            1,
            "row {id} must be delivered exactly once in run 2; got {ids:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Scenario 4: stopping with acknowledgments in flight must not wedge restarts.
// ---------------------------------------------------------------------------

/// PR #6652 ("fix shutdown").
///
/// Run 1 streams a burst of rows and stops right away, so etl still has
/// unanswered write acknowledgments when the connector shuts down. If the
/// connector drops those acks before telling etl to shut down, etl persists
/// an error for the table and the next run never streams again. Run 2 must
/// start cleanly, deliver a fresh row, and deliver every burst row at least
/// once.
#[test]
#[serial]
#[ignore]
fn test_restart_after_stop_with_inflight_acks() {
    const BURST: i64 = 2_000;
    let mut table = scenario_table("cdc_sc_inflight");
    insert_range(&mut table, 1, 1);
    let storage = TempDir::new().unwrap();

    let run1 = Run::start(&table, storage.path());
    run1.wait_for_inserts(1, "run 1 snapshot");
    run1.controller.checkpoint().unwrap();
    insert_range(&mut table, 2, BURST + 1);
    // Stop as soon as the burst starts flowing.
    run1.wait_for_inserts(2, "run 1 first burst rows");
    let seen_in_run1 = run1.inserted_ids();
    run1.stop();

    let states = etl_table_states(&table);
    println!("etl replication_state after stop: {states:?}");

    let run2 = Run::start(&table, storage.path());
    insert_range(&mut table, BURST + 2, BURST + 2);
    wait(
        || {
            let ids = run2.inserted_ids();
            ids.contains(&(BURST + 2)) || !run2.errors.is_empty()
        },
        WAIT_MS,
    )
    .unwrap_or_else(|_| {
        panic!(
            "timeout: run 2 never streamed the row inserted after restart; etl states: {states:?}; \
             run 2 output ids: {}",
            preview(&run2.inserted_ids())
        )
    });
    run2.assert_no_errors("run 2");
    std::thread::sleep(Duration::from_secs(2));

    let mut all = seen_in_run1;
    all.extend(run2.inserted_ids());
    run2.stop();
    let h = insert_histogram(all);
    let missing: Vec<i64> = (2..=BURST + 2).filter(|id| !h.contains_key(id)).collect();
    assert!(
        missing.is_empty(),
        "burst rows lost across restart: {} missing, e.g. {}",
        missing.len(),
        preview(&missing)
    );
    assert!(
        !states.iter().any(|s| s == "errored"),
        "etl persisted an errored table state on clean shutdown: {states:?}"
    );
}

// ---------------------------------------------------------------------------
// Scenario 5: where etl keeps its state.
// ---------------------------------------------------------------------------

/// Issue #6122. Pins the current behavior: the connector creates an `etl`
/// schema in the *source* database and stores replication state there. This
/// needs DDL privileges on the source and rules out read replicas. When the
/// state moves elsewhere, update this test.
#[test]
#[serial]
#[ignore]
fn test_etl_state_lives_in_source_database() {
    let mut table = scenario_table("cdc_sc_etl_schema");
    insert_range(&mut table, 1, 1);
    let storage = TempDir::new().unwrap();

    let run = Run::start(&table, storage.path());
    run.wait_for_inserts(1, "snapshot");
    run.stop();

    let objects = etl_schema_objects(&mut table);
    println!("objects in schema `etl` on the source database: {objects:?}");
    assert!(
        objects.iter().any(|o| o == "replication_state"),
        "expected etl.replication_state in the source database (see #6122); found {objects:?}"
    );
}

/// Tables in the `etl` schema of the source database.
fn etl_schema_objects(table: &mut CdcTestTable) -> Vec<String> {
    table
        .client
        .query(
            "SELECT table_name::text FROM information_schema.tables WHERE table_schema = 'etl' ORDER BY 1",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
}

/// Wait until etl reports `state` for this connector's table.
fn wait_for_etl_state(table: &CdcTestTable, state: &str) {
    wait(
        || etl_table_states(table).iter().any(|s| s == state),
        WAIT_MS,
    )
    .unwrap_or_else(|_| {
        panic!(
            "timeout waiting for etl state {state:?}; current states: {:?}",
            etl_table_states(table)
        )
    });
}

/// Current `state` of every table etl tracks for this connector's pipeline.
fn etl_table_states(table: &CdcTestTable) -> Vec<String> {
    let pipeline_id = crate::integrated::postgres::cdc_input::pipeline_id(
        &cdc_connector_url(&table.url),
        &table.publication_name,
        &format!("public.{}", table.table_name),
    ) as i64;
    let mut client = super::pg::pg_connect(&table.url, &None);
    client
        .query(
            "SELECT state::text FROM etl.replication_state WHERE pipeline_id = $1 AND is_current",
            &[&pipeline_id],
        )
        .unwrap_or_default()
        .iter()
        .map(|r| r.get(0))
        .collect()
}
