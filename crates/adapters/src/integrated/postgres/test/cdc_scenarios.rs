//! Postgres CDC crash and restart scenarios.
//!
//! Each test drives the connector through the full `Controller` with fault
//! tolerance enabled and checks what the connector claims across a stop and
//! a restart: at-least-once delivery with the replication slot never advancing
//! past a Feldera checkpoint, and the slot reuse, state location, and table
//! filtering that delivery rests on. A test that exercises a reported issue
//! names it.
//!
//! The tests assert the desired behavior. A failing test points at a
//! regression or an open issue, not at a broken test.
//! `test_etl_state_lives_in_source_database` is the exception: it pins the
//! current behavior discussed in #6122 so a change there is noticed.
//!
//! Requirements, same as [`super::cdc_tests`]: `POSTGRES_URL` pointing at a
//! server with `wal_level=logical`, a user with `REPLICATION` privilege, and
//! the `with-postgres-cdc` feature.
//!
//! Keep the test circuit checkpointable: a checkpoint that fails inside DBSP
//! hangs `Controller::checkpoint()` instead of returning an error (#7075).
//!
//! Run with:
//!
//! ```text
//! POSTGRES_URL=postgres://postgres:postgres@localhost:5435/postgres \
//!   cargo test -p dbsp_adapters --features with-postgres-cdc \
//!   cdc_scenarios -- --ignored --test-threads=1 --nocapture
//! ```

use super::cdc_tests::{
    CdcAllTypesStruct, CdcTestTable, cdc_connector_url, cdc_ft_test_circuit,
    cdc_ft_test_circuit_for, etl_table_states, read_output_json, wait_for_etl_state,
};
use super::*;
use crossbeam::channel::Receiver;
use dbsp::DBData;
use feldera_types::serde_with_context::DeserializeWithContext;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tempfile::{NamedTempFile, TempDir};

/// Timeout for waiting on rows to reach the output file.
const WAIT_MS: u128 = 60_000;

/// Window during which rows that must not arrive are watched for. There is
/// no event to wait on for something that should not happen, so a bounded
/// window is the only option; the existing CDC tests use the same one.
const NEGATIVE_OBSERVATION_MS: u64 = 2_000;

/// Key the fix for #6121 publishes in the connector's resume metadata; its
/// boolean value says whether the checkpoint holds the whole initial
/// snapshot. No connector on `main` emits it yet; see
/// [`checkpoint_after_snapshot`] for how its absence is handled.
const SNAPSHOT_COMPLETE_KEY: &str = "snapshot_complete";

/// Wait out [`NEGATIVE_OBSERVATION_MS`] for rows that must not arrive.
fn settle() {
    std::thread::sleep(Duration::from_millis(NEGATIVE_OBSERVATION_MS));
}

/// One pipeline run: a controller plus its error channel and output file.
struct Run {
    controller: Controller,
    errors: Receiver<String>,
    output: NamedTempFile,
    /// Ids parsed from `output` so far, and the file offset after the last
    /// complete line.
    tail: Mutex<OutputTail>,
    /// Connection URL of the source database.
    pg_url: String,
    /// The connector's pipeline id, a `_`-delimited token in every
    /// replication slot etl creates for it.
    slot_token: String,
}

/// Output of a finished run.
struct Stopped {
    inserted: Vec<i64>,
    deleted: Vec<i64>,
    output: NamedTempFile,
}

/// Incremental reader for the JSON-lines output file. Wait predicates poll
/// every 10 ms, and scenario 2 writes one line per snapshot row per run, so
/// re-parsing the whole file per poll would dominate the test's wall clock.
#[derive(Default)]
struct OutputTail {
    offset: u64,
    inserted: Vec<i64>,
    deleted: Vec<i64>,
}

impl OutputTail {
    /// Parse the lines appended since the last call. A trailing partial line
    /// is left for the next call.
    fn refresh(&mut self, path: &Path) {
        let mut file = std::fs::File::open(path).unwrap();
        file.seek(SeekFrom::Start(self.offset)).unwrap();
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();
        let Some(complete) = buf.rfind('\n') else {
            return;
        };
        for line in buf[..=complete].lines().filter(|l| !l.is_empty()) {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let id_of = |op: &str| row.get(op)?.get("id")?.as_i64();
            if let Some(id) = id_of("insert") {
                self.inserted.push(id);
            }
            if let Some(id) = id_of("delete") {
                self.deleted.push(id);
            }
        }
        self.offset += (complete + 1) as u64;
    }
}

impl Run {
    /// Start a fault-tolerant CDC pipeline on `storage`. Checkpoints only
    /// happen when the test asks for them (interval set to one hour).
    fn start(table: &CdcTestTable, storage: &Path) -> Self {
        let output = NamedTempFile::new().unwrap();
        let (controller, errors) = cdc_ft_test_circuit(
            &table.url,
            &table.publication_name,
            &format!("public.{}", table.table_name),
            storage,
            output.path(),
        );
        Self::started(table, controller, errors, output)
    }

    /// Like [`Run::start`] for record type `T` with `schema`, on `workers`
    /// worker threads.
    fn start_with<T>(table: &CdcTestTable, storage: &Path, schema: &[Field], workers: usize) -> Self
    where
        T: DBData
            + SerializeWithContext<SqlSerdeConfig>
            + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + Sync,
    {
        let output = NamedTempFile::new().unwrap();
        let (controller, errors) = cdc_ft_test_circuit_for::<T>(
            &table.url,
            &table.publication_name,
            &format!("public.{}", table.table_name),
            storage,
            output.path(),
            schema,
            workers,
        );
        Self::started(table, controller, errors, output)
    }

    fn started(
        table: &CdcTestTable,
        controller: Controller,
        errors: Receiver<String>,
        output: NamedTempFile,
    ) -> Self {
        controller.start();
        Run {
            controller,
            errors,
            output,
            tail: Mutex::new(OutputTail::default()),
            pg_url: table.url.clone(),
            slot_token: slot_token(table),
        }
    }

    /// Ids of all inserts written to the output so far.
    fn inserted_ids(&self) -> Vec<i64> {
        let mut tail = self.tail.lock().unwrap();
        tail.refresh(self.output.path());
        tail.inserted.clone()
    }

    fn insert_count(&self) -> usize {
        let mut tail = self.tail.lock().unwrap();
        tail.refresh(self.output.path());
        tail.inserted.len()
    }

    /// Inserted ids appended since `cursor`, which is advanced past them.
    /// Lets a wait predicate fold new ids into its own state instead of
    /// re-reading every id on each poll.
    fn inserted_since(&self, cursor: &mut usize) -> Vec<i64> {
        let mut tail = self.tail.lock().unwrap();
        tail.refresh(self.output.path());
        let new = tail.inserted[*cursor..].to_vec();
        *cursor = tail.inserted.len();
        new
    }

    fn delete_count(&self) -> usize {
        let mut tail = self.tail.lock().unwrap();
        tail.refresh(self.output.path());
        tail.deleted.len()
    }

    /// Wait until the output holds at least `inserts` inserts and `deletes`
    /// deletes.
    fn wait_for_changes(&self, inserts: usize, deletes: usize, what: &str) {
        wait(
            || {
                (self.insert_count() >= inserts && self.delete_count() >= deletes)
                    || !self.errors.is_empty()
            },
            WAIT_MS,
        )
        .unwrap_or_else(|_| {
            panic!(
                "timeout waiting for {what}: expected {inserts} inserts and {deletes} deletes, \
                 got {} and {}",
                self.insert_count(),
                self.delete_count()
            )
        });
        self.assert_no_errors(what);
    }

    /// Wait until the output holds at least `n` inserts. Panics on a
    /// connector error or a timeout.
    fn wait_for_inserts(&self, n: usize, what: &str) {
        wait(
            || self.insert_count() >= n || !self.errors.is_empty(),
            WAIT_MS,
        )
        .unwrap_or_else(|_| {
            panic!(
                "timeout waiting for {what}: expected {n} inserts, got {}",
                self.insert_count()
            )
        });
        self.assert_no_errors(what);
    }

    fn assert_no_errors(&self, what: &str) {
        if let Ok(e) = self.errors.try_recv() {
            panic!("connector error while waiting for {what}: {e}");
        }
    }

    /// Stop the pipeline without checkpointing, as a crash would. Returns
    /// the output, read only after the controller has stopped so the file
    /// sink has flushed all it was going to write.
    fn stop(self) -> Stopped {
        let Run {
            controller,
            output,
            tail,
            pg_url,
            slot_token,
            ..
        } = self;
        controller.stop().unwrap();
        // The next run can only reattach to the replication slots once the
        // walsender of this run has released them. At least the apply slot
        // must be there: an empty match would make this barrier a no-op.
        let mut client = super::pg::pg_connect(&pg_url, &None);
        wait(
            || {
                let slots = replication_slots_of(&mut client, &slot_token);
                !slots.is_empty() && slots.iter().all(|(_, active)| !active)
            },
            WAIT_MS,
        )
        .unwrap_or_else(|_| {
            panic!(
                "timeout: replication slots for pipeline {slot_token} still active or missing \
                 after stop: {:?}",
                replication_slots_of(&mut client, &slot_token)
            )
        });
        let mut tail = tail.into_inner().unwrap();
        tail.refresh(output.path());
        Stopped {
            inserted: tail.inserted,
            deleted: tail.deleted,
            output,
        }
    }
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
/// empty. The connector must deliver the snapshot again. Run 1 stops only
/// after etl has recorded the copy as finished, so the connector itself has
/// to notice that no checkpoint holds the rows.
///
/// Unlike scenario 2, this one asserts exactly-once. Run 2 starts from an
/// empty circuit and run 1 acknowledged nothing durable, so there is no
/// earlier delivery a row could legitimately repeat. The probe row 4 is
/// written only once run 2 is past its copy, so it cannot sit on the
/// copy/stream boundary either.
#[test]
#[serial]
#[ignore = "red until #6121 is fixed: the snapshot is lost; see PR #6652"]
fn test_snapshot_replayed_when_stopped_before_checkpoint() {
    let mut table = scenario_table("cdc_sc_snap_replay");
    insert_range(&mut table, 1, 3);
    let storage = TempDir::new().unwrap();

    let run1 = Run::start(&table, storage.path());
    run1.wait_for_inserts(3, "run 1 snapshot");
    // Stop only once etl has persisted the copy as complete. Stopping earlier
    // leaves etl in `data_sync`, and it redoes the copy on restart, which
    // hides the bug behind timing.
    wait_for_etl_state(&mut table, "ready");
    run1.stop();

    let run2 = Run::start(&table, storage.path());
    // Once etl is streaming, a probe row proves replication is live even if
    // the snapshot was not replayed, so the wait below fails only on a stall.
    wait_for_etl_state(&mut table, "ready");
    insert_range(&mut table, 4, 4);
    wait(
        || run2.inserted_ids().contains(&4) || !run2.errors.is_empty(),
        WAIT_MS,
    )
    .unwrap_or_else(|_| {
        panic!(
            "timeout: run 2 never streamed the probe row; got {:?}",
            run2.inserted_ids()
        )
    });
    run2.assert_no_errors("run 2 probe row");
    // The replayed snapshot may land after the probe row. Wait for it
    // briefly; the assertion below is what decides.
    let _ = wait(|| run2.insert_count() >= 4, NEGATIVE_OBSERVATION_MS as u128);

    let ids = run2.stop().inserted;
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
/// The table is large enough that the initial copy is still running when the
/// first rows come out. Run 1 pauses input at that point, checkpoints, and
/// stops. The pause blocks etl from handing over further batches, but the
/// batch already inside `write_table_rows` keeps filling the connector's
/// queue, the controller keeps stepping on what is queued, and etl may mark
/// the copy finished. Run 2 resumes from that checkpoint. Every row must be
/// in the checkpoint or arrive in run 2, whatever state the stop left etl in.
///
/// Rows checkpointed by run 1 may be delivered again by run 2: at-least-once
/// allows it, and a primary key on the Feldera table folds them. The test
/// reports how many were repeated.
#[test]
#[serial]
#[ignore = "red until #6121 is fixed: etl persists a table error on stop; see PR #6652"]
fn test_checkpoint_mid_snapshot_loses_nothing() {
    checkpoint_mid_snapshot_loses_nothing(1);
}

/// Scenario 2 on a four-worker circuit, where redelivered rows are
/// partitioned across workers.
#[test]
#[serial]
#[ignore = "red until #6121 is fixed: etl persists a table error on stop; see PR #6652"]
fn test_checkpoint_mid_snapshot_loses_nothing_multiworker() {
    checkpoint_mid_snapshot_loses_nothing(4);
}

fn checkpoint_mid_snapshot_loses_nothing(workers: usize) {
    // Enough rows that the pause below lands while the copy is still running.
    // Run 1 reports how far it got.
    const N: i64 = 100_000;
    let mut table = scenario_table("cdc_sc_mid_snap");
    insert_range(&mut table, 1, N);
    let storage = TempDir::new().unwrap();

    let run1 =
        Run::start_with::<TestStruct>(&table, storage.path(), &TestStruct::schema(), workers);
    run1.wait_for_inserts(1, "run 1 first snapshot rows");
    run1.controller.pause();
    let checkpoint = run1.controller.checkpoint().unwrap();
    // The pause blocks new etl batches, but the controller keeps stepping on
    // rows already queued, so run 1's output can run past the checkpoint. The
    // checkpoint counts the rows it holds, and the file sink writes steps in
    // order, so those are the first rows of the output.
    let in_checkpoint = checkpoint
        .input_statistics
        .get("cdc_in")
        .expect("checkpoint has no statistics for cdc_in")
        .circuit_input_records as usize;
    let delivered = run1.stop().inserted;
    assert!(
        delivered.len() >= in_checkpoint,
        "run 1 wrote {} rows but its checkpoint holds {in_checkpoint}",
        delivered.len()
    );
    // The scenario is about a checkpoint taken mid-copy. If the copy finished
    // first, the test would pass without exercising it.
    assert!(
        in_checkpoint < N as usize,
        "the copy finished before the pause: the checkpoint holds all {N} rows, so this run \
         no longer exercises a mid-copy checkpoint; raise N"
    );
    let checkpointed = delivered[..in_checkpoint].to_vec();
    println!(
        "run 1: checkpointed {in_checkpoint} of {N} snapshot rows and delivered {} more \
         before stopping",
        delivered.len() - in_checkpoint
    );

    // Stopping while paused ends the etl batch that was waiting in
    // `wait_unpaused`. Depending on whether etl sees the destination error
    // or the shutdown signal first, it either persists a table error, which
    // the connector must recover from on the next start (PR #6652), or stays
    // in `data_sync` and simply redoes the copy.
    let states = etl_table_states(&mut table);
    println!("etl replication_state after stop: {states:?}");

    let run2 =
        Run::start_with::<TestStruct>(&table, storage.path(), &TestStruct::schema(), workers);
    let checkpointed_set: BTreeSet<i64> = checkpointed.iter().copied().collect();
    // Distinct ids across both runs: the checkpoint plus what run 2 delivers
    // that the checkpoint did not hold, folded in as it arrives.
    let mut cursor = 0;
    let mut new_in_run2 = BTreeSet::new();
    wait(
        || {
            new_in_run2.extend(
                run2.inserted_since(&mut cursor)
                    .into_iter()
                    .filter(|id| !checkpointed_set.contains(id)),
            );
            checkpointed_set.len() + new_in_run2.len() >= N as usize || !run2.errors.is_empty()
        },
        WAIT_MS * 3,
    )
    .unwrap_or_else(|_| {
        panic!(
            "timeout: run 2 delivered {} rows, run 1 checkpointed {}, expected {N} distinct ids; \
             etl states after run 1: {states:?}",
            run2.insert_count(),
            checkpointed.len()
        )
    });
    run2.assert_no_errors("run 2");

    let mut all = checkpointed;
    all.extend(run2.stop().inserted);
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
    streamed_rows_after_checkpoint_are_redelivered(1);
}

/// Scenario 3 on a four-worker circuit: redelivered rows go through the
/// input partitioning path, which a single worker never exercises.
#[test]
#[serial]
#[ignore]
fn test_streamed_rows_after_checkpoint_are_redelivered_multiworker() {
    streamed_rows_after_checkpoint_are_redelivered(4);
}

fn streamed_rows_after_checkpoint_are_redelivered(workers: usize) {
    let mut table = scenario_table("cdc_sc_stream_replay");
    insert_range(&mut table, 1, 1);
    let storage = TempDir::new().unwrap();

    let run1 =
        Run::start_with::<TestStruct>(&table, storage.path(), &TestStruct::schema(), workers);
    run1.wait_for_inserts(1, "run 1 snapshot");
    checkpoint_after_snapshot(&run1, &mut table);

    insert_range(&mut table, 2, 3);
    run1.wait_for_inserts(3, "run 1 streamed rows");
    run1.stop();

    let run2 =
        Run::start_with::<TestStruct>(&table, storage.path(), &TestStruct::schema(), workers);
    insert_range(&mut table, 4, 4);
    // 2, 3 replayed plus 4 streamed.
    run2.wait_for_inserts(3, "run 2 replay and new row");
    // Row 1 must not come back.
    settle();

    let ids = run2.stop().inserted;
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

/// Checkpoint once etl has recorded the copy as finished and, where the
/// connector can say so, the checkpoint covers the snapshot.
///
/// The fix for #6121 reports [`SNAPSHOT_COMPLETE_KEY`] in the resume
/// metadata; with it, checkpoints are taken until the key reads true. The
/// connector on `main` does not emit the key, so there the loop exits after
/// a single checkpoint and the helper is a plain "checkpoint after `ready`".
fn checkpoint_after_snapshot(run: &Run, table: &mut CdcTestTable) {
    wait_for_etl_state(table, "ready");
    let deadline = Instant::now() + Duration::from_millis(WAIT_MS as u64);
    loop {
        let checkpoint = run.controller.checkpoint().unwrap();
        let complete = checkpoint
            .input_metadata
            .0
            .get("cdc_in")
            .and_then(|m| m.get(SNAPSHOT_COMPLETE_KEY))
            .and_then(|v| v.as_bool());
        match complete {
            None | Some(true) => return,
            Some(false) => {
                assert!(
                    Instant::now() < deadline,
                    "timeout: the connector never reported the snapshot as checkpointed; \
                     resume metadata: {:?}",
                    checkpoint.input_metadata.0.get("cdc_in")
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario 4: stopping with acknowledgments in flight must not wedge restarts.
// ---------------------------------------------------------------------------

/// PR #6652 ("fix shutdown").
///
/// Run 1 streams a burst of rows and stops right away, so etl still has
/// unanswered write acknowledgments when the connector shuts down. If the
/// connector drops those acks before telling etl to shut down, etl may record
/// the interrupted batch as a table error, and a connector that does not
/// clear it never streams again. Run 2 must start, deliver a fresh row, leave
/// no table errored, and deliver every burst row at least once.
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
    let seen_in_run1 = run1.stop().inserted;

    let states = etl_table_states(&mut table);
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
    // Whatever etl recorded at the stop, run 2 must have cleared it to stream.
    let states_after_restart = etl_table_states(&mut table);
    assert!(
        !states_after_restart.iter().any(|s| s == "errored"),
        "etl table still errored after run 2 resumed streaming; after the stop {states:?}, \
         now {states_after_restart:?}"
    );
    // Duplicates would arrive after the distinct count is already complete.
    settle();

    let mut all = seen_in_run1;
    all.extend(run2.stop().inserted);
    let h = insert_histogram(all);
    let missing: Vec<i64> = (2..=BURST + 2).filter(|id| !h.contains_key(id)).collect();
    assert!(
        missing.is_empty(),
        "burst rows lost across restart: {} missing, e.g. {}",
        missing.len(),
        preview(&missing)
    );
}

// ---------------------------------------------------------------------------
// Scenario 5: where etl keeps its state.
// ---------------------------------------------------------------------------

/// Issue #6122. Pins the current behavior: the connector creates an `etl`
/// schema in the source database and stores replication state there. This
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

// ---------------------------------------------------------------------------
// Scenario 6: updates and deletes after the last checkpoint are redelivered.
// ---------------------------------------------------------------------------

/// `REPLICA IDENTITY FULL` exists so updates and deletes carry the old row.
/// Run 1 checkpoints the snapshot, then updates row 2 and deletes row 3 and
/// stops without a checkpoint. Run 2 must redeliver both changes, and the net
/// state after both runs must show row 1 unchanged, row 2 present with its new
/// value, and row 3 gone.
#[test]
#[serial]
#[ignore]
fn test_update_delete_after_checkpoint_are_redelivered() {
    update_delete_after_checkpoint_are_redelivered(1);
}

/// Scenario 6 on a four-worker circuit, where the redelivered retraction and
/// insertion of the updated row may land on different workers.
#[test]
#[serial]
#[ignore]
fn test_update_delete_after_checkpoint_are_redelivered_multiworker() {
    update_delete_after_checkpoint_are_redelivered(4);
}

fn update_delete_after_checkpoint_are_redelivered(workers: usize) {
    let mut table = scenario_table("cdc_sc_upd_del");
    insert_range(&mut table, 1, 3);
    let storage = TempDir::new().unwrap();

    let run1 =
        Run::start_with::<TestStruct>(&table, storage.path(), &TestStruct::schema(), workers);
    run1.wait_for_inserts(3, "run 1 snapshot");
    checkpoint_after_snapshot(&run1, &mut table);

    table.execute(&format!(
        "UPDATE {} SET s = 'updated' WHERE id = 2",
        table.table_name
    ));
    table.execute(&format!("DELETE FROM {} WHERE id = 3", table.table_name));
    // The update arrives as a delete of the old row and an insert of the new.
    run1.wait_for_changes(4, 2, "run 1 update and delete");
    run1.stop();

    let run2 =
        Run::start_with::<TestStruct>(&table, storage.path(), &TestStruct::schema(), workers);
    run2.wait_for_changes(1, 2, "run 2 replay of update and delete");
    // Nothing else may be redelivered.
    settle();
    let out = run2.stop();

    // Net weight per id: the snapshot in the checkpoint plus run 2's changes.
    let mut weight: BTreeMap<i64, i64> = (1..=3).map(|id| (id, 1)).collect();
    for id in &out.inserted {
        *weight.entry(*id).or_insert(0) += 1;
    }
    for id in &out.deleted {
        *weight.entry(*id).or_insert(0) -= 1;
    }
    assert_eq!(
        weight,
        BTreeMap::from([(1, 1), (2, 1), (3, 0)]),
        "net row weights after replay; run 2 inserted {:?}, deleted {:?}",
        out.inserted,
        out.deleted
    );
    let updated = read_output_json(out.output.path())
        .iter()
        .filter_map(|r| r.get("insert").cloned())
        .find(|r| r["id"] == json!(2))
        .expect("run 2 must redeliver the new image of row 2");
    assert_eq!(updated["s"], json!("updated"));
}

// ---------------------------------------------------------------------------
// Scenario 7: every supported column type survives a replay.
// ---------------------------------------------------------------------------

/// Run 1 snapshots a row of every supported Postgres type and checkpoints.
/// A second full row and an all-NULL row stream in, and the run stops without
/// a checkpoint. Run 2 must redeliver both with every value intact: the
/// replayed rows come from WAL decoding, a different code path from the
/// initial `COPY`, and NULL has its own encoding in WAL tuple data.
#[test]
#[serial]
#[ignore]
fn test_all_types_replayed_after_restart() {
    let url = postgres_url();
    let mut table = CdcTestTable::new_all_types(
        &unique_pg_name("cdc_sc_all_types"),
        &unique_pg_name("cdc_pub"),
        &url,
    );
    let insert_full_row = |table: &mut CdcTestTable, id: i64, text: &str, big: i64| {
        table.execute(&format!(
            r#"INSERT INTO {} (
                id, col_text, col_integer, col_bigint, col_boolean,
                col_real, col_double, col_date, col_time,
                col_timestamp, col_timestamptz, col_uuid, col_jsonb,
                col_bytea, col_numeric, col_smallint, col_int_array
            ) VALUES (
                {id}, '{text}', 42, {big}, true,
                3.14, 2.718281828, '2024-06-15', '14:30:00',
                '2024-01-01 12:00:00', '2024-01-01 12:00:00+00', '550e8400-e29b-41d4-a716-446655440000',
                '{{"key": "value", "nested": {{"a": 1}}}}',
                E'\\xDEADBEEF', 12345.67, 7, ARRAY[1, 2, 3]
            )"#,
            table.table_name
        ));
    };
    insert_full_row(&mut table, 1, "hello world", 9876543210);
    let storage = TempDir::new().unwrap();
    let schema = CdcAllTypesStruct::schema();

    let run1 = Run::start_with::<CdcAllTypesStruct>(&table, storage.path(), &schema, 1);
    run1.wait_for_inserts(1, "run 1 snapshot");
    checkpoint_after_snapshot(&run1, &mut table);
    insert_full_row(&mut table, 2, "streamed", 1234567890123);
    table.execute(&format!("INSERT INTO {} (id) VALUES (3)", table.table_name));
    run1.wait_for_inserts(3, "run 1 streamed rows");
    run1.stop();

    let run2 = Run::start_with::<CdcAllTypesStruct>(&table, storage.path(), &schema, 1);
    run2.wait_for_inserts(2, "run 2 replay");
    // The checkpointed row 1 must not come back.
    settle();
    let out = run2.stop();
    assert_eq!(
        out.inserted,
        vec![2, 3],
        "only the streamed rows are replayed"
    );

    let rows: Vec<serde_json::Value> = read_output_json(out.output.path())
        .into_iter()
        .filter_map(|r| r.get("insert").cloned())
        .collect();
    let null_row = rows.iter().find(|r| r["id"] == json!(3)).unwrap();
    for (column, value) in null_row.as_object().unwrap() {
        if column != "id" {
            assert!(value.is_null(), "{column} must replay as NULL, got {value}");
        }
    }
    let row = rows.into_iter().find(|r| r["id"] == json!(2)).unwrap();
    assert_eq!(row["col_text"], json!("streamed"));
    assert_eq!(row["col_integer"], json!(42));
    assert_eq!(row["col_bigint"], json!(1234567890123i64));
    assert_eq!(row["col_boolean"], json!(true));
    assert!((row["col_real"].as_f64().unwrap() - 3.14).abs() < 1e-3);
    assert!((row["col_double"].as_f64().unwrap() - 2.718281828).abs() < 1e-9);
    assert_eq!(row["col_date"], json!("2024-06-15"));
    assert_eq!(
        row["col_uuid"],
        json!("550e8400-e29b-41d4-a716-446655440000")
    );
    assert_eq!(row["col_jsonb"]["key"], json!("value"));
    assert_eq!(row["col_jsonb"]["nested"]["a"], json!(1));
    assert_eq!(row["col_numeric"], json!("12345.67"));
    assert_eq!(row["col_smallint"], json!(7));
    assert_eq!(row["col_int_array"], json!([1, 2, 3]));
    assert!(
        row["col_time"].as_str().unwrap().starts_with("14:30:00"),
        "col_time = {}",
        row["col_time"]
    );
    assert!(
        row["col_timestamp"]
            .as_str()
            .unwrap()
            .starts_with("2024-01-01"),
        "col_timestamp = {}",
        row["col_timestamp"]
    );
    assert!(
        row["col_timestamptz"]
            .as_str()
            .unwrap()
            .starts_with("2024-01-01"),
        "col_timestamptz = {}",
        row["col_timestamptz"]
    );
    // BYTEA is encoded as a JSON byte array, as in `test_cdc_all_data_types`.
    assert_eq!(row["col_bytea"], json!([0xde, 0xad, 0xbe, 0xef]));
}

// ---------------------------------------------------------------------------
// Scenario 8: replication slots outlive a stop and are reused on restart.
// ---------------------------------------------------------------------------

/// The connector resumes from etl's apply slot, so a stop must leave that
/// slot in place, inactive, and a restart must reattach to the same slot
/// rather than create another one. etl may add a table-sync slot while it
/// copies a table, so only the apply slot is pinned.
#[test]
#[serial]
#[ignore]
fn test_replication_slots_survive_restart() {
    let mut table = scenario_table("cdc_sc_slots");
    insert_range(&mut table, 1, 1);
    let storage = TempDir::new().unwrap();

    let run1 = Run::start(&table, storage.path());
    run1.wait_for_inserts(1, "run 1 snapshot");
    checkpoint_after_snapshot(&run1, &mut table);
    run1.stop();

    let after_stop = replication_slots(&mut table);
    let apply_slot = |slots: &[(String, bool)]| {
        slots
            .iter()
            .find(|(n, _)| n.starts_with("supabase_etl_apply_"))
            .cloned()
    };
    // `Run::stop` already waited for every slot to go inactive.
    let (apply_name, _) = apply_slot(&after_stop)
        .unwrap_or_else(|| panic!("apply slot gone after stop: {after_stop:?}"));

    let run2 = Run::start(&table, storage.path());
    insert_range(&mut table, 2, 2);
    run2.wait_for_inserts(1, "run 2 streamed row");
    let after_restart = replication_slots(&mut table);
    run2.stop();

    assert_eq!(
        apply_slot(&after_restart),
        Some((apply_name, true)),
        "a restart must reattach to the same apply slot and keep it active; before \
         {after_stop:?}, after {after_restart:?}"
    );
}

// ---------------------------------------------------------------------------
// Scenario 9: other tables in the publication are filtered out.
// ---------------------------------------------------------------------------

/// A publication may carry more tables than one connector reads. Connector A
/// reads `a` from a publication holding `a` and `b`; connector B reads `b`
/// from its own publication, at the same time. Each must see only its own
/// table's rows.
#[test]
#[serial]
#[ignore]
fn test_other_tables_in_publication_are_filtered() {
    let mut table_a = scenario_table("cdc_sc_pub_a");
    let mut table_b = scenario_table("cdc_sc_pub_b");
    table_a.execute(&format!(
        "ALTER PUBLICATION {} ADD TABLE {}",
        table_a.publication_name, table_b.table_name
    ));
    insert_range(&mut table_a, 1, 3);
    insert_range(&mut table_b, 101, 103);
    let storage_a = TempDir::new().unwrap();
    let storage_b = TempDir::new().unwrap();

    let run_a = Run::start(&table_a, storage_a.path());
    let run_b = Run::start(&table_b, storage_b.path());
    run_a.wait_for_inserts(3, "connector A snapshot");
    run_b.wait_for_inserts(3, "connector B snapshot");

    insert_range(&mut table_a, 4, 4);
    insert_range(&mut table_b, 104, 104);
    run_a.wait_for_inserts(4, "connector A streamed row");
    run_b.wait_for_inserts(4, "connector B streamed row");
    // Neither connector may receive the other table's rows.
    settle();

    let a = run_a.stop().inserted;
    let b = run_b.stop().inserted;
    assert_exactly_once(&a, 4, "connector A must see only table a");
    let b_shifted: Vec<i64> = b.iter().map(|id| id - 100).collect();
    assert_exactly_once(
        &b_shifted,
        4,
        "connector B must see only table b (ids shifted by 100)",
    );
}

// ---------------------------------------------------------------------------
// Scenario 10: transactions that straddle a restart.
// ---------------------------------------------------------------------------

/// Logical decoding emits a transaction only at its commit. A transaction
/// that is open while the connector stops and commits after the restart must
/// arrive exactly once in run 2. A transaction rolled back before the restart
/// must never arrive at all.
#[test]
#[serial]
#[ignore]
fn test_transactions_across_restart() {
    let mut table = scenario_table("cdc_sc_tx");
    insert_range(&mut table, 1, 1);
    let storage = TempDir::new().unwrap();
    let table_name = table.table_name.clone();
    let insert =
        move |id: i64| format!("INSERT INTO {table_name} VALUES ({id}, true, {id}, 'tx-{id}')");

    let run1 = Run::start(&table, storage.path());
    run1.wait_for_inserts(1, "run 1 snapshot");
    checkpoint_after_snapshot(&run1, &mut table);

    // Rolled back before the stop: must never show up.
    let mut aborted = super::pg::pg_connect(&table.url, &None);
    let mut tx = aborted.transaction().unwrap();
    for id in 20..=22 {
        tx.execute(insert(id).as_str(), &[]).unwrap();
    }
    tx.rollback().unwrap();

    // Still open across the stop: decoded only once it commits, in run 2.
    let mut pending = super::pg::pg_connect(&table.url, &None);
    let mut tx = pending.transaction().unwrap();
    for id in 10..=12 {
        tx.execute(insert(id).as_str(), &[]).unwrap();
    }
    // A committed marker row proves run 1 streamed past the aborted rows.
    insert_range(&mut table, 2, 2);
    run1.wait_for_inserts(2, "run 1 marker row");
    let seen_in_run1 = run1.stop().inserted;

    let run2 = Run::start(&table, storage.path());
    tx.commit().unwrap();
    run2.wait_for_inserts(3, "run 2 rows of the late commit");
    // Anything from the rolled-back transaction would be a decoding bug.
    settle();

    let mut all = seen_in_run1;
    all.extend(run2.stop().inserted);
    let h = insert_histogram(all);
    for id in 10..=12 {
        assert_eq!(
            h.get(&id).copied().unwrap_or(0),
            1,
            "row {id} of the transaction committed after the restart must arrive exactly once; \
             got {h:?}"
        );
    }
    for id in 20..=22 {
        assert!(
            !h.contains_key(&id),
            "row {id} of a rolled-back transaction must never arrive; got {h:?}"
        );
    }
}

/// The connector's pipeline id for `table`, as etl embeds it in slot names.
fn slot_token(table: &CdcTestTable) -> String {
    crate::integrated::postgres::cdc_input::pipeline_id(
        &cdc_connector_url(&table.url),
        &table.publication_name,
        &format!("public.{}", table.table_name),
    )
    .to_string()
}

/// Replication slots etl created for `table`'s connector, with their active
/// flag.
fn replication_slots(table: &mut CdcTestTable) -> Vec<(String, bool)> {
    let token = slot_token(table);
    replication_slots_of(&mut table.client, &token)
}

fn replication_slots_of(client: &mut postgres::Client, slot_token: &str) -> Vec<(String, bool)> {
    client
        .query(
            "SELECT slot_name::text, active FROM pg_replication_slots ORDER BY 1",
            &[],
        )
        .unwrap_or_else(|e| panic!("querying pg_replication_slots failed: {e}"))
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, bool>(1)))
        .filter(|(name, _)| name.split('_').any(|token| token == slot_token))
        .collect()
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
