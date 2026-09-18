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
    CdcAllTypesStruct, CdcTestTable, CircuitShape, ETL_SYNC_COMPLETED_STATES, cdc_connector_url,
    cdc_ft_test_circuit, cdc_ft_test_circuit_for, etl_table_states, read_output_json,
    wait_for_etl_copy_in_progress, wait_for_etl_sync_completed,
};
use super::*;
use crossbeam::channel::Receiver;
use dbsp::DBData;
use feldera_types::serde_with_context::DeserializeWithContext;
use feldera_types::suspend::{SuspendError, TemporarySuspendError};
use std::collections::BTreeMap;
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
    /// Like [`Run::start`], with the reader taking `max_batch_size` records a
    /// step, so a write that arrives as several buffers reaches the circuit
    /// over as many steps instead of in one.
    fn start_batched(table: &CdcTestTable, storage: &Path, max_batch_size: u64) -> Self {
        let output = NamedTempFile::new().unwrap();
        let (controller, errors) = cdc_ft_test_circuit_for::<TestStruct>(
            &table.url,
            &table.publication_name,
            &format!("public.{}", table.table_name),
            storage,
            output.path(),
            &TestStruct::schema(),
            CircuitShape {
                max_batch_size: Some(max_batch_size),
                ..CircuitShape::default()
            },
        );
        Self::started(table, controller, errors, output)
    }

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
            CircuitShape {
                workers,
                ..CircuitShape::default()
            },
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

    /// Records the circuit has taken from the CDC connector so far. This is
    /// the counter a checkpoint reports as `circuit_input_records`, so a
    /// prefix measured here and a checkpoint's total are comparable.
    fn circuit_input_records(&self) -> u64 {
        let status = self.controller.status();
        let inputs = status.input_status();
        let endpoint = inputs
            .values()
            .find(|e| e.endpoint_name == "cdc_in")
            .expect("no input status for endpoint cdc_in");
        endpoint
            .metrics
            .circuit_input_records
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn insert_count(&self) -> usize {
        let mut tail = self.tail.lock().unwrap();
        tail.refresh(self.output.path());
        tail.inserted.len()
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
            let slots = replication_slots_of(&mut client, &slot_token);
            assert!(
                !slots.is_empty(),
                "pipeline {slot_token} created no replication slot, so this run copied \
                 nothing and its stop says nothing about slot release"
            );
            panic!(
                "timeout: replication slots for pipeline {slot_token} still active after \
                 stop: {slots:?}"
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
fn test_snapshot_replayed_when_stopped_before_checkpoint() {
    let mut table = scenario_table("cdc_sc_snap_replay");
    insert_range(&mut table, 1, 3);
    let storage = TempDir::new().unwrap();

    let run1 = Run::start(&table, storage.path());
    run1.wait_for_inserts(3, "run 1 snapshot");
    // Stop only once etl has persisted the copy as complete. Stopping earlier
    // leaves etl in `data_sync` or `finished_copy`, and it redoes the copy on
    // restart, which hides the bug behind timing.
    wait_for_etl_sync_completed(&mut table);
    run1.stop();

    let run2 = Run::start(&table, storage.path());
    // Once etl is streaming, a probe row proves replication is live even if
    // the snapshot was not replayed, so the wait below fails only on a stall.
    wait_for_etl_sync_completed(&mut table);
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
// Scenario 2: a checkpoint requested mid-copy waits for the whole copy.
// ---------------------------------------------------------------------------

/// Issue #6121.
///
/// A checkpoint holding part of the initial copy would be unusable: on resume
/// the connector cannot ask etl for the rest, because etl streams from the
/// replication slot once it considers the copy done. The connector therefore
/// reports a barrier for every step until the copy is in the circuit, and the
/// controller defers a checkpoint requested before then.
///
/// The test measures how much of the copy the circuit holds while etl still
/// reports `data_sync`, and requires that prefix to be non-empty and shorter
/// than the table. The checkpoint requested at that moment must still hold
/// every row of the table, which is what the deferral buys, and a restart
/// from it must neither lose a row nor read the table again.
///
/// What the assertions cannot reach from here is the narrower window in which
/// etl reports the copy finished while its last buffers are still queued for
/// the circuit. Nothing in this black-box path holds the pipeline inside that
/// window; the unit tests of the copy state cover it.
#[test]
#[serial]
fn test_checkpoint_mid_snapshot_waits_for_the_whole_copy() {
    checkpoint_mid_snapshot_waits_for_the_whole_copy(1);
}

/// Scenario 2 on a four-worker circuit, where the copy is partitioned across
/// workers.
#[test]
#[serial]
fn test_checkpoint_mid_snapshot_waits_for_the_whole_copy_multiworker() {
    checkpoint_mid_snapshot_waits_for_the_whole_copy(4);
}

/// Attempts a scenario makes at catching rows still on their way to the
/// circuit before it gives up.
const CATCH_MIDWAY_ATTEMPTS: u32 = 3;

/// What one attempt at catching rows still on their way to the circuit made of
/// its run. The variants say only whether there was anything for the checks to
/// assert against; every assertion about the connector stays inside the
/// attempt.
#[derive(Clone, Copy)]
enum Attempt {
    /// The poll caught what the scenario had to catch, and the checks resting
    /// on it ran.
    Caught,
    /// The poll never saw it: the rows reached the circuit whole between two
    /// polls. A wider window is the answer, and a scenario that holds its own
    /// window open has none left to widen.
    Outrun,
    /// The poll caught it, but the run then lost a race it has no lever over,
    /// which left the checks nothing to assert against. Another attempt is the
    /// only answer, whatever the scenario can do about its window.
    Raced,
}

/// Run a scenario that must catch rows still on their way to the circuit, on
/// tables of `base_rows`, then twice and four times as many rows, until
/// `attempt` reports [`Attempt::Caught`]. `attempt` receives the row count,
/// builds its own table and run, and stops that run before reporting anything
/// else. `what` names the scenario and `caught` names what it had to catch, for
/// the messages this helper prints.
///
/// How fast rows reach the circuit is a property of the runner, not of the
/// connector, so an attempt that was outrun or lost its race is reported and
/// the scenario is tried again on a larger table. A runner that outruns or
/// outraces every attempt leaves the scenario unexercised, which the test says
/// instead of going red: red must mean the connector is wrong. A scenario that
/// has to tell a broken setup from a fast runner checks that premise
/// deterministically inside `attempt` rather than reading it off a poll.
fn retry_until_caught_midway(
    what: &str,
    caught: &str,
    base_rows: i64,
    mut attempt: impl FnMut(i64) -> Attempt,
) {
    let mut rows = base_rows;
    let mut raced = false;
    for i in 1..=CATCH_MIDWAY_ATTEMPTS {
        let why = match attempt(rows) {
            Attempt::Caught => return,
            Attempt::Outrun => format!("{caught} of {rows} rows outran the poll"),
            Attempt::Raced => {
                raced = true;
                format!(
                    "the poll caught {caught} of {rows} rows, and the run then lost the race \
                     that follows it"
                )
            }
        };
        if i < CATCH_MIDWAY_ATTEMPTS {
            println!("{what}: {why}; retrying with {} rows", rows * 2);
            rows *= 2;
        } else {
            println!("{what}: {why}");
        }
    }
    let missed = format!(
        "{what}: scenario not exercised: in {CATCH_MIDWAY_ATTEMPTS} attempts, on tables of up to \
         {rows} rows, no attempt reached the checks that rest on catching {caught}. The connector \
         passed every check that ran"
    );
    if raced {
        eprintln!(
            "{missed}; at least one attempt caught {caught} and then lost the race that follows \
             it, which no scenario holds open"
        );
    } else {
        eprintln!("{missed}; this runner is too fast for the window this scenario needs");
    }
}

fn checkpoint_mid_snapshot_waits_for_the_whole_copy(workers: usize) {
    // etl copies the table in 16 ctid partitions (4 copy connections, 4
    // partitions each) and hands the connector each partition as one
    // write_table_rows call, which the connector queues as one snapshot buffer
    // of n / 16 rows: about 50 bytes of JSON per row, below the 2 MiB buffer
    // cap at these row counts. The reader flushes whole buffers per step until
    // it reaches max_batch_size, 10_000 records per worker, and
    // wait_for_etl_copy_in_progress polls the circuit every 10 ms. Whether a
    // poll lands inside the copy depends on the runner. Every measured run on
    // the development machine saw exactly one partition in the circuit: 6_256
    // rows, 190 to 235 ms after etl entered data_sync, in four single-worker
    // runs; 25_024 rows, 330 to 385 ms, in three four-worker runs. A runner
    // that flushes the whole copy between two polls gets the scenario again
    // on a table four times as large, up to three attempts, through
    // retry_until_caught_midway. A first attempt takes about 15 s with one worker
    // and 19 s with four.
    let base_rows: i64 = 100_000 * workers as i64;
    retry_until_caught_midway("scenario 2", "the copy in progress", base_rows, |n| {
        let mut table = scenario_table("cdc_sc_mid_snap");
        insert_range(&mut table, 1, n);
        let storage = TempDir::new().unwrap();

        let run1 =
            Run::start_with::<TestStruct>(&table, storage.path(), &TestStruct::schema(), workers);
        // Measure the part of the copy the circuit holds while etl is still
        // copying. The checkpoint below is requested against that partial
        // state, so what it holds is what the connector's barrier decides.
        let prefix = wait_for_etl_copy_in_progress(
            &mut table,
            || run1.circuit_input_records(),
            n as u64,
            "run 1 checkpoint requested mid-copy",
        );
        run1.assert_no_errors("run 1 mid-copy");
        let Some(prefix) = prefix else {
            // The whole copy reached the circuit before the checkpoint could
            // be requested, so this run says nothing about a deferred
            // checkpoint.
            run1.stop();
            return Attempt::Outrun;
        };
        let checkpoint = run1.controller.checkpoint().unwrap();
        let in_checkpoint = checkpoint
            .input_statistics
            .get("cdc_in")
            .expect("checkpoint has no statistics for cdc_in")
            .circuit_input_records;
        assert_eq!(
            in_checkpoint, n as u64,
            "the checkpoint must hold the whole copy: only {prefix} of {n} rows were in the \
             circuit when it was requested, and the connector reports a barrier until the rest \
             arrive"
        );
        // The barrier lifts once the copy is in the circuit, which is before
        // etl records the copy as finished and hands the table over to
        // streaming. Stopping inside that window would let etl read the table
        // again on the next start, which is legal but not what this test is
        // about.
        wait_for_etl_sync_completed(&mut table);
        let delivered = run1.stop().inserted;
        assert_exactly_once(&delivered, n, "run 1 output");

        // Run 2 resumes from a checkpoint that holds the copy, so it streams
        // from the slot and does not read the table again.
        let run2 =
            Run::start_with::<TestStruct>(&table, storage.path(), &TestStruct::schema(), workers);
        insert_range(&mut table, n + 1, n + 1);
        run2.wait_for_inserts(1, "run 2 streamed row");
        settle();
        let ids = run2.stop().inserted;
        assert_eq!(
            ids,
            vec![n + 1],
            "run 2 must deliver only the row inserted after the restart"
        );
        Attempt::Caught
    });
}

// ---------------------------------------------------------------------------
// Scenario 2b: a suspend requested mid-copy is refused.
// ---------------------------------------------------------------------------

/// Issue #6121, the property scenario 2 rests on. While the initial copy is
/// only partly in the circuit, the controller must refuse to suspend, naming
/// the connector's barrier. A pipeline paused mid-copy keeps that barrier
/// until it is resumed, which is the price of never recording a partial copy.
///
/// The test pauses only once it has measured a non-empty, strictly partial
/// copy in the circuit, so a connector that has fatally errored and is copying
/// nothing cannot pass by reporting a barrier of its own.
#[test]
#[serial]
fn test_suspend_mid_copy_is_refused() {
    // etl hands the connector 16 ctid partitions of n / 16 rows, each one
    // snapshot buffer; the reader flushes whole buffers per step until it
    // reaches max_batch_size, 10_000 records on this single-worker circuit,
    // and wait_for_etl_copy_in_progress polls every 10 ms. In four measured
    // runs on the development machine one partition, 6_256 rows, was in the
    // circuit 180 to 265 ms after etl entered data_sync. A runner that
    // flushes the whole copy between two polls gets the scenario again on a
    // table four times as large, up to three attempts, through
    // retry_until_caught_midway.
    const BASE_ROWS: i64 = 100_000;
    retry_until_caught_midway("scenario 2b", "the copy in progress", BASE_ROWS, |n| {
        let mut table = scenario_table("cdc_sc_partial");
        insert_range(&mut table, 1, n);
        let storage = TempDir::new().unwrap();

        let run = Run::start(&table, storage.path());
        let prefix = wait_for_etl_copy_in_progress(
            &mut table,
            || run.circuit_input_records(),
            n as u64,
            "suspend requested mid-copy",
        );
        run.assert_no_errors("the copy to start");
        let Some(prefix) = prefix else {
            // The whole copy reached the circuit before the pause, so this
            // run says nothing about a suspend requested mid-copy.
            run.stop();
            return Attempt::Outrun;
        };
        // Pausing stops etl from handing over the rest of the copy, so the
        // barrier stays up for as long as the pause does.
        run.controller.pause();
        let blocked = |status: &Result<(), SuspendError>| {
            matches!(
                status,
                Err(SuspendError::Temporary(reasons))
                    if reasons
                        .iter()
                        .any(|r| matches!(r, TemporarySuspendError::InputEndpointBarrier(_)))
            )
        };
        let refused = wait(
            || blocked(&run.controller.can_checkpoint()) || !run.errors.is_empty(),
            WAIT_MS,
        );
        let status = run.controller.can_checkpoint();
        // A connector that died mid-copy also refuses to suspend, so report
        // the error rather than reading it as the barrier this test is about.
        run.assert_no_errors("suspend refused mid-copy");
        // Let the copy finish so the run can stop.
        run.controller.start();
        run.stop();
        refused.unwrap_or_else(|_| {
            panic!(
                "suspending mid-copy must be refused with a barrier, got {status:?} with \
                 {prefix} of {n} rows in the circuit"
            )
        });
        Attempt::Caught
    });
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

/// Checkpoint once the initial copy is in the circuit.
///
/// The connector reports a barrier for every step until then, so the
/// controller defers this checkpoint by itself; waiting for etl to complete
/// the table sync first only keeps the deferral short.
fn checkpoint_after_snapshot(run: &Run, table: &mut CdcTestTable) {
    wait_for_etl_sync_completed(table);
    run.controller.checkpoint().unwrap();
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
                col_real, col_double, col_date, col_time, col_timetz, col_timetz_array,
                col_timestamp, col_timestamptz, col_uuid, col_jsonb,
                col_bytea, col_numeric, col_smallint, col_int_array
            ) VALUES (
                {id}, '{text}', 42, {big}, true,
                3.5, 2.25, '2024-06-15', '14:30:00',
                '14:30:00+02', ARRAY['08:30:00-05'::timetz, NULL],
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
    // REAL travels as f32; 1e-6 is a few f32 rounding steps at 3.5, the
    // tolerance test_cdc_all_data_types uses for the same column.
    assert!((row["col_real"].as_f64().unwrap() - 3.5).abs() < 1e-6);
    assert!((row["col_double"].as_f64().unwrap() - 2.25).abs() < 1e-9);
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
    // TIMETZ replays with the offset it was inserted with; the WAL carries
    // the offset next to the time, so no session zone can rewrite it.
    assert_eq!(row["col_timetz"], json!("14:30:00+02"));
    assert_eq!(row["col_timetz_array"], json!(["08:30:00-05", null]));
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

    // Connector A's output says nothing about table b, which etl copies under
    // the same pipeline: a copy of b that A answered wrongly leaves b errored
    // in etl's state while A's rows all still arrive. So ask etl.
    let deadline = Instant::now() + Duration::from_millis(WAIT_MS as u64);
    loop {
        let states = etl_table_states(&mut table_a);
        if states.len() == 2
            && states
                .iter()
                .all(|s| ETL_SYNC_COMPLETED_STATES.contains(&s.as_str()))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "both tables of connector A's publication must complete their sync; states: {states:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    run_a.assert_no_errors("connector A after streaming");
    run_b.assert_no_errors("connector B after streaming");

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

// ---------------------------------------------------------------------------
// Scenario 11: a checkpoint inside one write must not acknowledge the write.
// ---------------------------------------------------------------------------

/// Issue #7122.
///
/// etl hands rows over as writes the connector queues as several buffers, and
/// the reader flushes those buffers over as many steps. A checkpoint taken
/// between two of those steps holds only a prefix of a write. The connector
/// must not answer etl for a write until its last buffer is in the circuit,
/// because the answer moves etl's flush position past every row of it: a crash
/// after such a checkpoint would then resume from a position that no longer
/// carries the rows the checkpoint lacks.
///
/// Run 1 checkpoints once inside a write and stops without another one, so
/// every row the checkpoint does not hold must come back in run 2.
///
/// One worker, so the circuit takes the rows in the order the source produced
/// them and the checkpoint holds ids `1..=R` for the `R` records it reports.
/// The test checks that prefix property against run 1's output rather than
/// assume it.
///
/// What this test does and does not prove: it drives the whole path, from a
/// checkpoint inside a multi-buffer write to a crash and a resume, and asserts
/// the property a user cares about, that no row is lost. It did not go red
/// against the connector before the fix, because the early answer moved
/// nothing: etl left its flush position untouched for the 45 seconds measured
/// after the checkpoint, so the rows came back from the replication slot
/// regardless. The unit tests of the acknowledgment path are what pin the fix
/// itself.
#[test]
#[serial]
fn test_a_checkpoint_inside_one_write_does_not_acknowledge_it() {
    // Rows are padded so that a few of them carry megabytes: `write_events`
    // cuts a buffer every 2 MiB of serialized JSON, so 160 rows of 128 KiB are
    // about ten buffers. The run below caps the reader at one buffer a step,
    // so those ten buffers reach the circuit over ten steps and the checkpoint
    // has a window to land in. Without that cap the default of 10_000 records
    // a step never binds at 16 rows per buffer, and the window would be only as
    // wide as etl's pacing happened to make it. Measured here: the checkpoint
    // held 17 of 161 records.
    const PAD_BYTES: usize = 128 * 1024;
    retry_until_caught_midway(
        "scenario 11",
        "a write still reaching the circuit",
        160,
        |n| {
            let mut table = scenario_table("cdc_sc_mid_write");
            insert_range(&mut table, 1, 1);
            let storage = TempDir::new().unwrap();

            // One buffer a step, so the write spreads over as many steps as
            // it has buffers instead of arriving in one. That is what leaves a
            // window for the checkpoint to land inside the write.
            let run1 = Run::start_batched(&table, storage.path(), 1);
            run1.wait_for_inserts(1, "run 1 snapshot");
            checkpoint_after_snapshot(&run1, &mut table);

            // The premise, checked deterministically rather than by catching a
            // mid-write poll: one buffer a step means the write advances the
            // step counter by as many buffers as it has. A reader that swallows
            // the whole write in one step fails this before any poll can miss
            // it.
            let steps_before = run1
                .controller
                .status()
                .global_metrics
                .total_initiated_steps();

            // One transaction per row, so etl's flush position crosses a commit
            // between one queued buffer and the next: answering a write whose rows
            // are still queued then moves etl past rows no checkpoint holds.
            insert_padded_rows(&mut table, 2, n + 1, PAD_BYTES);

            // Each retry carries twice the bytes of the last, so the deadline
            // grows with them, but a cap keeps a worst-case run of this serial
            // test bounded.
            let wait_ms = (WAIT_MS * (n / 160).max(1) as u128).min(WAIT_MS * 2);

            // Catch the circuit holding a strict, non-empty part of the write.
            let total = (n + 1) as u64;
            let caught = wait(
                || {
                    let taken = run1.circuit_input_records();
                    taken > 1 && taken < total
                },
                wait_ms,
            )
            .is_ok();
            if !caught {
                // The write reached the circuit whole between two polls, so no
                // checkpoint of this run can fall inside it. Verify the premise
                // before giving up on the attempt: the write must still have
                // spread over several steps, which is the cap's doing. A fast
                // runner is a reason to report; a cap that stopped working is a
                // reason to fail, and only this check tells them apart.
                if wait(|| run1.circuit_input_records() >= total, wait_ms).is_err() {
                    // The write never reached the circuit within the deadline.
                    // A runner that slow is worth a retry, not a red build: red
                    // must mean the connector is wrong.
                    run1.stop();
                    return Attempt::Raced;
                }
                let steps_after = run1
                    .controller
                    .status()
                    .global_metrics
                    .total_initiated_steps();
                assert!(
                    steps_after > steps_before + 1,
                    "the write of {total} records must span several steps, which the reader's \
                     one-buffer-a-step cap guarantees, but the step counter advanced by only {}; \
                     that cap stopped working",
                    steps_after - steps_before
                );
                run1.stop();
                return Attempt::Outrun;
            }

            let checkpoint = run1.controller.checkpoint().unwrap();
            let in_checkpoint = checkpoint
                .input_statistics
                .get("cdc_in")
                .expect("checkpoint has no statistics for cdc_in")
                .circuit_input_records;
            if in_checkpoint >= total {
                // The rest of the write arrived while the checkpoint was being
                // written, so it holds the whole write and leaves run 2 nothing
                // to recover. Nothing here paces the checkpoint against the
                // reader's next steps, so this is a race to retry rather than a
                // window to widen.
                run1.stop();
                return Attempt::Raced;
            }
            run1.assert_no_errors("run 1 mid-write checkpoint");
            println!(
                "scenario 11: {n} padded rows, checkpoint holds {in_checkpoint} of {total} records"
            );

            // Let the rest of the write reach the circuit before stopping, so the
            // stop finds no write still on its way from etl. The answer to the
            // write is what this test is about, and it cannot go out before
            // another checkpoint, which this run never takes.
            if wait(|| run1.circuit_input_records() >= total, wait_ms).is_err() {
                // The rest of the write did not reach the circuit within the
                // deadline, which says this runner is slow, not that the
                // connector dropped rows. Retry rather than go red.
                run1.stop();
                return Attempt::Raced;
            }
            run1.assert_no_errors("run 1 after the write landed");

            // Stop without a second checkpoint, as a crash would.
            let seen_in_run1 = run1.stop().inserted;
            assert_prefix_of_ids(&seen_in_run1, "run 1 output");

            let run2 = Run::start(&table, storage.path());
            let first_missing = in_checkpoint as i64 + 1;
            // A runner slow enough to need longer than the window buys more of
            // it by making progress. Only a run 2 that has stopped delivering
            // without the rows lets the assertion below speak.
            wait_while_delivering(
                wait_ms,
                || {
                    let h = insert_histogram(run2.inserted_ids());
                    (first_missing..=n + 1).all(|id| h.contains_key(&id))
                },
                || run2.insert_count(),
            );
            run2.assert_no_errors("run 2");
            let ids = run2.stop().inserted;

            let h = insert_histogram(ids.iter().copied());
            let missing: Vec<i64> = (first_missing..=n + 1)
                .filter(|id| !h.contains_key(id))
                .collect();
            assert!(
                missing.is_empty(),
                "the checkpoint holds ids 1..={in_checkpoint} of {total}, so run 2 must redeliver \
             every id from {first_missing} on; {} are missing, e.g. {}. The connector answered \
             etl for a write the checkpoint does not hold in full, and etl moved its flush \
             position past the rest",
                missing.len(),
                preview(&missing)
            );
            Attempt::Caught
        },
    );
}

/// Assert that the distinct ids in `ids` are the gap-free prefix `1..=k`, which
/// is what makes a record count comparable with an id. Counting distinct ids
/// rather than deliveries keeps a redelivered row, which at-least-once allows,
/// from demanding ids that were never inserted.
fn assert_prefix_of_ids(ids: &[i64], what: &str) {
    let h = insert_histogram(ids.iter().copied());
    let distinct = h.len() as i64;
    let missing: Vec<i64> = (1..=distinct).filter(|id| !h.contains_key(id)).collect();
    assert!(
        missing.is_empty(),
        "{what}: expected the {distinct} distinct ids delivered so far to be the gap-free prefix \
         1..={distinct}, but {} are missing, e.g. {}. The circuit did not take the rows in source \
         order, so a record count no longer names an id",
        missing.len(),
        preview(&missing)
    );
}

/// Extra windows `wait_while_delivering` grants a runner that is still
/// delivering rows. The cap is what keeps a pipeline that delivers for ever,
/// without ever delivering the rows the caller waits for, from hanging the
/// whole serial module instead of failing one assertion.
const EXTRA_DELIVERY_WINDOWS: u32 = 3;

/// Wait for `done`, giving up once `delivered` stops moving or after
/// [`EXTRA_DELIVERY_WINDOWS`] further windows of `wait_ms`, whichever comes
/// first. The caller states the window, because a scenario that doubles its
/// payload on a retry needs a deadline that doubles with it.
///
/// A row count that is still rising means the pipeline is working and the
/// runner is merely slow, which is a reason to keep waiting rather than to
/// fail. A count that stands still through a whole window means waiting longer
/// would not change the answer. Returning either way leaves the caller's
/// assertion to speak, so the worst case is a red test rather than a job that
/// hangs with nothing to read.
fn wait_while_delivering(wait_ms: u128, done: impl Fn() -> bool, delivered: impl Fn() -> usize) {
    let mut last = delivered();
    for _ in 0..=EXTRA_DELIVERY_WINDOWS {
        if wait(&done, wait_ms).is_ok() {
            return;
        }
        let now = delivered();
        if now == last {
            return;
        }
        last = now;
    }
}

/// Insert ids `from..=to`, one transaction per row, padding each row to about
/// `pad_bytes` so that a few rows fill the connector's buffers.
///
/// One transaction per row matters: PostgreSQL decodes a transaction whole, so
/// a flush position inside one still replays all of it. Only a position that
/// has passed a commit can drop the rows behind it.
fn insert_padded_rows(table: &mut CdcTestTable, from: i64, to: i64, pad_bytes: usize) {
    for id in from..=to {
        table.execute(&format!(
            "INSERT INTO {} VALUES ({id}, {}, {}, repeat('x', {pad_bytes}))",
            table.table_name,
            id % 2 == 0,
            id * 10
        ));
    }
}
