//! An exhaustive test framework for [`RootCircuit::add_lazy_input_map`].
//!
//! A program is a list of commands grouped into steps and steps into
//! transactions.  [`check`] runs one through a circuit holding both the lazy map
//! and the eager [`add_input_map`](RootCircuit::add_input_map), and
//! checks four things after every transaction:
//!
//! | Check | What it catches |
//! |-------|-----------------|
//! | The deltas sum to the map a model computes | a wrong value, a lost or duplicated record |
//! | The integral holds that same map | a delta that is right while the state feeding the next transaction is not |
//! | A transaction's deltas name only the keys it wrote | output for a key the transaction never touched |
//! | Every emitted batch is consolidated | a builder handed a value twice, or a weight of zero |
//!
//! The eager map runs beside the lazy one and faces the same checks, which is
//! what keeps the model honest: the two operators are required to agree, so a
//! model that has drifted from the intended semantics fails on both rather than
//! on one.
//!
//! [`check_every_program`] supplies the exhaustive part and [`Config`] the
//! second dimension.  Every configuration runs the same enumeration, and the
//! configurations differ in how the updates reach the operator and what it does
//! with its output: how many workers divide the keys, whether a step arrives as
//! one append or several, how many records a chunk holds, whether batches live
//! in memory or in files, how many keys it resolves before it yields, and
//! whether the circuit takes input while it commits.
//!
//! [`staged`] covers the same ground from the other end, with hand-written
//! appends rather than an enumeration: it is the only path on which a worker
//! sees more than one input vector, so it is the only one where the merge in
//! `Stamp` decides anything.
//!
//! # Validation
//!
//! Each defect below was injected into the operator, one at a time, and the
//! tests rerun.  Every one of them failed several tests, including at least one
//! that runs a single transaction.
//!
//! * The integral's record for a written key is not retracted.
//! * A displaced running maximum is not retracted.
//! * A surviving delete does not cancel its record.
//! * The earliest update of a transaction wins rather than the latest.
//! * A key's adjustments reach the builder unconsolidated.
//! * The stamp does not advance from one step to the next.
//! * The last output chunk never reaches the delta stream.
//! * The last output chunk never reaches the integral.
//! * The adjustments never reach the delta stream.
//! * The merge in `Stamp` takes the last update among equal keys, not the first.
//! * The merge drops an exhausted vector with `swap_remove`, reordering the rest.
//! * The first update to a key wins rather than the last.
//! * The key under construction is never marked present.
//! * The sort that feeds `Stamp` orders by the whole pair rather than the key.
//! * The updates are exchanged again after arriving sharded.
//! * The last chunk of adjustments is never emitted.
//! * A step's adjustments never reach the accumulator.
//! * A step's adjustments never reach the delta stream.
//! * Only a full chunk ends a step, never a run of keys that fills none.
//! * A step reports completion before the operator has finished.

use super::*;
use crate::circuit::{CircuitConfig, CircuitStorageConfig};
use crate::operator::dynamic::input_upsert::Update;
use crate::operator::input::{LazyMapHandle, MapHandle, StagedBuffers};
use crate::trace::Cursor;
use crate::typed_batch::{BatchReader as _, OrdIndexedZSet as TypedIndexedZSet};
use crate::{Runtime, Stream};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Mutex;
use tempfile::TempDir;

/// The map under test, and the eager map it is checked against.
type Map = TypedIndexedZSet<i32, i32>;

/// A write, `Some(value)`, or a delete, `None`.
type Command = (i32, Option<i32>);

/// Commands grouped into steps and steps into transactions.
type Program = Vec<Vec<Vec<Command>>>;

/// One batch as the delta stream emitted it, in cursor order.
type Batch = Vec<((i32, i32), ZWeight)>;

/// The map a program leaves behind: the last write to a key wins, and a delete
/// removes it.
#[derive(Clone, Default)]
struct Model(BTreeMap<i32, i32>);

impl Model {
    fn apply(&mut self, commands: &[Command]) {
        for &(key, value) in commands {
            match value {
                Some(value) => self.0.insert(key, value),
                None => self.0.remove(&key),
            };
        }
    }

    /// The map as the Z-set the deltas sum to: one record per key, weight one.
    fn zset(&self) -> BTreeMap<(i32, i32), ZWeight> {
        self.0
            .iter()
            .map(|(&key, &value)| ((key, value), 1))
            .collect()
    }
}

/// The knobs that steer the operator down different code paths.
#[derive(Clone, Copy)]
struct Config {
    workers: usize,

    /// Records per output chunk.  One cuts a chunk at nearly every key, which
    /// no program here is large enough to reach by default.
    chunk_size: Option<u64>,

    /// Storage spills batches to files, so the operator projects file-backed
    /// batches rather than in-memory ones.
    storage: bool,

    /// Whether the circuit accepts input while a transaction commits.  Turning
    /// it off lets the scheduler take a shorter path through the commit, which
    /// is the step the operator does all of its work in.
    input_during_commit: bool,

    /// How long the map may hold a step, in microseconds.  Zero ends a step on
    /// every key, whichever way the adjustments fall, which no program here
    /// reaches by default.
    usecs_per_step: Option<u64>,

    /// Whether a step's commands arrive as several staged appends rather than
    /// one.  `LazyMapHandle::append` concatenates into one vector per worker, so
    /// only `LazyMapHandle::stage` gives a step more than one, which is what makes
    /// the merge in `Stamp` do any merging.
    staged: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            workers: 1,
            chunk_size: None,
            storage: false,
            input_during_commit: true,
            usecs_per_step: None,
            staged: false,
        }
    }
}

impl Config {
    fn circuit_config(&self, storage_dir: &Path) -> CircuitConfig {
        let mut config = CircuitConfig::with_workers(self.workers)
            .with_allow_input_during_commit(self.input_during_commit);
        if let Some(chunk_size) = self.chunk_size {
            config = config.with_splitter_chunk_size_records(chunk_size);
        }
        if let Some(usecs_per_step) = self.usecs_per_step {
            config = config.with_operator_usecs_per_step(usecs_per_step);
        }
        if self.storage {
            config = config.with_storage(Some(
                CircuitStorageConfig::for_config(
                    StorageConfig {
                        path: storage_dir.to_string_lossy().into_owned(),
                        cache: StorageCacheConfig::default(),
                    },
                    StorageOptions {
                        // Send every batch to a file, so the projection runs
                        // over file-backed batches rather than in-memory ones.
                        min_storage_bytes: Some(0),
                        ..StorageOptions::default()
                    },
                )
                .unwrap(),
            ));
        }
        config
    }
}

/// Writes a step's commands to one of the two maps.
///
/// The maps take them through different handles.  The lazy one's cannot be sent
/// an `Update`, so it takes an option; the eager one's takes an `Update` whose
/// third variant this framework never writes.  A test drives both from one list
/// of commands, so each handle says how to take them.
trait Push {
    fn append_commands(&mut self, commands: &[Command]);

    /// One staged append, which reaches each worker as a vector of its own.
    fn stage_commands(&self, commands: &[Command]);
}

impl Push for LazyMapHandle<i32, i32> {
    fn append_commands(&mut self, commands: &[Command]) {
        let mut pairs: Vec<Tup2<i32, Option<i32>>> =
            commands.iter().map(|&(k, v)| Tup2(k, v)).collect();
        self.append(&mut pairs);
    }

    fn stage_commands(&self, commands: &[Command]) {
        let pairs: Vec<Tup2<i32, Option<i32>>> =
            commands.iter().map(|&(k, v)| Tup2(k, v)).collect();
        self.stage([VecDeque::from(pairs)]).flush();
    }
}

impl Push for MapHandle<i32, i32, i32> {
    fn append_commands(&mut self, commands: &[Command]) {
        let mut pairs: Vec<Tup2<i32, Update<i32, i32>>> =
            commands.iter().map(|&(k, v)| Tup2(k, update(v))).collect();
        self.append(&mut pairs);
    }

    fn stage_commands(&self, commands: &[Command]) {
        let pairs: Vec<Tup2<i32, Update<i32, i32>>> =
            commands.iter().map(|&(k, v)| Tup2(k, update(v))).collect();
        self.stage([VecDeque::from(pairs)]).flush();
    }
}

fn update(value: Option<i32>) -> Update<i32, i32> {
    match value {
        Some(value) => Update::Insert(value),
        None => Update::Delete,
    }
}

/// The number of staged appends a step is split into, when it is split at all.
///
/// Three is enough for a key written twice in one step to have its writes land
/// in different appends, which is the order the merge in `Stamp` has to keep.
const APPENDS_PER_STEP: usize = 3;

fn push_staged(handle: &impl Push, commands: &[Command]) {
    handle.stage_commands(commands);
}

fn push(handle: &mut impl Push, commands: &[Command], staged: bool) {
    if !staged {
        handle.append_commands(commands);
        return;
    }

    // Contiguous chunks, so the order the appends go out in is the order the
    // commands were written in.
    let chunk = commands.len().div_ceil(APPENDS_PER_STEP).max(1);
    for commands in commands.chunks(chunk) {
        handle.stage_commands(commands);
    }
}

/// Collects every batch a stream emits.  With several workers a step emits one
/// batch per worker, and each lands here on its own.
fn record(stream: &Stream<RootCircuit, Map>, into: Arc<Mutex<Vec<Batch>>>) {
    stream.inspect(move |batch| {
        let mut records = Vec::new();
        let mut cursor = batch.inner().cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let key = *unsafe { cursor.key().downcast::<i32>() };
                let value = *unsafe { cursor.val().downcast::<i32>() };
                records.push(((key, value), **cursor.weight()));
                cursor.step_val();
            }
            cursor.step_key();
        }
        into.lock().unwrap().push(records);
    });
}

/// Collects the integral of a stream, which is what an operator downstream of
/// the map reads: a join or an aggregate over the table sees this, not the
/// deltas.  Only the latest step's contents are kept, since that is the only one
/// a transaction boundary asks about, and each worker holds the shard of the map
/// its keys hash to.
///
/// The map caches its integral under the delta stream's id, so this finds that
/// one rather than building a second from the deltas.  The factories are what a
/// miss would build from, and a miss would make this check say nothing new: an
/// integral summed from the deltas agrees with them by construction.
fn record_integral(stream: &Stream<RootCircuit, Map>, into: Arc<Mutex<Vec<Batch>>>) {
    stream
        .accumulate_integrate_trace()
        // `apply` rather than `inspect`: a `Spine` cannot be cloned, and
        // `inspect` clones what it passes through.
        .apply(move |spine| {
            let mut records = Vec::new();
            let mut cursor = spine.inner().cursor();
            while cursor.key_valid() {
                while cursor.val_valid() {
                    let key = *unsafe { cursor.key().downcast::<i32>() };
                    let value = *unsafe { cursor.val().downcast::<i32>() };
                    records.push(((key, value), **cursor.weight()));
                    cursor.step_val();
                }
                cursor.step_key();
            }
            let mut shards = into.lock().unwrap();
            let worker = Runtime::worker_index();
            if shards.len() <= worker {
                shards.resize(worker + 1, Vec::new());
            }
            shards[worker] = records;
        });
}

/// The collection a run of batches describes.
fn collection(batches: &[Batch]) -> BTreeMap<(i32, i32), ZWeight> {
    let mut sums: BTreeMap<(i32, i32), ZWeight> = BTreeMap::new();
    for batch in batches {
        for &(record, weight) in batch {
            *sums.entry(record).or_default() += weight;
        }
    }
    sums.retain(|_, weight| *weight != 0);
    sums
}

/// Names a key's history, so a failure in a run of thousands of keys says which
/// program failed rather than only which key.
type Explain<'a> = &'a dyn Fn(i32) -> String;

/// Checks a transaction's output from one of the two maps.
fn check_transaction(
    map: &str,
    batches: &[Batch],
    since: usize,
    written: &BTreeSet<i32>,
    model: &Model,
    explain: Explain,
) {
    for batch in &batches[since..] {
        assert!(
            batch.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "the {map} map emitted a batch that is not consolidated: {batch:?}"
        );
        assert!(
            batch.iter().all(|&(_, weight)| weight != 0),
            "the {map} map emitted a batch holding a weight of zero: {batch:?}"
        );
        for &((key, _), _) in batch {
            assert!(
                written.contains(&key),
                "the {map} map emitted a record for key {key}, \
                 which the transaction never wrote{}",
                explain(key)
            );
        }
    }

    check_collection(
        &format!("the {map} map's deltas"),
        &collection(batches),
        &model.zset(),
        explain,
    );
}

/// Compares a collection against the model, naming the keys that differ.
fn check_collection(
    what: &str,
    emitted: &BTreeMap<(i32, i32), ZWeight>,
    expected: &BTreeMap<(i32, i32), ZWeight>,
    explain: Explain,
) {
    if emitted != expected {
        let mut differing: Vec<i32> = emitted
            .iter()
            .filter(|&(record, weight)| expected.get(record) != Some(weight))
            .chain(
                expected
                    .iter()
                    .filter(|&(record, weight)| emitted.get(record) != Some(weight)),
            )
            .map(|((key, _), _)| *key)
            .collect();
        differing.sort_unstable();
        differing.dedup();
        let histories: String = differing.iter().map(|&key| explain(key)).collect();
        panic!(
            "{what} does not hold the collection the commands describe\n\
             differing keys: {differing:?}{histories}\n\
             emitted:  {:?}\n\
             expected: {:?}",
            emitted
                .iter()
                .filter(|(record, _)| differing.binary_search(&record.0).is_ok())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .filter(|(record, _)| differing.binary_search(&record.0).is_ok())
                .collect::<Vec<_>>(),
        );
    }
}

/// Runs `program` under `config` and checks it.
fn check(program: &Program, config: Config) {
    check_explained(program, config, &|_| String::new());
}

/// Runs `program` under `config` and checks both maps after every transaction.
fn check_explained(program: &Program, config: Config, explain: Explain) {
    let lazy_batches = Arc::new(Mutex::new(Vec::new()));
    let eager_batches = Arc::new(Mutex::new(Vec::new()));
    let lazy_integral = Arc::new(Mutex::new(Vec::new()));
    let (lazy_out, eager_out) = (lazy_batches.clone(), eager_batches.clone());
    let integral_out = lazy_integral.clone();

    let storage_dir = TempDir::new().expect("cannot create a directory for storage");
    let (mut dbsp, (mut lazy_handle, mut eager_handle)) =
        Runtime::init_circuit(config.circuit_config(storage_dir.path()), move |circuit| {
            let (lazy, lazy_handle) = circuit.add_lazy_input_map::<i32, i32>();
            record(&lazy, lazy_out.clone());
            record_integral(&lazy, integral_out.clone());

            let (eager, eager_handle) = circuit.add_input_map::<i32, i32, i32, _>(|_, _| {
                unreachable!("no `Update` commands are sent")
            });
            record(&eager, eager_out.clone());

            Ok((lazy_handle, eager_handle))
        })
        .unwrap();

    let mut model = Model::default();
    let (mut lazy_since, mut eager_since) = (0, 0);

    for transaction in program {
        dbsp.start_transaction().unwrap();
        for step in transaction {
            push(&mut lazy_handle, step, config.staged);
            push(&mut eager_handle, step, config.staged);
            dbsp.step().unwrap();
        }
        dbsp.commit_transaction().unwrap();

        // The commit has returned, so every worker has emitted everything this
        // transaction produces and the recorded batches can be read.
        model.apply(&transaction.concat());
        let written: BTreeSet<i32> = transaction.iter().flatten().map(|&(key, _)| key).collect();

        let lazy = lazy_batches.lock().unwrap();
        check_transaction("lazy", &lazy, lazy_since, &written, &model, explain);
        lazy_since = lazy.len();
        drop(lazy);

        let eager = eager_batches.lock().unwrap();
        check_transaction("eager", &eager, eager_since, &written, &model, explain);
        eager_since = eager.len();
        drop(eager);

        check_collection(
            "the integral a downstream operator reads",
            &collection(&lazy_integral.lock().unwrap()),
            &model.zset(),
            explain,
        );
    }

    dbsp.kill().unwrap();
}

/// The commands a program can be built from.
///
/// Two values, so that rewriting a key with the value it already holds is
/// distinguishable from rewriting it with another, and zero among them, because
/// a delete travels as a record holding the value type's default: a program that
/// writes zero and then deletes makes the two collide.
const ALPHABET: [Option<i32>; 3] = [Some(0), Some(1), None];

/// Every program of `length` commands over one key, laid out in one circuit.
///
/// A program is a command sequence plus a shape, which says where the boundaries
/// fall: after each command the next either joins it in the same step, opens a
/// new step, or opens a new transaction.  For `length` commands over the
/// three-command [`ALPHABET`] that is `3^length` sequences times `3^(length-1)`
/// shapes.
///
/// They share a circuit because they cannot interfere: each program writes a key
/// of its own.  One schedule holds all of them, since a program's command `i`
/// belongs in the transaction counting the transaction boundaries before it and
/// the step counting the step boundaries since the last transaction boundary,
/// and neither count can reach `length`.  A schedule of `length` transactions of
/// `length` steps therefore has a place for every command of every shape.
fn every_program(length: usize) -> Program {
    assert!(length >= 1);
    let mut schedule = vec![vec![Vec::new(); length]; length];
    for key in 0..program_count(length) {
        let (mut sequence, mut shape) = decode(key, length);
        let (mut transaction, mut step) = (0, 0);
        for command in 0..length {
            schedule[transaction][step].push((key, ALPHABET[sequence % ALPHABET.len()]));
            sequence /= ALPHABET.len();
            if command + 1 < length {
                match shape % BOUNDARIES {
                    SAME_STEP => {}
                    NEW_STEP => step += 1,
                    _ => {
                        transaction += 1;
                        step = 0;
                    }
                }
                shape /= BOUNDARIES;
            }
        }
    }
    schedule
}

/// What can follow a command: another in the same step, one in a new step, or
/// one in a new transaction.
const BOUNDARIES: usize = 3;
const SAME_STEP: usize = 0;
const NEW_STEP: usize = 1;

/// Splits a key into the command sequence and the shape it stands for.
fn decode(key: i32, length: usize) -> (usize, usize) {
    let sequences = ALPHABET.len().pow(length as u32);
    (key as usize % sequences, key as usize / sequences)
}

/// How many programs of `length` commands [`every_program`] lays out.
fn program_count(length: usize) -> i32 {
    let sequences = ALPHABET.len().pow(length as u32);
    let shapes = BOUNDARIES.pow(length as u32 - 1);
    (sequences * shapes) as i32
}

/// The program [`every_program`] gave `key`, written out for a failure message.
///
/// A comma joins two commands in one step, `|` opens a step, and `||` opens a
/// transaction.
fn program_of(key: i32, length: usize) -> String {
    let (mut sequence, mut shape) = decode(key, length);
    let mut program = format!("\n  key {key}: ");
    for command in 0..length {
        program += &match ALPHABET[sequence % ALPHABET.len()] {
            Some(value) => format!("write {value}"),
            None => String::from("delete"),
        };
        sequence /= ALPHABET.len();
        if command + 1 < length {
            program += match shape % BOUNDARIES {
                SAME_STEP => ", ",
                NEW_STEP => " | ",
                _ => " || ",
            };
            shape /= BOUNDARIES;
        }
    }
    program
}

/// Runs every program of `length` commands under `config`.
fn check_every_program(length: usize, config: Config) {
    check_explained(&every_program(length), config, &|key| {
        program_of(key, length)
    });
}

/// Every program of one to four commands, which covers every way up to four
/// writes and deletes of one key can be spread over steps and transactions.
#[test]
fn every_short_program() {
    for length in 1..=4 {
        check_every_program(length, Config::default());
    }
}

/// Every program of five commands, which is where a key's history first holds a
/// run of writes on either side of a delete and still crosses two transaction
/// boundaries.
#[test]
fn every_five_command_program() {
    check_every_program(5, Config::default());
}

/// Every short program with the keys spread over four workers, where each worker
/// resolves its own share of the transaction against its own integral.
#[test]
fn every_short_program_across_four_workers() {
    for length in 1..=4 {
        check_every_program(
            length,
            Config {
                workers: 4,
                ..Config::default()
            },
        );
    }
}

/// Every short program with the output chunked at a record, which cuts a chunk
/// at nearly every key rather than at none of them.
#[test]
fn every_short_program_chunked_at_every_record() {
    for length in 1..=4 {
        check_every_program(
            length,
            Config {
                chunk_size: Some(1),
                ..Config::default()
            },
        );
    }
}

/// Every short program with storage on, so the batches the operator projects
/// are file-backed rather than in memory.
#[test]
fn every_short_program_on_storage() {
    for length in 1..=4 {
        check_every_program(
            length,
            Config {
                storage: true,
                ..Config::default()
            },
        );
    }
}

/// Every short program whose steps arrive as several staged appends, so a key
/// written twice in one step has its writes in different vectors and the merge
/// in `Stamp` decides which one lands last.
#[test]
fn every_short_program_in_staged_appends() {
    for length in 1..=4 {
        check_every_program(
            length,
            Config {
                staged: true,
                ..Config::default()
            },
        );
    }
}

/// The same, sharded, where a worker's vectors come from its own share of each
/// append rather than from the whole of it.
#[test]
fn every_short_program_in_staged_appends_across_four_workers() {
    for length in 1..=4 {
        check_every_program(
            length,
            Config {
                staged: true,
                workers: 4,
                ..Config::default()
            },
        );
    }
}

/// The map builds no exchange on a single host.
///
/// `UpsertHandle` hashes each key to a worker as the client appends, so the
/// updates reach the circuit already sharded.  An exchange here would be a round
/// of communication that moves every record to the worker it is already on.
#[test]
fn a_single_host_map_builds_no_exchange() {
    let (dbsp, _handle) = Runtime::init_circuit(CircuitConfig::with_workers(4), |circuit| {
        let (delta, handle) = circuit.add_lazy_input_map::<i32, i32>();
        delta.inspect(|_| {});
        let graph = circuit.to_dot(
            |node| {
                Some(crate::utils::DotNodeAttributes::new().with_label(&format!("{}", node.name())))
            },
            |_| None,
        );
        assert!(
            !graph.contains("Exchange"),
            "the map exchanged updates that arrived sharded:\n{graph}"
        );
        assert!(
            !graph.contains("ApplyOwned"),
            "the map put a hop in front of `Stamp`, which sorts its own input:\n{graph}"
        );
        Ok(handle)
    })
    .unwrap();
    dbsp.kill().unwrap();
}

/// A downstream operator asking for the map's integral finds the one the map
/// already built.
///
/// The map caches its integral under the stream's id, and an operator that wants
/// the table's state looks it up there.  A key that does not match is not an
/// error: the lookup builds a second integral from the delta stream instead, so
/// the circuit carries two copies of the table and every check against "the
/// integral" compares the deltas with themselves.  Counting the nodes the lookup
/// adds is what tells the two apart.
///
/// Both spellings are checked because both have to resolve here.  The sharded
/// one hands a sharded stream to the plain one, and on a single host this stream
/// is sharded.
#[test]
fn a_downstream_operator_finds_the_maps_integral() {
    fn nodes(circuit: &RootCircuit) -> usize {
        circuit
            .to_dot(
                |_| Some(crate::utils::DotNodeAttributes::new().with_label("n")),
                |_| None,
            )
            .matches("[label=")
            .count()
    }

    for workers in [1, 4] {
        let (dbsp, _handle) =
            Runtime::init_circuit(CircuitConfig::with_workers(workers), |circuit| {
                let (delta, handle) = circuit.add_lazy_input_map::<i32, i32>();
                delta.inspect(|_| {});

                let before = nodes(circuit);
                let plain = delta.accumulate_integrate_trace();
                let sharded = delta.shard_accumulate_integrate_trace();
                assert_eq!(
                    nodes(circuit),
                    before,
                    "asking for the integral built another one instead of finding the map's"
                );

                plain.apply(|_| {});
                sharded.apply(|_| {});
                Ok(handle)
            })
            .unwrap();
        dbsp.kill().unwrap();
    }
}

/// The integral carries the persistent id a restart looks it up by.
///
/// A persistent id names an operator's state across restarts, so renaming one
/// silently drops the state it named: the circuit comes back without it and
/// rebuilds from nothing.  The integral is the map's only persistent state, and
/// `accintegral` is what an integral built on `AccumulateZ1Trace` is called
/// throughout the tree.
#[test]
fn the_integral_is_named_for_a_restart() {
    let (dbsp, _handle) = Runtime::init_circuit(CircuitConfig::with_workers(1), |circuit| {
        let (delta, handle) = circuit.add_lazy_input_map_persistent::<i32, i32>(Some("table"));
        delta.inspect(|_| {});
        let graph = circuit.to_dot(
            |node| {
                Some(
                    crate::utils::DotNodeAttributes::new()
                        .with_label(node.get_label("persistent_id").unwrap_or_default()),
                )
            },
            |_| None,
        );
        assert!(
            graph.contains("table.accintegral"),
            "the map's integral is not named `table.accintegral`:\n{graph}"
        );
        Ok(handle)
    })
    .unwrap();
    dbsp.kill().unwrap();
}

/// Every short program on a circuit that takes no input while a transaction
/// commits, which is what a client that stops writing before it commits allows.
#[test]
fn every_short_program_without_input_during_commit() {
    for length in 1..=4 {
        check_every_program(
            length,
            Config {
                input_during_commit: false,
                ..Config::default()
            },
        );
    }
}

/// Every short program with the map yielding on every key, so a transaction
/// resolves over several steps whatever its adjustments look like.
#[test]
fn every_short_program_yielding_on_every_key() {
    for length in 1..=4 {
        check_every_program(
            length,
            Config {
                usecs_per_step: Some(0),
                ..Config::default()
            },
        );
    }
}

/// Every short program under all five knobs at once, which is the only
/// configuration where a file-backed batch is chunked and sharded as well.
#[test]
fn every_short_program_under_every_knob() {
    for length in 1..=4 {
        check_every_program(
            length,
            Config {
                workers: 4,
                chunk_size: Some(1),
                storage: true,
                input_during_commit: false,
                usecs_per_step: Some(0),
                staged: true,
            },
        );
    }
}

/// Programs longer than the enumeration reaches, over keys few enough that they
/// collide often.
fn a_random_program() -> impl Strategy<Value = Program> {
    let command = (0i32..4, prop_oneof![Just(None), (0i32..3).prop_map(Some)]);
    let step = prop::collection::vec(command, 0..4);
    let transaction = prop::collection::vec(step, 1..4);
    prop::collection::vec(transaction, 1..6)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A random program of up to five transactions over four keys.
    #[test]
    fn a_random_program_agrees(program in a_random_program()) {
        check(&program, Config::default());
    }

    /// The same under all four knobs at once.
    #[test]
    fn a_random_program_agrees_under_every_knob(program in a_random_program()) {
        check(
            &program,
            Config {
                workers: 4,
                chunk_size: Some(1),
                storage: true,
                input_during_commit: false,
                usecs_per_step: Some(0),
                staged: true,
            },
        );
    }
}

/// One write in one transaction: the smallest thing the circuit can do.
#[test]
fn a_single_write() {
    check(&vec![vec![vec![(1, Some(10))]]], Config::default());
}

/// A key written again in a later transaction, which is where the integral holds
/// the value being replaced.
#[test]
fn a_key_rewritten_across_transactions() {
    check(
        &vec![
            vec![vec![(1, Some(10))]],
            vec![vec![(1, Some(20))]],
            vec![vec![(1, Some(30))]],
        ],
        Config::default(),
    );
}

/// Deleting a key the integral holds, and one it does not.
#[test]
fn deletes() {
    check(
        &vec![vec![vec![(1, Some(10))]], vec![vec![(1, None), (2, None)]]],
        Config::default(),
    );
}

/// Steps and transactions carrying no commands at all, which the enumeration
/// never produces because it fills every step of its schedule.
#[test]
fn empty_steps_and_transactions() {
    check(
        &vec![
            vec![vec![]],
            vec![vec![(1, Some(10))], vec![]],
            vec![vec![]],
            vec![vec![(1, None)]],
        ],
        Config::default(),
    );
}

/// More keys than one output chunk holds, so a transaction's adjustments reach
/// the integral as several batches.
#[test]
fn more_keys_than_a_chunk_holds() {
    let write: Vec<Command> = (0..50).map(|key| (key, Some(key * 10))).collect();
    let rewrite: Vec<Command> = (0..50).map(|key| (key, Some(key * 10 + 1))).collect();
    let delete: Vec<Command> = (0..50).step_by(2).map(|key| (key, None)).collect();
    check(
        &vec![vec![write], vec![rewrite], vec![delete]],
        Config {
            chunk_size: Some(7),
            ..Config::default()
        },
    );
}

/// A transaction that writes every key of a large map, which is the shape the
/// operator exists for: one ordered pass over the integral rather than a probe
/// for each key.
#[test]
fn a_transaction_that_rewrites_a_large_map() {
    let write: Vec<Command> = (0..2000).map(|key| (key, Some(key))).collect();
    let rewrite: Vec<Command> = (0..2000).map(|key| (key, Some(-key))).collect();
    check(
        &vec![vec![write], vec![rewrite]],
        Config {
            workers: 4,
            ..Config::default()
        },
    );
}

/// The enumeration lays out the program it promises for each key.
///
/// `every_program` is the framework's own coverage claim, so the layout is
/// checked directly rather than only through the circuit: a schedule that
/// quietly dropped commands would leave every test passing over less than it
/// reports.
#[test]
fn the_enumeration_lays_out_every_program() {
    for length in 1..=4 {
        let schedule = every_program(length);
        assert_eq!(schedule.len(), length);
        assert!(
            schedule
                .iter()
                .all(|transaction| transaction.len() == length)
        );

        let commands: Vec<Command> = schedule.iter().flatten().flatten().copied().collect();
        assert_eq!(commands.len(), program_count(length) as usize * length);

        // Every key runs its whole program, and no key appears that the
        // enumeration did not hand out.
        let mut per_key: BTreeMap<i32, usize> = BTreeMap::new();
        for (key, _) in commands {
            *per_key.entry(key).or_default() += 1;
        }
        assert_eq!(per_key.len(), program_count(length) as usize);
        assert!(per_key.values().all(|&count| count == length));
    }

    // A program's commands arrive in order, in the transactions and steps its
    // shape asks for.  Key 0 takes the first symbol at every position and the
    // first shape, which keeps everything in one step.
    assert_eq!(
        every_program(3)[0][0]
            .iter()
            .filter(|&&(key, _)| key == 0)
            .count(),
        3
    );
    assert_eq!(program_of(0, 3), "\n  key 0: write 0, write 0, write 0");

    // The last key takes the last symbol at every position and the last shape,
    // which puts every command in a transaction of its own.
    let last = program_count(3) - 1;
    assert_eq!(
        program_of(last, 3),
        "\n  key 242: delete || delete || delete"
    );
    for transaction in 0..3 {
        assert_eq!(
            every_program(3)[transaction][0]
                .iter()
                .filter(|&&(key, _)| key == last)
                .count(),
            1
        );
    }
}

/// Updates that reach a worker as several vectors.
///
/// `LazyMapHandle::append` concatenates everything a client writes in a step into
/// one vector per worker, so `Stamp` has nothing to merge.  `LazyMapHandle::stage`
/// hands each call its own vector, and the last update to a key then lives in
/// whichever vector wrote it last.  These tests drive that path directly, with
/// control over how the appends are shaped, rather than through the chunking
/// [`push`] applies.
mod staged {
    use super::*;

    /// Runs steps whose commands arrive as several staged appends, and checks
    /// both maps against the model when the transaction commits.
    fn check_appends(steps: &[Vec<Vec<Command>>], workers: usize) {
        let lazy_batches = Arc::new(Mutex::new(Vec::new()));
        let eager_batches = Arc::new(Mutex::new(Vec::new()));
        let (lazy_out, eager_out) = (lazy_batches.clone(), eager_batches.clone());

        let (mut dbsp, (lazy_handle, eager_handle)) =
            Runtime::init_circuit(CircuitConfig::with_workers(workers), move |circuit| {
                let (lazy, lazy_handle) = circuit.add_lazy_input_map::<i32, i32>();
                record(&lazy, lazy_out.clone());

                let (eager, eager_handle) = circuit.add_input_map::<i32, i32, i32, _>(|_, _| {
                    unreachable!("no `Update` commands are sent")
                });
                record(&eager, eager_out.clone());

                Ok((lazy_handle, eager_handle))
            })
            .unwrap();

        let mut model = Model::default();
        dbsp.start_transaction().unwrap();
        for appends in steps {
            for commands in appends {
                push_staged(&lazy_handle, commands);
                push_staged(&eager_handle, commands);
                model.apply(commands);
            }
            dbsp.step().unwrap();
        }
        dbsp.commit_transaction().unwrap();

        let explain: Explain = &|_| String::new();
        check_collection(
            "the lazy map's deltas",
            &collection(&lazy_batches.lock().unwrap()),
            &model.zset(),
            explain,
        );
        check_collection(
            "the eager map's deltas",
            &collection(&eager_batches.lock().unwrap()),
            &model.zset(),
            explain,
        );
        dbsp.kill().unwrap();
    }

    /// Runs one step's appends on one worker and on four.
    fn check_step(appends: &[Vec<Command>]) {
        for workers in [1, 4] {
            check_appends(std::slice::from_ref(&appends.to_vec()), workers);
        }
    }

    fn writes(commands: &[Command]) -> Vec<Command> {
        commands.to_vec()
    }

    /// Every append writes the same key, so only the last one may survive.
    #[test]
    fn every_append_writes_one_key() {
        check_step(&[
            writes(&[(1, Some(10))]),
            writes(&[(1, Some(20))]),
            writes(&[(1, Some(30))]),
        ]);
        check_step(&[
            writes(&[(1, Some(10))]),
            writes(&[(1, None)]),
            writes(&[(1, Some(30))]),
        ]);
        check_step(&[
            writes(&[(1, Some(10))]),
            writes(&[(1, Some(20))]),
            writes(&[(1, None)]),
        ]);
        check_step(&[
            writes(&[(1, None)]),
            writes(&[(1, Some(20))]),
            writes(&[(1, None)]),
        ]);
    }

    /// A key that an append in the middle does not write still takes the last
    /// update written to it.
    #[test]
    fn a_key_skips_an_append() {
        check_step(&[
            writes(&[(1, Some(10))]),
            writes(&[(2, Some(20))]),
            writes(&[(1, Some(30))]),
        ]);
        check_step(&[
            writes(&[(1, Some(10)), (2, Some(11))]),
            writes(&[(2, Some(20))]),
            writes(&[(1, Some(30))]),
        ]);
    }

    /// Appends of different lengths, so a worker's vectors run out at different
    /// points in the merge.
    #[test]
    fn appends_of_different_lengths() {
        check_step(&[
            writes(&[(1, Some(10))]),
            writes(&[(1, Some(20)), (2, Some(21))]),
            writes(&[(1, Some(30)), (2, Some(31)), (3, Some(32))]),
        ]);
        check_step(&[
            writes(&[(1, Some(10)), (2, Some(11)), (3, Some(12))]),
            writes(&[(1, Some(20)), (2, Some(21))]),
            writes(&[(1, Some(30))]),
        ]);
    }

    /// Appends carrying nothing, among appends that carry something.
    #[test]
    fn empty_appends() {
        check_step(&[writes(&[]), writes(&[(1, Some(10))]), writes(&[])]);
        check_step(&[writes(&[]), writes(&[]), writes(&[])]);
        check_step(&[
            writes(&[(1, Some(10))]),
            writes(&[]),
            writes(&[(1, Some(20))]),
        ]);
    }

    /// Far more appends than a step would normally carry, all writing the same
    /// keys.
    #[test]
    fn many_appends() {
        let appends: Vec<Vec<Command>> = (0..32)
            .map(|round| vec![(1, Some(round)), (2, Some(100 + round))])
            .collect();
        check_step(&appends);
    }

    /// Appends spread over several steps, where the stamp advances between them
    /// and the merge runs again on each.
    #[test]
    fn appends_across_several_steps() {
        let steps = vec![
            vec![vec![(1, Some(10))], vec![(1, Some(20)), (2, Some(21))]],
            vec![vec![(2, None)], vec![(1, Some(30))]],
            vec![vec![(1, None)], vec![(3, Some(40))], vec![(1, Some(50))]],
        ];
        for workers in [1, 4] {
            check_appends(&steps, workers);
        }
    }

    /// Keys few enough that the appends collide on nearly all of them.
    fn random_appends() -> impl Strategy<Value = Vec<Vec<Vec<Command>>>> {
        let command = (0i32..4, prop_oneof![Just(None), (0i32..3).prop_map(Some)]);
        let append = prop::collection::vec(command, 0..4);
        let step = prop::collection::vec(append, 1..5);
        prop::collection::vec(step, 1..4)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Random appends over random steps, on one worker.
        #[test]
        fn random_appends_agree(steps in random_appends()) {
            check_appends(&steps, 1);
        }

        /// The same across four workers, where each append reaches a worker as
        /// its own vector of that worker's share.
        #[test]
        fn random_appends_agree_across_four_workers(steps in random_appends()) {
            check_appends(&steps, 4);
        }
    }
}
