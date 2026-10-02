//! Runs circuits through workloads and checks them against models.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

use proptest::prelude::*;

use crate::{
    DBData, OutputHandle, RootCircuit, Runtime, ZWeight,
    circuit::CircuitConfig,
    typed_batch::{IndexedZSetReader, OrdZSet, SpineSnapshot},
};

/// A Z-set in a model: the weight of each row.
pub(super) type ZSet<T> = BTreeMap<T, ZWeight>;

/// The input changes pushed before each step of one transaction.
pub(super) type Transaction<I> = Vec<I>;

/// Proposed changes to a set: each row is inserted if the set lacks it, and
/// deleted if the set holds it and the flag is set.
pub(super) type Proposals<T> = Vec<(T, bool)>;

/// Circuit settings that must not change a circuit's output.
#[derive(Clone, Copy, Debug)]
pub(super) struct Config {
    /// Number of worker threads.
    pub workers: usize,

    /// Maximum number of records in a chunk of a splitter operator's output,
    /// or `None` for the default.
    pub chunk_size: Option<usize>,
}

impl Config {
    /// Builds the circuit configuration for these settings.
    ///
    /// # Returns
    ///
    /// A configuration with the number of workers and chunk size of `self`.
    fn circuit_config(&self) -> CircuitConfig {
        let config = CircuitConfig::from(self.workers);
        match self.chunk_size {
            Some(records) => config.with_splitter_chunk_size_records(records as u64),
            None => config,
        }
    }
}

/// Every combination of 1, 2, and 3 workers with a chunk size of 1, 2, and
/// the default.
///
/// # Returns
///
/// The configurations, smallest first.
pub(super) fn configs() -> Vec<Config> {
    [1, 2, 3]
        .into_iter()
        .flat_map(|workers| {
            [Some(1), Some(2), None]
                .into_iter()
                .map(move |chunk_size| Config {
                    workers,
                    chunk_size,
                })
        })
        .collect()
}

/// A circuit under test, with a model of its output.
pub(super) trait Program: Clone + Send + 'static {
    /// Changes to the circuit's inputs, pushed before one step.
    type Input: Clone + Debug;

    /// Handles for pushing inputs and reading outputs.
    type Handles: Send + 'static;

    /// The circuit's output, accumulated since it started.
    type Output: PartialEq + Debug;

    /// Builds the circuit.
    ///
    /// # Arguments
    ///
    /// * `circuit` - the root circuit to build in.
    ///
    /// # Returns
    ///
    /// Handles for pushing inputs and reading outputs.
    fn build(&self, circuit: &mut RootCircuit) -> Self::Handles;

    /// Pushes changes into the circuit's inputs.
    ///
    /// # Arguments
    ///
    /// * `handles` - the handles `build` returned.
    /// * `input` - the changes to push.
    fn push(&self, handles: &Self::Handles, input: &Self::Input);

    /// Reads the circuit's accumulated output.
    ///
    /// # Arguments
    ///
    /// * `handles` - the handles `build` returned.
    ///
    /// # Returns
    ///
    /// Everything the circuit has output since it started.
    fn read(&self, handles: &Self::Handles) -> Self::Output;

    /// Computes the output a correct circuit accumulates from some inputs.
    ///
    /// # Arguments
    ///
    /// * `inputs` - every change pushed since the circuit started, in order.
    ///
    /// # Returns
    ///
    /// The output the circuit must have accumulated.
    fn model(&self, inputs: &[Self::Input]) -> Self::Output;
}

/// Runs a program through a workload and checks its output after every
/// transaction.
///
/// A transaction with several steps runs as one explicit transaction, so the
/// recursion runs once per step, every time at the same parent timestamp.
///
/// # Arguments
///
/// * `program` - the program to test.
/// * `workload` - the transactions to run, in order.
/// * `config` - the circuit settings to run under.
///
/// # Panics
///
/// Panics if the circuit's accumulated output differs from the model's after
/// any transaction.
pub(super) fn check<P: Program>(program: &P, workload: &[Transaction<P::Input>], config: Config) {
    let builder = program.clone();
    let (mut circuit, handles) = Runtime::init_circuit(config.circuit_config(), move |circuit| {
        Ok(builder.build(circuit))
    })
    .unwrap();

    let mut history = Vec::new();
    for (index, transaction) in workload.iter().enumerate() {
        if let [input] = transaction.as_slice() {
            program.push(&handles, input);
            circuit.transaction().unwrap();
        } else {
            circuit.start_transaction().unwrap();
            for input in transaction {
                program.push(&handles, input);
                circuit.step().unwrap();
            }
            circuit.commit_transaction().unwrap();
        }
        history.extend(transaction.iter().cloned());

        assert_eq!(
            program.read(&handles),
            program.model(&history),
            "{config:?}, after transaction {index}"
        );
    }

    circuit.kill().unwrap();
}

/// Reads an integrated output as a model Z-set.
///
/// # Arguments
///
/// * `handle` - output of a stream's `accumulate_integrate()`.
///
/// # Returns
///
/// The rows of the integral with their weights.
pub(super) fn read_zset<T: DBData>(handle: &OutputHandle<SpineSnapshot<OrdZSet<T>>>) -> ZSet<T> {
    let mut rows = ZSet::new();
    for (row, (), weight) in handle.concat().consolidate().iter() {
        *rows.entry(row).or_default() += weight;
    }
    rows
}

/// Turns proposed changes into changes that keep a set a set.
///
/// # Arguments
///
/// * `live` - the set's current contents, updated in place.
/// * `proposals` - rows to insert or delete.
///
/// # Returns
///
/// The accepted changes, with weight 1 for an insertion and -1 for a deletion.
pub(super) fn apply_proposals<T: Ord + Clone>(
    live: &mut BTreeSet<T>,
    proposals: &Proposals<T>,
) -> Vec<(T, ZWeight)> {
    let mut changes = Vec::new();
    for (row, delete) in proposals {
        if !live.contains(row) {
            live.insert(row.clone());
            changes.push((row.clone(), 1));
        } else if *delete {
            live.remove(row);
            changes.push((row.clone(), -1));
        }
    }
    changes
}

/// Adds changes to a model Z-set.
///
/// # Arguments
///
/// * `zset` - the Z-set to update.
/// * `changes` - rows and weights to add.
pub(super) fn add_changes<T: Ord + Clone>(zset: &mut ZSet<T>, changes: &[(T, ZWeight)]) {
    for (row, weight) in changes {
        *zset.entry(row.clone()).or_default() += weight;
    }
    zset.retain(|_, weight| *weight != 0);
}

/// Applies changes to a set input, in order, starting from empty.
///
/// # Arguments
///
/// * `changes` - the changes of each step.
///
/// # Returns
///
/// The rows in the set after all changes.
///
/// # Panics
///
/// Panics if the changes do not keep the input a set.
pub(super) fn set_after<'a, T: Ord + Clone + Debug + 'a>(
    changes: impl IntoIterator<Item = &'a [(T, ZWeight)]>,
) -> BTreeSet<T> {
    let mut zset = ZSet::new();
    for step in changes {
        add_changes(&mut zset, step);
    }
    assert!(
        zset.values().all(|weight| *weight == 1),
        "not a set: {zset:?}"
    );
    zset.into_keys().collect()
}

/// Computes the fixed point that a recursion reaches from `start`.
///
/// A recursive circuit computes, in iteration `n + 1`, its step function
/// applied to the value of iteration `n`, so the model iterates the same
/// function until its value stops changing.
///
/// # Arguments
///
/// * `start` - the value before the first iteration.
/// * `step` - the recursion's step function.
///
/// # Returns
///
/// The first value that `step` maps to itself.
///
/// # Panics
///
/// Panics if no such value appears within 10,000 iterations.
pub(super) fn fixpoint<S: PartialEq>(start: S, step: impl Fn(&S) -> S) -> S {
    let mut current = start;
    for _ in 0..10_000 {
        let next = step(&current);
        if next == current {
            return current;
        }
        current = next;
    }
    panic!("the model did not reach a fixed point");
}

/// Converts a set into a model Z-set.
///
/// # Arguments
///
/// * `set` - the rows.
///
/// # Returns
///
/// The rows, each with weight 1.
pub(super) fn set_zset<T: Ord + Clone>(set: &BTreeSet<T>) -> ZSet<T> {
    set.iter().map(|row| (row.clone(), 1)).collect()
}

/// Generates the shape of workloads: up to `max_transactions` transactions of
/// one to `max_steps` steps, each step built by `step`.
///
/// # Arguments
///
/// * `step` - generates the raw input of one step.
/// * `max_transactions` - the maximum number of transactions.
/// * `max_steps` - the maximum number of steps per transaction.
///
/// # Returns
///
/// A strategy for raw workloads.
pub(super) fn workloads<S: Strategy>(
    step: S,
    max_transactions: usize,
    max_steps: usize,
) -> impl Strategy<Value = Vec<Vec<S::Value>>> {
    prop::collection::vec(
        prop::collection::vec(step, 1..=max_steps),
        1..=max_transactions,
    )
}

/// Generates proposed changes to a set.
///
/// # Arguments
///
/// * `element` - generates the set's elements.
/// * `max` - the maximum number of proposals.
///
/// # Returns
///
/// A strategy for proposals.
pub(super) fn proposals<T: Clone + Debug>(
    element: impl Strategy<Value = T>,
    max: usize,
) -> impl Strategy<Value = Proposals<T>> {
    prop::collection::vec((element, any::<bool>()), 0..=max)
}

/// Converts every step of a raw workload, in order.
///
/// # Arguments
///
/// * `raw` - the raw workload, as generated by [`workloads`].
/// * `step` - converts the raw input of one step, and may keep state across
///   steps, such as the contents of the program's inputs.
///
/// # Returns
///
/// The converted workload.
pub(super) fn map_steps<R, I>(
    raw: Vec<Vec<R>>,
    mut step: impl FnMut(R) -> I,
) -> Vec<Transaction<I>> {
    raw.into_iter()
        .map(|transaction| transaction.into_iter().map(&mut step).collect())
        .collect()
}

/// Picks one of the configurations in [`configs`].
///
/// # Returns
///
/// A strategy for configurations.
pub(super) fn any_config() -> impl Strategy<Value = Config> {
    prop::sample::select(configs())
}
