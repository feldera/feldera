//! Convenience API for defining recursive computations.

use crate::{
    Timestamp,
    algebra::IndexedZSet,
    circuit::{
        ChildCircuit, Circuit, Stream, circuit_builder::IterativeCircuit,
        schedule::Error as SchedulerError,
    },
    operator::{DelayedFeedback, dynamic::distinct::DistinctFactories},
    trace::Spine,
};

use crate::circuit::checkpointer::Checkpoint;
use impl_trait_for_tuples::impl_for_tuples;
use size_of::SizeOf;
use std::result::Result;

/// Generalizes stream operators to groups of streams.
///
/// This is a helper trait for the
/// [`ChildCircuit::recursive`](`crate::ChildCircuit::recursive`) method.  The
/// method internally performs several transformations on each recursive stream:
/// `distinct`, `connect`, `export`, `consolidate`.  This trait generalizes
/// these methods to operate on multiple streams (e.g., tuples and vectors) of
/// Z-sets, so that we can define recursive computations over multiple streams.
pub trait RecursiveStreams<C> {
    /// Generalizes: [`DelayedFeedback`] type to a group of streams; contains a
    /// `DelayedFeedback` instance for each stream in the group.
    type Feedback;

    /// Represents streams in the group exported to the parent circuit.
    type Export;

    /// Type of the final result of the recursive computation: computed output
    /// streams exported to the parent circuit and consolidated.
    type Output;

    type Factories;

    /// Create a group of recursive streams along with their feedback
    /// connectors.
    fn new(circuit: &C, factories: &Self::Factories) -> (Self::Feedback, Self);

    /// Apply `distinct` to all streams in `self`.
    fn distinct(self, factories: &Self::Factories) -> Self;

    /// Close feedback loop for all streams in `self`.
    fn connect(&self, vars: Self::Feedback);

    /// Export all streams in `self` to the parent circuit.
    fn export(self, factories: &Self::Factories) -> Self::Export;

    /// Apply [`Stream::dyn_consolidate`] to all streams in `exports`.
    fn consolidate(exports: Self::Export, factories: &Self::Factories) -> Self::Output;
}

impl<C, B> RecursiveStreams<C> for Stream<C, B>
where
    C: Circuit,
    C::Parent: Circuit,
    B: Checkpoint + IndexedZSet + Send + Sync,
    Spine<B>: SizeOf,
{
    type Feedback = DelayedFeedback<C, B>;
    type Export = Stream<C::Parent, Spine<B>>;
    type Output = Stream<C::Parent, B>;
    type Factories = DistinctFactories<B, C::Time>;

    fn new(circuit: &C, factories: &Self::Factories) -> (Self::Feedback, Self) {
        let feedback =
            DelayedFeedback::with_default(circuit, B::dyn_empty(&factories.input_factories));
        let stream = feedback.stream().clone();
        (feedback, stream)
    }

    fn distinct(self, factories: &Self::Factories) -> Self {
        Stream::dyn_distinct(&self, factories).set_persistent_id(
            self.get_persistent_id()
                .map(|name| format!("{name}.distinct"))
                .as_deref(),
        )
    }

    fn connect(&self, var: Self::Feedback) {
        var.connect(self)
    }

    fn export(self, factories: &Self::Factories) -> Self::Export {
        Stream::export(&self.dyn_integrate_trace(&factories.input_factories))
    }

    fn consolidate(exports: Self::Export, factories: &Self::Factories) -> Self::Output {
        Stream::dyn_consolidate(&exports, &factories.input_factories)
    }
}

/// Recursion over a group of streams whose size is only known at runtime.
///
/// The arity of the group (the number of mutually recursive streams) is
/// determined by the length of the `factories` vector passed to
/// [`new`](RecursiveStreams::new).  Every other method preserves this arity:
/// the closure driving the recursion must therefore return exactly as many
/// streams as it received.  Unlike the tuple implementations, all streams in
/// the group share the same batch type `B`.
impl<C, B> RecursiveStreams<C> for Vec<Stream<C, B>>
where
    C: Circuit,
    C::Parent: Circuit,
    B: Checkpoint + IndexedZSet + Send + Sync,
    Spine<B>: SizeOf,
{
    type Feedback = Vec<DelayedFeedback<C, B>>;
    type Export = Vec<Stream<C::Parent, Spine<B>>>;
    type Output = Vec<Stream<C::Parent, B>>;
    type Factories = Vec<DistinctFactories<B, C::Time>>;

    fn new(circuit: &C, factories: &Self::Factories) -> (Self::Feedback, Self) {
        factories
            .iter()
            .map(|factory| {
                let feedback =
                    DelayedFeedback::with_default(circuit, B::dyn_empty(&factory.input_factories));
                let stream = feedback.stream().clone();
                (feedback, stream)
            })
            .unzip()
    }

    fn distinct(mut self, factories: &Self::Factories) -> Self {
        assert_eq!(self.len(), factories.len());

        for (stream, factory) in self.iter_mut().zip(factories) {
            let persistent_id = stream
                .get_persistent_id()
                .map(|name| format!("{name}.distinct"));
            *stream =
                Stream::dyn_distinct(stream, factory).set_persistent_id(persistent_id.as_deref());
        }

        self
    }

    fn connect(&self, vars: Self::Feedback) {
        assert_eq!(self.len(), vars.len());

        for (stream, var) in self.iter().zip(vars) {
            var.connect(stream);
        }
    }

    fn export(self, factories: &Self::Factories) -> Self::Export {
        assert_eq!(self.len(), factories.len());

        self.into_iter()
            .zip(factories)
            .map(|(stream, factory)| {
                Stream::export(&stream.dyn_integrate_trace(&factory.input_factories))
            })
            .collect()
    }

    fn consolidate(exports: Self::Export, factories: &Self::Factories) -> Self::Output {
        assert_eq!(exports.len(), factories.len());

        exports
            .into_iter()
            .zip(factories)
            .map(|(stream, factory)| Stream::dyn_consolidate(&stream, &factory.input_factories))
            .collect()
    }
}

#[allow(clippy::unused_unit)]
#[impl_for_tuples(14)]
#[tuple_types_custom_trait_bound(Clone + RecursiveStreams<C>)]
impl<C> RecursiveStreams<C> for Tuple {
    for_tuples!( type Feedback = ( #( Tuple::Feedback ),* ); );
    for_tuples!( type Export = ( #( Tuple::Export ),* ); );
    for_tuples!( type Output = ( #( Tuple::Output ),* ); );
    for_tuples!( type Factories = ( #( Tuple::Factories ),* ); );

    fn new(circuit: &C, factories: &Self::Factories) -> (Self::Feedback, Self) {
        let res = (for_tuples!( #( Tuple::new(circuit, &factories.Tuple) ),* ));

        let streams = (for_tuples!( #( { let stream = &res.Tuple; stream.1.clone() } ),* ));
        let feedback = (for_tuples!( #( { let stream = res.Tuple; stream.0 } ),* ));

        (feedback, streams)
    }

    fn distinct(self, factories: &Self::Factories) -> Self {
        (for_tuples!( #( self.Tuple.distinct(&factories.Tuple) ),* ))
    }

    fn connect(&self, vars: Self::Feedback) {
        for_tuples!( #( self.Tuple.connect(vars.Tuple); )* );
    }

    fn export(self, factories: &Self::Factories) -> Self::Export {
        (for_tuples!( #( self.Tuple.export(&factories.Tuple) ),* ))
    }

    fn consolidate(exports: Self::Export, factories: &Self::Factories) -> Self::Output {
        (for_tuples!( #( Tuple::consolidate(exports.Tuple, &factories.Tuple) ),* ))
    }
}

// We skip formatting this until
// https://github.com/rust-lang/rustfmt/issues/5420 is resolved
// (or we can run this doctest with persistence enabled)
#[rustfmt::skip]
impl<P, T> ChildCircuit<P, T>
where
    P: 'static,
    T: Timestamp,
    Self: Circuit,
{
    /// See [`ChildCircuit::recursive`].
    pub fn dyn_recursive<F, S>(&self, factories: &S::Factories, f: F) -> Result<S::Output, SchedulerError>
    where
        S: RecursiveStreams<IterativeCircuit<Self>>,
        F: FnOnce(&IterativeCircuit<Self>, S) -> Result<S, SchedulerError>,
    {
        // The actual circuit we build:
        //
        // ```
        //     ┌───────────────────────────────────────────────────────────────┐
        //     │                                                               │
        //  i  │               ┌───┐                                           │
        // ────┼──►δ0─────────►│   │      ┌────────┐       ┌───────────────┐   │   ┌───────────┐
        //     │               │ f ├─────►│distinct├──┬───►│integrate_trace├───┼──►│consolidate├───────►
        //     │       ┌──────►│   │      └────────┘  │    └───────────────┘   │   └───────────┘
        //     │       │       └───┘                  │                        │
        //     │       │                              │                        │
        //     │       │                              │                        │
        //     │       │       ┌────┐                 │                        │
        //     │       └───────┤z^-1│◄────────────────┘                        │
        //     │               └────┘                                          │
        //     │                                                               │
        //     └───────────────────────────────────────────────────────────────┘
        // ```
        //
        // where
        // * `integrate_trace` integrates outputs computed across multiple fixed point
        //   iterations.
        // * `consolidate` consolidates the output of the nested circuit into a single
        //   batch.
        let traces = self.fixedpoint(|child| {
            let (vars, input_streams) = S::new(child, factories);
            let output_streams = f(child, input_streams)?;
            let output_streams = S::distinct(output_streams, factories);
            S::connect(&output_streams, vars);
            Ok(S::export(output_streams, factories))
        })?;

        Ok(S::consolidate(traces, factories))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Circuit, Runtime, Stream, operator::Generator, typed_batch::OrdZSet, utils::Tup2, zset,
    };
    use std::{
        thread,
        time::{Duration, Instant},
        vec,
    };

    /// Measures how often a nested circuit runs, and how often it gets input,
    /// in transactions that change, or do not change, its input.
    ///
    /// The circuit has two parts that share no input:
    /// - `edges` feeds the nested circuit (see [`Nested`]);
    /// - `facts` feeds a chain of joins in the root circuit, so that one commit
    ///   takes more than one root step.
    ///
    /// Run with `--nocapture` to see the measurements.
    mod run_stats {
        use super::*;
        use crate::{
            DBSPHandle, ZSetHandle,
            algebra::HasZero,
            circuit::{
                OwnershipPreference, Scope,
                metadata::{
                    MetaItem, MetricId, SUBCIRCUIT_RUNS_COUNT, SUBCIRCUIT_RUNS_INPUT_UNKNOWN_COUNT,
                    SUBCIRCUIT_RUNS_WITH_INPUT_COUNT,
                },
                operator_traits::{Data, ImportOperator, Operator},
                trace::SchedulerEvent,
            },
            operator::Delta0,
            profile::DbspProfile,
        };
        use std::{
            borrow::Cow,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering::Relaxed},
            },
        };

        /// The nested circuit that `edges` feeds.
        #[derive(Clone, Copy, Debug)]
        enum Nested {
            /// A transitive closure, imported with `delta0`.
            Recursive,
            /// The same transitive closure, imported with [`OpaqueDelta0`].
            RecursiveOpaque,
            /// A non-incremental circuit, which imports with an accumulator.
            NonIncremental,
        }

        /// `Delta0` that does not say if its input was empty: it keeps the
        /// default [`ImportOperator::imported_empty`].
        struct OpaqueDelta0<D>(Delta0<D>);

        impl<D: Data> Operator for OpaqueDelta0<D> {
            fn name(&self) -> Cow<'static, str> {
                Cow::from("OpaqueDelta0")
            }

            fn fixedpoint(&self, scope: Scope) -> bool {
                self.0.fixedpoint(scope)
            }
        }

        impl<D> ImportOperator<D, D> for OpaqueDelta0<D>
        where
            D: HasZero + Clone + 'static,
        {
            async fn import(&mut self, val: &D) {
                self.0.import(val).await
            }

            async fn import_owned(&mut self, val: D) {
                self.0.import_owned(val).await
            }

            async fn eval(&mut self) -> D {
                self.0.eval().await
            }

            fn input_preference(&self) -> OwnershipPreference {
                self.0.input_preference()
            }
        }

        /// Number of joins in the root chain.
        const CHAIN: u64 = 8;

        /// Number of keys in the dimension table that the chain joins with.
        const KEYS: u64 = 16;

        /// Number of nodes in the path graph that the recursion walks.
        const NODES: u64 = 20;

        type Pair = Tup2<u64, u64>;

        /// Root and inner steps, counted on worker 0.
        #[derive(Clone, Default)]
        struct StepCounts {
            root: Arc<AtomicUsize>,
            inner: Arc<AtomicUsize>,
        }

        struct Pipeline {
            handle: DBSPHandle,
            edges: ZSetHandle<Pair>,
            facts: ZSetHandle<Pair>,
            dim: ZSetHandle<Pair>,
            steps: StepCounts,
        }

        fn build(workers: usize, nested: Nested) -> Pipeline {
            let steps = StepCounts::default();
            let worker_steps = steps.clone();
            let (handle, (edges, facts, dim)) = Runtime::init_circuit(workers, move |circuit| {
                if Runtime::worker_index() == 0 {
                    let steps = worker_steps.clone();
                    circuit.register_scheduler_event_handler("run_stats", move |event| {
                        if let SchedulerEvent::StepStart { circuit_id } = event {
                            if circuit_id.path().is_empty() {
                                steps.root.fetch_add(1, Relaxed);
                            } else {
                                steps.inner.fetch_add(1, Relaxed);
                            }
                        }
                    });
                }

                let (edges, edges_handle) = circuit.add_input_zset::<Pair>();
                let (facts, facts_handle) = circuit.add_input_zset::<Pair>();
                let (dim, dim_handle) = circuit.add_input_zset::<Pair>();

                match nested {
                    // Transitive closure over `edges`.
                    Nested::Recursive | Nested::RecursiveOpaque => {
                        let paths = circuit
                            .recursive(|child, paths: Stream<_, OrdZSet<Pair>>| {
                                let edges = match nested {
                                    Nested::Recursive => edges.delta0(child),
                                    _ => child.import_stream(OpaqueDelta0(Delta0::new()), &edges),
                                };
                                let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
                                let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                                Ok(edges.plus(
                                    &paths_indexed
                                        .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                                ))
                            })
                            .unwrap();
                        paths.output();
                    }
                    Nested::NonIncremental => {
                        circuit
                            .non_incremental(&edges, |_child, edges| Ok(edges.differentiate()))
                            .unwrap()
                            .output();
                    }
                }

                // Chain of joins over `facts`.  Each join keys its input again,
                // and keeps the number of rows.
                let dim = dim.map_index(|Tup2(k, v)| (*k, *v));
                let mut chain = facts;
                for i in 0..CHAIN {
                    chain = chain
                        .map_index(|Tup2(a, b)| (*b % KEYS, *a))
                        .join(&dim, move |_k, a, d| Tup2(*a, *d + i));
                }
                chain.output();

                Ok((edges_handle, facts_handle, dim_handle))
            })
            .unwrap();

            Pipeline {
                handle,
                edges,
                facts,
                dim,
                steps,
            }
        }

        /// What one transaction cost.
        #[derive(Debug, Default, Clone, Copy)]
        struct Measurement {
            /// Root steps, counted by the caller.
            root_steps: usize,
            /// Root steps, counted from the scheduler events.
            root_step_events: usize,
            /// Inner steps of the nested circuit, on worker 0.
            inner_steps: usize,
            /// Runs of the nested circuit, on worker 0.
            runs: usize,
            /// Runs with input, highest count over the workers.
            runs_with_input_max: usize,
            /// Runs with input, summed over the workers.
            runs_with_input_sum: usize,
            /// Runs whose input was not classified, summed over the workers.
            runs_input_unknown: usize,
            millis: f64,
        }

        /// Reads one counter of the nested circuit, per worker.
        fn counter(profile: &DbspProfile, metric: &MetricId) -> Vec<usize> {
            profile
                .worker_profiles
                .iter()
                .map(|worker| {
                    // The root entry holds the sum over all nodes; skip it.
                    let mut values = worker.attribute_profile(metric);
                    values.retain(|id, _| !id.path().is_empty());
                    assert_eq!(values.len(), 1, "expected one nested circuit: {values:?}");
                    match values.into_values().next().unwrap() {
                        MetaItem::Count(n) => n,
                        other => panic!("unexpected value {other:?}"),
                    }
                })
                .collect()
        }

        /// Reads (runs, runs with input, runs with unknown input), per worker.
        fn counters(handle: &mut DBSPHandle) -> [Vec<usize>; 3] {
            let profile = handle.retrieve_profile().unwrap();
            [
                counter(&profile, &SUBCIRCUIT_RUNS_COUNT),
                counter(&profile, &SUBCIRCUIT_RUNS_WITH_INPUT_COUNT),
                counter(&profile, &SUBCIRCUIT_RUNS_INPUT_UNKNOWN_COUNT),
            ]
        }

        fn transaction(
            p: &mut Pipeline,
            mut edges: Vec<Tup2<Pair, i64>>,
            mut facts: Vec<Tup2<Pair, i64>>,
            mut dim: Vec<Tup2<Pair, i64>>,
        ) -> Measurement {
            let [runs0, input0, unknown0] = counters(&mut p.handle);
            let root0 = p.steps.root.load(Relaxed);
            let inner0 = p.steps.inner.load(Relaxed);

            let start = Instant::now();
            p.handle.start_transaction().unwrap();
            p.edges.append(&mut edges);
            p.facts.append(&mut facts);
            p.dim.append(&mut dim);
            p.handle.start_commit_transaction().unwrap();
            let mut root_steps = 0;
            loop {
                root_steps += 1;
                if p.handle.step().unwrap() {
                    break;
                }
            }
            let millis = start.elapsed().as_secs_f64() * 1000.0;

            let [runs1, input1, unknown1] = counters(&mut p.handle);
            let delta = |after: &[usize], before: &[usize]| -> Vec<usize> {
                after.iter().zip(before).map(|(a, b)| a - b).collect()
            };
            let runs = delta(&runs1, &runs0);
            let input = delta(&input1, &input0);
            let unknown = delta(&unknown1, &unknown0);

            // Every worker runs the nested circuit the same number of times.
            assert!(
                runs.iter().all(|r| *r == runs[0]),
                "runs per worker: {runs:?}"
            );

            Measurement {
                root_steps,
                root_step_events: p.steps.root.load(Relaxed) - root0,
                inner_steps: p.steps.inner.load(Relaxed) - inner0,
                runs: runs[0],
                runs_with_input_max: input.iter().copied().max().unwrap(),
                runs_with_input_sum: input.iter().sum(),
                runs_input_unknown: unknown.iter().sum(),
                millis,
            }
        }

        /// Runs the same transactions with 1 and 4 workers, prints what each
        /// one cost, and checks what all kinds of nested circuit share.
        ///
        /// Returns the measurements for each worker count.
        fn run(nested: Nested) -> Vec<(usize, Vec<(&'static str, Measurement)>)> {
            let mut results = Vec::new();
            for workers in [1, 4] {
                let mut p = build(workers, nested);

                // Load the path graph, the dimension table and some facts.
                let edges = (0..NODES).map(|i| Tup2(Tup2(i, i + 1), 1)).collect();
                let dim = (0..KEYS).map(|k| Tup2(Tup2(k, k * 7 % KEYS), 1)).collect();
                let facts = (0..100).map(|i| Tup2(Tup2(i, i * 3), 1)).collect();
                let load = transaction(&mut p, edges, facts, dim);

                let mut rows = vec![("load", load)];
                for t in 0..3u64 {
                    let facts = (0..10)
                        .map(|i| Tup2(Tup2(1000 * (t + 1) + i, i), 1))
                        .collect();
                    rows.push(("facts only", transaction(&mut p, vec![], facts, vec![])));
                }
                for _ in 0..3 {
                    rows.push(("empty", transaction(&mut p, vec![], vec![], vec![])));
                }
                for t in 0..3u64 {
                    let edges = vec![Tup2(Tup2(NODES + t, NODES + t + 1), 1)];
                    rows.push(("edges only", transaction(&mut p, edges, vec![], vec![])));
                }

                println!(
                    "\n{nested:?}, workers={workers}\n{:<11} {:>10} {:>11} {:>5} {:>11} {:>11} {:>7} {:>9} {:>8}",
                    "transaction",
                    "root steps",
                    "inner steps",
                    "runs",
                    "input(max)",
                    "input(sum)",
                    "unknown",
                    "inner/run",
                    "ms"
                );
                for (name, m) in &rows {
                    println!(
                        "{:<11} {:>10} {:>11} {:>5} {:>11} {:>11} {:>7} {:>9.1} {:>8.1}",
                        name,
                        m.root_steps,
                        m.inner_steps,
                        m.runs,
                        m.runs_with_input_max,
                        m.runs_with_input_sum,
                        m.runs_input_unknown,
                        m.inner_steps as f64 / m.runs as f64,
                        m.millis
                    );
                }

                for (name, m) in &rows {
                    // The scheduler events agree with the caller's count.
                    assert_eq!(m.root_steps, m.root_step_events, "{name}");
                    // The nested circuit runs once per root step.
                    assert_eq!(m.runs, m.root_steps, "{name}");
                }
                results.push((workers, rows));
            }
            results
        }

        /// Whether the transaction changed the input of the nested circuit.
        fn changes_input(name: &str) -> bool {
            matches!(name, "load" | "edges only")
        }

        /// A recursive sub-circuit runs once at every root step of a commit,
        /// also when no worker has input for it.  Each run costs at least one
        /// inner step.
        ///
        /// This is the behavior that a skip of runs without input would
        /// change: the assertion `runs == root_steps` in [`run`] would then
        /// fail for the "facts only" and "empty" transactions.
        #[test]
        fn recursion_runs_at_every_root_step() {
            for (_workers, rows) in run(Nested::Recursive) {
                for (name, m) in &rows {
                    // Each run takes at least one inner step.
                    assert!(m.inner_steps >= m.runs, "{name}");
                    // `Delta0` always knows if its input was empty.
                    assert_eq!(m.runs_input_unknown, 0, "{name}");
                    assert_eq!(m.runs_with_input_max > 0, changes_input(name), "{name}");
                }
            }
        }

        /// An import that cannot tell if its input was empty makes each run
        /// count as a run with unknown input, never as a run with input.
        #[test]
        fn opaque_import_counts_runs_as_unknown() {
            for (workers, rows) in run(Nested::RecursiveOpaque) {
                for (name, m) in &rows {
                    assert_eq!(m.runs_with_input_max, 0, "{name}");
                    assert_eq!(m.runs_input_unknown, m.runs * workers, "{name}");
                }
            }
        }

        /// A non-incremental circuit also runs once per root step, and its
        /// accumulating import knows if its input was empty.
        #[test]
        fn non_incremental_runs_at_every_root_step() {
            for (_workers, rows) in run(Nested::NonIncremental) {
                for (name, m) in &rows {
                    assert_eq!(m.runs_input_unknown, 0, "{name}");
                    assert_eq!(m.runs_with_input_max > 0, changes_input(name), "{name}");
                }
            }
        }
    }

    // See https://github.com/feldera/feldera/issues/4168
    #[test]
    fn issue4168() {
        let (mut circuit, edges_handle) = Runtime::init_circuit(8, move |circuit| {
            let (edges_stream, edges_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();

            // Create two identical recursive fragments. issue4168 caused them to deadlock.
            let _ = circuit
                .recursive(|child, paths: Stream<_, OrdZSet<Tup2<u64, u64>>>| {
                    let edges = edges_stream.delta0(child);

                    let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
                    let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

                    Ok(edges.plus(
                        &paths_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                    ))
                })
                .unwrap();

            let _ = circuit
                .recursive(|child, paths: Stream<_, OrdZSet<Tup2<u64, u64>>>| {
                    let edges = edges_stream.delta0(child);

                    let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
                    let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

                    Ok(edges.plus(
                        &paths_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                    ))
                })
                .unwrap();

            Ok(edges_handle)
        })
        .unwrap();

        let handle = thread::spawn(move || {
            for i in 0..100 {
                edges_handle.append(&mut vec![Tup2(Tup2(i, i + 1), 1)]);
                circuit.transaction().unwrap();
            }
        });

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(200) {
            if handle.is_finished() {
                handle.join().unwrap();
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }

        panic!("Deadlock in test 'issue4168'");
    }

    // See https://github.com/feldera/feldera/issues/4028
    #[test]
    fn issue4028() {
        // Changes to the edges relation.
        let insert_edges = (0..100)
            .map(|i| Tup2(Tup2(i, i + 1), 1))
            .collect::<Vec<_>>();
        let delete_edges = (0..100)
            .map(|i| Tup2(Tup2(i, i + 1), -1))
            .collect::<Vec<_>>();

        let (mut root, (edges_handle, paths_handle)) = Runtime::init_circuit(1, move |circuit| {
            let (edges, edges_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();

            let paths = circuit
                .recursive(|child, paths: Stream<_, OrdZSet<Tup2<u64, u64>>>| {
                    let edges = edges.delta0(child);

                    let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
                    let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

                    Ok(edges.plus(
                        &paths_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                    ))
                })
                .unwrap();

            let paths_handle = paths.integrate().output();

            Ok((edges_handle, paths_handle))
        })
        .unwrap();

        for _ in 0..10 {
            edges_handle.append(&mut insert_edges.clone());
            root.transaction().unwrap();

            edges_handle.append(&mut delete_edges.clone());
            root.transaction().unwrap();

            let paths = paths_handle.consolidate();
            assert!(paths.is_empty());
        }
    }

    mod reachability {
        use super::*;
        use crate::{
            DBSPHandle, FallbackZSet, OutputHandle, RootCircuit,
            algebra::AddByRef,
            circuit::{CircuitConfig, CircuitStorageConfig, Mode, StorageConfig},
            typed_batch::SpineSnapshot,
        };
        use std::ops::Range;
        use uuid::Uuid;

        type Edge = Tup2<usize, usize>;

        /// Changes to the edges relation.
        fn edges_data() -> Vec<OrdZSet<Edge>> {
            vec![
                zset! { Tup2(1, 2) => 1 },
                zset! { Tup2(2, 3) => 1},
                zset! { Tup2(1, 3) => 1},
                zset! { Tup2(3, 1) => 1},
                zset! { Tup2(3, 1) => -1},
                zset! { Tup2(1, 2) => -1},
                zset! { Tup2(2, 4) => 1, Tup2(4, 1) => 1 },
                zset! { Tup2(2, 3) => -1, Tup2(3, 2) => 1 },
            ]
        }

        /// Expected output to the reachable relation.
        fn expected_reachable() -> Vec<OrdZSet<Edge>> {
            vec![
                zset! { Tup2(1, 2) => 1 },
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 1) => 1, Tup2(2, 2) => 1, Tup2(3, 3) => 1,
                Tup2(1, 2) => 1, Tup2(1, 3) => 1, Tup2(2, 3) => 1,
                Tup2(2, 1) => 1, Tup2(3, 1) => 1, Tup2(3, 2) => 1},
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 3) => 1, Tup2(2, 3) => 1, Tup2(2, 4) => 1,
                Tup2(2, 1) => 1, Tup2(4, 1) => 1, Tup2(4, 3) => 1 },
                zset! { Tup2(1, 1) => 1, Tup2(2, 2) => 1, Tup2(3, 3) => 1,
                Tup2(4, 4) => 1, Tup2(1, 2) => 1, Tup2(1, 3) => 1,
                Tup2(1, 4) => 1, Tup2(2, 1) => 1, Tup2(2, 3) => 1,
                Tup2(2, 4) => 1, Tup2(3, 1) => 1, Tup2(3, 2) => 1,
                Tup2(3, 4) => 1, Tup2(4, 1) => 1, Tup2(4, 2) => 1,
                Tup2(4, 3) => 1 },
            ]
        }

        /// Output of one recursive relation: the changes it makes in each
        /// transaction.
        type Deltas = OutputHandle<SpineSnapshot<OrdZSet<Edge>>>;

        /// Runs a reachability circuit over [`edges_data`], checkpointing it
        /// halfway through and restarting it from that checkpoint.
        ///
        /// `build` populates the circuit, skipping the first `skip` inputs so
        /// that the restarted circuit picks up where the first one stopped, and
        /// returns one handle per recursive relation.  The running sum of each
        /// relation's per-transaction changes must equal
        /// [`expected_reachable`] at every step, and the sums carry across the
        /// restart.
        ///
        /// The relation is maintained inside the recursive scope, and the
        /// circuit does not change across the restart, so no operator needs a
        /// backfill: the checkpoint is the only place that state can come from.
        /// A scope that came back empty computes the wrong changes and fails
        /// the comparison.
        fn checkpoint_and_restart<F>(build: F)
        where
            F: Fn(&mut RootCircuit, usize) -> Vec<Deltas> + Clone + Send + Sync + 'static,
        {
            let expected = expected_reachable();
            let steps = expected.len();
            let restart_after = steps / 2;
            let path = tempfile::tempdir().unwrap().keep();

            let config = |init_checkpoint: Option<Uuid>| {
                CircuitConfig::with_workers(1)
                    .with_mode(Mode::Persistent)
                    .with_storage(Some(
                        CircuitStorageConfig::for_config(
                            StorageConfig {
                                path: path.to_string_lossy().into_owned(),
                                cache: Default::default(),
                            },
                            Default::default(),
                        )
                        .unwrap()
                        .with_init_checkpoint(init_checkpoint),
                    ))
            };

            // Running sum of each relation's changes, carried across the restart.
            let mut reachable: Vec<OrdZSet<Edge>> = Vec::new();

            let mut run = |handle: &mut DBSPHandle, outputs: &[Deltas], steps: Range<usize>| {
                reachable.resize(outputs.len(), OrdZSet::empty());
                for step in steps {
                    handle.transaction().unwrap();
                    for (relation, output) in reachable.iter_mut().zip(outputs) {
                        let delta = SpineSnapshot::<OrdZSet<Edge>>::concat(&output.take_from_all())
                            .consolidate();
                        *relation = relation.add_by_ref(&delta);
                        assert_eq!(*relation, expected[step], "wrong output in step {step}");
                    }
                }
            };

            let checkpoint = {
                let build = build.clone();
                let (mut handle, outputs) =
                    Runtime::init_circuit(config(None), move |circuit| Ok(build(circuit, 0)))
                        .unwrap();

                run(&mut handle, &outputs, 0..restart_after);

                let checkpoint = handle.checkpoint().run().unwrap();
                handle.kill().unwrap();
                checkpoint
            };

            let (mut handle, outputs) =
                Runtime::init_circuit(config(Some(checkpoint.uuid)), move |circuit| {
                    Ok(build(circuit, restart_after))
                })
                .unwrap();

            // Nothing changed, so nothing may be backfilled: a backfill would
            // rebuild the scope from replayed input and leave the checkpoint
            // untested.
            assert!(!handle.bootstrap_in_progress());

            run(&mut handle, &outputs, restart_after..steps);
            handle.kill().unwrap();
        }

        #[test]
        fn reachability() {
            checkpoint_and_restart(|circuit, skip| {
                let mut edges = edges_data().into_iter().skip(skip);
                let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

                let reachable = circuit
                    .recursive(|child, reachable: Stream<_, OrdZSet<Edge>>| {
                        // Checkpointing a recursive scope requires its operators
                        // to be named; see `ChildCircuit::recursive`.
                        reachable.set_persistent_id(Some("reachable"));

                        let edges = edges.delta0(child);
                        let edges_indexed = edges
                            .map_index(|Tup2(x, y)| (*x, *y))
                            .set_persistent_id(Some("edges_indexed"));

                        let reachable_indexed = reachable
                            .map_index(|&Tup2(x, y)| (y, x))
                            .set_persistent_id(Some("reachable_indexed"));

                        let reachable_next = edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );
                        reachable_next.set_persistent_id(Some("reachable_next"));

                        Ok(reachable_next)
                    })
                    .unwrap();

                vec![reachable.accumulate_output_persistent(Some("reachable_out"))]
            });
        }

        /// A rewrite of [`reachability()`] using
        /// [`recursive_dynamic`](crate::ChildCircuit::recursive_dynamic):
        /// A single recursive relation supplied as a one-element vector
        /// (arity 1).  It must produce exactly the same output as the
        /// single-`Stream` implementation.
        #[test]
        fn reachability_dynamic() {
            checkpoint_and_restart(|circuit, skip| {
                let mut edges = edges_data().into_iter().skip(skip);
                let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

                let mut recursive_streams = circuit
                    .recursive_dynamic(
                        1,
                        |child, mut recursive_streams: Vec<Stream<_, OrdZSet<Edge>>>| {
                            let edges = edges.delta0(child);
                            let edges_indexed = edges
                                .map_index(|Tup2(x, y)| (*x, *y))
                                .set_persistent_id(Some("edges_indexed"));

                            let reachable = &mut recursive_streams[0];
                            reachable.set_persistent_id(Some("reachable"));
                            let reachable_indexed = reachable
                                .map_index(|&Tup2(x, y)| (y, x))
                                .set_persistent_id(Some("reachable_indexed"));

                            let reachable_next = edges.plus(
                                &reachable_indexed
                                    .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                            );
                            reachable_next.set_persistent_id(Some("reachable_next"));

                            // We can even reuse the allocated vector and spare us a reallocation.
                            *reachable = reachable_next;
                            Ok(recursive_streams)
                        },
                    )
                    .unwrap();

                let reachable = recursive_streams.pop().unwrap();

                vec![reachable.accumulate_output_persistent(Some("reachable_out"))]
            });
        }

        // Somewhat lame multiple recursion example to test RecursiveStreams impl for
        // tuples: compute forward and backward reachability at the same time.
        #[test]
        fn reachability2() {
            checkpoint_and_restart(|circuit, skip| {
                let mut edges = edges_data().into_iter().skip(skip);
                let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

                let (reachable, reachable_reverse) = circuit
                    .recursive(
                        |child,
                         (reachable, reachable_reverse): (
                            Stream<_, FallbackZSet<Edge>>,
                            Stream<_, FallbackZSet<Edge>>,
                        )| {
                            reachable.set_persistent_id(Some("reachable"));
                            reachable_reverse.set_persistent_id(Some("reachable_reverse"));

                            let edges = edges.delta0(child);

                            let edges_indexed = edges
                                .map_index(|Tup2(x, y)| (*x, *y))
                                .set_persistent_id(Some("edges_indexed"));
                            let reachable_indexed = reachable
                                .map_index(|&Tup2(x, y)| (y, x))
                                .set_persistent_id(Some("reachable_indexed"));
                            let reachable_reverse_indexed = reachable_reverse
                                .map_index(|&Tup2(x, y)| (y, x))
                                .set_persistent_id(Some("reachable_reverse_indexed"));
                            let reverse_edges = edges
                                .map(|&Tup2(x, y)| Tup2(y, x))
                                .set_persistent_id(Some("reverse_edges"));
                            let reverse_edges_indexed = reverse_edges
                                .map_index(|Tup2(x, y)| (*x, *y))
                                .set_persistent_id(Some("reverse_edges_indexed"));

                            let reachable_next = edges.plus(
                                &reachable_indexed
                                    .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                            );
                            reachable_next.set_persistent_id(Some("reachable_next"));
                            let reachable_reverse_next = reverse_edges.plus(
                                &reachable_reverse_indexed
                                    .join(&reverse_edges_indexed, |_via, from, to| {
                                        Tup2(*from, *to)
                                    }),
                            );
                            reachable_reverse_next
                                .set_persistent_id(Some("reachable_reverse_next"));

                            Ok((reachable_next, reachable_reverse_next))
                        },
                    )
                    .unwrap();

                let reachable: Stream<_, OrdZSet<Edge>> = reachable.map(|Tup2(x, y)| Tup2(*x, *y));
                let reachable_reverse: Stream<_, OrdZSet<Edge>> =
                    reachable_reverse.map(|Tup2(x, y)| Tup2(*y, *x));

                vec![
                    reachable.accumulate_output_persistent(Some("reachable_out")),
                    reachable_reverse.accumulate_output_persistent(Some("reachable_reverse_out")),
                ]
            });
        }

        /// A rewrite of [`reachability2()`] using
        /// [`recursive_dynamic`](crate::ChildCircuit::recursive_dynamic):
        /// Forward and backward reachability as two recursive relations
        /// supplied as a two-element vector (arity 2).  It must match the
        /// tuple implementation.
        #[test]
        fn reachability2_dynamic() {
            checkpoint_and_restart(|circuit, skip| {
                let mut edges = edges_data().into_iter().skip(skip);
                let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

                let mut recursive_streams = circuit
                    .recursive_dynamic(
                        2,
                        |child, mut recursive_streams: Vec<Stream<_, OrdZSet<Edge>>>| {
                            let edges = edges.delta0(child);

                            let (reachable, rest) = recursive_streams.split_first_mut().unwrap();
                            let reachable_reverse = rest.first_mut().unwrap();

                            reachable.set_persistent_id(Some("reachable"));
                            reachable_reverse.set_persistent_id(Some("reachable_reverse"));

                            let edges_indexed = edges
                                .map_index(|Tup2(x, y)| (*x, *y))
                                .set_persistent_id(Some("edges_indexed"));
                            let reachable_indexed = reachable
                                .map_index(|&Tup2(x, y)| (y, x))
                                .set_persistent_id(Some("reachable_indexed"));
                            let reachable_reverse_indexed = reachable_reverse
                                .map_index(|&Tup2(x, y)| (y, x))
                                .set_persistent_id(Some("reachable_reverse_indexed"));
                            let reverse_edges = edges
                                .map(|&Tup2(x, y)| Tup2(y, x))
                                .set_persistent_id(Some("reverse_edges"));
                            let reverse_edges_indexed = reverse_edges
                                .map_index(|Tup2(x, y)| (*x, *y))
                                .set_persistent_id(Some("reverse_edges_indexed"));

                            let reachable_next = edges.plus(
                                &reachable_indexed
                                    .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                            );
                            reachable_next.set_persistent_id(Some("reachable_next"));

                            let reachable_reverse_next = reverse_edges.plus(
                                &reachable_reverse_indexed
                                    .join(&reverse_edges_indexed, |_via, from, to| {
                                        Tup2(*from, *to)
                                    }),
                            );
                            reachable_reverse_next
                                .set_persistent_id(Some("reachable_reverse_next"));

                            // We can even reuse the allocated vector and spare us a reallocation.
                            *reachable = reachable_next;
                            *reachable_reverse = reachable_reverse_next;
                            Ok(recursive_streams)
                        },
                    )
                    .unwrap();

                let reachable_reverse = recursive_streams.pop().unwrap();
                let reachable = recursive_streams.pop().unwrap();

                let reachable_reverse = reachable_reverse.map(|Tup2(x, y)| Tup2(*y, *x));

                vec![
                    reachable.accumulate_output_persistent(Some("reachable_out")),
                    reachable_reverse.accumulate_output_persistent(Some("reachable_reverse_out")),
                ]
            });
        }
    }
}
