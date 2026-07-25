use crate::Timestamp;
use crate::circuit::Consensus;
use crate::circuit::checkpointer::Checkpoint;
use crate::circuit::circuit_builder::{CircuitBase, IterativeCircuit};
use crate::{
    ChildCircuit, Circuit, DBData, SchedulerError, Stream, ZWeight,
    dynamic::Erase,
    operator::{
        DelayedFeedback,
        dynamic::{
            distinct::DistinctFactories, recursive::RecursiveStreams as DynRecursiveStreams,
        },
    },
    trace::{Batch, Spine},
    typed_batch::{BatchReader, DynIndexedZSet, TypedBatch},
};
use impl_trait_for_tuples::impl_for_tuples;
use size_of::SizeOf;
use std::cell::Cell;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::rc::Rc;

pub trait RecursiveStreams<C>: Clone {
    type Inner: DynRecursiveStreams<C> + Clone;
    type Output;

    /// Returns a strongly typed version of the streams.
    ///
    /// # Safety
    ///
    /// `inner` must be backed by concrete types that match `Self`.
    unsafe fn typed(inner: &Self::Inner) -> Self;

    /// Returns a strongly typed version of output streams.
    ///
    /// # Safety
    ///
    /// `inner` must be backed by concrete types that match `Self::Output`.
    unsafe fn typed_exports(
        inner: &<Self::Inner as DynRecursiveStreams<C>>::Output,
    ) -> Self::Output;

    fn inner(&self) -> Self::Inner;
    fn factories() -> <Self::Inner as DynRecursiveStreams<C>>::Factories;
}

impl<K, V, B, C> RecursiveStreams<C> for Stream<C, TypedBatch<K, V, ZWeight, B>>
where
    C: Circuit,
    C::Parent: Circuit,
    B: Checkpoint + DynIndexedZSet + Send + Sync,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
{
    type Inner = Stream<C, B>;
    type Output = Stream<C::Parent, TypedBatch<K, V, ZWeight, B>>;

    unsafe fn typed(inner: &Self::Inner) -> Self {
        Stream::typed(inner)
    }

    unsafe fn typed_exports(
        inner: &<Self::Inner as DynRecursiveStreams<C>>::Output,
    ) -> Self::Output {
        Stream::typed(inner)
    }

    fn inner(&self) -> Self::Inner {
        self.inner()
    }

    fn factories() -> <Self::Inner as DynRecursiveStreams<C>>::Factories {
        DistinctFactories::new::<K, V>()
    }
}

#[allow(clippy::unused_unit)]
#[impl_for_tuples(14)]
#[tuple_types_custom_trait_bound(RecursiveStreams<C>)]
impl<C> RecursiveStreams<C> for Tuple {
    for_tuples!( type Inner = ( #( Tuple::Inner ),* ); );
    for_tuples!( type Output = ( #( Tuple::Output ),* ); );

    unsafe fn typed(inner: &Self::Inner) -> Self {
        (for_tuples!( #( Tuple::typed(&inner.Tuple) ),* ))
    }

    unsafe fn typed_exports(
        inner: &<Self::Inner as DynRecursiveStreams<C>>::Output,
    ) -> Self::Output {
        (for_tuples!( #( Tuple::typed_exports(&inner.Tuple) ),* ))
    }

    fn inner(&self) -> Self::Inner {
        (for_tuples!( #( self.Tuple.inner() ),* ))
    }

    fn factories() -> <Self::Inner as DynRecursiveStreams<C>>::Factories {
        (for_tuples!( #( Tuple::factories() ),* ))
    }
}

impl<P, T> ChildCircuit<P, T>
where
    P: 'static,
    T: Timestamp,
    Self: Circuit,
{
    /// Create a nested circuit that computes one or more mutually recursive
    /// streams of Z-sets.
    ///
    /// This method implements a common form of iteration that computes a
    /// solution to an equation `x = f(i, x)` as a fixed point of function
    /// `f`.  Here `x` is a single Z-set or multiple mutually recursive
    /// Z-sets.  The computation is maintained incrementally: at each clock
    /// cycle, the parent circuit feeds an update `Δi` to the external input
    /// `i` of the nested circuit, and the nested circuit computes `Δx = y
    /// - x`, where `y` is a solution to the equation `y = f(i+Δi, y)`.
    ///
    /// This method is a wrapper around [`Circuit::fixedpoint`] that
    /// conceptually constructs the following circuit (the exact circuit is
    /// somewhat different as it takes care of maintaining the computation
    /// incrementally):
    ///
    /// ```text
    ///     ┌────────────────────────────────────────┐
    ///     │                                        │
    ///  i  │            ┌───┐                       │
    /// ────┼──►δ0──────►│   │      ┌────────┐       │
    ///     │            │ f ├─────►│distinct├──┬────┼──►
    ///     │    ┌──────►│   │      └────────┘  │    │
    ///     │    │       └───┘                  │    │
    ///     │    │                              │    │
    ///     │    │                              │    │
    ///     │    │       ┌────┐                 │    │
    ///     │    └───────┤z^-1│◄────────────────┘    │
    ///     │            └────┘                      │
    ///     │                                        │
    ///     └────────────────────────────────────────┘
    /// ```
    ///
    /// where the `z^-1` operator connects the previous output of function `f`
    /// to its input at the next iteration of the fixed point computation.
    ///
    /// Note the `distinct` operator attached to the output of `f`.  Most
    /// recursive computations over Z-sets require this for convergence;
    /// otherwise their output weights keep growing even when the set of
    /// elements in the Z-set no longer changes. Hence, strictly speaking
    /// this circuit computes the fixed point of equation
    /// `y = distinct(f(i+Δi, y))`.
    ///
    /// Finally, the `δ0` block in the diagram represents the
    /// [`delta0`](`crate::circuit::Stream::delta0`) operator, which imports
    /// streams from the parent circuit into the nested circuit.  This
    /// operator must be instantiated manually by the closure `f` for each
    /// input stream.
    ///
    /// # Examples
    ///
    /// ```
    /// use dbsp::{
    ///     operator::Generator,
    ///     OrdZSet,
    ///     Circuit, RootCircuit, Stream, zset, zset_set,
    ///     utils::Tup2,
    ///     Error as DbspError, Runtime
    /// };
    ///
    /// const STEPS: usize = 3;
    ///
    /// // Propagate labels along graph edges.
    /// let (mut circuit_handle, _output_handle) = Runtime::init_circuit(1, move |root_circuit| {
    ///     // Graph topology.
    ///     let mut edges = ([
    ///         // Start with four nodes connected in a cycle.
    ///         zset_set! { Tup2(1, 2), Tup2(2, 3), Tup2(3, 4), Tup2(4, 1) },
    ///         // Add an edge.
    ///         zset_set! { Tup2(4, 5) },
    ///         // Remove an edge, breaking the cycle.
    ///         zset! { Tup2(1, 2) => -1 },
    ///      ] as [OrdZSet<Tup2<u64, u64>>; STEPS])
    ///          .into_iter();
    ///
    ///     let edges = root_circuit
    ///             .add_source(Generator::new(move || edges.next().unwrap()));
    ///
    ///     // Initial labeling of the graph.
    ///     let mut init_labels = ([
    ///         // Start with a single label on node 1.
    ///         zset_set! { Tup2(1, "l1".to_string()) },
    ///         // Add a label to node 2.
    ///         zset_set! { Tup2(2, "l2".to_string()) },
    ///         zset! { },
    ///     ] as [OrdZSet<Tup2<u64, String>>; STEPS])
    ///         .into_iter();
    ///
    ///     let init_labels = root_circuit
    ///             .add_source(Generator::new(move || init_labels.next().unwrap()));
    ///
    ///     // Expected _changes_ to the output graph labeling after each clock cycle.
    ///     let mut expected_outputs = ([
    ///         zset! { Tup2(1, "l1".to_string()) => 1, Tup2(2, "l1".to_string()) => 1, Tup2(3, "l1".to_string()) => 1, Tup2(4, "l1".to_string()) => 1 },
    ///         zset! { Tup2(1, "l2".to_string()) => 1, Tup2(2, "l2".to_string()) => 1, Tup2(3, "l2".to_string()) => 1, Tup2(4, "l2".to_string()) => 1, Tup2(5, "l1".to_string()) => 1, Tup2(5, "l2".to_string()) => 1 },
    ///         zset! { Tup2(2, "l1".to_string()) => -1, Tup2(3, "l1".to_string()) => -1, Tup2(4, "l1".to_string()) => -1, Tup2(5, "l1".to_string()) => -1 },
    ///     ] as [OrdZSet<Tup2<u64, String>>; STEPS])
    ///         .into_iter();
    ///
    ///     let labels = root_circuit.recursive(|child_circuit, labels: Stream<_, OrdZSet<Tup2<u64, String>>>| {
    ///         // Import `edges` and `init_labels` relations from the parent circuit.
    ///         let edges = edges.delta0(child_circuit);
    ///         let init_labels = init_labels.delta0(child_circuit);
    ///
    ///         // Given an edge `from -> to` where the `from` node is labeled with `l`,
    ///         // propagate `l` to node `to`.
    ///         let result = labels.map_index(|Tup2(x,y)| (x.clone(), y.clone()))
    ///               .join(
    ///                   &edges.map_index(|Tup2(x,y)| (x.clone(), y.clone())),
    ///                   |_from, l, to| Tup2(*to, l.clone()),
    ///               )
    ///               .plus(&init_labels);
    ///         Ok(result)
    ///     })?;
    ///
    ///     labels.inspect(move |ls| {
    ///         assert_eq!(*ls, expected_outputs.next().unwrap());
    ///     });
    ///
    ///     Ok(labels.output())
    /// })?;
    ///
    /// for _ in 0..STEPS {
    ///     circuit_handle.transaction().unwrap();
    /// }
    ///
    /// Ok::<(), DbspError>(())
    /// ```
    #[track_caller]
    pub fn recursive<F, S>(&self, f: F) -> Result<S::Output, SchedulerError>
    where
        S: RecursiveStreams<IterativeCircuit<Self>>,
        F: FnOnce(&IterativeCircuit<Self>, S) -> Result<S, SchedulerError>,
    {
        self.dyn_recursive(&S::factories(), |circuit, streams: S::Inner| {
            f(circuit, unsafe { S::typed(&streams) }).map(|streams| streams.inner())
        })
        .map(|streams| unsafe { S::typed_exports(&streams) })
    }

    /// Like [`ChildCircuit::recursive`], but for a group of mutually recursive
    /// streams whose size is only known at runtime.
    ///
    /// Whereas [`recursive`](ChildCircuit::recursive) fixes the number of
    /// recursive streams at compile time (a single stream or a tuple of
    /// streams), this method computes a fixed point over `arity` mutually
    /// recursive streams that all share the same key type `K`, value type `V`,
    /// and batch type `B`.  The `arity` cannot be inferred, because the
    /// recursive streams are the feedback Z-sets created *before* the closure
    /// runs; it must therefore be supplied explicitly by the caller.
    ///
    /// The closure `f` receives a vector of `arity` recursive input streams and
    /// must return a vector of exactly `arity` output streams, one per recursive
    /// relation.
    ///
    /// Similar to [`recursive`](ChildCircuit::recursive), the underlying
    /// circuit also applies an implicit distinct to the output of each
    /// recursive step.
    ///
    /// # Panics
    ///
    /// Panics if the returned vector from the closure parameter has a different
    /// length than the `arity` parameter.
    ///
    /// # Examples
    ///
    /// The circuit below computes a two-coloring (red and blue) of a graph.  If
    /// no node is both red and blue the graph happens to be bipartite.  In the
    /// first two computation steps the graph is bipartite but the added edge
    /// in the third step adds an odd-length cycle which destroys the bipartite
    /// property and all nodes are colored red and blue.
    ///
    /// ```
    /// use dbsp::{
    ///     operator::Generator,
    ///     OrdZSet, Circuit, RootCircuit, Stream, zset, ZWeight,
    ///     utils::Tup2, Error as DbspError, Runtime, NestedCircuit
    /// };
    ///
    /// type Edge = Tup2<usize, usize>;
    /// type Node = usize;
    ///
    /// const STEPS: usize = 3;
    ///
    /// let mut init_data = ([
    ///     vec![Tup2(0, 1)],
    ///     vec![],
    ///     vec![]
    /// ] as [Vec<Tup2<Node, ZWeight>>; STEPS]).into_iter();
    ///
    /// let mut edges_data = ([
    ///     // The first step adds a graph of four nodes:
    ///     // |0| --> |1| --> |2| --> |3| --> |4|
    ///     vec![
    ///         Tup2(Tup2(0, 1), 1),
    ///         Tup2(Tup2(1, 2), 1),
    ///         Tup2(Tup2(2, 3), 1),
    ///         Tup2(Tup2(3, 4), 1),
    ///     ],
    ///     // Now, we have the following graph in total:
    ///     // |0| --> |1| --> |2| --> |3| --> |4|
    ///     //  ^               |
    ///     //  |               |
    ///     //  ------ |5| <-----
    ///     vec![Tup2(Tup2(2, 5), 1), Tup2(Tup2(5, 0), 1)],
    ///     // And we introduce an odd-length cycle, rendering the graph
    ///     // non-bipartite anymore (all nodes are red _and_ blue):
    ///     // |0| --> |1| --> |2| --> |3| --> |4|
    ///     //  ^               |               |
    ///     //  |               |               |
    ///     //  ------ |5| <-----               |
    ///     //  |                               |
    ///     //  ---------------------------------
    ///     vec![Tup2(Tup2(4, 0), 1)],
    /// ] as [Vec<Tup2<Edge, ZWeight>>; STEPS]).into_iter();
    ///
    /// let mut expected_red_output = ([
    ///     zset! {
    ///         0 => 1,
    ///         2 => 1,
    ///         4 => 1,
    ///     },
    ///     zset! {},
    ///     zset! {
    ///         1 => 1,
    ///         3 => 1,
    ///         5 => 1,
    ///     },
    /// ] as [OrdZSet<Node>; STEPS]).into_iter();
    ///
    /// let mut expected_blue_output = ([
    ///     zset! {
    ///         1 => 1,
    ///         3 => 1,
    ///     },
    ///     zset! {
    ///         5 => 1,
    ///     },
    ///     zset! {
    ///         0 => 1,
    ///         2 => 1,
    ///         4 => 1,
    ///     },
    /// ] as [OrdZSet<Node>; STEPS]).into_iter();
    ///
    /// let (mut circuit_handle, ((init_input, edges_input), (red_output, blue_output))) =
    ///     Runtime::init_circuit(2, move |root_circuit| {
    ///         let (edges, edges_input) = root_circuit.add_input_zset::<Edge>();
    ///         let (init, init_input) = root_circuit.add_input_zset::<Node>();
    ///
    ///         let recursive_streams = root_circuit.recursive_dynamic(
    ///             2,
    ///             |child_circuit, mut recursive_streams: Vec<Stream<NestedCircuit, OrdZSet<usize>>>| {
    ///                 // delta0 fires only at inner step 0, injecting the base case exactly once.
    ///                 let edges = edges.delta0(child_circuit);
    ///                 let init = init.delta0(child_circuit);
    ///
    ///                 let red = &recursive_streams[0];
    ///                 let blue = &recursive_streams[1];
    ///
    ///                 let new_red = blue
    ///                     .map_index(|blue_node| (*blue_node, *blue_node))
    ///                     .join(
    ///                         &edges.map_index(|Tup2(from, to)| (*from, *to)),
    ///                         |_blue_node, _, new_red_node| *new_red_node,
    ///                     )
    ///                     .plus(&init);
    ///
    ///                 let new_blue = red.map_index(|red_node| (*red_node, *red_node)).join(
    ///                     &edges.map_index(|Tup2(from, to)| (*from, *to)),
    ///                     |_red_node, _, new_blue_node| *new_blue_node,
    ///                 );
    ///
    ///                 recursive_streams[0] = new_red;
    ///                 recursive_streams[1] = new_blue;
    ///                 Ok(recursive_streams)
    ///             },
    ///         )?;
    ///
    ///         let red_output = recursive_streams[0].accumulate_output();
    ///         let blue_output = recursive_streams[1].accumulate_output();
    ///
    ///         Ok((
    ///             (init_input, edges_input),
    ///             (red_output, blue_output),
    ///         ))
    ///     })?;
    ///
    /// for i in 0..STEPS {
    ///     init_input.append(&mut init_data.next().unwrap());
    ///     edges_input.append(&mut edges_data.next().unwrap());
    ///     circuit_handle.transaction().unwrap();
    ///     assert_eq!(red_output.concat().consolidate(), expected_red_output.next().unwrap());
    ///     assert_eq!(blue_output.concat().consolidate(), expected_blue_output.next().unwrap());
    /// }
    ///
    /// Ok::<(), DbspError>(())
    /// ```
    #[track_caller]
    pub fn recursive_dynamic<F, K, V, B>(
        &self,
        arity: usize,
        f: F,
    ) -> Result<Vec<Stream<Self, TypedBatch<K, V, ZWeight, B>>>, SchedulerError>
    where
        B: Checkpoint + DynIndexedZSet + Send + Sync,
        K: DBData + Erase<B::Key>,
        V: DBData + Erase<B::Val>,
        F: FnOnce(
            &IterativeCircuit<Self>,
            Vec<Stream<IterativeCircuit<Self>, TypedBatch<K, V, ZWeight, B>>>,
        ) -> Result<
            Vec<Stream<IterativeCircuit<Self>, TypedBatch<K, V, ZWeight, B>>>,
            SchedulerError,
        >,
    {
        let factories: Vec<DistinctFactories<B, _>> = (0..arity)
            .map(|_| DistinctFactories::new::<K, V>())
            .collect();

        self.dyn_recursive(&factories, |circuit, streams: Vec<Stream<_, B>>| {
            let typed = streams.iter().map(Stream::typed).collect();
            f(circuit, typed).map(|streams| streams.iter().map(Stream::inner).collect())
        })
        .map(|exports| exports.iter().map(Stream::typed).collect())
    }

    /// Create a single recursive variable: an initially empty feedback stream
    /// that a [`RecursionBuilder`] later ties into a fixed-point loop.
    ///
    /// This is the building block of the unified recursion API.  Call it once
    /// per mutually recursive relation inside the builder's `init` closure; the
    /// number of calls determines the arity of the recursion, so the caller
    /// never supplies it explicitly.  See [`RecursionBuilder`] for a complete
    /// example.
    pub fn recursive_var<Z>(&self) -> RecursiveVar<Self, Z>
    where
        Z: BatchReader<R = ZWeight>,
        Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
        Spine<Z::Inner>: SizeOf,
        <Self as Circuit>::Parent: Circuit,
    {
        let factories = DistinctFactories::new::<Z::Key, Z::Val>();
        let feedback =
            DelayedFeedback::with_default(self, Z::Inner::dyn_empty(&factories.input_factories));
        let stream = feedback.stream().typed::<Z>();

        RecursiveVar {
            feedback,
            factories,
            stream,
        }
    }

    /// Define a recursive computation over one or more mutually recursive
    /// streams.
    ///
    /// This is the unified entry point that subsumes both
    /// [`recursive`](Self::recursive) and
    /// [`recursive_dynamic`](Self::recursive_dynamic).  The `init` closure sets
    /// up the recursive variables with [`recursive_var`](Self::recursive_var);
    /// the shape it returns — a single [`RecursiveVar`], a tuple of them, or a
    /// [`Vec`] — fixes the arity, so none has to be supplied.  The `step`
    /// closure receives the matching feedback streams and defines the recursive
    /// computation.
    ///
    /// The call returns a [`RecursionBuilder`] for this specific recursive
    /// computation, and the builder offers optional modifiers
    /// (for example [`without_distinct`](RecursionBuilder::without_distinct))
    /// before [`finish`](RecursionBuilder::finish) builds it.
    ///
    /// See [`RecursionBuilder`] for a complete example.
    pub fn recursion<V, F1, F2>(&self, init: F1, step: F2) -> RecursionBuilder<'_, Self, F1, F2>
    where
        V: RecursionVars<IterativeCircuit<Self>>,
        F1: FnOnce(&IterativeCircuit<Self>) -> Result<V, SchedulerError>,
        F2: FnOnce(&IterativeCircuit<Self>, V::Streams) -> Result<V::Streams, SchedulerError>,
    {
        RecursionBuilder {
            circuit: self,
            init,
            step,
            distinct: true,
            bounded: None,
            report: NoReport(PhantomData),
        }
    }
}

/// A single recursive variable created by
/// [`recursive_var`](ChildCircuit::recursive_var).
///
/// It bundles an initially empty feedback stream with the factories and
/// feedback connector needed to close its loop once the step function has
/// produced the next iteration.  A [`RecursionBuilder`] performs the wiring;
/// callers only ever touch the [`stream`](RecursiveVar::stream) they feed into
/// their recursive step.
pub struct RecursiveVar<C, Z>
where
    C: Circuit,
    Z: BatchReader,
    Z::Inner: DynIndexedZSet,
{
    feedback: DelayedFeedback<C, Z::Inner>,
    factories: DistinctFactories<Z::Inner, C::Time>,
    stream: Stream<C, Z>,
}

impl<C, Z> RecursiveVar<C, Z>
where
    C: Circuit,
    C::Parent: Circuit,
    Z: BatchReader<R = ZWeight>,
    Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
    Spine<Z::Inner>: SizeOf,
{
    /// Close this variable's loop: optionally apply `distinct`, connect the
    /// feedback, and export the integrated trace to the parent circuit.
    fn close(self, next: Stream<C, Z>, distinct: bool) -> ClosedVar<C, Z> {
        let RecursiveVar {
            feedback,
            factories,
            ..
        } = self;

        let next = next.inner();
        let next = if distinct {
            let persistent_id = next
                .get_persistent_id()
                .map(|name| format!("{name}.distinct"));
            next.dyn_distinct(&factories)
                .set_persistent_id(persistent_id.as_deref())
        } else {
            next
        };

        feedback.connect(&next);
        let export = next
            .dyn_integrate_trace(&factories.input_factories)
            .export();

        ClosedVar { export, factories }
    }
}

/// A [`RecursiveVar`] whose loop has been closed.
///
/// Retains the factories needed to consolidate the exported trace into the
/// final output stream in the parent circuit.
pub struct ClosedVar<C, Z>
where
    C: Circuit,
    Z: BatchReader,
    Z::Inner: DynIndexedZSet,
{
    export: Stream<C::Parent, Spine<Z::Inner>>,
    factories: DistinctFactories<Z::Inner, C::Time>,
}

impl<C, Z> ClosedVar<C, Z>
where
    C: Circuit,
    C::Parent: Circuit,
    Z: BatchReader<R = ZWeight>,
    Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
    Spine<Z::Inner>: SizeOf,
{
    fn consolidate(self) -> Stream<C::Parent, Z> {
        self.export
            .dyn_consolidate(&self.factories.input_factories)
            .typed::<Z>()
    }
}

/// Generalizes closing a recursive fixed-point loop over a group of
/// [`RecursiveVar`]s.
///
/// This is the [`RecursionBuilder`] counterpart to [`DynRecursiveStreams`]: it
/// is implemented for the shapes an `init` closure may return, namely a single
/// [`RecursiveVar`], a tuple of [`RecursiveVar`]s, or a [`Vec`] of them.
/// A single variable (or a tuple of variables) recovers the behavior of
/// [`recursive`](ChildCircuit::recursive) over one stream, whereas the vector
/// recovers [`recursive_dynamic`](ChildCircuit::recursive_dynamic) with its
/// arity inferred from the vector's length.
pub trait RecursionVars<C: Circuit> {
    /// Streams handed to the step closure and returned by it.
    type Streams;

    /// Per-variable output traces exported to the parent circuit.
    type Export;

    /// Final, consolidated output streams in the parent circuit.
    type Output;

    /// The feedback streams to feed into the recursive step.
    fn streams(&self) -> Self::Streams;

    /// Close every feedback loop in the group, returning their exported traces.
    ///
    /// # Panics
    ///
    /// Panics if `next` does not contain exactly one stream per recursive
    /// variable in the group.
    fn close(self, next: Self::Streams, distinct: bool) -> Self::Export;

    /// Consolidate the exported traces into the final output streams.
    fn consolidate(exports: Self::Export) -> Self::Output;

    /// Produce a per-transaction [`RecursionReport`] stream by sampling
    /// `outcome` once the nested epoch has finished.
    ///
    /// The sampler is attached to one of the recursion's *existing* export
    /// streams (never a fresh in-loop operator, which — by changing every
    /// iteration — would prevent the fixed-point check from ever succeeding),
    /// so the sample is naturally scheduled after the recursion has run for the
    /// transaction.  Any group with at least one variable can report.
    fn report<G>(exports: &Self::Export, outcome: G) -> Stream<C::Parent, RecursionReport>
    where
        G: Fn() -> RecursionReport + 'static;
}

impl<C, Z> RecursionVars<C> for RecursiveVar<C, Z>
where
    C: Circuit,
    C::Parent: Circuit,
    Z: BatchReader<R = ZWeight>,
    Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
    Spine<Z::Inner>: SizeOf,
{
    type Streams = Stream<C, Z>;
    type Export = ClosedVar<C, Z>;
    type Output = Stream<C::Parent, Z>;

    fn streams(&self) -> Self::Streams {
        self.stream.clone()
    }

    fn close(self, next: Self::Streams, distinct: bool) -> Self::Export {
        RecursiveVar::close(self, next, distinct)
    }

    fn consolidate(export: Self::Export) -> Self::Output {
        ClosedVar::consolidate(export)
    }

    fn report<G>(export: &Self::Export, outcome: G) -> Stream<C::Parent, RecursionReport>
    where
        G: Fn() -> RecursionReport + 'static,
    {
        export.export.apply(move |_| outcome())
    }
}

impl<C, Z> RecursionVars<C> for Vec<RecursiveVar<C, Z>>
where
    C: Circuit,
    C::Parent: Circuit,
    Z: BatchReader<R = ZWeight>,
    Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
    Spine<Z::Inner>: SizeOf,
{
    type Streams = Vec<Stream<C, Z>>;
    type Export = Vec<ClosedVar<C, Z>>;
    type Output = Vec<Stream<C::Parent, Z>>;

    fn streams(&self) -> Self::Streams {
        self.iter().map(|var| var.stream.clone()).collect()
    }

    fn close(self, next: Self::Streams, distinct: bool) -> Self::Export {
        assert_eq!(
            self.len(),
            next.len(),
            "the recursive step must return exactly one stream per recursive variable"
        );

        self.into_iter()
            .zip(next)
            .map(|(var, next)| var.close(next, distinct))
            .collect()
    }

    fn consolidate(exports: Self::Export) -> Self::Output {
        exports.into_iter().map(ClosedVar::consolidate).collect()
    }

    fn report<G>(exports: &Self::Export, outcome: G) -> Stream<C::Parent, RecursionReport>
    where
        G: Fn() -> RecursionReport + 'static,
    {
        // The Vec's first element creates the report; other elements are left
        // untouched, so exactly one report stream is created.
        exports
            .first()
            .expect("a recursion has at least one variable")
            .export
            .apply(move |_| outcome())
    }
}

/// A fixed-size, heterogeneous group of recursive variables.
///
/// Each element may carry a different batch type, so this recovers the
/// mutually-recursive-streams-of-different-types case handled by
/// [`recursive`](ChildCircuit::recursive) over a tuple.
#[allow(clippy::unused_unit)]
#[impl_for_tuples(1, 14)]
#[tuple_types_custom_trait_bound(RecursionVars<C>)]
impl<C: Circuit> RecursionVars<C> for Tuple {
    for_tuples!( type Streams = ( #( Tuple::Streams ),* ); );
    for_tuples!( type Export = ( #( Tuple::Export ),* ); );
    for_tuples!( type Output = ( #( Tuple::Output ),* ); );

    fn streams(&self) -> Self::Streams {
        (for_tuples!( #( self.Tuple.streams() ),* ))
    }

    fn close(self, next: Self::Streams, distinct: bool) -> Self::Export {
        (for_tuples!( #( self.Tuple.close(next.Tuple, distinct) ),* ))
    }

    fn consolidate(exports: Self::Export) -> Self::Output {
        (for_tuples!( #( Tuple::consolidate(exports.Tuple) ),* ))
    }

    fn report<G>(exports: &Self::Export, outcome: G) -> Stream<C::Parent, RecursionReport>
    where
        G: Fn() -> RecursionReport + 'static,
    {
        // Delegate to the tuple's first element; other elements are left
        // untouched, so exactly one report stream is created.
        <TupleElement0 as RecursionVars<C>>::report(&exports.0, outcome)
    }
}

/// A report on a recursive computation for a single transaction, produced when
/// reporting is enabled via [`with_report`](RecursionBuilder::with_report).
///
/// One value is emitted per transaction on the stream returned alongside the
/// recursion's output.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecursionReport {
    /// Number of fixed-point iterations performed this transaction.
    iterations: u64,

    /// Whether the recursion reached a fixed point.  `false` means it was cut
    /// short by the bound set with [`with_bound`](RecursionBuilder::with_bound),
    /// i.e. the result is a truncated approximation.
    converged: bool,
}

impl RecursionReport {
    /// Returns `Some(iterations)` if the computation naturally reached
    /// a fixed point without being cut short by a bound set with
    /// [`with_bound`](RecursionBuilder::with_bound).
    /// In the latter case, `None` is returned.
    pub fn converged_iterations(&self) -> Option<u64> {
        if self.converged {
            Some(self.iterations)
        } else {
            None
        }
    }
    /// Returns `true` if the computation did converge and any bound set with
    /// [`with_bound`](RecursionBuilder::with_bound) was *not* effective.
    pub fn converged(&self) -> bool {
        self.converged
    }
    /// Returns `true` if the computation did not converge but short-circuited
    /// by the bound set with [`with_bound`](RecursionBuilder::with_bound).
    /// Note that the computation result is a truncated, partial result.
    pub fn truncated(&self) -> bool {
        !self.converged
    }
    /// Reports back the number of iterations the computation took.
    pub fn iterations(&self) -> u64 {
        self.iterations
    }
}

mod sealed {
    /// Private supertrait that seals [`ReportMode`](super::ReportMode) to this
    /// module: it cannot be  implemented from outside, so neither can
    /// `ReportMode`.
    pub trait Sealed {}
}

/// Sealed marker trait for a [`RecursionBuilder`]'s reporting type-state.
///
/// Implemented only by [`NoReport`] and [`Reporting`]; the sealed supertrait
/// makes it impossible to implement for any other type.  This closes the set of
/// reporting states, so a `RecursionBuilder` can never be parameterized with a
/// foreign `R` that would leave it without a
/// [`finish`](RecursionBuilder::finish) implementation.
///
/// Each state also *carries* its own reporting state: [`Reporting`] owns a
/// shared cell (allocated by [`with_report`](RecursionBuilder::with_report))
/// that the recursion records its outcome into and later samples into a stream,
/// whereas [`NoReport`] is zero-sized and does neither. Hence, a non-reporting
/// run allocates nothing, records nothing, and builds no stream.  The `Clone`
/// and `'static` supertrait bounds let a run clone this state into the
/// termination closure that the scheduler holds across nested clock cycles.
pub trait ReportMode: sealed::Sealed + Clone + 'static {
    /// Record the recursion's final outcome once it stops.  A no-op under
    /// [`NoReport`], so a non-reporting run performs no writes.
    fn record(&self, outcome: RecursionReport);

    /// Build the reporting stream from the exported traces, before the exports
    /// are consolidated.  Returns `None` under [`NoReport`] and `Some(stream)`
    /// under [`Reporting`], where it samples the recorded outcome once per
    /// transaction.
    fn build_report<C, V>(&self, exports: &V::Export) -> Option<Stream<C, RecursionReport>>
    where
        C: Circuit,
        V: RecursionVars<IterativeCircuit<C>>;
}

/// Type-state marker for a [`RecursionBuilder`] that does not report its
/// [`RecursionReport`]. [`finish`](RecursionBuilder::finish) returns the output
/// streams only.
///
/// Zero-sized: it carries no reporting state.  Its private field keeps it
/// non-constructible outside this module.
#[derive(Clone)]
pub struct NoReport(PhantomData<()>);

/// Type-state marker for a [`RecursionBuilder`] with reporting enabled.
/// [`finish`](RecursionBuilder::finish) additionally returns a
/// [`RecursionReport`] stream.  Enable through
/// [`with_report`](RecursionBuilder::with_report).
///
/// Carries the shared cell the recursion records its outcome into.  Its private
/// field also keeps it non-constructible outside this module.
#[derive(Clone)]
pub struct Reporting {
    /// Cell the termination check writes the final outcome into and the report
    /// stream samples after the nested epoch.  Allocated in
    /// [`with_report`](RecursionBuilder::with_report).
    recorder: Rc<Cell<RecursionReport>>,
}

impl Reporting {
    fn new() -> Self {
        Self {
            recorder: Rc::new(Cell::new(RecursionReport::default())),
        }
    }
}

impl sealed::Sealed for NoReport {}
impl sealed::Sealed for Reporting {}

impl ReportMode for NoReport {
    fn record(&self, _report: RecursionReport) {}

    fn build_report<C, V>(&self, _exports: &V::Export) -> Option<Stream<C, RecursionReport>>
    where
        C: Circuit,
        V: RecursionVars<IterativeCircuit<C>>,
    {
        None
    }
}

impl ReportMode for Reporting {
    fn record(&self, report: RecursionReport) {
        self.recorder.set(report);
    }

    fn build_report<C, V>(&self, exports: &V::Export) -> Option<Stream<C, RecursionReport>>
    where
        C: Circuit,
        V: RecursionVars<IterativeCircuit<C>>,
    {
        let recorder = self.recorder.clone();
        Some(V::report(exports, move || recorder.get()))
    }
}

/// A unified builder for recursive computations, returned by
/// [`recursion`](ChildCircuit::recursion).
///
/// [`RecursionBuilder`] subsumes both [`recursive`](ChildCircuit::recursive)
/// and [`recursive_dynamic`](ChildCircuit::recursive_dynamic) behind a single
/// entry point built from two closures:
///
/// 1. An **init** closure sets up the recursive variables by calling
///    [`recursive_var`](ChildCircuit::recursive_var) once per mutually
///    recursive relation.  Because the variables are *returned* rather than
///    counted, the arity of the recursion is inferred from the shape of the
///    return value; unlike `recursive_dynamic`, no explicit `arity` is needed.
/// 2. A **step** closure receives the recursive variables' feedback streams and
///    returns the next iteration.
///
/// The builder computes a fixed point of the step closure, applying an implicit
/// `distinct` to each recursive stream (disable with
/// [`without_distinct`](RecursionBuilder::without_distinct)).  Just like the
/// closures passed to `recursive`, the step closure imports base-case relations
/// from the parent circuit with [`delta0`](crate::circuit::Stream::delta0),
/// which injects them once at the first iteration.
///
/// [`with_report`](RecursionBuilder::with_report) opts into per-transaction
/// [`RecursionReport`] reporting: `finish` then also returns a stream of
/// outcomes (iteration count and whether the recursion converged), which is
/// especially useful together with [`with_bound`](RecursionBuilder::with_bound)
/// to detect truncated results.
///
/// # Examples
///
/// A single recursive relation (transitive closure), matching the shape handled
/// by [`recursive`](ChildCircuit::recursive):
///
/// ```
/// use dbsp::{
///     operator::Generator,
///     Circuit, RootCircuit, OrdZSet, zset,
///     utils::Tup2, Error as DbspError, Runtime,
/// };
///
/// type Edge = Tup2<u64, u64>;
///
/// let (mut circuit, _) = Runtime::init_circuit(1, |root_circuit| {
///     let mut edges = [zset! { Tup2(1u64, 2u64) => 1, Tup2(2, 3) => 1 }].into_iter();
///     let edges = root_circuit.add_source(Generator::new(move || edges.next().unwrap()));
///
///     // The `recursion` call defines the computation; the closures' types are
///     // inferred, so no circuit or stream annotations are needed.
///     let reachable = root_circuit
///         .recursion(
///             |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
///             |child, reachable| {
///                 let edges = edges.delta0(child);
///                 let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
///                 let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));
///
///                 Ok(edges.plus(
///                     &reachable_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
///                 ))
///             },
///         )
///         .finish()?;
///
///     Ok(reachable.output())
/// })?;
///
/// circuit.transaction().unwrap();
/// Ok::<(), DbspError>(())
/// ```
pub struct RecursionBuilder<'a, C, F1, F2, R: ReportMode = NoReport> {
    circuit: &'a C,
    init: F1,
    step: F2,
    distinct: bool,
    bounded: Option<NonZeroU64>,
    report: R,
}

impl<'a, C, F1, F2, R> RecursionBuilder<'a, C, F1, F2, R>
where
    C: Circuit,
    R: ReportMode,
{
    /// Do not apply an implicit `distinct` to the recursive streams.
    ///
    /// Most recursive computations over Z-sets require `distinct` to converge;
    /// disable it only when the step function already guarantees that the
    /// output weights stabilize.
    pub fn without_distinct(mut self) -> Self {
        self.distinct = false;
        self
    }

    /// Stop the recursion after at most `max_iterations` fixed-point
    /// iterations, even if no fixed point has been reached.
    ///
    /// By default the recursion runs until it converges (see
    /// [`finish`](Self::finish)).  With a bound, iteration also stops once
    /// `max_iterations` nested clock cycles have elapsed, whichever comes
    /// first.  This is useful to cap the cost of computations that converge
    /// slowly, or as a safety valve against non-converging steps.  Combine with
    /// [`with_report`](RecursionBuilder::with_report) to learn, per
    /// transaction, whether the bound truncated the result.
    pub fn with_bound<T: Into<NonZeroU64>>(mut self, max_iterations: T) -> Self {
        self.bounded = Some(max_iterations.into());
        self
    }

    /// Build the recursion, returning the consolidated output streams together
    /// with an optional per-transaction [`RecursionReport`] stream.
    ///
    /// Both the bounded and unbounded variants are driven through
    /// [`Circuit::iterate`], differing only in the termination check.  The
    /// check reproduces [`Circuit::fixedpoint`]'s condition — every operator is
    /// stable and all workers agree via [`Consensus`] — and additionally stops
    /// once the optional iteration bound is reached.  Because `converged` is a
    /// consensus value and the iteration counter advances in lockstep, every
    /// worker computes the same termination decision on the same iteration,
    /// which is required to avoid a deadlock.
    fn run<V>(self) -> Result<(V::Output, Option<Stream<C, RecursionReport>>), SchedulerError>
    where
        V: RecursionVars<IterativeCircuit<C>>,
        F1: FnOnce(&IterativeCircuit<C>) -> Result<V, SchedulerError>,
        F2: FnOnce(&IterativeCircuit<C>, V::Streams) -> Result<V::Streams, SchedulerError>,
    {
        let RecursionBuilder {
            circuit,
            init,
            step,
            distinct,
            bounded,
            report,
        } = self;

        // `report` carries the recorder chosen by the type-state: nothing under
        // `NoReport`, a shared `Rc<Cell<..>>` allocated by `with_report` under
        // `Reporting`. A clone goes into the termination closure to record the
        // outcome; the original samples that outcome into the report stream
        // after the epoch. So a non-reporting run allocates nothing, and its
        // `record`/`build_report` are no-ops.
        let terminate_report = report.clone();

        let exports = circuit.iterate(|child| {
            let vars = init(child)?;
            let streams = vars.streams();
            let next = step(child, streams)?;
            let exports = vars.close(next, distinct);

            let child = child.clone();
            let consensus = Consensus::new("recursion fixed point");
            // Counts iterations within the current nested epoch.  It persists
            // across transactions (the closure is built once), so it must be
            // reset when the epoch ends — otherwise the bound would be spent by
            // the first transaction and later ones would stop immediately.
            let iteration = Cell::new(0u64);

            let terminate = async move || {
                let count = iteration.get() + 1;
                iteration.set(count);

                let converged = consensus.check(child.check_fixedpoint(0)).await?;
                let stop = converged || bounded.is_some_and(|max| count >= u64::from(max));

                if stop {
                    terminate_report.record(RecursionReport {
                        iterations: count,
                        converged,
                    });
                    // Start the next transaction's epoch from zero. Explicitly
                    // tested in `with_bound_counter_resets_across_transactions`
                    // below.
                    debug_assert!(
                        stop,
                        "iteration.set(0) must only fire on the last epoch iteration"
                    );
                    iteration.set(0);
                }

                Ok(stop)
            };

            Ok((terminate, exports))
        })?;

        let report_stream = report.build_report::<C, V>(&exports);
        let output = V::consolidate(exports);

        Ok((output, report_stream))
    }
}

impl<'a, C, F1, F2> RecursionBuilder<'a, C, F1, F2, NoReport>
where
    C: Circuit,
{
    /// Emit a per-transaction [`RecursionReport`] alongside the recursion's
    /// output.
    ///
    /// After calling this, [`finish`](Self::finish) returns a tuple whose second
    /// element is a stream carrying one [`RecursionReport`] per transaction.
    pub fn with_report(self) -> RecursionBuilder<'a, C, F1, F2, Reporting> {
        RecursionBuilder {
            circuit: self.circuit,
            init: self.init,
            step: self.step,
            distinct: self.distinct,
            bounded: self.bounded,
            report: Reporting::new(),
        }
    }

    /// Build the recursive computation and return the consolidated output
    /// streams exported to the parent circuit.
    ///
    /// The recursion iterates to a fixed point, or until the bound set by
    /// [`with_bound`](Self::with_bound) is reached, whichever comes first.  The
    /// concrete return type mirrors the shape produced by `init`: a single
    /// stream yields a single output stream, a vector yields a vector of output
    /// streams.
    #[track_caller]
    pub fn finish<V>(self) -> Result<V::Output, SchedulerError>
    where
        V: RecursionVars<IterativeCircuit<C>>,
        F1: FnOnce(&IterativeCircuit<C>) -> Result<V, SchedulerError>,
        F2: FnOnce(&IterativeCircuit<C>, V::Streams) -> Result<V::Streams, SchedulerError>,
    {
        Ok(self.run::<V>()?.0)
    }
}

impl<'a, C, F1, F2> RecursionBuilder<'a, C, F1, F2, Reporting>
where
    C: Circuit,
{
    /// Build the recursive computation, returning its output streams together
    /// with a per-transaction [`RecursionReport`] stream.
    ///
    /// Like [`finish`](RecursionBuilder::finish) on the non-reporting builder,
    /// but the returned tuple's second element is a `Stream` that carries one
    /// [`RecursionReport`] per transaction (iteration count and whether the
    /// recursion converged).
    #[track_caller]
    pub fn finish<V>(self) -> Result<(V::Output, Stream<C, RecursionReport>), SchedulerError>
    where
        V: RecursionVars<IterativeCircuit<C>>,
        F1: FnOnce(&IterativeCircuit<C>) -> Result<V, SchedulerError>,
        F2: FnOnce(&IterativeCircuit<C>, V::Streams) -> Result<V::Streams, SchedulerError>,
    {
        let (output, report) = self.run::<V>()?;

        Ok((
            output,
            report.expect("reporting builds always produce an outcome stream"),
        ))
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroU64;

    use crate::{Circuit, Runtime, operator::Generator, typed_batch::OrdZSet, utils::Tup2, zset};

    type Edge = Tup2<usize, usize>;

    /// Changes to the edges relation, shared by the tests below.  Copied from
    /// the dynamic-layer tests so the builder API is checked against the exact
    /// same fixture as [`recursive`](crate::ChildCircuit::recursive) and
    /// [`recursive_dynamic`](crate::ChildCircuit::recursive_dynamic).
    fn edges_data() -> Vec<OrdZSet<Edge>> {
        vec![
            zset! { Tup2(1, 2) => 1 },
            zset! { Tup2(2, 3) => 1 },
            zset! { Tup2(1, 3) => 1 },
            zset! { Tup2(3, 1) => 1 },
            zset! { Tup2(3, 1) => -1 },
            zset! { Tup2(1, 2) => -1 },
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
            Tup2(2, 1) => 1, Tup2(3, 1) => 1, Tup2(3, 2) => 1 },
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

    /// Transitive closure via [`RecursionBuilder`] over a *single* recursive
    /// variable.  Must reproduce the output of the single-`Stream`
    /// [`recursive`](crate::ChildCircuit::recursive) implementation.
    #[test]
    fn reachability_builder() {
        let edges_data = edges_data();
        let steps = edges_data.len();
        let mut edges = edges_data.into_iter();
        let mut expected_reachable = expected_reachable().into_iter();

        let (mut handle, _) = Runtime::init_circuit(1, move |circuit| {
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let reachable = circuit
                .recursion(
                    // The number of `recursive_var` calls fixes the arity; here
                    // it is a single stream, so no arity has to be supplied.
                    |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
                    |child, reachable| {
                        let edges = edges.delta0(child);
                        let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                        let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));

                        Ok(edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        ))
                    },
                )
                .finish()
                .unwrap();

            reachable
                .integrate()
                .stream_distinct()
                .inspect(move |reachable| {
                    assert_eq!(*reachable, expected_reachable.next().unwrap());
                });

            Ok(())
        })
        .unwrap();

        for _ in 0..steps {
            handle.transaction().unwrap();
        }
    }

    /// Forward and backward reachability via [`RecursionBuilder`] over a *vector*
    /// of two recursive variables.  Unlike
    /// [`recursive_dynamic`](crate::ChildCircuit::recursive_dynamic), the arity
    /// (2) is inferred from the vector returned by the init closure.  Must match
    /// the tuple/dynamic implementations.
    #[test]
    fn reachability2_builder() {
        let edges_data = edges_data();
        let steps = edges_data.len();
        let mut edges = edges_data.into_iter();
        let expected_reachable = expected_reachable();
        let expected_reachable_reverse = expected_reachable.clone();
        let mut expected_reachable = expected_reachable.into_iter();
        let mut expected_reachable_reverse = expected_reachable_reverse.into_iter();

        let (mut root, _) = Runtime::init_circuit(1, move |circuit| {
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let mut reachable = circuit
                .recursion(
                    |child| {
                        Ok(vec![
                            child.recursive_var::<OrdZSet<Edge>>(),
                            child.recursive_var::<OrdZSet<Edge>>(),
                        ])
                    },
                    |child, streams| {
                        let edges = edges.delta0(child);

                        let reachable = &streams[0];
                        let reachable_reverse = &streams[1];

                        let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                        let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));
                        let reachable_reverse_indexed =
                            reachable_reverse.map_index(|&Tup2(x, y)| (y, x));
                        let reverse_edges = edges.map(|&Tup2(x, y)| Tup2(y, x));
                        let reverse_edges_indexed = reverse_edges.map_index(|Tup2(x, y)| (*x, *y));

                        let reachable_next = edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );
                        let reachable_reverse_next = reverse_edges.plus(
                            &reachable_reverse_indexed
                                .join(&reverse_edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );

                        Ok(vec![reachable_next, reachable_reverse_next])
                    },
                )
                .finish()
                .unwrap();

            let reachable_reverse = reachable.pop().unwrap();
            let reachable = reachable.pop().unwrap();

            reachable.integrate().stream_distinct().inspect(move |ps| {
                assert_eq!(*ps, expected_reachable.next().unwrap());
            });
            reachable_reverse
                .map(|Tup2(x, y)| Tup2(*y, *x))
                .integrate()
                .stream_distinct()
                .inspect(move |ps: &OrdZSet<_>| {
                    assert_eq!(*ps, expected_reachable_reverse.next().unwrap());
                });

            Ok(())
        })
        .unwrap();

        for _ in 0..steps {
            root.transaction().unwrap();
        }
    }

    /// The same forward/backward reachability as [`reachability2_builder`], but
    /// with the two recursive variables supplied as a *tuple* instead of a
    /// `Vec`.  This exercises the tuple [`RecursionVars`](super::RecursionVars)
    /// implementation and must produce identical output.
    #[test]
    fn reachability2_builder_tuple() {
        let edges_data = edges_data();
        let steps = edges_data.len();
        let mut edges = edges_data.into_iter();
        let expected_reachable = expected_reachable();
        let expected_reachable_reverse = expected_reachable.clone();
        let mut expected_reachable = expected_reachable.into_iter();
        let mut expected_reachable_reverse = expected_reachable_reverse.into_iter();

        let (mut root, _) = Runtime::init_circuit(1, move |circuit| {
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let ((reachable, reachable_reverse), report) = circuit
                .recursion(
                    // Two recursive variables of the same type, returned as a
                    // tuple; the arity (2) is fixed by the tuple's shape.
                    |child| {
                        Ok((
                            child.recursive_var::<OrdZSet<Edge>>(),
                            child.recursive_var::<OrdZSet<Edge>>(),
                        ))
                    },
                    |child, (reachable, reachable_reverse)| {
                        let edges = edges.delta0(child);

                        let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                        let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));
                        let reachable_reverse_indexed =
                            reachable_reverse.map_index(|&Tup2(x, y)| (y, x));
                        let reverse_edges = edges.map(|&Tup2(x, y)| Tup2(y, x));
                        let reverse_edges_indexed = reverse_edges.map_index(|Tup2(x, y)| (*x, *y));

                        let reachable_next = edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );
                        let reachable_reverse_next = reverse_edges.plus(
                            &reachable_reverse_indexed
                                .join(&reverse_edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );

                        Ok((reachable_next, reachable_reverse_next))
                    },
                )
                .with_report()
                .finish()
                .unwrap();

            report.inspect(move |report| {
                assert!(report.converged());
            });
            reachable.integrate().stream_distinct().inspect(move |ps| {
                assert_eq!(*ps, expected_reachable.next().unwrap());
            });
            reachable_reverse
                .map(|Tup2(x, y)| Tup2(*y, *x))
                .integrate()
                .stream_distinct()
                .inspect(move |ps: &OrdZSet<_>| {
                    assert_eq!(*ps, expected_reachable_reverse.next().unwrap());
                });

            Ok(())
        })
        .unwrap();

        for _ in 0..steps {
            root.transaction().unwrap();
        }
    }

    /// A bound larger than the number of iterations needed to converge must not
    /// change the result: it reproduces [`reachability_builder`] exactly.
    #[test]
    fn with_large_bound_is_noop() {
        let edges_data = edges_data();
        let steps = edges_data.len();
        let mut edges = edges_data.into_iter();
        let mut expected_reachable = expected_reachable().into_iter();

        let (mut handle, _) = Runtime::init_circuit(1, move |circuit| {
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let reachable = circuit
                .recursion(
                    |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
                    |child, reachable| {
                        let edges = edges.delta0(child);
                        let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                        let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));

                        Ok(edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        ))
                    },
                )
                .with_bound(NonZeroU64::new(1_000_000).unwrap())
                .finish()
                .unwrap();

            reachable
                .integrate()
                .stream_distinct()
                .inspect(move |reachable| {
                    assert_eq!(*reachable, expected_reachable.next().unwrap());
                });

            Ok(())
        })
        .unwrap();

        for _ in 0..steps {
            handle.transaction().unwrap();
        }
    }

    /// A recursion that provably never reaches a fixed point: every iteration
    /// shifts the single element up by one, so the recursive stream keeps
    /// changing.  Without a bound this would iterate forever; `with_bound`
    /// caps it at a fixed number of iterations.  The test completing at all
    /// proves the bound is enforced, and the union of the per-iteration values
    /// gives a deterministic result.
    #[test]
    fn with_bound_caps_non_converging() {
        const BOUND: NonZeroU64 = NonZeroU64::new(3).unwrap();

        let (mut handle, output) = Runtime::init_circuit(1, move |circuit| {
            let mut seed = [zset! { 0usize => 1 }].into_iter();
            let seed = circuit.add_source(Generator::new(move || seed.next().unwrap_or_default()));

            let result = circuit
                .recursion(
                    |child| Ok(child.recursive_var::<OrdZSet<usize>>()),
                    move |child, x| {
                        // `seed` is injected once (at iteration 0) via delta0;
                        // thereafter each iteration re-emits the previous value
                        // shifted up by one, so the stream never stabilizes.
                        let seed = seed.delta0(child);
                        Ok(seed.plus(&x.map(|v| *v + 1)))
                    },
                )
                .with_bound(BOUND)
                .finish()
                .unwrap();

            Ok(result.output())
        })
        .unwrap();

        handle.transaction().unwrap();

        // Three iterations emit 0, 1, 2 respectively.
        assert_eq!(
            output.consolidate(),
            zset! { 0usize => 1, 1usize => 1, 2usize => 1 },
        );
    }

    /// With reporting enabled, a bounded run over the non-converging recursion
    /// reports that it was truncated: `converged == false` and `iterations`
    /// equal to the bound.
    #[test]
    fn with_report_signals_truncation() {
        const BOUND: NonZeroU64 = NonZeroU64::new(3).unwrap();

        let (mut handle, outcome) = Runtime::init_circuit(1, move |circuit| {
            let mut seed = [zset! { 0usize => 1 }].into_iter();
            let seed = circuit.add_source(Generator::new(move || seed.next().unwrap_or_default()));

            let (result, outcome) = circuit
                .recursion(
                    |child| Ok(child.recursive_var::<OrdZSet<usize>>()),
                    move |child, x| {
                        let seed = seed.delta0(child);
                        Ok(seed.plus(&x.map(|v| *v + 1)))
                    },
                )
                .with_bound(BOUND)
                .with_report()
                .finish()
                .unwrap();

            // The output stream is unaffected by reporting; keep it alive.
            result.output();

            Ok(outcome.output())
        })
        .unwrap();

        handle.transaction().unwrap();

        let outcome = outcome.take_from_all();
        let outcome = outcome.first().expect("one worker, one outcome");
        assert!(
            outcome.truncated(),
            "a bounded non-converging recursion must report truncation"
        );
        assert_eq!(outcome.converged(), false);
        assert_eq!(outcome.converged_iterations(), None);
        assert_eq!(outcome.iterations(), BOUND.get());
    }

    /// With reporting enabled, a converging recursion reports convergence and an
    /// iteration count strictly below the (generous) bound, for every
    /// transaction.
    #[test]
    fn with_report_signals_convergence() {
        let edges_data = edges_data();
        let steps = edges_data.len();
        let mut edges = edges_data.into_iter();

        let (mut handle, outcome) = Runtime::init_circuit(1, move |circuit| {
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let (reachable, outcome) = circuit
                .recursion(
                    |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
                    |child, reachable| {
                        let edges = edges.delta0(child);
                        let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                        let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));

                        Ok(edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        ))
                    },
                )
                .with_bound(NonZeroU64::new(1_000).unwrap())
                .with_report()
                .finish()
                .unwrap();

            reachable.output();

            Ok(outcome.output())
        })
        .unwrap();

        for _ in 0..steps {
            handle.transaction().unwrap();

            let outcome = outcome.take_from_all();
            let outcome = outcome.first().expect("one worker, one outcome");
            let iterations = outcome
                .converged_iterations()
                .expect("reachability must converge within the bound");
            assert_eq!(outcome.converged(), true);
            assert_eq!(outcome.truncated(), false);
            assert_eq!(outcome.iterations(), iterations);
            assert!(
                (1..1_000).contains(&iterations),
                "unexpected iteration count: {iterations}"
            );
        }
    }

    /// Regression test for the per-epoch iteration counter: it must reset when
    /// each transaction's nested epoch ends.  A fresh seed on every transaction
    /// keeps the recursion non-converging, so every transaction runs exactly to
    /// the bound.  If the counter leaked across transactions, the bound would be
    /// exhausted after the first one and later transactions would stop after a
    /// single iteration (reporting a growing `iterations` instead of `BOUND`).
    #[test]
    fn with_bound_counter_resets_across_transactions() {
        const BOUND: NonZeroU64 = NonZeroU64::new(3).unwrap();
        const TRANSACTIONS: usize = 4;

        let (mut handle, (result, report)) = Runtime::init_circuit(1, move |circuit| {
            // A distinct seed element per transaction, so the shift recursion
            // always has work to do and never converges within the bound.
            let mut n = 0usize;
            let seed = circuit.add_source(Generator::new(move || {
                let batch = zset! { n => 1 };
                n += 1;
                batch
            }));

            let (result, report) = circuit
                .recursion(
                    |child| Ok(child.recursive_var::<OrdZSet<usize>>()),
                    move |child, x| {
                        let seed = seed.delta0(child);
                        Ok(seed.plus(&x.map(|v| *v + 1)))
                    },
                )
                .with_bound(BOUND)
                .without_distinct()
                .with_report()
                .finish()
                .unwrap();

            Ok((result.accumulate_output(), report.output()))
        })
        .unwrap();

        for transaction in 0..TRANSACTIONS {
            handle.transaction().unwrap();

            let report = report.take_from_all();
            let report = report.first().expect("one worker, one outcome");
            assert!(
                report.truncated(),
                "transaction {transaction}: non-converging recursion must report truncation",
            );
            assert_eq!(
                report.iterations(),
                BOUND.get(),
                "transaction {transaction}: counter must reset to report per-transaction iterations",
            );
            let result = result.concat().consolidate();
            assert_eq!(
                result,
                OrdZSet::from_keys(
                    (),
                    (transaction..(transaction + BOUND.get() as usize))
                        .map(|i| {
                            let zweight = 1;
                            Tup2(i, zweight)
                        })
                        .collect::<Vec<_>>()
                ),
                "transaction {transaction}: invalid computation result",
            );
        }
    }

    /// Without the implicit `distinct`, the recursive fixed point over Z-sets
    /// does not converge and the circuit iterates forever.  We therefore assert
    /// only that [`without_distinct`](RecursionBuilder::without_distinct)
    /// type-checks and builds; convergence is exercised by the tests above.
    #[test]
    fn without_distinct_builds() {
        let (_handle, _) = Runtime::init_circuit(1, move |circuit| {
            let (edges, edges_handle) = circuit.add_input_zset::<Edge>();

            // A step that stabilizes on its own (a plain map), so dropping the
            // `distinct` is safe: the output weights do not grow unboundedly.
            let closure = circuit
                .recursion(
                    |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
                    move |child, _reachable| {
                        let edges = edges.delta0(child);
                        Ok(edges.map(|&Tup2(x, y)| Tup2(x, y)))
                    },
                )
                .without_distinct()
                .finish()
                .unwrap();

            closure.output();

            Ok(edges_handle)
        })
        .unwrap();
    }
}
