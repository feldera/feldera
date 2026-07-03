use crate::Timestamp;
use crate::circuit::checkpointer::Checkpoint;
use crate::circuit::circuit_builder::IterativeCircuit;
use crate::{
    ChildCircuit, Circuit, DBData, SchedulerError, Stream, ZWeight,
    dynamic::Erase,
    operator::dynamic::{
        distinct::DistinctFactories, recursive::RecursiveStreams as DynRecursiveStreams,
    },
    typed_batch::{DynIndexedZSet, TypedBatch},
};
use impl_trait_for_tuples::impl_for_tuples;

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
}
