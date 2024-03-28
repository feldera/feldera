use crate::{
    circuit::WithClock,
    dynamic::Erase,
    operator::dynamic::{
        distinct::DistinctFactories, recursive::RecursiveStreams as DynRecursiveStreams,
    },
    typed_batch::{DynIndexedZSet, TypedBatch},
    ChildCircuit, Circuit, DBData, SchedulerError, Stream, ZWeight,
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
    B: DynIndexedZSet + Send,
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

#[impl_for_tuples(2, 12)]
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

impl<P> ChildCircuit<P>
where
    P: WithClock,
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
    ///     time::NestedTimestamp32,
    ///     OrdZSet,
    ///     Circuit, RootCircuit, Stream, zset, zset_set,
    ///     utils::Tup2,
    /// };
    /// use std::vec;
    ///
    /// // Propagate labels along graph edges.
    /// let root = RootCircuit::build(move |circuit| {
    ///     // Graph topology.
    ///     let mut edges = vec![
    ///         // Start with four nodes connected in a cycle.
    ///         zset_set! { Tup2(1, 2), Tup2(2, 3), Tup2(3, 4), Tup2(4, 1) },
    ///         // Add an edge.
    ///         zset_set! { Tup2(4, 5) },
    ///         // Remove an edge, breaking the cycle.
    ///         zset! { Tup2(1, 2) => -1 },
    ///     ]
    ///     .into_iter();
    ///
    ///     let edges = circuit
    ///             .add_source(Generator::new(move || edges.next().unwrap()));
    ///
    ///     // Initial labeling of the graph.
    ///     let mut init_labels = vec![
    ///         // Start with a single label on node 1.
    ///         zset_set! { Tup2(1, "l1".to_string()) },
    ///         // Add a label to node 2.
    ///         zset_set! { Tup2(2, "l2".to_string()) },
    ///         zset! { },
    ///     ]
    ///     .into_iter();
    ///
    ///     let init_labels = circuit
    ///             .add_source(Generator::new(move || init_labels.next().unwrap()));
    ///
    ///     // Expected _changes_ to the output graph labeling after each clock cycle.
    ///     let mut expected_outputs = vec![
    ///         zset! { Tup2(1, "l1".to_string()) => 1, Tup2(2, "l1".to_string()) => 1, Tup2(3, "l1".to_string()) => 1, Tup2(4, "l1".to_string()) => 1 },
    ///         zset! { Tup2(1, "l2".to_string()) => 1, Tup2(2, "l2".to_string()) => 1, Tup2(3, "l2".to_string()) => 1, Tup2(4, "l2".to_string()) => 1, Tup2(5, "l1".to_string()) => 1, Tup2(5, "l2".to_string()) => 1 },
    ///         zset! { Tup2(2, "l1".to_string()) => -1, Tup2(3, "l1".to_string()) => -1, Tup2(4, "l1".to_string()) => -1, Tup2(5, "l1".to_string()) => -1 },
    ///     ]
    ///     .into_iter();
    ///
    ///     let labels = circuit.recursive(|child, labels: Stream<_, OrdZSet<Tup2<u64, String>>>| {
    ///         // Import `edges` and `init_labels` relations from the parent circuit.
    ///         let edges = edges.delta0(child);
    ///         let init_labels = init_labels.delta0(child);
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
    ///     })
    ///     .unwrap();
    ///
    ///     labels.inspect(move |ls| {
    ///         assert_eq!(*ls, expected_outputs.next().unwrap());
    ///     });
    ///     Ok(())
    /// })
    /// .unwrap().0;
    ///
    /// for _ in 0..3 {
    ///     root.step().unwrap();
    /// }
    /// ```
    pub fn recursive<F, S>(&self, f: F) -> Result<S::Output, SchedulerError>
    where
        S: RecursiveStreams<ChildCircuit<Self>>,
        F: FnOnce(&ChildCircuit<Self>, S) -> Result<S, SchedulerError>,
    {
        self.dyn_recursive(&S::factories(), |circuit, streams: S::Inner| {
            f(circuit, unsafe { S::typed(&streams) }).map(|streams| streams.inner())
        })
        .map(|streams| unsafe { S::typed_exports(&streams) })
    }
}
