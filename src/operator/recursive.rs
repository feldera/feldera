//! Convenience API for defining recursive computations.

use crate::{
    algebra::{ZRingValue, ZSet},
    circuit::{schedule::Error as SchedulerError, Circuit, Stream},
    operator::DelayedFeedback,
    trace::spine_fueled::Spine,
};
use deepsize::DeepSizeOf;
use impl_trait_for_tuples::impl_for_tuples;
use std::{hash::Hash, result::Result};

/// Generalizes stream operators to groups of streams.
///
/// This is a helper trait for the
/// [`Circuit::recursive`](`crate::circuit::Circuit::recursive`) method.  The
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

    /// Create a group of recursive streams along with their feedback
    /// connectors.
    fn new(circuit: &C) -> (Self::Feedback, Self);

    /// Apply `distinct` to all streams in `self`.
    fn distinct(self) -> Self;

    /// Close feedback loop for all streams in `self`.
    fn connect(&self, vars: Self::Feedback);

    /// Export all streams in `self` to the parent circuit.
    fn export(self) -> Self::Export;

    /// Apply [`Stream::consolidate`] to all streams in `exports`.
    fn consolidate(exports: Self::Export) -> Self::Output;
}

// TODO: Generalize this impl to support arbitrarily nested circuits by
// fixing `distinct_trace` to work for any nesting depth.
// Additionally, if we generalize `distinct_trace` to work on arbitrary
// batches, we can ditch the `B::Val = ()` constraint.
impl<B> RecursiveStreams<Circuit<Circuit<()>>> for Stream<Circuit<Circuit<()>>, B>
where
    //P: Clone +
    // 'static,
    B: ZSet + DeepSizeOf + Send + 'static,
    B::Key: Clone + Ord + Hash + DeepSizeOf,
    B::R: DeepSizeOf + ZRingValue,
{
    type Feedback = DelayedFeedback<Circuit<()>, B>;
    type Export = Stream<Circuit<()>, Spine<B>>;
    type Output = Stream<Circuit<()>, B>;

    fn new(circuit: &Circuit<Circuit<()>>) -> (Self::Feedback, Self) {
        let feedback = DelayedFeedback::new(circuit);
        let stream = feedback.stream().clone();
        (feedback, stream)
    }

    fn distinct(self) -> Self {
        self.distinct_trace()
    }

    fn connect(&self, vars: Self::Feedback) {
        vars.connect(self)
    }

    fn export(self) -> Self::Export {
        self.integrate_trace().export()
    }

    fn consolidate(exports: Self::Export) -> Self::Output {
        Stream::consolidate(&exports)
    }
}

// TODO: `impl RecursiveStreams for Vec<Stream>`.

#[impl_for_tuples(2, 12)]
#[tuple_types_custom_trait_bound(Clone + RecursiveStreams<C>)]
impl<C> RecursiveStreams<C> for Tuple {
    for_tuples!( type Feedback = ( #( Tuple::Feedback ),* ); );
    for_tuples!( type Export = ( #( Tuple::Export ),* ); );
    for_tuples!( type Output = ( #( Tuple::Output ),* ); );

    fn new(circuit: &C) -> (Self::Feedback, Self) {
        let res = (for_tuples!( #( Tuple::new(circuit) ),* ));

        let streams = (for_tuples!( #( { let stream = &res.Tuple; stream.1.clone() } ),* ));
        let feedback = (for_tuples!( #( { let stream = res.Tuple; stream.0 } ),* ));

        (feedback, streams)
    }

    fn distinct(self) -> Self {
        (for_tuples!( #( self.Tuple.distinct() ),* ))
    }

    fn connect(&self, vars: Self::Feedback) {
        for_tuples!( #( self.Tuple.connect(vars.Tuple); )* );
    }

    fn export(self) -> Self::Export {
        (for_tuples!( #( self.Tuple.export() ),* ))
    }

    fn consolidate(exports: Self::Export) -> Self::Output {
        (for_tuples!( #( Tuple::consolidate(exports.Tuple) ),* ))
    }
}

impl<P: Clone + 'static> Circuit<P> {
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
    ///     circuit::{Root, Stream},
    ///     operator::Generator,
    ///     time::NestedTimestamp32,
    ///     trace::ord::OrdZSet,
    ///     zset, zset_set,
    /// };
    /// use std::vec;
    ///
    /// // Propagate labels along graph edges.
    /// let root = Root::build(move |circuit| {
    ///     // Graph topology.
    ///     let mut edges = vec![
    ///         // Start with four nodes connected in a cycle.
    ///         zset_set! { (1, 2), (2, 3), (3, 4), (4, 1) },
    ///         // Add an edge.
    ///         zset_set! { (4, 5) },
    ///         // Remove an edge, breaking the cycle.
    ///         zset! { (1, 2) => -1 },
    ///     ]
    ///     .into_iter();
    ///
    ///     let edges = circuit
    ///             .add_source(Generator::new(move || edges.next().unwrap()));
    ///
    ///     // Initial labeling of the graph.
    ///     let mut init_labels = vec![
    ///         // Start with a single label on node 1.
    ///         zset_set! { (1, "l1") },
    ///         // Add a label to node 2.
    ///         zset_set! { (2, "l2") },
    ///         zset! { },
    ///     ]
    ///     .into_iter();
    ///
    ///     let init_labels = circuit
    ///             .add_source(Generator::new(move || init_labels.next().unwrap()));
    ///
    ///     // Expected _changes_ to the output graph labeling after each clock cycle.
    ///     let mut expected_outputs = vec![
    ///         zset! { (1, "l1") => 1, (2, "l1") => 1, (3, "l1") => 1, (4, "l1") => 1 },
    ///         zset! { (1, "l2") => 1, (2, "l2") => 1, (3, "l2") => 1, (4, "l2") => 1, (5, "l1") => 1, (5, "l2") => 1 },
    ///         zset! { (2, "l1") => -1, (3, "l1") => -1, (4, "l1") => -1, (5, "l1") => -1 },
    ///     ]
    ///     .into_iter();
    ///
    ///     let labels = circuit.recursive(|child, labels: Stream<_, OrdZSet<(usize, &'static str), isize>>| {
    ///         // Import `edges` and `init_labels` relations from the parent circuit.
    ///         let edges = edges.delta0(child);
    ///         let init_labels = init_labels.delta0(child);
    ///
    ///         // Given an edge `from -> to` where the `from` node is labeled with `l`,
    ///         // propagate `l` to node `to`.
    ///         let result = labels.index()
    ///               .join::<NestedTimestamp32, _, _, _>(
    ///                   &edges.index(),
    ///                   |_from, l, to| (*to, *l),
    ///               )
    ///               .plus(&init_labels);
    ///         Ok(result)
    ///     })
    ///     .unwrap();
    ///
    ///     labels.inspect(move |ls| {
    ///         assert_eq!(*ls, expected_outputs.next().unwrap());
    ///     });
    /// })
    /// .unwrap();
    ///
    /// for _ in 0..3 {
    ///     root.step().unwrap();
    /// }
    /// ```
    pub fn recursive<F, S>(&self, f: F) -> Result<S::Output, SchedulerError>
    where
        S: RecursiveStreams<Circuit<Self>>,
        F: FnOnce(&Circuit<Self>, S) -> Result<S, SchedulerError>,
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
            let (vars, input_streams) = S::new(child);
            let output_streams = f(child, input_streams)?;
            let output_streams = S::distinct(output_streams);
            S::connect(&output_streams, vars);
            Ok(S::export(output_streams))
        })?;

        Ok(S::consolidate(traces))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        circuit::{Root, Stream},
        operator::{FilterMap, Generator},
        time::NestedTimestamp32,
        trace::ord::OrdZSet,
        zset,
    };
    use std::vec;

    #[test]
    fn reachability() {
        let root = Root::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges = vec![
                zset! { (1, 2) => 1 },
                zset! { (2, 3) => 1},
                zset! { (1, 3) => 1},
                zset! { (3, 1) => 1},
                zset! { (3, 1) => -1},
                zset! { (1, 2) => -1},
                zset! { (2, 4) => 1, (4, 1) => 1 },
                zset! { (2, 3) => -1, (3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let mut outputs = vec![
                zset! { (1, 2) => 1 },
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (1, 2) => 1, (1, 3) => 1, (2, 3) => 1, (2, 1) => 1, (3, 1) => 1, (3, 2) => 1},
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 3) => 1, (2, 3) => 1, (2, 4) => 1, (2, 1) => 1, (4, 1) => 1, (4, 3) => 1 },
                zset! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (4, 4) => 1,
                        (1, 2) => 1, (1, 3) => 1, (1, 4) => 1,
                        (2, 1) => 1, (2, 3) => 1, (2, 4) => 1,
                        (3, 1) => 1, (3, 2) => 1, (3, 4) => 1,
                        (4, 1) => 1, (4, 2) => 1, (4, 3) => 1 },
            ]
            .into_iter();

            let edges = circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let paths = circuit.recursive(|child, paths: Stream<_, OrdZSet<(usize, usize), isize>>| {
                let edges = edges.delta0(child);

                let paths_indexed = paths.index_with(|&(x, y)| (y, x));
                let edges_indexed = edges.index();

                Ok(edges.plus(&paths_indexed.join::<NestedTimestamp32, _, _, _>(&edges_indexed, |_via, from, to| (*from, *to))))
            })
            .unwrap();

            paths.integrate().distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            });
        })
        .unwrap();

        for _ in 0..8 {
            root.step().unwrap();
        }
    }

    // Somewhat lame multiple recursion example to test RecursiveStreams impl for
    // tuples: compute forward and backward reachability at the same time.
    #[test]
    fn reachability2() {
        type Edges<S> = Stream<S, OrdZSet<(usize, usize), isize>>;

        let root = Root::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges = vec![
                zset! { (1, 2) => 1 },
                zset! { (2, 3) => 1},
                zset! { (1, 3) => 1},
                zset! { (3, 1) => 1},
                zset! { (3, 1) => -1},
                zset! { (1, 2) => -1},
                zset! { (2, 4) => 1, (4, 1) => 1 },
                zset! { (2, 3) => -1, (3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let output_vec = vec![
                zset! { (1, 2) => 1 },
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (1, 2) => 1, (1, 3) => 1, (2, 3) => 1, (2, 1) => 1, (3, 1) => 1, (3, 2) => 1},
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 3) => 1, (2, 3) => 1, (2, 4) => 1, (2, 1) => 1, (4, 1) => 1, (4, 3) => 1 },
                zset! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (4, 4) => 1,
                              (1, 2) => 1, (1, 3) => 1, (1, 4) => 1,
                              (2, 1) => 1, (2, 3) => 1, (2, 4) => 1,
                              (3, 1) => 1, (3, 2) => 1, (3, 4) => 1,
                              (4, 1) => 1, (4, 2) => 1, (4, 3) => 1 },
            ];

            let mut outputs = output_vec.clone().into_iter();
            let mut outputs2 = output_vec.into_iter();

            let edges = circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let (paths, reverse_paths) = circuit.recursive(|child, (paths, reverse_paths): (Edges<_>, Edges<_>)| {
                let edges = edges.delta0(child);

                let paths_indexed = paths.index_with(|&(x, y)| (y, x));
                let reverse_paths_indexed = reverse_paths.index_with(|&(x, y)| (y, x));
                let edges_indexed = edges.index();
                let reverse_edges = edges.map(|&(x, y)| (y, x));
                let reverse_edges_indexed = reverse_edges.index();

                Ok((edges.plus(&paths_indexed.join::<NestedTimestamp32, _, _, _>(&edges_indexed, |_via, from, to| (*from, *to))),
                    reverse_edges.plus(&reverse_paths_indexed.join::<NestedTimestamp32, _, _, _>(&reverse_edges_indexed, |_via, from, to| (*from, *to)))
                ))
            })
            .unwrap();

            paths.integrate().distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            });

            reverse_paths.map(|(x, y)| (*y, *x)).integrate().distinct().inspect(move |ps: &OrdZSet<_,_>| {
                assert_eq!(*ps, outputs2.next().unwrap());
            });
        })
        .unwrap();

        for _ in 0..8 {
            root.step().unwrap();
        }
    }
}
