//! Convenience API for defining recursive computations.

use crate::{
    algebra::IndexedZSet,
    circuit::{schedule::Error as SchedulerError, ChildCircuit, Circuit, Stream, WithClock},
    operator::{dynamic::distinct::DistinctFactories, DelayedFeedback},
    trace::{Spillable, Spine},
};

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
    B: IndexedZSet + Spillable + Send,
    Spine<B>: SizeOf,
{
    type Feedback = DelayedFeedback<C, B>;
    type Export = Stream<C::Parent, Spine<B::Spilled>>;
    type Output = Stream<C::Parent, B::Spilled>;
    type Factories = DistinctFactories<B, C::Time>;

    fn new(circuit: &C, factories: &Self::Factories) -> (Self::Feedback, Self) {
        let feedback =
            DelayedFeedback::with_default(circuit, B::dyn_empty(&factories.input_factories, ()));
        let stream = feedback.stream().clone();
        (feedback, stream)
    }

    fn distinct(self, factories: &Self::Factories) -> Self {
        Stream::dyn_distinct(&self, factories)
    }

    fn connect(&self, vars: Self::Feedback) {
        vars.connect(self)
    }

    fn export(self, factories: &Self::Factories) -> Self::Export {
        Stream::export(
            &self
                .dyn_spill(&factories.stored_factories)
                .dyn_integrate_trace(&factories.stored_factories),
        )
    }

    fn consolidate(exports: Self::Export, factories: &Self::Factories) -> Self::Output {
        Stream::dyn_consolidate(&exports, &factories.stored_factories)
    }
}

// TODO: `impl RecursiveStreams for Vec<Stream>`.

#[impl_for_tuples(2, 12)]
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
impl<P> ChildCircuit<P>
where
    P: WithClock,
    Self: Circuit,
{
    /// See [`ChildCircuit::recursive`].
    pub fn dyn_recursive<F, S>(&self, factories: &S::Factories, f: F) -> Result<S::Output, SchedulerError>
    where
        S: RecursiveStreams<ChildCircuit<Self>>,
        F: FnOnce(&ChildCircuit<Self>, S) -> Result<S, SchedulerError>,
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
        operator::Generator, typed_batch::FileZSet, typed_batch::OrdZSet, utils::Tup2, zset,
        Circuit, RootCircuit, Stream,
    };
    use std::vec;

    #[test]
    fn reachability() {
        let root = RootCircuit::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges = vec![
                zset! { Tup2(1, 2) => 1 },
                zset! { Tup2(2, 3) => 1},
                zset! { Tup2(1, 3) => 1},
                zset! { Tup2(3, 1) => 1},
                zset! { Tup2(3, 1) => -1},
                zset! { Tup2(1, 2) => -1},
                zset! { Tup2(2, 4) => 1, Tup2(4, 1) => 1 },
                zset! { Tup2(2, 3) => -1, Tup2(3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let mut outputs = vec![
                zset! { Tup2(1, 2) => 1 },
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 1) => 1, Tup2(2, 2) => 1, Tup2(3, 3) => 1, Tup2(1, 2) => 1, Tup2(1, 3) => 1, Tup2(2, 3) => 1, Tup2(2, 1) => 1, Tup2(3, 1) => 1, Tup2(3, 2) => 1},
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 3) => 1, Tup2(2, 3) => 1, Tup2(2, 4) => 1, Tup2(2, 1) => 1, Tup2(4, 1) => 1, Tup2(4, 3) => 1 },
                zset! { Tup2(1, 1) => 1, Tup2(2, 2) => 1, Tup2(3, 3) => 1, Tup2(4, 4) => 1,
                        Tup2(1, 2) => 1, Tup2(1, 3) => 1, Tup2(1, 4) => 1,
                        Tup2(2, 1) => 1, Tup2(2, 3) => 1, Tup2(2, 4) => 1,
                        Tup2(3, 1) => 1, Tup2(3, 2) => 1, Tup2(3, 4) => 1,
                        Tup2(4, 1) => 1, Tup2(4, 2) => 1, Tup2(4, 3) => 1 },
            ]
            .into_iter();

            let edges = circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let paths = circuit.recursive(|child, paths: Stream<_, OrdZSet<Tup2<u64, u64>>>| {
                let edges = edges.delta0(child);

                let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
                let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

                Ok(edges.plus(&paths_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to))))
            })
            .unwrap();

            paths.integrate().unspill().stream_distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            });
            Ok(())
        })
        .unwrap().0;

        for _ in 0..8 {
            root.step().unwrap();
        }
    }

    // Somewhat lame multiple recursion example to test RecursiveStreams impl for
    // tuples: compute forward and backward reachability at the same time.
    #[test]
    fn reachability2() {
        type Edges<S> = Stream<S, OrdZSet<Tup2<u64, u64>>>;

        let root = RootCircuit::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges = vec![
                zset! { Tup2(1, 2) => 1 },
                zset! { Tup2(2, 3) => 1},
                zset! { Tup2(1, 3) => 1},
                zset! { Tup2(3, 1) => 1},
                zset! { Tup2(3, 1) => -1},
                zset! { Tup2(1, 2) => -1},
                zset! { Tup2(2, 4) => 1, Tup2(4, 1) => 1 },
                zset! { Tup2(2, 3) => -1, Tup2(3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let output_vec = vec![
                zset! { Tup2(1, 2) => 1 },
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 1) => 1, Tup2(2, 2) => 1, Tup2(3, 3) => 1, Tup2(1, 2) => 1, Tup2(1, 3) => 1, Tup2(2, 3) => 1, Tup2(2, 1) => 1, Tup2(3, 1) => 1, Tup2(3, 2) => 1},
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 3) => 1, Tup2(2, 3) => 1, Tup2(2, 4) => 1, Tup2(2, 1) => 1, Tup2(4, 1) => 1, Tup2(4, 3) => 1 },
                zset! { Tup2(1, 1) => 1, Tup2(2, 2) => 1, Tup2(3, 3) => 1, Tup2(4, 4) => 1,
                              Tup2(1, 2) => 1, Tup2(1, 3) => 1, Tup2(1, 4) => 1,
                              Tup2(2, 1) => 1, Tup2(2, 3) => 1, Tup2(2, 4) => 1,
                              Tup2(3, 1) => 1, Tup2(3, 2) => 1, Tup2(3, 4) => 1,
                              Tup2(4, 1) => 1, Tup2(4, 2) => 1, Tup2(4, 3) => 1 },
            ];

            let mut outputs = output_vec.clone().into_iter();
            let mut outputs2 = output_vec.into_iter();

            let edges = circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let (paths, reverse_paths):  (Stream<_, FileZSet<Tup2<u64, u64>>>, Stream<_, FileZSet<Tup2<u64, u64>>>) =
                circuit.recursive(|child, (paths, reverse_paths): (Edges<_>, Edges<_>)| {
                let edges = edges.delta0(child);

                let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
                let reverse_paths_indexed = reverse_paths.map_index(|&Tup2(x, y)| (y, x));
                let edges_indexed = edges.map_index(|Tup2(x,y)| (*x, *y));
                let reverse_edges = edges.map(|&Tup2(x, y)| Tup2(y, x));
                let reverse_edges_indexed = reverse_edges.map_index(|Tup2(x,y)| (*x, *y));

                Ok((edges.plus(&paths_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to))),
                    reverse_edges.plus(&reverse_paths_indexed.join(&reverse_edges_indexed, |_via, from, to| Tup2(*from, *to)))
                ))
            })
            .unwrap();

            paths.unspill().integrate().stream_distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            });

            reverse_paths.map(|Tup2(x, y)| Tup2(*y, *x)).integrate().stream_distinct().inspect(move |ps: &OrdZSet<_>| {
                assert_eq!(*ps, outputs2.next().unwrap());
            });
            Ok(())
        })
        .unwrap().0;

        for _ in 0..8 {
            root.step().unwrap();
        }
    }
}
