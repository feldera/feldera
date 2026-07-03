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
        use crate::FallbackZSet;

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

        #[test]
        fn reachability() {
            let edges_data = edges_data();
            let steps = edges_data.len();
            let mut edges = edges_data.into_iter();
            let mut expected_reachable = expected_reachable().into_iter();

            let (mut handle, _) = Runtime::init_circuit(1, move |circuit| {
                let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

                let reachable = circuit
                    .recursive(|child, reachable: Stream<_, OrdZSet<Edge>>| {
                        let edges = edges.delta0(child);
                        let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

                        let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));

                        let reachable_next = edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );

                        Ok(reachable_next)
                    })
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

        /// A rewrite of [`reachability()`] using
        /// [`recursive_dynamic`](crate::ChildCircuit::recursive_dynamic):
        /// A single recursive relation supplied as a one-element vector
        /// (arity 1).  It must produce exactly the same output as the
        /// single-`Stream` implementation.
        #[test]
        fn reachability_dynamic() {
            let edges_data = edges_data();
            let steps = edges_data.len();
            let mut edges = edges_data.into_iter();
            let mut expected_reachable = expected_reachable().into_iter();

            let (mut handle, _) = Runtime::init_circuit(1, move |circuit| {
                let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

                let mut recursive_streams = circuit
                    .recursive_dynamic(
                        1,
                        |child, mut recursive_streams: Vec<Stream<_, OrdZSet<Edge>>>| {
                            let edges = edges.delta0(child);
                            let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

                            let reachable = &mut recursive_streams[0];
                            let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));

                            let reachable_next = edges.plus(
                                &reachable_indexed
                                    .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                            );

                            // We can even reuse the allocated vector and spare us a reallocation.
                            *reachable = reachable_next;
                            Ok(recursive_streams)
                        },
                    )
                    .unwrap();

                let reachable = recursive_streams.pop().unwrap();

                reachable.integrate().stream_distinct().inspect(move |ps| {
                    assert_eq!(*ps, expected_reachable.next().unwrap());
                });

                Ok(())
            })
            .unwrap();

            for _ in 0..steps {
                handle.transaction().unwrap();
            }
        }

        // Somewhat lame multiple recursion example to test RecursiveStreams impl for
        // tuples: compute forward and backward reachability at the same time.
        #[test]
        fn reachability2() {
            let edges_data = edges_data();
            let steps = edges_data.len();
            let mut edges = edges_data.into_iter();
            let expected_reachable = expected_reachable();
            let expected_reachable_reverse = expected_reachable.clone();
            let mut expected_reachable = expected_reachable.into_iter();
            let mut expected_reachable_reverse = expected_reachable_reverse.into_iter();

            let (mut root, _) = Runtime::init_circuit(1, move |circuit| {
                let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

                let (reachable, reachable_reverse) = circuit
                    .recursive(
                        |child,
                         (reachable, reachable_reverse): (
                            Stream<_, FallbackZSet<Edge>>,
                            Stream<_, FallbackZSet<Edge>>,
                        )| {
                            let edges = edges.delta0(child);

                            let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                            let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));
                            let reachable_reverse_indexed =
                                reachable_reverse.map_index(|&Tup2(x, y)| (y, x));
                            let reverse_edges = edges.map(|&Tup2(x, y)| Tup2(y, x));
                            let reverse_edges_indexed =
                                reverse_edges.map_index(|Tup2(x, y)| (*x, *y));

                            let reachable_next = edges.plus(
                                &reachable_indexed
                                    .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                            );
                            let reachable_reverse_next = reverse_edges.plus(
                                &reachable_reverse_indexed
                                    .join(&reverse_edges_indexed, |_via, from, to| {
                                        Tup2(*from, *to)
                                    }),
                            );

                            Ok((reachable_next, reachable_reverse_next))
                        },
                    )
                    .unwrap();

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

        /// A rewrite of [`reachability2()`] using
        /// [`recursive_dynamic`](crate::ChildCircuit::recursive_dynamic):
        /// Forward and backward reachability as two recursive relations
        /// supplied as a two-element vector (arity 2).  It must match the
        /// tuple implementation.
        #[test]
        fn reachability2_dynamic() {
            let edges_data = edges_data();
            let steps = edges_data.len();
            let mut edges = edges_data.into_iter();
            let expected_reachable = expected_reachable();
            let expected_reachable_reverse = expected_reachable.clone();
            let mut expected_reachable = expected_reachable.into_iter();
            let mut expected_reachable_reverse = expected_reachable_reverse.into_iter();

            let (mut root, _) = Runtime::init_circuit(1, move |circuit| {
                let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

                let mut recursive_streams = circuit
                    .recursive_dynamic(
                        2,
                        |child, mut recursive_streams: Vec<Stream<_, OrdZSet<Edge>>>| {
                            let edges = edges.delta0(child);

                            let (reachable, rest) = recursive_streams.split_first_mut().unwrap();
                            let reachable_reverse = rest.first_mut().unwrap();

                            let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                            let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));
                            let reachable_reverse_indexed =
                                reachable_reverse.map_index(|&Tup2(x, y)| (y, x));
                            let reverse_edges = edges.map(|&Tup2(x, y)| Tup2(y, x));
                            let reverse_edges_indexed =
                                reverse_edges.map_index(|Tup2(x, y)| (*x, *y));

                            let reachable_next = edges.plus(
                                &reachable_indexed
                                    .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                            );

                            let reachable_reverse_next = reverse_edges.plus(
                                &reachable_reverse_indexed
                                    .join(&reverse_edges_indexed, |_via, from, to| {
                                        Tup2(*from, *to)
                                    }),
                            );

                            // We can even reuse the allocated vector and spare us a reallocation.
                            *reachable = reachable_next;
                            *reachable_reverse = reachable_reverse_next;
                            Ok(recursive_streams)
                        },
                    )
                    .unwrap();

                let reachable_reverse = recursive_streams.pop().unwrap();
                let reachable = recursive_streams.pop().unwrap();

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
    }
}
