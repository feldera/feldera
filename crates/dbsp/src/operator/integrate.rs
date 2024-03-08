//! Integration operators.

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero},
    circuit::{Circuit, GlobalNodeId, OwnershipPreference, Stream},
    circuit_cache_key,
    operator::{
        differentiate::DifferentiateId,
        z1::{DelayedFeedback, DelayedNestedFeedback},
        Plus,
    },
    NumEntries,
};
use size_of::SizeOf;
use std::ops::Add;

circuit_cache_key!(IntegralId<C, D>(GlobalNodeId => Stream<C, D>));
circuit_cache_key!(NestedIntegralId<C, D>(GlobalNodeId => Stream<C, D>));

impl<C, D> Stream<C, D>
where
    C: Circuit,
    D: Add<Output = D>
        + AddByRef
        + AddAssignByRef
        + Clone
        + Eq
        + HasZero
        + SizeOf
        + NumEntries
        + 'static,
{
    /// Integrate the input stream.
    ///
    /// Computes the sum of values in the input stream.
    /// The first output value is the first input value, the second output
    /// value is the sum of the first two inputs, and so on.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbsp::{
    /// #     operator::Generator,
    /// #     Circuit, RootCircuit,
    /// # };
    /// let circuit = RootCircuit::build(move |circuit| {
    ///     // Generate a stream of 1's.
    ///     let stream = circuit.add_source(Generator::new(|| 1));
    ///     stream.inspect(move |n| eprintln!("{n}"));
    ///     // Integrate the stream.
    ///     let integral = stream.integrate();
    ///     integral.inspect(move |n| eprintln!("{n}"));
    ///     let mut counter1 = 0;
    ///     eprintln!("{counter1}");
    ///     integral.inspect(move |n| {
    ///         counter1 += 1;
    ///         assert_eq!(*n, counter1)
    ///     });
    ///     let mut counter2 = 0;
    ///     integral.delay().inspect(move |n| {
    ///         assert_eq!(*n, counter2);
    ///         counter2 += 1;
    ///     });
    ///     Ok(())
    /// })
    /// .unwrap()
    /// .0;
    ///
    /// for _ in 0..5 {
    ///     circuit.step().unwrap();
    /// }
    /// ```
    ///
    /// The above example generates the following input/output mapping:
    ///
    /// ```text
    /// input:  1, 1, 1, 1, 1, ...
    /// output: 1, 2, 3, 4, 5, ...
    /// ```
    pub fn integrate(&self) -> Stream<C, D> {
        self.circuit()
            .cache_get_or_insert_with(IntegralId::new(self.origin_node_id().clone()), || {
                // Integration circuit:
                // ```
                //              input
                //   ┌─────────────────►
                //   │
                //   │    ┌───┐ current
                // ──┴───►│   ├────────►
                //        │ + │
                //   ┌───►│   ├────┐
                //   │    └───┘    │
                //   │             │
                //   │    ┌───┐    │
                //   │    │   │    │
                //   └────┤z-1├────┘
                //        │   │
                //        └───┴────────►
                //              delayed
                //              export
                // ```
                self.circuit().region("integrate", || {
                    let feedback = DelayedFeedback::new(self.circuit());
                    let integral = self.circuit().add_binary_operator_with_preference(
                        <Plus<D>>::new(),
                        (
                            feedback.stream(),
                            OwnershipPreference::STRONGLY_PREFER_OWNED,
                        ),
                        (self, OwnershipPreference::PREFER_OWNED),
                    );
                    feedback.connect(&integral);

                    self.circuit().cache_insert(
                        DifferentiateId::new(integral.origin_node_id().clone()),
                        self.clone(),
                    );
                    integral
                })
            })
            .clone()
    }

    /// Integrate stream of streams.
    ///
    /// Computes the sum of nested streams, i.e., rather than integrating values
    /// in each nested stream, this function sums up entire input streams
    /// across all parent timestamps, where the sum of streams is defined as
    /// a stream of point-wise sums of their elements: `integral[i,j] =
    /// sum(input[k,j]), k<=i`, where `stream[i,j]` is the value of `stream`
    /// at time `[i,j]`, `i` is the parent timestamp, and `j` is the child
    /// timestamp.
    ///
    /// Yields the sum element-by-element as the input stream is fed to the
    /// integral.
    ///
    /// # Examples
    ///
    /// Input stream (one row per parent timestamps):
    ///
    /// ```text
    /// 1 2 3 4
    /// 1 1 1 1 1
    /// 2 2 2 0 0
    /// ```
    ///
    /// Integral:
    ///
    /// ```text
    /// 1 2 3 4
    /// 2 3 4 5 1
    /// 4 5 6 5 1
    /// ```
    pub fn integrate_nested(&self) -> Stream<C, D> {
        self.circuit()
            .cache_get_or_insert_with(NestedIntegralId::new(self.origin_node_id().clone()), || {
                self.circuit().region("integrate_nested", || {
                    let feedback = DelayedNestedFeedback::new(self.circuit());
                    let integral = self.circuit().add_binary_operator_with_preference(
                        Plus::new(),
                        (
                            feedback.stream(),
                            OwnershipPreference::STRONGLY_PREFER_OWNED,
                        ),
                        (self, OwnershipPreference::PREFER_OWNED),
                    );
                    feedback.connect(&integral);
                    integral
                })
            })
            .clone()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::HasZero,
        monitor::TraceMonitor,
        operator::{DelayedFeedback, Generator},
        typed_batch::OrdZSet,
        utils::Tup2,
        zset, Circuit, RootCircuit, ZWeight,
    };

    #[test]
    fn scalar_integrate() {
        let circuit = RootCircuit::build(move |circuit| {
            let source = circuit.add_source(Generator::new(|| 1));
            let mut counter = 0;
            source.integrate().inspect(move |n| {
                counter += 1;
                assert_eq!(*n, counter);
            });
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn zset_integrate() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut counter1: u64 = 0;
            let mut s = <OrdZSet<u64>>::zero();
            let source = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s = s.merge(&zset! { counter1 => 1});
                counter1 += 1;
                res
            }));

            let integral = source.integrate();
            let mut counter2 = 0;
            integral.inspect(move |s| {
                let mut batch = Vec::with_capacity(counter2);
                for i in 0..counter2 {
                    batch.push(Tup2(Tup2(i as u64, ()), (counter2 - i) as ZWeight));
                }
                assert_eq!(s, &<OrdZSet<_>>::from_tuples((), batch));
                counter2 += 1;
            });
            let mut counter3 = 0;
            integral.delay().inspect(move |s| {
                let mut batch = Vec::with_capacity(counter2);
                for i in 1..counter3 {
                    batch.push(Tup2(Tup2((i - 1) as u64, ()), (counter3 - i) as ZWeight));
                }
                assert_eq!(s, &<OrdZSet<_>>::from_tuples((), batch));
                counter3 += 1;
            });
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }
    }

    /// ```text
    ///            ┌───────────────────────────────────────────────────────────────────────────────────┐
    ///            │                                                                                   │
    ///            │                           3,2,1,0,0,0                     3,2,1,0,                │
    ///            │                           4,3,2,1,0,0                     7,5,3,1,0,              │
    ///            │                    ┌───┐  2,1,0,0,0,0                     9,6,3,1,0,              │
    ///  3,4,2,5   │                    │   │  5,4,3,2,1,0                     14,10,6,3,1,0           │ 6,16,19,34
    /// ───────────┼──►delta0──────────►│ + ├──────────┬─────►integrate_nested───────────────integrate─┼─────────────►
    ///            │          3,0,0,0,0 │   │          │                                               │
    ///            │          4,0,0,0,0 └───┘          ▼                                               │
    ///            │          2,0,0,0,0   ▲    ┌──┐   ┌───┐                                            │
    ///            │          5,0,0,0,0   └────┤-1│◄──|z-1|                                            │
    ///            │                           └──┘   └───┘                                            │
    ///            │                                                                                   │
    ///            └───────────────────────────────────────────────────────────────────────────────────┘
    /// ```
    #[test]
    fn scalar_integrate_nested() {
        let circuit = RootCircuit::build(move |circuit| {
            TraceMonitor::new_panic_on_error().attach(circuit, "monitor");

            let mut input = vec![3, 4, 2, 5].into_iter();

            let mut expected_counters =
                vec![3, 2, 1, 0, 4, 3, 2, 1, 0, 2, 1, 0, 0, 0, 5, 4, 3, 2, 1, 0].into_iter();
            let mut expected_integrals =
                vec![3, 2, 1, 0, 7, 5, 3, 1, 0, 9, 6, 3, 1, 0, 14, 10, 6, 3, 1, 0].into_iter();
            let mut expected_outer_integrals = vec![6, 16, 19, 34].into_iter();

            let source = circuit.add_source(Generator::new(move || input.next().unwrap()));
            let integral = circuit
                .iterate_with_condition(|child| {
                    let source = source.delta0(child);
                    let feedback = DelayedFeedback::new(child);
                    let plus =
                        source.plus(&feedback.stream().apply(|&n| if n > 0 { n - 1 } else { n }));
                    plus.inspect(move |n| assert_eq!(*n, expected_counters.next().unwrap()));
                    feedback.connect(&plus);
                    let integral = plus.integrate_nested();
                    integral.inspect(move |n| assert_eq!(*n, expected_integrals.next().unwrap()));
                    Ok((
                        integral.condition(|n| *n == 0),
                        integral.apply(|rc| *rc).integrate().export(),
                    ))
                })
                .unwrap();
            integral.inspect(move |n| assert_eq!(*n, expected_outer_integrals.next().unwrap()));
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..4 {
            circuit.step().unwrap();
        }
    }
}
