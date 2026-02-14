//! Integration operators.

use crate::circuit::checkpointer::Checkpoint;
use crate::circuit::circuit_builder::StreamId;
use crate::dynamic::Erase;
use crate::typed_batch::TypedBatch;
use crate::{ChildCircuit, DBData, DBWeight, Timestamp};
use crate::{
    NumEntries,
    algebra::{AddAssignByRef, AddByRef, HasZero, IndexedZSet as DynIndexedZSet},
    circuit::{Circuit, OwnershipPreference, Stream},
    circuit_cache_key,
    operator::{
        Plus,
        differentiate::DifferentiateId,
        z1::DelayedFeedback,
    },
};
use size_of::SizeOf;

circuit_cache_key!(IntegralId<C, D>(StreamId => Stream<C, D>));

impl<C, D> Stream<C, D>
where
    C: Circuit,
    D: Checkpoint
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
    ///     circuit.transaction().unwrap();
    /// }
    /// ```
    ///
    /// The above example generates the following input/output mapping:
    ///
    /// ```text
    /// input:  1, 1, 1, 1, 1, ...
    /// output: 1, 2, 3, 4, 5, ...
    /// ```
    #[track_caller]
    pub fn integrate(&self) -> Stream<C, D> {
        self.circuit()
            .cache_get_or_insert_with(IntegralId::new(self.stream_id()), || {
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

                    self.circuit()
                        .cache_insert(DifferentiateId::new(integral.stream_id()), self.clone());
                    integral
                })
            })
            .clone()
    }
}

impl<C, T, K, V, R, B> Stream<ChildCircuit<C, T>, TypedBatch<K, V, R, B>>
where
    C: Clone + 'static,
    T: Timestamp,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
    B: DynIndexedZSet + Checkpoint,
{
    /// Integrate the input stream, updating the output once per clock tick.
    pub fn accumulate_integrate(&self) -> Stream<ChildCircuit<C, T>, TypedBatch<K, V, R, B>> {
        self.circuit()
            .non_incremental(self, |_child_circuit, stream| Ok(stream.integrate()))
            .unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Circuit, RootCircuit, ZWeight,
        algebra::HasZero,
        operator::Generator,
        typed_batch::OrdZSet,
        utils::Tup2,
        zset,
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
            circuit.transaction().unwrap();
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
            circuit.transaction().unwrap();
        }
    }
}
