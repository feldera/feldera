//! Integral operator that panics when any record's accumulated weight
//! becomes negative.

use crate::{
    NumEntries, ZWeight,
    algebra::{AddAssignByRef, AddByRef, HasZero},
    circuit::{Circuit, Stream, checkpointer::Checkpoint},
    dynamic::Erase,
    trace::{BatchReader as TraceBatchReader, Cursor as TraceCursor},
    typed_batch::{Batch, IndexedZSetReader},
};
use size_of::SizeOf;

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Checkpoint
        + AddByRef
        + AddAssignByRef
        + Clone
        + Eq
        + HasZero
        + SizeOf
        + NumEntries
        + IndexedZSetReader
        + Batch<Time = ()>
        + 'static,
    B::InnerBatch: Send,
{
    /// Integrates the stream, panicking if any record's weight in the integral
    /// becomes negative.
    ///
    /// Maintains a running integral of the input stream.
    /// After every clock tick, every key that appears in the
    /// current delta is checked: if any `(key, value, weight)` triple in the
    /// integral has a non-positive weight, the operator panics, displaying the
    /// offending key, value, weight and a custom message.
    ///
    /// This operator is intended for debugging and testing: it verifies that a
    /// stream behaves like a valid set or multiset where every element has a
    /// positive multiplicity.
    ///
    /// The operator passes the input delta through unchanged as its output.
    ///
    /// # Panics
    ///
    /// Panics when any weight in the integral is ≤ 0.
    #[track_caller]
    pub fn integral_weight_validator(&self, message: &str) -> Self {
        let sharded = self.shard();
        let integral = sharded.integrate();
        let message = message.to_owned();
        // Only examine keys that appear in the current delta — those are the
        // only keys whose integral weight could have decreased this step.
        sharded.apply2(&integral, move |delta, integrated| {
            let inner = integrated.inner();
            let mut cursor = TraceBatchReader::cursor(inner);
            for (key, _val, _w) in delta.iter() {
                if cursor.seek_key_exact(key.erase(), None) {
                    while cursor.val_valid() {
                        let weight: ZWeight = **cursor.weight();
                        let val = cursor.val();
                        assert!(
                            weight >= 0,
                            "{message}: negative weight found {weight} for key {key:?}, val {val:?}"
                        );
                        cursor.step_val();
                    }
                }
            }
        });
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Circuit, RootCircuit, ZWeight, operator::Generator, typed_batch::OrdZSet, utils::Tup2, zset,
    };

    /// A stream of purely positive deltas should pass the check at every step.
    #[test]
    fn integral_weight_validator_positive_weights() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut step = 0u64;
            let source = circuit.add_source(Generator::new(move || {
                let batch = zset! { step => 1 };
                step += 1;
                batch
            }));
            let out = source.integral_weight_validator("test_table");
            let mut expected_step = 0u64;
            out.inspect(move |delta| {
                // The output is the original delta, not the integral.
                assert_eq!(delta, &zset! { expected_step => 1 });
                expected_step += 1;
            });
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..10 {
            circuit.transaction().unwrap();
        }
    }

    /// Inserting an element once and then deleting it twice causes the integral
    /// weight to go negative, which must trigger a panic.
    #[test]
    #[should_panic(expected = "table 'test': negative weight found")]
    fn integral_weight_validator_panics_on_negative_integral() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut step = 0i64;
            let source = circuit.add_source(Generator::new(move || {
                // Step 0: insert key 42 with weight +1 → integral weight 1.
                // Step 1: delete key 42 with weight -2 → integral weight -1 (panic).
                let batch: OrdZSet<i64> = if step == 0 {
                    OrdZSet::from_tuples((), vec![Tup2(Tup2(42i64, ()), 1 as ZWeight)])
                } else {
                    OrdZSet::from_tuples((), vec![Tup2(Tup2(42i64, ()), -2 as ZWeight)])
                };
                step += 1;
                batch
            }));
            source.integral_weight_validator("table 'test'");
            Ok(())
        })
        .unwrap()
        .0;

        circuit.transaction().unwrap(); // step 0: integral weight 1 (ok)
        circuit.transaction().unwrap(); // step 1: integral weight -1 (panic)
    }

    /// A weight that goes negative
    #[test]
    #[should_panic(expected = "table 'test': negative weight found")]
    fn integral_weight_validator_panics_on_negative_weight() {
        let circuit = RootCircuit::build(move |circuit| {
            // Emit weight -1 immediately; integral weight is -1 after step 0.
            let source = circuit.add_source(Generator::new(|| zset! { 42i64 => -1 }));
            source.integral_weight_validator("table 'test'");
            Ok(())
        })
        .unwrap()
        .0;

        circuit.transaction().unwrap();
    }
}
