//! Integral operator that panics when any record's accumulated weight
//! becomes negative.

use crate::{
    ZWeight,
    circuit::{Circuit, Stream, checkpointer::Checkpoint},
    trace::{BatchReader as TraceBatchReader, Cursor as TraceCursor},
    typed_batch::{BatchReader as TypedBatchReader, Spine, ZSet},
};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Checkpoint + Clone + ZSet + 'static,
    B::InnerBatch: Send,
{
    /// Integrates the stream, panicking if any record's weight in the integral
    /// becomes negative.
    ///
    /// Maintains a running trace of the input stream.
    /// After every clock tick, every key that appears in the
    /// current delta is checked: if the key's weight in the trace is
    /// negative, the operator panics, displaying the offending key, weight,
    /// and a custom message.
    ///
    /// This operator is intended for debugging: it verifies that a stream
    /// behaves like a valid set or multiset where every element has positive
    /// multiplicity.
    ///
    /// The operator passes the input delta through unchanged as its output.
    ///
    /// # Panics
    ///
    /// Panics when any weight in the integral is < 0.
    #[track_caller]
    pub fn integral_weight_validator(&self, message: &str) -> Self {
        let message = message.to_owned();
        self.circuit().region("integral_weight_validator", || {
            let sharded = self.shard();
            let trace = sharded.integrate_trace();
            sharded.apply2(&trace, move |delta: &B, integrated: &Spine<B>| {
                let mut delta_cursor = TraceBatchReader::cursor(delta.inner());
                let mut trace_cursor = TraceBatchReader::cursor(integrated.inner());
                while delta_cursor.key_valid() {
                    // Only examine keys that appear in the current delta with a negative weight
                    let delta_weight: ZWeight = **delta_cursor.weight();
                    if delta_weight < 0 {
                        let key = delta_cursor.key();
                        if trace_cursor.seek_key_exact(key, None) {
                            debug_assert!(trace_cursor.val_valid());
                            let weight: ZWeight = **trace_cursor.weight();
                            if weight < 0 {
                                panic!("{message}: negative weight found {weight} for key {key:?}");
                            }
                        }
                    }
                    delta_cursor.step_key();
                }
            });
            self.clone()
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Circuit, Runtime, ZWeight, operator::Generator, typed_batch::OrdZSet, utils::Tup2, zset,
    };

    /// A stream of purely positive deltas should pass the check at every step.
    #[test]
    fn integral_weight_validator_positive_weights() {
        let (mut circuit, ()) = Runtime::init_circuit(1, move |circuit| {
            let mut step = 0u64;
            let source = circuit.add_source(Generator::new(move || {
                let batch = zset! { step => 1 };
                step += 1;
                batch
            }));
            let out = source.integral_weight_validator("test_table");
            let mut expected_step = 0u64;
            out.inspect(move |delta| {
                assert_eq!(delta, &zset! { expected_step => 1 });
                expected_step += 1;
            });
            Ok(())
        })
        .unwrap();

        for _ in 0..10 {
            circuit.transaction().unwrap();
        }
        circuit.kill().unwrap();
    }

    /// Inserting an element once and then deleting it twice causes the trace
    /// weight to go negative, which must trigger a panic.
    #[test]
    fn integral_weight_validator_panics_on_negative_integral() {
        let (mut circuit, ()) = Runtime::init_circuit(1, move |circuit| {
            let mut step = 0i64;
            let source = circuit.add_source(Generator::new(move || {
                // Step 0: insert key 42 with weight +1 → weight 1.
                // Step 1: delete key 42 with weight -2 → weight -1 (panic).
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
        .unwrap();

        circuit.transaction().unwrap(); // step 0: weight 1 (ok)
        let err = circuit.transaction().expect_err("expected a panic");
        assert!(
            err.to_string()
                .contains("table 'test': negative weight found"),
            "unexpected error: {err}"
        );
        circuit.kill().unwrap();
    }

    /// A weight that goes negative (e.g., deleting something never inserted)
    /// must also panic.
    #[test]
    fn integral_weight_validator_panics_on_negative_weight() {
        let (mut circuit, ()) = Runtime::init_circuit(1, move |circuit| {
            // Emit weight -1 immediately
            let source = circuit.add_source(Generator::new(|| zset! { 42i64 => -1 }));
            source.integral_weight_validator("table 'test'");
            Ok(())
        })
        .unwrap();

        let err = circuit.transaction().expect_err("expected a panic");
        assert!(
            err.to_string()
                .contains("table 'test': negative weight found"),
            "unexpected error: {err}"
        );
        circuit.kill().unwrap();
    }
}
