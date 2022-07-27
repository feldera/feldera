use super::NexmarkStream;
use crate::operator::FilterMap;
/// Passthrough
///
/// Measures the monitoring overhead including the source generator.
/// See [Nexmark q0.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q0.sql)
pub fn q0(input: NexmarkStream) -> NexmarkStream {
    input.map(|event| event.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexmark::tests::{generate_expected_zset_tuples, make_source_with_wallclock_times};
    use crate::{Circuit, OrdZSet, trace::Batch};

    #[test]
    fn test_q0() {
        let (source, _) = make_source_with_wallclock_times(1..3, 10);

        let circuit = Circuit::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q0(input);

            output.inspect(move |e| {
                assert_eq!(
                    e,
                    &OrdZSet::from_tuples((), generate_expected_zset_tuples(0, 10))
                )
            });
        })
        .unwrap();

        circuit.step().unwrap();
    }
}
