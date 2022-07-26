use super::NexmarkStream;
use crate::operator::FilterMap;
/// Passthrough
///
/// Measures the monitoring overhead including the source generator.
pub fn q0(input: NexmarkStream) -> NexmarkStream {
    input.map(|event| event.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexmark::tests::{generate_expected_zset_tuples, make_test_source};
    use crate::{circuit::Root, trace::ord::OrdZSet, trace::Batch};

    #[test]
    fn test_q0() {
        let mut source = make_test_source(0, 10);
        source.set_wallclock_time_iterator((0..1).into_iter());

        let root = Root::build(move |circuit| {
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

        root.step().unwrap();
    }
}
