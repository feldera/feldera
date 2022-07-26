use super::NexmarkStream;
use crate::nexmark::model::{Bid, Event};
use crate::operator::FilterMap;

/// Currency Conversion
///
/// Convert each bid value from dollars to euros.
pub fn q1(input: NexmarkStream) -> NexmarkStream {
    input.map(|event| match event {
        Event::Bid(b) => Event::Bid(Bid {
            price: b.price * 89 / 100,
            ..b.clone()
        }),
        _ => event.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexmark::tests::{generate_expected_zset_tuples, make_test_source};
    use crate::{circuit::Root, trace::ord::OrdZSet, trace::Batch};

    #[test]
    fn test_q1() {
        let mut source = make_test_source(0, 10);
        source.set_wallclock_time_iterator((0..1).into_iter());

        let root = Root::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q1(input);

            // Manually update the generated test result with the expected prices
            // for a single batch.
            output.inspect(move |e| {
                assert_eq!(
                    e,
                    &OrdZSet::from_tuples(
                        (),
                        generate_expected_zset_tuples(0, 10)
                            .into_iter()
                            .map(|((event, _), w)| {
                                let event = match event {
                                    Event::Bid(b) => Event::Bid(Bid { price: 89, ..b }),
                                    _ => event,
                                };
                                ((event, ()), w)
                            })
                            .collect()
                    )
                )
            });
        })
        .unwrap();

        root.step().unwrap();
    }
}
