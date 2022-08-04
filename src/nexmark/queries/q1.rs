use super::NexmarkStream;
use crate::nexmark::model::{Bid, Event};
use crate::operator::FilterMap;

/// Currency Conversion
///
/// Convert each bid value from dollars to euros. Illustrates a simple
/// transformation.
///
/// From [Nexmark q1.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q1.sql):
///
/// CREATE TABLE discard_sink (
///   auction  BIGINT,
///   bidder  BIGINT,
///   price  DECIMAL(23, 3),
///   dateTime  TIMESTAMP(3),
///   extra  VARCHAR
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     auction,
///     bidder,
///     0.908 * price as price, -- convert dollar to euro
///     dateTime,
///     extra
/// FROM bid;
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
    use crate::nexmark::tests::{generate_expected_zset_tuples, make_source_with_wallclock_times};
    use crate::{trace::Batch, Circuit, OrdZSet};

    #[test]
    fn test_q1() {
        let (source, _) = make_source_with_wallclock_times(0..2, 10);

        let circuit = Circuit::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q1(input);

            // Manually update the generated test result with the expected prices
            // for a single batch.
            output.inspect(move |e| {
                assert_eq!(
                    e,
                    &OrdZSet::from_keys(
                        (),
                        generate_expected_zset_tuples(0, 10)
                            .into_iter()
                            .map(|(event, w)| {
                                let event = match event {
                                    Event::Bid(b) => Event::Bid(Bid { price: 89, ..b }),
                                    _ => event,
                                };
                                (event, w)
                            })
                            .collect()
                    )
                )
            });
        })
        .unwrap();

        circuit.step().unwrap();
    }
}
