use super::NexmarkStream;
use crate::model::Event;
use dbsp::{operator::FilterMap, RootCircuit, OrdZSet, Stream};

/// Selection
///
/// Find bids with specific auction ids and show their bid price.
///
/// From [Nexmark q2.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q2.sql):
///
/// CREATE TABLE discard_sink (
///   auction  BIGINT,
///   price  BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;
const AUCTION_ID_MODULO: u64 = 123;

pub fn q2(input: NexmarkStream) -> Stream<RootCircuit, OrdZSet<(u64, usize), isize>> {
    input.flat_map(|event| match event {
        Event::Bid(b) => match b.auction % AUCTION_ID_MODULO == 0 {
            true => Some((b.auction, b.price)),
            false => None,
        },
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        generator::tests::make_bid,
        model::{Bid, Event},
    };
    use dbsp::{trace::Batch, RootCircuit, OrdZSet};

    #[test]
    fn test_q2() {
        let input_vecs: Vec<Vec<(Event, isize)>> = vec![
            vec![
                (
                    Event::Bid(Bid {
                        auction: 1,
                        price: 80,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        auction: AUCTION_ID_MODULO,
                        price: 111,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        auction: AUCTION_ID_MODULO + 1,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            vec![
                (
                    Event::Bid(Bid {
                        auction: 3 * AUCTION_ID_MODULO + 25,
                        price: 80,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        auction: 4 * AUCTION_ID_MODULO,
                        price: 222,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
        ];

        let (circuit, mut input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = q2(stream);

            let mut expected_output = vec![
                OrdZSet::from_keys((), vec![((AUCTION_ID_MODULO, 111), 1)]),
                OrdZSet::from_keys((), vec![((4 * AUCTION_ID_MODULO, 222), 1)]),
            ]
            .into_iter();

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            input_handle
        })
        .unwrap();

        for mut vec in input_vecs.into_iter() {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }
}
