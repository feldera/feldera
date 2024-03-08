use super::NexmarkStream;
use crate::model::Event;
use dbsp::{utils::Tup2, OrdZSet, RootCircuit, Stream};

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

pub fn q2(input: NexmarkStream) -> Stream<RootCircuit, OrdZSet<Tup2<u64, u64>>> {
    input.flat_map(|event| match event {
        Event::Bid(b) => match b.auction % AUCTION_ID_MODULO == 0 {
            true => Some(Tup2(b.auction, b.price)),
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
    use dbsp::{OrdZSet, RootCircuit, ZWeight};

    #[test]
    fn test_q2() {
        let input_vecs: Vec<Vec<Tup2<Event, ZWeight>>> = vec![
            vec![
                Tup2(
                    Event::Bid(Bid {
                        auction: 1,
                        price: 80,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: AUCTION_ID_MODULO,
                        price: 111,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: AUCTION_ID_MODULO + 1,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            vec![
                Tup2(
                    Event::Bid(Bid {
                        auction: 3 * AUCTION_ID_MODULO + 25,
                        price: 80,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 4 * AUCTION_ID_MODULO,
                        price: 222,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
        ];

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q2(stream);

            let mut expected_output = vec![
                OrdZSet::from_keys((), vec![Tup2(Tup2(AUCTION_ID_MODULO, 111), 1)]),
                OrdZSet::from_keys((), vec![Tup2(Tup2(4 * AUCTION_ID_MODULO, 222), 1)]),
            ]
            .into_iter();

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs.into_iter() {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }
}
