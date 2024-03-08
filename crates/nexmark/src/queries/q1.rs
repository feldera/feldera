use super::NexmarkStream;
use crate::model::{Bid, Event};

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
    use crate::{
        generator::tests::{make_auction, make_bid},
        model::{Auction, Bid, Event},
    };
    use dbsp::{utils::Tup2, OrdZSet, RootCircuit, ZWeight};

    #[test]
    fn test_q1() {
        fn input_vecs() -> Vec<Vec<Tup2<Event, ZWeight>>> {
            vec![
                vec![
                    Tup2(
                        Event::Auction(Auction {
                            id: 1,
                            seller: 99,
                            expires: 10_000,
                            ..make_auction()
                        }),
                        1,
                    ),
                    Tup2(
                        Event::Bid(Bid {
                            auction: 1,
                            date_time: 1_000,
                            price: 80,
                            ..make_bid()
                        }),
                        1,
                    ),
                    Tup2(
                        Event::Bid(Bid {
                            auction: 1,
                            date_time: 2_000,
                            price: 100,
                            ..make_bid()
                        }),
                        1,
                    ),
                ],
                vec![
                    Tup2(
                        Event::Auction(Auction {
                            id: 2,
                            seller: 99,
                            expires: 10_000,
                            ..make_auction()
                        }),
                        1,
                    ),
                    Tup2(
                        Event::Bid(Bid {
                            auction: 2,
                            date_time: 1_000,
                            price: 80,
                            ..make_bid()
                        }),
                        1,
                    ),
                    Tup2(
                        Event::Bid(Bid {
                            auction: 2,
                            date_time: 2_000,
                            price: 100,
                            ..make_bid()
                        }),
                        1,
                    ),
                ],
            ]
        }

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q1(stream);

            let mut expected_output = input_vecs().into_iter().map(|v| {
                let expected_v = v
                    .into_iter()
                    .map(|Tup2(e, w)| match e {
                        Event::Bid(b) => Tup2(
                            Event::Bid(Bid {
                                price: b.price * 89 / 100,
                                ..b
                            }),
                            w,
                        ),
                        _ => Tup2(e, w),
                    })
                    .collect();
                OrdZSet::from_keys((), expected_v)
            });

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs().into_iter() {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }
}
