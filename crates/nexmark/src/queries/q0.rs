use super::NexmarkStream;
/// Passthrough
///
/// Measures the monitoring overhead including the source generator.
/// See [Nexmark q0.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q0.sql)
pub fn q0(input: NexmarkStream) -> NexmarkStream {
    input
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
    fn test_q0() {
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

            let output = q0(stream);

            let mut expected_output = input_vecs().into_iter().map(|v| OrdZSet::from_keys((), v));

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
