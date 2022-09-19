use super::NexmarkStream;
use crate::{
    nexmark::model::{Bid, Event},
    operator::{FilterMap, Fold},
    Circuit, OrdZSet, Stream,
};
use std::collections::VecDeque;

///
/// Query 19: Auction TOP-10 Price (Not in original suite)
///
/// What's the top price 10 bids of an auction?
/// Illustrates a TOP-N query.
///
/// ```sql
/// CREATE TABLE discard_sink (
///     auction  BIGINT,
///     bidder  BIGINT,
///     price  BIGINT,
///     channel  VARCHAR,
///     url  VARCHAR,
///     dateTime  TIMESTAMP(3),
///     extra  VARCHAR,
///     rank_number  BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT * FROM
/// (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
/// WHERE rank_number <= 10;
/// ```

type Q19Stream = Stream<Circuit<()>, OrdZSet<Bid, isize>>;

const TOP_BIDS: usize = 10;

pub fn q19(input: NexmarkStream) -> Q19Stream {
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, b.clone())),
        _ => None,
    });

    bids_by_auction
        .aggregate::<(), _>(Fold::new(
            VecDeque::with_capacity(TOP_BIDS),
            |top: &mut VecDeque<Bid>, val: &Bid, _w| {
                if top.len() >= TOP_BIDS {
                    top.pop_front();
                }
                top.push_back(val.clone());
            },
        ))
        .flat_map(|(_, vec)| -> VecDeque<Bid> { (*vec).clone() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nexmark::{generator::tests::make_bid, model::Bid},
        zset,
    };
    use rstest::rstest;

    #[rstest]
    #[case::top_bids_for_single_auction(
        vec![
            vec![
                (1, 100),
                (1, 1_200),
                (1, 1_100),
                (1, 1_000),
                (1, 200),
                (1, 300),
                (1, 400),
                (1, 500),
                (1, 600),
                (1, 700),
                (1, 800),
                (1, 900),

            ],
            vec![
                (1, 1_300),
                (1, 50),
            ]
        ],
        vec![zset![
            Bid {
                auction: 1,
                price: 300,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 400,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 500,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 600,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 700,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 800,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 900,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 1000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 1100,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 1200,
                ..make_bid()
            } => 1,
        ], zset![
            Bid {
                auction: 1,
                price: 300,
                ..make_bid()
            } => -1,
            Bid {
                auction: 1,
                price: 1_300,
                ..make_bid()
            } => 1,
        ]]
    )]
    #[case::top_bids_for_multiple_auctions(
        vec![
            vec![
                (1, 100),
                (1, 200),
                (7, 100),
                (7, 1_200),
                (7, 1_100),
                (7, 1_000),
                (7, 200),
                (7, 300),
                (7, 400),
                (7, 500),
                (7, 600),
                (7, 700),
                (7, 800),
                (7, 900),

            ],
            vec![
                (1, 1_300),
                (1, 50),
            ]
        ],
        vec![zset![
            Bid {
                auction: 1,
                price: 100,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 200,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 300,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 400,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 500,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 600,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 700,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 800,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 900,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 1000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 1100,
                ..make_bid()
            } => 1,
            Bid {
                auction: 7,
                price: 1200,
                ..make_bid()
            } => 1,
        ], zset![
            Bid {
                auction: 1,
                price: 50,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 1_300,
                ..make_bid()
            } => 1,
        ]]
    )]
    pub fn test_q19(
        #[case] input_bid_batches: Vec<Vec<(u64, usize)>>,
        #[case] expected_zsets: Vec<OrdZSet<Bid, isize>>,
    ) {
        let input_vecs = input_bid_batches.into_iter().map(|batch| {
            batch
                .into_iter()
                .map(|(auction, price)| {
                    (
                        Event::Bid(Bid {
                            auction,
                            price,
                            ..make_bid()
                        }),
                        1,
                    )
                })
                .collect()
        });

        let (circuit, mut input_handle) = Circuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = q19(stream);

            let mut expected_output = expected_zsets.into_iter();
            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            input_handle
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }
}
