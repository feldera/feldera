use super::NexmarkStream;
use dbsp::{
    algebra::UnimplementedSemigroup,
    operator::{FilterMap, Fold},
    RootCircuit, OrdZSet, Stream,
};
use crate::model::{Bid, Event};

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

type Q19Stream = Stream<RootCircuit, OrdZSet<Bid, isize>>;

const TOP_BIDS: usize = 10;

pub fn q19(input: NexmarkStream) -> Q19Stream {
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, (b.price, b.clone()))),
        _ => None,
    });

    bids_by_auction
        .aggregate(<Fold<_, UnimplementedSemigroup<_>, _, _>>::new(
            Vec::with_capacity(TOP_BIDS),
            |top: &mut Vec<Bid>, (_price, bid): &(usize, Bid), _w| {
                if top.len() >= TOP_BIDS {
                    top.remove(0);
                }
                top.push(bid.clone());
            },
        ))
        .flat_map(|(_, vec)| -> Vec<Bid> { (*vec).clone() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp::zset;
    use crate::{generator::tests::make_bid, model::Bid};
    use rstest::rstest;

    #[rstest]
    #[case::top_bids_for_single_auction(
        vec![
            vec![
                (1, 12, 100),
                (1, 1, 1_200),
                (1, 3, 1_100),
                (1, 4, 1_000),
                (1, 5, 200),
                (1, 6, 300),
                (1, 7, 400),
                (1, 8, 500),
                (1, 9, 600),
                (1, 10, 700),
                (1, 11, 800),
                (1, 12, 900),

            ],
            vec![
                (1, 1, 1_300),
                (1, 1, 50),
            ]
        ],
        vec![zset![
            Bid {
                auction: 1,
                bidder: 6,
                price: 300,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 400,
                bidder: 7,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 500,
                bidder: 8,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 600,
                bidder: 9,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 700,
                bidder: 10,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 800,
                bidder: 11,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 900,
                bidder: 12,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 1000,
                bidder: 4,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 1100,
                bidder: 3,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                price: 1200,
                bidder: 1,
                ..make_bid()
            } => 1,
        ], zset![
            Bid {
                auction: 1,
                price: 300,
                bidder: 6,
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
                (1, 1, 100),
                (1, 1, 200),
                (7, 1, 100),
                (7, 1, 1_200),
                (7, 1, 1_100),
                (7, 1, 1_000),
                (7, 1, 200),
                (7, 1, 300),
                (7, 1, 400),
                (7, 1, 500),
                (7, 1, 600),
                (7, 1, 700),
                (7, 1, 800),
                (7, 1, 900),

            ],
            vec![
                (1, 1, 1_300),
                (1, 1, 50),
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
        #[case] input_bid_batches: Vec<Vec<(u64, u64, usize)>>,
        #[case] expected_zsets: Vec<OrdZSet<Bid, isize>>,
    ) {
        let input_vecs = input_bid_batches.into_iter().map(|batch| {
            batch
                .into_iter()
                .map(|(auction, bidder, price)| {
                    (
                        Event::Bid(Bid {
                            auction,
                            bidder,
                            price,
                            ..make_bid()
                        }),
                        1,
                    )
                })
                .collect()
        });

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = q19(stream);

            let mut expected_output = expected_zsets.into_iter();
            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }
}
