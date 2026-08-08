use super::NexmarkStream;
use crate::model::{Bid, Event};
use dbsp::{
    algebra::UnimplementedSemigroup, operator::Fold, utils::Tup2, OrdZSet, RootCircuit, Stream,
};

// Each output row is a bid and its rank_number, following SELECT * of the
// original query, which includes the ROW_NUMBER column.
type Q19Stream = Stream<RootCircuit, OrdZSet<Tup2<Bid, u64>>>;

const TOP_BIDS: usize = 10;

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
pub fn q19(_circuit: &mut RootCircuit, input: NexmarkStream) -> Q19Stream {
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, Tup2(b.price, b.clone()))),
        _ => None,
    });

    bids_by_auction
        .aggregate(<Fold<_, _, UnimplementedSemigroup<_>, _, _>>::new(
            Vec::with_capacity(TOP_BIDS),
            |top: &mut Vec<Bid>, Tup2(_price, bid): &Tup2<u64, Bid>, _w| {
                if top.len() >= TOP_BIDS {
                    top.remove(0);
                }
                top.push(bid.clone());
            },
        ))
        // The vector contains the top bids in ascending price order, so the
        // last one has rank 1.
        .flat_map(|(_, vec)| -> Vec<Tup2<Bid, u64>> {
            let len = vec.len();
            vec.iter()
                .enumerate()
                .map(|(i, bid)| Tup2(bid.clone(), (len - i) as u64))
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{generator::tests::make_bid, model::Bid};
    use rstest::rstest;

    // (auction, bidder, price, rank_number, weight)
    type ExpectedRow = (u64, u64, u64, u64, i64);

    fn expected_zset(rows: &[ExpectedRow]) -> OrdZSet<Tup2<Bid, u64>> {
        OrdZSet::from_keys(
            (),
            rows.iter()
                .map(|&(auction, bidder, price, rank, w)| {
                    Tup2(
                        Tup2(
                            Bid {
                                auction,
                                bidder,
                                price,
                                ..make_bid()
                            },
                            rank,
                        ),
                        w,
                    )
                })
                .collect(),
        )
    }

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
        vec![vec![
            (1, 1, 1_200, 1, 1),
            (1, 3, 1_100, 2, 1),
            (1, 4, 1_000, 3, 1),
            (1, 12, 900, 4, 1),
            (1, 11, 800, 5, 1),
            (1, 10, 700, 6, 1),
            (1, 9, 600, 7, 1),
            (1, 8, 500, 8, 1),
            (1, 7, 400, 9, 1),
            (1, 6, 300, 10, 1),
        ], vec![
            // The new top bid of 1300 shifts every rank down by one and pushes
            // the bid of 300 out of the top 10.
            (1, 1, 1_200, 1, -1),
            (1, 3, 1_100, 2, -1),
            (1, 4, 1_000, 3, -1),
            (1, 12, 900, 4, -1),
            (1, 11, 800, 5, -1),
            (1, 10, 700, 6, -1),
            (1, 9, 600, 7, -1),
            (1, 8, 500, 8, -1),
            (1, 7, 400, 9, -1),
            (1, 6, 300, 10, -1),
            (1, 1, 1_300, 1, 1),
            (1, 1, 1_200, 2, 1),
            (1, 3, 1_100, 3, 1),
            (1, 4, 1_000, 4, 1),
            (1, 12, 900, 5, 1),
            (1, 11, 800, 6, 1),
            (1, 10, 700, 7, 1),
            (1, 9, 600, 8, 1),
            (1, 8, 500, 9, 1),
            (1, 7, 400, 10, 1),
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
        vec![vec![
            (1, 1, 200, 1, 1),
            (1, 1, 100, 2, 1),
            (7, 1, 1_200, 1, 1),
            (7, 1, 1_100, 2, 1),
            (7, 1, 1_000, 3, 1),
            (7, 1, 900, 4, 1),
            (7, 1, 800, 5, 1),
            (7, 1, 700, 6, 1),
            (7, 1, 600, 7, 1),
            (7, 1, 500, 8, 1),
            (7, 1, 400, 9, 1),
            (7, 1, 300, 10, 1),
        ], vec![
            // Auction 7 is unchanged; the ranks of auction 1 shift.
            (1, 1, 200, 1, -1),
            (1, 1, 100, 2, -1),
            (1, 1, 1_300, 1, 1),
            (1, 1, 200, 2, 1),
            (1, 1, 100, 3, 1),
            (1, 1, 50, 4, 1),
        ]]
    )]
    pub fn test_q19(
        #[case] input_bid_batches: Vec<Vec<(u64, u64, u64)>>,
        #[case] expected_batches: Vec<Vec<ExpectedRow>>,
    ) {
        let input_vecs = input_bid_batches.into_iter().map(|batch| {
            batch
                .into_iter()
                .map(|(auction, bidder, price)| {
                    Tup2(
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

        let (mut circuit, input_handle) = dbsp::Runtime::init_circuit(1, move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q19(circuit, stream);

            let mut expected_output = expected_batches.into_iter().map(|b| expected_zset(&b));
            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.transaction().unwrap();
        }
    }
}
