use super::NexmarkStream;
use crate::model::Event;
use dbsp::{
    algebra::UnimplementedSemigroup,
    operator::{Fold, Max},
    utils::Tup2,
    OrdIndexedZSet, OrdZSet, RootCircuit, Stream,
};

/// Query 6: Average Selling Price by Seller
///
/// What is the average selling price per seller for their last 10 closed
/// auctions. Shares the same ‘winning bids’ core as for Query4, and illustrates
/// a specialized combiner.
///
/// From [Nexmark q6.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q6.sql):
///
/// ```sql
/// CREATE TABLE discard_sink (
///   seller VARCHAR,
///   avg_price  BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// -- TODO: this query is not supported yet in Flink SQL, because the OVER WINDOW operator doesn't
/// --  support to consume retractions.
/// INSERT INTO discard_sink
/// SELECT
///     Q.seller,
///     AVG(Q.final) OVER
///         (PARTITION BY Q.seller ORDER BY Q.dateTime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
/// FROM (
///     SELECT MAX(B.price) AS final, A.seller, B.dateTime
///     FROM auction AS A, bid AS B
///     WHERE A.id = B.auction and B.dateTime between A.dateTime and A.expires
///     GROUP BY A.id, A.seller
/// ) AS Q;
/// ```

type Q6Stream = Stream<RootCircuit, OrdIndexedZSet<u64, u64>>;

const NUM_AUCTIONS_PER_SELLER: usize = 10;

pub fn q6(input: NexmarkStream) -> Q6Stream {
    // Select auctions sellers and index by auction id.
    let auctions_by_id = input.flat_map_index(|event| match event {
        Event::Auction(a) => Some((a.id, (a.seller, a.date_time, a.expires))),
        _ => None,
    });

    // Select bids and index by auction id.
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, (b.price, b.date_time))),
        _ => None,
    });

    type BidsAuctionsJoin = Stream<RootCircuit, OrdZSet<((u64, u64, u64, u64), (u64, u64))>>;

    // Join to get bids for each auction.
    let bids_for_auctions: BidsAuctionsJoin = auctions_by_id.join(
        &bids_by_auction,
        |&auction_id, &(seller, a_date_time, a_expires), &(bid_price, bid_date_time)| {
            (
                (auction_id, seller, a_date_time, a_expires),
                (bid_price, bid_date_time),
            )
        },
    );

    // Filter out the invalid bids while indexing.
    // TODO: update to use incremental version of `join_range` once implemented
    // (#137).
    let bids_for_auctions_indexed = bids_for_auctions.flat_map_index(
        |&((auction_id, seller, a_date_time, a_expires), (bid_price, bid_date_time))| {
            if bid_date_time >= a_date_time && bid_date_time <= a_expires {
                Some(((auction_id, seller), bid_price))
            } else {
                None
            }
        },
    );

    // winning_bids_by_seller: once we have the winning bids, we don't
    // need the auction ids anymore.
    // TODO: We can optimize this given that there are no deletions, as DBSP
    // doesn't need to keep records of the bids for future max calculations.
    type WinningBidsBySeller = Stream<RootCircuit, OrdIndexedZSet<u64, Tup2<u64, u64>>>;
    let winning_bids_by_seller_indexed: WinningBidsBySeller = bids_for_auctions_indexed
        .aggregate(Max)
        .map_index(|(key, max)| (key.1, Tup2(key.0, *max)));

    // Finally, calculate the average winning bid per seller, using the last
    // 10 closed auctions.
    // TODO: use linear aggregation when ready (#138).
    winning_bids_by_seller_indexed.aggregate(
        <Fold<_, _, UnimplementedSemigroup<_>, _, _>>::with_output(
            Vec::with_capacity(NUM_AUCTIONS_PER_SELLER),
            |top: &mut Vec<u64>, val: &Tup2<u64, u64>, _w| {
                if top.len() >= NUM_AUCTIONS_PER_SELLER {
                    top.remove(0);
                }
                top.push(val.1);
            },
            |top: Vec<u64>| -> u64 {
                let len = top.len() as u64;
                let sum: u64 = Iterator::sum(top.into_iter());
                sum / len
            },
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        generator::tests::{make_auction, make_bid},
        model::{Auction, Bid, Event},
    };
    use dbsp::{indexed_zset, RootCircuit};

    #[test]
    fn test_q6_single_seller_single_auction() {
        let input_vecs = vec![
            // The first batch has a single auction for seller 99 with a highest bid of 100
            // (currently).
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
            // The second batch has a new highest bid for the (currently) only auction.
            vec![Tup2(
                Event::Bid(Bid {
                    auction: 1,
                    date_time: 9_000,
                    price: 200,
                    ..make_bid()
                }),
                1,
            )],
            // The third batch has a new bid but it's not higher, so no effect.
            vec![Tup2(
                Event::Bid(Bid {
                    auction: 1,
                    date_time: 9_500,
                    price: 150,
                    ..make_bid()
                }),
                1,
            )],
        ]
        .into_iter();

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let mut expected_output = vec![
                // First batch has a single auction seller with best bid of 100.
                indexed_zset! { 99 => {100 => 1} },
                // The second batch just updates the best bid for the single auction to 200 (ie. no
                // averaging).
                indexed_zset! { 99 => {100 => -1, 200 => 1} },
                // The third batch has a bid that isn't higher, so no change.
                indexed_zset! {},
            ]
            .into_iter();

            let output = q6(stream);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }

    #[test]
    fn test_q6_single_seller_multiple_auctions() {
        let input_vecs = vec![
            // The first batch has a single auction for seller 99 with a highest bid of 100.
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
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            // The second batch adds a new auction for the same seller, with
            // a final bid of 200, so the average should be 150 for this seller.
            vec![
                Tup2(
                    Event::Auction(Auction {
                        id: 2,
                        seller: 99,
                        expires: 20_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 2,
                        date_time: 15_000,
                        price: 200,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
        ]
        .into_iter();

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();
            let mut expected_output = vec![
                // First batch has a single auction seller with best bid of 100.
                indexed_zset! { 99 => {100 => 1} },
                // The second batch adds another auction for the same seller with a final bid of
                // 200, so average is 150.
                indexed_zset! { 99 => {100 => -1, 150 => 1} },
            ]
            .into_iter();

            let output = q6(stream);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }

    #[test]
    fn test_q6_single_seller_more_than_10_auctions() {
        let input_vecs = vec![
            // The first batch has 5 auctions all with single bids of 100, except
            // the first which is at 200.
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
                        date_time: 2_000,
                        price: 200,
                        ..make_bid()
                    }),
                    1,
                ),
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
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 3,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 3,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 4,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 4,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 5,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 5,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            // The second batch has another 5 auctions all with single bids of 100.
            vec![
                Tup2(
                    Event::Auction(Auction {
                        id: 6,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 6,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 7,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 7,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 8,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 8,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 9,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 9,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 10,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 10,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            // The third batch has a single auction and bid of 100. The last
            // 10 auctions all have 100 now, so average is 100.
            vec![
                Tup2(
                    Event::Auction(Auction {
                        id: 11,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 11,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
        ]
        .into_iter();

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();
            let mut expected_output = vec![
                // First has 5 auction for person 99, but average is (200 + 100 * 4) / 5.
                indexed_zset! { 99 => {120 => 1} },
                // Second batch adds another 5 auctions for person 99, but average is still 110.
                indexed_zset! { 99 => {120 => -1, 110 => 1} },
                // Third batch adds a single auction with bid of 100, pushing
                // out the first bid so average is now 100.
                indexed_zset! { 99 => {110 => -1, 100 => 1} },
            ]
            .into_iter();

            let output = q6(stream);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }

    #[test]
    fn test_q6_multiple_sellers_multiple_auctions() {
        let input_vecs = vec![
            // The first batch has a single auction for seller 99 with a highest bid of 100.
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
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            // The second batch adds a new auction for a different seller, with
            // a final bid of 200, so the two sellers have 100 and 200 as
            // their averages.
            vec![
                Tup2(
                    Event::Auction(Auction {
                        id: 2,
                        seller: 33,
                        expires: 20_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 2,
                        date_time: 15_000,
                        price: 200,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            // The third batch adds a new auction for both sellers, with
            // final bids of 200, so the two sellers have 150 and 200 as
            // their averages.
            vec![
                Tup2(
                    Event::Auction(Auction {
                        id: 3,
                        seller: 99,
                        expires: 20_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 3,
                        date_time: 15_000,
                        price: 200,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 4,
                        seller: 33,
                        expires: 20_000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 4,
                        date_time: 15_000,
                        price: 200,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
        ]
        .into_iter();

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let mut expected_output = vec![
                // First batch has a single auction seller with best bid of 100.
                indexed_zset! { 99 => {100 => 1} },
                // The second batch adds another auction for the same seller with a final bid of
                // 200, so average is 150.
                indexed_zset! { 33 => {200 => 1} },
                // The average for person 33 doesn't change, only for 99.
                indexed_zset! { 99 => {100 => -1, 150 => 1} },
            ]
            .into_iter();

            let output = q6(stream);

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
