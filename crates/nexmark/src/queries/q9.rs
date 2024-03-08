use super::NexmarkStream;
use crate::model::Event;
use dbsp::{
    operator::Max,
    utils::{Tup10, Tup2, Tup4, Tup9},
    OrdIndexedZSet, OrdZSet, RootCircuit, Stream,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

/// Query 9: Winning Bids (Not in original suite)
///
/// Find the winning bid for each auction.
///
/// From https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/resources/queries/q9.sql
///
/// TODO: streaming join doesn't support rowtime attribute in input, this should
/// be fixed by FLINK-18651. As a workaround, we re-create a new view without
/// rowtime attribute for now.
///
/// ```sql
/// DROP VIEW IF EXISTS auction;
/// DROP VIEW IF EXISTS bid;
/// CREATE VIEW auction AS SELECT auction.* FROM ${NEXMARK_TABLE} WHERE event_type = 1;
/// CREATE VIEW bid AS SELECT bid.* FROM ${NEXMARK_TABLE} WHERE event_type = 2;
///
/// CREATE TABLE discard_sink (
///   id  BIGINT,
///   itemName  VARCHAR,
///   description  VARCHAR,
///   initialBid  BIGINT,
///   reserve  BIGINT,
///   dateTime  TIMESTAMP(3),
///   expires  TIMESTAMP(3),
///   seller  BIGINT,
///   category  BIGINT,
///   extra  VARCHAR,
///   auction  BIGINT,
///   bidder  BIGINT,
///   price  BIGINT,
///   bid_dateTime  TIMESTAMP(3),
///   bid_extra  VARCHAR
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra,
///     auction, bidder, price, bid_dateTime, bid_extra
/// FROM (
///    SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra,
///      ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
///    FROM auction A, bid B
///    WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
/// )
/// WHERE rownum <= 1;
/// ```

#[derive(
    Eq,
    Clone,
    Default,
    Debug,
    Hash,
    PartialEq,
    PartialOrd,
    Ord,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Q9Output(
    u64,
    String,
    String,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    String,
    u64,
    u64,
    u64,
    u64,
    String,
);

type Q9Stream = Stream<RootCircuit, OrdZSet<Q9Output>>;

pub fn q9(input: NexmarkStream) -> Q9Stream {
    // Select auctions and index by auction id.
    let auctions_by_id = input.flat_map_index(|event| match event {
        Event::Auction(a) => Some((
            a.id,
            Tup9(
                a.item_name.clone(),
                a.description.clone(),
                a.initial_bid,
                a.reserve,
                a.date_time,
                a.expires,
                a.seller,
                a.category,
                a.extra.clone(),
            ),
        )),
        _ => None,
    });

    // Select bids and index by auction id.
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((
            b.auction,
            Tup4(b.bidder, b.price, b.date_time, b.extra.clone()),
        )),
        _ => None,
    });

    type BidsAuctionsJoin = Stream<
        RootCircuit,
        OrdZSet<
            Tup2<
                Tup10<u64, String, String, u64, u64, u64, u64, u64, u64, String>,
                Tup4<u64, u64, u64, String>,
            >,
        >,
    >;

    // Join to get bids for each auction.
    let bids_for_auctions: BidsAuctionsJoin = auctions_by_id.join(
        &bids_by_auction,
        |&auction_id,
         Tup9(
            a_item_name,
            a_description,
            a_initial_bid,
            a_reserve,
            a_date_time,
            a_expires,
            a_seller,
            a_category,
            a_extra,
        ),
         Tup4(b_bidder, b_price, b_date_time, b_extra)| {
            Tup2(
                Tup10(
                    auction_id,
                    a_item_name.clone(),
                    a_description.clone(),
                    *a_initial_bid,
                    *a_reserve,
                    *a_date_time,
                    *a_expires,
                    *a_seller,
                    *a_category,
                    a_extra.clone(),
                ),
                Tup4(*b_bidder, *b_price, *b_date_time, b_extra.clone()),
            )
        },
    );

    // Filter out the invalid bids while indexing.
    // TODO: update to use incremental version of `join_range` once implemented
    // (#137).
    let bids_for_auctions_indexed = bids_for_auctions.flat_map_index(
        |Tup2(
            Tup10(
                auction_id,
                a_item_name,
                a_description,
                a_initial_bid,
                a_reserve,
                a_date_time,
                a_expires,
                a_seller,
                a_category,
                a_extra,
            ),
            Tup4(b_bidder, b_price, b_date_time, b_extra),
        )| {
            if b_date_time >= a_date_time && b_date_time <= a_expires {
                Some((
                    Tup10(
                        *auction_id,
                        a_item_name.to_string(),
                        a_description.to_string(),
                        *a_initial_bid,
                        *a_reserve,
                        *a_date_time,
                        *a_expires,
                        *a_seller,
                        *a_category,
                        a_extra.to_string(),
                    ),
                    // Note that the price of the bid is first in the tuple here to ensure that the
                    // default lexicographic Ord of tuples does what we want below.
                    Tup4(*b_price, *b_bidder, *b_date_time, b_extra.clone()),
                ))
            } else {
                None
            }
        },
    );

    // TODO: We can optimize this given that there are no deletions, as DBSP
    // doesn't need to keep records of the bids for future max calculations.
    type AuctionsWithWinningBids = Stream<
        RootCircuit,
        OrdIndexedZSet<
            Tup10<u64, String, String, u64, u64, u64, u64, u64, u64, String>,
            Tup4<u64, u64, u64, String>,
        >,
    >;
    let auctions_with_winning_bids: AuctionsWithWinningBids =
        bids_for_auctions_indexed.aggregate(Max);

    // Finally, put the output together as expected and flip the price/bidder
    // into the output order.
    auctions_with_winning_bids.map(
        |(
            Tup10(
                auction_id,
                a_item_name,
                a_description,
                a_initial_bid,
                a_reserve,
                a_date_time,
                a_expires,
                a_seller,
                a_category,
                a_extra,
            ),
            Tup4(b_price, b_bidder, b_date_time, b_extra),
        )| {
            Q9Output(
                *auction_id,
                a_item_name.clone(),
                a_description.clone(),
                *a_initial_bid,
                *a_reserve,
                *a_date_time,
                *a_expires,
                *a_seller,
                *a_category,
                a_extra.clone(),
                *auction_id,
                *b_bidder,
                *b_price,
                *b_date_time,
                b_extra.clone(),
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        generator::tests::{make_auction, make_bid},
        model::{Auction, Bid, Event},
    };
    use dbsp::zset;

    #[test]
    fn test_q9() {
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
            // And adds a new auction without any bids (empty join).
            vec![
                Tup2(
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 9_000,
                        price: 200,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 2,
                        seller: 101,
                        expires: 20_000,
                        ..make_auction()
                    }),
                    1,
                ),
            ],
            // The third batch has a new bid but it's not higher, so no effect to the first
            // auction. A bid added for the second auction, so it is added.
            vec![
                Tup2(
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 9_500,
                        price: 150,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 2,
                        date_time: 19_000,
                        price: 400,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            // The fourth and final batch has a new bid for auction 2, but it's
            // come in (one millisecond) too late to be valid, so no change.
            vec![Tup2(
                Event::Bid(Bid {
                    auction: 2,
                    date_time: 20_001,
                    price: 999,
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
                zset! { Q9Output(1, "item-name".to_string(), "description".to_string(), 5, 10, 0, 10000, 99, 1, "".to_string(), 1, 1, 100, 2000, "".to_string()) => 1 },
                // The second batch just updates the best bid for the single auction to 200.
                zset! { Q9Output(1, "item-name".to_string(), "description".to_string(), 5, 10, 0, 10000, 99, 1, "".to_string(), 1, 1, 100, 2000, "".to_string()) => -1, Q9Output(1, "item-name".to_string(), "description".to_string(), 5, 10, 0, 10000, 99, 1, "".to_string(), 1, 1, 200, 9000, "".to_string()) => 1 },
                // The third batch has a bid for the first auction that isn't
                // higher than the existing bid, so no change there. A (first)
                // bid for the second auction creates a new addition:
                zset! { Q9Output(2, "item-name".to_string(), "description".to_string(), 5, 10, 0, 20_000, 101, 1, "".to_string(), 2, 1, 400, 19_000, "".to_string()) => 1 },
                // The last batch just has an invalid (too late) winning bid for
                // auction 2, so no change.
                zset! {},
            ]
            .into_iter();

            let output = q9(stream);
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
