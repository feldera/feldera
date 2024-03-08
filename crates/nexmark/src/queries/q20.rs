use super::NexmarkStream;
use crate::model::{Auction, Bid, Event};
use dbsp::{utils::Tup2, OrdZSet, RootCircuit, Stream};

///
/// Query 20: Expand bid with auction (Not in original suite)
///
/// Get bids with the corresponding auction information where category is 10.
/// Illustrates a filter join.
//
// -- TODO: streaming join doesn't support rowtime attribute in input, this
// should be fixed by FLINK-18651. --  As a workaround, we re-create a new view
// without rowtime attribute for now. DROP VIEW IF EXISTS auction;
// DROP VIEW IF EXISTS bid;
// CREATE VIEW auction AS SELECT auction.* FROM ${NEXMARK_TABLE} WHERE
// event_type = 1; CREATE VIEW bid AS SELECT bid.* FROM ${NEXMARK_TABLE} WHERE
// event_type = 2;
//
// CREATE TABLE discard_sink (
//     auction  BIGINT,
//     bidder  BIGINT,
//     price  BIGINT,
//     channel  VARCHAR,
//     url  VARCHAR,
//     bid_dateTime  TIMESTAMP(3),
//     bid_extra  VARCHAR,
//
//     itemName  VARCHAR,
//     description  VARCHAR,
//     initialBid  BIGINT,
//     reserve  BIGINT,
//     auction_dateTime  TIMESTAMP(3),
//     expires  TIMESTAMP(3),
//     seller  BIGINT,
//     category  BIGINT,
//     auction_extra  VARCHAR
// ) WITH (
//     'connector' = 'blackhole'
// );
//
// INSERT INTO discard_sink
// SELECT
//     auction, bidder, price, channel, url, B.dateTime, B.extra,
//     itemName, description, initialBid, reserve, A.dateTime, expires, seller,
// category, A.extra FROM
//     bid AS B INNER JOIN auction AS A on B.auction = A.id
// WHERE A.category = 10;
//

type Q20Stream = Stream<RootCircuit, OrdZSet<Tup2<Bid, Auction>>>;

const FILTERED_CATEGORY: u64 = 10;

pub fn q20(input: NexmarkStream) -> Q20Stream {
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, b.clone())),
        _ => None,
    });

    let auctions_indexed = input.flat_map_index(|event| match event {
        Event::Auction(a) => match a.category {
            FILTERED_CATEGORY => Some((a.id, a.clone())),
            _ => None,
        },
        _ => None,
    });

    bids_by_auction.join(&auctions_indexed, |_, bid, auction| {
        Tup2(bid.clone(), auction.clone())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        generator::tests::{make_auction, make_bid},
        model::{Auction, Bid},
    };
    use dbsp::zset;
    use rstest::rstest;

    #[rstest]
    #[case::auction_bids_single_auction(
        vec![vec![
            Event::Auction(Auction {
                id: 1,
                category: FILTERED_CATEGORY,
                ..make_auction()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 10,
                price: 10,
                ..make_bid()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 20,
                price: 20,
                ..make_bid()
            }),
            // This bid should not appear anywhere as the auction does not exist.
            Event::Bid(Bid {
                auction: 2,
                bidder: 50,
                price: 50,
                ..make_bid()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 30,
                price: 30,
                ..make_bid()
            }),
        ], vec![
            Event::Bid(Bid {
                auction: 1,
                bidder: 40,
                price: 40,
                ..make_bid()
            }),
        ]],
        vec![zset! {
            Tup2(Bid { auction: 1, bidder: 10, price: 10, ..make_bid()}, Auction { id: 1, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
            Tup2(Bid { auction: 1, bidder: 20, price: 20, ..make_bid()}, Auction { id: 1, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
            Tup2(Bid { auction: 1, bidder: 30, price: 30, ..make_bid()}, Auction { id: 1, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
        }, zset! {
            Tup2(Bid { auction: 1, bidder: 40, price: 40, ..make_bid()}, Auction { id: 1, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
        }])]
    #[case::auction_bids_wrong_category(
        vec![vec![
            Event::Auction(Auction {
                id: 1,
                category: 9,
                ..make_auction()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 10,
                price: 10,
                ..make_bid()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 20,
                price: 20,
                ..make_bid()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 30,
                price: 30,
                ..make_bid()
            }),
        ], vec![
            Event::Bid(Bid {
                auction: 1,
                bidder: 40,
                price: 40,
                ..make_bid()
            }),
        ]],
        vec![zset! {}, zset! {}])]
    #[case::auction_bids_multiple_auctions(
        vec![vec![
            Event::Auction(Auction {
                id: 1,
                category: FILTERED_CATEGORY,
                ..make_auction()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 10,
                price: 10,
                ..make_bid()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 20,
                price: 20,
                ..make_bid()
            }),
            Event::Auction(Auction {
                id: 2,
                category: FILTERED_CATEGORY,
                ..make_auction()
            }),
            Event::Bid(Bid {
                auction: 2,
                bidder: 50,
                price: 50,
                ..make_bid()
            }),
            Event::Bid(Bid {
                auction: 1,
                bidder: 30,
                price: 30,
                ..make_bid()
            }),
        ], vec![
            Event::Bid(Bid {
                auction: 1,
                bidder: 40,
                price: 40,
                ..make_bid()
            }),
            Event::Bid(Bid {
                auction: 2,
                bidder: 60,
                price: 60,
                ..make_bid()
            }),
        ]],
        vec![zset! {
            Tup2(Bid { auction: 1, bidder: 10, price: 10, ..make_bid()}, Auction { id: 1, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
            Tup2(Bid { auction: 1, bidder: 20, price: 20, ..make_bid()}, Auction { id: 1, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
            Tup2(Bid { auction: 1, bidder: 30, price: 30, ..make_bid()}, Auction { id: 1, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
            Tup2(Bid { auction: 2, bidder: 50, price: 50, ..make_bid()}, Auction { id: 2, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
        }, zset! {
            Tup2(Bid { auction: 1, bidder: 40, price: 40, ..make_bid()}, Auction { id: 1, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
            Tup2(Bid { auction: 2, bidder: 60, price: 60, ..make_bid()}, Auction { id: 2, category: FILTERED_CATEGORY, ..make_auction() }) => 1,
        }])]
    fn test_q20(
        #[case] input_event_batches: Vec<Vec<Event>>,
        #[case] expected_zsets: Vec<OrdZSet<Tup2<Bid, Auction>>>,
    ) {
        let input_vecs = input_event_batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|e| Tup2(e, 1)).collect());

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q20(stream);

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
