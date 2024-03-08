use super::NexmarkStream;
use crate::model::Event;
use dbsp::{
    operator::Max,
    utils::{Tup2, Tup3},
    OrdIndexedZSet, OrdZSet, RootCircuit, Stream, ZWeight,
};

/// Query 4: Average Price for a Category
///
/// Select the average of the wining bid prices for all auctions in each
/// category. Illustrates complex join and aggregation.
///
/// From [Nexmark q4.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q4.sql):
///
/// ```sql
/// DROP VIEW IF EXISTS auction;
/// DROP VIEW IF EXISTS bid;
/// CREATE VIEW auction AS SELECT auction.* FROM ${NEXMARK_TABLE} WHERE event_type = 1;
/// CREATE VIEW bid AS SELECT bid.* FROM ${NEXMARK_TABLE} WHERE event_type = 2;
///
/// CREATE TABLE discard_sink (
///   id BIGINT,
///   final BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     Q.category,
///     AVG(Q.final)
/// FROM (
///     SELECT MAX(B.price) AS final, A.category
///     FROM auction A, bid B
///     WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
///     GROUP BY A.id, A.category
/// ) Q
/// GROUP BY Q.category;
/// ```

type Q4Stream = Stream<RootCircuit, OrdZSet<Tup2<u64, u64>>>;

pub fn q4(input: NexmarkStream) -> Q4Stream {
    // Select auctions and index by auction id.
    let auctions_by_id = input.flat_map_index(|event| match event {
        Event::Auction(a) => Some((a.id, Tup3(a.category, a.date_time, a.expires))),
        _ => None,
    });

    // Select bids and index by auction id.
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, Tup2(b.price, b.date_time))),
        _ => None,
    });

    // Join to get bids for each auction.
    // Filter out the invalid bids while indexing.
    let bids_for_auctions_indexed = auctions_by_id.join_index(
        &bids_by_auction,
        |&auction_id, &Tup3(category, a_date_time, a_expires), &Tup2(bid_price, bid_date_time)| {
            if bid_date_time >= a_date_time && bid_date_time <= a_expires {
                Some((Tup2(auction_id, category), bid_price))
            } else {
                None
            }
        },
    );

    // winning_bids_by_category: once we have the winning bids, we don't
    // need the auction ids anymore.
    // TODO: We can optimize this given that there are no deletions, as DBSP
    // doesn't need to keep records of the bids for future max calculations.
    let winning_bids: Stream<RootCircuit, OrdIndexedZSet<Tup2<u64, u64>, u64>> =
        bids_for_auctions_indexed.aggregate(Max);
    let winning_bids_by_category_indexed =
        winning_bids.map_index(|(Tup2(_, category), winning_bid)| (*category, *winning_bid));

    // Finally, calculate the average winning bid per category.
    // TODO: use linear aggregation when ready (#138).
    winning_bids_by_category_indexed
        .average(|val| *val as ZWeight)
        .map(|(category, avg): (&u64, &ZWeight)| Tup2(*category, *avg as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        generator::tests::{make_auction, make_bid},
        model::{Auction, Bid, Event},
    };
    use dbsp::{OrdZSet, RootCircuit};

    #[test]
    fn test_q4_average_final_bids_per_category() {
        let input_vecs: Vec<Vec<Tup2<Event, ZWeight>>> = vec![
            vec![
                Tup2(
                    Event::Auction(Auction {
                        id: 1,
                        category: 1,
                        date_time: 1000,
                        expires: 2000,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 2,
                        category: 1,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 3,
                        category: 2,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 1100,
                        price: 80,
                        ..make_bid()
                    }),
                    1,
                ),
                // Winning bid for auction 1 (category 1).
                Tup2(
                    Event::Bid(Bid {
                        price: 100,
                        auction: 1,
                        date_time: 1500,
                        ..make_bid()
                    }),
                    1,
                ),
                // This bid would have one but isn't included as it came in too late.
                Tup2(
                    Event::Bid(Bid {
                        price: 500,
                        auction: 1,
                        date_time: 2500,
                        ..make_bid()
                    }),
                    1,
                ),
                // Max bid for auction 2 (category 1).
                Tup2(
                    Event::Bid(Bid {
                        price: 300,
                        auction: 2,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        price: 200,
                        auction: 2,
                        ..make_bid()
                    }),
                    1,
                ),
                // Only bid for auction 3 (category 2)
                Tup2(
                    Event::Bid(Bid {
                        price: 20,
                        auction: 3,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            vec![
                // Another bid for auction 3 that should update the winning bid for category 2.
                Tup2(
                    Event::Bid(Bid {
                        price: 30,
                        auction: 3,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            vec![
                // Another auction with a single winning bid in category 2.
                Tup2(
                    Event::Auction(Auction {
                        id: 4,
                        category: 2,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        price: 60,
                        auction: 4,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
        ];

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q4(stream);

            let mut expected_output = vec![
                OrdZSet::from_keys((), vec![Tup2(Tup2(1, 200), 1), Tup2(Tup2(2, 20), 1)]),
                // The winning bid for auction 3 (only auction in category 2) updates the
                // average (of the single auction) to 30.
                OrdZSet::from_keys((), vec![Tup2(Tup2(2, 20), -1), Tup2(Tup2(2, 30), 1)]),
                // The average for category 2 is now 30 + 60 / 2 = 45.
                OrdZSet::from_keys((), vec![Tup2(Tup2(2, 30), -1), Tup2(Tup2(2, 45), 1)]),
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
