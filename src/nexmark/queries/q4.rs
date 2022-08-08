use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

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

type Q4Stream = Stream<Circuit<()>, OrdZSet<(usize, usize), isize>>;

pub fn q4(input: NexmarkStream) -> Q4Stream {
    // Select auctions and index by auction id.
    let auctions_by_id = input.flat_map_index(|event| match event {
        Event::Auction(a) => Some((a.id, (a.category, a.date_time, a.expires))),
        _ => None,
    });

    // Select bids and index by auction id.
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, (b.price, b.date_time))),
        _ => None,
    });

    type BidsAuctionsJoin =
        Stream<Circuit<()>, OrdZSet<((u64, usize, u64, u64), (usize, u64)), isize>>;

    // Join to get bids for each auction.
    let bids_for_auctions: BidsAuctionsJoin = auctions_by_id.join::<(), _, _, _>(
        &bids_by_auction,
        |&auction_id, &(category, a_date_time, a_expires), &(bid_price, bid_date_time)| {
            (
                (auction_id, category, a_date_time, a_expires),
                (bid_price, bid_date_time),
            )
        },
    );

    // Filter out the invalid bids while indexing.
    // TODO: update to use incremental version of `join_range` once implemented
    // (#137).
    let bids_for_auctions_indexed = bids_for_auctions.flat_map_index(
        |&((auction_id, category, a_date_time, a_expires), (bid_price, bid_date_time))| {
            if bid_date_time >= a_date_time && bid_date_time <= a_expires {
                Some(((auction_id, category), bid_price))
            } else {
                None
            }
        },
    );

    // winning_bids_by_category: once we have the winning bids, we don't
    // need the auction ids anymore.
    // TODO: We can optimize this given that there are no deletions, as DBSP
    // doesn't need to keep records of the bids for future max calculations.
    let winning_bids_by_category: Stream<Circuit<()>, OrdZSet<(usize, usize), isize>> =
        bids_for_auctions_indexed.aggregate_incremental(|&key, vals| -> (usize, usize) {
            // `vals` is sorted in ascending order for each key, so we can
            // just grab the last one.
            let (&max, _) = vals.last().unwrap();
            (key.1, max)
        });
    let winning_bids_by_category_indexed = winning_bids_by_category.index();

    // Finally, calculate the average winning bid per category.
    // TODO: use linear aggregation when ready (#138).
    winning_bids_by_category_indexed.aggregate_incremental(|&key, vals| -> (usize, usize) {
        let num_items = vals.len();
        let sum = vals.drain(..).map(|(bid, _)| bid).sum::<usize>();
        (key, sum / num_items)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexmark::{
        generator::{
            tests::{make_auction, make_bid, make_next_event, CannedEventGenerator},
            NextEvent,
        },
        model::{Auction, Bid, Event},
        NexmarkSource,
    };
    use crate::{circuit::Root, trace::ord::OrdZSet, trace::Batch};
    use rand::rngs::mock::StepRng;
    use std::sync::mpsc;

    #[test]
    fn test_q4_average_final_bids_per_category() {
        let canned_events: Vec<NextEvent> = vec![
            // Auction 1 is open between time 1000 and 2000.
            NextEvent {
                event: Event::Auction(Auction {
                    id: 1,
                    category: 1,
                    date_time: 1000,
                    expires: 2000,
                    ..make_auction()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Auction(Auction {
                    id: 2,
                    category: 1,
                    ..make_auction()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Auction(Auction {
                    id: 3,
                    category: 2,
                    ..make_auction()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Bid(Bid {
                    auction: 1,
                    date_time: 1100,
                    price: 80,
                    ..make_bid()
                }),
                ..make_next_event()
            },
            // Winning bid for auction 1 (category 1).
            NextEvent {
                event: Event::Bid(Bid {
                    price: 100,
                    auction: 1,
                    date_time: 1500,
                    ..make_bid()
                }),
                ..make_next_event()
            },
            // This bid would have one but isn't included as it came in too late.
            NextEvent {
                event: Event::Bid(Bid {
                    price: 500,
                    auction: 1,
                    date_time: 2500,
                    ..make_bid()
                }),
                ..make_next_event()
            },
            // Max bid for auction 2 (category 1).
            NextEvent {
                event: Event::Bid(Bid {
                    price: 300,
                    auction: 2,
                    ..make_bid()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Bid(Bid {
                    price: 200,
                    auction: 2,
                    ..make_bid()
                }),
                ..make_next_event()
            },
            // Only bid for auction 3 (category 2).
            NextEvent {
                event: Event::Bid(Bid {
                    price: 20,
                    auction: 3,
                    ..make_bid()
                }),
                ..make_next_event()
            },
        ];

        let (tx, _) = mpsc::channel();
        let source: NexmarkSource<StepRng, isize, OrdZSet<Event, isize>> =
            NexmarkSource::from_generator(CannedEventGenerator::new(canned_events), tx);

        // Winning bids for auctions in category 1 are 100 and 300 - ie. AVG of 200
        // Winning (only) bid for auction in category 2 is 20.
        let root = Root::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q4(input);

            output.inspect(move |e| {
                assert_eq!(
                    e,
                    &OrdZSet::from_tuples((), vec![(((1, 200), ()), 1), (((2, 20), ()), 1),])
                )
            });
        })
        .unwrap();

        root.step().unwrap();
    }
}
