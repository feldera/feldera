use super::NexmarkStream;
use crate::{
    nexmark::model::Event,
    operator::{FilterMap, Fold},
    Circuit, OrdZSet, Stream,
};
use deepsize::DeepSizeOf;
use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};
use time::{
    format_description::well_known::{iso8601, iso8601::FormattedComponents, Iso8601},
    OffsetDateTime,
};

/// Query 15: Bidding Statistics Report (Not in original suite)
///
/// How many distinct users join the bidding for different level of price?
/// Illustrates multiple distinct aggregations with filters.
///
/// ```sql
/// CREATE TABLE discard_sink (
///   `day` VARCHAR,
///   total_bids BIGINT,
///   rank1_bids BIGINT,
///   rank2_bids BIGINT,
///   rank3_bids BIGINT,
///   total_bidders BIGINT,
///   rank1_bidders BIGINT,
///   rank2_bidders BIGINT,
///   rank3_bidders BIGINT,
///   total_auctions BIGINT,
///   rank1_auctions BIGINT,
///   rank2_auctions BIGINT,
///   rank3_auctions BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///      DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
///      count(*) AS total_bids,
///      count(*) filter (where price < 10000) AS rank1_bids,
///      count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
///      count(*) filter (where price >= 1000000) AS rank3_bids,
///      count(distinct bidder) AS total_bidders,
///      count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
///      count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
///      count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
///      count(distinct auction) AS total_auctions,
///      count(distinct auction) filter (where price < 10000) AS rank1_auctions,
///      count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
///      count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
/// FROM bid
/// GROUP BY DATE_FORMAT(dateTime, 'yyyy-MM-dd');
/// ```

#[derive(Eq, Clone, DeepSizeOf, Debug, Default, Hash, PartialEq, PartialOrd, Ord)]
pub struct Q15Output {
    day: String,
    total_bids: usize,
    rank1_bids: usize,
    rank2_bids: usize,
    rank3_bids: usize,
    total_bidders: usize,
    rank1_bidders: usize,
    rank2_bidders: usize,
    rank3_bidders: usize,
    total_auctions: usize,
    rank1_auctions: usize,
    rank2_auctions: usize,
    rank3_auctions: usize,
}

type Q15Stream = Stream<Circuit<()>, OrdZSet<Q15Output, isize>>;

pub fn q15(input: NexmarkStream) -> Q15Stream {
    // Dug for a long time to figure out how to use the const generics
    // for time formats, not well documented in docs themselves, but
    // great examples in the integration tests:
    // https://github.com/time-rs/time/blob/6894ec115a8fdb7baf105604bdcc0902dc941e9c/tests/integration/formatting.rs#L118
    let iso8601_day_format = &Iso8601::<
        {
            iso8601::Config::DEFAULT
                .set_formatted_components(FormattedComponents::Date)
                .encode()
        },
    >;

    // Group/index and aggregate by day - keeping only the price, bidder, auction
    let bids_indexed = input.flat_map_index(|event| match event {
        Event::Bid(b) => {
            let date_time = SystemTime::UNIX_EPOCH + Duration::from_millis(b.date_time);

            let day = <SystemTime as Into<OffsetDateTime>>::into(date_time)
                .format(iso8601_day_format)
                .unwrap();
            Some((day, (b.auction, b.price, b.bidder)))
        }
        _ => None,
    });

    // Not sure of the best way to calculate the various distinct bidders and
    // auctions.
    // I've initially use HashSets to count each while already dealing
    // with the bids for the day, but probably should instead use filter operations
    // like the original.
    type Accumulator = (
        usize,
        usize,
        usize,
        usize,
        HashSet<u64>,
        HashSet<u64>,
        HashSet<u64>,
        HashSet<u64>,
        HashSet<u64>,
        HashSet<u64>,
        HashSet<u64>,
        HashSet<u64>,
    );

    let bids_per_day = bids_indexed.aggregate::<(), _>(Fold::with_output(
        (
            0,
            0,
            0,
            0,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
        ),
        |(
            total_bids,
            rank1_bids,
            rank2_bids,
            rank3_bids,
            total_bidders,
            rank1_bidders,
            rank2_bidders,
            rank3_bidders,
            total_auctions,
            rank1_auctions,
            rank2_auctions,
            rank3_auctions,
        ): &mut Accumulator,
         (auction, price, bidder): &(u64, usize, u64),
         _w| {
            *total_bids += 1;
            total_bidders.insert(*bidder);
            total_auctions.insert(*auction);
            if *price < 10_000 {
                *rank1_bids += 1;
                rank1_bidders.insert(*bidder);
                rank1_auctions.insert(*auction);
            } else if *price < 1_000_000 {
                *rank2_bids += 1;
                rank2_bidders.insert(*bidder);
                rank2_auctions.insert(*auction);
            } else if *price >= 1_000_000 {
                *rank3_bids += 1;
                rank3_bidders.insert(*bidder);
                rank3_auctions.insert(*auction);
            }
        },
        |(
            total_bids,
            rank1_bids,
            rank2_bids,
            rank3_bids,
            total_bidders,
            rank1_bidders,
            rank2_bidders,
            rank3_bidders,
            total_auctions,
            rank1_auctions,
            rank2_auctions,
            rank3_auctions,
        ): Accumulator| {
            Q15Output {
                day: String::new(),
                total_bids,
                rank1_bids,
                rank2_bids,
                rank3_bids,
                total_bidders: total_bidders.len(),
                rank1_bidders: rank1_bidders.len(),
                rank2_bidders: rank2_bidders.len(),
                rank3_bidders: rank3_bidders.len(),
                total_auctions: total_auctions.len(),
                rank1_auctions: rank1_auctions.len(),
                rank2_auctions: rank2_auctions.len(),
                rank3_auctions: rank3_auctions.len(),
            }
        },
    ));

    bids_per_day.map(|(day, output)| Q15Output {
        day: day.clone(),
        ..*output
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nexmark::{generator::tests::make_bid, model::Bid},
        zset, Runtime,
    };
    use rstest::rstest;

    // 52 years with 13 leap years (extra days)
    const MILLIS_2022_01_01: u64 = (52 * 365 + 13) * 24 * 60 * 60 * 1000;

    // Note: I really would have liked to test each piece of functionality in a
    // separate unit test, but I can't iterate over a batch to compare just the
    // fields that would have been of interest to that test, afaict, so comparing
    // the complete expected output is what I've done as usual. Let me know if
    // there's a way.
    #[rstest]
    #[case::single_threaded(1)]
    #[case::multi_threaded_2_threads(2)]
    #[case::multi_threaded_4_threads(4)]
    fn test_q15(#[case] num_threads: usize) {
        let input_vecs = vec![
            vec![(
                Event::Bid(Bid {
                    // Right on 1970 epoch
                    date_time: 0,
                    auction: 1,
                    bidder: 1,
                    ..make_bid()
                }),
                1,
            )],
            vec![
                (
                    Event::Bid(Bid {
                        // Six minutes after epoch
                        date_time: 1000 * 6,
                        // Rank 2 bid
                        price: 10_001,
                        auction: 2,
                        bidder: 1,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        // One millisecond before next day
                        date_time: 24 * 60 * 60 * 1000 - 1,
                        // Rank 3 bid
                        price: 1_000_001,
                        auction: 3,
                        bidder: 2,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        // One millisecond into Jan 2 1970
                        date_time: 24 * 60 * 60 * 1000 + 1,
                        // Rank 3 bid
                        auction: 3,
                        bidder: 3,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        // One millisecond into Jan 3 1970
                        date_time: 2 * 24 * 60 * 60 * 1000 + 1,
                        // Rank 3 bid
                        auction: 3,
                        bidder: 4,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        // One millisecond into Jan 4 1970
                        date_time: 3 * 24 * 60 * 60 * 1000 + 1,
                        // Rank 3 bid
                        auction: 3,
                        bidder: 5,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        // One millisecond into Jan 5 1970
                        date_time: 4 * 24 * 60 * 60 * 1000 + 1,
                        // Rank 3 bid
                        auction: 3,
                        bidder: 2,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            vec![(
                Event::Bid(Bid {
                    date_time: MILLIS_2022_01_01,
                    auction: 4,
                    ..make_bid()
                }),
                1,
            )],
        ]
        .into_iter();

        let (mut dbsp, mut input_handle) = Runtime::init_circuit(num_threads, move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let mut expected_output = vec![
                zset![
                    Q15Output {
                        day: String::from("1970-01-01"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q15Output::default()
                    } => 1,
                ],
                zset![
                    Q15Output {
                        day: String::from("1970-01-01"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q15Output::default()
                    } => -1,
                    Q15Output {
                        day: String::from("1970-01-01"),
                        total_bids: 3,
                        rank1_bids: 1,
                        rank2_bids: 1,
                        rank3_bids: 1,
                        total_bidders: 2,
                        rank1_bidders: 1,
                        rank2_bidders: 1,
                        rank3_bidders: 1,
                        total_auctions: 3,
                        rank1_auctions: 1,
                        rank2_auctions: 1,
                        rank3_auctions: 1,
                        ..Q15Output::default()
                    } => 1,
                    Q15Output {
                        day: String::from("1970-01-02"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q15Output::default()
                    } => 1,
                    Q15Output {
                        day: String::from("1970-01-03"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q15Output::default()
                    } => 1,
                    Q15Output {
                        day: String::from("1970-01-04"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q15Output::default()
                    } => 1,
                    Q15Output {
                        day: String::from("1970-01-05"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q15Output::default()
                    } => 1,
                ],
                zset![
                    Q15Output {
                        day: String::from("2022-01-01"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q15Output::default()
                    } => 1,
                ],
            ]
            .into_iter();

            let output = q15(stream);

            output.gather(0).inspect(move |batch| {
                if Runtime::worker_index() == 0 {
                    assert_eq!(batch, &expected_output.next().unwrap())
                }
            });

            input_handle
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            dbsp.step().unwrap();
        }
    }
}
