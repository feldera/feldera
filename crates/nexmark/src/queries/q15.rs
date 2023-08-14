use super::NexmarkStream;
use dbsp::{
    operator::FilterMap,
    RootCircuit, OrdIndexedZSet, OrdZSet, Stream,
};
use rkyv::{Archive, Serialize, Deserialize};
use crate::{model::Event, queries::OrdinalDate};
use size_of::SizeOf;
use std::{
    hash::Hash,
    time::{Duration, SystemTime},
};
use time::{
    format_description::well_known::{iso8601, iso8601::FormattedComponents, Iso8601},
    Date, OffsetDateTime,
};

/// Query 15: Bidding Statistics Report (Not in original suite)
///
/// How many distinct users join the bidding for different level of price?
/// Illustrates multiple distinct aggregations with filters.
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

#[derive(Eq, Clone, SizeOf, Debug, Default, Hash, PartialEq, PartialOrd, Ord, Archive, Serialize, Deserialize)]
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

type Q15Stream = Stream<RootCircuit, OrdZSet<Q15Output, isize>>;

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
    let bids = input.flat_map(|event| match event {
        Event::Bid(b) => {
            let date_time = SystemTime::UNIX_EPOCH + Duration::from_millis(b.date_time);

            let day = <SystemTime as Into<OffsetDateTime>>::into(date_time)
                .date()
                .to_ordinal_date();
            //.format(iso8601_day_format)
            //.unwrap();
            Some((day, (b.auction, b.price, b.bidder)))
        }
        _ => None,
    });

    // Partition bids based on price.
    let rank1_bids = bids.filter(|(_day, (_auction, price, _bidder))| *price < 10_000);
    let rank2_bids =
        bids.filter(|(_day, (_auction, price, _bidder))| *price >= 10_000 && *price < 1_000_000);
    let rank3_bids = bids.filter(|(_day, (_auction, price, _bidder))| *price >= 1_000_000);

    // Compute unique bidders across all bids and for each price range.
    let distinct_bidder = bids
        .map(|(day, (_auction, _price, bidder))| (*day, *bidder))
        .distinct()
        .index();
    let rank1_distinct_bidder = rank1_bids
        .map(|(day, (_auction, _price, bidder))| (*day, *bidder))
        .distinct()
        .index();
    let rank2_distinct_bidder = rank2_bids
        .map(|(day, (_auction, _price, bidder))| (*day, *bidder))
        .distinct()
        .index();
    let rank3_distinct_bidder = rank3_bids
        .map(|(day, (_auction, _price, bidder))| (*day, *bidder))
        .distinct()
        .index();

    // Compute unique auctions across all bids and for each price range.
    let distinct_auction = bids
        .map(|(day, (auction, _price, _bidder))| (*day, *auction))
        .distinct()
        .index();
    let rank1_distinct_auction = rank1_bids
        .map(|(day, (auction, _price, _bidder))| (*day, *auction))
        .distinct()
        .index();
    let rank2_distinct_auction = rank2_bids
        .map(|(day, (auction, _price, _bidder))| (*day, *auction))
        .distinct()
        .index();
    let rank3_distinct_auction = rank3_bids
        .map(|(day, (auction, _price, _bidder))| (*day, *auction))
        .distinct()
        .index();

    // Compute bids per day.
    let count_total_bids: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> = bids
        .index()
        .weighted_count();
    let count_rank1_bids: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> = rank1_bids
        .index()
        .weighted_count();
    let count_rank2_bids: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> = rank2_bids
        .index()
        .weighted_count();
    let count_rank3_bids: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> = rank3_bids
        .index()
        .weighted_count();

    // Count unique bidders per day.
    let count_total_bidders: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> =
        distinct_bidder.weighted_count();
    let count_rank1_bidders: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> =
        rank1_distinct_bidder.weighted_count();
    let count_rank2_bidders: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> =
        rank2_distinct_bidder.weighted_count();
    let count_rank3_bidders: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> =
        rank3_distinct_bidder.weighted_count();

    // Count unique auctions per day.
    let count_total_auctions: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> =
        distinct_auction.weighted_count();
    let count_rank1_auctions: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> =
        rank1_distinct_auction.weighted_count();
    let count_rank2_auctions: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> =
        rank2_distinct_auction.weighted_count();
    let count_rank3_auctions: Stream<_, OrdIndexedZSet<OrdinalDate, isize, _>> =
        rank3_distinct_auction.weighted_count();

    // The following abomination simply joins all aggregates computed above into a
    // single output stream.
    count_total_bids
        .outer_join_default(&count_rank1_bids, |date, total_bids, rank1_bids| {
            (*date, (*total_bids, *rank1_bids))
        })
        .index()
        .outer_join_default(
            &count_rank2_bids,
            |date, (total_bids, rank1_bids), rank2_bids| {
                (*date, (*total_bids, *rank1_bids, *rank2_bids))
            },
        )
        .index()
        .outer_join_default(
            &count_rank3_bids,
            |date, (total_bids, rank1_bids, rank2_bids), rank3_bids| {
                (*date, (*total_bids, *rank1_bids, *rank2_bids, *rank3_bids))
            },
        )
        .index()
        .outer_join_default(
            &count_total_bidders,
            |date, (total_bids, rank1_bids, rank2_bids, rank3_bids), total_bidders| {
                (
                    *date,
                    (
                        *total_bids,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                        *total_bidders,
                    ),
                )
            },
        )
        .index()
        .outer_join_default(
            &count_rank1_bidders,
            |date,
             (total_bids, rank1_bids, rank2_bids, rank3_bids, total_bidders),
             rank1_bidders| {
                (
                    *date,
                    (
                        *total_bids,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                        *total_bidders,
                        *rank1_bidders,
                    ),
                )
            },
        )
        .index()
        .outer_join_default(
            &count_rank2_bidders,
            |date,
             (total_bids, rank1_bids, rank2_bids, rank3_bids, total_bidders, rank1_bidders),
             rank2_bidders| {
                (
                    *date,
                    (
                        *total_bids,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                        *total_bidders,
                        *rank1_bidders,
                        *rank2_bidders,
                    ),
                )
            },
        )
        .index()
        .outer_join_default(
            &count_rank3_bidders,
            |date,
             (
                total_bids,
                rank1_bids,
                rank2_bids,
                rank3_bids,
                total_bidders,
                rank1_bidders,
                rank2_bidders,
            ),
             rank3_bidders| {
                (
                    *date,
                    (
                        *total_bids,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                        *total_bidders,
                        *rank1_bidders,
                        *rank2_bidders,
                        *rank3_bidders,
                    ),
                )
            },
        )
        .index()
        .outer_join_default(
            &count_total_auctions,
            |date,
             (
                total_bids,
                rank1_bids,
                rank2_bids,
                rank3_bids,
                total_bidders,
                rank1_bidders,
                rank2_bidders,
                rank3_bidders,
            ),
             total_auctions| {
                (
                    *date,
                    (
                        *total_bids,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                        *total_bidders,
                        *rank1_bidders,
                        *rank2_bidders,
                        *rank3_bidders,
                        *total_auctions,
                    ),
                )
            },
        )
        .index()
        .outer_join_default(
            &count_rank1_auctions,
            |date,
             (
                total_bids,
                rank1_bids,
                rank2_bids,
                rank3_bids,
                total_bidders,
                rank1_bidders,
                rank2_bidders,
                rank3_bidders,
                total_auctions,
            ),
             rank1_auctions| {
                (
                    *date,
                    (
                        *total_bids,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                        *total_bidders,
                        *rank1_bidders,
                        *rank2_bidders,
                        *rank3_bidders,
                        *total_auctions,
                        *rank1_auctions,
                    ),
                )
            },
        )
        .index()
        .outer_join_default(
            &count_rank2_auctions,
            |date,
             (
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
            ),
             rank2_auctions| {
                (
                    *date,
                    (
                        (
                            *total_bids,
                            *rank1_bids,
                            *rank2_bids,
                            *rank3_bids,
                            *total_bidders,
                            *rank1_bidders,
                            *rank2_bidders,
                            *rank3_bidders,
                            *total_auctions,
                            *rank1_auctions,
                        ),
                        *rank2_auctions,
                    ),
                )
            },
        )
        .index()
        .outer_join_default(
            &count_rank3_auctions,
            |date,
             (
                (
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
                ),
                rank2_auctions,
            ),
             rank3_auctions| Q15Output {
                day: Date::from_ordinal_date(date.0, date.1)
                    .unwrap()
                    .format(iso8601_day_format)
                    .unwrap(),
                total_bids: *total_bids as usize,
                rank1_bids: *rank1_bids as usize,
                rank2_bids: *rank2_bids as usize,
                rank3_bids: *rank3_bids as usize,
                total_bidders: *total_bidders as usize,
                rank1_bidders: *rank1_bidders as usize,
                rank2_bidders: *rank2_bidders as usize,
                rank3_bidders: *rank3_bidders as usize,
                total_auctions: *total_auctions as usize,
                rank1_auctions: *rank1_auctions as usize,
                rank2_auctions: *rank2_auctions as usize,
                rank3_auctions: *rank3_auctions as usize,
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp::{
        zset, Runtime,
    };
    use crate::{generator::tests::make_bid, model::Bid};
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

        let (mut dbsp, input_handle) = Runtime::init_circuit(num_threads, move |circuit| {
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

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            dbsp.step().unwrap();
        }
    }
}
