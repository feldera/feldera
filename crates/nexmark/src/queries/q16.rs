use super::NexmarkStream;
use crate::{model::Event, queries::OrdinalDate};
use dbsp::{
    operator::Max,
    utils::{Tup10, Tup2, Tup3, Tup4, Tup5, Tup6, Tup7, Tup8, Tup9},
    OrdIndexedZSet, OrdZSet, RootCircuit, Stream, ZWeight,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    hash::Hash,
    time::{Duration, SystemTime},
};
use time::{
    format_description::{
        self,
        well_known::{iso8601, iso8601::FormattedComponents, Iso8601},
    },
    Date, OffsetDateTime, Time,
};

///
/// Query 16: Channel Statistics Report (Not in original suite)
///
/// How many distinct users join the bidding for different level of price for a
/// channel? Illustrates multiple distinct aggregations with filters for
/// multiple keys.
///
///
/// ```sql
/// CREATE TABLE discard_sink (
///     channel VARCHAR,
///     `day` VARCHAR,
///     `minute` VARCHAR,
///     total_bids BIGINT,
///     rank1_bids BIGINT,
///     rank2_bids BIGINT,
///     rank3_bids BIGINT,
///     total_bidders BIGINT,
///     rank1_bidders BIGINT,
///     rank2_bidders BIGINT,
///     rank3_bidders BIGINT,
///     total_auctions BIGINT,
///     rank1_auctions BIGINT,
///     rank2_auctions BIGINT,
///     rank3_auctions BIGINT
/// ) WITH (
///     'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     channel,
///     DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
///     max(DATE_FORMAT(dateTime, 'HH:mm')) as `minute`,
///     count(*) AS total_bids,
///     count(*) filter (where price < 10000) AS rank1_bids,
///     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
///     count(*) filter (where price >= 1000000) AS rank3_bids,
///     count(distinct bidder) AS total_bidders,
///     count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
///     count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
///     count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
///     count(distinct auction) AS total_auctions,
///     count(distinct auction) filter (where price < 10000) AS rank1_auctions,
///     count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
///     count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
/// FROM bid
/// GROUP BY channel, DATE_FORMAT(dateTime, 'yyyy-MM-dd');
/// ```

#[derive(
    Eq,
    Clone,
    Debug,
    Default,
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
pub struct Q16Output {
    channel: String,
    day: String,
    minute: String,
    total_bids: u64,
    rank1_bids: u64,
    rank2_bids: u64,
    rank3_bids: u64,
    total_bidders: u64,
    rank1_bidders: u64,
    rank2_bidders: u64,
    rank3_bidders: u64,
    total_auctions: u64,
    rank1_auctions: u64,
    rank2_auctions: u64,
    rank3_auctions: u64,
}

type Q16Stream = Stream<RootCircuit, OrdZSet<Q16Output>>;

#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    Hash,
    PartialEq,
    PartialOrd,
    Ord,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct Q16Intermediate1(
    ZWeight,
    Tup2<u8, u8>,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
);

#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    Hash,
    PartialEq,
    PartialOrd,
    Ord,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct Q16Intermediate2(
    ZWeight,
    Tup2<u8, u8>,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
    ZWeight,
);

pub fn q16(input: NexmarkStream) -> Q16Stream {
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

    // Group/index and aggregate by (channel, day) - keeping only the price, bidder,
    // auction, and remaining millis for the day.
    let bids = input.flat_map(|event| match event {
        Event::Bid(b) => {
            let date_time = SystemTime::UNIX_EPOCH + Duration::from_millis(b.date_time);

            let date_time = <SystemTime as Into<OffsetDateTime>>::into(date_time);
            let day = date_time.date().to_ordinal_date();
            let time = date_time.time();
            Some(Tup2(
                Tup2(b.channel.to_string(), day),
                Tup4(
                    b.auction,
                    b.price,
                    b.bidder,
                    Tup2(time.hour(), time.minute()),
                ),
            ))
        }
        _ => None,
    });

    // Partition bids based on price.
    let rank1_bids =
        bids.filter(|Tup2(_channel_day, Tup4(_auction, price, _bidder, _mins))| *price < 10_000);
    let rank2_bids = bids.filter(
        |Tup2(_channel_day, Tup4(_auction, price, _bidder, _mins))| {
            *price >= 10_000 && *price < 1_000_000
        },
    );
    let rank3_bids = bids
        .filter(|Tup2(_channel_day, Tup4(_auction, price, _bidder, _mins))| *price >= 1_000_000);

    // Compute unique bidders across all bids and for each price range.
    let distinct_bidder = bids
        .map(
            |Tup2(Tup2(channel, day), Tup4(_auction, _price, bidder, _mins))| {
                Tup2(Tup2(channel.clone(), *day), *bidder)
            },
        )
        .distinct()
        .map_index(|Tup2(k, v)| (k.clone(), *v));
    let rank1_distinct_bidder = rank1_bids
        .map(
            |Tup2(Tup2(channel, day), Tup4(_auction, _price, bidder, _mins))| {
                Tup2(Tup2(channel.clone(), *day), *bidder)
            },
        )
        .distinct()
        .map_index(|Tup2(k, v)| (k.clone(), *v));
    let rank2_distinct_bidder = rank2_bids
        .map(
            |Tup2(Tup2(channel, day), Tup4(_auction, _price, bidder, _mins))| {
                Tup2(Tup2(channel.clone(), *day), *bidder)
            },
        )
        .distinct()
        .map_index(|Tup2(k, v)| (k.clone(), *v));
    let rank3_distinct_bidder = rank3_bids
        .map(
            |Tup2(Tup2(channel, day), Tup4(_auction, _price, bidder, _mins))| {
                Tup2(Tup2(channel.clone(), *day), *bidder)
            },
        )
        .distinct()
        .map_index(|Tup2(k, v)| (k.clone(), *v));

    // Compute unique auctions across all bids and for each price range.
    let distinct_auction = bids
        .map(
            |Tup2(Tup2(channel, day), Tup4(auction, _price, _bidder, _mins))| {
                Tup2(Tup2(channel.clone(), *day), *auction)
            },
        )
        .distinct()
        .map_index(|Tup2(k, v)| (k.clone(), *v));
    let rank1_distinct_auction = rank1_bids
        .map(
            |Tup2(Tup2(channel, day), Tup4(auction, _price, _bidder, _mins))| {
                Tup2(Tup2(channel.clone(), *day), *auction)
            },
        )
        .distinct()
        .map_index(|Tup2(k, v)| (k.clone(), *v));
    let rank2_distinct_auction = rank2_bids
        .map(
            |Tup2(Tup2(channel, day), Tup4(auction, _price, _bidder, _mins))| {
                Tup2(Tup2(channel.clone(), *day), *auction)
            },
        )
        .distinct()
        .map_index(|Tup2(k, v)| (k.clone(), *v));
    let rank3_distinct_auction = rank3_bids
        .map(
            |Tup2(Tup2(channel, day), Tup4(auction, _price, _bidder, _mins))| {
                Tup2(Tup2(channel.clone(), *day), *auction)
            },
        )
        .distinct()
        .map_index(|Tup2(k, v)| (k.clone(), *v));

    // Compute bids per channel per day.
    let count_total_bids: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> = bids
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .weighted_count();
    let max_minutes = bids
        .map_index(
            |Tup2(Tup2(channel, day), Tup4(_auction, _price, _bidder, mins))| {
                (Tup2(channel.clone(), *day), *mins)
            },
        )
        .aggregate(Max);
    let count_rank1_bids: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank1_bids
            .map_index(|Tup2(k, v)| (k.clone(), *v))
            .weighted_count();
    let count_rank2_bids: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank2_bids
            .map_index(|Tup2(k, v)| (k.clone(), *v))
            .weighted_count();
    let count_rank3_bids: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank3_bids
            .map_index(|Tup2(k, v)| (k.clone(), *v))
            .weighted_count();

    // Count unique bidders per channel per day.
    let count_total_bidders: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        distinct_bidder.weighted_count();
    let count_rank1_bidders: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank1_distinct_bidder.weighted_count();
    let count_rank2_bidders: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank2_distinct_bidder.weighted_count();
    let count_rank3_bidders: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank3_distinct_bidder.weighted_count();

    // Count unique auctions per channel per day.
    let count_total_auctions: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        distinct_auction.weighted_count();
    let count_rank1_auctions: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank1_distinct_auction.weighted_count();
    let count_rank2_auctions: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank2_distinct_auction.weighted_count();
    let count_rank3_auctions: Stream<_, OrdIndexedZSet<Tup2<String, OrdinalDate>, ZWeight>> =
        rank3_distinct_auction.weighted_count();

    // The following abomination simply joins all aggregates computed above into a
    // single output stream.
    count_total_bids
        .outer_join_default(
            &max_minutes,
            |Tup2(channel, day), total_bids, max_minutes| {
                Tup2(Tup2(channel.clone(), *day), Tup2(*total_bids, *max_minutes))
            },
        )
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_rank1_bids,
            |Tup2(channel, day), Tup2(total_bids, max_minutes), rank1_bids| {
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup3(*total_bids, *max_minutes, *rank1_bids),
                )
            },
        )
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_rank2_bids,
            |Tup2(channel, day), Tup3(total_bids, max_minutes, rank1_bids), rank2_bids| {
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup4(*total_bids, *max_minutes, *rank1_bids, *rank2_bids),
                )
            },
        )
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_rank3_bids,
            |Tup2(channel, day),
             Tup4(total_bids, max_minutes, rank1_bids, rank2_bids),
             rank3_bids| {
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup5(
                        *total_bids,
                        *max_minutes,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                    ),
                )
            },
        )
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_total_bidders,
            |Tup2(channel, day),
             Tup5(total_bids, max_minutes, rank1_bids, rank2_bids, rank3_bids),
             total_bidders| {
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup6(
                        *total_bids,
                        *max_minutes,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                        *total_bidders,
                    ),
                )
            },
        )
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_rank1_bidders,
            |Tup2(channel, day),
             Tup6(total_bids, max_minutes, rank1_bids, rank2_bids, rank3_bids, total_bidders),
             rank1_bidders| {
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup7(
                        *total_bids,
                        *max_minutes,
                        *rank1_bids,
                        *rank2_bids,
                        *rank3_bids,
                        *total_bidders,
                        *rank1_bidders,
                    ),
                )
            },
        )
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_rank2_bidders,
            |Tup2(channel, day),
             Tup7(
                total_bids,
                max_minutes,
                rank1_bids,
                rank2_bids,
                rank3_bids,
                total_bidders,
                rank1_bidders,
            ),
             rank2_bidders| {
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup8(
                        *total_bids,
                        *max_minutes,
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
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_rank3_bidders,
            |Tup2(channel, day),
             Tup8(
                total_bids,
                max_minutes,
                rank1_bids,
                rank2_bids,
                rank3_bids,
                total_bidders,
                rank1_bidders,
                rank2_bidders,
            ),
             rank3_bidders| {
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup9(
                        *total_bids,
                        *max_minutes,
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
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_total_auctions,
            |Tup2(channel, day),
             Tup9(
                total_bids,
                max_minutes,
                rank1_bids,
                rank2_bids,
                rank3_bids,
                total_bidders,
                rank1_bidders,
                rank2_bidders,
                rank3_bidders,
            ),
             total_auctions| {
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup10(
                        *total_bids,
                        *max_minutes,
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
        .map_index(|Tup2(k, v)| (k.clone(), *v))
        .outer_join_default(
            &count_rank1_auctions,
            |Tup2(channel, day),
             Tup10(
                total_bids,
                max_minutes,
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
                Tup2(
                    Tup2(channel.clone(), *day),
                    Q16Intermediate1(
                        *total_bids,
                        *max_minutes,
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
        .map_index(|Tup2(k, v)| (k.clone(), v.clone()))
        .outer_join_default(
            &count_rank2_auctions,
            |Tup2(channel, day),
             Q16Intermediate1(
                total_bids,
                max_minutes,
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
                Tup2(
                    Tup2(channel.clone(), *day),
                    Tup2(
                        Q16Intermediate2(
                            *total_bids,
                            *max_minutes,
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
        .map_index(|Tup2(k, v)| (k.clone(), v.clone()))
        .outer_join_default(
            &count_rank3_auctions,
            |Tup2(channel, day),
             Tup2(
                Q16Intermediate2(
                    total_bids,
                    max_minutes,
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
             rank3_auctions| Q16Output {
                channel: channel.clone(),
                day: Date::from_ordinal_date(day.0, day.1)
                    .unwrap()
                    .format(iso8601_day_format)
                    .unwrap(),
                minute: Time::from_hms(max_minutes.0, max_minutes.1, 0)
                    .unwrap()
                    .format(&format_description::parse("[hour]:[minute]").unwrap())
                    .unwrap(),
                total_bids: *total_bids as u64,
                rank1_bids: *rank1_bids as u64,
                rank2_bids: *rank2_bids as u64,
                rank3_bids: *rank3_bids as u64,
                total_bidders: *total_bidders as u64,
                rank1_bidders: *rank1_bidders as u64,
                rank2_bidders: *rank2_bidders as u64,
                rank3_bidders: *rank3_bidders as u64,
                total_auctions: *total_auctions as u64,
                rank1_auctions: *rank1_auctions as u64,
                rank2_auctions: *rank2_auctions as u64,
                rank3_auctions: *rank3_auctions as u64,
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{generator::tests::make_bid, model::Bid};
    use dbsp::{zset, Runtime};
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
    fn test_q16(#[case] num_threads: usize) {
        let input_vecs = vec![
            vec![Tup2(
                Event::Bid(Bid {
                    channel: String::from("channel-1"),
                    // Right on 1970 epoch
                    date_time: 0,
                    auction: 1,
                    bidder: 1,
                    ..make_bid()
                }),
                1,
            )],
            vec![
                Tup2(
                    Event::Bid(Bid {
                        channel: String::from("channel-1"),
                        // Six minutes after epoch
                        date_time: 1000 * 6 * 60,
                        // Rank 2 bid
                        price: 10_001,
                        auction: 2,
                        bidder: 1,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        channel: String::from("channel-1"),
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
                Tup2(
                    Event::Bid(Bid {
                        channel: String::from("channel-1"),
                        // One millisecond into Jan 2 1970
                        date_time: 24 * 60 * 60 * 1000 + 1,
                        // Rank 3 bid
                        auction: 3,
                        bidder: 3,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        channel: String::from("channel-1"),
                        // One millisecond into Jan 3 1970
                        date_time: 2 * 24 * 60 * 60 * 1000 + 1,
                        // Rank 3 bid
                        auction: 3,
                        bidder: 4,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        channel: String::from("channel-1"),
                        // One millisecond into Jan 4 1970
                        date_time: 3 * 24 * 60 * 60 * 1000 + 1,
                        // Rank 3 bid
                        auction: 3,
                        bidder: 5,
                        ..make_bid()
                    }),
                    1,
                ),
                Tup2(
                    Event::Bid(Bid {
                        channel: String::from("channel-1"),
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
            vec![Tup2(
                Event::Bid(Bid {
                    channel: String::from("channel-1"),
                    date_time: MILLIS_2022_01_01,
                    auction: 4,
                    ..make_bid()
                }),
                1,
            )],
        ]
        .into_iter();

        let (mut dbsp, input_handle) = Runtime::init_circuit(num_threads, move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let mut expected_output = vec![
                zset![
                    Q16Output {
                        channel: String::from("channel-1"),
                        day: String::from("1970-01-01"),
                        minute: String::from("00:00"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q16Output::default()
                    } => 1,
                ],
                zset![
                    Q16Output {
                        channel: String::from("channel-1"),
                        day: String::from("1970-01-01"),
                        minute: String::from("00:00"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q16Output::default()
                    } => -1,
                    Q16Output {
                        channel: String::from("channel-1"),
                        day: String::from("1970-01-01"),
                        minute: String::from("23:59"),
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
                    Q16Output {
                        channel: String::from("channel-1"),
                        day: String::from("1970-01-02"),
                        minute: String::from("00:00"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q16Output::default()
                    } => 1,
                    Q16Output {
                        channel: String::from("channel-1"),
                        day: String::from("1970-01-03"),
                        minute: String::from("00:00"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q16Output::default()
                    } => 1,
                    Q16Output {
                        channel: String::from("channel-1"),
                        day: String::from("1970-01-04"),
                        minute: String::from("00:00"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q16Output::default()
                    } => 1,
                    Q16Output {
                        channel: String::from("channel-1"),
                        day: String::from("1970-01-05"),
                        minute: String::from("00:00"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q16Output::default()
                    } => 1,
                ],
                zset![
                    Q16Output {
                        channel: String::from("channel-1"),
                        day: String::from("2022-01-01"),
                        minute: String::from("00:00"),
                        total_bids: 1,
                        rank1_bids: 1,
                        total_bidders: 1,
                        rank1_bidders: 1,
                        total_auctions: 1,
                        rank1_auctions: 1,
                        ..Q16Output::default()
                    } => 1,
                ],
            ]
            .into_iter();

            let output = q16(stream);

            output.gather(0).inspect(move |batch| {
                if Runtime::worker_index() == 0 {
                    assert_eq!(batch, &expected_output.next().unwrap())
                }
            });

            Ok(input_handle)
        })
        .unwrap();

        dbsp.enable_cpu_profiler().unwrap();
        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            dbsp.step().unwrap();
        }
        dbsp.dump_profile(std::env::temp_dir().join("q16")).unwrap();
    }
}
