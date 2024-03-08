use super::{NexmarkStream, WATERMARK_INTERVAL_SECONDS};
use crate::model::Event;
use dbsp::{
    dynamic::DynData, operator::Max, typed_batch::TypedBox, utils::Tup2, OrdIndexedZSet, OrdZSet,
    RootCircuit, Stream,
};

/// Query 5: Hot Items
///
/// Which auctions have seen the most bids in the last period?
/// Illustrates sliding windows and combiners.
///
/// The original Nexmark Query5 calculate the hot items in the last hour
/// (updated every minute). To make things a bit more dynamic and easier to test
/// we use much shorter windows, i.e. in the last 10 seconds and update every 2
/// seconds.
///
/// From [Nexmark q5.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q5.sql):
///
/// ```sql
/// CREATE TABLE discard_sink (
///   auction  BIGINT,
///   num  BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT AuctionBids.auction, AuctionBids.num
///  FROM (
///    SELECT
///      B1.auction,
///      count(*) AS num,
///      HOP_START(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
///      HOP_END(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
///    FROM bid B1
///    GROUP BY
///      B1.auction,
///      HOP(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
///  ) AS AuctionBids
///  JOIN (
///    SELECT
///      max(CountBids.num) AS maxn,
///      CountBids.starttime,
///      CountBids.endtime
///    FROM (
///      SELECT
///        count(*) AS num,
///        HOP_START(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
///        HOP_END(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
///      FROM bid B2
///      GROUP BY
///        B2.auction,
///        HOP(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
///      ) AS CountBids
///    GROUP BY CountBids.starttime, CountBids.endtime
///  ) AS MaxBids
///  ON AuctionBids.starttime = MaxBids.starttime AND
///     AuctionBids.endtime = MaxBids.endtime AND
///     AuctionBids.num >= MaxBids.maxn;
/// ```

/// If I am reading [Flink docs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/)
/// correctly, its default behavior is to trigger computation on
/// a window once the watermark passes the end of the window. Furthermore, since
/// the default "lateness" attribute of a stream is 0 the aggregate won't get
/// updated once the watermark passes the end of the window.  In other words, it
/// will aggregate within each window exactly once, which is what we implement
/// here.

type Q5Stream = Stream<RootCircuit, OrdZSet<Tup2<u64, u64>>>;

const WINDOW_WIDTH_SECONDS: u64 = 10;
const TUMBLE_SECONDS: u64 = 2;

pub fn q5(input: NexmarkStream) -> Q5Stream {
    // All bids indexed by date time to be able to window the result.
    let bids_by_time: Stream<_, OrdIndexedZSet<u64, u64>> =
        input.flat_map_index(|event| match event {
            Event::Bid(b) => Some((b.date_time, b.auction)),
            _ => None,
        });

    // Extract the largest timestamp from the input stream. We will use it as
    // current time. Set watermark to `WATERMARK_INTERVAL_SECONDS` in the past.
    let watermark: Stream<_, TypedBox<u64, DynData>> = bids_by_time.waterline_monotonic(
        || 0,
        |date_time| date_time - WATERMARK_INTERVAL_SECONDS * 1000,
    );

    // 10-second window with 2-second step.
    let window_bounds = watermark.apply(|watermark: &TypedBox<u64, _>| {
        let watermark = **watermark;
        let watermark_rounded = watermark - (watermark % (TUMBLE_SECONDS * 1000));
        (
            TypedBox::<u64, DynData>::new(
                watermark_rounded.saturating_sub(WINDOW_WIDTH_SECONDS * 1000),
            ),
            TypedBox::<u64, DynData>::new(watermark_rounded),
        )
    });

    // Only consider bids within the current window.
    let windowed_bids = bids_by_time
        .window(&window_bounds)
        .map(|(_time, auction)| *auction);

    // Count the number of bids per auction.
    let auction_counts = windowed_bids.weighted_count();

    // Find the largest number of bids across all auctions.
    let max_auction_count = auction_counts
        .map_index(|(_auction, count)| ((), *count))
        .aggregate(Max)
        .map_index(|((), max_count)| (*max_count, ()));

    // Filter out auctions with the largest number of bids.
    // TODO: once the query works, this can be done more efficiently
    // using `apply2`.
    let auction_by_count = auction_counts.map_index(|(auction, count)| (*count, *auction));

    max_auction_count.join(&auction_by_count, |max_count, &(), &auction| {
        Tup2(auction, *max_count as u64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        generator::tests::make_bid,
        model::{Bid, Event},
    };
    use dbsp::{utils::Tup2, zset, RootCircuit};
    use rstest::rstest;

    #[rstest]
    // Auction 2 has a single bid at t=20_000, so window is 6_000-16_000, which
    // leaves auction 1 as the hottest with a single bid (11_000).
    #[case::latest_bid_determines_window(
        vec![vec![2_001, 4_000, 11_000]],
        vec![vec![20_000]],
        vec![zset! { Tup2(1, 1) => 1}] )]
    // Auction 2's single bid is at 19_000 which leaves the rounded window at
    // 4_000-14_000, capturing 2 bids from auction 1 only (4_000 and 11_000).
    #[case::windows_rounded_to_2_s_boundary(
        vec![vec![2_001, 4_000, 11_000, 15_000]],
        vec![vec![19_000]],
        vec![zset! { Tup2(1, 2) => 1}] )]
    // Both auctions have the maximum two bids in the window (0 - 2000)
    #[case::multiple_auctions_have_same_hotness(
        vec![vec![2_000, 3_999, 8_000]],
        vec![vec![2_000, 3_999]],
        vec![zset! { Tup2(1, 2) => 1, Tup2(2, 2) => 1}])]
    // A second batch arrives changing the window to 6_000-16_000, switching
    // the hottest auction from 1 to 2.
    #[case::batch_2_updates_hotness_to_new_window(
        vec![vec![2_000, 4_000, 6_000], vec![20_000]],
        vec![vec![2_000, 4_000, 8_000, 12_000], vec![]],
        vec![zset! {Tup2(1, 3) => 1}, zset! {Tup2(2, 2) => 1, Tup2(1, 3) => -1}])]
    fn test_q5(
        #[case] auction1_batches: Vec<Vec<u64>>,
        #[case] auction2_batches: Vec<Vec<u64>>,
        #[case] expected_zsets: Vec<OrdZSet<Tup2<u64, u64>>>,
    ) {
        // Just ensure we don't get a false positive with zip only including
        // part of the input data. We could instead directly import zip_eq?
        assert_eq!(
            auction1_batches.len(),
            auction2_batches.len(),
            "Input batches for auction 1 and 2 must have the same length."
        );
        let input_vecs =
            auction1_batches
                .into_iter()
                .zip(auction2_batches)
                .map(|(a1_batch, a2_batch)| {
                    a1_batch
                        .into_iter()
                        .map(|date_time| {
                            Tup2(
                                Event::Bid(Bid {
                                    auction: 1,
                                    date_time,
                                    ..make_bid()
                                }),
                                1,
                            )
                        })
                        .chain(a2_batch.into_iter().map(|date_time| {
                            Tup2(
                                Event::Bid(Bid {
                                    auction: 2,
                                    date_time,
                                    ..make_bid()
                                }),
                                1,
                            )
                        }))
                        .collect()
                });

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q5(stream);

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
