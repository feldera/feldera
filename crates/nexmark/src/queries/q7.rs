use super::{NexmarkStream, WATERMARK_INTERVAL_SECONDS};
use crate::model::Event;
use dbsp::{
    dynamic::DynData,
    operator::Max,
    typed_batch::TypedBox,
    utils::{Tup4, Tup5},
    OrdIndexedZSet, OrdZSet, RootCircuit, Stream,
};

///
/// Query 7: Highest Bid
///
/// What are the highest bids per period?
///
/// The original Nexmark Query7 calculate the highest bids in the last minute.
/// We will use a shorter window (10 seconds) to help make testing easier.
///
/// ```sql
/// CREATE TABLE discard_sink (
///   auction  BIGINT,
///   bidder  BIGINT,
///   price  BIGINT,
///   dateTime  TIMESTAMP(3),
///   extra  VARCHAR
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
/// from bid B
/// JOIN (
///   SELECT MAX(B1.price) AS maxprice, TUMBLE_ROWTIME(B1.dateTime, INTERVAL '10' SECOND) as dateTime
///   FROM bid B1
///   GROUP BY TUMBLE(B1.dateTime, INTERVAL '10' SECOND)
/// ) B1
/// ON B.price = B1.maxprice
/// WHERE B.dateTime BETWEEN B1.dateTime  - INTERVAL '10' SECOND AND B1.dateTime;
/// ```

type Q7Output = Tup5<u64, u64, u64, u64, String>;
type Q7Stream = Stream<RootCircuit, OrdZSet<Q7Output>>;

const TUMBLE_SECONDS: u64 = 10;

pub fn q7(input: NexmarkStream) -> Q7Stream {
    // All bids indexed by date time to be able to window the result.
    let bids_by_time: Stream<_, OrdIndexedZSet<u64, _>> =
        input.flat_map_index(|event| match event {
            Event::Bid(b) => Some((
                b.date_time,
                Tup4(b.auction, b.bidder, b.price, b.extra.clone()),
            )),
            _ => None,
        });

    // Similar to the sliding window of q5, we want to find the largest timestamp
    // from the input stream for the current time, with the window ending at the
    // previous 10 second multiple.
    // Set the watermark to `WATERMARK_INTERVAL_SECONDS` in the past.
    let watermark: Stream<_, TypedBox<u64, DynData>> = bids_by_time.waterline_monotonic(
        || 0,
        |date_time| date_time - WATERMARK_INTERVAL_SECONDS * 1000,
    );

    // In this case we have a 10-second window with 10-second steps (tumbling).
    let window_bounds = watermark.apply(|watermark| {
        let watermark = **watermark;
        let watermark_rounded = watermark - (watermark % (TUMBLE_SECONDS * 1000));
        (
            TypedBox::<u64, DynData>::new(watermark_rounded.saturating_sub(TUMBLE_SECONDS * 1000)),
            TypedBox::<u64, DynData>::new(watermark_rounded),
        )
    });

    // Only consider bids within the current window.
    let windowed_bids = bids_by_time.window(&window_bounds);
    let bids_by_price =
        windowed_bids.map_index(|(date_time, Tup4(auction, bidder, price, extra))| {
            (
                *price,
                Tup5(*auction, *bidder, *price, *date_time, extra.clone()),
            )
        });

    // Find the maximum bid across all bids.
    windowed_bids
        .map_index(|(_date_time, Tup4(_auction, _bidder, price, _extra))| ((), *price))
        .aggregate(Max)
        .map_index(|((), price)| (*price, ()))
        // Find _all_ bids with computed max price.
        .join(&bids_by_price, |_price, &(), tuple| tuple.clone())
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

    type Q7Tuple = Tup5<u64, u64, u64, u64, String>;

    #[rstest]
    // The latest bid is at t=32_000, so the watermark as at t=28_000
    // and the tumbled window is from 10_000 - 20_000.
    #[case::latest_bid_determines_window(
        vec![vec![(9_000, 1_000_000), (11_000, 50), (14_000, 90), (16_000, 70), (21_000, 1_000_000), (32_000, 1_000_000)]],
        vec![zset! {Tup5(1, 1, 90, 14_000, String::new()) => 1}],
    )]
    // The window is rounded to the 10 second boundary
    #[case::window_boundary_below(
        vec![vec![(9_999, 50), (32_000, 1_000_000)]],
        vec![zset! {}],
    )]
    #[case::window_boundary_lower(
        vec![vec![(10_000, 50), (32_000, 1_000_000)]],
        vec![zset! {Tup5(1, 1, 50, 10_000, String::new()) => 1}],
    )]
    #[case::window_boundary_upper(
        vec![vec![(19_999, 50), (32_000, 1_000_000)]],
        vec![zset! {Tup5(1, 1, 50, 19_999, String::new()) => 1}],
    )]
    #[case::window_boundary_above(
        vec![vec![(20_000, 50), (32_000, 1_000_000)]],
        vec![zset! {}],
    )]
    #[case::tumble_into_new_window(
        vec![vec![(9_000, 1_000_000), (11_000, 50), (14_000, 90), (16_000, 70), (21_000, 1_000_000)], vec![(32_000, 10)], vec![(42_000, 10)]],
        vec![
            zset! {Tup5(1, 1, 1_000_000, 9_000, String::new()) => 1},
            zset! {
                Tup5(1, 1, 1_000_000, 9_000, String::new()) => -1,
                Tup5(1, 1, 90, 14_000, String::new()) => 1,
            },
            zset! {
                Tup5(1, 1, 90, 14_000, String::new()) => -1,
                Tup5(1, 1, 1_000_000, 21_000, String::new()) => 1,
            }],
    )]
    #[case::multiple_max_bids(
        vec![vec![(11_000, 90), (14_000, 90), (16_000, 90), (21_000, 1_000_000), (32_000, 1_000_000)]],
        vec![zset! {Tup5(1, 1, 90, 11_000, String::new()) => 1, Tup5(1, 1, 90, 14_000, String::new()) => 1, Tup5(1, 1, 90, 16_000, String::new()) => 1}],
    )]
    fn test_q7(
        #[case] input_batches: Vec<Vec<(u64, u64)>>,
        #[case] expected_zsets: Vec<OrdZSet<Q7Tuple>>,
    ) {
        let input_vecs = input_batches.into_iter().map(|batch| {
            batch
                .into_iter()
                .map(|(date_time, price)| {
                    Tup2(
                        Event::Bid(Bid {
                            date_time,
                            price,
                            ..make_bid()
                        }),
                        1,
                    )
                })
                .collect()
        });

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q7(stream);

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
