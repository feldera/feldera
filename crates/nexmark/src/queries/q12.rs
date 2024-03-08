use super::{process_time, NexmarkStream};
use crate::model::Event;
use dbsp::{utils::Tup4, OrdZSet, RootCircuit, Stream};

///
/// Query 12: Processing Time Windows (Not in original suite)
///
/// How many bids does a user make within a fixed processing time limit?
/// Illustrates working in processing time window.
///
/// Group bids by the same user into processing time windows of 10 seconds.
/// Emit the count of bids per window.
///
/// ```sql
/// CREATE TABLE discard_sink (
///   bidder BIGINT,
///   bid_count BIGINT,
///   starttime TIMESTAMP(3),
///   endtime TIMESTAMP(3)
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     B.bidder,
///     count(*) as bid_count,
///     TUMBLE_START(B.p_time, INTERVAL '10' SECOND) as starttime,
///     TUMBLE_END(B.p_time, INTERVAL '10' SECOND) as endtime
/// FROM (SELECT *, PROCTIME() as p_time FROM bid) B
/// GROUP BY B.bidder, TUMBLE(B.p_time, INTERVAL '10' SECOND);
/// ```

type Q12Stream = Stream<RootCircuit, OrdZSet<Tup4<u64, u64, u64, u64>>>;
const TUMBLE_SECONDS: u64 = 10;

fn window_for_process_time(ptime: u64) -> (u64, u64) {
    let window_lower = ptime - (ptime % (TUMBLE_SECONDS * 1000));
    (window_lower, window_lower + TUMBLE_SECONDS * 1000)
}

// This function enables us to test the q12 functionality without using the
// actual process time, while the actual q12 function below uses the real
// process time.
fn q12_for_process_time<F>(input: NexmarkStream, process_time: F) -> Q12Stream
where
    F: Fn() -> u64 + 'static,
{
    let bids_by_bidder_window = input.flat_map_index(move |event| match event {
        Event::Bid(b) => {
            let (starttime, endtime) = window_for_process_time(process_time());
            Some(((b.bidder, starttime, endtime), ()))
        }
        _ => None,
    });

    bids_by_bidder_window
        .weighted_count()
        .map(|(&(bidder, starttime, endtime), &count)| {
            Tup4(bidder, count as u64, starttime, endtime)
        })
}

pub fn q12(input: NexmarkStream) -> Q12Stream {
    q12_for_process_time(input, process_time)
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
    use std::cell::RefCell;

    #[rstest]
    #[case::one_bidder_single_window(
        vec![vec![(1, 1), (1, 2), (1, 99), (1, 25)], vec![(1, 16), (1, 2)]],
        vec![3_000, 4_000, 5_000, 6_000, 7_000, 8_000],
        vec![
            zset! {Tup4(1, 4, 0, 10_000) => 1},
            zset! { Tup4(1, 4, 0, 10_000) => -1, Tup4(1, 6, 0, 10_000) => 1},
        ],
    )]
    #[case::one_bidder_multiple_windows(
        vec![vec![(1, 99), (1, 63), (1, 2), (1, 45)], vec![(1, 29), (1, 21)]],
        vec![3_000, 4_000, 5_000, 6_000, 11_000, 12_000],
        vec![
            zset! {Tup4(1, 4, 0, 10_000) => 1},
            zset! {Tup4(1, 2, 10_000, 20_000) => 1},
        ],
    )]
    #[case::multiple_bidders_multiple_windows(
        vec![vec![(1, 12), (1, 102), (1, 22), (1, 79), (2, 16), (2, 81)], vec![(1, 49), (1, 77)]],
        vec![3_000, 4_000, 5_000, 6_000, 7_000, 8_000, 11_000, 12_000],
        vec![
            zset! {Tup4(1, 4, 0, 10_000) => 1, Tup4(2, 2, 0, 10_000) => 1},
            zset! {Tup4(1, 2, 10_000, 20_000) => 1},
        ],
    )]
    fn test_q12(
        #[case] bidder_bid_batches: Vec<Vec<(u64, u64)>>,
        #[case] proc_times: Vec<u64>,
        #[case] expected_zsets: Vec<OrdZSet<Tup4<u64, u64, u64, u64>>>,
    ) {
        let input_vecs = bidder_bid_batches.into_iter().map(|batch| {
            batch
                .into_iter()
                .map(|(bidder, auction)| {
                    Tup2(
                        Event::Bid(Bid {
                            bidder,
                            auction,
                            ..make_bid()
                        }),
                        1,
                    )
                })
                .collect()
        });

        let proc_time_iter = RefCell::new(proc_times.into_iter());
        let process_time = move || -> u64 { proc_time_iter.borrow_mut().next().unwrap() };

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q12_for_process_time(stream, process_time);

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
