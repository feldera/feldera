use super::NexmarkStream;
use crate::model::Event;
use dbsp::{
    dynamic::DynData,
    typed_batch::TypedBox,
    utils::{Tup2, Tup3},
    OrdIndexedZSet, OrdZSet, RootCircuit, Stream,
};

///
/// Query 8: Monitor New Users
///
/// Select people who have entered the system and created auctions in the last
/// period. Illustrates a simple join.
///
/// The original Nexmark Query8 monitors the new users the last 12 hours,
/// updated every 12 hours. To make things a bit more dynamic and easier to test
/// we use much shorter windows (10 seconds).
///
/// ```sql
/// CREATE TABLE discard_sink (
///   id  BIGINT,
///   name  VARCHAR,
///   stime  TIMESTAMP(3)
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT P.id, P.name, P.starttime
/// FROM (
///   SELECT P.id, P.name,
///          TUMBLE_START(P.dateTime, INTERVAL '10' SECOND) AS starttime,
///          TUMBLE_END(P.dateTime, INTERVAL '10' SECOND) AS endtime
///   FROM person P
///   GROUP BY P.id, P.name, TUMBLE(P.dateTime, INTERVAL '10' SECOND)
/// ) P
/// JOIN (
///   SELECT A.seller,
///          TUMBLE_START(A.dateTime, INTERVAL '10' SECOND) AS starttime,
///          TUMBLE_END(A.dateTime, INTERVAL '10' SECOND) AS endtime
///   FROM auction A
///   GROUP BY A.seller, TUMBLE(A.dateTime, INTERVAL '10' SECOND)
/// ) A
/// ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;
/// ```

type Q8Stream = Stream<RootCircuit, OrdZSet<Tup3<u64, String, u64>>>;

const TUMBLE_SECONDS: u64 = 10;

pub fn q8(input: NexmarkStream) -> Q8Stream {
    // People indexed by the date they entered the system.
    let people_by_time = input.flat_map_index(|event| match event {
        Event::Person(p) => Some((p.date_time, Tup2(p.id, p.name.clone()))),
        _ => None,
    });

    // Auctions indexed by the date they were created.
    let auctions_by_time: Stream<_, OrdIndexedZSet<u64, u64>> =
        input.flat_map_index(|event| match event {
            Event::Auction(a) => Some((a.date_time, a.seller)),
            _ => None,
        });

    // Use the latest auction for the watermark
    let watermark: Stream<_, TypedBox<u64, DynData>> =
        auctions_by_time.waterline_monotonic(|| 0, |date_time| date_time - TUMBLE_SECONDS * 1000);
    let window_bounds = watermark.apply(|watermark| {
        let watermark = **watermark;
        let watermark_rounded = watermark - (watermark % (TUMBLE_SECONDS * 1000));
        (
            TypedBox::<u64, DynData>::new(watermark_rounded.saturating_sub(TUMBLE_SECONDS * 1000)),
            TypedBox::<u64, DynData>::new(watermark_rounded),
        )
    });

    // Only consider people and auctions within the current window.
    // Similar to queries 5 and 6, this differs in semantics from the SQL which
    // reads as though it calculates all the windows, but as per the comment in
    // q5.rs, based on the Flink docs, it is aggregated within each window
    // exactly once as we do here.
    let windowed_people = people_by_time.window(&window_bounds);
    let windowed_auctions = auctions_by_time.window(&window_bounds);

    let people_by_id = windowed_people
        .map_index(|(date_time, Tup2(id, name))| (*id, Tup2(name.clone(), *date_time)));

    // Re-calculate the window start-time to include in the output.
    people_by_id.join(
        &windowed_auctions.map_index(|(_date_time, seller)| (*seller, ())),
        |&p_id, Tup2(p_name, p_date_time), ()| {
            Tup3(
                p_id,
                p_name.clone(),
                *p_date_time - (*p_date_time % (TUMBLE_SECONDS * 1000)),
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        generator::tests::{make_auction, make_person},
        model::{Auction, Event, Person},
    };
    use dbsp::{zset, RootCircuit};
    use rstest::rstest;

    #[rstest]
    // Persons 2 and 3 were both added during the 10-20 interval and created auctions in
    // that same interval. Person 1 was added in the previous interval (0-10) though their
    // auction is in the correct interval. Person 4 was added in the interval, but their auction is
    // in the next.
    #[case::people_with_auction(
        vec![vec![
            (1, String::from("James Potter"), 9_000),
            (2, String::from("Lily Potter"), 12_000),
            (3, String::from("Harry Potter"), 15_000),
            (4, String::from("Albus D"), 18_000)]],
        vec![vec![
            (1, 11_000),
            (2, 15_000),
            (3, 18_000),
            (4, 21_000),
            (99, 32_000)]],
        vec![zset! {
            Tup3(2, String::from("Lily Potter"), 10_000) => 1,
            Tup3(3, String::from("Harry Potter"), 10_000) => 1,
        }],
    )]
    // In this case, both persons 1 and 2 are added in the 10-20 window,
    // and add corresponding auctions in the same. But they are only seen when
    // auction 99 arrives setting the appropriate window to 10-20.
    // Person 3 only appears when the window advances to 20-30 (by auction 101)
    #[case::multiple_batches(
        vec![
            vec![
                (1, String::from("James Potter"), 10_000),
                (2, String::from("Lily Potter"), 12_000),
            ],
            vec![(3, String::from("Harry Potter"), 22_000)],
            vec![],
        ],
        vec![
            vec![
                (1, 14_000),
                (2, 15_000),
            ],
            vec![(3, 25_000), (99, 32_000)],
            vec![(101, 42_000)]
        ],
        vec![zset! {}, zset! {
            Tup3(1, String::from("James Potter"), 10_000) => 1,
            Tup3(2, String::from("Lily Potter"), 10_000) => 1,
        }, zset! {
            Tup3(1, String::from("James Potter"), 10_000) => -1,
            Tup3(2, String::from("Lily Potter"), 10_000) => -1,
            Tup3(3, String::from("Harry Potter"), 20_000) => 1,
        }]
    )]
    fn test_q8(
        #[case] input_people_batches: Vec<Vec<(u64, String, u64)>>,
        #[case] input_auction_batches: Vec<Vec<(u64, u64)>>,
        #[case] expected_zsets: Vec<OrdZSet<Tup3<u64, String, u64>>>,
    ) {
        // Just ensure we don't get a false positive with zip only including
        // part of the input data. We could instead directly import zip_eq?
        assert_eq!(
            input_people_batches.len(),
            input_auction_batches.len(),
            "Input batches for people and auctions must have the same length."
        );

        let input_vecs = input_people_batches
            .into_iter()
            .zip(input_auction_batches)
            .map(|(p_batch, a_batch)| {
                p_batch
                    .into_iter()
                    .map(|(id, name, date_time)| {
                        Tup2(
                            Event::Person(Person {
                                id,
                                name,
                                date_time,
                                ..make_person()
                            }),
                            1,
                        )
                    })
                    .chain(a_batch.into_iter().map(|(seller, date_time)| {
                        Tup2(
                            Event::Auction(Auction {
                                seller,
                                date_time,
                                ..make_auction()
                            }),
                            1,
                        )
                    }))
                    .collect()
            });

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q8(stream);

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
