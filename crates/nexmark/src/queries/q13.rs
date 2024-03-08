use super::{process_time, NexmarkStream};
use crate::model::Event;
use dbsp::{
    utils::{Tup2, Tup3, Tup5, Tup7},
    OrdZSet, RootCircuit, Stream, ZWeight,
};

use csv;
use std::{
    fs::File,
    io::{BufReader, Read, Result},
};

/// Query 13: Bounded Side Input Join (Not in original suite)
///
/// Joins a stream to a bounded side input, modeling basic stream enrichment.
///
/// TODO: use the new "filesystem" connector once FLINK-17397 is done
/// ```sql
/// CREATE TABLE side_input (
///   key BIGINT,
///   `value` VARCHAR
/// ) WITH (
///   'connector.type' = 'filesystem',
///   'connector.path' = 'file://${FLINK_HOME}/data/side_input.txt',
///   'format.type' = 'csv'
/// );
///
/// CREATE TABLE discard_sink (
///   auction  BIGINT,
///   bidder  BIGINT,
///   price  BIGINT,
///   dateTime  TIMESTAMP(3),
///   `value`  VARCHAR
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     B.auction,
///     B.bidder,
///     B.price,
///     B.dateTime,
///     S.`value`
/// FROM (SELECT *, PROCTIME() as p_time FROM bid) B
/// JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
/// ON mod(B.auction, 10000) = S.key;
/// ```
///
/// NOTE: although the Flink test uses a static file as the side input, the
/// query itself allows joining the temporal table from the filesystem file that
/// is updated while the query runs, joining the temporal table using process
/// time. Flink itself ensures that the file is monitored for changes. The
/// [current documentation for this connector](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/filesystem/)
/// is not that used above, since the new connector (in 1.17-SNAPSHOT) does not
/// allow specifying a file name, but only a directory to monitor. Rather, the
/// Nexmark test appears to have used the previous legacy filesystem connector, [stable filesystem connector](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/filesystem/)
/// which does allow specifying a file path.
///
/// Also see [Flink's Join with a Temporal Table](https://nightlies.apache.org/flink/flink-docs-release-1.11/dev/table/streaming/joins.html#join-with-a-temporal-table).
///
/// So, although Flink supports monitoring the side-loaded file for updates, a
/// simple static file is used for this bounded side-input for the Nexmark tests
/// and that is also what is tested here.

type Q13Stream = Stream<RootCircuit, OrdZSet<Tup5<u64, u64, u64, u64, String>>>;

type SideInputStream = Stream<RootCircuit, OrdZSet<Tup3<u64, String, u64>>>;

const Q13_SIDE_INPUT_CSV: &str = "benches/nexmark/data/side_input.txt";

fn read_side_input<R: Read>(reader: R) -> Result<Vec<(u64, String)>> {
    let reader = BufReader::new(reader);
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(reader);
    Ok(csv_reader.deserialize().map(|r| r.unwrap()).collect())
}

pub fn q13_side_input() -> Vec<Tup2<Tup3<u64, String, u64>, ZWeight>> {
    let p_time = process_time();
    read_side_input(File::open(Q13_SIDE_INPUT_CSV).unwrap())
        .unwrap()
        .into_iter()
        .map(|(k, v)| Tup2(Tup3(k, v, p_time), 1))
        .collect()
}

pub fn q13(input: NexmarkStream, side_input: SideInputStream) -> Q13Stream {
    // Index bids by the modulo value.
    let bids_by_auction_mod = input.flat_map_index(move |event| match event {
        Event::Bid(b) => Some((
            b.auction % 10_000,
            Tup5(b.auction, b.bidder, b.price, b.date_time, process_time()),
        )),
        _ => None,
    });

    // Index the side_input by the key.
    let side_input_indexed = side_input.map_index(|Tup3(k, v, t)| (*k, Tup2(v.clone(), *t)));

    // Join on the key from the side input
    bids_by_auction_mod
        .join(
            &side_input_indexed,
            |&_,
             &Tup5(auction, bidder, price, date_time, b_p_time),
             Tup2(input_value, input_p_time)| {
                Tup7(
                    auction,
                    bidder,
                    price,
                    date_time,
                    input_value.clone(),
                    b_p_time,
                    *input_p_time,
                )
            },
        )
        .flat_map(
            |Tup7(auction, bidder, price, date_time, input_value, b_p_time, input_p_time)| {
                if b_p_time >= input_p_time {
                    Some(Tup5(
                        *auction,
                        *bidder,
                        *price,
                        *date_time,
                        input_value.clone(),
                    ))
                } else {
                    None
                }
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{generator::tests::make_bid, model::Bid};
    use dbsp::zset;

    #[test]
    fn test_q13() {
        let input_vecs = vec![vec![
            Tup2(
                Event::Bid(Bid {
                    auction: 1005,
                    ..make_bid()
                }),
                1,
            ),
            Tup2(
                Event::Bid(Bid {
                    auction: 10005,
                    ..make_bid()
                }),
                1,
            ),
        ]]
        .into_iter();

        let (circuit, (input_handle, side_input_handle)) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();
            let (side_stream, side_input_handle) =
                circuit.add_input_zset::<Tup3<u64, String, u64>>();

            let mut expected_output = vec![zset![
                Tup5(1_005, 1, 99, 0, String::from("1005")) => 1,
                Tup5(10_005, 1, 99, 0, String::from("5")) => 1,
            ]]
            .into_iter();

            let output = q13(stream, side_stream);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok((input_handle, side_input_handle))
        })
        .unwrap();

        side_input_handle.append(&mut q13_side_input());
        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }

    #[test]
    fn test_read_side_input() {
        let reader = "1,five\n2,four\n3,three".as_bytes();

        let got = read_side_input(reader).unwrap();

        assert_eq!(
            vec![
                (1, String::from("five")),
                (2, String::from("four")),
                (3, String::from("three")),
            ],
            got
        );
    }
}
