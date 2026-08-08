use super::NexmarkStream;
use crate::model::Event;
use dbsp::{utils::Tup7, OrdZSet, RootCircuit, Stream};

type Q22Set = OrdZSet<Tup7<u64, u64, u64, String, Option<String>, Option<String>, Option<String>>>;
type Q22Stream = Stream<RootCircuit, Q22Set>;

/// Query 22: Get URL Directories (Not in original suite)
///
/// What is the directory structure of the URL?
/// Illustrates a SPLIT_INDEX SQL.
///
/// ```sql
/// CREATE TABLE discard_sink (
///       auction  BIGINT,
///       bidder  BIGINT,
///       price  BIGINT,
///       channel  VARCHAR,
///       dir1  VARCHAR,
///       dir2  VARCHAR,
///       dir3  VARCHAR
/// ) WITH (
///     'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     auction, bidder, price, channel,
///     SPLIT_INDEX(url, '/', 3) as dir1,
///     SPLIT_INDEX(url, '/', 4) as dir2,
///     SPLIT_INDEX(url, '/', 5) as dir3 FROM bid;
/// ```
pub fn q22(_circuit: &mut RootCircuit, input: NexmarkStream) -> Q22Stream {
    input.flat_map(|event| match event {
        Event::Bid(b) => {
            // SPLIT_INDEX returns NULL when the index is out of range.
            let mut split = b.url.as_str().split('/').skip(3);
            let (dir1, dir2, dir3) = (
                split.next().map(String::from),
                split.next().map(String::from),
                split.next().map(String::from),
            );

            Some(Tup7(
                b.auction,
                b.bidder,
                b.price,
                b.channel.clone(),
                dir1,
                dir2,
                dir3,
            ))
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{generator::tests::make_bid, model::Bid};
    use dbsp::{utils::Tup2, zset};
    use rstest::rstest;

    fn make_url_bid(url: &str) -> Bid {
        Bid {
            channel: String::from(url),
            url: String::from(url),
            ..make_bid()
        }
    }

    #[rstest]
    #[case::bids_with_well_formed_urls(
        vec![vec![
            Event::Bid(make_url_bid("https://example.com/foo/bar/zed")),
            Event::Bid(make_url_bid("https://example.com/dir1/dir2/dir3/dir4/dir5")),
        ]],
        vec![zset!{
            Tup7(1, 1, 99, String::from("https://example.com/foo/bar/zed"), Some(String::from("foo")), Some(String::from("bar")), Some(String::from("zed"))) => 1,
            Tup7(1, 1, 99, String::from("https://example.com/dir1/dir2/dir3/dir4/dir5"), Some(String::from("dir1")), Some(String::from("dir2")), Some(String::from("dir3"))) => 1,
        }],
    )]
    #[case::bids_mixed_with_non_urls(
        vec![vec![
            Event::Bid(make_url_bid("https://example.com/foo/bar/zed")),
            Event::Bid(make_url_bid("Google")),
            Event::Bid(make_url_bid("https:badly.formed/dir1/dir2/dir3")),
        ]],
        vec![zset!{
            Tup7(1, 1, 99, String::from("https://example.com/foo/bar/zed"), Some(String::from("foo")), Some(String::from("bar")), Some(String::from("zed"))) => 1,
            Tup7(1, 1, 99, String::from("Google"), None, None, None) => 1,
            Tup7(1, 1, 99, String::from("https:badly.formed/dir1/dir2/dir3"), Some(String::from("dir3")), None, None) => 1,
        }],
    )]
    fn test_q22(#[case] input_event_batches: Vec<Vec<Event>>, #[case] expected_zsets: Vec<Q22Set>) {
        let input_vecs = input_event_batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|e| Tup2(e, 1)).collect());

        let (mut circuit, input_handle) = dbsp::Runtime::init_circuit(1, move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q22(circuit, stream);

            let mut expected_output = expected_zsets.into_iter();
            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.transaction().unwrap();
        }
    }
}
