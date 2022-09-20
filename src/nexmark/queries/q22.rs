use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};
use arcstr::ArcStr;

///
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

type Q22Stream =
    Stream<Circuit<()>, OrdZSet<(u64, u64, usize, ArcStr, ArcStr, ArcStr, ArcStr), isize>>;

pub fn q22(input: NexmarkStream) -> Q22Stream {
    input.flat_map(|event| match event {
        Event::Bid(b) => {
            // Just ensure the split vector has a length of at least 6 to index 5.
            let mut split: Vec<&str> = b.channel.as_str().split('/').collect();
            if split.len() < 6 {
                split.extend(vec![""; 6 - split.len()])
            }
            let (dir1, dir2, dir3) = (split[3], split[4], split[5]);
            Some((
                b.auction,
                b.bidder,
                b.price,
                b.channel.clone(),
                dir1.into(),
                dir2.into(),
                dir3.into(),
            ))
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nexmark::{generator::tests::make_bid, model::Bid},
        zset,
    };
    use rstest::rstest;

    #[rstest]
    #[case::bids_with_well_formed_urls(
        vec![vec![
            Event::Bid(Bid {
                channel: ArcStr::from("https://example.com/foo/bar/zed"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: ArcStr::from("https://example.com/dir1/dir2/dir3/dir4/dir5"),
                ..make_bid()
            }),
        ]],
        vec![zset!{
            (1, 1, 99, ArcStr::from("https://example.com/foo/bar/zed"), ArcStr::from("foo"), ArcStr::from("bar"), ArcStr::from("zed")) => 1,
            (1, 1, 99, ArcStr::from("https://example.com/dir1/dir2/dir3/dir4/dir5"), ArcStr::from("dir1"), ArcStr::from("dir2"), ArcStr::from("dir3")) => 1,
        }])]
    #[case::bids_mixed_with_non_urls(
        vec![vec![
            Event::Bid(Bid {
                channel: ArcStr::from("https://example.com/foo/bar/zed"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: ArcStr::from("Google"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: ArcStr::from("https:badly.formed/dir1/dir2/dir3"),
                ..make_bid()
            }),
        ]],
        vec![zset!{
            (1, 1, 99, ArcStr::from("https://example.com/foo/bar/zed"), ArcStr::from("foo"), ArcStr::from("bar"), ArcStr::from("zed")) => 1,
            (1, 1, 99, ArcStr::from("Google"), ArcStr::from(""), ArcStr::from(""), ArcStr::from("")) => 1,
            (1, 1, 99, ArcStr::from("https:badly.formed/dir1/dir2/dir3"), ArcStr::from("dir3"), ArcStr::from(""), ArcStr::from("")) => 1,
        }])]
    fn test_q22(
        #[case] input_event_batches: Vec<Vec<Event>>,
        #[case] expected_zsets: Vec<
            OrdZSet<(u64, u64, usize, ArcStr, ArcStr, ArcStr, ArcStr), isize>,
        >,
    ) {
        let input_vecs = input_event_batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|e| (e, 1)).collect());

        let (circuit, mut input_handle) = Circuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = q22(stream);

            let mut expected_output = expected_zsets.into_iter();
            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            input_handle
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }
}
