use super::NexmarkStream;
use dbsp::{operator::FilterMap, RootCircuit, OrdZSet, Stream};
use crate::model::Event;
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

type Q22Set = OrdZSet<(u64, u64, usize, ArcStr, ArcStr, ArcStr, ArcStr), isize>;
type Q22Stream = Stream<RootCircuit, Q22Set>;

pub fn q22(input: NexmarkStream) -> Q22Stream {
    input.flat_map(|event| match event {
        Event::Bid(b) => {
            let mut split = b.channel.as_str().split('/').skip(3);
            let (dir1, dir2, dir3) = (
                split.next().unwrap_or_default(),
                split.next().unwrap_or_default(),
                split.next().unwrap_or_default(),
            );

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
    use crate::{generator::tests::make_bid, model::Bid};
    use dbsp::zset;
    use rstest::rstest;

    #[rstest]
    #[case::bids_with_well_formed_urls(
        vec![vec![
            Event::Bid(Bid {
                channel: arcstr::literal!("https://example.com/foo/bar/zed"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: arcstr::literal!("https://example.com/dir1/dir2/dir3/dir4/dir5"),
                ..make_bid()
            }),
        ]],
        vec![zset!{
            (1, 1, 99, arcstr::literal!("https://example.com/foo/bar/zed"), arcstr::literal!("foo"), arcstr::literal!("bar"), arcstr::literal!("zed")) => 1,
            (1, 1, 99, arcstr::literal!("https://example.com/dir1/dir2/dir3/dir4/dir5"), arcstr::literal!("dir1"), arcstr::literal!("dir2"), arcstr::literal!("dir3")) => 1,
        }],
    )]
    #[case::bids_mixed_with_non_urls(
        vec![vec![
            Event::Bid(Bid {
                channel: arcstr::literal!("https://example.com/foo/bar/zed"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: arcstr::literal!("Google"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: arcstr::literal!("https:badly.formed/dir1/dir2/dir3"),
                ..make_bid()
            }),
        ]],
        vec![zset!{
            (1, 1, 99, arcstr::literal!("https://example.com/foo/bar/zed"), arcstr::literal!("foo"), arcstr::literal!("bar"), arcstr::literal!("zed")) => 1,
            (1, 1, 99, arcstr::literal!("Google"), arcstr::literal!(""), arcstr::literal!(""), arcstr::literal!("")) => 1,
            (1, 1, 99, arcstr::literal!("https:badly.formed/dir1/dir2/dir3"), arcstr::literal!("dir3"), arcstr::literal!(""), arcstr::literal!("")) => 1,
        }],
    )]
    fn test_q22(#[case] input_event_batches: Vec<Vec<Event>>, #[case] expected_zsets: Vec<Q22Set>) {
        let input_vecs = input_event_batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|e| (e, 1)).collect());

        let (circuit, mut input_handle) = RootCircuit::build(move |circuit| {
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
