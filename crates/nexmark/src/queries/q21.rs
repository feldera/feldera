use super::NexmarkStream;
use dbsp::{operator::FilterMap, RootCircuit, OrdZSet, Stream};
use crate::model::Event;
use dbsp::{algebra::ArcStr, arcstr_literal};
use regex::Regex;

///
/// -- Query 21: Add channel id (Not in original suite)
///
/// -- Add a channel_id column to the bid table.
/// -- Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL.
///
/// ```sql
/// CREATE TABLE discard_sink (
///     auction  BIGINT,
///     bidder  BIGINT,
///     price  BIGINT,
///     channel  VARCHAR,
///     channel_id  VARCHAR
/// ) WITH (
///     'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     auction, bidder, price, channel,
///     CASE
///         WHEN lower(channel) = 'apple' THEN '0'
///         WHEN lower(channel) = 'google' THEN '1'
///         WHEN lower(channel) = 'facebook' THEN '2'
///         WHEN lower(channel) = 'baidu' THEN '3'
///         ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)
///         END
///     AS channel_id FROM bid
///     where REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) is not null or
///           lower(channel) in ('apple', 'google', 'facebook', 'baidu');
/// ```

type Q21Set = OrdZSet<(u64, u64, usize, ArcStr, ArcStr), isize>;
type Q21Stream = Stream<RootCircuit, Q21Set>;

pub fn q21(input: NexmarkStream) -> Q21Stream {
    let channel_regex = Regex::new(r"channel_id=([^&]*)").unwrap();

    input.flat_map(move |event| match event {
        Event::Bid(b) => {
            let channel_id = match b.channel.to_lowercase().as_str() {
                "apple" => Some(arcstr_literal!("0")),
                "google" => Some(arcstr_literal!("1")),
                "facebook" => Some(arcstr_literal!("2")),
                "baidu" => Some(arcstr_literal!("3")),
                _ => channel_regex
                    .captures(b.channel.as_str())
                    .and_then(|caps| match caps.len() {
                        2 => Some(ArcStr::from(caps.get(1).unwrap().as_str())),
                        _ => None,
                    }),
            };
            channel_id.map(|ch_id| (b.auction, b.bidder, b.price, b.channel.clone(), ch_id))
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
    #[case::bids_with_known_channel_ids(
        vec![vec![
            Event::Bid(Bid {
                channel: arcstr_literal!("ApPlE"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: arcstr_literal!("FaceBook"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: arcstr_literal!("GooGle"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: arcstr_literal!("Baidu"),
                ..make_bid()
            }),
        ]],
        vec![zset!{
            (1, 1, 99, arcstr_literal!("ApPlE"), arcstr_literal!("0")) => 1,
            (1, 1, 99, arcstr_literal!("GooGle"), arcstr_literal!("1")) => 1,
            (1, 1, 99, arcstr_literal!("FaceBook"), arcstr_literal!("2")) => 1,
            (1, 1, 99, arcstr_literal!("Baidu"), arcstr_literal!("3")) => 1,
        }],
    )]
    #[case::bids_with_unknown_channel_ids(
        vec![vec![
            Event::Bid(Bid {
                channel: arcstr_literal!("https://example.com/?channel_id=ubuntu"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: arcstr_literal!("https://example.com/?channel_id=cherry-pie"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: arcstr_literal!("https://example.com/?not_channelid=should-not-appear"),
                ..make_bid()
            }),
        ]],
        vec![zset!{
            (1, 1, 99, arcstr_literal!("https://example.com/?channel_id=ubuntu"), arcstr_literal!("ubuntu")) => 1,
            (1, 1, 99, arcstr_literal!("https://example.com/?channel_id=cherry-pie"), arcstr_literal!("cherry-pie")) => 1,
        }],
    )]
    fn test_q21(#[case] input_event_batches: Vec<Vec<Event>>, #[case] expected_zsets: Vec<Q21Set>) {
        let input_vecs = input_event_batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|e| (e, 1)).collect());

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = q21(stream);

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
