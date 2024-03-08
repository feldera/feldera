use super::NexmarkStream;
use crate::model::Event;
use dbsp::{utils::Tup5, OrdZSet, RootCircuit, Stream};
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

type Q21Set = OrdZSet<Tup5<u64, u64, u64, String, String>>;
type Q21Stream = Stream<RootCircuit, Q21Set>;

pub fn q21(input: NexmarkStream) -> Q21Stream {
    let channel_regex = Regex::new(r"channel_id=([^&]*)").unwrap();

    input.flat_map(move |event| match event {
        Event::Bid(b) => {
            let channel_id = match b.channel.to_lowercase().as_str() {
                "apple" => Some(String::from("0")),
                "google" => Some(String::from("1")),
                "facebook" => Some(String::from("2")),
                "baidu" => Some(String::from("3")),
                _ => channel_regex
                    .captures(b.channel.as_str())
                    .and_then(|caps| match caps.len() {
                        2 => Some(String::from(caps.get(1).unwrap().as_str())),
                        _ => None,
                    }),
            };
            channel_id.map(|ch_id| Tup5(b.auction, b.bidder, b.price, b.channel.clone(), ch_id))
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

    #[rstest]
    #[case::bids_with_known_channel_ids(
        vec![vec![
            Event::Bid(Bid {
                channel: String::from("ApPlE"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: String::from("FaceBook"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: String::from("GooGle"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: String::from("Baidu"),
                ..make_bid()
            }),
        ]],
        vec![zset!{
            Tup5(1, 1, 99, String::from("ApPlE"), String::from("0")) => 1,
            Tup5(1, 1, 99, String::from("GooGle"), String::from("1")) => 1,
            Tup5(1, 1, 99, String::from("FaceBook"), String::from("2")) => 1,
            Tup5(1, 1, 99, String::from("Baidu"), String::from("3")) => 1,
        }],
    )]
    #[case::bids_with_unknown_channel_ids(
        vec![vec![
            Event::Bid(Bid {
                channel: String::from("https://example.com/?channel_id=ubuntu"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: String::from("https://example.com/?channel_id=cherry-pie"),
                ..make_bid()
            }),
            Event::Bid(Bid {
                channel: String::from("https://example.com/?not_channelid=should-not-appear"),
                ..make_bid()
            }),
        ]],
        vec![zset!{
            Tup5(1, 1, 99, String::from("https://example.com/?channel_id=ubuntu"), String::from("ubuntu")) => 1,
            Tup5(1, 1, 99, String::from("https://example.com/?channel_id=cherry-pie"), String::from("cherry-pie")) => 1,
        }],
    )]
    fn test_q21(#[case] input_event_batches: Vec<Vec<Event>>, #[case] expected_zsets: Vec<Q21Set>) {
        let input_vecs = input_event_batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|e| Tup2(e, 1)).collect());

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

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
