use super::NexmarkStream;
use crate::model::Event;
use dbsp::{operator::FilterMap, RootCircuit, OrdZSet, Stream};
use dbsp::algebra::ArcStr;
use rust_decimal::Decimal;
use size_of::SizeOf;
use std::hash::Hash;
use std::ops::Deref;

/// Query 14: Calculation (Not in original suite)
///
/// Convert bid timestamp into types and find bids with specific price.
/// Illustrates duplicate expressions and usage of user-defined-functions.
///
/// ```sql
/// CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';
///
/// CREATE TABLE discard_sink (
///     auction BIGINT,
///     bidder BIGINT,
///     price  DECIMAL(23, 3),
///     bidTimeType VARCHAR,
///     dateTime TIMESTAMP(3),
///     extra VARCHAR,
///     c_counts BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     auction,
///     bidder,
///     0.908 * price as price,
///     CASE
///         WHEN HOUR(dateTime) >= 8 AND HOUR(dateTime) <= 18 THEN 'dayTime'
///         WHEN HOUR(dateTime) <= 6 OR HOUR(dateTime) >= 20 THEN 'nightTime'
///         ELSE 'otherTime'
///     END AS bidTimeType,
///     dateTime,
///     extra,
///     count_char(extra, 'c') AS c_counts
/// FROM bid
/// WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;
/// ```

#[derive(
    Eq, Clone, Debug, Hash, PartialEq, PartialOrd, Ord, SizeOf, bincode::Decode, bincode::Encode,
)]
pub struct Q14Output(
    u64,
    u64,
    BincodeDecimal,
    BidTimeType,
    u64,
     ArcStr,
    usize,
);

type Q14Stream = Stream<RootCircuit, OrdZSet<Q14Output, isize>>;

/// Wrapper type for `Decimal` that implements Decode and Encode.
///
/// # Note
/// Since the query doesn't use spine we don't actually end up
/// serializing/deserializing Decimals but we still need to implement it to
/// satisfy trait constraints.
///
/// For the future we can submit a PR to `rust_decimal` to implement `Decode`
/// and `Encode` for `Decimal` directly or if we end up using rykv anyways
/// `rust_decimal` has support for it already.
#[derive(Eq, Clone, Debug, Hash, PartialEq, PartialOrd, Ord)]
struct BincodeDecimal(Decimal);

impl bincode::Encode for BincodeDecimal {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> core::result::Result<(), bincode::error::EncodeError> {
        bincode::Encode::encode(&self.0.to_string(), encoder)?;
        Ok(())
    }
}

impl bincode::Decode for BincodeDecimal {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let s: String = bincode::Decode::decode(decoder)?;
        Ok(Self(Decimal::from_str_exact(&s).unwrap()))
    }
}

impl<'de> bincode::BorrowDecode<'de> for BincodeDecimal {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D
    ) -> Result<Self, bincode::error::DecodeError> {
        let s: String = bincode::BorrowDecode::borrow_decode(decoder)?;
        Ok(Self(Decimal::from_str_exact(&s).unwrap()))
    }
}

impl Deref for BincodeDecimal {
    type Target = Decimal;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SizeOf for BincodeDecimal {
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.0.size_of_children(context);
    }
}

#[derive(Eq, Clone, Debug, Hash, PartialEq, PartialOrd, Ord, SizeOf, bincode::Decode, bincode::Encode)]
enum BidTimeType {
    Day,
    Night,
    Other,
}

// This is used because we can't currently use chrono.Utc, which would simply
// be Utc.timestamp_millis(b.date_time as i64).hour(), as it's waiting on a
// release to fix a security issue.
fn hour_for_millis(millis: u64) -> usize {
    let millis_for_day = millis % (24 * 60 * 60 * 1000);
    (millis_for_day / (60 * 60 * 1000)) as usize
}

pub fn q14(input: NexmarkStream) -> Q14Stream {
    input.flat_map(|event| match event {
        Event::Bid(b) => {
            let new_price = Decimal::new((b.price * 100) as i64, 2) * Decimal::new(908, 3);
            if new_price > Decimal::new(1_000_000, 0) && new_price < Decimal::new(50_000_000, 0) {
                Some(Q14Output(
                    b.auction,
                    b.bidder,
                    BincodeDecimal(new_price),
                    match hour_for_millis(b.date_time) {
                        8..=18 => BidTimeType::Day,
                        20..=23 | 0..=6 => BidTimeType::Night,
                        _ => BidTimeType::Other,
                    },
                    b.date_time,
                    b.extra.clone(),
                    b.extra.matches('c').count(),
                ))
            } else {
                None
            }
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
    #[case::decimal_price_converted(2_000_000, 0, "", zset![Q14Output(1, 1, BincodeDecimal(Decimal::new(1_816_000_000, 3)), BidTimeType::Night, 0, ArcStr::new(), 0) => 1])]
    #[case::decimal_price_converted_outside_range(1_000_000, 0, "", zset![])]
    #[case::decimal_price_converted_on_exclusive_boundary(1_000_000, 0, "", zset![])]
    #[case::date_time_is_nighttime(2_000_000, 20*60*60*1000 + 1, "", zset![Q14Output(1, 1, BincodeDecimal(Decimal::new(1_816_000_000, 3)), BidTimeType::Night, 20*60*60*1000 + 1, ArcStr::new(), 0) => 1])]
    #[case::date_time_is_daytime(2_000_000, 8*60*60*1000 + 1, "", zset![Q14Output(1, 1, BincodeDecimal(Decimal::new(1_816_000_000, 3)), BidTimeType::Day, 8*60*60*1000 + 1, ArcStr::new(), 0) => 1])]
    #[case::date_time_is_daytime_2022(2_000_000, 52*366*24*60*60*1000 + 8*60*60*1000 + 1, "", zset![Q14Output(1, 1, BincodeDecimal(Decimal::new(1_816_000_000, 3)), BidTimeType::Day, 52*366*24*60*60*1000 + 8*60*60*1000 + 1, ArcStr::new(), 0) => 1])]
    #[case::date_time_is_othertime(2_000_000, 8*60*60*1000 - 1, "", zset![Q14Output(1, 1, BincodeDecimal(Decimal::new(1_816_000_000, 3)), BidTimeType::Other, 8*60*60*1000 - 1, ArcStr::new(), 0) => 1])]
    #[case::counts_cs_in_extra(2_000_000, 0, "cause I can't calculate has four of them.", zset![Q14Output(1, 1, BincodeDecimal(Decimal::new(1_816_000_000, 3)), BidTimeType::Night, 0, String::from("cause I can't calculate has four of them.").into(), 4) => 1])]
    fn test_q14(
        #[case] price: usize,
        #[case] date_time: u64,
        #[case] extra: &str,
        #[case] expected_zset: OrdZSet<Q14Output, isize>,
    ) {
        let input_vecs = vec![vec![(
            Event::Bid(Bid {
                price,
                date_time,
                extra: String::from(extra).into(),
                ..make_bid()
            }),
            1,
        )]]
        .into_iter();

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let mut expected_output = vec![expected_zset].into_iter();

            let output = q14(stream);

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
