use super::NexmarkStream;
use crate::model::{Bid, Event};
use dbsp::{algebra::UnimplementedSemigroup, operator::Fold, OrdZSet, RootCircuit, Stream};

//
// Query 18: Find last bid (Not in original suite)
//
// What's a's last bid for bidder to auction?
// Illustrates a Deduplicate query.
//
/// ```sql
/// CREATE TABLE discard_sink (
///     auction  BIGINT,
///     bidder  BIGINT,
///     price  BIGINT,
///     channel  VARCHAR,
///     url  VARCHAR,
///     dateTime  TIMESTAMP(3),
///     extra  VARCHAR
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT auction, bidder, price, channel, url, dateTime, extra
///  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY dateTime DESC) AS rank_number
///        FROM bid)
///  WHERE rank_number <= 1;
/// ```

type Q18Stream = Stream<RootCircuit, OrdZSet<Bid>>;

pub fn q18(input: NexmarkStream) -> Q18Stream {
    let bids_by_auction_bidder = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some(((b.auction, b.bidder), b.clone())),
        _ => None,
    });

    bids_by_auction_bidder
        .aggregate(<Fold<_, _, UnimplementedSemigroup<_>, _, _>>::new(
            Bid::default(),
            |top: &mut Bid, val: &Bid, _w| {
                if val.date_time > top.date_time {
                    *top = val.clone();
                }
            },
        ))
        .map(|((_, _), bid)| bid.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{generator::tests::make_bid, model::Bid};
    use dbsp::{utils::Tup2, zset};
    use rstest::rstest;

    #[rstest]
    #[case::last_bid_for_single_bidder_single_auction(
        vec![vec![
            Bid {
                auction: 1,
                bidder: 1,
                price: 10,
                date_time: 1_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 1,
                price: 20,
                date_time: 3_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 1,
                price: 30,
                date_time: 2_000,
                ..make_bid()
            },
        ], vec![
            Bid {
                auction: 1,
                bidder: 1,
                price: 50,
                date_time: 4_000,
                ..make_bid()
            },
        ]],
        vec![zset! {
            Bid {
                auction: 1,
                bidder: 1,
                price: 20,
                date_time: 3_000,
                ..make_bid()
            } => 1,
        }, zset! {
            Bid {
                auction: 1,
                bidder: 1,
                price: 20,
                date_time: 3_000,
                ..make_bid()
            } => -1,
            Bid {
                auction: 1,
                bidder: 1,
                price: 50,
                date_time: 4_000,
                ..make_bid()
            } => 1,
        }]
    )]
    #[case::last_bid_for_multi_bidders_single_auction(
        vec![vec![
            Bid {
                auction: 1,
                bidder: 1,
                price: 10,
                date_time: 1_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 2,
                price: 20,
                date_time: 3_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 1,
                price: 30,
                date_time: 2_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 2,
                price: 40,
                date_time: 4_000,
                ..make_bid()
            },
        ], vec! [
            Bid {
                auction: 1,
                bidder: 1,
                price: 40,
                date_time: 5_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 2,
                price: 50,
                date_time: 6_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 1,
                price: 70,
                date_time: 7_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 2,
                price: 80,
                date_time: 8_000,
                ..make_bid()
            },
        ]],
        vec![zset! {
            Bid {
                auction: 1,
                bidder: 1,
                price: 30,
                date_time: 2_000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                bidder: 2,
                price: 40,
                date_time: 4_000,
                ..make_bid()
            } => 1,
        }, zset! {
            Bid {
                auction: 1,
                bidder: 1,
                price: 30,
                date_time: 2_000,
                ..make_bid()
            } => -1,
            Bid {
                auction: 1,
                bidder: 2,
                price: 40,
                date_time: 4_000,
                ..make_bid()
            } => -1,
            Bid {
                auction: 1,
                bidder: 1,
                price: 70,
                date_time: 7_000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                bidder: 2,
                price: 80,
                date_time: 8_000,
                ..make_bid()
            } => 1,
        }]
    )]
    #[case::last_bid_for_multi_bidders_multi_auctions(
        vec![vec![
            Bid {
                auction: 1,
                bidder: 1,
                price: 10,
                date_time: 1_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 2,
                price: 20,
                date_time: 2_000,
                ..make_bid()
            },
            Bid {
                auction: 2,
                bidder: 1,
                price: 30,
                date_time: 3_000,
                ..make_bid()
            },
            Bid {
                auction: 2,
                bidder: 2,
                price: 40,
                date_time: 4_000,
                ..make_bid()
            },
        ], vec! [
            Bid {
                auction: 1,
                bidder: 1,
                price: 50,
                date_time: 5_000,
                ..make_bid()
            },
            Bid {
                auction: 1,
                bidder: 2,
                price: 60,
                date_time: 6_000,
                ..make_bid()
            },
            Bid {
                auction: 2,
                bidder: 1,
                price: 70,
                date_time: 7_000,
                ..make_bid()
            },
            Bid {
                auction: 2,
                bidder: 2,
                price: 80,
                date_time: 8_000,
                ..make_bid()
            },
        ]],
        vec![zset! {
            Bid {
                auction: 1,
                bidder: 1,
                price: 10,
                date_time: 1_000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                bidder: 2,
                price: 20,
                date_time: 2_000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 2,
                bidder: 1,
                price: 30,
                date_time: 3_000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 2,
                bidder: 2,
                price: 40,
                date_time: 4_000,
                ..make_bid()
            } => 1,
        }, zset! {
            Bid {
                auction: 1,
                bidder: 1,
                price: 10,
                date_time: 1_000,
                ..make_bid()
            } => -1,
            Bid {
                auction: 1,
                bidder: 2,
                price: 20,
                date_time: 2_000,
                ..make_bid()
            } => -1,
            Bid {
                auction: 2,
                bidder: 1,
                price: 30,
                date_time: 3_000,
                ..make_bid()
            } => -1,
            Bid {
                auction: 2,
                bidder: 2,
                price: 40,
                date_time: 4_000,
                ..make_bid()
            } => -1,
            Bid {
                auction: 1,
                bidder: 1,
                price: 50,
                date_time: 5_000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 1,
                bidder: 2,
                price: 60,
                date_time: 6_000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 2,
                bidder: 1,
                price: 70,
                date_time: 7_000,
                ..make_bid()
            } => 1,
            Bid {
                auction: 2,
                bidder: 2,
                price: 80,
                date_time: 8_000,
                ..make_bid()
            } => 1,
        }]
    )]
    fn test_q18(
        #[case] input_bid_batches: Vec<Vec<Bid>>,
        #[case] expected_zsets: Vec<OrdZSet<Bid>>,
    ) {
        let input_vecs = input_bid_batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|b| Tup2(Event::Bid(b), 1)).collect());

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q18(stream);

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
