use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

/// Selection
///
/// Find bids with specific auction ids and show their bid price.
///
/// From [Nexmark q2.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q2.sql):
///
/// CREATE TABLE discard_sink (
///   auction  BIGINT,
///   price  BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;
const AUCTION_ID_MODULO: u64 = 123;

pub fn q2(input: NexmarkStream) -> Stream<Circuit<()>, OrdZSet<(u64, usize), isize>> {
    input.flat_map(|event| match event {
        Event::Bid(b) => match b.auction % AUCTION_ID_MODULO == 0 {
            true => Some((b.auction, b.price)),
            false => None,
        },
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexmark::{
        generator::{
            tests::{make_auction, make_bid, make_next_event, CannedEventGenerator},
            NextEvent,
        },
        model::Bid,
        NexmarkSource,
    };
    use crate::{trace::Batch, OrdZSet};
    use rand::rngs::mock::StepRng;
    use std::sync::mpsc;

    #[test]
    fn test_q2() {
        let canned_events: Vec<NextEvent> = vec![
            NextEvent {
                event: Event::Bid(Bid {
                    auction: AUCTION_ID_MODULO,
                    price: 99,
                    ..make_bid()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Bid(Bid {
                    auction: 125,
                    price: 101,
                    ..make_bid()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Auction(make_auction()),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Bid(Bid {
                    auction: 5 * AUCTION_ID_MODULO,
                    price: 125,
                    ..make_bid()
                }),
                ..make_next_event()
            },
        ];
        let (tx, _) = mpsc::channel();
        let source: NexmarkSource<StepRng, isize, OrdZSet<Event, isize>> =
            NexmarkSource::from_generator(CannedEventGenerator::new(canned_events), tx);

        let root = Circuit::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q2(input);

            output.inspect(move |e| {
                // Only the two bids with an auction (id) that is modulo
                // AUCTION_ID_MODULO are included in the tuple results of
                // id and price.
                assert_eq!(
                    e,
                    &OrdZSet::from_keys(
                        (),
                        vec![
                            ((AUCTION_ID_MODULO, 99), 1),
                            ((5 * AUCTION_ID_MODULO, 125), 1),
                        ]
                    )
                )
            });
        })
        .unwrap()
        .0;

        root.step().unwrap();
    }
}
