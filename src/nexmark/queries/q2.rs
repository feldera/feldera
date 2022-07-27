use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

/// Selection
///
/// Find bids with specific auction ids and show their bid price.
/// See https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q2.sql
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
        generator::{tests::CannedEventGenerator, NextEvent},
        model::{Auction, Bid},
        NexmarkSource,
    };
    use crate::{circuit::Root, trace::ord::OrdZSet, trace::Batch};
    use rand::rngs::mock::StepRng;

    fn default_bid() -> Bid {
        Bid {
            auction: AUCTION_ID_MODULO,
            bidder: 0,
            price: 99,
            channel: String::from("my-channel"),
            url: String::from("https://example.com"),
            date_time: 0,
            extra: String::new(),
        }
    }

    fn default_auction() -> Auction {
        Auction {
            id: 1,
            item_name: String::from("item-name"),
            description: String::from("description"),
            initial_bid: 5,
            reserve: 10,
            date_time: 0,
            expires: 0,
            seller: 1,
            category: 1,
        }
    }

    fn default_next_event() -> NextEvent {
        NextEvent {
            wallclock_timestamp: 0,
            event_timestamp: 0,
            event: Event::Bid(default_bid()),
            watermark: 0,
        }
    }

    #[test]
    fn test_q2() {
        let canned_events: Vec<NextEvent> = vec![
            NextEvent {
                event: Event::Bid(Bid {
                    auction: AUCTION_ID_MODULO,
                    price: 99,
                    ..default_bid()
                }),
                ..default_next_event()
            },
            NextEvent {
                event: Event::Bid(Bid {
                    auction: 125,
                    price: 101,
                    ..default_bid()
                }),
                ..default_next_event()
            },
            NextEvent {
                event: Event::Auction(default_auction()),
                ..default_next_event()
            },
            NextEvent {
                event: Event::Bid(Bid {
                    auction: 5 * AUCTION_ID_MODULO,
                    price: 125,
                    ..default_bid()
                }),
                ..default_next_event()
            },
        ];
        let source: NexmarkSource<StepRng, isize, OrdZSet<Event, isize>> =
            NexmarkSource::from_generator(CannedEventGenerator::new(canned_events));

        let root = Root::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q2(input);

            output.inspect(move |e| {
                // Only the two bids with an auction (id) that is modulo
                // AUCTION_ID_MODULO are included in the tuple results of
                // id and price.
                assert_eq!(
                    e,
                    &OrdZSet::from_tuples(
                        (),
                        vec![
                            (((AUCTION_ID_MODULO, 99), ()), 1),
                            (((5 * AUCTION_ID_MODULO, 125), ()), 1),
                        ]
                    )
                )
            });
        })
        .unwrap();

        for _ in 0..1 {
            root.step().unwrap();
        }
    }
}
