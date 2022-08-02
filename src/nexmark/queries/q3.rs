use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

/// Local Item Suggestion
///
/// Who is selling in OR, ID or CA in category 10, and for what auction ids?
/// Illustrates an incremental join (using per-key state and timer) and filter.
///
/// From [Nexmark q3.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q3.sql)
///
/// CREATE TABLE discard_sink (
///   name  VARCHAR,
///   city  VARCHAR,
///   state  VARCHAR,
///   id  BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     P.name, P.city, P.state, A.id
/// FROM
///     auction AS A INNER JOIN person AS P on A.seller = P.id
/// WHERE
///     A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state =
/// 'CA');

const STATES_OF_INTEREST: &[&str] = &["OR", "ID", "CA"];
const CATEGORY_OF_INTEREST: usize = 10;

type Q3Stream = Stream<Circuit<()>, OrdZSet<(String, String, String, u64), isize>>;

pub fn q3(input: NexmarkStream) -> Q3Stream {
    // Select auctions of interest and index them by seller id.
    let auction_by_seller = input.flat_map_index(|event| match event {
        Event::Auction(a) if a.category == CATEGORY_OF_INTEREST => Some((a.seller, a.id)),
        _ => None,
    });

    // Select people from states of interest and index them by person id.
    let person_by_id = input.flat_map_index(|event| match event {
        Event::Person(p) => match STATES_OF_INTEREST.contains(&p.state.as_str()) {
            true => Some((p.id, (p.name.clone(), p.city.clone(), p.state.clone()))),
            false => None,
        },
        _ => None,
    });

    // In the future, it won't be necessary to specify type arguments to join.
    auction_by_seller.join::<(), _, _, _>(
        &person_by_id,
        |_seller, &auction_id, (name, city, state)| {
            (
                name.to_string(),
                city.to_string(),
                state.to_string(),
                auction_id,
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexmark::{
        generator::{
            tests::{make_auction, make_next_event, make_person, CannedEventGenerator},
            NextEvent,
        },
        model::{Auction, Person},
        NexmarkSource,
    };
    use crate::{circuit::Root, trace::ord::OrdZSet, trace::Batch};
    use rand::rngs::mock::StepRng;

    #[test]
    fn test_q3_people() {
        let canned_events: Vec<NextEvent> = vec![
            NextEvent {
                event: Event::Person(Person {
                    id: 1,
                    name: String::from("NL Seller"),
                    state: String::from("NL"),
                    ..make_person()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Person(Person {
                    id: 2,
                    name: String::from("CA Seller"),
                    state: String::from("CA"),
                    ..make_person()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Person(Person {
                    id: 3,
                    name: String::from("ID Seller"),
                    state: String::from("ID"),
                    ..make_person()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Auction(Auction {
                    id: 999,
                    seller: 2,
                    category: CATEGORY_OF_INTEREST,
                    ..make_auction()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Auction(Auction {
                    id: 452,
                    seller: 3,
                    category: CATEGORY_OF_INTEREST,
                    ..make_auction()
                }),
                ..make_next_event()
            },
        ];

        let source: NexmarkSource<StepRng, isize, OrdZSet<Event, isize>> =
            NexmarkSource::from_generator(CannedEventGenerator::new(canned_events));

        let root = Root::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q3(input);

            output.inspect(move |e| {
                assert_eq!(
                    e,
                    &OrdZSet::from_tuples(
                        (),
                        vec![
                            (
                                (
                                    (
                                        String::from("CA Seller"),
                                        String::from("Phoenix"),
                                        String::from("CA"),
                                        999,
                                    ),
                                    ()
                                ),
                                1
                            ),
                            (
                                (
                                    (
                                        String::from("ID Seller"),
                                        String::from("Phoenix"),
                                        String::from("ID"),
                                        452,
                                    ),
                                    ()
                                ),
                                1
                            ),
                        ]
                    )
                )
            });
        })
        .unwrap();

        root.step().unwrap();
    }
}
