use super::NexmarkStream;
use crate::model::Event;
use dbsp::{
    utils::{Tup3, Tup4},
    OrdZSet, RootCircuit, Stream,
};

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
const CATEGORY_OF_INTEREST: u64 = 10;

type Q3Stream = Stream<RootCircuit, OrdZSet<Tup4<String, String, String, u64>>>;

pub fn q3(input: NexmarkStream) -> Q3Stream {
    // Select auctions of interest and index them by seller id.
    let auction_by_seller = input.flat_map_index(|event| match event {
        Event::Auction(a) if a.category == CATEGORY_OF_INTEREST => Some((a.seller, a.id)),
        _ => None,
    });

    // Select people from states of interest and index them by person id.
    let person_by_id = input.flat_map_index(|event| match event {
        Event::Person(p) => match STATES_OF_INTEREST.contains(&p.state.as_str()) {
            true => Some((p.id, Tup3(p.name.clone(), p.city.clone(), p.state.clone()))),
            false => None,
        },
        _ => None,
    });

    // In the future, it won't be necessary to specify type arguments to join.
    auction_by_seller.join(
        &person_by_id,
        |_seller, &auction_id, Tup3(name, city, state)| {
            Tup4(
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
    use crate::{
        generator::tests::{make_auction, make_person},
        model::{Auction, Person},
    };
    use dbsp::{utils::Tup2, OrdZSet, RootCircuit, ZWeight};

    #[test]
    fn test_q3_people() {
        let input_vecs: Vec<Vec<Tup2<Event, ZWeight>>> = vec![
            vec![
                Tup2(
                    Event::Person(Person {
                        id: 1,
                        name: String::from("NL Seller"),
                        state: String::from("NL"),
                        ..make_person()
                    }),
                    1,
                ),
                Tup2(
                    Event::Person(Person {
                        id: 2,
                        name: String::from("CA Seller"),
                        state: String::from("CA"),
                        ..make_person()
                    }),
                    1,
                ),
                Tup2(
                    Event::Person(Person {
                        id: 3,
                        name: String::from("ID Seller"),
                        state: String::from("ID"),
                        ..make_person()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 999,
                        seller: 2,
                        category: CATEGORY_OF_INTEREST,
                        ..make_auction()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 452,
                        seller: 3,
                        category: CATEGORY_OF_INTEREST,
                        ..make_auction()
                    }),
                    1,
                ),
            ],
            vec![
                // This person is selling in OR, but a different category.
                Tup2(
                    Event::Person(Person {
                        id: 4,
                        name: String::from("OR Seller"),
                        state: String::from("OR"),
                        ..make_person()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 999,
                        seller: 4,
                        category: CATEGORY_OF_INTEREST + 1,
                        ..make_auction()
                    }),
                    1,
                ),
                // This person is selling in OR in the category of interest.
                Tup2(
                    Event::Person(Person {
                        id: 5,
                        name: String::from("OR Seller"),
                        state: String::from("OR"),
                        ..make_person()
                    }),
                    1,
                ),
                Tup2(
                    Event::Auction(Auction {
                        id: 333,
                        seller: 5,
                        category: CATEGORY_OF_INTEREST,
                        ..make_auction()
                    }),
                    1,
                ),
            ],
        ];

        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = q3(stream);

            let mut expected_output = vec![
                OrdZSet::from_keys(
                    (),
                    vec![
                        Tup2(
                            Tup4(
                                String::from("CA Seller"),
                                String::from("Phoenix"),
                                String::from("CA"),
                                999,
                            ),
                            1,
                        ),
                        Tup2(
                            Tup4(
                                String::from("ID Seller"),
                                String::from("Phoenix"),
                                String::from("ID"),
                                452,
                            ),
                            1,
                        ),
                    ],
                ),
                OrdZSet::from_keys(
                    (),
                    vec![Tup2(
                        Tup4(
                            String::from("OR Seller"),
                            String::from("Phoenix"),
                            String::from("OR"),
                            333,
                        ),
                        1,
                    )],
                ),
            ]
            .into_iter();

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            Ok(input_handle)
        })
        .unwrap();

        for mut vec in input_vecs.into_iter() {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }
}
