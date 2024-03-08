//! Based on the Network Analysis query from [GKG 2.0 Sample Queries](https://blog.gdeltproject.org/google-bigquery-gkg-2-0-sample-queries/)
//!
//! ```sql
//! SELECT a.name, b.name, COUNT(*) as count
//! FROM (
//!     FLATTEN(
//!         SELECT GKGRECORDID, UNIQUE(REGEXP_REPLACE(SPLIT(V2Persons, ';'), r',.*', ")) name
//!         FROM [gdelt-bq:gdeltv2.gkg]
//!         WHERE DATE > 20150302000000 and DATE < 20150304000000 and V2Persons like '%Tsipras%', name
//!     )
//! ) a
//! JOIN EACH (
//!     SELECT GKGRECORDID, UNIQUE(REGEXP_REPLACE(SPLIT(V2Persons, ';'), r',.*', ")) name
//!     FROM [gdelt-bq:gdeltv2.gkg]
//!     WHERE DATE > 20150302000000 and DATE < 20150304000000 and V2Persons like '%Tsipras%
//! ) b
//! ON a.GKGRECORDID = b.GKGRECORDID
//! WHERE a.name < b.name
//! GROUP EACH BY 1,2
//! ORDER BY 3 DESC
//! LIMIT 250
//! ```

use crate::data::PersonalNetworkGkgEntry;
use dbsp::{utils::Tup2, OrdZSet, RootCircuit, Stream};

pub fn personal_network(
    target: String,
    date_start: Option<u64>,
    date_end: Option<u64>,
    events: &Stream<RootCircuit, OrdZSet<PersonalNetworkGkgEntry>>,
) -> Stream<RootCircuit, OrdZSet<Tup2<String, String>>> {
    // Filter out events outside of our date range and that don't mention our target
    let events_filter: Box<dyn Fn(&PersonalNetworkGkgEntry) -> bool> = match (date_start, date_end)
    {
        (None, None) => Box::new(move |entry| entry.people.contains(&target)),
        (Some(start), None) => {
            Box::new(move |entry| entry.date >= start && entry.people.contains(&target))
        }
        (None, Some(end)) => {
            Box::new(move |entry| entry.date <= end && entry.people.contains(&target))
        }
        (Some(start), Some(end)) => Box::new(move |entry| {
            entry.date >= start && entry.date <= end && entry.people.contains(&target)
        }),
    };
    let relevant_events = events.filter(events_filter);

    let forward_events =
        relevant_events.map_index(|entry| (entry.id.clone(), entry.people.clone()));
    let flattened = relevant_events.flat_map_index(|event| {
        event
            .people
            .iter()
            .map(|person| (event.id.clone(), person.clone()))
            .collect::<Vec<_>>()
    });

    let joined = flattened
        .join_index(&forward_events, |_id, a, people| {
            people
                .iter()
                .filter(|&b| (a < b))
                .map(|b| (Tup2(a.clone(), b.clone()), ()))
                .collect::<Vec<_>>()
        })
        .map(|(Tup2(a, b), ())| Tup2(a.clone(), b.clone()));

    // expected.minus(&joined).gather(0).inspect(|errors| {
    //     let mut cursor = errors.cursor();
    //     while cursor.key_valid() {
    //         let mentions = cursor.weight();
    //         let (source, target) = cursor.key();
    //         println!(
    //             "error, {}: {source}, {target}, {mentions}",
    //             if mentions.is_positive() {
    //                 "missing"
    //             } else {
    //                 "added"
    //             },
    //         );
    //         cursor.step_key();
    //     }
    // });

    // TODO: topk 250
    // TODO: Is there a better thing to do other than integration?
    joined.integrate()
}
