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
use arcstr::ArcStr;
use dbsp::{
    algebra::MulByRef,
    operator::FilterMap,
    trace::{ord::OrdKeySpine, Batch, BatchReader, Builder, Cursor},
    Circuit, DBData, DBWeight, OrdIndexedZSet, OrdZSet, Stream,
};
use std::{cmp::Ordering, collections::BinaryHeap};
// use std::ops::Range;

struct TopkElem<K, R>(K, R);

impl<K, R> PartialEq for TopkElem<K, R>
where
    K: PartialEq,
    R: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl<K, R> Eq for TopkElem<K, R>
where
    K: Eq,
    R: Eq,
{
}

impl<K, R> PartialOrd for TopkElem<K, R>
where
    K: PartialOrd,
    R: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.1
            .partial_cmp(&other.1)
            .map(Ordering::reverse)
            .and_then(|ordering| Some(ordering.then(self.0.partial_cmp(&other.0)?)))
    }
}

impl<K, R> Ord for TopkElem<K, R>
where
    K: Ord,
    R: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.1
            .cmp(&other.1)
            .reverse()
            .then_with(|| self.0.cmp(&other.0))
    }
}

// TODO: Hash collections
fn hashjoin<C, F, K, V1, V2, V3, R1, R2, R3>(
    lhs: &Stream<C, OrdIndexedZSet<K, V1, R1>>,
    rhs: &Stream<C, OrdIndexedZSet<K, V2, R2>>,
    join: F,
) -> Stream<C, OrdIndexedZSet<K, V3, R3>>
where
    F: Fn(&K, &V1, &V2) -> V3 + Clone + 'static,
    K: DBData,
    V1: DBData,
    V2: DBData,
    V3: DBData,
    R1: DBWeight + MulByRef<R2, Output = R3>,
    R2: DBWeight,
    R3: DBWeight,
{
    todo!()
}

// TODO: Proper topk operator
pub fn mentioned_people(
    events: &Stream<Circuit<()>, OrdZSet<PersonalNetworkGkgEntry, i32>>,
) -> Stream<Circuit<()>, OrdZSet<ArcStr, i32>> {
    events
        .flat_map(|event| event.people.clone())
        .trace::<OrdKeySpine<ArcStr, (), i32>>()
        .apply_named("TopK", |people| {
            let mut topk = BinaryHeap::with_capacity(10);

            let mut cursor = people.cursor();
            while cursor.key_valid() {
                let person = cursor.key().clone();
                let mut mentions = 0;
                cursor.map_times(|_, &weight| mentions += weight);

                if mentions != 0 {
                    topk.push(TopkElem(person, mentions));
                    if topk.len() > 10 {
                        topk.pop();
                    }
                }

                cursor.step_key();
            }

            let mut builder =
                <OrdZSet<ArcStr, i32> as Batch>::Builder::with_capacity((), topk.len());
            builder.extend(
                topk.into_sorted_vec()
                    .into_iter()
                    .map(|TopkElem(person, weight)| (person, weight)),
            );
            builder.done()
        })
}

// The default query uses the target of `Tsipras` and the date range of
// `20150302000000..20150304000000`
pub fn personal_network(
    target: ArcStr,
    // date_range: Range<u64>,
    events: &Stream<Circuit<()>, OrdZSet<PersonalNetworkGkgEntry, i32>>,
) -> Stream<Circuit<()>, OrdZSet<(ArcStr, ArcStr), i32>> {
    // Filter out events outside of our date range and that don't mention our target
    let relevant_events = events.filter(move |entry| entry.people.contains(&target)); // date_range.contains(&entry.date) &&

    let forward_events =
        relevant_events.index_with(|entry| (entry.id.clone(), entry.people.clone()));
    let flattened = relevant_events
        .flat_map(|event| {
            event
                .people
                .iter()
                .map(|person| (event.id.clone(), person.clone()))
                .collect::<Vec<_>>()
        })
        .index();

    // TODO: Hashjoin
    flattened
        .join_generic::<(), _, _, OrdZSet<_, _>, _>(&forward_events, |_id, a, people| {
            people
                .iter()
                .map(|b| ((a.clone(), b.clone()), ()))
                .collect::<Vec<_>>()
        })
        // TODO: Is there a better thing to do other than integration?
        .integrate()
}
