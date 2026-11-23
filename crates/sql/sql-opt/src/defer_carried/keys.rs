//! Choose a key whose values survive the entire left-join chain. Among
//! surviving keys, prefer the one that leaves the most columns to defer.
use super::{Column, Part};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainKeys {
    /// In Java's key order (fewest values first), then equivalence-set and column order.
    keys: Vec<Vec<Vec<Column>>>,
    carried: BTreeSet<Column>,
    read: BTreeSet<Column>,
    length: i32,
    end_width: i32,
    index_width: Option<usize>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ChainKeyChoice {
    key: Option<usize>,
    lookup: Option<LookupColumns>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LookupColumns {
    reuse_index: bool,
    key: Vec<Column>,
    deferred: Vec<Column>,
}

pub fn choose_chain_key(input: &ChainKeys) -> ChainKeyChoice {
    select_key(
        &input.keys,
        input.index_width,
        &input.carried,
        &input.read,
        input.length,
        input.end_width,
    )
}

impl ChainKeyChoice {
    pub(super) fn profitable(&self) -> bool {
        self.lookup.is_some()
    }
}

pub(super) fn select_key(
    keys: &[Vec<Vec<Column>>],
    index_width: Option<usize>,
    carried: &BTreeSet<Column>,
    read: &BTreeSet<Column>,
    length: i32,
    end_width: i32,
) -> ChainKeyChoice {
    let unread: BTreeSet<_> = carried.difference(read).copied().collect();
    let mut best = None;
    let mut most_deferred = 0;
    for (index, key) in keys.iter().enumerate() {
        // Each equivalence set contributes its first surviving copy, in key order.
        let Some(columns) = key
            .iter()
            .map(|set| set.iter().find(|col| carried.contains(col)).copied())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        // A singleton's empty key cannot drive a lookup join.
        let deferred = if columns.is_empty() {
            0
        } else {
            let key_columns: BTreeSet<_> = columns.iter().copied().collect();
            unread.difference(&key_columns).count()
        };
        if best.is_none() || deferred > most_deferred {
            best = Some((index, columns));
            most_deferred = deferred;
        }
    }

    let Some((index, columns)) = best else {
        return ChainKeyChoice {
            key: None,
            lookup: None,
        };
    };
    let key = &keys[index];
    // A reused index dictates the order of the lookup fields. Each slot may
    // reach the chain output through an equivalent value column.
    let index_key: Option<Vec<_>> =
        index_width
            .filter(|&width| width == key.len())
            .and_then(|width| {
                (0..width)
                    .map(|field| {
                        let column = Column {
                            part: Part::Index,
                            field,
                        };
                        key.iter()
                            .rposition(|set| set.contains(&column))
                            .map(|i| columns[i])
                    })
                    .collect()
            });
    // Preserve the Java heuristic's signed 32-bit arithmetic on wide rows.
    let count = most_deferred as i32;
    let added = if index_key.is_some() {
        end_width.wrapping_sub(count)
    } else {
        end_width.wrapping_add(key.len() as i32)
    };
    let lookup = (count != 0 && length.wrapping_mul(count) > added).then(|| {
        let mut deferred = unread;
        for column in &columns {
            deferred.remove(column);
        }
        LookupColumns {
            reuse_index: index_key.is_some(),
            key: index_key.unwrap_or(columns),
            deferred: deferred.into_iter().collect(),
        }
    });
    ChainKeyChoice {
        key: Some(index),
        lookup,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_fields_follow_the_reused_index_or_the_chosen_key() {
        let index = |field| Column {
            part: Part::Index,
            field,
        };
        let value = |field| Column {
            part: Part::Value,
            field,
        };
        let mut facts = ChainKeys {
            keys: vec![vec![vec![index(1), value(3)], vec![index(0), value(1)]]],
            carried: [value(3), value(2), value(1), value(0)].into(),
            read: BTreeSet::new(),
            length: 5,
            end_width: 4,
            index_width: Some(2),
        };
        for (width, reuse_index, key) in [
            (Some(2), true, vec![value(1), value(3)]),
            (Some(1), false, vec![value(3), value(1)]),
        ] {
            facts.index_width = width;
            assert_eq!(
                choose_chain_key(&facts).lookup,
                Some(LookupColumns {
                    reuse_index,
                    key,
                    deferred: vec![value(0), value(2)],
                })
            );
        }
    }

    #[test]
    fn read_key_wins_and_reuses_an_equivalent_index() {
        let index = Column {
            part: Part::Index,
            field: 0,
        };
        let value = |field| Column {
            part: Part::Value,
            field,
        };
        let mut facts = ChainKeys {
            keys: vec![vec![vec![value(0)]], vec![vec![index, value(1)]]],
            carried: [value(0), value(1), value(2)].into(),
            read: [value(1)].into(),
            length: 2,
            end_width: 5,
            index_width: Some(1),
        };
        assert_eq!(
            choose_chain_key(&facts),
            ChainKeyChoice {
                key: Some(1),
                lookup: Some(LookupColumns {
                    reuse_index: true,
                    key: vec![value(1)],
                    deferred: vec![value(0), value(2)]
                })
            }
        );
        facts.index_width = None;
        assert_eq!(
            choose_chain_key(&facts),
            ChainKeyChoice {
                key: Some(1),
                lookup: None
            }
        );
        facts.carried.remove(&value(1));
        assert_eq!(choose_chain_key(&facts).key, Some(0));
    }

    #[test]
    fn ties_keep_the_first_key_and_break_even_savings_do_not_qualify() {
        let a = Column {
            part: Part::None,
            field: 0,
        };
        let b = Column {
            part: Part::None,
            field: 1,
        };
        let mut facts = ChainKeys {
            keys: vec![vec![vec![a]], vec![vec![b]]],
            carried: [a, b].into(),
            read: BTreeSet::new(),
            length: 3,
            end_width: 2,
            index_width: None,
        };
        assert_eq!(
            choose_chain_key(&facts),
            ChainKeyChoice {
                key: Some(0),
                lookup: None
            }
        );
        facts.keys = vec![vec![]];
        assert_eq!(
            choose_chain_key(&facts),
            ChainKeyChoice {
                key: Some(0),
                lookup: None
            }
        );
    }
}
