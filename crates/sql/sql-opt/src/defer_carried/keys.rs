//! Choose a key whose values survive the entire left-join chain. Among
//! surviving keys, prefer the one that leaves the most columns to defer.
use super::{Column, Part};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainKeys {
    /// In Java's key order (fewest values first), then equivalence-set and column order.
    keys: Vec<Vec<Vec<Column>>>,
    carried: HashSet<Column>,
    read: HashSet<Column>,
    length: i32,
    end_width: i32,
    index_width: Option<usize>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ChainKeyChoice {
    key: Option<usize>,
    worthwhile: bool,
}

pub fn choose_chain_key(input: &ChainKeys) -> ChainKeyChoice {
    let unread: HashSet<_> = input.carried.difference(&input.read).copied().collect();
    let mut best = None;
    let mut most_deferred = 0;
    for (index, key) in input.keys.iter().enumerate() {
        // Each equivalence set represents one key value. Use its first surviving copy.
        let Some(columns) = key
            .iter()
            .map(|set| set.iter().find(|col| input.carried.contains(col)).copied())
            .collect::<Option<HashSet<_>>>()
        else {
            continue;
        };
        // A zero-column key describes a singleton collection. The lookup
        // operator needs at least one column on which to join.
        let deferred = if columns.is_empty() {
            0
        } else {
            unread.difference(&columns).count()
        };
        if best.is_none() || deferred > most_deferred {
            best = Some(index);
            most_deferred = deferred;
        }
    }
    let worthwhile = best.is_some_and(|index| {
        let key = &input.keys[index];
        let reuse = input.index_width == Some(key.len())
            && (0..key.len()).all(|field| {
                let column = Column {
                    part: Part::Index,
                    field,
                };
                key.iter().any(|set| set.contains(&column))
            });
        // Preserve the Java heuristic's signed 32-bit arithmetic on wide rows.
        let deferred = most_deferred as i32;
        let added = if reuse {
            input.end_width.wrapping_sub(deferred)
        } else {
            input.end_width.wrapping_add(key.len() as i32)
        };
        deferred != 0 && input.length.wrapping_mul(deferred) > added
    });
    ChainKeyChoice {
        key: best,
        worthwhile,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                worthwhile: true
            }
        );
        facts.index_width = None;
        assert_eq!(
            choose_chain_key(&facts),
            ChainKeyChoice {
                key: Some(1),
                worthwhile: false
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
            read: HashSet::new(),
            length: 3,
            end_width: 2,
            index_width: None,
        };
        assert_eq!(
            choose_chain_key(&facts),
            ChainKeyChoice {
                key: Some(0),
                worthwhile: false
            }
        );
        facts.keys = vec![vec![]];
        assert_eq!(
            choose_chain_key(&facts),
            ChainKeyChoice {
                key: Some(0),
                worthwhile: false
            }
        );
    }
}
