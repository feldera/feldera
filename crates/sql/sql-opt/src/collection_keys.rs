//! Derive keys through filters, projections and joins. Multiple columns can
//! name the same value of a key.
use crate::lineage::{Column, ColumnCopy, Part};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
struct EquivalentColumns {
    columns: BTreeSet<Column>,
}

#[derive(Clone, Default, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct KeyColumns {
    sets: BTreeSet<EquivalentColumns>,
}

impl KeyColumns {
    fn within_index(&self) -> bool {
        self.sets
            .iter()
            .all(|set| set.columns.iter().any(|c| c.part == Part::Index))
    }

    fn project(&self, mut copy_to: impl FnMut(Column, &mut BTreeSet<Column>)) -> Option<Self> {
        let mut sets = BTreeSet::new();
        for set in &self.sets {
            let mut columns = BTreeSet::new();
            for &alias in &set.columns {
                copy_to(alias, &mut columns);
            }
            if columns.is_empty() {
                return None;
            }
            sets.insert(EquivalentColumns { columns });
        }
        Some(Self { sets })
    }
}

/// The join closure receives a shared index followed by the value of each input.
/// A ranked output keeps the index and computes new value fields from parameter one.
#[derive(Clone, Copy, Deserialize)]
pub enum KeyProjection {
    Row,
    Ranked,
    #[serde(skip)]
    Join {
        value_parameter: usize,
    },
}

type Copies = BTreeMap<(usize, Column), BTreeSet<Column>>;

fn copies_by_source(copies: &[ColumnCopy]) -> Copies {
    let mut result = Copies::new();
    for copy in copies {
        result
            .entry((copy.parameter, copy.source))
            .or_default()
            .insert(copy.output);
    }
    result
}

impl KeyProjection {
    fn map(self, keys: &[KeyColumns], copies: &Copies) -> Vec<KeyColumns> {
        let copy_to = |column: Column, output: &mut BTreeSet<Column>| {
            let field = Column {
                part: Part::None,
                field: column.field,
            };
            let source = match self {
                Self::Row => (0, column),
                Self::Ranked if column.part == Part::Index => {
                    output.insert(column);
                    return;
                }
                Self::Ranked => (1, field),
                Self::Join { value_parameter } => (
                    if column.part == Part::Index {
                        0
                    } else {
                        value_parameter
                    },
                    field,
                ),
            };
            if let Some(columns) = copies.get(&source) {
                output.extend(columns);
            }
        };
        minimal(keys.iter().filter_map(|key| key.project(copy_to)).collect())
    }

    /// Keep keys for which every value reaches at least one output column.
    pub fn keys(self, input: &[KeyColumns], copies: &[ColumnCopy]) -> Vec<KeyColumns> {
        if input.is_empty() {
            return Vec::new();
        }
        self.map(input, &copies_by_source(copies))
    }
}

/// Match the Java key budget, including truncation before superset pruning.
fn minimal(mut keys: Vec<KeyColumns>) -> Vec<KeyColumns> {
    keys.sort_unstable_by(|a, b| {
        a.sets
            .len()
            .cmp(&b.sets.len())
            .then_with(|| a.sets.cmp(&b.sets))
    });
    keys.dedup();
    keys.truncate(64);
    let mut result: Vec<KeyColumns> = Vec::new();
    for key in keys {
        if !result
            .iter()
            .any(|smaller| smaller.sets.len() < key.sets.len() && smaller.sets.is_subset(&key.sets))
        {
            result.push(key);
        }
    }
    result
}

/// Input keys are already minimal. Fixing any alias fixes its entire key value.
pub fn filter_keys(mut keys: Vec<KeyColumns>, fixed: &BTreeSet<Column>) -> Vec<KeyColumns> {
    if fixed.is_empty() {
        return keys;
    }
    for key in &mut keys {
        key.sets.retain(|set| set.columns.is_disjoint(fixed));
    }
    minimal(keys)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinKeys {
    inputs: Vec<Vec<KeyColumns>>,
    copies: Vec<ColumnCopy>,
    left_join: bool,
}

impl JoinKeys {
    pub fn derive(self) -> Vec<KeyColumns> {
        let copies = copies_by_source(&self.copies);
        let unique: Vec<_> = self
            .inputs
            .iter()
            .map(|keys| keys.iter().any(KeyColumns::within_index))
            .collect();
        let mut result = Vec::new();
        let mut combinations = vec![KeyColumns::default()];
        for (input, keys) in self.inputs.iter().enumerate() {
            let projected = KeyProjection::Join {
                value_parameter: input + 1,
            }
            .map(keys, &copies);
            // A side's key alone survives when every other input matches at
            // most once. An unmatched left row has NULLs on the right.
            if (!self.left_join || input == 0)
                && unique.iter().enumerate().all(|(i, one)| i == input || *one)
            {
                result.extend_from_slice(&projected);
            }
            let mut combined = Vec::new();
            for prefix in &combinations {
                for key in &projected {
                    combined.push(KeyColumns {
                        sets: prefix.sets.union(&key.sets).cloned().collect(),
                    });
                }
            }
            combinations = minimal(combined);
        }
        result.extend(combinations);
        minimal(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(part: Part, field: usize) -> Column {
        Column { part, field }
    }

    fn key(columns: &[Column]) -> KeyColumns {
        KeyColumns {
            sets: columns
                .iter()
                .map(|&c| EquivalentColumns {
                    columns: BTreeSet::from([c]),
                })
                .collect(),
        }
    }

    #[test]
    fn java_caps_keys_before_dropping_supersets() {
        let empty = KeyColumns::default();
        let first = column(Part::Value, 0);
        let unrelated = key(&[column(Part::Value, 100), column(Part::Value, 101)]);
        let mut keys = vec![key(&[first]), unrelated];
        for n in 1..=64 {
            keys.push(key(&[first, column(Part::Value, n)]));
        }
        // The last independent key falls beyond the first 64 sorted keys.
        assert_eq!(minimal(keys), [key(&[first])]);
        assert_eq!(minimal(vec![key(&[first]), empty.clone()]), [empty]);
    }

    #[test]
    fn fixing_an_alias_reduces_keys_and_preserves_the_single_row_key() {
        let index = column(Part::Index, 0);
        let alias = column(Part::Value, 2);
        let other = column(Part::Value, 0);
        let aliases = EquivalentColumns {
            columns: BTreeSet::from([index, alias]),
        };
        let mut compound = key(&[other]);
        compound.sets.insert(aliases.clone());
        let fixed = BTreeSet::from([alias]);
        assert_eq!(filter_keys(vec![compound], &fixed), [key(&[other])]);

        let unique = KeyColumns {
            sets: BTreeSet::from([aliases]),
        };
        assert_eq!(
            filter_keys(vec![unique, key(&[other])], &fixed),
            [KeyColumns::default()]
        );
        assert!(filter_keys(Vec::new(), &fixed).is_empty());
    }

    #[test]
    fn ranked_values_and_row_fields_read_different_parameters() {
        let index = column(Part::Index, 0);
        let value = column(Part::Value, 0);
        let moved = column(Part::Value, 2);
        let plain = column(Part::None, 0);
        let copies = [
            ColumnCopy {
                parameter: 0,
                source: value,
                output: plain,
            },
            ColumnCopy {
                parameter: 1,
                source: plain,
                output: moved,
            },
        ];
        let keys = [key(&[index, value]), key(&[column(Part::Value, 1)])];
        assert_eq!(
            KeyProjection::Ranked.keys(&keys, &copies),
            [key(&[index, moved])]
        );
        assert!(KeyProjection::Row.keys(&keys, &copies).is_empty());
        assert_eq!(
            KeyProjection::Row.keys(&[key(&[value])], &copies),
            [key(&[plain])]
        );
        assert_eq!(
            KeyProjection::Row.keys(&[KeyColumns::default()], &[]),
            [KeyColumns::default()]
        );
    }

    #[test]
    fn join_keys_follow_shared_index_copies_and_preserve_the_unmatched_left_row() {
        let ix = column(Part::Index, 0);
        let val = column(Part::Value, 0);
        let out = |field| column(Part::None, field);
        let layout = || {
            vec![
                ColumnCopy {
                    parameter: 0,
                    source: out(0),
                    output: out(2),
                },
                ColumnCopy {
                    parameter: 0,
                    source: out(0),
                    output: out(3),
                },
                ColumnCopy {
                    parameter: 2,
                    source: out(0),
                    output: out(1),
                },
            ]
        };
        let derive = |left_join| {
            JoinKeys {
                inputs: vec![vec![key(&[ix])], vec![key(&[val])]],
                copies: layout(),
                left_join,
            }
            .derive()
        };
        // A right row matches at most one left row. An inner join may keep
        // the right key alone. Multiple unmatched left rows can share a NULL.
        assert_eq!(derive(false), [key(&[out(1)])]);
        let both = KeyColumns {
            sets: BTreeSet::from([
                EquivalentColumns {
                    columns: BTreeSet::from([out(1)]),
                },
                EquivalentColumns {
                    columns: BTreeSet::from([out(2), out(3)]),
                },
            ]),
        };
        assert_eq!(derive(true), [both]);

        let star = JoinKeys {
            inputs: vec![
                vec![key(&[ix])],
                vec![key(&[val])],
                vec![KeyColumns::default()],
            ],
            copies: layout(),
            left_join: false,
        };
        assert_eq!(star.derive(), [key(&[out(1)])]);
    }
}
