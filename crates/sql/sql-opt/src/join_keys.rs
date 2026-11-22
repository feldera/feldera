//! Derive collection keys for binary and star joins from the columns copied by
//! their output closure. Multiple columns can name the same value of a key.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "UPPERCASE")]
enum Part {
    None,
    Index,
    Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
struct Column {
    part: Part,
    field: usize,
}

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

    fn project(
        &self,
        value_parameter: usize,
        copies: &BTreeMap<(usize, Column), BTreeSet<Column>>,
    ) -> Option<Self> {
        let sets = self
            .sets
            .iter()
            .map(|set| {
                let mut output = BTreeSet::new();
                for column in &set.columns {
                    // Parameter zero is the shared join index. Each other
                    // parameter supplies the value tuple of one input.
                    let parameter = if column.part == Part::Index {
                        0
                    } else {
                        value_parameter
                    };
                    let source = Column {
                        part: Part::None,
                        field: column.field,
                    };
                    if let Some(columns) = copies.get(&(parameter, source)) {
                        output.extend(columns);
                    }
                }
                (!output.is_empty()).then_some(EquivalentColumns { columns: output })
            })
            .collect::<Option<_>>()?;
        Some(Self { sets })
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

#[derive(Deserialize)]
struct ColumnCopy {
    parameter: usize,
    source: Column,
    output: Column,
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
        let mut copies: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        for copy in self.copies {
            copies
                .entry((copy.parameter, copy.source))
                .or_default()
                .insert(copy.output);
        }
        let unique: Vec<_> = self
            .inputs
            .iter()
            .map(|keys| keys.iter().any(KeyColumns::within_index))
            .collect();
        let mut result = Vec::new();
        let mut combinations = vec![KeyColumns::default()];
        for (input, keys) in self.inputs.iter().enumerate() {
            let projected = minimal(
                keys.iter()
                    .filter_map(|key| key.project(input + 1, &copies))
                    .collect(),
            );
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
