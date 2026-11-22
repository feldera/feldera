//! Choose which timestamp each temporal filter should index when several
//! filters can share an input. A filter may join only one sharing group.
use indexmap::IndexMap;
use serde::Deserialize;
use std::cmp::Reverse;
use std::hash::Hash;

#[derive(Deserialize)]
pub struct TemporalCandidate<K = String> {
    pub fingerprint: K,
    pub conjunct: usize,
}

/// Each top-level conjunct either supplies a timestamp bound or has no
/// temporal classification. Bounds with the same fingerprint share a window.
#[derive(Deserialize)]
pub struct TemporalBound {
    pub fingerprint: String,
    pub lower: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct TimestampPartition<'a> {
    pub first: bool,
    pub timestamps: Vec<&'a str>,
}

/// Apply these stable partitions in sequence. Rebuilding an AND can insert
/// nullable casts, so later partitions inspect the rebuilt expression tree.
pub(super) fn temporal_filter_order<'a>(
    bounds: &'a [Option<TemporalBound>],
    shared: Option<&'a str>,
) -> Vec<TimestampPartition<'a>> {
    let mut bounded_below = IndexMap::<&str, bool>::new();
    for bound in bounds.iter().flatten() {
        *bounded_below.entry(&bound.fingerprint).or_default() |= bound.lower;
    }
    let mut partitions = Vec::new();
    let trailing: Vec<_> = bounded_below
        .keys()
        .copied()
        .filter(|k| Some(*k) != shared)
        .collect();
    if !trailing.is_empty() {
        partitions.push(TimestampPartition {
            first: false,
            timestamps: trailing,
        });
    }
    if let Some(key) = shared {
        partitions.push(TimestampPartition {
            first: true,
            timestamps: vec![key],
        });
    }
    let unbounded: Vec<_> = bounded_below
        .into_iter()
        .filter_map(|(key, lower)| (!lower && Some(key) != shared).then_some(key))
        .collect();
    if !unbounded.is_empty() {
        partitions.push(TimestampPartition {
            first: false,
            timestamps: unbounded,
        });
    }
    partitions
}

/// Each group lists (filter ordinal, conjunct ordinal) in circuit order.
/// Choose the largest remaining group, breaking ties by the fingerprint's
/// first occurrence across all filters.
pub fn shared_temporal_inputs<K: Eq + Hash>(
    filters: &[Vec<TemporalCandidate<K>>],
    threshold: usize,
) -> Vec<Vec<(usize, usize)>> {
    if threshold == 0 {
        return Vec::new();
    }
    let mut groups = IndexMap::<&K, IndexMap<usize, usize>>::new();
    for (filter, candidates) in filters.iter().enumerate() {
        for candidate in candidates {
            groups
                .entry(&candidate.fingerprint)
                .or_default()
                .entry(filter)
                .or_insert(candidate.conjunct);
        }
    }
    let mut chosen = Vec::new();
    while let Some((index, members)) = groups
        .values()
        .enumerate()
        .max_by_key(|(index, members)| (members.len(), Reverse(*index)))
    {
        if members.len() < threshold {
            break;
        }
        let (_, members) = groups.shift_remove_index(index).expect("selected group");
        for filter in members.keys() {
            for candidate in &filters[*filter] {
                if let Some(other) = groups.get_mut(&candidate.fingerprint) {
                    other.shift_remove(filter);
                }
            }
        }
        chosen.push(members.into_iter().collect());
    }
    chosen
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_window_precedes_plain_predicates_and_upper_only_windows_run_last() {
        let bound = |name: &str, lower| {
            Some(TemporalBound {
                fingerprint: name.into(),
                lower,
            })
        };
        let conjuncts = [
            None,
            bound("upper-only", false),
            bound("both", false),
            bound("shared", false),
            bound("both", true),
        ];
        assert_eq!(
            temporal_filter_order(&conjuncts, Some("shared")),
            [
                TimestampPartition {
                    first: false,
                    timestamps: vec!["upper-only", "both"]
                },
                TimestampPartition {
                    first: true,
                    timestamps: vec!["shared"]
                },
                TimestampPartition {
                    first: false,
                    timestamps: vec!["upper-only"]
                },
            ]
        );
        assert_eq!(
            temporal_filter_order(&conjuncts, None),
            [
                TimestampPartition {
                    first: false,
                    timestamps: vec!["upper-only", "both", "shared"]
                },
                TimestampPartition {
                    first: false,
                    timestamps: vec!["upper-only", "shared"]
                },
            ]
        );
    }

    #[test]
    fn competing_windows_choose_the_largest_remaining_input_in_first_use_order() {
        let filter = |pairs: &[(&str, usize)]| {
            pairs
                .iter()
                .map(|&(fingerprint, conjunct)| TemporalCandidate {
                    fingerprint: fingerprint.to_owned(),
                    conjunct,
                })
                .collect()
        };
        let filters = [
            filter(&[("b", 3), ("a", 0), ("b", 4)]),
            filter(&[("a", 6), ("c", 1)]),
            filter(&[("b", 0), ("c", 2)]),
            filter(&[("c", 5)]),
            filter(&[("b", 2)]),
        ];
        assert_eq!(
            shared_temporal_inputs(&filters, 2),
            [vec![(0, 3), (2, 0), (4, 2)], vec![(1, 1), (3, 5)]]
        );
        assert!(shared_temporal_inputs(&filters, 4).is_empty());
        assert!(shared_temporal_inputs(&filters, 0).is_empty());
    }
}
