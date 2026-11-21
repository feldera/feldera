//! Choose which timestamp each temporal filter should index when several
//! filters can share an input. A filter may join only one sharing group.
use indexmap::IndexMap;
use serde::Deserialize;
use std::cmp::Reverse;

#[derive(Deserialize)]
pub struct TemporalCandidate {
    pub fingerprint: String,
    pub conjunct: usize,
}

/// Each group lists (filter ordinal, conjunct ordinal) in circuit order.
/// Choose the largest remaining group, breaking ties by the fingerprint's
/// first occurrence across all filters.
pub fn shared_temporal_inputs(
    filters: &[Vec<TemporalCandidate>],
    threshold: usize,
) -> Vec<Vec<(usize, usize)>> {
    if threshold == 0 {
        return Vec::new();
    }
    let mut groups = IndexMap::<&str, IndexMap<usize, usize>>::new();
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
                if let Some(other) = groups.get_mut(candidate.fingerprint.as_str()) {
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
    fn competing_windows_choose_the_largest_remaining_input_in_first_use_order() {
        let filter = |pairs: &[(&str, usize)]| {
            pairs
                .iter()
                .map(|&(fingerprint, conjunct)| TemporalCandidate {
                    fingerprint: fingerprint.into(),
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
