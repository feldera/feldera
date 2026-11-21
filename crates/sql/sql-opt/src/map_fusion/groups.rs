//! Find connected components of sibling maps sharing expensive expressions.
use indexmap::IndexMap;
use std::collections::HashMap;

fn root(parents: &mut [usize], mut member: usize) -> usize {
    while parents[member] != member {
        parents[member] = parents[parents[member]];
        member = parents[member];
    }
    member
}

/// Each integer names an expression fingerprint within one group of siblings.
/// Emit components and their members in first-map order. A singleton remains
/// eligible when it computes an expensive output column twice.
pub fn map_fusion_groups(maps: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let mut parents: Vec<_> = (0..maps.len()).collect();
    let mut first_map = HashMap::new();
    for (map, fingerprints) in maps.iter().enumerate() {
        for &fingerprint in fingerprints {
            let first = *first_map.entry(fingerprint).or_insert(map);
            if first != map {
                let first_root = root(&mut parents, first);
                let map_root = root(&mut parents, map);
                parents[map_root] = first_root;
            }
        }
    }

    let mut groups = IndexMap::<_, Vec<usize>>::new();
    for map in 0..maps.len() {
        groups.entry(root(&mut parents, map)).or_default().push(map);
    }
    groups.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn later_map_connects_nonadjacent_groups() {
        let maps = [vec![7, 7], vec![3], vec![4], vec![5], vec![4, 7], vec![3]];
        assert_eq!(
            map_fusion_groups(&maps),
            [vec![0, 2, 4], vec![1, 5], vec![3]]
        );
    }
}
