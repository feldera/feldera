//! Find connected components of sibling maps sharing expensive expressions.
use crate::projection_key::{row_parameter, ProjectionKey};
use feldera_sql_ir::{CircuitEditor, Error, NodeId};
use indexmap::{IndexMap, IndexSet};
use std::collections::HashMap;
use std::hash::Hash;

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
pub fn map_fusion_groups<K: Eq + Hash>(maps: &[Vec<K>]) -> Vec<Vec<usize>> {
    let mut parents: Vec<_> = (0..maps.len()).collect();
    let mut first_map = HashMap::new();
    for (map, fingerprints) in maps.iter().enumerate() {
        for fingerprint in fingerprints {
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

/// Each sibling contributes its parameter followed by its minimal expensive
/// fragments. A key that needs the general printer requires comparing the whole
/// group there, since it could match a native key in another member.
pub fn sibling_map_groups(
    edit: &CircuitEditor,
    mut roots: &[NodeId],
    fragment_counts: &[usize],
) -> Result<Option<Vec<Vec<usize>>>, Error> {
    let mut rows = IndexSet::new();
    let mut maps = Vec::with_capacity(fragment_counts.len());
    for &count in fragment_counts {
        let (parameter, rest) = roots
            .split_first()
            .ok_or_else(|| Error::new("missing sibling map parameter"))?;
        let (fragments, remaining) = rest
            .split_at_checked(count)
            .ok_or_else(|| Error::new("missing sibling map fragments"))?;
        roots = remaining;
        let parameter = row_parameter(edit, *parameter)?;
        let Some(row) = ProjectionKey::parameter_type(edit, parameter)? else {
            return Ok(None);
        };
        let (row, _) = rows.insert_full(row);
        let mut keys = Vec::with_capacity(count);
        for &fragment in fragments {
            let Some(key) = ProjectionKey::read(edit, edit.expression(fragment)?, parameter)?
            else {
                return Ok(None);
            };
            keys.push((row, key));
        }
        maps.push(keys);
    }
    if !roots.is_empty() {
        return Err(Error::new("unexpected trailing sibling map fragments"));
    }
    Ok(Some(map_fusion_groups(&maps)))
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
