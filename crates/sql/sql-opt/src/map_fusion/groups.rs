//! Find connected components of sibling maps sharing expensive expressions.
use super::{has_duplicate_expensive_field, map_fusion_candidate};
use crate::index_key_equivalence::{ComparisonDiagnostic, KeyComparisonError};
use crate::projection_key::{row_parameter, ProjectionKey};
use feldera_sql_expr::SimplifyWarning;
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

/// Emit connected components and their members in first-map order.
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

/// A fragment's fingerprint includes its enclosing parameter's printed type.
#[derive(Default)]
struct FragmentKeys {
    rows: IndexSet<ProjectionKey>,
}

impl FragmentKeys {
    fn read(
        &mut self,
        edit: &CircuitEditor,
        parameter: NodeId,
        fragments: impl Iterator<Item = NodeId>,
    ) -> Result<Option<Vec<(usize, ProjectionKey)>>, Error> {
        let parameter = row_parameter(edit, parameter)?;
        let Some(row) = ProjectionKey::parameter_type(edit, parameter)? else {
            return Ok(None);
        };
        let (row, _) = self.rows.insert_full(row);
        fragments
            .map(|fragment| {
                Ok(
                    ProjectionKey::read(edit, edit.expression(fragment)?, parameter)?
                        .map(|key| (row, key)),
                )
            })
            .collect()
    }
}

/// Each sibling contributes its parameter followed by its minimal expensive
/// fragments. A key that needs the general printer requires comparing the whole
/// group there, since it could match a native key in another member.
pub fn sibling_map_groups(
    edit: &CircuitEditor,
    mut roots: &[NodeId],
    fragment_counts: &[usize],
) -> Result<Option<Vec<Vec<usize>>>, Error> {
    let mut keys = FragmentKeys::default();
    let mut maps = Vec::with_capacity(fragment_counts.len());
    for &count in fragment_counts {
        let (parameter, rest) = roots
            .split_first()
            .ok_or_else(|| Error::new("missing sibling map parameter"))?;
        let (fragments, remaining) = rest
            .split_at_checked(count)
            .ok_or_else(|| Error::new("missing sibling map fragments"))?;
        roots = remaining;
        let Some(fragments) = keys.read(edit, *parameter, fragments.iter().copied())? else {
            return Ok(None);
        };
        maps.push(fragments);
    }
    if !roots.is_empty() {
        return Err(Error::new("unexpected trailing sibling map fragments"));
    }
    Ok(Some(map_fusion_groups(&maps)))
}

pub struct FusionCandidate {
    pub map: usize,
    pub function: NodeId,
    pub fragments: Vec<u64>,
}

pub struct FusionScan {
    pub candidates: Vec<FusionCandidate>,
    /// An unresolved fingerprint requires the live printer to group the siblings.
    pub clusters: Option<FusionClusters>,
}

#[derive(Default)]
pub struct FusionClusters {
    /// Indices in the candidate list, up to the first failing comparison.
    pub members: Vec<Vec<usize>>,
    pub comparison_failure: Option<ComparisonDiagnostic>,
}

/// Simplify maps in traversal order so diagnostics retain their positions.
/// Only siblings on the same output port can share a computation.
pub fn scan_map_fusion(
    edit: &mut CircuitEditor,
    functions: &[NodeId],
    input_ports: &[usize],
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<FusionScan, Error> {
    if functions.len() != input_ports.len() {
        return Err(Error::new("expected one input port for each map"));
    }
    let mut candidates = Vec::new();
    let mut siblings = IndexMap::<usize, Vec<usize>>::new();
    for (map, (&function, &port)) in functions.iter().zip(input_ports).enumerate() {
        if let Some((function, fragments)) = map_fusion_candidate(edit, function, warnings)? {
            siblings.entry(port).or_default().push(candidates.len());
            candidates.push(FusionCandidate {
                map,
                function,
                fragments,
            });
        }
    }
    let clusters = select_clusters(edit, &candidates, siblings.into_values())?;
    Ok(FusionScan {
        candidates,
        clusters,
    })
}

fn select_clusters(
    edit: &CircuitEditor,
    candidates: &[FusionCandidate],
    siblings: impl Iterator<Item = Vec<usize>>,
) -> Result<Option<FusionClusters>, Error> {
    let mut clusters = FusionClusters::default();
    for members in siblings {
        let components = if members.len() == 1 {
            vec![members]
        } else {
            let mut keys = FragmentKeys::default();
            let mut maps = Vec::with_capacity(members.len());
            for &member in &members {
                let map = &candidates[member];
                let [parameter] = edit.node(map.function)?.edges("parameters")?[..] else {
                    return Err(Error::new("expected one sibling map parameter"));
                };
                let fragments = map.fragments.iter().copied().map(NodeId::Inner);
                let Some(fields) = keys.read(edit, parameter, fragments)? else {
                    return Ok(None);
                };
                maps.push(fields);
            }
            map_fusion_groups(&maps)
                .into_iter()
                .map(|group| group.into_iter().map(|i| members[i]).collect())
                .collect()
        };
        for component in components {
            if let [member] = component[..] {
                match has_duplicate_expensive_field(edit, candidates[member].function) {
                    Ok(Some(false)) => continue,
                    Ok(Some(true)) => {}
                    Ok(None) => return Ok(None),
                    Err(KeyComparisonError::Diagnostic(failure)) => {
                        clusters.comparison_failure = Some(failure);
                        return Ok(Some(clusters));
                    }
                    Err(KeyComparisonError::InvalidIr(error)) => return Err(error),
                }
            }
            clusters.members.push(component);
        }
    }
    Ok(Some(clusters))
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
