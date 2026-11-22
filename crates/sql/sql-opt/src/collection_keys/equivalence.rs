//! Row columns that always carry the same value. Singleton classes are implicit.
use super::{Column, EquivalentColumns, KeyColumns};
use std::collections::BTreeSet;

#[derive(Clone, Default)]
pub(super) struct ColumnEquivalence(Vec<BTreeSet<Column>>);

impl ColumnEquivalence {
    pub(super) fn include(&mut self, columns: impl IntoIterator<Item = Column>) {
        let mut group: BTreeSet<_> = columns.into_iter().collect();
        if group.len() < 2 {
            return;
        }
        // Existing classes are disjoint. Absorb each class touched by the new equality.
        self.0.retain(|known| {
            if known.is_disjoint(&group) {
                true
            } else {
                group.extend(known);
                false
            }
        });
        self.0.push(group);
    }

    pub(super) fn extend(&mut self, other: Self) {
        for group in other.0 {
            self.include(group);
        }
    }

    pub(super) fn after(&self, mut project: impl FnMut(Column, &mut BTreeSet<Column>)) -> Self {
        let mut projected = Self::default();
        for group in &self.0 {
            let mut columns = BTreeSet::new();
            for &column in group {
                project(column, &mut columns);
            }
            projected.include(columns);
        }
        projected
    }

    pub(super) fn intersect(&self, other: &Self) -> Self {
        let mut common = Vec::new();
        for left in &self.0 {
            for right in &other.0 {
                let columns: BTreeSet<_> = left.intersection(right).copied().collect();
                if columns.len() > 1 {
                    common.push(columns);
                }
            }
        }
        Self(common)
    }

    pub(super) fn key(&self, columns: impl IntoIterator<Item = Column>) -> KeyColumns {
        let sets = columns
            .into_iter()
            .map(|column| EquivalentColumns {
                columns: self
                    .0
                    .iter()
                    .find(|group| group.contains(&column))
                    .cloned()
                    .unwrap_or_else(|| BTreeSet::from([column])),
            })
            .collect();
        KeyColumns { sets }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lineage::Part;

    #[test]
    fn a_union_preserves_only_aliases_shared_by_both_inputs() {
        let [a, b, c, d, e] = [0, 1, 2, 3, 4].map(|field| Column {
            part: Part::None,
            field,
        });
        let mut left = ColumnEquivalence::default();
        left.include([a, b]);
        left.include([c, d]);
        left.include([b, c]);
        let mut right = ColumnEquivalence::default();
        right.include([b, c, e]);
        let common = left.intersect(&right);
        assert_eq!(common.key([b]), common.key([c]));
        assert_ne!(common.key([b]), common.key([a]));
        assert_ne!(common.key([b]), common.key([e]));
        assert_eq!(common.key([b, c]).sets.len(), 1);
    }
}
