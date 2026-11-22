//! Interpret parameter field paths at the depth of a row projection.
use feldera_sql_ir::{Circuit, Error, InnerKind, NodeId, TypeKind as Ty};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Included, Unbounded};

pub(super) struct RowFields<'a> {
    circuit: &'a Circuit,
    unread_depths: HashMap<NodeId, Option<usize>>,
}

impl<'a> RowFields<'a> {
    pub(super) fn new(circuit: &'a Circuit) -> Self {
        Self {
            circuit,
            unread_depths: HashMap::new(),
        }
    }

    /// Minimum projection depth at which a row with no reads has an unused field.
    /// An empty tuple is always kept. A row containing both an empty tuple and
    /// a scalar must be opened at least once before the scalar can be discarded.
    fn unread_depth(&mut self, root: NodeId) -> Result<Option<usize>, Error> {
        if let Some(depth) = self.unread_depths.get(&root) {
            return Ok(*depth);
        }
        let mut pending = vec![(root, false)];
        let mut active = HashSet::new();
        while let Some((id, finish)) = pending.pop() {
            if self.unread_depths.contains_key(&id) {
                continue;
            }
            let node = self.circuit.node(id).expect("validated field type");
            let kind = node.inner_kind();
            let children = match kind {
                Some(InnerKind::Type(Ty::Ref)) => vec![node.edge("type")?],
                Some(InnerKind::Type(Ty::Tuple | Ty::RawTuple)) => node.edges("tupFields")?,
                _ => {
                    self.unread_depths.insert(id, Some(0));
                    continue;
                }
            };
            if !finish {
                if !active.insert(id) {
                    return Err(Error::new("cycle in an input field type"));
                }
                pending.push((id, true));
                pending.extend(children.into_iter().map(|child| (child, false)));
                continue;
            }
            let depth = if matches!(kind, Some(InnerKind::Type(Ty::Ref))) {
                self.unread_depths[&children[0]]
            } else if !children.is_empty()
                && children.iter().all(|c| self.unread_depths[c] == Some(0))
            {
                Some(0)
            } else {
                children
                    .iter()
                    .filter_map(|c| self.unread_depths[c])
                    .min()
                    .map(|n| n + 1)
            };
            active.remove(&id);
            self.unread_depths.insert(id, depth);
        }
        Ok(self.unread_depths[&root])
    }

    pub(super) fn unused(
        &mut self,
        ty: NodeId,
        prefix: &[usize],
        paths: &BTreeSet<Vec<usize>>,
        depth: usize,
    ) -> Result<bool, Error> {
        if paths.contains(prefix) || self.unread_depth(ty)?.is_none_or(|min| min > depth) {
            return Ok(false);
        }
        let mut pending = vec![(ty, prefix.to_vec(), depth)];
        while let Some((id, path, remaining)) = pending.pop() {
            // The first path at or after this prefix is either an exact read,
            // a read of a descendant, or the start of a different column.
            match paths
                .range::<[usize], _>((Included(path.as_slice()), Unbounded))
                .next()
            {
                Some(read) if *read == path => continue,
                Some(read) if read.starts_with(&path) => {}
                _ => {
                    if self
                        .unread_depth(id)?
                        .is_some_and(|depth| depth <= remaining)
                    {
                        return Ok(true);
                    }
                    continue;
                }
            }
            // A read of any descendant keeps the entire column at the cutoff.
            if remaining == 0 {
                continue;
            }
            let node = self.circuit.node(id).expect("validated field type");
            match node.inner_kind() {
                Some(InnerKind::Type(Ty::Ref)) => {
                    pending.push((node.edge("type")?, path, remaining))
                }
                Some(InnerKind::Type(Ty::Tuple | Ty::RawTuple)) => {
                    for (i, child) in node.edges("tupFields")?.into_iter().enumerate().rev() {
                        let mut child_path = path.clone();
                        child_path.push(i);
                        pending.push((child, child_path, remaining - 1));
                    }
                }
                _ => {}
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::CircuitEditor;

    #[test]
    fn deeply_shared_rows_keep_empty_tuples_at_the_projection_cutoff() -> Result<(), Error> {
        let circuit = Circuit::from_json(serde_json::json!({
            "class": "DBSPCircuit", "id": 0, "allOperators": [], "declarations": []
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let empty = edit.tuple_type(site, false, [])?;
        let scalar = edit.bool_type(site, false)?;
        let mut row = edit.tuple_type(site, false, [empty.into(), scalar.into()])?;
        for _ in 0..160 {
            row = edit.tuple_type(site, false, [row.into(), row.into()])?;
        }
        let borrowed = edit.reference_type(row)?;
        let mut fields = RowFields::new(edit.circuit());
        let none = BTreeSet::new();
        assert!(!fields.unused(borrowed.into(), &[], &none, 160)?);
        assert!(fields.unused(borrowed.into(), &[], &none, 161)?);
        let both = BTreeSet::from([vec![0], vec![1]]);
        assert!(!fields.unused(borrowed.into(), &[], &both, usize::MAX)?);
        let left = BTreeSet::from([vec![0]]);
        assert!(fields.unused(borrowed.into(), &[], &left, usize::MAX)?);
        Ok(())
    }
}
