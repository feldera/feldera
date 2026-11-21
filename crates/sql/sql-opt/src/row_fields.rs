//! Interpret parameter field paths at the depth of a row projection.
use feldera_sql_ir::{Circuit, Error, InnerKind, NodeId, TypeKind as Ty};
use std::collections::{BTreeSet, HashMap};

pub(super) struct RowFields<'a> {
    circuit: &'a Circuit,
    empty_tuples: HashMap<NodeId, bool>,
}

impl<'a> RowFields<'a> {
    pub(super) fn new(circuit: &'a Circuit) -> Self {
        Self {
            circuit,
            empty_tuples: HashMap::new(),
        }
    }

    // A zero-column tuple counts as used, even when its enclosing row is discarded.
    fn contains_empty_tuple(&mut self, ty: NodeId, level: usize) -> Result<bool, Error> {
        if level > 128 {
            return Err(Error::new("input field type exceeds the nesting limit"));
        }
        if let Some(&empty) = self.empty_tuples.get(&ty) {
            return Ok(empty);
        }
        let node = self.circuit.node(ty).expect("validated row type");
        let result = match node.inner_kind() {
            Some(InnerKind::Type(Ty::Ref)) => {
                self.contains_empty_tuple(node.edge("type")?, level + 1)?
            }
            Some(InnerKind::Type(Ty::Tuple | Ty::RawTuple)) => {
                let fields = node.edges("tupFields")?;
                let mut empty = fields.is_empty();
                for f in fields {
                    if self.contains_empty_tuple(f, level + 1)? {
                        empty = true;
                        break;
                    }
                }
                empty
            }
            _ => false,
        };
        self.empty_tuples.insert(ty, result);
        Ok(result)
    }

    pub(super) fn unused(
        &mut self,
        ty: NodeId,
        prefix: &mut Vec<usize>,
        paths: &BTreeSet<Vec<usize>>,
        depth: usize,
        level: usize,
    ) -> Result<bool, Error> {
        if level > 128 {
            return Err(Error::new("input field type exceeds the nesting limit"));
        }
        if paths.contains(prefix) {
            return Ok(false);
        }
        let node = self.circuit.node(ty).expect("validated field type");
        match node.inner_kind() {
            Some(InnerKind::Type(Ty::Ref)) => {
                self.unused(node.edge("type")?, prefix, paths, depth, level + 1)
            }
            Some(InnerKind::Type(Ty::Tuple | Ty::RawTuple)) if depth > 0 => {
                for (i, field) in node.edges("tupFields")?.into_iter().enumerate() {
                    prefix.push(i);
                    let unused = self.unused(field, prefix, paths, depth - 1, level + 1)?;
                    prefix.pop();
                    if unused {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Ok(!paths.iter().any(|p| p.starts_with(prefix))
                && !self.contains_empty_tuple(ty, level)?),
        }
    }
}
