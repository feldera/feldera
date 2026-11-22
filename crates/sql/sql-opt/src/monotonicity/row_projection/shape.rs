use crate::limiters::Projection;
use feldera_sql_ir::{CircuitEditor, Error, NodeId, TypeId, TypeKind};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Local variables can refer to the same row many times. Share its children
/// and cache their combined bound so inspecting a nested alias stays constant time.
#[derive(Clone)]
pub(super) enum Shape {
    Scalar(bool),
    Reference(Rc<Self>),
    Tuple {
        fields: Rc<[Self]>,
        any_bound: bool,
        nullable: bool,
    },
}

impl Shape {
    pub fn tuple(fields: Vec<Self>, nullable: bool) -> Self {
        let any_bound = fields.iter().any(Self::monotone);
        Self::Tuple {
            fields: fields.into(),
            any_bound,
            nullable,
        }
    }

    /// Declarations may reuse a field type in several rows. Compute its shape
    /// once and share the children, including those of an unbounded nested row.
    pub fn unbounded(edit: &CircuitEditor, root: TypeId) -> Result<Option<Self>, Error> {
        fn scalar(kind: TypeKind) -> Option<Shape> {
            // NonMonotoneType has no representation for a struct or a stream.
            (!matches!(kind, TypeKind::Struct | TypeKind::Stream)).then_some(Shape::Scalar(false))
        }
        let structured =
            |kind| matches!(kind, TypeKind::Tuple | TypeKind::RawTuple | TypeKind::Ref);
        let root_kind = edit.type_kind(root)?;
        if !structured(root_kind) {
            return Ok(scalar(root_kind));
        }
        let root = NodeId::from(root);
        let mut completed = HashMap::<NodeId, Self>::new();
        let mut active = HashSet::new();
        let mut pending = vec![(root, false)];
        while let Some((id, finish)) = pending.pop() {
            if completed.contains_key(&id) {
                continue;
            }
            let ty = edit.type_id(id)?;
            let kind = edit.type_kind(ty)?;
            let node = edit.node(id)?;
            if !structured(kind) {
                let Some(shape) = scalar(kind) else {
                    return Ok(None);
                };
                completed.insert(id, shape);
                continue;
            }
            if !finish {
                if !active.insert(id) {
                    return Err(Error::new("cycle in a waterline type"));
                }
                pending.push((id, true));
                if kind == TypeKind::Ref {
                    pending.push((node.edge("type")?, false));
                } else {
                    pending.extend(
                        node.edges("tupFields")?
                            .into_iter()
                            .rev()
                            .map(|id| (id, false)),
                    );
                }
                continue;
            }
            let shape = if kind == TypeKind::Ref {
                Self::Reference(Rc::new(completed[&node.edge("type")?].clone()))
            } else {
                let fields = node
                    .edges("tupFields")?
                    .iter()
                    .map(|id| completed[id].clone())
                    .collect();
                Self::tuple(fields, edit.nullable(ty)?)
            };
            active.remove(&id);
            completed.insert(id, shape);
        }
        Ok(completed.remove(&root))
    }

    pub fn from_input(shape: &Projection) -> Option<Self> {
        Some(match shape {
            Projection::Other { .. } => return None,
            Projection::Scalar { monotone } => Self::Scalar(*monotone),
            Projection::Reference { base } => Self::Reference(Self::from_input(base)?.into()),
            Projection::Tuple {
                fields, nullable, ..
            } => Self::tuple(
                fields.iter().map(Self::from_input).collect::<Option<_>>()?,
                *nullable,
            ),
        })
    }

    pub fn monotone(&self) -> bool {
        let mut value = self;
        loop {
            match value {
                Self::Reference(inner) => value = inner,
                Self::Scalar(bound) => return *bound,
                Self::Tuple {
                    fields,
                    any_bound,
                    nullable,
                } => return *any_bound || fields.is_empty() && !nullable,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;

    #[test]
    fn a_deep_row_shares_the_shape_of_repeated_field_types() -> Result<(), Error> {
        let graph = Circuit::from_json(serde_json::json!({
            "class":"DBSPCircuit", "id":0, "allOperators":[], "declarations":[]
        }))?;
        let root = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let mut ty = edit.bool_type(root, false)?;
        for _ in 0..160 {
            ty = edit.tuple_type(root, false, [ty.into(), ty.into()])?;
        }
        let reference = edit.reference_type(ty)?;
        let Some(Shape::Reference(shape)) = Shape::unbounded(&edit, reference)? else {
            panic!("expected a reference to an unbounded row");
        };
        assert!(!shape.monotone());
        let mut row = shape.as_ref();
        for _ in 1..160 {
            let Shape::Tuple { fields, .. } = row else {
                panic!("expected row");
            };
            let [
                Shape::Tuple { fields: left, .. },
                Shape::Tuple { fields: right, .. },
            ] = fields.as_ref()
            else {
                panic!("expected two nested rows");
            };
            assert!(Rc::ptr_eq(left, right));
            row = &fields[0];
        }
        Ok(())
    }
}
