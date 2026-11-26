use super::WaterlineTypeError;
use crate::limiters::Projection;
use feldera_sql_ir::{CircuitEditor, Error, NodeId, TypeId, TypeKind};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Local variables can refer to the same row many times. Share its children
/// and cache their combined bound so inspecting a nested alias stays constant time.
#[derive(Clone)]
pub(super) enum Shape {
    Scalar(bool),
    /// A function field can occupy a projected slot without supporting scalar transfers.
    Closure(bool),
    Opaque(bool),
    Reference(Rc<Self>),
    Tuple {
        fields: Rc<[Self]>,
        any_bound: bool,
        raw: bool,
        nullable: bool,
    },
}

impl Shape {
    pub fn tuple(fields: Vec<Self>, raw: bool, nullable: bool) -> Self {
        let any_bound = fields.iter().any(Self::monotone);
        Self::Tuple {
            fields: fields.into(),
            any_bound,
            raw,
            nullable,
        }
    }

    pub fn from_input(shape: &Projection) -> Self {
        match shape {
            Projection::Other {
                monotone,
                closure: true,
            } => Self::Closure(*monotone),
            Projection::Other { monotone, .. } => Self::Opaque(*monotone),
            Projection::Scalar { monotone } => Self::Scalar(*monotone),
            Projection::Reference { base } => Self::Reference(Self::from_input(base).into()),
            Projection::Tuple {
                fields,
                raw,
                nullable,
            } => Self::tuple(
                fields.iter().map(Self::from_input).collect(),
                *raw,
                *nullable,
            ),
        }
    }

    pub fn monotone(&self) -> bool {
        let mut value = self;
        loop {
            match value {
                Self::Reference(inner) => value = inner,
                Self::Scalar(bound) | Self::Closure(bound) | Self::Opaque(bound) => return *bound,
                Self::Tuple {
                    fields,
                    any_bound,
                    nullable,
                    ..
                } => return *any_bound || fields.is_empty() && !nullable,
            }
        }
    }
}

/// The declared shape of a row with no LATENESS. A nonnullable empty tuple
/// provides a bound, including when nested inside an otherwise unordered row.
pub(super) struct UnorderedRow {
    pub shape: Shape,
    pub projected: Option<TypeId>,
}

pub(super) enum UnorderedType {
    Row(UnorderedRow),
    Invalid(WaterlineTypeError, TypeId),
}

impl UnorderedRow {
    /// Declarations may reuse a field type in several rows. Compute its shape
    /// once and share the children, including those of an unbounded nested row.
    pub fn from_type(edit: &mut CircuitEditor, root: TypeId) -> Result<UnorderedType, Error> {
        fn scalar(kind: TypeKind) -> Option<UnorderedRow> {
            // NonMonotoneType has no representation for a struct or a stream.
            (!matches!(kind, TypeKind::Struct | TypeKind::Stream)).then_some(UnorderedRow {
                shape: Shape::Scalar(false),
                projected: None,
            })
        }
        let structured =
            |kind| matches!(kind, TypeKind::Tuple | TypeKind::RawTuple | TypeKind::Ref);
        let root_kind = edit.type_kind(root)?;
        if !structured(root_kind) {
            return Ok(match scalar(root_kind) {
                Some(row) => UnorderedType::Row(row),
                None => UnorderedType::Invalid(WaterlineTypeError::UnsupportedType, root),
            });
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
                    return Ok(UnorderedType::Invalid(
                        WaterlineTypeError::UnsupportedType,
                        ty,
                    ));
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
            let row = if kind == TypeKind::Ref {
                let base = &completed[&node.edge("type")?];
                Self {
                    shape: Shape::Reference(base.shape.clone().into()),
                    projected: base
                        .projected
                        .map(|ty| edit.reference_type(ty))
                        .transpose()?,
                }
            } else {
                let members = node.edges("tupFields")?;
                let fields = members
                    .iter()
                    .map(|id| completed[id].shape.clone())
                    .collect();
                // Java constructs the field shapes before it checks the raw tuple itself.
                let raw = kind == TypeKind::RawTuple;
                let nullable = edit.nullable(ty)?;
                if raw && nullable {
                    return Ok(UnorderedType::Invalid(
                        WaterlineTypeError::NullableRawTuple,
                        ty,
                    ));
                }
                let shape = Shape::tuple(fields, raw, nullable);
                let projected = if shape.monotone() {
                    let kept = members
                        .iter()
                        .filter_map(|id| completed[id].projected.map(NodeId::from));
                    Some(edit.tuple_type(id, kind == TypeKind::RawTuple, kept)?)
                } else {
                    None
                };
                Self { shape, projected }
            };
            active.remove(&id);
            completed.insert(id, row);
        }
        Ok(UnorderedType::Row(
            completed.remove(&root).expect("visited root type"),
        ))
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
        let UnorderedType::Row(UnorderedRow {
            shape: Shape::Reference(shape),
            projected: None,
        }) = UnorderedRow::from_type(&mut edit, reference)?
        else {
            panic!("expected a reference to an unbounded row");
        };
        assert!(!shape.monotone());
        let mut row = shape.as_ref();
        for _ in 1..160 {
            let Shape::Tuple { fields, .. } = row else {
                panic!("expected row");
            };
            let [Shape::Tuple { fields: left, .. }, Shape::Tuple { fields: right, .. }] =
                fields.as_ref()
            else {
                panic!("expected two nested rows");
            };
            assert!(Rc::ptr_eq(left, right));
            row = &fields[0];
        }
        Ok(())
    }
}
