//! Project a field read or value wrapper against its operand waterline.
use super::{ProjectedValue, RowType, Shape, unbounded_type};
use crate::limiters::Projection;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind, Field, NodeId, TypeId};

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowRead {
    Clone,
    Some,
    Unwrap,
    Borrow,
    Deref,
    Field { index: usize },
}

impl RowRead {
    /// The general visitor supplies the live operand shape and its optional bound.
    /// Its type recipe refers back to that shape, preserving field identity.
    pub fn project(
        self,
        edit: &mut CircuitEditor,
        site: NodeId,
        ty: TypeId,
        source: &Projection,
        bound: Option<ExpressionId>,
    ) -> Result<Option<ProjectedValue>, Error> {
        let Some(shape) = Shape::from_input(source) else {
            return Ok(None);
        };
        if shape.monotone() && bound.is_none() {
            // Let the general visitor report the missing bound at the original read.
            return Ok(None);
        }
        self.apply(
            edit,
            site,
            ty,
            ProjectedValue {
                shape,
                row_type: RowType::Parameter { index: 0 },
                reduced: bound,
                constant: false,
            },
        )
    }

    pub(super) fn apply(
        self,
        edit: &mut CircuitEditor,
        site: NodeId,
        ty: TypeId,
        mut value: ProjectedValue,
    ) -> Result<Option<ProjectedValue>, Error> {
        let projected = match self {
            Self::Clone => {
                if let Some(bound) = value.reduced {
                    value.reduced = Some(edit.make_expr(
                        site,
                        ExpressionKind::Clone,
                        edit.expression_type(bound)?,
                        [("expression", Field::Reference(bound.into()))],
                    )?);
                }
                value
            }
            Self::Some | Self::Unwrap => {
                let nullable = matches!(self, Self::Some);
                value.row_type = RowType::WithNullability {
                    value: Box::new(value.row_type),
                    nullable,
                };
                if let Shape::Tuple {
                    nullable: tuple_nullable,
                    ..
                } = &mut value.shape
                {
                    *tuple_nullable = nullable;
                }
                // The general visitor exposes the operand bound even when Some(())
                // makes the result unbounded. Consumers decide whether to use it.
                value
            }
            Self::Borrow => {
                let Some(bound) = value.reduced else {
                    // An unbounded borrow takes its shape from its declared reference type.
                    let mut result = unbounded_type(edit, ty)?;
                    if let Some(result) = &mut result {
                        result.constant = value.constant;
                    }
                    return Ok(result);
                };
                ProjectedValue {
                    shape: Shape::Reference(value.shape.into()),
                    row_type: RowType::Borrow {
                        value: Box::new(value.row_type),
                    },
                    reduced: Some(edit.borrow_value(bound)?),
                    constant: value.constant,
                }
            }
            Self::Deref => {
                let Shape::Reference(base) = value.shape else {
                    return Ok(None);
                };
                ProjectedValue {
                    shape: (*base).clone(),
                    row_type: RowType::Deref {
                        value: Box::new(value.row_type),
                    },
                    reduced: value.reduced.map(|v| edit.dereference(v)).transpose()?,
                    constant: value.constant,
                }
            }
            Self::Field { index } => {
                let Shape::Tuple {
                    fields, nullable, ..
                } = value.shape
                else {
                    return Ok(None);
                };
                let Some(field) = fields.get(index) else {
                    return Ok(None);
                };
                let mut shape = field.clone();
                if let Shape::Tuple {
                    nullable: inner, ..
                } = &mut shape
                {
                    *inner |= nullable;
                }
                let reduced = if shape.monotone() {
                    let Some(row) = value.reduced else {
                        return Ok(None);
                    };
                    let compressed = fields[..index]
                        .iter()
                        .filter(|field| field.monotone())
                        .count();
                    Some(edit.tuple_field(row, compressed)?)
                } else {
                    None
                };
                ProjectedValue {
                    shape,
                    constant: false,
                    row_type: RowType::Field {
                        row: Box::new(value.row_type),
                        index,
                    },
                    reduced,
                }
            }
        };
        Ok(Some(projected))
    }
}
