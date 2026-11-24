//! Project a field read or value wrapper against its operand waterline.
use super::evaluate::Evaluation;
use super::{ProjectedValue, RowType, Shape, WaterlineError, unbounded_type};
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

pub enum ProjectedRead {
    Value(ProjectedValue),
    MissingBound,
    NeedsVisitor,
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
    ) -> Result<ProjectedRead, Error> {
        let shape = Shape::from_input(source);
        if shape.monotone() && bound.is_none() {
            // A partially monotone tuple can still supply an unbounded field
            // without reading the tuple's reduced expression.
            let reads_bound = match (&self, &shape) {
                (Self::Field { index }, Shape::Tuple { fields, .. }) => {
                    let Some(field) = fields.get(*index) else {
                        return Ok(ProjectedRead::NeedsVisitor);
                    };
                    field.monotone()
                }
                (Self::Field { .. }, _) => return Ok(ProjectedRead::NeedsVisitor),
                _ => true,
            };
            if reads_bound {
                return Ok(ProjectedRead::MissingBound);
            }
        }
        let result = self.apply(
            edit,
            site,
            ty,
            ProjectedValue {
                shape,
                row_type: RowType::Parameter { index: 0 },
                reduced: bound,
                constant: false,
            },
        )?;
        Ok(match result {
            Evaluation::Value(value) => ProjectedRead::Value(value),
            Evaluation::NeedsVisitor | Evaluation::Failed(_) => ProjectedRead::NeedsVisitor,
        })
    }

    pub(super) fn apply(
        self,
        edit: &mut CircuitEditor,
        site: NodeId,
        ty: TypeId,
        mut value: ProjectedValue,
    ) -> Result<Evaluation, Error> {
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
                if nullable && matches!(value.shape, Shape::Reference(_)) {
                    return Ok(Evaluation::Failed(WaterlineError::NullableReference {
                        source: value.row_type,
                        field: None,
                    }));
                }
                // A closure has no withMaybeNull operation. Preserve the diagnostic
                // at the live expression even when its result is later discarded.
                if matches!(value.shape, Shape::Opaque(_)) {
                    return Ok(Evaluation::NeedsVisitor);
                }
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
                    return unbounded_type(edit, ty, value.constant);
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
                    return Ok(Evaluation::NeedsVisitor);
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
                    return Ok(Evaluation::NeedsVisitor);
                };
                let Some(field) = fields.get(index) else {
                    return Ok(Evaluation::NeedsVisitor);
                };
                if nullable && matches!(field, Shape::Reference(_)) {
                    return Ok(Evaluation::Failed(WaterlineError::NullableReference {
                        source: value.row_type,
                        field: Some(index),
                    }));
                }
                if nullable && matches!(field, Shape::Opaque(_)) {
                    return Ok(Evaluation::NeedsVisitor);
                }
                let mut shape = field.clone();
                if let Shape::Tuple {
                    nullable: inner, ..
                } = &mut shape
                {
                    *inner |= nullable;
                }
                let reduced = if shape.monotone() {
                    let Some(row) = value.reduced else {
                        // The requested field has a bound, so Java reads the
                        // tuple's reduced expression before compressing its index.
                        let operand = edit.expression(edit.node(site)?.edge("expression")?)?;
                        return Ok(Evaluation::Failed(WaterlineError::MissingBound(operand)));
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
        Ok(Evaluation::Value(projected))
    }
}
