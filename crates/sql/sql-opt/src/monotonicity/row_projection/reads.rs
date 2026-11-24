//! Project a field read or value wrapper against its operand waterline.
use super::{ProjectedValue, RowType, Shape, UnorderedRow, UnorderedType};
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
    InvalidProjection(RowType),
    UnsupportedType(TypeId),
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
        self.apply(
            edit,
            site,
            ty,
            ProjectedValue {
                shape: Shape::from_input(source),
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
    ) -> Result<ProjectedRead, Error> {
        // A borrow checks its declared type first. A field checks and lifts the
        // selected field type before deciding whether it needs a bound at all.
        if !matches!(self, Self::Borrow | Self::Field { .. }) && value.missing_bound() {
            return Ok(ProjectedRead::MissingBound);
        }
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
                let projected = RowType::WithNullability {
                    value: Box::new(value.row_type),
                    nullable,
                };
                value.row_type = match set_nullability(&mut value.shape, nullable, projected) {
                    Ok(ty) => ty,
                    Err(failure) => return Ok(failure),
                };
                // The general visitor exposes the operand bound even when Some(())
                // makes the result unbounded. Consumers decide whether to use it.
                value
            }
            Self::Borrow => {
                let declared = match UnorderedRow::from_type(edit, ty)? {
                    UnorderedType::Row(row) => row,
                    UnorderedType::Unsupported(ty) => {
                        return Ok(ProjectedRead::UnsupportedType(ty));
                    }
                };
                let Some(bound) = value.reduced else {
                    return Ok(if value.missing_bound() {
                        ProjectedRead::MissingBound
                    } else {
                        ProjectedRead::Value(declared.unbounded_value(ty, value.constant))
                    });
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
                let projected = RowType::Deref {
                    value: Box::new(value.row_type),
                };
                let Shape::Reference(base) = value.shape else {
                    return Ok(ProjectedRead::InvalidProjection(projected));
                };
                ProjectedValue {
                    shape: (*base).clone(),
                    row_type: projected,
                    reduced: value.reduced.map(|v| edit.dereference(v)).transpose()?,
                    constant: value.constant,
                }
            }
            Self::Field { index } => {
                let projected = RowType::Field {
                    row: Box::new(value.row_type),
                    index,
                };
                let Shape::Tuple {
                    fields, nullable, ..
                } = value.shape
                else {
                    return Ok(ProjectedRead::InvalidProjection(projected));
                };
                let Some(field) = fields.get(index) else {
                    return Ok(ProjectedRead::InvalidProjection(projected));
                };
                let mut shape = field.clone();
                let projected = if nullable {
                    match set_nullability(&mut shape, true, projected) {
                        Ok(ty) => ty,
                        Err(failure) => return Ok(failure),
                    }
                } else {
                    projected
                };
                let reduced = if shape.monotone() {
                    let Some(row) = value.reduced else {
                        return Ok(ProjectedRead::MissingBound);
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
                    row_type: projected,
                    reduced,
                }
            }
        };
        Ok(ProjectedRead::Value(projected))
    }
}

/// The live recipe owns the field or wrapper location used by Java's diagnostic.
fn set_nullability(
    shape: &mut Shape,
    nullable: bool,
    ty: RowType,
) -> Result<RowType, ProjectedRead> {
    match shape {
        Shape::Closure(_) => Err(ProjectedRead::InvalidProjection(ty)),
        Shape::Reference(_) | Shape::Tuple { raw: true, .. } if nullable => {
            Err(ProjectedRead::InvalidProjection(ty))
        }
        Shape::Opaque(_) => Err(ProjectedRead::NeedsVisitor),
        Shape::Tuple {
            nullable: current, ..
        } => {
            *current = nullable;
            Ok(ty)
        }
        _ => Ok(ty),
    }
}
