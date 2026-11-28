//! Propagate integer constants across filters and grouping keys. A projection
//! keeps its declared type even when substitution proves its value non-null.

use super::{ProjectInput, ProjectValue, Projection, TableOperand, TypeId, filter};
use crate::program::graph::insert_type;
use feldera_sql_rel_model::{
    TypeTable,
    expression_objects::ObjectPosition,
    types::{SqlType, TypeKind},
};

#[derive(Clone, Copy)]
pub(super) struct Integer {
    ty: TypeId,
    value: i64,
}

fn fixed_column(
    types: &mut TypeTable,
    predicate: Option<&filter::Predicate>,
) -> Option<(u32, Integer)> {
    let Some(filter::Predicate::Expression {
        value:
            ProjectValue::Call {
                key: "SqlStdOperatorTable.EQUALS",
                arguments,
                ..
            },
        ..
    }) = predicate
    else {
        return None;
    };
    let [left, TableOperand::Integer { ty, value }] = arguments.as_slice() else {
        return None;
    };
    let (column, literal_ty) = match left {
        TableOperand::Column(column) => (column, *ty),
        TableOperand::Computed(call) => {
            let ProjectValue::Call {
                key: "SqlStdOperatorTable.CAST",
                arguments,
                ..
            } = call.as_ref()
            else {
                return None;
            };
            let [TableOperand::Column(column)] = arguments.as_slice() else {
                return None;
            };
            let column_type = &types.types[column.ty.0 as usize];
            let TypeKind::Integer {
                width,
                signed: true,
            } = column_type.kind
            else {
                return None;
            };
            // Equality fixes the source column only if the widened value can
            // also be represented in the original column type.
            if !width.contains_signed(*value) {
                return None;
            }
            let ty = insert_type(
                types,
                SqlType {
                    nullable: false,
                    ..column_type.clone()
                },
            );
            (column, ty)
        }
        _ => return None,
    };
    Some((
        column.field,
        Integer {
            ty: literal_ty,
            value: *value,
        },
    ))
}

pub(super) fn apply_filtered_projection(
    types: &mut TypeTable,
    projection: &mut [Projection],
    predicate: Option<&filter::Predicate>,
) {
    if let Some((field, literal)) = fixed_column(types, predicate) {
        let values = projection.iter_mut().map(|p| &mut p.value);
        substitute(types, values, |i| (i == field).then_some(literal));
    }
}

impl Integer {
    pub(super) fn from_project(value: &ProjectValue) -> Option<Self> {
        match value {
            ProjectValue::Integer { ty, value } => Some(Self {
                ty: *ty,
                value: *value,
            }),
            ProjectValue::Call {
                key: "SqlStdOperatorTable.CAST",
                arguments,
                ..
            } => {
                let [TableOperand::Integer { ty, value }] = arguments.as_slice() else {
                    return None;
                };
                Some(Self {
                    ty: *ty,
                    value: *value,
                })
            }
            _ => None,
        }
    }

    fn operand(self) -> TableOperand {
        TableOperand::Integer {
            ty: self.ty,
            value: self.value,
        }
    }
}

fn as_output(operand: &TableOperand, output: TypeId) -> ProjectValue {
    let value = match operand {
        TableOperand::Integer { ty, value } => ProjectValue::Integer {
            ty: *ty,
            value: *value,
        },
        TableOperand::Column(column) => ProjectValue::Column(*column),
        TableOperand::Computed(value) => *value.clone(),
    };
    if value.ty() == output {
        return value;
    }
    ProjectValue::Call {
        registry: crate::frel::CALCITE,
        key: "SqlStdOperatorTable.CAST",
        arguments: vec![operand.clone()],
        ty: output,
        position: ObjectPosition::Absent {},
    }
}

pub(super) fn substitute<'a>(
    types: &TypeTable,
    values: impl IntoIterator<Item = &'a mut ProjectValue>,
    known: impl Fn(u32) -> Option<Integer>,
) {
    let mut folded = Vec::new();
    let mut overflow = false;
    for value in values {
        match value {
            ProjectValue::Column(ProjectInput { field, ty }) => {
                if let Some(integer) = known(*field) {
                    *value = as_output(&integer.operand(), *ty);
                }
            }
            ProjectValue::Call {
                key,
                arguments,
                ty,
                position,
                ..
            } => {
                for arg in &mut *arguments {
                    if let TableOperand::Column(col) = arg
                        && let Some(integer) = known(col.field)
                    {
                        *arg = integer.operand();
                    }
                }
                let [left, right] = arguments.as_slice() else {
                    continue;
                };
                let literal = |arg: &TableOperand| match arg {
                    TableOperand::Integer { value, .. } => Some(*value),
                    _ => None,
                };
                let (lhs, rhs) = (literal(left), literal(right));
                let identity = match (*key, lhs, rhs) {
                    ("SqlStdOperatorTable.CHECKED_PLUS", Some(0), _)
                    | ("SqlStdOperatorTable.CHECKED_MULTIPLY", Some(1), _) => Some(right),
                    (
                        "SqlStdOperatorTable.CHECKED_PLUS" | "SqlStdOperatorTable.CHECKED_MINUS",
                        _,
                        Some(0),
                    )
                    | (
                        "SqlStdOperatorTable.CHECKED_MULTIPLY"
                        | "SqlStdOperatorTable.CHECKED_DIVIDE",
                        _,
                        Some(1),
                    ) => Some(left),
                    _ => None,
                };
                if let Some(operand) = identity {
                    let mut simplified = as_output(operand, *ty);
                    if let ProjectValue::Call {
                        position: cast_pos, ..
                    } = &mut simplified
                        && matches!(operand, TableOperand::Column(_))
                    {
                        *cast_pos = position.clone();
                    }
                    *value = simplified;
                    continue;
                }
                let (Some(l), Some(r), TableOperand::Integer { ty: literal_ty, .. }) =
                    (lhs, rhs, left)
                else {
                    continue;
                };
                let TypeKind::Integer {
                    width,
                    signed: true,
                } = types.types[literal_ty.0 as usize].kind
                else {
                    continue;
                };
                let result = match *key {
                    "SqlStdOperatorTable.CHECKED_PLUS" => l.checked_add(r),
                    "SqlStdOperatorTable.CHECKED_MINUS" => l.checked_sub(r),
                    "SqlStdOperatorTable.CHECKED_MULTIPLY" => l.checked_mul(r),
                    "SqlStdOperatorTable.CHECKED_DIVIDE" => l.checked_div(r),
                    _ => continue,
                };
                if let Some(number) = result.filter(|n| width.contains_signed(*n)) {
                    let replacement = as_output(
                        &TableOperand::Integer {
                            ty: *literal_ty,
                            value: number,
                        },
                        *ty,
                    );
                    folded.push((value, replacement));
                } else {
                    overflow = true;
                }
            }
            ProjectValue::Integer { .. } => {}
        }
    }
    // Calcite evaluates a projection's reducible constants together. A
    // checked arithmetic error leaves that batch for runtime evaluation.
    if !overflow {
        for (field, replacement) in folded {
            *field = replacement;
        }
    }
}
