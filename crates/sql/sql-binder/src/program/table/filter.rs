//! Bind a predicate on a single plain table.

use super::{ProjectInput, ProjectValue, TableColumn, TableOperand, column_ordinal, render_column};
use crate::{
    BinaryOperator, Expr, Failure, Spanned, Value,
    frel::CALCITE,
    program::{graph::insert_type, source::DeclarationTokens},
    unsupported,
};
use feldera_sql_rel_model::{
    program::Program,
    source::Origin,
    types::{IntegerWidth, SqlType, TypeKind},
};

pub(super) enum Predicate {
    Constant(bool),
    Expression {
        value: ProjectValue,
        pretty: String,
        diagnostic: String,
    },
}

impl Predicate {
    pub(super) fn render(&self) -> (&str, &str) {
        match self {
            Self::Constant(true) => ("TRUE", "TRUE"),
            Self::Constant(false) => ("FALSE", "FALSE"),
            Self::Expression {
                pretty, diagnostic, ..
            } => (pretty, diagnostic),
        }
    }
}

pub(super) fn bind(
    program: &mut Program,
    expression: &Expr,
    columns: &[TableColumn],
    qualifier: &str,
    tokens: &DeclarationTokens<'_>,
) -> Result<Predicate, Failure> {
    let column = |expression| {
        let field = column_ordinal(expression, columns, qualifier)?;
        let column = &columns[field];
        let (pretty, diagnostic) = render_column(qualifier, &column.name.name);
        Ok::<_, Failure>((
            ProjectInput {
                field: field as u32,
                ty: column.ty,
            },
            pretty,
            diagnostic,
        ))
    };
    let types = &mut program.table.type_table;
    let boolean_column = |expression| {
        let (input, pretty, diagnostic) = column(expression)?;
        if !matches!(types.types[input.ty.0 as usize].kind, TypeKind::Boolean {}) {
            return Err(unsupported("table predicate requires a BOOLEAN column"));
        }
        Ok((input, pretty, diagnostic))
    };
    let (value, pretty, diagnostic) = match expression {
        Expr::Value(value) => match value.value {
            Value::Boolean(value) => return Ok(Predicate::Constant(value)),
            _ => return Err(unsupported("table predicate requires a BOOLEAN literal")),
        },
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let (input, pretty, diagnostic) = boolean_column(expression)?;
            (ProjectValue::Column(input), pretty, diagnostic)
        }
        Expr::IsTrue(operand) => {
            let (input, pretty, diagnostic) = boolean_column(operand)?;
            (
                ProjectValue::Call {
                    registry: CALCITE,
                    key: "SqlStdOperatorTable.IS_TRUE",
                    arguments: vec![TableOperand::Column(input)],
                    ty: insert_type(types, boolean(false)),
                    position: Some(tokens.end_at_is_true(operand.span())?),
                },
                format!("{pretty} IS TRUE"),
                format!("({diagnostic} IS TRUE)"),
            )
        }
        Expr::BinaryOp { left, op, right } => {
            let (key, token) = match op {
                BinaryOperator::Eq => ("SqlStdOperatorTable.EQUALS", "="),
                BinaryOperator::NotEq => ("SqlStdOperatorTable.NOT_EQUALS", "<>"),
                BinaryOperator::Lt => ("SqlStdOperatorTable.LESS_THAN", "<"),
                BinaryOperator::LtEq => ("SqlStdOperatorTable.LESS_THAN_OR_EQUAL", "<="),
                BinaryOperator::Gt => ("SqlStdOperatorTable.GREATER_THAN", ">"),
                BinaryOperator::GtEq => ("SqlStdOperatorTable.GREATER_THAN_OR_EQUAL", ">="),
                _ => return Err(unsupported("predicate outside the table filter slice")),
            };
            let (left, left_pretty, left_diagnostic) = column(left)?;
            let input_type = &types.types[left.ty.0 as usize];
            if !signed_integer(input_type) {
                return Err(unsupported(
                    "table comparison requires a signed INTEGER or BIGINT column",
                ));
            }
            let left_nullable = input_type.nullable;
            let (right, right_pretty, right_diagnostic, right_nullable) = match right.as_ref() {
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                    let (right, pretty, diagnostic) = column(right)?;
                    let ty = &types.types[right.ty.0 as usize];
                    if ty.kind != input_type.kind {
                        return Err(unsupported(
                            "comparison across different integer widths requires coercion",
                        ));
                    }
                    if left.field == right.field {
                        return Err(unsupported(
                            "self-comparison requires separate simplification",
                        ));
                    }
                    (TableOperand::Column(right), pretty, diagnostic, ty.nullable)
                }
                Expr::Value(literal) => {
                    let Value::Number(text, false) = &literal.value else {
                        return Err(unsupported("table comparison requires an INTEGER literal"));
                    };
                    let value = text
                        .parse::<i64>()
                        .map_err(|_| unsupported("table comparison requires an INTEGER literal"))?;
                    if matches!(
                        input_type.kind,
                        TypeKind::Integer {
                            width: IntegerWidth::I32,
                            ..
                        }
                    ) && i32::try_from(value).is_err()
                    {
                        return Err(unsupported("integer literal requires a wider comparison"));
                    }
                    let literal_ty = insert_type(
                        types,
                        SqlType {
                            nullable: false,
                            ..input_type.clone()
                        },
                    );
                    let text = value.to_string();
                    (
                        TableOperand::Integer {
                            ty: literal_ty,
                            value,
                        },
                        text.clone(),
                        text,
                        false,
                    )
                }
                _ => {
                    return Err(unsupported(
                        "table comparison requires an INTEGER column or literal",
                    ));
                }
            };
            (
                ProjectValue::Call {
                    registry: CALCITE,
                    key,
                    arguments: vec![TableOperand::Column(left), right],
                    ty: insert_type(types, boolean(left_nullable || right_nullable)),
                    position: Some(tokens.coordinates(expression.span())?),
                },
                format!("{left_pretty} {token} {right_pretty}"),
                format!("({left_diagnostic} {token} {right_diagnostic})"),
            )
        }
        _ => return Err(unsupported("predicate outside the table filter slice")),
    };
    Ok(Predicate::Expression {
        value,
        pretty,
        diagnostic,
    })
}

fn signed_integer(ty: &SqlType) -> bool {
    matches!(
        ty.kind,
        TypeKind::Integer {
            width: IntegerWidth::I32 | IntegerWidth::I64,
            signed: true
        }
    )
}

pub(super) fn boolean(nullable: bool) -> SqlType {
    SqlType {
        nullable,
        origin: Origin::Absent {},
        kind: TypeKind::Boolean {},
    }
}
