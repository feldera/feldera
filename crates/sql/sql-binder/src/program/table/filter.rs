//! Bind a predicate on a single plain table.

mod comparison;

use super::{ProjectInput, ProjectValue, TableColumn, TableOperand, column_ordinal, render_column};
use crate::{
    Expr, Failure, Spanned, Value,
    frel::CALCITE,
    program::{graph::insert_type, source::DeclarationTokens},
    unsupported,
};
use feldera_sql_rel_model::{
    expression_objects::ObjectPosition,
    program::Program,
    source::Origin,
    types::{SqlType, TypeKind},
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
    let column = |expression: &Expr| {
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
            let value = if types.types[input.ty.0 as usize].nullable {
                ProjectValue::Call {
                    registry: CALCITE,
                    key: "SqlStdOperatorTable.IS_TRUE",
                    arguments: vec![TableOperand::Column(input)],
                    ty: insert_type(types, boolean(false)),
                    position: ObjectPosition::user(tokens.end_at_is_true(operand.span())?),
                }
            } else {
                ProjectValue::Column(input)
            };
            (
                value,
                format!("{pretty} IS TRUE"),
                format!("({diagnostic} IS TRUE)"),
            )
        }
        Expr::BinaryOp { left, op, right } => {
            return comparison::bind(types, left, op, right, &column, tokens);
        }
        _ => return Err(unsupported("predicate outside the table filter slice")),
    };
    Ok(Predicate::Expression {
        value,
        pretty,
        diagnostic,
    })
}

pub(super) fn boolean(nullable: bool) -> SqlType {
    SqlType {
        nullable,
        origin: Origin::Absent {},
        kind: TypeKind::Boolean {},
    }
}
