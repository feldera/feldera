//! Bind an integer arithmetic or floating-point ABS projection.

use super::{
    ProjectInput, ProjectValue, Projection, TableColumn, TableOperand, column_ordinal,
    render_column,
};
use crate::{
    BinaryOperator, Expr, Failure, Spanned,
    frel::{CALCITE, FELDERA},
    program::{
        graph::insert_type,
        source::{coordinates, end_at_call_close},
    },
    scalar_args, unsupported,
};
use feldera_sql_rel_model::{
    catalog::Name,
    program::Program,
    types::{FloatFlavor, IntegerWidth, TypeKind},
};
use sqlparser_064::ast::Ident;

pub(super) fn projection(
    program: &mut Program,
    expression: &Expr,
    alias: Option<&Ident>,
    columns: &[TableColumn],
    qualifier: &str,
    sql: &str,
    ordinal: usize,
) -> Result<Projection, Failure> {
    let argument = |value| {
        let index = column_ordinal(value, columns, qualifier)?;
        let column = &columns[index];
        Ok(ProjectInput {
            field: index as u32,
            ty: column.ty,
        })
    };
    let render =
        |value: ProjectInput| render_column(qualifier, &columns[value.field as usize].name.name);
    let types = &mut program.table.type_table;
    let (value, pretty, diagnostic) = match expression {
        Expr::BinaryOp { left, op, right } => {
            let key = match op {
                BinaryOperator::Plus => "SqlStdOperatorTable.CHECKED_PLUS",
                BinaryOperator::Minus => "SqlStdOperatorTable.CHECKED_MINUS",
                BinaryOperator::Multiply => "SqlStdOperatorTable.CHECKED_MULTIPLY",
                _ => return Err(unsupported("arithmetic outside the table projection slice")),
            };
            let left = argument(left)?;
            let right = argument(right)?;
            let operands = [
                &types.types[left.ty.0 as usize],
                &types.types[right.ty.0 as usize],
            ];
            if left.field == right.field
                || !operands.iter().all(|operand| {
                    matches!(
                        operand.kind,
                        TypeKind::Integer {
                            width: IntegerWidth::I32,
                            signed: true
                        }
                    )
                })
            {
                return Err(unsupported(
                    "arithmetic requires distinct signed INTEGER columns",
                ));
            }
            let mut result = operands[0].clone();
            result.nullable = operands.iter().any(|operand| operand.nullable);
            let ty = insert_type(types, result);
            let (left_pretty, left_diagnostic) = render(left);
            let (right_pretty, right_diagnostic) = render(right);
            (
                ProjectValue::Call {
                    registry: CALCITE,
                    key,
                    arguments: vec![TableOperand::Column(left), TableOperand::Column(right)],
                    ty,
                    position: Some(coordinates(expression.span())),
                },
                format!("{left_pretty} {op} {right_pretty}"),
                format!("({left_diagnostic} {op} {right_diagnostic})"),
            )
        }
        Expr::Function(function) => {
            let (name, arguments) = scalar_args(function)?;
            let [source] = arguments.as_slice() else {
                return Err(unsupported("expected one ABS argument"));
            };
            let input = argument(source)?;
            let ty = input.ty;
            if !name.eq_ignore_ascii_case("ABS")
                || !matches!(
                    types.types[ty.0 as usize].kind,
                    TypeKind::Float {
                        flavor: FloatFlavor::Double
                    }
                )
            {
                return Err(unsupported(
                    "table function projection requires ABS of DOUBLE",
                ));
            }
            let (pretty, diagnostic) = render(input);
            (
                ProjectValue::Call {
                    registry: FELDERA,
                    key: "org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions$AbsFunction#abs",
                    arguments: vec![TableOperand::Column(input)],
                    ty,
                    position: Some(end_at_call_close(sql, expression.span(), source.span())?),
                },
                format!("ABS({pretty})"),
                format!("ABS({diagnostic})"),
            )
        }
        _ => return Err(unsupported("expression outside the computed table slice")),
    };
    let mut result = Projection {
        name: Name {
            name: format!("EXPR${ordinal}"),
            quoted: true,
        },
        value,
        pretty,
        diagnostic,
    };
    result.apply_alias(alias);
    Ok(result)
}
