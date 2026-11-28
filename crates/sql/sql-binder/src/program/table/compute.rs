//! Bind arithmetic and ABS projections over numeric table columns.

use super::{
    ProjectInput, ProjectValue, Projection, TableColumn, TableOperand, column_ordinal,
    render_column,
};
use crate::{
    Expr, Failure, Spanned,
    frel::{CALCITE, FELDERA, arithmetic_key},
    program::{graph::insert_type, source::DeclarationTokens},
    scalar_args, unsupported,
};
use feldera_sql_rel_model::{
    catalog::Name,
    expression_objects::ObjectPosition,
    operators::ABS_KEY,
    program::Program,
    types::{FloatFlavor, TypeKind},
};
use sqlparser_064::ast::Ident;

pub(super) fn projection(
    program: &mut Program,
    expression: &Expr,
    alias: Option<&Ident>,
    columns: &[TableColumn],
    qualifier: &str,
    tokens: &DeclarationTokens<'_>,
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
            let left = argument(left)?;
            let right = argument(right)?;
            let left_type = &types.types[left.ty.0 as usize];
            let right_type = &types.types[right.ty.0 as usize];
            if left_type.kind != right_type.kind {
                return Err(unsupported(
                    "arithmetic between different column types requires coercion",
                ));
            }
            let checked = match left_type.kind {
                TypeKind::Integer { .. } => true,
                TypeKind::Float {
                    flavor: FloatFlavor::Real | FloatFlavor::Double,
                } => false,
                _ => {
                    return Err(unsupported(
                        "arithmetic requires integer, REAL or DOUBLE columns",
                    ));
                }
            };
            let key = arithmetic_key(&op.to_string(), checked)?;
            let mut result = left_type.clone();
            result.nullable |= right_type.nullable;
            let ty = insert_type(types, result);
            let (left_pretty, left_diagnostic) = render(left);
            let (right_pretty, right_diagnostic) = render(right);
            (
                ProjectValue::Call {
                    registry: CALCITE,
                    key,
                    arguments: vec![TableOperand::Column(left), TableOperand::Column(right)],
                    ty,
                    position: ObjectPosition::user(tokens.coordinates(expression.span())?),
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
                    TypeKind::Integer { .. }
                        | TypeKind::Float {
                            flavor: FloatFlavor::Real | FloatFlavor::Double
                        }
                )
            {
                return Err(unsupported(
                    "expected ABS of an integer, REAL or DOUBLE column",
                ));
            }
            let (pretty, diagnostic) = render(input);
            (
                ProjectValue::Call {
                    registry: FELDERA,
                    key: ABS_KEY,
                    arguments: vec![TableOperand::Column(input)],
                    ty,
                    position: ObjectPosition::user(tokens.call_position(function.name.span(), 0)?),
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
