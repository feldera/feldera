//! Bind literal arguments of unary trigonometric functions.

use super::{ConstantOperand, graph, literal::Cell, source::SqlDisplay, table::null_cast_type};
use crate::{Expr, Failure, unsupported};
use feldera_fold_probe::{SemanticsProfile, trigonometric_kernel};
use feldera_sql_rel_model::{
    expression_objects::ObjectPosition,
    literals::LiteralValue,
    operators::OperatorObject,
    program::Program,
    relation_metadata::Coordinates,
    source::Origin,
    types::{FloatFlavor, SqlType, TypeKind},
};
use sqlparser_064::ast::CastKind;

pub(super) fn trigonometric_call(
    program: &mut Program,
    name: &str,
    arguments: &[&Expr],
    position: impl FnOnce() -> Result<Coordinates, Failure>,
) -> Result<Option<ConstantOperand>, Failure> {
    let Some(kernel) = trigonometric_kernel(name, SemanticsProfile::LegacyJanino) else {
        return Ok(None);
    };
    let &[mut argument] = arguments else {
        return Err(unsupported("expected one trigonometric argument"));
    };
    while let Expr::Nested(inner) = argument {
        argument = inner;
    }
    // Calcite folds exact SQL numbers here. With an explicitly approximate
    // argument it converts the literal and leaves the call in a Project.
    let double = TypeKind::Float {
        flavor: FloatFlavor::Double,
    };
    let (argument, cast) = match argument {
        Expr::Cast {
            kind: CastKind::Cast,
            expr,
            data_type,
            format: None,
        } => {
            let (kind, spelling) = null_cast_type(data_type)?;
            if kind != double {
                return Err(unsupported("expected a DOUBLE trigonometric argument"));
            }
            (expr.as_ref(), Some(spelling))
        }
        _ => (argument, None),
    };
    let Ok(literal) = Cell::parse(argument) else {
        return Ok(None);
    };
    let text = literal.text.render(false);
    let number: Option<f64> = match literal.value {
        LiteralValue::Integer { .. } | LiteralValue::Decimal { .. } => Some(
            text.parse()
                .map_err(|_| unsupported("invalid numeric trigonometric argument"))?,
        ),
        LiteralValue::Null {} if matches!(literal.kind, TypeKind::Null {}) => None,
        _ => return Err(unsupported("expected an exact number or NULL")),
    };
    let ty = graph::insert_type(
        &mut program.table.type_table,
        SqlType {
            kind: double,
            nullable: number.is_none(),
            origin: Origin::Absent {},
        },
    );
    let folded = cast.is_none();
    let value = number.map_or(LiteralValue::Null {}, |n| {
        let n = if folded { kernel(n) } else { n };
        LiteralValue::Float64 {
            bits: if n.is_nan() {
                f64::NAN.to_bits()
            } else {
                n.to_bits()
            },
        }
    });
    let literal = graph::append_literal(program, ty, value);
    let (root, display) = match cast {
        None => (literal, SqlDisplay::atom(format!("{name}({text})"))),
        Some(cast) => {
            let root = graph::append_constant_call(
                program,
                OperatorObject::Builtin {
                    registry: crate::frel::CALCITE.into(),
                    registry_version: "1".into(),
                    key: format!("SqlStdOperatorTable.{name}"),
                },
                vec![literal],
                ty,
                ObjectPosition::Present {
                    position: position()?,
                    internal: Some(false),
                },
            );
            let render = |quoted| format!("{name}(CAST({text} AS {}))", cast.spelling(quoted));
            let mut display = SqlDisplay::atom(render(true));
            display.diagnostic = render(false);
            (root, display)
        }
    };
    Ok(Some(ConstantOperand {
        root,
        ty,
        folded,
        display,
    }))
}
