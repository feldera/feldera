//! Expand TIMESTAMPDIFF into the interval subtraction and checked integer
//! arithmetic used by Calcite's TimestampDiffConvertlet.

use super::{
    ConstantOperand, graph,
    literal::Cell,
    source::{DeclarationTokens, SqlDisplay},
};
use crate::{Expr, Failure, unsupported};
use feldera_sql_rel_model::{
    TypeId,
    expression_objects::ObjectPosition,
    expressions::ExprId,
    literals::LiteralValue,
    operators::OperatorObject,
    program::Program,
    relation_metadata::Coordinates,
    source::Origin,
    types::{IntegerWidth, IntervalEnd, IntervalUnit, Precision, SqlType, TimeZone, TypeKind},
};

enum Scale {
    Identity,
    Multiply(i64),
    Divide(i64),
}

struct Unit {
    interval: IntervalUnit,
    scale: Scale,
    width: IntegerWidth,
    spelling: SqlDisplay,
}

impl Unit {
    fn parse(expr: &Expr) -> Result<Self, Failure> {
        let Expr::Identifier(ident) = expr else {
            return Err(unsupported("expected a TIMESTAMPDIFF unit name"));
        };
        if ident.quote_style.is_some() {
            return Err(unsupported("quoted TIMESTAMPDIFF unit"));
        }
        let name = ident.value.to_ascii_uppercase();
        use IntervalUnit::*;
        let (interval, scale) = match name.as_str() {
            "YEAR" | "SQL_TSI_YEAR" => (Year, Scale::Identity),
            "MONTH" | "SQL_TSI_MONTH" => (Month, Scale::Identity),
            "DAY" | "SQL_TSI_DAY" => (Day, Scale::Identity),
            "HOUR" | "SQL_TSI_HOUR" => (Hour, Scale::Identity),
            "MINUTE" | "SQL_TSI_MINUTE" => (Minute, Scale::Identity),
            "SECOND" | "SQL_TSI_SECOND" => (Second, Scale::Identity),
            "QUARTER" | "SQL_TSI_QUARTER" => (Month, Scale::Divide(3)),
            "CENTURY" => (Month, Scale::Divide(1200)),
            "MILLENNIUM" => (Month, Scale::Divide(12000)),
            "WEEK" | "SQL_TSI_WEEK" => (Second, Scale::Divide(604800)),
            "MILLISECOND" => (Second, Scale::Multiply(1000)),
            "MICROSECOND" | "SQL_TSI_MICROSECOND" => (Second, Scale::Multiply(1_000_000)),
            "NANOSECOND" | "SQL_TSI_FRAC_SECOND" => (Second, Scale::Multiply(1_000_000_000)),
            _ => {
                return Err(unsupported(format!(
                    "unsupported TIMESTAMPDIFF unit {ident}"
                )));
            }
        };
        let width = if matches!(name.as_str(), "NANOSECOND" | "SQL_TSI_FRAC_SECOND") {
            IntegerWidth::I64
        } else {
            IntegerWidth::I32
        };
        let spelling = if name.starts_with("SQL_TSI_") {
            let identifier = name.to_ascii_lowercase();
            let mut spelling = SqlDisplay::atom(identifier.clone());
            spelling.pretty = format!("`{identifier}`");
            spelling
        } else {
            SqlDisplay::atom(name)
        };
        Ok(Self {
            interval,
            scale,
            width,
            spelling,
        })
    }
}

fn scalar_type(program: &mut Program, kind: TypeKind) -> TypeId {
    graph::insert_type(
        &mut program.table.type_table,
        SqlType {
            kind,
            nullable: false,
            origin: Origin::Absent {},
        },
    )
}

fn call(
    program: &mut Program,
    op: &str,
    args: Vec<ExprId>,
    ty: TypeId,
    position: &Coordinates,
) -> ExprId {
    graph::append_constant_call(
        program,
        OperatorObject::Builtin {
            registry: crate::frel::CALCITE.into(),
            registry_version: "1".into(),
            key: format!("SqlStdOperatorTable.{op}"),
        },
        args,
        ty,
        ObjectPosition::Present {
            position: position.clone(),
            internal: Some(false),
        },
    )
}

pub(super) fn bind(
    program: &mut Program,
    tokens: &DeclarationTokens<'_>,
    args: &[&Expr],
    position: Coordinates,
) -> Result<ConstantOperand, Failure> {
    let &[unit, start, end] = args else {
        return Err(unsupported(
            "TIMESTAMPDIFF requires a unit and two datetime operands",
        ));
    };
    let unit = Unit::parse(unit)?;
    let start_cell = Cell::parse(start)?;
    let end_cell = Cell::parse(end)?;
    let (start, start_sql) =
        datetime_operand(program, tokens, start, &start_cell, &end_cell, &position)?;
    let (end, end_sql) = datetime_operand(program, tokens, end, &end_cell, &start_cell, &position)?;
    let interval = scalar_type(
        program,
        TypeKind::Interval {
            start: unit.interval,
            end: IntervalEnd::Single,
            leading_precision: Precision::Unspecified,
            fractional_precision: Precision::Unspecified,
        },
    );
    let delta = call(program, "MINUS_DATE", vec![end, start], interval, &position);
    let ty = scalar_type(
        program,
        TypeKind::Integer {
            signed: true,
            width: unit.width,
        },
    );
    let count = call(program, "CAST", vec![delta], ty, &position);
    let scale = match unit.scale {
        Scale::Identity => None,
        Scale::Multiply(factor) => Some(("CHECKED_MULTIPLY", factor)),
        Scale::Divide(factor) => Some(("CHECKED_DIVIDE", factor)),
    };
    let root = if let Some((op, factor)) = scale {
        let int = scalar_type(
            program,
            TypeKind::Integer {
                signed: true,
                width: IntegerWidth::I32,
            },
        );
        let factor = graph::append_integer(program, int, factor);
        call(program, op, vec![count, factor], ty, &position)
    } else {
        count
    };
    let render = |unit: &str| format!("TIMESTAMPDIFF({unit}, {start_sql}, {end_sql})");
    let mut display = SqlDisplay::atom(render(&unit.spelling.pretty));
    display.diagnostic = render(&unit.spelling.diagnostic);
    Ok(ConstantOperand {
        root,
        ty,
        folded: false,
        display,
    })
}

/// Calcite promotes a DATE paired with a TIME to TIMESTAMP(6) during validation.
fn datetime_operand(
    program: &mut Program,
    tokens: &DeclarationTokens<'_>,
    expr: &Expr,
    cell: &Cell,
    other: &Cell,
    position: &Coordinates,
) -> Result<(ExprId, String), Failure> {
    if !matches!(
        cell.value,
        LiteralValue::Date { .. } | LiteralValue::Time { .. } | LiteralValue::Timestamp { .. }
    ) {
        return Err(unsupported("expected a datetime literal for TIMESTAMPDIFF"));
    }
    let (root, _) = cell.emit(program, tokens, expr)?;
    let text = cell.text.render(false).into_owned();
    if matches!(cell.kind, TypeKind::Date {}) && matches!(other.kind, TypeKind::Time { .. }) {
        let ty = scalar_type(
            program,
            TypeKind::Timestamp {
                precision: Precision::Specified(6),
                zone: TimeZone::None,
            },
        );
        Ok((
            call(program, "CAST", vec![root], ty, position),
            format!("CAST({text} AS TIMESTAMP(6))"),
        ))
    } else {
        Ok((root, text))
    }
}
