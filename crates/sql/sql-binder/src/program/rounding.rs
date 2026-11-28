//! Constant reduction for Calcite's exact numeric ROUND and TRUNCATE overloads.

use super::{
    ConstantOperand, graph,
    literal::{Cell, CellText},
    source::SqlDisplay,
    table::null_cast_type,
};
use crate::{DataType, Expr, Failure, unsupported};
use feldera_sql_rel_model::{
    literals::{DecimalRounding, ExactDecimal, LiteralValue},
    program::Program,
    source::Origin,
    types::{IntegerWidth, SqlType, TypeKind},
};
use sqlparser_064::ast::{CastKind, ExactNumberInfo, UnaryOperator};

pub(super) fn bind(
    program: &mut Program,
    name: &str,
    arguments: &[&Expr],
    fold: bool,
) -> Result<ConstantOperand, Failure> {
    let (value, places) = match arguments {
        [value] => (value, None),
        [value, places] => (value, Some(places)),
        _ => {
            return Err(unsupported(
                "expected a number and an optional rounding scale",
            ));
        }
    };
    if !fold {
        return Err(unsupported("rounding requires runtime evaluation"));
    }
    let mut value = Operand::parse(value)?;
    let mut places = places.map(|expr| Cell::parse(expr)).transpose()?;
    let digits = match &places {
        None => Some(0),
        Some(scale) => scale.as_nullable_integer()?,
    };
    if let Some(scale) = &mut places
        && matches!(scale.kind, TypeKind::Null {})
    {
        // Calcite validates an unknown scale as its default fractional DECIMAL.
        // With two unknowns it also infers that type for the value.
        let ty = DataType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 19));
        let (kind, text) = null_cast_type(&ty)?;
        scale.text = CellText::CastNull(text);
        if matches!(value.kind, TypeKind::Null {}) {
            value.kind = kind;
            value.display = SqlDisplay::literal(&scale.text);
        }
    }
    let rounding = if name == "ROUND" {
        DecimalRounding::HalfUp
    } else {
        DecimalRounding::Truncate
    };
    let literal = round(&value, digits, rounding)?;
    let render = |quoted| {
        let operand = if quoted {
            &value.display.pretty
        } else {
            &value.display.diagnostic
        };
        match &places {
            None => format!("{name}({operand})"),
            Some(places) => format!("{name}({operand}, {})", places.text.render(quoted)),
        }
    };
    let mut display = SqlDisplay::atom(render(true));
    display.diagnostic = render(false);
    let ty = graph::insert_type(
        &mut program.table.type_table,
        SqlType {
            nullable: matches!(literal, LiteralValue::Null {}),
            kind: value.kind,
            origin: Origin::Absent {},
        },
    );
    Ok(ConstantOperand {
        root: graph::append_literal(program, ty, literal),
        ty,
        folded: true,
        reduction_changed: true,
        display,
    })
}

struct Operand {
    kind: TypeKind,
    value: LiteralValue,
    display: SqlDisplay,
}

impl Operand {
    fn parse(expr: &Expr) -> Result<Self, Failure> {
        if let Ok(cell) = Cell::parse(expr) {
            return Ok(Self {
                display: SqlDisplay::literal(&cell.text),
                kind: cell.kind,
                value: cell.value,
            });
        }
        let (expr, _) = super::without_parentheses(expr);
        let (expr, negate) = match expr {
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => (super::without_parentheses(expr).0, true),
            _ => (expr, false),
        };
        let Expr::Cast {
            kind: cast @ (CastKind::Cast | CastKind::DoubleColon),
            expr,
            data_type,
            format: None,
        } = expr
        else {
            return Err(unsupported("expected a literal numeric rounding operand"));
        };
        let (kind, ty) = null_cast_type(data_type)?;
        let TypeKind::Decimal {
            precision, scale, ..
        } = kind
        else {
            return Err(unsupported("expected an exact decimal cast"));
        };
        let cell = Cell::parse(expr)?;
        let value = if matches!(cell.value, LiteralValue::Null {}) {
            LiteralValue::Null {}
        } else {
            let mut decimal = cell
                .value
                .as_exact_decimal()
                .and_then(|n| n.at_sql_scale(scale, DecimalRounding::Truncate))
                .filter(|n| n.coefficient.trim_start_matches('-').len() <= precision as usize)
                .ok_or_else(|| unsupported("rounding operand needs a runtime decimal cast"))?;
            if negate {
                let n = decimal
                    .coefficient
                    .parse::<i128>()
                    .map_err(|_| unsupported("invalid decimal coefficient"))?;
                decimal.coefficient = (-n).to_string();
            }
            LiteralValue::Decimal { value: decimal }
        };
        let render = |quoted| {
            let literal = cell.text.render(quoted);
            let ty = ty.spelling(quoted);
            let cast = if *cast == CastKind::Cast {
                format!("CAST({literal} AS {ty})")
            } else {
                format!("({literal} :: {ty})")
            };
            match (negate, quoted) {
                (false, _) => cast,
                (true, true) => format!("- {cast}"),
                (true, false) => format!("(- {cast})"),
            }
        };
        let mut display = SqlDisplay::atom(render(true));
        display.diagnostic = render(false);
        Ok(Self {
            kind,
            value,
            display,
        })
    }
}

fn round(
    value: &Operand,
    digits: Option<i32>,
    rounding: DecimalRounding,
) -> Result<LiteralValue, Failure> {
    if !matches!(
        value.kind,
        TypeKind::Integer { signed: true, .. } | TypeKind::Decimal { .. }
    ) {
        return Err(unsupported("expected an exact numeric rounding operand"));
    }
    let Some(digits) = digits else {
        return Ok(LiteralValue::Null {});
    };
    if matches!(value.value, LiteralValue::Null {}) {
        return Ok(LiteralValue::Null {});
    }
    // BigDecimal.setScale can fail at extreme positions. In that case Calcite
    // retains the call and can leave sibling projections unreduced.
    if !(-100..=100).contains(&digits) {
        return Err(unsupported(
            "rounding outside the measured constant scale range",
        ));
    }
    let n = value
        .value
        .as_exact_decimal()
        .and_then(|n| n.rounded_coefficient(digits, rounding))
        .ok_or_else(|| unsupported("rounding exceeds the decimal coefficient range"))?;
    match value.kind {
        TypeKind::Decimal {
            precision, scale, ..
        } => {
            if n.unsigned_abs() >= 10_u128.pow(precision) {
                return Err(unsupported(
                    "rounding exceeds the declared decimal precision",
                ));
            }
            Ok(LiteralValue::Decimal {
                value: ExactDecimal {
                    coefficient: n.to_string(),
                    scale: scale as i32,
                },
            })
        }
        TypeKind::Integer { width, .. } => {
            // The reference converts BigDecimal back with intValue or longValue.
            let value = match width {
                IntegerWidth::I32 => (n as i32).to_string(),
                IntegerWidth::I64 => (n as i64).to_string(),
                _ => return Err(unsupported("expected an INTEGER or BIGINT literal")),
            };
            Ok(LiteralValue::Integer { value })
        }
        _ => Err(unsupported("expected an exact numeric literal")),
    }
}
