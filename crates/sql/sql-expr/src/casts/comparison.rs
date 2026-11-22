//! Comparisons between scalar literals in the optimizer's Simplify pass.
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, Node, NodeId,
    TypeKind,
};
use std::cmp::Ordering;

pub(super) fn fold(editor: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
    let binary = editor.node(id)?;
    let left = editor.expression(binary.edge("left")?)?;
    let right = editor.expression(binary.edge("right")?)?;
    let kind = editor.expr_kind(left)?;
    let ty = editor.expression_type(left)?;
    if !kind.wire_name().ends_with("Literal")
        || kind != editor.expr_kind(right)?
        || !editor.same_type(ty, editor.expression_type(right)?)?
    {
        return Ok(id);
    }
    let opcode = binary
        .data("opcode")
        .and_then(|v| v.as_str())
        .and_then(BinaryOp::from_wire);
    let output = editor.type_id(binary.edge("type")?)?;
    let left_null = editor.is_null_literal(left)?;
    let right_null = editor.is_null_literal(right)?;
    let result = if left_null || right_null {
        if opcode != Some(BinaryOp::IsDistinct) {
            return editor.typed_null(output).map(Into::into);
        }
        left_null != right_null
    } else {
        let a = editor.node(left.into())?;
        let b = editor.node(right.into())?;
        let ty = editor.type_kind(ty)?;
        match opcode {
            Some(BinaryOp::Eq | BinaryOp::Neq | BinaryOp::IsDistinct) => {
                let Some(equal) = same_literal_value(editor, left, right)? else {
                    return Ok(id);
                };
                if opcode == Some(BinaryOp::Eq) {
                    equal
                } else {
                    !equal
                }
            }
            _ => {
                let Some(order) = compare_numbers(ty, a, b)? else {
                    return Ok(id);
                };
                match opcode {
                    Some(BinaryOp::Lt) => order.is_lt(),
                    Some(BinaryOp::Lte) => order.is_le(),
                    Some(BinaryOp::Gt) => order.is_gt(),
                    Some(BinaryOp::Gte) => order.is_ge(),
                    _ => return Ok(id),
                }
            }
        }
    };
    editor
        .make_expr(
            id,
            Expr::BoolLiteral,
            output,
            [("value", Field::Data(result.into()))],
        )
        .map(Into::into)
}

/// Java's ISameValue comparison on scalar literals. Decimal scale and raw
/// string spelling are significant. Floating NaNs compare equal and zoned
/// timestamps compare by their runtime microsecond representation.
pub fn same_literal_value(
    editor: &CircuitEditor,
    left: ExpressionId,
    right: ExpressionId,
) -> Result<Option<bool>, Error> {
    let kind = editor.expr_kind(left)?;
    if kind != editor.expr_kind(right)? {
        return Ok(Some(false));
    }
    let a = editor.node(left.into())?;
    let b = editor.node(right.into())?;
    let ty = editor.expression_type(left)?;
    let equal = match kind {
        Expr::DecimalLiteral => {
            editor.same_type(ty, editor.expression_type(right)?)?
                && a.data("value") == b.data("value")
        }
        Expr::KeywordLiteral => a.data("keyword") == b.data("keyword"),
        Expr::StrLiteral => a.data("raw") == b.data("raw") && a.data("value") == b.data("value"),
        Expr::RealLiteral | Expr::DoubleLiteral if a.data("value") != b.data("value") => {
            if a.data("value").is_none() || b.data("value").is_none() {
                false
            } else {
                compare_numbers(editor.type_kind(ty)?, a, b)? == Some(Ordering::Equal)
            }
        }
        Expr::TimestampTzLiteral if a.data("value") != b.data("value") => {
            let (Some(a), Some(b)) = (a.data("value"), b.data("value")) else {
                return Ok(Some(false));
            };
            let (Some(a), Some(b)) = (
                a.as_str().and_then(zoned_micros),
                b.as_str().and_then(zoned_micros),
            ) else {
                return Ok(None);
            };
            a == b
        }
        kind if kind.wire_name().ends_with("Literal") => a.data("value") == b.data("value"),
        _ => return Err(Error::new("expected a scalar literal")),
    };
    Ok(Some(equal))
}

fn float_value(node: &Node, ty: TypeKind) -> Result<f64, Error> {
    let bits = node
        .data("value")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| Error::new("invalid floating-point literal bits"))?;
    Ok(if ty == TypeKind::Real {
        f64::from(f32::from_bits(bits as u32))
    } else {
        f64::from_bits(bits as u64)
    })
}

fn exact_value(node: &Node) -> Result<bigdecimal::BigDecimal, Error> {
    let value = node
        .data("value")
        .ok_or_else(|| Error::new("missing numeric literal"))?;
    if let Some(n) = value.as_i64() {
        return Ok(n.into());
    }
    if let Some(n) = value.as_u64() {
        return Ok(n.into());
    }
    value
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::new("invalid exact numeric literal"))
}

/// Implements the compiler's strict positive-literal test for waterline scaling.
/// Java's numeric ordering puts NaN above infinity, but NaN is never a positive factor.
pub fn literal_is_positive(editor: &CircuitEditor, literal: ExpressionId) -> Result<bool, Error> {
    let node = editor.node(literal.into())?;
    if node.data("value").is_none_or(|value| value.is_null()) {
        return Ok(false);
    }
    let ty = editor.type_kind(editor.expression_type(literal)?)?;
    match ty {
        TypeKind::Real | TypeKind::Double => Ok(float_value(node, ty)? > 0.0),
        TypeKind::Integer
        | TypeKind::Decimal
        | TypeKind::ISize
        | TypeKind::USize
        | TypeKind::LongInterval
        | TypeKind::ShortInterval => Ok(exact_value(node)? > 0),
        _ => Ok(false),
    }
}

fn compare_numbers(ty: TypeKind, a: &Node, b: &Node) -> Result<Option<Ordering>, Error> {
    use TypeKind as Ty;
    let order = match ty {
        Ty::Real | Ty::Double => {
            // Java Float.compare and Double.compare order all NaNs together above infinity.
            let canonical = |n: f64| if n.is_nan() { f64::NAN } else { n };
            canonical(float_value(a, ty)?).total_cmp(&canonical(float_value(b, ty)?))
        }
        Ty::Integer
        | Ty::Decimal
        | Ty::ISize
        | Ty::USize
        | Ty::LongInterval
        | Ty::ShortInterval => exact_value(a)?.cmp(&exact_value(b)?),
        _ => return Ok(None),
    };
    Ok(Some(order))
}

/// Calcite stores local time and a zone name. Java resolves an overlap with
/// the earlier offset and moves a time in a spring gap forward by the gap.
fn zoned_micros(value: &str) -> Option<i64> {
    use chrono::{FixedOffset, NaiveDateTime, Offset, TimeZone};
    let (time, zone) = value.rsplit_once(' ')?;
    let local = NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S%.f").ok()?;
    if let Ok(tz) = zone.parse::<chrono_tz::Tz>() {
        if let Some(instant) = tz.from_local_datetime(&local).earliest() {
            return Some(instant.timestamp_micros());
        }
        let (_, before) = chrono_tz::GapInfo::new(&local, &tz)?.begin?;
        return before
            .fix()
            .from_local_datetime(&local)
            .single()
            .map(|t| t.timestamp_micros());
    }
    let offset = zone
        .strip_prefix("GMT")
        .unwrap_or(zone)
        .parse::<FixedOffset>()
        .ok()?;
    Some(
        offset
            .from_local_datetime(&local)
            .single()?
            .timestamp_micros(),
    )
}

#[cfg(test)]
mod tests {
    use super::zoned_micros;

    #[test]
    fn canonical_timestamp_handles_offsets_gaps_and_overlaps() {
        for (local, utc) in [
            (
                "2020-01-01 08:10:10.1234569 America/New_York",
                "2020-01-01 13:10:10.123456 UTC",
            ),
            (
                "2024-03-10 02:30:00 America/New_York",
                "2024-03-10 07:30:00 UTC",
            ),
            (
                "2024-11-03 01:30:00 America/New_York",
                "2024-11-03 05:30:00 UTC",
            ),
            ("2020-01-01 05:30:00 GMT+05:30", "2020-01-01 00:00:00 UTC"),
        ] {
            let expected = zoned_micros(utc).unwrap();
            assert_eq!(zoned_micros(local), Some(expected));
        }
    }
}
