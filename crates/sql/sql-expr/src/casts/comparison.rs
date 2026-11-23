//! Comparisons between scalar literals in the optimizer's Simplify pass.
use super::zoned_timestamp::zoned_micros;
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, Node,
    NodeId, TypeKind,
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

/// Java's BigDecimal spelling distinguishes the scale as well as the coefficient.
pub fn decimal_literal_parts(node: &Node) -> Result<(String, i64), Error> {
    let (coefficient, scale) = exact_value(node)?.into_bigint_and_scale();
    Ok((coefficient.to_string(), scale))
}

/// Implements the compiler's strict positive-literal test for waterline scaling.
/// Java's numeric ordering puts NaN above infinity, but NaN is never a positive factor.
pub fn literal_is_positive(editor: &CircuitEditor, literal: ExpressionId) -> Result<bool, Error> {
    numeric_value_is_positive(editor.node(literal.into())?)
}

/// Compare the encoded literal directly, so a read-only circuit analysis and
/// an expression rewrite use the same exact-decimal and floating-point rules.
pub fn numeric_value_is_positive(node: &Node) -> Result<bool, Error> {
    if node.data("value").is_none_or(|value| value.is_null()) {
        return Ok(false);
    }
    match node.inner_kind() {
        Some(InnerKind::Expression(Expr::RealLiteral)) => {
            Ok(float_value(node, TypeKind::Real)? > 0.0)
        }
        Some(InnerKind::Expression(Expr::DoubleLiteral)) => {
            Ok(float_value(node, TypeKind::Double)? > 0.0)
        }
        Some(InnerKind::Expression(kind)) if kind.is_numeric_literal() => {
            Ok(exact_value(node)? > 0)
        }
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
