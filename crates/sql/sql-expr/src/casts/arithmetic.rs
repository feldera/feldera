//! Scalar arithmetic identities used by the circuit simplifier.
use super::SimplifyWarning;
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId,
    TypeKind as Ty,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Literal {
    Null,
    Zero,
    One,
    Other,
}

/// Decimal equality includes scale, and floating-point equality distinguishes signed zero.
fn numeric_literal(editor: &CircuitEditor, expr: ExpressionId) -> Result<Option<Literal>, Error> {
    let kind = editor.expr_kind(expr)?;
    if !kind.wire_name().ends_with("Literal") {
        return Ok(None);
    }
    let ty = editor.type_kind(editor.expression_type(expr)?)?;
    if !matches!(
        ty,
        Ty::Integer | Ty::ISize | Ty::USize | Ty::Decimal | Ty::Real | Ty::Double
    ) {
        return Ok(None);
    }
    let Some(value) = editor.node(expr.into())?.data("value") else {
        return Ok(Some(Literal::Null));
    };
    let literal = match ty {
        Ty::Real | Ty::Double => {
            let one = if ty == Ty::Real {
                i64::from(1f32.to_bits())
            } else {
                1f64.to_bits() as i64
            };
            match value.as_i64() {
                Some(0) => Literal::Zero,
                Some(bits) if bits == one => Literal::One,
                _ => Literal::Other,
            }
        }
        _ => match value.as_str() {
            Some("0") => Literal::Zero,
            Some("1") => Literal::One,
            _ => Literal::Other,
        },
    };
    Ok(Some(literal))
}

/// Keep the unary expression when its value cannot be represented at the operand width.
pub(super) fn negate(
    editor: &mut CircuitEditor,
    operand: ExpressionId,
) -> Result<Option<ExpressionId>, Error> {
    let kind = editor.expr_kind(operand)?;
    let ty = editor.expression_type(operand)?;
    let Some(value) = editor.node(operand.into())?.data("value") else {
        return Ok(Some(operand));
    };
    let number = match kind {
        Expr::DoubleLiteral | Expr::RealLiteral => {
            let bits = value
                .as_i64()
                .ok_or_else(|| Error::new("expected floating-point literal bits"))?;
            if kind == Expr::DoubleLiteral {
                let ty = editor.float_literal_type(ty, editor.nullable(ty)?)?;
                let id = editor.derive_unlocated_inner(
                    operand.into(),
                    [
                        ("type", Field::Reference(ty.into())),
                        ("value", Field::Data((bits ^ i64::MIN).into())),
                    ],
                )?;
                return editor.expression(id).map(Some);
            }
            ((bits as i32) ^ i32::MIN).into()
        }
        Expr::DecimalLiteral => {
            let text = value
                .as_str()
                .ok_or_else(|| Error::new("expected decimal literal"))?;
            let magnitude = text.trim_start_matches('-');
            let mantissa = magnitude.split(['E', 'e']).next().unwrap_or(magnitude);
            if text.starts_with('-') || !mantissa.bytes().any(|b| matches!(b, b'1'..=b'9')) {
                magnitude.into()
            } else {
                format!("-{text}").into()
            }
        }
        Expr::I8Literal
        | Expr::I16Literal
        | Expr::I32Literal
        | Expr::I64Literal
        | Expr::I128Literal
        | Expr::ISizeLiteral
        | Expr::LongIntervalLiteral
        | Expr::ShortIntervalLiteral => {
            let value = value
                .as_str()
                .and_then(|v| v.parse::<i128>().ok())
                .ok_or_else(|| Error::new("expected signed literal"))?;
            let min = match kind {
                Expr::I8Literal => i128::from(i8::MIN),
                Expr::I16Literal => i128::from(i16::MIN),
                Expr::I32Literal | Expr::LongIntervalLiteral => i128::from(i32::MIN),
                Expr::I128Literal => i128::MIN,
                _ => i128::from(i64::MIN),
            };
            if value == min {
                return Ok(None);
            }
            (-value).to_string().into()
        }
        _ => return Ok(None),
    };
    editor
        .make_expr(operand.into(), kind, ty, [("value", Field::Data(number))])
        .map(Some)
}

fn multiply_interval(
    editor: &mut CircuitEditor,
    interval: ExpressionId,
    factor: ExpressionId,
) -> Result<Option<ExpressionId>, Error> {
    let ty = editor.expression_type(interval)?;
    let kind = editor.expr_kind(interval)?;
    if !matches!(kind, Expr::LongIntervalLiteral | Expr::ShortIntervalLiteral)
        || editor.type_kind(editor.expression_type(factor)?)? != Ty::Integer
        || !editor.expr_kind(factor)?.wire_name().ends_with("Literal")
    {
        return Ok(None);
    }
    let read = |expr: ExpressionId| -> Result<Option<num_bigint::BigInt>, Error> {
        editor
            .node(expr.into())?
            .data("value")
            .map(|v| {
                v.as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| Error::new("expected integer or interval literal"))
            })
            .transpose()
    };
    let Some(value) = read(interval)? else {
        return Ok(Some(interval));
    };
    let Some(factor) = read(factor)? else {
        return Ok(None);
    };
    use num_traits::ToPrimitive;
    let product = value * factor;
    let Some(value) = product.to_i64() else {
        return Ok(None);
    };
    let field = ("value", Field::Data(value.to_string().into()));
    if kind == Expr::LongIntervalLiteral {
        if i32::try_from(value).is_err() {
            return Ok(None);
        }
        return editor
            .make_expr(interval.into(), kind, ty, [field])
            .map(Some);
    }
    // The microsecond constructor creates a fresh, non-nullable type with the interval's units.
    let ty =
        editor.derive_unlocated_inner(ty.into(), [("mayBeNull", Field::Data(false.into()))])?;
    let literal =
        editor.derive_unlocated_inner(interval.into(), [("type", Field::Reference(ty)), field])?;
    editor.expression(literal).map(Some)
}

pub(super) fn simplify(
    editor: &mut CircuitEditor,
    id: NodeId,
    warnings: Option<&mut Vec<SimplifyWarning>>,
) -> Result<NodeId, Error> {
    use BinaryOp::{Add, Coalesce, Div, DivInterval, Mod, Mul, MulInterval, Sub};
    use Literal::{Null, One, Zero};
    let node = editor.node(id)?;
    let Some(op @ (Add | Sub | Mul | MulInterval | Div | DivInterval | Mod | Coalesce)) = node
        .data("opcode")
        .and_then(|v| v.as_str())
        .and_then(BinaryOp::from_wire)
    else {
        return Ok(id);
    };
    let left = editor.expression(node.edge("left")?)?;
    let right = editor.expression(node.edge("right")?)?;
    let ty = editor.type_id(node.edge("type")?)?;
    let lt = editor.expression_type(left)?;
    let rt = editor.expression_type(right)?;
    let l = numeric_literal(editor, left)?;
    let r = numeric_literal(editor, right)?;
    let float = |ty| matches!(ty, Ty::Real | Ty::Double);

    // Java inspects the left literal first, even when both sides are constants.
    let operand = match op {
        Add if l == Some(Zero) => Some(right),
        Add | Sub if l == Some(Null) => Some(left),
        Add | Sub if l.is_none() && r == Some(Zero) => Some(left),
        Add | Sub if l.is_none() && r == Some(Null) => Some(right),
        Mul | MulInterval if l == Some(One) => Some(right),
        Mul | MulInterval if l == Some(Null) => Some(left),
        Mul | MulInterval
            if l == Some(Zero) && !editor.nullable(rt)? && !float(editor.type_kind(lt)?) =>
        {
            Some(left)
        }
        Mul | MulInterval if l.is_none() && r == Some(One) => Some(left),
        Mul | MulInterval if l.is_none() && r == Some(Null) => Some(right),
        Mul | MulInterval
            if l.is_none()
                && r == Some(Zero)
                && !editor.nullable(lt)?
                && !float(editor.type_kind(rt)?) =>
        {
            Some(right)
        }
        Mul | MulInterval if l.is_some() => multiply_interval(editor, right, left)?,
        Mul | MulInterval if r.is_some() => multiply_interval(editor, left, right)?,
        Div | DivInterval if r == Some(One) => Some(left),
        Div | DivInterval | Mod
            if r == Some(Zero) && (op == Mod || !float(editor.type_kind(rt)?)) =>
        {
            if let Some(warnings) = warnings {
                warnings.push(SimplifyWarning {
                    position: node
                        .field("position")
                        .map(Field::source_range)
                        .transpose()?
                        .unwrap_or_default(),
                    kind: "Division by zero",
                    message: if op == Mod {
                        " Modulus by constant zero value as divisor."
                    } else {
                        " Division by constant zero value."
                    }
                    .into(),
                });
            }
            None
        }
        Coalesce if editor.expr_kind(left)?.wire_name().ends_with("Literal") => {
            Some(if editor.is_null_literal(left)? {
                right
            } else {
                left
            })
        }
        Coalesce if editor.is_null_literal(right)? => Some(left),
        _ => None,
    };
    let result = match operand {
        Some(value) => value,
        None if op == Mod || op == Coalesce => return Ok(id),
        None => {
            let folded = fold_integer_arithmetic(editor, id)?;
            editor.expression(folded)?
        }
    };
    editor.cast(id, result, ty).map(Into::into)
}

fn fold_integer_arithmetic(editor: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
    let binary = editor.node(id)?;
    let left = editor.expression(binary.edge("left")?)?;
    let right = editor.expression(binary.edge("right")?)?;
    let kind = editor.expr_kind(left)?;
    if editor.type_kind(editor.expression_type(left)?)? == Ty::Integer
        && kind.wire_name().ends_with("Literal")
        && editor.expr_kind(right)?.wire_name().ends_with("Literal")
    {
        for operand in [left, right] {
            if editor.is_null_literal(operand)? {
                return Ok(operand.into());
            }
        }
    }
    if !matches!(kind, Expr::I8Literal | Expr::I32Literal) || editor.expr_kind(right)? != kind {
        return Ok(id);
    }
    let value = |expr: ExpressionId| -> Result<Option<i32>, Error> {
        editor
            .node(expr.into())?
            .data("value")
            .map(|value| {
                value
                    .as_str()
                    .and_then(|s| s.parse::<i32>().ok())
                    .ok_or_else(|| Error::new("invalid signed integer literal"))
            })
            .transpose()
    };
    let (Some(a), Some(b)) = (value(left)?, value(right)?) else {
        return Ok(id);
    };
    let result = match binary.data("opcode").and_then(|op| op.as_str()) {
        Some("ADD") => a.checked_add(b),
        Some("SUB") => a.checked_sub(b),
        Some("MUL") => a.checked_mul(b),
        Some("DIV") => a.checked_div(b),
        _ => None,
    };
    let Some(result) = result.filter(|n| kind != Expr::I8Literal || i8::try_from(*n).is_ok())
    else {
        return Ok(id);
    };
    let ty = editor.expression_type(left)?;
    editor
        .make_expr(
            left.into(),
            kind,
            ty,
            [("value", Field::Data(result.to_string().into()))],
        )
        .map(Into::into)
}
