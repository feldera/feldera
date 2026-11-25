//! Constant conversions used by the Java default printer's simplification pass.
use super::temporal;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId, TypeId,
    TypeKind as Ty,
};
use num_bigint::BigInt;
use num_traits::{FromPrimitive, One, ToPrimitive};

/// Leave a cast in the expression when the runtime must report an error or round a float.
pub(super) fn fold(
    editor: &mut CircuitEditor,
    cast: NodeId,
    source: ExpressionId,
    target: TypeId,
) -> Result<Option<ExpressionId>, Error> {
    let source_kind = editor.expr_kind(source)?;
    let source_type = editor.type_kind(editor.expression_type(source)?)?;
    let target_kind = editor.type_kind(target)?;
    let Some(value) = editor
        .node(source.into())?
        .data("value")
        .and_then(|v| v.as_str())
    else {
        return Ok(None);
    };
    let from_text = source_kind == Expr::StringLiteral;
    if from_text && !value.is_ascii() {
        return Ok(None);
    }
    let text = if from_text {
        value.trim_matches(|c: char| c <= ' ')
    } else {
        value
    };
    let result = match (source_type, target_kind) {
        (Ty::String, Ty::Bool) if from_text => {
            let value = if text.eq_ignore_ascii_case("true") {
                true
            } else if text.eq_ignore_ascii_case("false") {
                false
            } else {
                return Ok(None);
            };
            Some((Expr::BoolLiteral, Field::Data(value.into())))
        }
        (Ty::String, Ty::Date | Ty::Timestamp | Ty::Time) if from_text => {
            temporal::fold(text, target_kind)
        }
        (Ty::String, Ty::Uuid) if from_text => {
            // Calcite passes the untrimmed string to its UUID parser.
            uuid_literal(value).map(|v| (Expr::UuidLiteral, Field::Data(v.to_string().into())))
        }
        (Ty::String | Ty::Integer | Ty::Decimal, Ty::Decimal) => {
            let ty = editor.node(target.into())?;
            let precision = ty.data("precision").and_then(|v| v.as_u64());
            let scale = ty.data("scale").and_then(|v| v.as_u64());
            let (Some(precision), Some(scale)) = (precision, scale) else {
                return Err(Error::new("DECIMAL requires a precision and scale"));
            };
            decimal(text, precision, scale, from_text)
                .map(|v| (Expr::DecimalLiteral, Field::Data(v.into())))
        }
        (Ty::Integer | Ty::String, Ty::Integer | Ty::Double | Ty::Real | Ty::ISize | Ty::USize) => {
            let number = if from_text {
                if target_kind != Ty::Integer {
                    return Ok(None);
                }
                if text.starts_with('-')
                    && editor
                        .node(target.into())?
                        .data("signed")
                        .and_then(|v| v.as_bool())
                        == Some(false)
                {
                    return Ok(None);
                }
                text.parse::<i64>().ok().map(BigInt::from)
            } else {
                text.parse().ok()
            };
            let Some(number) = number else {
                return Ok(None);
            };
            integer(editor, &number, target, target_kind)?
        }
        _ => None,
    };
    if let Some((Expr::UuidLiteral, value)) = result {
        let ty = editor.uuid_type(source.into(), editor.nullable(target)?)?;
        let node = editor.empty_origin_node(
            source.into(),
            feldera_sql_ir::InnerKind::Expression(Expr::UuidLiteral),
            [("value", value), ("type", Field::Reference(ty.into()))],
        )?;
        return editor.expression(node).map(Some);
    }
    // Integer folding attributes a conversion to the original operand, even
    // when that operand is a cast that folded earlier in this traversal.
    let origin = match (source_type, target_kind) {
        (Ty::Integer, _) => editor.node(cast)?.edge("source")?,
        (Ty::String, Ty::Decimal) => target.into(),
        _ => source.into(),
    };
    result
        .map(|(kind, value)| editor.make_expr(origin, kind, target, [("value", value)]))
        .transpose()
}

fn integer(
    editor: &CircuitEditor,
    n: &BigInt,
    target: TypeId,
    kind: Ty,
) -> Result<Option<(Expr, Field)>, Error> {
    let expr = match kind {
        Ty::Integer => {
            let ty = editor.node(target.into())?;
            let width = ty.data("width").and_then(|v| v.as_u64());
            let signed = ty.data("signed").and_then(|v| v.as_bool());
            let (Some(width), Some(signed)) = (width, signed) else {
                return Err(Error::new("integer type requires width and signedness"));
            };
            let literal = match (signed, width) {
                (true, 8) => Expr::I8Literal,
                (true, 16) => Expr::I16Literal,
                (true, 32) => Expr::I32Literal,
                (true, 64) => Expr::I64Literal,
                (true, 128) => Expr::I128Literal,
                (false, 8) => Expr::U8Literal,
                (false, 16) => Expr::U16Literal,
                (false, 32) => Expr::U32Literal,
                (false, 64) => Expr::U64Literal,
                (false, 128) => Expr::U128Literal,
                _ => return Err(Error::new("invalid integer width in column default")),
            };
            let limit: BigInt = BigInt::one() << (width - u64::from(signed));
            if n >= &limit
                || (signed && n < &-limit)
                || (!signed && n.sign() == num_bigint::Sign::Minus)
            {
                return Ok(None);
            }
            literal
        }
        Ty::Double => {
            return Ok(n
                .to_f64()
                .filter(|v| BigInt::from_f64(*v).as_ref() == Some(n))
                .map(|v| {
                    (
                        Expr::DoubleLiteral,
                        Field::Data((v.to_bits() as i64).into()),
                    )
                }))
        }
        Ty::Real => {
            return Ok(n
                .to_f32()
                .filter(|v| BigInt::from_f32(*v).as_ref() == Some(n))
                .map(|v| (Expr::RealLiteral, Field::Data((v.to_bits() as i32).into()))))
        }
        Ty::ISize if n.to_i64().is_some() => Expr::ISizeLiteral,
        Ty::USize if n.to_u64().is_some() => Expr::USizeLiteral,
        _ => return Ok(None),
    };
    Ok(Some((expr, Field::Data(n.to_string().into()))))
}

/// Calcite accepts paired braces and a separator after any group of four hex digits.
fn uuid_literal(s: &str) -> Option<uuid::Uuid> {
    let s = s
        .strip_prefix('{')
        .and_then(|v| v.strip_suffix('}'))
        .unwrap_or(s);
    let mut digits = String::with_capacity(32);
    let mut previous_separator = false;
    for ch in s.chars() {
        if ch == '-' {
            if previous_separator
                || digits.is_empty()
                || digits.len() == 32
                || !digits.len().is_multiple_of(4)
            {
                return None;
            }
            previous_separator = true;
        } else {
            if !ch.is_ascii_hexdigit() || digits.len() >= 32 {
                return None;
            }
            digits.push(ch);
            previous_separator = false;
        }
    }
    uuid::Uuid::parse_str(&digits).ok()
}

/// Java BigDecimal uses half-even for strings and truncation for existing decimals.
/// Bound intermediate powers of ten by the destination precision, including scientific input.
fn decimal(s: &str, precision: u64, scale: u64, round_even: bool) -> Option<String> {
    if precision == 0 || precision > 38 || scale > precision {
        return None;
    }
    let negative = s.starts_with('-');
    let s = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (mantissa, exponent) = match s.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (s, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all = format!("{whole}{fraction}");
    if all.is_empty() || !all.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits = all.trim_start_matches('0');
    let places = scale as i64 + i64::from(exponent) - i64::try_from(fraction.len()).ok()?;
    let magnitude = if digits.is_empty() {
        0
    } else if places >= 0 {
        if digits.len() as u64 + places as u64 > precision {
            return None;
        }
        digits.parse::<u128>().ok()? * 10u128.pow(places as u32)
    } else {
        let cut = digits.len() as i64 + places;
        if cut > precision as i64 {
            return None;
        }
        if cut < 0 {
            0
        } else {
            let (kept, discarded) = digits.split_at(cut as usize);
            let value = if kept.is_empty() {
                0
            } else {
                kept.parse::<u128>().ok()?
            };
            let (first, rest) = discarded.as_bytes().split_first()?;
            let up = *first > b'5'
                || (*first == b'5' && (value % 2 == 1 || rest.iter().any(|&b| b != b'0')));
            value + u128::from(round_even && up)
        }
    };
    let digits = magnitude.to_string();
    if digits.len() as u64 > precision {
        return None;
    }
    let sign = if negative && magnitude != 0 { "-" } else { "" };
    let exponent = digits.len() as i64 - 1 - scale as i64;
    let value = if scale == 0 {
        digits
    } else if exponent < -6 {
        let (first, tail) = digits.split_at(1);
        if tail.is_empty() {
            format!("{first}E{exponent}")
        } else {
            format!("{first}.{tail}E{exponent}")
        }
    } else if digits.len() > scale as usize {
        let (w, f) = digits.split_at(digits.len() - scale as usize);
        format!("{w}.{f}")
    } else {
        format!("0.{:0>width$}", digits, width = scale as usize)
    };
    Some(format!("{sign}{value}"))
}
