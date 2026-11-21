use crate::model::{Refusal, Result, SqlType, Value, unsupported};
use feldera_fxp::{DynamicDecimal, Fixed};

/// Match F32/F64 construction at runtime operation boundaries. Literal wire
/// values remain untouched so the Java journal's original bits are observable.
pub(crate) fn canonical_float(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    }
}

pub(crate) fn floating(value: f64, ty: &SqlType) -> Value {
    Value::Float(canonical_float(if ty.name == "REAL" {
        f64::from(value as f32)
    } else {
        value
    }))
}

pub(crate) fn integer(value: i128, ty: &SqlType) -> Result<Value> {
    let valid = match ty.name.as_str() {
        "TINYINT" => i8::try_from(value).is_ok(),
        "SMALLINT" => i16::try_from(value).is_ok(),
        "INTEGER" => i32::try_from(value).is_ok(),
        "BIGINT" => i64::try_from(value).is_ok(),
        "UTINYINT" => u8::try_from(value).is_ok(),
        "USMALLINT" => u16::try_from(value).is_ok(),
        "UINTEGER" => u32::try_from(value).is_ok(),
        "UBIGINT" => u64::try_from(value).is_ok(),
        _ => return Err(unsupported(format!("integer result type {}", ty.name))),
    };
    if valid {
        Ok(Value::Int(value))
    } else {
        Err(Refusal::RuntimeError("integer overflow"))
    }
}

pub(crate) fn decimal(value: DynamicDecimal, ty: &SqlType) -> Result<Value> {
    fn convert<const S: usize>(value: DynamicDecimal) -> Option<DynamicDecimal> {
        // Decimal-to-decimal casts in sqllib use Fixed::convert, which truncates.
        Fixed::<38, S>::new(value.significand(), i32::from(value.exponent())).map(Into::into)
    }
    macro_rules! scales {
        ($($scale:literal),*) => { match ty.scale { $($scale => convert::<$scale>(value),)* _ => None } };
    }
    if !(1..=38).contains(&ty.precision) || !(0..=ty.precision).contains(&ty.scale) {
        return Err(unsupported("decimal precision/scale"));
    }
    let value = scales!(
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38
    )
    .ok_or(Refusal::RuntimeError("decimal overflow"))?;
    let padding = (ty.scale - i32::from(value.exponent())) as u32;
    let scaled = value.significand().checked_mul(10i128.pow(padding));
    if scaled.is_none_or(|v| v.unsigned_abs() >= 10u128.pow(ty.precision as u32)) {
        return Err(Refusal::RuntimeError("decimal overflow"));
    }
    Ok(Value::Decimal(value))
}

pub(crate) fn arithmetic(op: &str, left: Value, right: Value, ty: &SqlType) -> Result<Value> {
    // Calcite can retain integer literals beside decimals after inferring a
    // decimal result. Promote those literals exactly, before runtime arithmetic.
    let decimal_operand = |value| match value {
        Value::Int(n) if ty.name == "DECIMAL" => Value::Decimal(DynamicDecimal::new(n, 0)),
        value => value,
    };
    let (left, right) = (decimal_operand(left), decimal_operand(right));
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => {
            let value = match op {
                "+" => a.checked_add(b),
                "-" => a.checked_sub(b),
                "*" => a.checked_mul(b),
                "/" => a.checked_div(b),
                "%" if b == -1 => Some(0),
                "%" => a.checked_rem(b),
                _ => return Err(unsupported(op)),
            }
            .ok_or(Refusal::RuntimeError("integer arithmetic"))?;
            integer(value, ty)
        }
        (Value::Float(a), Value::Float(b)) => {
            let a = canonical_float(a);
            let b = canonical_float(b);
            let value = match op {
                "+" => a + b,
                "-" => a - b,
                "*" => a * b,
                "/" => a / b,
                "%" => a % b,
                _ => return Err(unsupported(op)),
            };
            Ok(floating(value, ty))
        }
        (Value::Decimal(a), Value::Decimal(b)) => {
            // A bounded adaptation of the actual runtime Fixed arithmetic. Values
            // that would lose digits on entry are refused, never rounded silently.
            if a.exponent() > 18 || b.exponent() > 18 || ty.scale > 18 {
                return Err(unsupported("decimal arithmetic scale above 18"));
            }
            let a = Fixed::<38, 18>::new(a.significand(), a.exponent().into())
                .ok_or_else(|| unsupported("decimal operand exceeds prototype envelope"))?;
            let b = Fixed::<38, 18>::new(b.significand(), b.exponent().into())
                .ok_or_else(|| unsupported("decimal operand exceeds prototype envelope"))?;
            if matches!(op, "/" | "%") && b == Fixed::<38, 18>::ZERO {
                return Err(Refusal::RuntimeError("decimal division or modulo by zero"));
            }
            let value: Option<Fixed<38, 18>> = match op {
                "+" => a.checked_add_generic(b),
                "-" => a.checked_sub_generic(b),
                "*" => a.checked_mul_generic(b),
                "/" => a.checked_div_generic(b),
                "%" => a.checked_rem(b),
                _ => return Err(unsupported(op)),
            };
            let value = value.ok_or_else(|| {
                unsupported("decimal intermediate exceeds prototype envelope or divides by zero")
            })?;
            decimal(value.into(), ty)
        }
        _ => Err(unsupported("mixed arithmetic operands")),
    }
}
