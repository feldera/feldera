//! Rounding uses the same fixed-decimal operations as sqllib. Float scaling
//! follows sqllib::float::round_to_digits, including zero and overflow handling.

use crate::model::{Refusal, Result, SqlType, Value, unsupported};
use feldera_fxp::{DynamicDecimal, Fixed};

pub(crate) fn evaluate(name: &str, values: &[Value], ty: &SqlType) -> Result<Value> {
    let (value, digits) = match values {
        [value] => (value, 0),
        [value, Value::Int(digits)] if matches!(name, "ROUND" | "TRUNCATE" | "TRUNC") => (
            value,
            i32::try_from(*digits).map_err(|_| unsupported("rounding digits exceed i32"))?,
        ),
        _ => return Err(unsupported("numeric rounding arguments")),
    };
    match value {
        Value::Int(value) if digits >= 0 => crate::numeric::integer(*value, ty),
        // Java rounds integers at negative positions; the runtime compiler
        // currently treats integer ROUND/TRUNCATE as a no-op with a warning.
        Value::Int(_) => Err(unsupported("negative-position integer rounding semantics")),
        Value::Decimal(value) => {
            fn apply<const S: usize>(
                value: DynamicDecimal,
                name: &str,
                digits: i32,
            ) -> Result<DynamicDecimal> {
                let value = Fixed::<38, S>::new(value.significand(), value.exponent().into())
                    .ok_or_else(|| unsupported("rounding operand exceeds decimal envelope"))?;
                let rounded = match name {
                    "ROUND" => value.checked_round(digits),
                    "TRUNCATE" | "TRUNC" => Some(value.trunc_digits(digits)),
                    "FLOOR" => value.checked_floor(),
                    "CEIL" => value.checked_ceil(),
                    _ => return Err(unsupported("rounding operation")),
                };
                rounded
                    .map(Into::into)
                    .ok_or(Refusal::RuntimeError("decimal rounding overflow"))
            }
            macro_rules! scales {
                ($($scale:literal),*) => {
                    match value.exponent() {
                        $($scale => apply::<$scale>(*value, name, digits),)*
                        _ => Err(unsupported("decimal rounding scale above 38")),
                    }
                };
            }
            let result = scales!(
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38
            )?;
            crate::numeric::decimal(result, ty)
        }
        Value::Float(value) => {
            macro_rules! rounded {
                ($float:ty) => {{
                    let value = crate::numeric::canonical_float(*value) as $float;
                    match name {
                        "FLOOR" => value.floor(),
                        "CEIL" => value.ceil(),
                        "ROUND" | "TRUNCATE" | "TRUNC" => {
                            let tens = (10.0 as $float).powi(digits);
                            let scaled = value * tens;
                            let result = if scaled.is_finite() {
                                let rounded = if name == "ROUND" {
                                    scaled.round()
                                } else {
                                    scaled.trunc()
                                };
                                if rounded == 0.0 {
                                    rounded
                                } else {
                                    rounded / tens
                                }
                            } else {
                                value
                            };
                            if result == 0.0 { 0.0 } else { result }
                        }
                        _ => return Err(unsupported("rounding operation")),
                    }
                }};
            }
            Ok(crate::numeric::floating(
                if ty.name == "REAL" {
                    f64::from(rounded!(f32))
                } else {
                    rounded!(f64)
                },
                ty,
            ))
        }
        _ => Err(unsupported("numeric rounding operand")),
    }
}
