//! Pure floating-point kernels follow sqllib::float. Exact Java and target
//! agreement is measured separately; libm results are not assumed portable.

use crate::model::{Refusal, Result, Value, unsupported};

/// Unary trigonometric kernels shared by the fold evaluator and SQL binder.
pub fn trigonometric_kernel(
    name: &str,
    profile: crate::SemanticsProfile,
) -> Option<fn(f64) -> f64> {
    let legacy = profile == crate::SemanticsProfile::LegacyJanino;
    Some(match name {
        "SIN" if legacy => crate::legacy::trigonometry::sin,
        "COS" if legacy => crate::legacy::trigonometry::cos,
        "SIN" => f64::sin,
        "COS" => f64::cos,
        "TAN" if legacy => libm::tan,
        "TAN" => f64::tan,
        "COT" if legacy => |n| 1.0 / libm::tan(n),
        "COT" => |n| 1.0 / n.tan(),
        "ASIN" if legacy => libm::asin,
        "ACOS" if legacy => libm::acos,
        "ATAN" if legacy => libm::atan,
        "ASIN" => f64::asin,
        "ACOS" => f64::acos,
        "ATAN" => f64::atan,
        "DEGREES" => f64::to_degrees,
        "RADIANS" => f64::to_radians,
        _ => return None,
    })
}

pub(crate) fn registered(name: &str) -> bool {
    trigonometric_kernel(name, crate::SemanticsProfile::Runtime).is_some()
        || matches!(
            name,
            "COTH"
                | "CSCH"
                | "SEC"
                | "CSC"
                | "ATAN2"
                | "LN"
                | "LOG"
                | "LOG10"
                | "EXP"
                | "POWER"
                | "IS_NAN"
                | "IS_INF"
        )
}

fn number(value: &Value) -> Result<f64> {
    match value {
        Value::Int(n) => Ok(*n as f64),
        Value::Float(n) => Ok(crate::numeric::canonical_float(*n)),
        Value::Decimal(n) => Ok(f64::from(*n)),
        _ => Err(unsupported("floating-point function operand")),
    }
}

pub(crate) fn evaluate(
    name: &str,
    values: &[Value],
    profile: crate::SemanticsProfile,
) -> Result<Value> {
    let result = match values {
        [value] => {
            let n = number(value)?;
            match name {
                "IS_NAN" => return Ok(Value::Bool(n.is_nan())),
                "IS_INF" => return Ok(Value::Bool(n.is_infinite())),
                "COTH" => 1.0 / n.tanh(),
                "CSCH" => 1.0 / n.sinh(),
                "SEC" => 1.0 / n.cos(),
                "CSC" => 1.0 / n.sin(),
                "EXP" => match profile {
                    crate::SemanticsProfile::Runtime => n.exp(),
                    crate::SemanticsProfile::LegacyExpLog
                    | crate::SemanticsProfile::LegacyJanino => libm::exp(n),
                },
                "LN" | "LOG" | "LOG10" => {
                    if n.is_sign_negative() {
                        return Err(Refusal::RuntimeError("negative logarithm argument"));
                    }
                    if name == "LOG10" { n.log10() } else { n.ln() }
                }
                _ => {
                    let kernel = trigonometric_kernel(name, profile)
                        .ok_or_else(|| unsupported("unary floating-point function"))?;
                    kernel(n)
                }
            }
        }
        [left, right] => {
            let a = number(left)?;
            let b = number(right)?;
            match name {
                "ATAN2" => a.atan2(b),
                "POWER" => match right {
                    // The compiler selects sqllib's integer exponent overload.
                    Value::Int(n) => a.powi(
                        i32::try_from(*n).map_err(|_| unsupported("POWER exponent exceeds i32"))?,
                    ),
                    _ if b.is_nan() => b,
                    _ => a.powf(b),
                },
                "LOG" => {
                    if profile == crate::SemanticsProfile::LegacyJanino && (a <= 0.0 || b <= 0.0) {
                        return Err(Refusal::RuntimeError(
                            "nonpositive legacy logarithm argument",
                        ));
                    }
                    if a.is_sign_negative() || b.is_sign_negative() {
                        return Err(Refusal::RuntimeError("negative logarithm argument"));
                    }
                    if b == 0.0 {
                        0.0
                    } else {
                        match profile {
                            crate::SemanticsProfile::Runtime => a.log(b),
                            crate::SemanticsProfile::LegacyExpLog
                            | crate::SemanticsProfile::LegacyJanino => libm::log(a) / libm::log(b),
                        }
                    }
                }
                _ => return Err(unsupported("binary floating-point function")),
            }
        }
        _ => return Err(unsupported("floating-point function arity")),
    };
    Ok(Value::Float(crate::numeric::canonical_float(result)))
}
