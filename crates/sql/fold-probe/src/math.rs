//! Pure floating-point kernels follow sqllib::float. Exact Java and target
//! agreement is measured separately; libm results are not assumed portable.

use crate::model::{Refusal, Result, Value, unsupported};

pub(crate) fn registered(name: &str) -> bool {
    matches!(
        name,
        "SIN"
            | "COS"
            | "TAN"
            | "COT"
            | "COTH"
            | "CSCH"
            | "SEC"
            | "CSC"
            | "ASIN"
            | "ACOS"
            | "ATAN"
            | "ATAN2"
            | "LN"
            | "LOG"
            | "LOG10"
            | "EXP"
            | "POWER"
            | "DEGREES"
            | "RADIANS"
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
                "SIN" => n.sin(),
                "COS" => n.cos(),
                "TAN" if profile == crate::SemanticsProfile::LegacyJanino => libm::tan(n),
                "TAN" => n.tan(),
                "COT" => 1.0 / n.tan(),
                "COTH" => 1.0 / n.tanh(),
                "CSCH" => 1.0 / n.sinh(),
                "SEC" => 1.0 / n.cos(),
                "CSC" => 1.0 / n.sin(),
                "ASIN" => n.asin(),
                "ACOS" => n.acos(),
                "ATAN" => n.atan(),
                "EXP" => match profile {
                    crate::SemanticsProfile::Runtime => n.exp(),
                    crate::SemanticsProfile::LegacyExpLog
                    | crate::SemanticsProfile::LegacyJanino => libm::exp(n),
                },
                "DEGREES" => n.to_degrees(),
                "RADIANS" => n.to_radians(),
                "LN" | "LOG" | "LOG10" => {
                    if n.is_sign_negative() {
                        return Err(Refusal::RuntimeError("negative logarithm argument"));
                    }
                    if name == "LOG10" { n.log10() } else { n.ln() }
                }
                _ => return Err(unsupported("unary floating-point function")),
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
