//! Declarative SEARCH arguments. Preserve disjoint ordered intervals so lowering
//! performs the same expansion as the SQL frontend's resolved expression tree.
use crate::{
    LiteralLimits, TypeId, ValidatedTypes, ValidationError,
    literals::{Literal, LiteralValue},
    source::Origin,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

type Result<T> = std::result::Result<T, ValidationError>;
fn error(message: &'static str) -> ValidationError {
    ValidationError {
        path: "search_argument".into(),
        message,
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UnknownAs {
    True,
    False,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRange {
    pub lower: SearchBound,
    pub upper: SearchBound,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchBound {
    Unbounded {},
    Bounded { closed: bool, literal: Literal },
}

impl SearchRange {
    pub(crate) fn validate_all(
        ranges: &[Self],
        ty: TypeId,
        types: &ValidatedTypes<'_>,
        limits: LiteralLimits,
    ) -> Result<()> {
        let mut previous: Option<&Self> = None;
        for range in ranges {
            for bound in [&range.lower, &range.upper] {
                if let SearchBound::Bounded { literal, .. } = bound {
                    if literal.ty != ty || !matches!(literal.origin, Origin::Absent {}) {
                        return Err(error("search endpoint type or origin differs"));
                    }
                    types.validate_literal(literal, limits)?;
                    // Also reject payloads with no defined SEARCH ordering.
                    compare(&literal.value, &literal.value)?;
                }
            }
            if let (
                SearchBound::Bounded {
                    closed: left,
                    literal: lower,
                },
                SearchBound::Bounded {
                    closed: right,
                    literal: upper,
                },
            ) = (&range.lower, &range.upper)
            {
                let order = compare(&lower.value, &upper.value)?;
                if order == Ordering::Greater || (order == Ordering::Equal && !(*left && *right)) {
                    return Err(error("empty or reversed search range"));
                }
            }
            if let Some(previous) = previous {
                match (&previous.upper, &range.lower) {
                    (
                        SearchBound::Bounded {
                            closed: left,
                            literal: upper,
                        },
                        SearchBound::Bounded {
                            closed: right,
                            literal: lower,
                        },
                    ) => {
                        let order = compare(&upper.value, &lower.value)?;
                        if order == Ordering::Greater
                            || (order == Ordering::Equal && (*left || *right))
                        {
                            return Err(error(
                                "search ranges overlap, connect, or are out of order",
                            ));
                        }
                    }
                    _ => return Err(error("unbounded search range is out of order")),
                }
            }
            previous = Some(range);
        }
        Ok(())
    }
}

fn compare(a: &LiteralValue, b: &LiteralValue) -> Result<Ordering> {
    use LiteralValue as V;
    Ok(match (a, b) {
        (V::Boolean { value: a }, V::Boolean { value: b }) => a.cmp(b),
        (V::Integer { value: a }, V::Integer { value: b }) => decimal_cmp(a, 0, b, 0),
        (V::Decimal { value: a }, V::Decimal { value: b }) => {
            decimal_cmp(&a.coefficient, a.scale, &b.coefficient, b.scale)
        }
        (V::Interval { value: a, unit: x }, V::Interval { value: b, unit: y }) if x == y => {
            decimal_cmp(&a.coefficient, a.scale, &b.coefficient, b.scale)
        }
        (V::Character { value: a }, V::Character { value: b }) => {
            a.encode_utf16().cmp(b.encode_utf16())
        }
        (V::Binary { value: a }, V::Binary { value: b }) => a.cmp(b),
        (V::Date { epoch_days: a }, V::Date { epoch_days: b }) => a.cmp(b),
        (
            V::Time {
                nanoseconds_since_midnight: a,
            },
            V::Time {
                nanoseconds_since_midnight: b,
            },
        ) => a.cmp(b),
        (
            V::Timestamp {
                epoch_days: a,
                nanoseconds_since_midnight: x,
            },
            V::Timestamp {
                epoch_days: b,
                nanoseconds_since_midnight: y,
            },
        ) => (a, x).cmp(&(b, y)),
        (V::Float32 { bits: a }, V::Float32 { bits: b }) => {
            float_cmp(f32::from_bits(*a) as f64, f32::from_bits(*b) as f64)
        }
        (V::Float64 { bits: a }, V::Float64 { bits: b }) => {
            float_cmp(f64::from_bits(*a), f64::from_bits(*b))
        }
        _ => return Err(error("unsupported search endpoint ordering")),
    })
}

// Java Double.compare canonicalizes NaNs but distinguishes signed zero.
fn float_cmp(a: f64, b: f64) -> Ordering {
    let canonical = |value: f64| if value.is_nan() { f64::NAN } else { value };
    canonical(a).total_cmp(&canonical(b))
}

// Compare decimal strings without allocation, floating-point conversion, or
// padding proportional to scale. Validated payloads contain only signed digits.
fn decimal_cmp(a: &str, scale_a: i32, b: &str, scale_b: i32) -> Ordering {
    fn parts(text: &str) -> (bool, &str) {
        let digits = text.trim_start_matches(['+', '-']).trim_start_matches('0');
        (text.starts_with('-') && !digits.is_empty(), digits)
    }
    let (negative_a, a) = parts(a);
    let (negative_b, b) = parts(b);
    if negative_a != negative_b {
        return negative_b.cmp(&negative_a);
    }
    let order = if a.is_empty() || b.is_empty() {
        a.len().cmp(&b.len())
    } else {
        let exponent_a = a.len() as i64 - i64::from(scale_a);
        let exponent_b = b.len() as i64 - i64::from(scale_b);
        exponent_a.cmp(&exponent_b).then_with(|| {
            a.bytes()
                .chain(std::iter::repeat(b'0'))
                .take(a.len().max(b.len()))
                .cmp(
                    b.bytes()
                        .chain(std::iter::repeat(b'0'))
                        .take(a.len().max(b.len())),
                )
        })
    };
    if negative_a { order.reverse() } else { order }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_order_preserves_sign_scale_and_large_coefficients() {
        for (a, sa, b, sb) in [
            ("1", 0, "10", 1),
            ("-0", -100, "+000", 200),
            (
                "-123456789012345678901234567890",
                20,
                "-1234567890123456789012345678900",
                21,
            ),
        ] {
            assert_eq!(decimal_cmp(a, sa, b, sb), Ordering::Equal);
        }
        for (a, sa, b, sb) in [
            ("-1", 0, "0", 0),
            ("1", 100, "1", 99),
            ("-11", 1, "-1", 0),
            ("999999999999999999999999999999", 0, "1", -30),
        ] {
            assert_eq!(decimal_cmp(a, sa, b, sb), Ordering::Less);
            assert_eq!(decimal_cmp(b, sb, a, sa), Ordering::Greater);
        }
    }

    #[test]
    fn endpoint_order_matches_java_utf16_nan_and_signed_zero() {
        assert_eq!(
            compare(
                &LiteralValue::Character {
                    value: "\u{10000}".into()
                },
                &LiteralValue::Character {
                    value: "\u{e000}".into()
                }
            )
            .unwrap(),
            Ordering::Less
        );
        assert_eq!(float_cmp(-0.0, 0.0), Ordering::Less);
        assert_eq!(
            float_cmp(f64::from_bits(0xfff8000000000001), f64::NAN),
            Ordering::Equal
        );
        assert_eq!(float_cmp(f64::INFINITY, f64::NAN), Ordering::Less);
    }
}
