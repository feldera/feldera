//! Typed literal payloads for the draft relational boundary.
//!
//! Precision belongs to the referenced SQL type. Payloads retain exact bits,
//! decimal coefficients, collection ordering and nested type identities. These
//! definitions still need projection tests against Calcite before S1 adoption.

use crate::{TypeId, source::Origin};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Calcite DATE literals use four unsigned year digits and the proleptic
/// Gregorian calendar. Its DATE range starts at 0001-01-01.
pub fn date_from_text(text: &str) -> Option<i32> {
    use chrono::Datelike;
    if text.len() != 10 {
        return None;
    }
    let date: chrono::NaiveDate = text.parse().ok()?;
    (date.year() > 0 && date.to_string() == text).then(|| date.to_epoch_days())
}

pub fn date_to_text(days: i32) -> Option<String> {
    use chrono::Datelike;
    let date = chrono::NaiveDate::from_epoch_days(days)?;
    (1..=9999).contains(&date.year()).then(|| date.to_string())
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Literal {
    pub ty: TypeId,
    pub origin: Origin,
    pub value: LiteralValue,
}

/// Integer text is a signed base-ten coefficient, without exponent notation.
/// Scale is explicit and may be negative; decoding never routes through f64.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExactDecimal {
    pub coefficient: String,
    pub scale: i32,
}

/// The VALUES common type rounds ties away from zero. Destination casts
/// discard excess fractional digits.
#[derive(Clone, Copy)]
pub enum DecimalRounding {
    HalfUp,
    Truncate,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LiteralValue {
    Null {},
    Boolean {
        value: bool,
    },
    Integer {
        value: String,
    },
    Decimal {
        value: ExactDecimal,
    },
    Float32 {
        bits: u32,
    },
    Float64 {
        bits: u64,
    },
    Character {
        value: String,
    },
    Binary {
        value: Vec<u8>,
    },
    Date {
        epoch_days: i32,
    },
    Time {
        nanoseconds_since_midnight: u64,
    },
    /// Local calendar fields; no implicit timezone conversion occurs here.
    Timestamp {
        epoch_days: i32,
        nanoseconds_since_midnight: u64,
    },
    /// The instant and timezone presentation remain distinct.
    TimestampWithZone {
        epoch_seconds: i64,
        nanoseconds: u32,
        zone: String,
    },
    /// Preserve a local calendar value and its zone label before resolving DST
    /// gaps or overlaps. This differs from an already resolved instant.
    LocalTimestampWithZone {
        epoch_days: i32,
        nanoseconds_since_midnight: u64,
        zone: String,
    },
    /// Calcite stores year/month intervals as months and day/time intervals
    /// as milliseconds. Preserve its exact decimal payload without rounding.
    Interval {
        value: ExactDecimal,
        unit: IntervalStorageUnit,
    },
    /// JTS Point subset used by current Java lowering. Ordinates are raw IEEE
    /// bits in coordinate-sequence order; empty points have no ordinates.
    GeometryPoint {
        srid: i32,
        dimension: u8,
        measures: u8,
        ordinates: Vec<u64>,
    },
    Uuid {
        bytes: [u8; 16],
    },
    Array {
        elements: Vec<Literal>,
    },
    /// Rows retain each field's own type, including typed NULLs.
    Row {
        fields: Vec<Literal>,
    },
    /// Preserve entry ordering and duplicates until the adapter proves its
    /// map semantics; deserialization must not collapse keys into a HashMap.
    Map {
        entries: Vec<MapEntry>,
    },
    Multiset {
        entries: Vec<MultisetEntry>,
    },
    Symbol {
        domain: String,
        name: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntervalStorageUnit {
    Months,
    Milliseconds,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MapEntry {
    pub key: Literal,
    pub value: Literal,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MultisetEntry {
    pub value: Literal,
    pub multiplicity: String,
}

impl ExactDecimal {
    /// Validate the coefficient spelling, preserving leading zeros and scale.
    pub fn has_valid_coefficient(&self) -> bool {
        valid_integer(&self.coefficient)
    }

    /// Convert between fixed scales in the compiler's 38-digit decimal range.
    pub fn at_sql_scale(&self, scale: u32, rounding: DecimalRounding) -> Option<Self> {
        if !(0..=38).contains(&self.scale) || scale > 38 {
            return None;
        }
        let n: i128 = self.coefficient.parse().ok()?;
        let change = scale as i32 - self.scale;
        let factor = 10_i128.pow(change.unsigned_abs());
        let result = if change >= 0 {
            n.checked_mul(factor)?
        } else {
            let integral = n / factor;
            if matches!(rounding, DecimalRounding::HalfUp) && (n % factor).abs() >= factor / 2 {
                integral + n.signum()
            } else {
                integral
            }
        };
        Some(Self {
            coefficient: result.to_string(),
            scale: scale as i32,
        })
    }

    /// The SQL writer keeps fractional zeros. Java's BigDecimal uses
    /// scientific notation for the same value when its exponent is below -6.
    pub fn sql_and_digest(&self) -> Option<(String, String)> {
        let scale = usize::try_from(self.scale).ok().filter(|&s| s <= 38)?;
        let coefficient: i128 = self.coefficient.parse().ok()?;
        let sign = if coefficient < 0 { "-" } else { "" };
        let digits = coefficient.unsigned_abs().to_string();
        let exponent = digits.len() as i32 - self.scale - 1;
        let plain = if scale == 0 {
            format!("{sign}{digits}")
        } else if digits.len() > scale {
            let (whole, fraction) = digits.split_at(digits.len() - scale);
            format!("{sign}{whole}.{fraction}")
        } else {
            format!("{sign}0.{digits:0>scale$}")
        };
        let digest = if exponent < -6 {
            let (first, rest) = digits.split_at(1);
            let point = if rest.is_empty() { "" } else { "." };
            format!("{sign}{first}{point}{rest}E{exponent}")
        } else {
            plain.clone()
        };
        Some((plain, digest))
    }
}

impl LiteralValue {
    pub fn as_exact_decimal(&self) -> Option<ExactDecimal> {
        match self {
            Self::Integer { value } => Some(ExactDecimal {
                coefficient: value.clone(),
                scale: 0,
            }),
            Self::Decimal { value } => Some(value.clone()),
            _ => None,
        }
    }
}

pub(crate) fn valid_integer(value: &str) -> bool {
    let digits = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn literal(value: LiteralValue) -> Literal {
        Literal {
            ty: TypeId(0),
            origin: Origin::Absent {},
            value,
        }
    }

    #[test]
    fn roundtrip_preserves_float_payloads_signed_zero_and_exact_decimal_scale() {
        for value in [
            LiteralValue::Float32 { bits: 0x8000_0000 },
            LiteralValue::Float32 { bits: 0x7fc0_0042 },
            LiteralValue::Float64 {
                bits: 0x8000_0000_0000_0000,
            },
            LiteralValue::Float64 {
                bits: 0x7ff8_0000_0000_0042,
            },
            LiteralValue::Integer {
                value: "18446744073709551615".into(),
            },
            LiteralValue::Decimal {
                value: ExactDecimal {
                    coefficient: "1234567890123456789012345678901234567890".into(),
                    scale: -9,
                },
            },
        ] {
            let value = literal(value);
            let decoded: Literal =
                serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn maps_keep_duplicate_entries_and_nested_null_type_identity() {
        let mut null = literal(LiteralValue::Null {});
        null.ty = TypeId(3);
        let entry = MapEntry {
            key: literal(LiteralValue::Integer { value: "1".into() }),
            value: null,
        };
        let value = literal(LiteralValue::Map {
            entries: vec![entry.clone(), entry],
        });
        let encoded = serde_json::to_string(&value).unwrap();
        let decoded: Literal = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, value);
        let LiteralValue::Map { entries } = decoded.value else {
            panic!()
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].value.ty, TypeId(3));
    }

    #[test]
    fn wire_format_refuses_unknown_fields_even_on_null() {
        for input in [
            r#"{"kind":"null","value":0}"#,
            r#"{"kind":"decimal","value":{"coefficient":"1","scale":0,"precision":1}}"#,
            r#"{"kind":"float64","bits":-1}"#,
            r#"{"kind":"float64","bits":1.5}"#,
            r#"{"kind":"geometry","value":"opaque"}"#,
        ] {
            assert!(
                serde_json::from_str::<LiteralValue>(input).is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn coefficients_are_decimal_digits_without_exponents_or_empty_signs() {
        for value in ["0", "-0", "+0", "001", "-123456789012345678901234567890"] {
            assert!(valid_integer(value), "{value}");
        }
        for value in ["", "-", "+", "1e3", "1.0", " 1", "١", "--1"] {
            assert!(!valid_integer(value), "{value}");
        }
    }
}
