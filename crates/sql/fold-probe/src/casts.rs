//! Scalar conversions shared by explicit casts and typed collection results.

use crate::model::{Refusal, Result, SqlType, Value, unsupported};
use crate::numeric;
use feldera_fxp::DynamicDecimal;

const MAX_STRING_BYTES: usize = 1024 * 1024;

pub(crate) fn cast(
    value: Value,
    source: &SqlType,
    ty: &SqlType,
    profile: crate::SemanticsProfile,
) -> Result<Value> {
    if value == Value::Null || (source == ty && matches!(value, Value::Array(_))) {
        return Ok(value);
    }
    match ty.name.as_str() {
        "ARRAY" => match value {
            // No element conversions are needed. Nonempty arrays require the
            // component types that the current fold wire format omits.
            Value::Array(values) if values.is_empty() => Ok(Value::Array(values)),
            _ => Err(unsupported("nonempty array cast requires component types")),
        },
        "BINARY" | "VARBINARY" => crate::binary::cast(value, source, ty),
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "UTINYINT" | "USMALLINT" | "UINTEGER"
        | "UBIGINT" => {
            let n = match value {
                Value::Int(v) => v,
                Value::Bool(v) => i128::from(v),
                Value::Decimal(_) if ty.name.starts_with('U') => {
                    return Err(unsupported("decimal to unsigned integer cast"));
                }
                Value::Decimal(v) => v.into(),
                Value::String(v) if ty.name.starts_with('U') => {
                    // Rust's unsigned parser refuses even '-0'; parsing through
                    // i128 and then checking the range would incorrectly accept it.
                    i128::from(
                        v.trim_matches(|c: char| c <= ' ')
                            .parse::<u64>()
                            .map_err(|_| Refusal::RuntimeError("unsigned integer cast"))?,
                    )
                }
                Value::String(v) => v
                    .trim_matches(|c: char| c <= ' ')
                    .parse()
                    .map_err(|_| Refusal::RuntimeError("integer cast"))?,
                Value::Float(_) => return Err(unsupported("float to integer cast")),
                _ => return Err(unsupported("integer cast source")),
            };
            numeric::integer(n, ty)
        }
        "DECIMAL" => {
            let decimal = match value {
                Value::Int(v) => DynamicDecimal::new(v, 0),
                Value::Decimal(v) => v,
                Value::String(_) => return Err(unsupported("string decimal parser rounding")),
                _ => return Err(unsupported("decimal cast source")),
            };
            numeric::decimal(decimal, ty)
        }
        "REAL" | "FLOAT" | "DOUBLE" => {
            let number = match value {
                Value::Float(v) => v,
                Value::Int(v) => v as f64,
                Value::Decimal(v) => f64::from(v),
                Value::String(v) => {
                    let text = v.trim_matches(|c: char| c <= ' ');
                    if profile == crate::SemanticsProfile::LegacyJanino {
                        crate::legacy::check_float_spelling(text)?;
                    }
                    text.parse()
                        .map_err(|_| Refusal::RuntimeError("float cast"))?
                }
                Value::Bool(v) => {
                    if v {
                        1.0
                    } else {
                        0.0
                    }
                }
                _ => return Err(unsupported("float cast source")),
            };
            Ok(numeric::floating(number, ty))
        }
        "BOOLEAN" => match value {
            Value::Bool(_) => Ok(value),
            Value::Int(v) => Ok(Value::Bool(v != 0)),
            Value::String(v) => match v
                .trim_matches(|c: char| c <= ' ')
                .to_ascii_lowercase()
                .as_str()
            {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => Err(Refusal::RuntimeError("boolean cast")),
            },
            _ => Err(unsupported("boolean cast source")),
        },
        "CHAR" | "VARCHAR" => {
            if ty.precision > MAX_STRING_BYTES as i32 {
                return Err(Refusal::Budget);
            }
            let text = match value {
                Value::String(v) => v,
                Value::Uuid(v) => crate::uuid::format(v),
                Value::Int(v) => v.to_string(),
                Value::Decimal(v) => v.to_string(),
                Value::Date(v) => {
                    use chrono::Datelike;
                    // Date's runtime Display uses a minimum four-character
                    // year, without chrono's extended-year sign convention.
                    format!("{:04}-{:02}-{:02}", v.year(), v.month(), v.day())
                }
                Value::Bool(v) => if v { "TRUE" } else { "FALSE" }.into(),
                _ => return Err(unsupported("string cast source")),
            };
            let mut text: String = if ty.precision >= 0 {
                text.chars().take(ty.precision as usize).collect()
            } else {
                text
            };
            if ty.name == "CHAR" && ty.precision >= 0 {
                text.extend(std::iter::repeat_n(
                    ' ',
                    (ty.precision as usize).saturating_sub(text.chars().count()),
                ));
            }
            Ok(Value::String(text))
        }
        "DATE" => match value {
            Value::Date(_) => Ok(value),
            Value::String(v) => crate::temporal::parse_date(&v).map(Value::Date),
            Value::Timestamp(v) => Ok(Value::Date(v.date())),
            _ => Err(unsupported("date cast source")),
        },
        "TIME" => match value {
            Value::Time(_) => Ok(value),
            Value::String(v) => crate::temporal::parse_time(&v).map(Value::Time),
            Value::Timestamp(v) => Ok(Value::Time(v.time())),
            _ => Err(unsupported("time cast source")),
        },
        "TIMESTAMP" => match value {
            Value::Timestamp(_) => Ok(value),
            Value::String(v) => {
                if profile == crate::SemanticsProfile::LegacyJanino {
                    crate::legacy::check_timestamp_spelling(&v)?;
                }
                crate::temporal::parse_timestamp(&v).map(Value::Timestamp)
            }
            Value::Date(v) => Ok(Value::Timestamp(v.and_hms_opt(0, 0, 0).unwrap())),
            _ => Err(unsupported("timestamp cast source")),
        },
        "UUID" => match value {
            Value::Uuid(_) => Ok(value),
            Value::String(v) => crate::uuid::parse(&v).map(Value::Uuid),
            Value::Binary(v) => {
                let bytes: [u8; 16] = v
                    .try_into()
                    .map_err(|_| Refusal::RuntimeError("UUID requires exactly 16 bytes"))?;
                Ok(Value::Uuid(u128::from_be_bytes(bytes)))
            }
            _ => Err(unsupported("UUID cast source")),
        },
        name if name.starts_with("INTERVAL_") => crate::temporal::cast_interval(value, ty),
        _ => Err(unsupported(format!("cast target {}", ty.name))),
    }
}
