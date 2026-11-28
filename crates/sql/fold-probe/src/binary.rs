//! Bounded byte operations. Binary indices count bytes, not Unicode characters.

use crate::model::{Refusal, Result, SqlType, Value, unsupported};
use md5::{Digest, Md5};
use std::fmt::Write;

const MAX_BYTES: usize = 1024 * 1024;

pub(crate) fn parse(text: &str) -> Result<Vec<u8>> {
    if text.len() > MAX_BYTES * 2 {
        return Err(Refusal::Budget);
    }
    if !text.len().is_multiple_of(2) || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(unsupported("malformed binary literal"));
    }
    Ok(text
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digit = |b: u8| (b as char).to_digit(16).expect("validated hexadecimal") as u8;
            digit(pair[0]) * 16 + digit(pair[1])
        })
        .collect())
}

pub(crate) fn format(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(result, "{byte:02x}").expect("writing to String cannot fail");
    }
    result
}

pub(crate) fn cast(value: Value, source: &SqlType, ty: &SqlType) -> Result<Value> {
    if let Value::Int(value) = value {
        return cast_integer(value, source, ty);
    }
    let mut bytes = match value {
        Value::Binary(bytes) => bytes,
        Value::String(text) => text.into_bytes(),
        Value::Uuid(value) => value.to_be_bytes().to_vec(),
        _ => return Err(unsupported("binary cast source")),
    };
    if ty.precision >= 0 {
        let size = ty.precision as usize;
        if size > MAX_BYTES {
            return Err(Refusal::Budget);
        }
        bytes.truncate(size);
        if ty.name == "BINARY" {
            bytes.resize(size, 0);
        }
    }
    Ok(Value::Binary(bytes))
}

fn cast_integer(value: i128, source: &SqlType, target: &SqlType) -> Result<Value> {
    let width = match source.name.as_str() {
        "TINYINT" | "UTINYINT" => 1,
        "SMALLINT" | "USMALLINT" => 2,
        "INTEGER" | "UINTEGER" => 4,
        "BIGINT" | "UBIGINT" => 8,
        _ => return Err(unsupported("integer binary cast source type")),
    };
    crate::numeric::integer(value, source)?;
    let size = if target.precision < 0 {
        width
    } else {
        let precision = target.precision as usize;
        if precision > MAX_BYTES {
            return Err(Refusal::Budget);
        }
        if target.name == "BINARY" {
            precision
        } else {
            precision.min(width)
        }
    };
    // Integer casts retain the least significant bytes and pad on the left
    // with zeros, even for negative values. Source SQL width is significant.
    let retained = size.min(width);
    let mut bytes = vec![0; size];
    bytes[size - retained..].copy_from_slice(&value.to_be_bytes()[16 - retained..]);
    Ok(Value::Binary(bytes))
}

pub(crate) fn evaluate(name: &str, values: &[Value]) -> Result<Value> {
    match (name, values) {
        ("CONCAT" | "||", [Value::Binary(a), Value::Binary(b)]) => {
            let size = a
                .len()
                .checked_add(b.len())
                .filter(|n| *n <= MAX_BYTES)
                .ok_or(Refusal::Budget)?;
            let mut result = Vec::with_capacity(size);
            result.extend_from_slice(a);
            result.extend_from_slice(b);
            Ok(Value::Binary(result))
        }
        ("OCTET_LENGTH", [Value::Binary(bytes)]) => Ok(Value::Int(bytes.len() as i128)),
        ("TO_HEX", [Value::Binary(bytes)]) => Ok(Value::String(format(bytes))),
        ("MD5", [Value::Binary(bytes)]) => Ok(Value::String(format!("{:x}", Md5::digest(bytes)))),
        ("STARTS_WITH", [Value::Binary(bytes), Value::Binary(prefix)]) => {
            Ok(Value::Bool(bytes.starts_with(prefix)))
        }
        ("ENDS_WITH", [Value::Binary(bytes), Value::Binary(suffix)]) => {
            Ok(Value::Bool(bytes.ends_with(suffix)))
        }
        ("POSITION", [Value::Binary(needle), Value::Binary(haystack)]) => {
            if needle.is_empty() {
                return Ok(Value::Int(1));
            }
            if needle.len() > haystack.len() {
                return Ok(Value::Int(0));
            }
            // Bound worst-case byte comparisons, including repeated prefixes.
            if (haystack.len() - needle.len() + 1)
                .checked_mul(needle.len())
                .is_none_or(|work| work > MAX_BYTES)
            {
                return Err(Refusal::Budget);
            }
            Ok(Value::Int(
                haystack
                    .windows(needle.len())
                    .position(|bytes| bytes == needle)
                    .map_or(0, |index| index as i128 + 1),
            ))
        }
        (name @ ("SUBSTRING" | "LEFT" | "RIGHT"), [Value::Binary(s), Value::Int(n)]) => {
            let slice = match name {
                "LEFT" => crate::StringSlice::Left(*n),
                "RIGHT" => crate::StringSlice::Right(*n),
                _ => crate::StringSlice::Substring {
                    start: *n,
                    length: None,
                },
            };
            Ok(Value::Binary(
                slice
                    .bytes(s)
                    .expect("a two-argument slice has no length error"),
            ))
        }
        ("SUBSTRING", [Value::Binary(s), Value::Int(start), Value::Int(length)]) => {
            let slice = crate::StringSlice::Substring {
                start: *start,
                length: Some(*length),
            };
            slice.bytes(s).map(Value::Binary).ok_or_else(|| {
                unsupported("negative binary SUBSTRING length differs between Java and runtime")
            })
        }
        (
            "OVERLAY",
            [
                Value::Binary(source),
                Value::Binary(replacement),
                Value::Int(start),
            ],
        ) => overlay(source, replacement, *start, replacement.len() as i128),
        (
            "OVERLAY",
            [
                Value::Binary(source),
                Value::Binary(replacement),
                Value::Int(start),
                Value::Int(remove),
            ],
        ) => overlay(source, replacement, *start, *remove),
        _ => Err(unsupported(format!("binary function {name}"))),
    }
}

fn overlay(source: &[u8], replacement: &[u8], start: i128, remove: i128) -> Result<Value> {
    if start < 1 || remove < 0 || start - 1 + remove > source.len() as i128 {
        return Err(unsupported(
            "binary OVERLAY range differs between Java and runtime",
        ));
    }
    let start = (start - 1) as usize;
    let remove = remove as usize;
    let size = (source.len() - remove)
        .checked_add(replacement.len())
        .filter(|n| *n <= MAX_BYTES)
        .ok_or(Refusal::Budget)?;
    let mut result = Vec::with_capacity(size);
    result.extend_from_slice(&source[..start]);
    result.extend_from_slice(replacement);
    result.extend_from_slice(&source[start + remove..]);
    Ok(Value::Binary(result))
}
