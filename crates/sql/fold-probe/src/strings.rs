//! Bounded string folds; runtime functions are in sqllib/src/string.rs.

use crate::model::{Refusal, Result, Value, unsupported};
use md5::{Digest, Md5};

const MAX_STRING_BYTES: usize = 1024 * 1024;

pub(crate) fn evaluate(
    name: &str,
    values: &[Value],
    profile: crate::SemanticsProfile,
) -> Result<Value> {
    // Case conversion can expand a Unicode character to three characters.
    // Refuse before allocating; a small result limit also bounds nested calls.
    let bytes: usize = values
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.len()),
            _ => None,
        })
        .sum();
    if bytes > MAX_STRING_BYTES / 3 {
        return Err(Refusal::Budget);
    }
    // Calcite indexes UTF-16 code units, while the runtime indexes Unicode scalar
    // values. Leave index-sensitive supplementary-character calls for runtime
    // until the frontend semantics discrepancy has an approved disposition.
    if matches!(
        name,
        "SUBSTRING" | "LEFT" | "RIGHT" | "POSITION" | "ASCII" | "OVERLAY"
    ) && values
        .iter()
        .any(|v| matches!(v, Value::String(s) if s.chars().any(|c| c.len_utf16() == 2)))
    {
        return Err(unsupported("UTF-16 versus Unicode scalar indexing"));
    }
    match (name, values) {
        ("CONCAT_WS", [Value::Null, ..]) => Ok(Value::Null),
        ("CONCAT_WS", [Value::String(separator), parts @ ..]) => {
            let parts = parts
                .iter()
                .filter(|v| **v != Value::Null)
                .map(|v| match v {
                    Value::String(s) => Ok(s.as_str()),
                    _ => Err(unsupported("CONCAT_WS operand")),
                })
                .collect::<Result<Vec<_>>>()?;
            if separator
                .len()
                .checked_mul(parts.len().saturating_sub(1))
                .and_then(|n| n.checked_add(bytes))
                .is_none_or(|n| n > MAX_STRING_BYTES)
            {
                return Err(Refusal::Budget);
            }
            Ok(Value::String(parts.join(separator)))
        }
        ("SPLIT", [Value::String(text)]) => split_with_profile(text, ",", profile),
        ("SPLIT", [Value::String(text), Value::String(separator)]) => {
            split_with_profile(text, separator, profile)
        }
        ("PI", []) => Ok(Value::Float(std::f64::consts::PI)),
        ("MD5", [Value::String(s)]) => {
            Ok(Value::String(format!("{:x}", Md5::digest(s.as_bytes()))))
        }
        ("CHAR_LENGTH" | "CHARACTER_LENGTH", [Value::String(v)]) => {
            Ok(Value::Int(v.chars().count() as i128))
        }
        ("UPPER", [Value::String(v)]) => Ok(Value::String(v.to_uppercase())),
        ("LOWER", [Value::String(v)]) => Ok(Value::String(v.to_lowercase())),
        ("INITCAP", [Value::String(text)]) => {
            // Java treats only ASCII letters/digits as word characters; the
            // runtime uses Unicode. Fold their common domain explicitly.
            if !text.is_ascii() {
                return Err(unsupported("INITCAP Unicode word boundaries"));
            }
            let mut start = true;
            let mut result = String::with_capacity(text.len());
            for c in text.chars() {
                result.push(if start {
                    c.to_ascii_uppercase()
                } else {
                    c.to_ascii_lowercase()
                });
                start = !c.is_ascii_alphanumeric();
            }
            Ok(Value::String(result))
        }
        ("CONCAT", parts) if parts.len() >= 2 => {
            let mut result = String::with_capacity(bytes);
            for part in parts {
                let Value::String(text) = part else {
                    return Err(unsupported("CONCAT operand"));
                };
                result.push_str(text);
            }
            Ok(Value::String(result))
        }
        (
            "OVERLAY",
            [
                Value::String(source),
                Value::String(replacement),
                Value::Int(start),
            ],
        ) => overlay(
            source,
            replacement,
            *start,
            replacement.chars().count() as i128,
        ),
        (
            "OVERLAY",
            [
                Value::String(source),
                Value::String(replacement),
                Value::Int(start),
                Value::Int(remove),
            ],
        ) => overlay(source, replacement, *start, *remove),
        ("STARTS_WITH", [Value::String(a), Value::String(b)]) => Ok(Value::Bool(a.starts_with(b))),
        ("ENDS_WITH", [Value::String(a), Value::String(b)]) => Ok(Value::Bool(a.ends_with(b))),
        ("SUBSTRING", [Value::String(s), Value::Int(start)]) => {
            Ok(Value::String(s.chars().skip(index(*start - 1)).collect()))
        }
        ("SUBSTRING", [Value::String(s), Value::Int(start), Value::Int(count)]) => {
            if *count < 0 {
                return Err(unsupported(
                    "negative SUBSTRING length differs between Java and runtime",
                ));
            }
            let count = if *start < 1 {
                (*count + *start - 1).max(0)
            } else {
                *count
            };
            Ok(Value::String(
                s.chars()
                    .skip(index(*start - 1))
                    .take(index(count))
                    .collect(),
            ))
        }
        ("LEFT", [Value::String(s), Value::Int(count)]) => {
            Ok(Value::String(s.chars().take(index(*count)).collect()))
        }
        ("RIGHT", [Value::String(s), Value::Int(count)]) => {
            let start = s.chars().count().saturating_sub(index(*count));
            Ok(Value::String(s.chars().skip(start).collect()))
        }
        ("POSITION", [Value::String(needle), Value::String(haystack)]) => Ok(Value::Int(
            haystack
                .find(needle)
                .map_or(0, |p| haystack[..p].chars().count() as i128 + 1),
        )),
        ("REVERSE", [Value::String(s)]) => Ok(Value::String(s.chars().rev().collect())),
        ("OCTET_LENGTH", [Value::String(s)]) => Ok(Value::Int(s.len() as i128)),
        ("ASCII", [Value::String(s)]) => Ok(Value::Int(s.chars().next().map_or(0, |c| c as i128))),
        ("REPEAT", [Value::String(s), Value::Int(count)]) => {
            if s.is_empty() {
                return Ok(Value::String(String::new()));
            }
            let count = index(*count);
            if s.len()
                .checked_mul(count)
                .is_none_or(|size| size > MAX_STRING_BYTES)
            {
                return Err(Refusal::Budget);
            }
            Ok(Value::String(s.repeat(count)))
        }
        (
            "REPLACE",
            [
                Value::String(s),
                Value::String(needle),
                Value::String(replacement),
            ],
        ) => {
            if needle.is_empty() {
                return Ok(Value::String(s.clone()));
            }
            let matches = s.matches(needle).count();
            let base = s.len() - matches * needle.len();
            if replacement
                .len()
                .checked_mul(matches)
                .and_then(|n| n.checked_add(base))
                .is_none_or(|size| size > MAX_STRING_BYTES)
            {
                return Err(Refusal::Budget);
            }
            Ok(Value::String(s.replace(needle, replacement)))
        }
        _ => Err(unsupported(format!("function {name}"))),
    }
}

fn overlay(source: &str, replacement: &str, start: i128, remove: i128) -> Result<Value> {
    // Java's substring calls throw outside these bounds; sqllib instead clips
    // or returns the source. Keep that disagreement explicit until reviewed.
    let len = source.chars().count();
    let offset = start.checked_sub(1).and_then(|v| usize::try_from(v).ok());
    let end = offset
        .zip(usize::try_from(remove).ok())
        .and_then(|(a, b)| a.checked_add(b));
    let (Some(offset), Some(end)) = (offset, end) else {
        return Err(unsupported(
            "OVERLAY range differs between Java and runtime",
        ));
    };
    if offset > len || end > len {
        return Err(unsupported(
            "OVERLAY range differs between Java and runtime",
        ));
    }
    if source
        .len()
        .checked_add(replacement.len())
        .is_none_or(|n| n > MAX_STRING_BYTES)
    {
        return Err(Refusal::Budget);
    }
    let mut result: String = source.chars().take(offset).collect();
    result.push_str(replacement);
    result.extend(source.chars().skip(end));
    Ok(Value::String(result))
}

fn split_with_profile(
    text: &str,
    separator: &str,
    profile: crate::SemanticsProfile,
) -> Result<Value> {
    // Calcite returns [] for an empty input even when the delimiter is empty;
    // sqllib returns [""] for that combination. Preserve both explicit profiles.
    if text.is_empty() && profile == crate::SemanticsProfile::LegacyJanino {
        return Ok(Value::Array(Vec::new()));
    }
    split(text, separator)
}

fn split(text: &str, separator: &str) -> Result<Value> {
    if separator.is_empty() {
        return Ok(Value::Array(vec![Value::String(text.to_owned())]));
    }
    if text.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }
    // Count before collecting so a short delimiter cannot cause a large array.
    if text.split(separator).take(4097).count() > 4096 {
        return Err(Refusal::Budget);
    }
    Ok(Value::Array(
        text.split(separator)
            .map(|s| Value::String(s.to_owned()))
            .collect(),
    ))
}

pub(crate) fn trim(mode: &str, remove: &Value, text: &Value) -> Result<Value> {
    if !matches!(mode, "BOTH" | "LEADING" | "TRAILING") {
        return Err(unsupported("TRIM flag"));
    }
    if *remove == Value::Null || *text == Value::Null {
        return Ok(Value::Null);
    }
    let (Value::String(remove), Value::String(text)) = (remove, text) else {
        return Err(unsupported("TRIM operands"));
    };
    // Calcite requires one UTF-16 character; sqllib accepts a set of characters.
    let mut chars = remove.chars();
    let c = chars
        .next()
        .filter(|c| c.len_utf16() == 1)
        .ok_or_else(|| unsupported("TRIM character semantics"))?;
    if chars.next().is_some() {
        return Err(unsupported("TRIM character semantics"));
    }
    Ok(Value::String(
        match mode {
            "LEADING" => text.trim_start_matches(c),
            "TRAILING" => text.trim_end_matches(c),
            _ => text.trim_matches(c),
        }
        .to_owned(),
    ))
}

fn index(value: i128) -> usize {
    usize::try_from(value.max(0)).unwrap_or(usize::MAX)
}
