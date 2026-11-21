//! Bounded adaptation of sqllib's UUID grammar: exactly 32 hex digits, optional
//! braces, and hyphens only between complete groups of four. No whitespace trim.

use crate::model::{Refusal, Result};

pub(crate) fn parse(text: &str) -> Result<u128> {
    let invalid = || Refusal::RuntimeError("invalid UUID string");
    if text.len() > 41 {
        return Err(invalid());
    }
    let body = text
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(text);
    let mut digits = 0usize;
    let mut previous_hyphen = false;
    let mut value = 0u128;
    for byte in body.bytes() {
        if byte == b'-' {
            if digits == 0 || !digits.is_multiple_of(4) || digits == 32 || previous_hyphen {
                return Err(invalid());
            }
            previous_hyphen = true;
        } else {
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(invalid()),
            };
            if digits == 32 {
                return Err(invalid());
            }
            value = (value << 4) | u128::from(digit);
            digits += 1;
            previous_hyphen = false;
        }
    }
    if digits != 32 {
        return Err(invalid());
    }
    Ok(value)
}

pub(crate) fn format(value: u128) -> String {
    let hex = format!("{value:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}
