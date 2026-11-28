// SPDX-License-Identifier: Apache-2.0

//! Feldera's Calcite lexer matches the quoted token before decoding C escapes.

use super::State;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

/// On entry the cursor points at the opening quote after E. If the quoted
/// token has no terminator, the lexer can still match E as an identifier.
pub(super) fn literal(input: &mut State<'_>) -> Result<Option<String>, &'static str> {
    let mut cursor = input.clone();
    cursor.next();
    let mut text = String::new();
    let mut last_quote = None;
    while let Some(ch) = cursor.next() {
        match ch {
            '\'' => {
                // The first of two quotes could also end the token. JavaCC
                // takes the longest complete match, including at EOF.
                last_quote = Some((cursor.clone(), text.len()));
                if cursor.peek() != Some(&'\'') {
                    break;
                }
                cursor.next();
                text.push_str("''");
            }
            '\\' => {
                text.push(ch);
                if let Some(escaped) = cursor.next() {
                    text.push(escaped);
                }
            }
            _ => text.push(ch),
        }
    }
    let Some((end, len)) = last_quote else {
        return Ok(None);
    };
    *input = end;
    let raw = text[..len].replace("''", "'");
    decode(&raw).map(Some)
}

fn decode(text: &str) -> Result<String, &'static str> {
    let mut chars = text.chars().peekable();
    let mut units = Vec::with_capacity(text.len());
    while let Some(ch) = chars.next() {
        if ch != '\\' || chars.peek().is_none() {
            units.extend_from_slice(ch.encode_utf16(&mut [0; 2]));
            continue;
        }
        let escape = chars.next().expect("checked following character");
        let escaped = match escape {
            'b' => '\x08',
            'f' => '\x0c',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            '\'' | '\\' => escape,
            '0'..='3' | 'x' => {
                let hex = escape == 'x';
                let radix = if hex { 16 } else { 8 };
                let mut number = escape.to_digit(radix);
                for _ in 0..2 {
                    let Some(digit) = chars.next_if(|c| c.is_digit(radix)) else {
                        break;
                    };
                    number = Some(number.unwrap_or(0) * radix + digit.to_digit(radix).unwrap());
                }
                number.and_then(char::from_u32).unwrap_or('x')
            }
            'u' | 'U' => {
                let mut number = 0;
                for _ in 0..if escape == 'u' { 4 } else { 8 } {
                    let digit = chars
                        .next()
                        .and_then(|ch| ch.to_digit(16))
                        .ok_or("Malformed Unicode escape")?;
                    number = number * 16 + digit;
                }
                if let Ok(unit) = u16::try_from(number) {
                    units.push(unit);
                    continue;
                }
                char::from_u32(number).ok_or("Invalid Unicode code point")?
            }
            _ => {
                units.push(b'\\' as u16);
                escape
            }
        };
        units.extend_from_slice(escaped.encode_utf16(&mut [0; 2]));
    }
    String::from_utf16(&units).map_err(|_| "Unpaired surrogate in escaped string literal")
}
