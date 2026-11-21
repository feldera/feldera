//! SQL LIKE with bounded dynamic programming, without regex backtracking.

use crate::model::{Refusal, Result, Value, unsupported};

#[derive(Clone, Copy)]
enum Token {
    Literal(char),
    One,
    Many,
}

pub(crate) fn evaluate(name: &str, values: &[Value]) -> Result<Value> {
    let (text, pattern, escape) = match values {
        [Value::String(text), Value::String(pattern)] => (text, pattern, None),
        [
            Value::String(text),
            Value::String(pattern),
            Value::String(escape),
        ] => {
            let mut chars = escape.chars();
            let character = chars
                .next()
                .ok_or(Refusal::RuntimeError("empty LIKE escape"))?;
            if chars.next().is_some() {
                return Err(Refusal::RuntimeError("LIKE escape must be one character"));
            }
            (text, pattern, Some(character))
        }
        _ => return Err(unsupported("LIKE argument types or arity")),
    };
    if !matches!(name, "LIKE" | "ILIKE") {
        return Err(unsupported("pattern operator"));
    }
    // Calcite's case-insensitive regex is ASCII-only; sqllib lowercases Unicode.
    // Its '_' also treats line terminators differently. Refuse those cases until
    // their semantics have a reviewed disposition.
    if name == "ILIKE" && (!text.is_ascii() || !pattern.is_ascii())
        || text.chars().chain(pattern.chars()).any(|c| {
            c == '\0'
                || c.len_utf16() == 2
                || matches!(c, '\n' | '\r' | '\u{85}' | '\u{2028}' | '\u{2029}')
        })
    {
        return Err(unsupported("LIKE Java/runtime character semantics"));
    }
    // Byte lengths overestimate character counts, and bound allocation before
    // collecting either input. Every DP cell takes constant work.
    if (text.len() + 1)
        .checked_mul(pattern.len() + 1)
        .is_none_or(|cells| cells > 1_000_000)
    {
        return Err(Refusal::Budget);
    }
    let normalize = |c: char| {
        if name == "ILIKE" {
            c.to_ascii_lowercase()
        } else {
            c
        }
    };
    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        let token = if Some(c) == escape {
            let next = chars
                .next()
                .ok_or(Refusal::RuntimeError("trailing LIKE escape"))?;
            if next != '%' && next != '_' && Some(next) != escape {
                return Err(Refusal::RuntimeError("invalid LIKE escape sequence"));
            }
            Token::Literal(normalize(next))
        } else {
            match c {
                '%' => Token::Many,
                '_' => Token::One,
                _ => Token::Literal(normalize(c)),
            }
        };
        tokens.push(token);
    }
    let mut previous = vec![false; tokens.len() + 1];
    let mut current = previous.clone();
    previous[0] = true;
    for (i, token) in tokens.iter().enumerate() {
        previous[i + 1] = previous[i] && matches!(token, Token::Many);
    }
    for c in text.chars().map(normalize) {
        current[0] = false;
        for (i, token) in tokens.iter().enumerate() {
            current[i + 1] = match token {
                Token::Many => current[i] || previous[i + 1],
                Token::One => previous[i],
                Token::Literal(expected) => previous[i] && c == *expected,
            };
        }
        std::mem::swap(&mut previous, &mut current);
    }
    Ok(Value::Bool(previous[tokens.len()]))
}
