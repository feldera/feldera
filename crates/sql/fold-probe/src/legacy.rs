//! Bounded legacy-parser checks. These run only in the named Janino profile,
//! and only when a cast is evaluated, preserving lazy CASE/COALESCE behavior.

pub(crate) mod trigonometry;

use crate::model::{Refusal, Result};

pub(crate) fn check_float_spelling(text: &str) -> Result<()> {
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    if (unsigned.eq_ignore_ascii_case("nan") && unsigned != "NaN")
        || (unsigned.eq_ignore_ascii_case("infinity") && unsigned != "Infinity")
        || unsigned.eq_ignore_ascii_case("inf")
    {
        return Err(Refusal::RuntimeError("legacy float spelling"));
    }
    Ok(())
}

pub(crate) fn check_timestamp_spelling(text: &str) -> Result<()> {
    let text = text.trim_matches(|c: char| c <= ' ');
    let bytes = text.as_bytes();
    let digits = |start, end| {
        bytes
            .get(start..end)
            .is_some_and(|s| s.iter().all(u8::is_ascii_digit))
    };
    let date = digits(0, 4)
        && bytes.get(4) == Some(&b'-')
        && digits(5, 7)
        && bytes.get(7) == Some(&b'-')
        && digits(8, 10);
    let clock = bytes.len() == 10
        || (bytes.get(10) == Some(&b' ')
            && digits(11, 13)
            && bytes.get(13) == Some(&b':')
            && digits(14, 16)
            && bytes.get(16) == Some(&b':')
            && digits(17, 19)
            && (bytes.len() == 19
                || (bytes.get(19) == Some(&b'.') && bytes.len() > 20 && digits(20, bytes.len()))));
    if !date || !clock {
        return Err(Refusal::RuntimeError("legacy timestamp spelling"));
    }
    Ok(())
}
