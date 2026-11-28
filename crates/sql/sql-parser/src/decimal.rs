//! Exact spelling of Calcite DECIMAL string literals.
use std::borrow::Cow;

/// Parse a quoted decimal's contents and return the plain spelling used to
/// derive its SQL type. The parser's 10,000-digit bound precedes expansion of
/// scientific notation. The binder checks the narrower SQL type range.
pub fn plain_decimal(text: &str) -> Option<String> {
    let text = text.trim_matches(' ');
    let ascii = if text.is_ascii() {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(
            text.chars()
                .map(decimal_character)
                .collect::<Option<String>>()?,
        )
    };
    let (mantissa, exponent) = match ascii.split_once(['e', 'E']) {
        Some((mantissa, power)) => (mantissa, power.parse::<i32>().ok()?),
        None => (ascii.as_ref(), 0),
    };
    let unsigned = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let coefficient = format!("{whole}{fraction}");
    let significant = coefficient.trim_start_matches('0');
    let digits = if significant.is_empty() {
        "0"
    } else {
        significant
    };
    let scale = i32::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    if digits.len().checked_add(scale.unsigned_abs() as usize)? > 10_000 {
        return None;
    }
    let mut plain = String::new();
    if mantissa.starts_with('-') && digits != "0" {
        plain.push('-');
    }
    if scale <= 0 {
        plain.push_str(digits);
        if digits != "0" {
            plain.extend(std::iter::repeat_n('0', scale.unsigned_abs() as usize));
        }
    } else if (scale as usize) < digits.len() {
        let (whole, fraction) = digits.split_at(digits.len() - scale as usize);
        plain.push_str(whole);
        plain.push('.');
        plain.push_str(fraction);
    } else {
        plain.push_str("0.");
        plain.extend(std::iter::repeat_n('0', scale as usize - digits.len()));
        plain.push_str(digits);
    }
    Some(plain)
}

fn decimal_character(c: char) -> Option<char> {
    if c.is_ascii() {
        return Some(c);
    }
    // BigDecimal examines individual UTF-16 code units. These are the
    // non-ASCII zeroes for Character.digit(char, 10) on the reference JDK.
    const ZEROES: &[u16] = &[
        0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66, 0x0ce6,
        0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0,
        0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0, 0xa9f0,
        0xaa50, 0xabf0, 0xff10,
    ];
    let unit = u16::try_from(u32::from(c)).ok()?;
    let index = ZEROES
        .partition_point(|zero| *zero <= unit)
        .checked_sub(1)?;
    char::from_digit(u32::from(unit - ZEROES[index]), 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_decimals_preserve_scale_and_check_expanded_size() {
        for (text, expected) in [
            (" +００۱.٢٠e+１ ", "12.0"),
            ("-0.00", "0.00"),
            ("-0e10", "0"),
            ("-.005", "-0.005"),
            ("0.4e1", "4"),
        ] {
            assert_eq!(plain_decimal(text).as_deref(), Some(expected), "{text}");
        }
        assert_eq!(plain_decimal("9e9999").unwrap().len(), 10_000);
        for text in ["9e10000", "3e2147483648", ".", " + ", "1.0.0", "𝟜", "1_2"] {
            assert!(plain_decimal(text).is_none(), "{text}");
        }
    }
}
