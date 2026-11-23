//! Window sharing compares Java's printed literals. Decimal scale, signed zero
//! and the different exponent conventions of BigDecimal and Double are visible.
use std::{fmt::LowerExp, str::FromStr};

/// BigDecimal.toString preserves trailing zeroes. In scientific notation it
/// prefixes a positive exponent with '+', unlike Float.toString and Double.toString.
pub(super) fn decimal_text(coefficient: &str, scale: i64) -> String {
    let digits = coefficient.trim_start_matches('-');
    let exponent = digits.len() as i128 - 1 - i128::from(scale);
    let mut text = if scale >= 0 && exponent >= -6 {
        fixed(digits, exponent)
    } else {
        let mut mantissa = digits.to_owned();
        if mantissa.len() > 1 {
            mantissa.insert(1, '.');
        }
        format!("{mantissa}E{exponent:+}")
    };
    if coefficient.starts_with('-') {
        text.insert(0, '-');
    }
    text
}

/// Place the decimal point after the leading digit of 10^exponent.
fn fixed(digits: &str, exponent: i128) -> String {
    if exponent < 0 {
        return format!("0.{}{digits}", "0".repeat((-exponent - 1) as usize));
    }
    let point = exponent as usize + 1;
    let mut text = digits.to_owned();
    if point < digits.len() {
        text.insert(point, '.');
    } else {
        text.extend(std::iter::repeat_n('0', point - digits.len()));
    }
    text
}

/// Return the significant digits (without trailing zeroes) and the base-10
/// exponent of a finite, nonzero decimal produced by a float formatter.
fn significand(text: &str) -> (String, i32) {
    let text = text.trim_start_matches('-');
    let (mantissa, power) = text.split_once('e').map_or((text, 0), |(m, e)| {
        (m, e.parse::<i32>().expect("floating-point exponent"))
    });
    let point = mantissa.find('.').unwrap_or(mantissa.len());
    let all = mantissa.replace('.', "");
    let digits = all.trim_start_matches('0');
    let exponent = power + point as i32 - (all.len() - digits.len()) as i32 - 1;
    (digits.trim_end_matches('0').to_owned(), exponent)
}

/// Java chooses the closest shortest round-tripping decimal. If one digit
/// suffices it also considers two-digit decimals (notably for subnormals).
pub(super) fn floating_text<F>(value: F) -> String
where
    F: ryu::Float + Into<f64> + LowerExp + FromStr + PartialEq,
{
    let wide = value.into();
    if !wide.is_finite() || wide == 0.0 {
        return match wide {
            n if n.is_nan() => "NaN",
            f64::INFINITY => "Infinity",
            f64::NEG_INFINITY => "-Infinity",
            n if n.is_sign_negative() => "-0.0",
            _ => "0.0",
        }
        .to_owned();
    }

    let mut buffer = ryu::Buffer::new();
    let (mut digits, mut exponent) = significand(buffer.format_finite(value));
    if digits.len() == 1 {
        let two = format!("{value:.1e}");
        if two.parse::<F>().ok() == Some(value) {
            (digits, exponent) = significand(&two);
        }
    }
    let mut text = if (-3..7).contains(&exponent) {
        let mut plain = fixed(&digits, i128::from(exponent));
        if !plain.contains('.') {
            plain.push_str(".0");
        }
        plain
    } else {
        if digits.len() == 1 {
            digits.push('0');
        }
        digits.insert(1, '.');
        format!("{digits}E{exponent}")
    };
    if wide.is_sign_negative() {
        text.insert(0, '-');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_numeric_spelling() {
        assert_eq!(floating_text(f32::from_bits(1)), "1.4E-45");
        assert_eq!(floating_text(f64::from_bits(1)), "4.9E-324");
        assert_eq!(floating_text(-0.0f32), "-0.0");
        assert_eq!(floating_text(f64::from_bits(0xfff8000000000001)), "NaN");
        assert_eq!(floating_text(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(floating_text(1.25f64), decimal_text("125", 2));
        assert_eq!(floating_text(-1.0e-7f32), decimal_text("-10", 8));
        assert_eq!(floating_text(0.001f32), "0.001");
        assert_eq!(floating_text(1e-4f64), "1.0E-4");
        assert_eq!(decimal_text("10", 5), "0.00010");
        assert_eq!(floating_text(1e6f32), "1000000.0");
        assert_eq!(floating_text(1e7f64), "1.0E7");
        assert_eq!(decimal_text("10", -6), "1.0E+7");
    }
}
