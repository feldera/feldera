//! Fold string casts using the same calendar and spelling rules as Java's Simplify.
use chrono::NaiveDate;
use feldera_sql_ir::{ExpressionKind as Expr, Field, TypeKind as Ty};

/// Calcite's wire spellings use fixed-width, unsigned ASCII fields.
fn fields(text: &str, separator: char, first_width: usize) -> Option<[i32; 3]> {
    fn digits(text: &str, width: usize) -> Option<i32> {
        if text.len() != width || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        text.parse().ok()
    }
    let (first, rest) = text.split_once(separator)?;
    let (second, third) = rest.split_once(separator)?;
    Some([
        digits(first, first_width)?,
        digits(second, 2)?,
        digits(third, 2)?,
    ])
}

fn time_fields(text: &str) -> Option<([i32; 3], &str)> {
    let (whole, fraction) = if let Some((whole, fraction)) = text.split_once('.') {
        if fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        (whole, fraction)
    } else {
        (text, "")
    };
    Some((fields(whole, ':', 2)?, fraction))
}

/// Calcite's TimestampString checks only the spelling. Avatica's ymdToUnixDate
/// uses Gregorian day-number arithmetic even for out-of-range fields. In
/// particular, February 30 can carry into March. Division truncates toward zero.
fn timestamp_epoch_days([year, month, day]: [i32; 3]) -> i64 {
    let jan_feb = (14 - month) / 12;
    let y = year + 4800 - jan_feb;
    let m = month + 12 * jan_feb - 3;
    let julian_day = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    i64::from(julian_day - 2_440_588)
}

pub(super) fn fold(text: &str, kind: Ty) -> Option<(Expr, Field)> {
    match kind {
        Ty::Date => {
            let [year, month, day] = fields(text, '-', 4)?;
            // Chrono accepts year zero, which Calcite's DateString rejects.
            if year == 0 {
                return None;
            }
            let date = NaiveDate::from_ymd_opt(year, month as u32, day as u32)?;
            Some((Expr::DateLiteral, Field::Data(date.to_epoch_days().into())))
        }
        Ty::Time => {
            let ([hour, minute, second], fraction) = time_fields(text)?;
            if hour >= 24 || minute >= 60 || second >= 60 {
                return None;
            }
            let fraction = fraction.trim_end_matches('0');
            let end = if fraction.is_empty() {
                8
            } else {
                9 + fraction.len()
            };
            Some((Expr::TimeLiteral, Field::Data(text[..end].into())))
        }
        Ty::Timestamp => {
            let (date, time) = text.split_once(' ')?;
            let days = timestamp_epoch_days(fields(date, '-', 4)?);
            let ([hour, minute, second], fraction) = time_fields(time)?;
            if fraction.len() > 6 {
                return None;
            }
            let seconds = days * 86_400 + i64::from(hour * 3600 + minute * 60 + second);
            let micros = fraction
                .bytes()
                .fold(0, |n, digit| n * 10 + i64::from(digit - b'0'));
            let value = seconds * 1_000_000 + micros * 10i64.pow(6 - fraction.len() as u32);
            Some((
                Expr::TimestampLiteral,
                Field::Data(value.to_string().into()),
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_use_calcite_field_arithmetic() {
        for (text, micros) in [
            ("2020-02-30 00:00:00", 1_583_020_800_000_000_i64),
            ("1969-12-31 24:00:00.000001", 1),
            ("1970-01-00 23:59:60", 0),
            ("1969-12-31 23:59:59.125", -875_000),
        ] {
            assert_eq!(
                fold(text, Ty::Timestamp),
                Some((
                    Expr::TimestampLiteral,
                    Field::Data(micros.to_string().into())
                )),
                "{text}"
            );
        }
    }

    #[test]
    fn temporal_types_enforce_their_own_spelling_and_ranges() {
        for (text, ty) in [
            ("-001-01-01", Ty::Date),
            ("0000-01-01", Ty::Date),
            ("-0001-01-01", Ty::Date),
            ("+10000-01-01", Ty::Date),
            ("+020-01-01", Ty::Date),
            ("2020-02-30", Ty::Date),
            ("24:00:00", Ty::Time),
            ("23:59:60", Ty::Time),
            ("1970-01-01 00:00:00.1234567", Ty::Timestamp),
            ("1970-01-01  0:00:00", Ty::Timestamp),
            ("1970-01-01 00:00:00.", Ty::Timestamp),
        ] {
            assert!(fold(text, ty).is_none(), "{text}");
        }
        for (text, days) in [("0001-01-01", -719_162), ("9999-12-31", 2_932_896)] {
            assert_eq!(
                fold(text, Ty::Date),
                Some((Expr::DateLiteral, Field::Data(days.into()))),
                "{text}"
            );
        }
    }
}
