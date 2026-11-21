//! Bounded adaptation of sqllib's interval arithmetic and chrono date operations.

use crate::model::{Refusal, Result, SqlType, Value, unsupported};
use chrono::{Datelike, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

pub(crate) fn parse_date(text: &str) -> Result<NaiveDate> {
    let date = NaiveDate::parse_from_str(text.trim_matches(|c: char| c <= ' '), "%Y-%m-%d")
        .map_err(|_| Refusal::RuntimeError("date cast"))?;
    if !(1..=9999).contains(&date.year()) {
        return Err(Refusal::RuntimeError("date out of range"));
    }
    Ok(date)
}

pub(crate) fn parse_time(text: &str) -> Result<NaiveTime> {
    let time = NaiveTime::parse_from_str(text.trim_matches(|c: char| c <= ' '), "%H:%M:%S%.f")
        .map_err(|_| Refusal::RuntimeError("time cast"))?;
    // Time::from_time retains nanoseconds and clamps leap seconds.
    Ok(time
        .with_nanosecond(time.nanosecond().min(999_999_999))
        .unwrap())
}

pub(crate) fn parse_timestamp(text: &str) -> Result<NaiveDateTime> {
    let text = text.trim_matches(|c: char| c <= ' ');
    let timestamp = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
        })
        .map_err(|_| Refusal::RuntimeError("timestamp cast"))?;
    if !(1..=9999).contains(&timestamp.year()) {
        return Err(Refusal::RuntimeError("timestamp out of range"));
    }
    // Match Timestamp::from_naiveDateTime's half-up microsecond rounding,
    // including a carry into the next second and dates before the epoch.
    // Clamp leap seconds before rounding, as sqllib::without_leap_second does.
    let nanos = if timestamp.nanosecond() >= 1_000_000_000 {
        999_999_000
    } else {
        timestamp.nanosecond()
    };
    let rounded = timestamp
        .with_nanosecond(0)
        .unwrap()
        .checked_add_signed(Duration::microseconds(i64::from((nanos + 500) / 1000)))
        .ok_or(Refusal::RuntimeError("timestamp rounding overflow"))?;
    if !(1..=9999).contains(&rounded.year()) {
        return Err(Refusal::RuntimeError("timestamp out of range"));
    }
    Ok(rounded)
}

pub(crate) fn arithmetic(op: &str, left: &Value, right: &Value, ty: &SqlType) -> Result<Value> {
    let error = || Refusal::RuntimeError("temporal arithmetic overflow");
    match (left, right) {
        (Value::Date(date), Value::Months(months)) if op == "+" || op == "-" => {
            let months = i64::from(*months) * if op == "-" { -1 } else { 1 };
            let count = Months::new(months.unsigned_abs() as u32);
            let date = if months < 0 {
                date.checked_sub_months(count)
            } else {
                date.checked_add_months(count)
            }
            .ok_or_else(error)?;
            if !(1..=9999).contains(&date.year()) {
                return Err(error());
            }
            Ok(Value::Date(date))
        }
        (Value::Date(date), Value::Micros(micros)) if op == "+" || op == "-" => {
            let days = micros / 86_400_000_000 * if op == "-" { -1 } else { 1 };
            let date = date
                .checked_add_signed(Duration::days(days))
                .ok_or_else(error)?;
            if !(1..=9999).contains(&date.year()) {
                return Err(error());
            }
            Ok(Value::Date(date))
        }
        (Value::Timestamp(date), Value::Months(months)) if op == "+" || op == "-" => {
            let months = i64::from(*months) * if op == "-" { -1 } else { 1 };
            let count = Months::new(months.unsigned_abs() as u32);
            let date = if months < 0 {
                date.checked_sub_months(count)
            } else {
                date.checked_add_months(count)
            }
            .ok_or_else(error)?;
            if !(1..=9999).contains(&date.year()) {
                return Err(error());
            }
            Ok(Value::Timestamp(date))
        }
        (Value::Timestamp(date), Value::Micros(micros)) if op == "+" || op == "-" => {
            let delta = Duration::microseconds(*micros);
            let date = if op == "-" {
                date.checked_sub_signed(delta)
            } else {
                date.checked_add_signed(delta)
            }
            .ok_or_else(error)?;
            if !(1..=9999).contains(&date.year()) {
                return Err(error());
            }
            Ok(Value::Timestamp(date))
        }
        (Value::Months(_) | Value::Micros(_), Value::Date(_) | Value::Timestamp(_))
            if op == "+" =>
        {
            arithmetic(op, right, left, ty)
        }
        (
            Value::Int(_) | Value::Float(_) | Value::Decimal(_),
            Value::Months(_) | Value::Micros(_),
        ) if op == "*" => arithmetic(op, right, left, ty),
        (Value::Months(months), other) => interval(op, i128::from(*months), other, true, ty),
        (Value::Micros(micros), other) => interval(op, i128::from(*micros), other, false, ty),
        _ => Err(unsupported("temporal operands")),
    }
}

fn interval(op: &str, value: i128, other: &Value, months: bool, ty: &SqlType) -> Result<Value> {
    let overflow = || Refusal::RuntimeError("interval overflow or division by zero");
    let result = match (op, other) {
        ("+", Value::Months(v)) if months => value.checked_add(i128::from(*v)),
        ("-", Value::Months(v)) if months => value.checked_sub(i128::from(*v)),
        ("+", Value::Micros(v)) if !months => value.checked_add(i128::from(*v)),
        ("-", Value::Micros(v)) if !months => value.checked_sub(i128::from(*v)),
        ("*", Value::Int(v)) => value.checked_mul(*v),
        ("/", Value::Int(v)) => value.checked_div(*v),
        ("*" | "/", Value::Float(v)) => {
            let result = if op == "*" {
                value as f64 * v
            } else {
                value as f64 / v
            };
            let (min, max_exclusive) = if months {
                (i32::MIN as f64, -(i32::MIN as f64))
            } else {
                (i64::MIN as f64, -(i64::MIN as f64))
            };
            if !result.is_finite() || result < min || result >= max_exclusive {
                return Err(overflow());
            }
            Some(result as i128)
        }
        ("*" | "/", Value::Decimal(v)) => {
            let scale = 10i128
                .checked_pow(v.exponent().into())
                .ok_or_else(|| unsupported("interval decimal scale"))?;
            let numerator = if op == "*" {
                value.checked_mul(v.significand())
            } else {
                value.checked_mul(scale)
            }
            .ok_or_else(|| unsupported("interval decimal intermediate exceeds i128"))?;
            numerator.checked_div(if op == "*" { scale } else { v.significand() })
        }
        _ => return Err(unsupported("interval operands")),
    }
    .ok_or_else(overflow)?;
    // Runtime arithmetic checks its storage width before SQL qualifier
    // truncation. Truncating first could hide an overflow near the boundary.
    if (months && i32::try_from(result).is_err()) || (!months && i64::try_from(result).is_err()) {
        return Err(overflow());
    }
    if months {
        cast_interval(
            Value::Months(i32::try_from(result).map_err(|_| overflow())?),
            ty,
        )
    } else {
        cast_interval(
            Value::Micros(i64::try_from(result).map_err(|_| overflow())?),
            ty,
        )
    }
}

/// Same-family casts truncate toward zero to the target's last field. Seconds
/// retain microseconds, matching sqllib's interval-to-interval cast functions.
pub(crate) fn cast_interval(value: Value, ty: &SqlType) -> Result<Value> {
    match value {
        Value::Months(value) => {
            let unit = match ty.name.as_str() {
                "INTERVAL_YEAR" => 12,
                "INTERVAL_MONTH" | "INTERVAL_YEAR_MONTH" => 1,
                _ => return Err(unsupported("year-month interval cast target")),
            };
            Ok(Value::Months(value / unit * unit))
        }
        Value::Micros(value) => {
            let unit = match ty.name.as_str() {
                "INTERVAL_DAY" => 86_400_000_000,
                "INTERVAL_HOUR" | "INTERVAL_DAY_HOUR" => 3_600_000_000,
                "INTERVAL_MINUTE" | "INTERVAL_DAY_MINUTE" | "INTERVAL_HOUR_MINUTE" => 60_000_000,
                "INTERVAL_SECOND"
                | "INTERVAL_DAY_SECOND"
                | "INTERVAL_HOUR_SECOND"
                | "INTERVAL_MINUTE_SECOND" => 1,
                _ => return Err(unsupported("day-time interval cast target")),
            };
            Ok(Value::Micros(value / unit * unit))
        }
        _ => Err(unsupported("interval cast source")),
    }
}
