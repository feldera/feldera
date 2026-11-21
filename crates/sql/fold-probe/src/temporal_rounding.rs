//! Calendar rounding follows sqllib, including Sunday weeks and legal ranges.

use crate::model::{Refusal, Result, Value, unsupported};
use chrono::{Datelike, Duration, Months, NaiveDate, NaiveDateTime, Timelike};

pub(crate) fn evaluate(operation: &str, unit: &str, value: &Value) -> Result<Value> {
    let months = match unit {
        "MILLENNIUM" => Some(12_000),
        "CENTURY" => Some(1_200),
        "DECADE" => Some(120),
        "YEAR" => Some(12),
        "QUARTER" => Some(3),
        "MONTH" => Some(1),
        "WEEK" | "DAY" => None,
        "HOUR" | "MINUTE" | "SECOND" | "MILLISECOND" | "MICROSECOND" | "NANOSECOND"
            if operation != "DATE_TRUNC" =>
        {
            None
        }
        _ => return Err(unsupported("temporal rounding unit")),
    };
    let timestamp = match value {
        Value::Null => return Ok(Value::Null),
        Value::Date(date) => date.and_hms_opt(0, 0, 0).unwrap(),
        Value::Timestamp(timestamp) => *timestamp,
        _ => return Err(unsupported("temporal rounding operand")),
    };
    let mut rounded = floor(timestamp, unit, operation == "DATE_TRUNC")?;
    if operation == "CEIL" && rounded != timestamp {
        rounded = if let Some(months) = months {
            rounded.checked_add_months(Months::new(months))
        } else {
            let delta = match unit {
                "WEEK" => Duration::weeks(1),
                "DAY" => Duration::days(1),
                "HOUR" => Duration::hours(1),
                "MINUTE" => Duration::minutes(1),
                "SECOND" => Duration::seconds(1),
                "MILLISECOND" => Duration::milliseconds(1),
                // Runtime timestamps already have microsecond precision.
                _ => Duration::zero(),
            };
            rounded.checked_add_signed(delta)
        }
        .ok_or(Refusal::RuntimeError("temporal rounding overflow"))?;
    }
    if !(1..=9999).contains(&rounded.year()) {
        return Err(Refusal::RuntimeError("temporal rounding out of range"));
    }
    Ok(if matches!(value, Value::Date(_)) {
        Value::Date(rounded.date())
    } else {
        Value::Timestamp(rounded)
    })
}

fn floor(value: NaiveDateTime, unit: &str, truncate: bool) -> Result<NaiveDateTime> {
    let date = value.date();
    let year = date.year();
    let year_start = |year| NaiveDate::from_ymd_opt(year, 1, 1);
    let rounded_date = match unit {
        "MILLENNIUM" | "CENTURY" => {
            let years = if unit == "MILLENNIUM" { 1000 } else { 100 };
            // DATE_TRUNC counts centuries from year 1; FLOOR uses multiples.
            let origin = i32::from(truncate);
            year_start((year - origin) / years * years + origin)
        }
        "DECADE" => year_start(year / 10 * 10),
        "YEAR" => year_start(year),
        "QUARTER" => NaiveDate::from_ymd_opt(year, (date.month() - 1) / 3 * 3 + 1, 1),
        "MONTH" => NaiveDate::from_ymd_opt(year, date.month(), 1),
        "WEEK" => date.checked_sub_signed(Duration::days(i64::from(
            date.weekday().num_days_from_sunday(),
        ))),
        "DAY" => Some(date),
        "HOUR" => return Ok(date.and_hms_opt(value.hour(), 0, 0).unwrap()),
        "MINUTE" => return Ok(date.and_hms_opt(value.hour(), value.minute(), 0).unwrap()),
        "SECOND" => return Ok(value.with_nanosecond(0).unwrap()),
        "MILLISECOND" => {
            return Ok(value
                .with_nanosecond(value.nanosecond() / 1_000_000 * 1_000_000)
                .unwrap());
        }
        "MICROSECOND" | "NANOSECOND" => return Ok(value),
        _ => return Err(unsupported("temporal rounding unit")),
    };
    rounded_date
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .ok_or(Refusal::RuntimeError("temporal rounding overflow"))
}
