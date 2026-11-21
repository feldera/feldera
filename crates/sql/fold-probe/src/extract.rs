//! Calendar and interval fields follow sqllib's EXTRACT functions. Calendar
//! centuries start at year 1; interval centuries are truncated durations.

use crate::model::{Result, SqlType, Value, unsupported};
use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};

pub(crate) fn evaluate(unit: &str, value: &Value, ty: &SqlType) -> Result<Value> {
    if !matches!(
        unit,
        "YEAR"
            | "MONTH"
            | "DAY"
            | "QUARTER"
            | "DECADE"
            | "CENTURY"
            | "MILLENNIUM"
            | "ISOYEAR"
            | "WEEK"
            | "DOW"
            | "ISODOW"
            | "DOY"
            | "EPOCH"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "MILLISECOND"
            | "MICROSECOND"
            | "NANOSECOND"
    ) {
        return Err(unsupported("EXTRACT unit"));
    }
    let result = match value {
        Value::Null => return Ok(Value::Null),
        Value::Date(date) => {
            if unit == "EPOCH" {
                Some(date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
            } else {
                date_field(unit, *date).or_else(|| time_field(unit, NaiveTime::MIN))
            }
        }
        Value::Timestamp(timestamp) => {
            if unit == "EPOCH" {
                // Runtime epoch extraction truncates toward zero before 1970.
                Some(timestamp.and_utc().timestamp_micros() / 1_000_000)
            } else {
                date_field(unit, timestamp.date()).or_else(|| time_field(unit, timestamp.time()))
            }
        }
        Value::Time(time) => time_field(unit, *time),
        Value::Months(months) => month_field(unit, *months),
        Value::Micros(micros) => microsecond_field(unit, *micros),
        _ => None,
    }
    .ok_or_else(|| unsupported("EXTRACT operand and unit"))?;
    crate::numeric::integer(i128::from(result), ty)
}

fn date_field(unit: &str, date: NaiveDate) -> Option<i64> {
    Some(match unit {
        "YEAR" => i64::from(date.year()),
        "MONTH" => i64::from(date.month()),
        "DAY" => i64::from(date.day()),
        "QUARTER" => i64::from(date.month0() / 3 + 1),
        "DECADE" => i64::from(date.year() / 10),
        "CENTURY" => i64::from((date.year() + 99) / 100),
        "MILLENNIUM" => i64::from((date.year() + 999) / 1000),
        "ISOYEAR" => i64::from(date.iso_week().year()),
        "WEEK" => i64::from(date.iso_week().week()),
        "DOW" => i64::from(date.weekday().number_from_sunday()),
        "ISODOW" => i64::from(date.weekday().number_from_monday()),
        "DOY" => i64::from(date.ordinal()),
        _ => return None,
    })
}

fn time_field(unit: &str, time: NaiveTime) -> Option<i64> {
    let second = i64::from(time.second());
    let nanos = i64::from(time.nanosecond());
    Some(match unit {
        "HOUR" => i64::from(time.hour()),
        "MINUTE" => i64::from(time.minute()),
        "SECOND" => second,
        "MILLISECOND" => second * 1000 + nanos / 1_000_000,
        "MICROSECOND" => second * 1_000_000 + nanos / 1000,
        "NANOSECOND" => second * 1_000_000_000 + nanos,
        _ => return None,
    })
}

fn month_field(unit: &str, months: i32) -> Option<i64> {
    Some(i64::from(match unit {
        "YEAR" => months / 12,
        "MONTH" => months % 12,
        "DECADE" => months / 120,
        "CENTURY" => months / 1200,
        "MILLENNIUM" => months / 12000,
        "DAY" | "HOUR" | "MINUTE" | "SECOND" | "MILLISECOND" | "MICROSECOND" => 0,
        _ => return None,
    }))
}

fn microsecond_field(unit: &str, micros: i64) -> Option<i64> {
    Some(match unit {
        "DAY" => micros / 86_400_000_000,
        "HOUR" => micros / 3_600_000_000 % 24,
        "MINUTE" => micros / 60_000_000 % 60,
        "SECOND" => micros / 1_000_000 % 60,
        "MILLISECOND" => micros % 60_000_000 / 1000,
        "MICROSECOND" => micros % 60_000_000,
        "EPOCH" => micros / 1_000_000,
        _ => return None,
    })
}
