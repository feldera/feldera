//! Bind local temporal literals. The statement keeps the fractional spelling
//! from SQL, while each Rex literal carries its resolved precision.

use super::{Cell, CellText};
use crate::DataType;
use feldera_sql_rel_model::{
    literals::{LiteralValue, date_from_text},
    types::{Precision, TimeZone, TypeKind},
};
use sqlparser_064::ast::TimezoneInfo;

pub(super) fn parse(ty: &DataType, text: &str) -> Option<Cell> {
    let zone = TimeZone::None;
    match ty {
        DataType::Date => {
            let (epoch_days, date) = date(text)?;
            Some(Cell {
                value: LiteralValue::Date { epoch_days },
                kind: TypeKind::Date {},
                text: CellText::Literal(format!("DATE '{date}'")),
            })
        }
        DataType::Time(None, TimezoneInfo::None) => {
            let (nanoseconds_since_midnight, precision, time) = clock(text, 9)?;
            Some(Cell {
                value: LiteralValue::Time {
                    nanoseconds_since_midnight,
                },
                kind: TypeKind::Time { precision, zone },
                text: CellText::Literal(format!("TIME '{time}'")),
            })
        }
        DataType::Timestamp(None, TimezoneInfo::None) => {
            let (day, time) = text.split_once(' ').unwrap_or((text, "00:00:00"));
            let (epoch_days, date) = date(day)?;
            let (nanoseconds_since_midnight, precision, time) =
                clock(time.trim_start_matches(' '), 6)?;
            Some(Cell {
                value: LiteralValue::Timestamp {
                    epoch_days,
                    nanoseconds_since_midnight,
                },
                kind: TypeKind::Timestamp { precision, zone },
                text: CellText::Literal(format!("TIMESTAMP '{date} {time}'")),
            })
        }
        _ => None,
    }
}

fn digits(text: &str, max_width: usize) -> Option<u32> {
    if text.len() > max_width || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn date(text: &str) -> Option<(i32, String)> {
    let (year, rest) = text.split_once('-')?;
    let (month, day) = rest.split_once('-')?;
    let (year, month, day) = (digits(year, 4)?, digits(month, 2)?, digits(day, 2)?);
    // Calcite validates typed literals using GregorianCalendar's cutover.
    if (year, month) == (1582, 10) && (5..=14).contains(&day) {
        return None;
    }
    let date = format!("{year:04}-{month:02}-{day:02}");
    Some((date_from_text(&date)?, date))
}

fn clock(text: &str, max_precision: usize) -> Option<(u64, Precision, String)> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let (hour, rest) = whole.split_once(':')?;
    let (minute, second) = rest.split_once(':')?;
    let (hour, minute, second) = (digits(hour, 2)?, digits(minute, 2)?, digits(second, 2)?);
    if hour >= 24 || minute >= 60 || second >= 60 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let precision = fraction.len().min(max_precision);
    let nanos = fraction[..precision]
        .bytes()
        .fold(0_u64, |n, digit| n * 10 + u64::from(digit - b'0'))
        * 10_u64.pow(9 - precision as u32);
    let seconds = hour * 3600 + minute * 60 + second;
    let mut sql = format!("{hour:02}:{minute:02}:{second:02}");
    if !fraction.is_empty() {
        sql.push('.');
        sql.push_str(fraction);
    }
    Some((
        u64::from(seconds) * 1_000_000_000 + nanos,
        Precision::Specified(precision as u32),
        sql,
    ))
}
