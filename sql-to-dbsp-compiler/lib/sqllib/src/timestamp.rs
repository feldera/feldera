//! Support for SQL Timestamp and Date data types.

use crate::interval::{LongInterval, ShortInterval};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use serde::{de::Error as _, ser::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use size_of::SizeOf;
use std::{
    borrow::Cow,
    fmt::{self, Debug},
    ops::Add,
};

/// Similar to a unix timestamp: a positive time interval between Jan 1 1970 and
/// the current time. The supported range is limited (e.g., up to 2038 in
/// MySQL). We use milliseconds to represent the interval.
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf)]
pub struct Timestamp {
    // since unix epoch
    milliseconds: i64,
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let datetime = NaiveDateTime::from_timestamp_millis(self.milliseconds).ok_or(fmt::Error)?;

        f.write_str(&datetime.format("%F %T%.f").to_string())
    }
}

/// Serialize timestamp into the `YYYY-MM-DD HH:MM:SS.fff` format, where
/// `fff` is the fractional part of a second.
impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let datetime =
            NaiveDateTime::from_timestamp_millis(self.milliseconds).ok_or_else(|| {
                S::Error::custom(format!(
                    "timestamp value '{}' out of range",
                    self.milliseconds
                ))
            })?;

        serializer.serialize_str(&datetime.format("%F %T%.f").to_string())
    }
}

/// Deserialize timestamp from the `YYYY-MM-DD HH:MM:SS.fff` format.
///
/// # Caveats
///
/// * The format does not include a time zone, because the `TIMESTAMP` type is
///   supposed to be timezone-agnostic.
/// * Different databases may use different default export formats for
///   timestamps.  Moreover, I suspect that binary serialization formats
///   represent timestamps as numbers (which is how they are stored inside the
///   DB), so in the future we may need a smarter implementation that supports
///   all these variants.
impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `timestamp_str: &'de` doesn't work for JSON, which escapes strings
        // and can only deserialize into an owned string.
        let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

        let timestamp = NaiveDateTime::parse_from_str(&timestamp_str, "%F %T%.f").map_err(|e| {
            D::Error::custom(format!("invalid timestamp string '{timestamp_str}': {e}"))
        })?;

        Ok(Self::new(timestamp.timestamp_millis()))
    }
}

impl Timestamp {
    pub const fn new(milliseconds: i64) -> Self {
        Self { milliseconds }
    }

    pub fn milliseconds(&self) -> i64 {
        self.milliseconds
    }

    pub fn to_dateTime(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(
            self.milliseconds / 1000,
            ((self.milliseconds % 1000i64) as u32) * 1_000_000,
        )
        .single()
        .unwrap()
    }

    pub fn from_dateTime(date: DateTime<Utc>) -> Self {
        Self {
            milliseconds: date.timestamp_millis(),
        }
    }
}

impl<T> From<T> for Timestamp
where
    i64: From<T>,
{
    fn from(value: T) -> Self {
        Self {
            milliseconds: i64::from(value),
        }
    }
}

impl Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, value: i64) -> Self {
        Self {
            milliseconds: self.milliseconds + value,
        }
    }
}

pub fn plus_Timestamp_ShortInterval(left: Timestamp, right: ShortInterval) -> Timestamp {
    left.add(right.milliseconds())
}

pub fn minus_Timestamp_Timestamp_ShortInterval(left: Timestamp, right: Timestamp) -> ShortInterval {
    ShortInterval::from(left.milliseconds() - right.milliseconds())
}

pub fn minus_TimestampN_Timestamp_ShortIntervalN(
    left: Option<Timestamp>,
    right: Timestamp,
) -> Option<ShortInterval> {
    left.map(|l| minus_Timestamp_Timestamp_ShortInterval(l, right))
}

pub fn minus_Timestamp_ShortInterval_Timestamp(left: Timestamp, right: ShortInterval) -> Timestamp {
    Timestamp::new(left.milliseconds() - right.milliseconds())
}

pub fn minus_Timestamp_TimestampN_ShortIntervalN(
    left: Timestamp,
    right: Option<Timestamp>,
) -> Option<ShortInterval> {
    right.map(|r| ShortInterval::from(left.milliseconds() - r.milliseconds()))
}

pub fn minus_Timestamp_Timestamp_LongInterval(left: Timestamp, right: Timestamp) -> LongInterval {
    let ldate = left.to_dateTime();
    let rdate = right.to_dateTime();
    let ly = ldate.year();
    let lm = ldate.month() as i32;
    let ld = ldate.day() as i32;
    let lt = ldate.time();

    let ry = rdate.year();
    let mut rm = rdate.month() as i32;
    let rd = rdate.day() as i32;
    let rt = rdate.time();
    if (ld < rd) || ((ld == rd) && lt < rt) {
        // the full month is not yet elapsed
        rm += 1;
    }
    LongInterval::from((ly - ry) * 12 + lm - rm)
}

pub fn minus_TimestampN_Timestamp_LongIntervalN(
    left: Option<Timestamp>,
    right: Timestamp,
) -> Option<LongInterval> {
    left.map(|l| minus_Timestamp_Timestamp_LongInterval(l, right))
}

pub fn extract_year_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.year().into()
}

pub fn extract_year_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_year_Timestamp)
}

pub fn extract_month_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.month().into()
}

pub fn extract_month_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_month_Timestamp)
}

pub fn extract_day_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.day().into()
}

pub fn extract_day_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_day_Timestamp)
}

pub fn extract_quarter_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    (Date.month0() / 3 + 1).into()
}

pub fn extract_quarter_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_quarter_Timestamp)
}

pub fn extract_decade_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    (Date.year() / 10).into()
}

pub fn extract_decade_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_decade_Timestamp)
}

pub fn extract_century_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    ((Date.year() + 99) / 100).into()
}

pub fn extract_century_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_century_Timestamp)
}

pub fn extract_millennium_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    ((Date.year() + 999) / 1000).into()
}

pub fn extract_millennium_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_millennium_Timestamp)
}

pub fn extract_isoyear_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.iso_week().year().into()
}

pub fn extract_isoyear_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_isoyear_Timestamp)
}

pub fn extract_week_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.iso_week().week().into()
}

pub fn extract_week_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_week_Timestamp)
}

pub fn extract_dow_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    (Date.weekday().num_days_from_sunday() as i64) + Date::first_day_of_week()
}

pub fn extract_dow_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_dow_Timestamp)
}

pub fn extract_isodow_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    (Date.weekday().num_days_from_monday() as i64) + 1
}

pub fn extract_isodow_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_isodow_Timestamp)
}

pub fn extract_doy_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.ordinal().into()
}

pub fn extract_doy_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_doy_Timestamp)
}

pub fn extract_epoch_Timestamp(t: Timestamp) -> i64 {
    t.milliseconds() / 1000
}

pub fn extract_epoch_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_epoch_Timestamp)
}

pub fn extract_millisecond_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    (Date.second() * 1000 + Date.timestamp_subsec_millis()).into()
}

pub fn extract_millisecond_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_millisecond_Timestamp)
}

pub fn extract_microsecond_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    (Date.second() * 1_000_000 + Date.timestamp_subsec_micros()).into()
}

pub fn extract_microsecond_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_microsecond_Timestamp)
}

pub fn extract_second_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.second().into()
}

pub fn extract_second_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_second_Timestamp)
}

pub fn extract_minute_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.minute().into()
}

pub fn extract_minute_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_minute_Timestamp)
}

pub fn extract_hour_Timestamp(value: Timestamp) -> i64 {
    let Date = value.to_dateTime();
    Date.hour().into()
}

pub fn extract_hour_TimestampN(value: Option<Timestamp>) -> Option<i64> {
    value.map(extract_hour_Timestamp)
}

pub fn lt_Timestamp_Timestamp(left: Timestamp, right: Timestamp) -> bool {
    left < right
}

pub fn lt_TimestampN_Timestamp(left: Option<Timestamp>, right: Timestamp) -> Option<bool> {
    left.map(|l| l < right)
}

pub fn lt_Timestamp_TimestampN(left: Timestamp, right: Option<Timestamp>) -> Option<bool> {
    right.map(|r| left < r)
}

pub fn lt_TimestampN_TimestampN(left: Option<Timestamp>, right: Option<Timestamp>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l < r),
    }
}

pub fn gt_Timestamp_Timestamp(left: Timestamp, right: Timestamp) -> bool {
    left > right
}

pub fn gt_TimestampN_Timestamp(left: Option<Timestamp>, right: Timestamp) -> Option<bool> {
    left.map(|l| l > right)
}

pub fn gt_Timestamp_TimestampN(left: Timestamp, right: Option<Timestamp>) -> Option<bool> {
    right.map(|r| left > r)
}

pub fn gt_TimestampN_TimestampN(left: Option<Timestamp>, right: Option<Timestamp>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l > r),
    }
}

pub fn eq_Timestamp_Timestamp(left: Timestamp, right: Timestamp) -> bool {
    left == right
}

pub fn eq_TimestampN_Timestamp(left: Option<Timestamp>, right: Timestamp) -> Option<bool> {
    left.map(|l| l == right)
}

pub fn eq_Timestamp_TimestampN(left: Timestamp, right: Option<Timestamp>) -> Option<bool> {
    right.map(|r| left == r)
}

pub fn eq_TimestampN_TimestampN(left: Option<Timestamp>, right: Option<Timestamp>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l == r),
    }
}

pub fn neq_Timestamp_Timestamp(left: Timestamp, right: Timestamp) -> bool {
    left != right
}

pub fn neq_TimestampN_Timestamp(left: Option<Timestamp>, right: Timestamp) -> Option<bool> {
    left.map(|l| l != right)
}

pub fn neq_Timestamp_TimestampN(left: Timestamp, right: Option<Timestamp>) -> Option<bool> {
    right.map(|r| left != r)
}

pub fn neq_TimestampN_TimestampN(
    left: Option<Timestamp>,
    right: Option<Timestamp>,
) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l != r),
    }
}

pub fn gte_Timestamp_Timestamp(left: Timestamp, right: Timestamp) -> bool {
    left >= right
}

pub fn gte_TimestampN_Timestamp(left: Option<Timestamp>, right: Timestamp) -> Option<bool> {
    left.map(|l| l >= right)
}

pub fn gte_Timestamp_TimestampN(left: Timestamp, right: Option<Timestamp>) -> Option<bool> {
    right.map(|r| left >= r)
}

pub fn gte_TimestampN_TimestampN(
    left: Option<Timestamp>,
    right: Option<Timestamp>,
) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l >= r),
    }
}

pub fn lte_Timestamp_Timestamp(left: Timestamp, right: Timestamp) -> bool {
    left <= right
}

pub fn lte_TimestampN_Timestamp(left: Option<Timestamp>, right: Timestamp) -> Option<bool> {
    left.map(|l| l <= right)
}

pub fn lte_Timestamp_TimestampN(left: Timestamp, right: Option<Timestamp>) -> Option<bool> {
    right.map(|r| left <= r)
}

pub fn lte_TimestampN_TimestampN(
    left: Option<Timestamp>,
    right: Option<Timestamp>,
) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l <= r),
    }
}

pub fn floor_week_Timestamp(value: Timestamp) -> Timestamp {
    let wd = extract_dow_Timestamp(value) - Date::first_day_of_week();
    let ts = value.to_dateTime();
    let notime = ts
        .with_hour(0)
        .unwrap()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let notimeTs = Timestamp::from_dateTime(notime);
    let interval = ShortInterval::new(wd * 86400 * 1000);

    minus_Timestamp_ShortInterval_Timestamp(notimeTs, interval)
}

pub fn floor_week_TimestampN(value: Option<Timestamp>) -> Option<Timestamp> {
    value.map(floor_week_Timestamp)
}

////////////////////////////

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf)]
pub struct Date {
    // since unix epoch
    days: i32,
}

impl Date {
    pub const fn new(days: i32) -> Self {
        Self { days }
    }

    pub fn days(&self) -> i32 {
        self.days
    }

    pub fn to_timestamp(&self) -> Timestamp {
        Timestamp::new((self.days as i64) * 86400 * 1000)
    }

    pub fn to_dateTime(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.days() as i64 * 86400, 0)
            .single()
            .unwrap()
    }

    pub fn first_day_of_week() -> i64 {
        // This should depend on the SQL dialect, but the calcite
        // optimizer seems to imply this for all dialects.
        1
    }
}

impl<T> From<T> for Date
where
    i32: From<T>,
{
    fn from(value: T) -> Self {
        Self {
            days: i32::from(value),
        }
    }
}

/// Serialize date into the `YYYY-MM-DD` format
impl Serialize for Date {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = (self.days as i64) * 86400 * 1000;
        let datetime = NaiveDateTime::from_timestamp_millis(millis)
            .ok_or_else(|| S::Error::custom(format!("date value '{}' out of range", self.days)))?;

        serializer.serialize_str(&datetime.format("%F").to_string())
    }
}

/// Deserialize date from the `YYYY-MM-DD` format.
impl<'de> Deserialize<'de> for Date {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: &'de str = Deserialize::deserialize(deserializer)?;
        let date = NaiveDate::parse_from_str(str, "%Y-%m-%d")
            .map_err(|e| D::Error::custom(format!("invalid date string '{str}': {e}")))?;
        Ok(Self::new(
            (date.and_time(NaiveTime::default()).timestamp() / 86400) as i32,
        ))
    }
}

pub fn lt_Date_Date(left: Date, right: Date) -> bool {
    left < right
}

pub fn lt_DateN_Date(left: Option<Date>, right: Date) -> Option<bool> {
    left.map(|l| l < right)
}

pub fn lt_Date_DateN(left: Date, right: Option<Date>) -> Option<bool> {
    right.map(|r| left < r)
}

pub fn lt_DateN_DateN(left: Option<Date>, right: Option<Date>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l < r),
    }
}

pub fn gt_Date_Date(left: Date, right: Date) -> bool {
    left > right
}

pub fn gt_DateN_Date(left: Option<Date>, right: Date) -> Option<bool> {
    left.map(|l| l > right)
}

pub fn gt_Date_DateN(left: Date, right: Option<Date>) -> Option<bool> {
    right.map(|r| left > r)
}

pub fn gt_DateN_DateN(left: Option<Date>, right: Option<Date>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l > r),
    }
}

pub fn eq_Date_Date(left: Date, right: Date) -> bool {
    left == right
}

pub fn eq_DateN_Date(left: Option<Date>, right: Date) -> Option<bool> {
    left.map(|l| l == right)
}

pub fn eq_Date_DateN(left: Date, right: Option<Date>) -> Option<bool> {
    right.map(|r| left == r)
}

pub fn eq_DateN_DateN(left: Option<Date>, right: Option<Date>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l == r),
    }
}

pub fn neq_Date_Date(left: Date, right: Date) -> bool {
    left != right
}

pub fn neq_DateN_Date(left: Option<Date>, right: Date) -> Option<bool> {
    left.map(|l| l != right)
}

pub fn neq_Date_DateN(left: Date, right: Option<Date>) -> Option<bool> {
    right.map(|r| left != r)
}

pub fn neq_DateN_DateN(left: Option<Date>, right: Option<Date>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l != r),
    }
}

pub fn gte_Date_Date(left: Date, right: Date) -> bool {
    left >= right
}

pub fn gte_DateN_Date(left: Option<Date>, right: Date) -> Option<bool> {
    left.map(|l| l >= right)
}

pub fn gte_Date_DateN(left: Date, right: Option<Date>) -> Option<bool> {
    right.map(|r| left >= r)
}

pub fn gte_DateN_DateN(left: Option<Date>, right: Option<Date>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l >= r),
    }
}

pub fn lte_Date_Date(left: Date, right: Date) -> bool {
    left <= right
}

pub fn lte_DateN_Date(left: Option<Date>, right: Date) -> Option<bool> {
    left.map(|l| l <= right)
}

pub fn lte_Date_DateN(left: Date, right: Option<Date>) -> Option<bool> {
    right.map(|r| left <= r)
}

pub fn lte_DateN_DateN(left: Option<Date>, right: Option<Date>) -> Option<bool> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(l <= r),
    }
}

pub fn minus_Date_Date_LongInterval(left: Date, right: Date) -> LongInterval {
    let ld = left.to_dateTime();
    let rd = right.to_dateTime();
    let ly = ld.year();
    let lm = ld.month() as i32;
    let ry = rd.year();
    let rm = rd.month() as i32;
    LongInterval::from((ly - ry) * 12 + lm - rm)
}

pub fn minus_DateN_Date_LongIntervaNl(left: Option<Date>, right: Date) -> Option<LongInterval> {
    left.map(|x| LongInterval::new(x.days() - right.days()))
}

pub fn minus_Date_DateN_LongInterval(left: Date, right: Option<Date>) -> Option<LongInterval> {
    right.map(|x| LongInterval::new(left.days() - x.days()))
}

pub fn minus_DateN_DateN_LongIntervalN(
    left: Option<Date>,
    right: Option<Date>,
) -> Option<LongInterval> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(x), Some(y)) => Some(LongInterval::new(x.days() - y.days())),
    }
}

pub fn minus_Date_Date_ShortInterval(left: Date, right: Date) -> ShortInterval {
    let ld = left.days() as i64;
    let rd = right.days() as i64;
    ShortInterval::new((ld - rd) * 86400 * 1000)
}

pub fn minus_DateN_Date_ShortIntervalN(left: Option<Date>, right: Date) -> Option<ShortInterval> {
    left.map(|x| minus_Date_Date_ShortInterval(x, right))
}

pub fn minus_Date_DateN_ShortIntervalN(left: Date, right: Option<Date>) -> Option<ShortInterval> {
    right.map(|x| minus_Date_Date_ShortInterval(left, x))
}

pub fn minus_DateN_DateN_ShortIntervalN(
    left: Option<Date>,
    right: Option<Date>,
) -> Option<ShortInterval> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(x), Some(y)) => Some(minus_Date_Date_ShortInterval(x, y)),
    }
}

pub fn extract_year_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.year().into()
}

pub fn extract_year_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_year_Date)
}

pub fn extract_month_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.month().into()
}

pub fn extract_month_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_month_Date)
}

pub fn extract_day_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.day().into()
}

pub fn extract_day_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_day_Date)
}

pub fn extract_quarter_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    (date.month0() / 3 + 1).into()
}

pub fn extract_quarter_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_quarter_Date)
}

pub fn extract_decade_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    (date.year() / 10).into()
}

pub fn extract_decade_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_decade_Date)
}

pub fn extract_century_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    ((date.year() + 99) / 100).into()
}

pub fn extract_century_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_century_Date)
}

pub fn extract_millennium_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    ((date.year() + 999) / 1000).into()
}

pub fn extract_millennium_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_millennium_Date)
}

pub fn extract_isoyear_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.iso_week().year().into()
}

pub fn extract_isoyear_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_isoyear_Date)
}

pub fn extract_week_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.iso_week().week().into()
}

pub fn extract_week_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_week_Date)
}

pub fn extract_dow_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    (date.weekday().num_days_from_sunday() as i64) + Date::first_day_of_week()
}

pub fn extract_dow_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_dow_Date)
}

pub fn extract_isodow_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    (date.weekday().num_days_from_monday() as i64) + 1
}

pub fn extract_isodow_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_isodow_Date)
}

pub fn extract_doy_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.ordinal().into()
}

pub fn extract_doy_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_doy_Date)
}

pub fn extract_epoch_Date(value: Date) -> i64 {
    (value.days() as i64) * 86400
}

pub fn extract_epoch_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_epoch_Date)
}

pub fn extract_second_Date(_value: Date) -> i64 {
    0
}

pub fn extract_second_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_second_Date)
}

pub fn extract_minute_Date(_value: Date) -> i64 {
    0
}

pub fn extract_minute_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_minute_Date)
}

pub fn extract_hour_Date(_value: Date) -> i64 {
    0
}

pub fn extract_hour_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_hour_Date)
}

pub fn extract_millisecond_Date(_value: Date) -> i64 {
    0
}

pub fn extract_millisecond_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_millisecond_Date)
}

pub fn extract_microsecond_Date(_value: Date) -> i64 {
    0
}

pub fn extract_microsecond_DateN(value: Option<Date>) -> Option<i64> {
    value.map(extract_microsecond_Date)
}
