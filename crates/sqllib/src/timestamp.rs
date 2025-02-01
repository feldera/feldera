//! Support for SQL Timestamp and Date data types.

use crate::{
    array::Array,
    casts::*,
    interval::{LongInterval, ShortInterval},
    FromInteger, ToInteger,
};
use chrono::format::ParseErrorKind;
use chrono::{
    DateTime, Datelike, Days, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeZone,
    Timelike, Utc,
};
use core::fmt::Formatter;
use dbsp::num_entries_scalar;
use feldera_types::serde_with_context::{
    DateFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig, TimeFormat,
    TimestampFormat,
};
use num::PrimInt;
use serde::{de::Error as _, ser::Error as _, Deserialize, Deserializer, Serializer};
use size_of::SizeOf;
use std::{
    borrow::Cow,
    fmt::{self, Debug},
    ops::{Add, Sub},
};

use crate::{
    operators::{eq, gt, gte, lt, lte, neq},
    polymorphic_return_function2, some_existing_operator, some_function2, some_operator,
    some_polymorphic_function1, some_polymorphic_function2, some_polymorphic_function3,
};

/// Represents a date and a time without timezone information.
#[derive(
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Timestamp {
    // since unix epoch
    milliseconds: i64,
}

#[doc(hidden)]
impl ToInteger<i64> for Timestamp {
    #[doc(hidden)]
    fn to_integer(&self) -> i64 {
        self.milliseconds
    }
}

#[doc(hidden)]
impl FromInteger<i64> for Timestamp {
    #[doc(hidden)]
    fn from_integer(value: &i64) -> Self {
        Timestamp {
            milliseconds: *value,
        }
    }
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let datetime = DateTime::from_timestamp_millis(self.milliseconds).ok_or(fmt::Error)?;

        f.write_str(&datetime.format("%F %T%.f").to_string())
    }
}

impl SerializeWithContext<SqlSerdeConfig> for Timestamp {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.timestamp_format {
            TimestampFormat::Rfc3339 => {
                let datetime =
                    DateTime::from_timestamp_millis(self.milliseconds).ok_or_else(|| {
                        S::Error::custom(format!(
                            "timestamp value '{}' out of range",
                            self.milliseconds
                        ))
                    })?;

                serializer.serialize_str(&datetime.to_rfc3339())
            }
            TimestampFormat::String(format_string) => {
                let datetime =
                    DateTime::from_timestamp_millis(self.milliseconds).ok_or_else(|| {
                        S::Error::custom(format!(
                            "timestamp value '{}' out of range",
                            self.milliseconds
                        ))
                    })?;

                serializer.serialize_str(&datetime.format(format_string).to_string())
            }
            TimestampFormat::MillisSinceEpoch => serializer.serialize_i64(self.milliseconds),
            TimestampFormat::MicrosSinceEpoch => serializer.serialize_i64(self.milliseconds * 1000),
        }
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for Timestamp {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match config.timestamp_format {
            TimestampFormat::Rfc3339 => {
                let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

                let timestamp =
                    DateTime::parse_from_rfc3339(timestamp_str.trim()).map_err(|e| {
                        D::Error::custom(format!(
                            "invalid RFC3339 timestamp string '{timestamp_str}': {e}"
                        ))
                    })?;

                Ok(Self::new(timestamp.timestamp_millis()))
            }
            TimestampFormat::String(format) => {
                // `timestamp_str: &'de` doesn't work for JSON, which escapes strings
                // and can only deserialize into an owned string.
                let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

                let timestamp = NaiveDateTime::parse_from_str(timestamp_str.trim(), format)
                    .map_err(|e| {
                        D::Error::custom(format!("invalid timestamp string '{timestamp_str}': {e} (expected format: '{format}')"))
                    })?;

                Ok(Self::new(timestamp.and_utc().timestamp_millis()))
            }
            TimestampFormat::MillisSinceEpoch => {
                let millis: i64 = Deserialize::deserialize(deserializer)?;
                Ok(Self::new(millis))
            }
            TimestampFormat::MicrosSinceEpoch => {
                let micros: i64 = Deserialize::deserialize(deserializer)?;
                Ok(Self::new(micros / 1000))
            }
        }
    }
}

/// Deserialize timestamp from the `YYYY-MM-DD HH:MM:SS.fff` format.
/// For a flexible deserialization framework use deserialize_with_context.
impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `timestamp_str: &'de` doesn't work for JSON, which escapes strings
        // and can only deserialize into an owned string.
        let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

        let timestamp =
            NaiveDateTime::parse_from_str(timestamp_str.trim(), "%F %T%.f").map_err(|e| {
                D::Error::custom(format!(
                    "invalid timestamp string '{timestamp_str}': {e} (expected format: '%F %T%.f')"
                ))
            })?;

        Ok(Self::new(timestamp.and_utc().timestamp_millis()))
    }
}

impl Timestamp {
    /// Create a new [Timestamp] from a number of milliseconds since the
    /// Unix epoch (January 1, 1970)
    pub const fn new(milliseconds: i64) -> Self {
        Self { milliseconds }
    }

    /// Return the number of millliseconds elapsed since the Unix
    /// epoch (January 1st, 1970)
    pub fn milliseconds(&self) -> i64 {
        self.milliseconds
    }

    /// Convert a [Timestamp] to a chrono [DateTime] in the [Utc]
    /// timezone.
    pub fn to_dateTime(&self) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(self.milliseconds)
            .single()
            .unwrap_or_else(|| panic!("Could not convert timestamp {:?} to DateTime", self))
    }

    /// Convert a [Timestamp] to a chrono [NaiveDateTime]
    pub fn to_naiveDateTime(&self) -> NaiveDateTime {
        Utc.timestamp_millis_opt(self.milliseconds)
            .single()
            .unwrap_or_else(|| panic!("Could not convert timestamp {:?} to DateTime", self))
            .naive_utc()
    }

    /// Create a [Timestamp] from a chrono [DateTime] in the [Utc]
    /// timezone.
    // Cannot make this into a From trait because Rust reserved it
    pub fn from_dateTime(date: DateTime<Utc>) -> Self {
        Self {
            milliseconds: date.timestamp_millis(),
        }
    }

    /// Create a [Timestamp] from a chrono [NaiveDateTime].
    pub fn from_naiveDateTime(date: NaiveDateTime) -> Self {
        Self {
            milliseconds: date.and_utc().timestamp_millis(),
        }
    }

    /// Create a [Timestamp] from a chrono [NaiveDate].  The result
    /// represents the midnight of the start of the date.
    // Cannot make this into a From trait because Rust reserved it
    pub fn from_naiveDate(date: NaiveDate) -> Self {
        let date = Date::from_date(date);
        Timestamp::from_dateTime(date.to_dateTime())
    }

    /// Create a [Timestamp] from a chrono [Date].  The result
    /// represents th emidnight of the start of the date.
    pub fn from_date(date: Date) -> Self {
        Timestamp::from_dateTime(date.to_dateTime())
    }

    /// Get the [Date] part of a [Timestamp]
    pub fn get_date(&self) -> Date {
        // Is this right for negative timestamps?
        Date::new((self.milliseconds / 86_400_000i64) as i32)
    }

    /// Check if the timestamp is legal; panic if it isn't
    pub fn check_legal(&self) {
        let date = self.to_naiveDateTime();
        if date.year() < 1 || date.year() > 9999 {
            panic!("Timestamp out of range");
        }
    }
}

#[doc(hidden)]
pub fn now() -> Timestamp {
    Timestamp::from_dateTime(Utc::now())
}

impl<T> From<T> for Timestamp
where
    i64: From<T>,
    T: PrimInt,
{
    /// Convert a value expressing a number of milliseconds since the
    /// Unix epoch (January 1st, 1970) into a [Timestamp].
    fn from(value: T) -> Self {
        Self {
            milliseconds: i64::from(value),
        }
    }
}

impl Add<i64> for Timestamp {
    type Output = Self;

    /// Add a number of milliseconds to a [Timestamp].
    fn add(self, value: i64) -> Self {
        Self {
            milliseconds: self.milliseconds + value,
        }
    }
}

#[doc(hidden)]
pub fn plus_Timestamp_ShortInterval_Timestamp(left: Timestamp, right: ShortInterval) -> Timestamp {
    left.add(right.milliseconds())
}

polymorphic_return_function2!(
    plus,
    Timestamp,
    Timestamp,
    ShortInterval,
    ShortInterval,
    Timestamp,
    Timestamp
);

#[doc(hidden)]
pub fn plus_Timestamp_LongInterval_Timestamp(left: Timestamp, right: LongInterval) -> Timestamp {
    let dt = left.to_naiveDateTime();
    let months = right.months();
    let ndt = if months < 0 {
        dt.checked_sub_months(Months::new(-months as u32))
    } else {
        dt.checked_add_months(Months::new(months as u32))
    };
    Timestamp::from_naiveDateTime(ndt.unwrap())
}

polymorphic_return_function2!(
    plus,
    Timestamp,
    Timestamp,
    LongInterval,
    LongInterval,
    Timestamp,
    Timestamp
);

#[doc(hidden)]
pub fn minus_Timestamp_LongInterval_Timestamp(left: Timestamp, right: LongInterval) -> Timestamp {
    let dt = left.to_naiveDateTime();
    let months = right.months();
    let ndt = if months < 0 {
        dt.checked_add_months(Months::new(-months as u32))
    } else {
        dt.checked_sub_months(Months::new(months as u32))
    };
    Timestamp::from_naiveDateTime(ndt.unwrap())
}

polymorphic_return_function2!(
    minus,
    Timestamp,
    Timestamp,
    LongInterval,
    LongInterval,
    Timestamp,
    Timestamp
);

#[doc(hidden)]
pub fn plus_Date_ShortInterval_Timestamp(left: Date, right: ShortInterval) -> Timestamp {
    plus_Timestamp_ShortInterval_Timestamp(cast_to_Timestamp_Date(left).unwrap(), right)
}

polymorphic_return_function2!(
    plus,
    Date,
    Date,
    ShortInterval,
    ShortInterval,
    Timestamp,
    Timestamp
);

#[doc(hidden)]
pub fn minus_Timestamp_Timestamp_ShortInterval(left: Timestamp, right: Timestamp) -> ShortInterval {
    ShortInterval::from(left.milliseconds() - right.milliseconds())
}

polymorphic_return_function2!(
    minus,
    Timestamp,
    Timestamp,
    Timestamp,
    Timestamp,
    ShortInterval,
    ShortInterval
);

#[doc(hidden)]
pub fn minus_Time_Time_ShortInterval(left: Time, right: Time) -> ShortInterval {
    ShortInterval::from((left.nanoseconds() as i64 - right.nanoseconds() as i64) / 1000000)
}

polymorphic_return_function2!(minus, Time, Time, Time, Time, ShortInterval, ShortInterval);

#[doc(hidden)]
pub fn minus_Timestamp_ShortInterval_Timestamp(left: Timestamp, right: ShortInterval) -> Timestamp {
    Timestamp::new(left.milliseconds() - right.milliseconds())
}

polymorphic_return_function2!(
    minus,
    Timestamp,
    Timestamp,
    ShortInterval,
    ShortInterval,
    Timestamp,
    Timestamp
);

#[doc(hidden)]
pub fn minus_Date_ShortInterval_Timestamp(left: Date, right: ShortInterval) -> Timestamp {
    minus_Timestamp_ShortInterval_Timestamp(cast_to_Timestamp_Date(left).unwrap(), right)
}

polymorphic_return_function2!(
    minus,
    Date,
    Date,
    ShortInterval,
    ShortInterval,
    Timestamp,
    Timestamp
);

#[doc(hidden)]
pub fn plus_Date_ShortInterval_Date(left: Date, right: ShortInterval) -> Date {
    let days = (right.milliseconds() / (86400 * 1000)) as i32;
    let diff = left.days() + days;
    Date::new(diff)
}

polymorphic_return_function2!(plus, Date, Date, ShortInterval, ShortInterval, Date, Date);

#[doc(hidden)]
pub fn minus_Date_ShortInterval_Date(left: Date, right: ShortInterval) -> Date {
    let days = (right.milliseconds() / (86400 * 1000)) as i32;
    let diff = left.days() - days;
    Date::new(diff)
}

polymorphic_return_function2!(minus, Date, Date, ShortInterval, ShortInterval, Date, Date);

#[doc(hidden)]
pub fn plus_Date_LongInterval_Date(left: Date, right: LongInterval) -> Date {
    let date = left.to_date();
    if right.months() < 0 {
        let result = date
            .checked_sub_months(Months::new(-right.months() as u32))
            .unwrap_or_else(|| panic!("Cannot add interval '{:?}' to date '{:?}'", right, date));
        Date::from_date(result)
    } else {
        let result = date
            .checked_add_months(Months::new(right.months() as u32))
            .unwrap_or_else(|| panic!("Cannot add interval '{:?}' to date '{:?}'", right, date));
        Date::from_date(result)
    }
}

polymorphic_return_function2!(plus, Date, Date, LongInterval, LongInterval, Date, Date);

#[doc(hidden)]
pub fn minus_Date_LongInterval_Date(left: Date, right: LongInterval) -> Date {
    let date = left.to_date();
    if right.months() < 0 {
        let result = date
            .checked_add_months(Months::new(-right.months() as u32))
            .unwrap_or_else(|| {
                panic!(
                    "Cannot subtract interval '{:?}' from date '{:?}'",
                    right, date
                )
            });
        Date::from_date(result)
    } else {
        let result = date
            .checked_sub_months(Months::new(right.months() as u32))
            .unwrap_or_else(|| {
                panic!(
                    "Cannot subtract interval '{:?}' from date '{:?}'",
                    right, date
                )
            });
        Date::from_date(result)
    }
}

polymorphic_return_function2!(minus, Date, Date, LongInterval, LongInterval, Date, Date);

#[doc(hidden)]
pub fn minus_Timestamp_Timestamp_LongInterval(left: Timestamp, right: Timestamp) -> LongInterval {
    let swap = left < right;
    let ldate;
    let rdate;
    if swap {
        rdate = left.to_dateTime();
        ldate = right.to_dateTime();
    } else {
        ldate = left.to_dateTime();
        rdate = right.to_dateTime();
    }
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
    let months = (ly - ry) * 12 + lm - rm;
    LongInterval::from(if swap { -months } else { months })
}

polymorphic_return_function2!(
    minus,
    Timestamp,
    Timestamp,
    Timestamp,
    Timestamp,
    LongInterval,
    LongInterval
);

#[doc(hidden)]
pub fn minus_Date_Timestamp_LongInterval(left: Date, right: Timestamp) -> LongInterval {
    minus_Timestamp_Timestamp_LongInterval(cast_to_Timestamp_Date(left).unwrap(), right)
}

polymorphic_return_function2!(
    minus,
    Date,
    Date,
    Timestamp,
    Timestamp,
    LongInterval,
    LongInterval
);

#[doc(hidden)]
pub fn minus_Timestamp_Date_LongInterval(left: Timestamp, right: Date) -> LongInterval {
    minus_Timestamp_Timestamp_LongInterval(left, cast_to_Timestamp_Date(right).unwrap())
}

polymorphic_return_function2!(
    minus,
    Timestamp,
    Timestamp,
    Date,
    Date,
    LongInterval,
    LongInterval
);

#[doc(hidden)]
pub fn extract_year_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.year().into()
}

#[doc(hidden)]
pub fn extract_month_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.month().into()
}

#[doc(hidden)]
pub fn extract_day_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.day().into()
}

#[doc(hidden)]
pub fn extract_quarter_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.month0() / 3 + 1).into()
}

#[doc(hidden)]
pub fn extract_decade_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.year() / 10).into()
}

#[doc(hidden)]
pub fn extract_century_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    ((date.year() + 99) / 100).into()
}

#[doc(hidden)]
pub fn extract_millennium_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    ((date.year() + 999) / 1000).into()
}

#[doc(hidden)]
pub fn extract_isoyear_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.iso_week().year().into()
}

#[doc(hidden)]
pub fn extract_week_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.iso_week().week().into()
}

#[doc(hidden)]
pub fn extract_dow_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.weekday().num_days_from_sunday() as i64) + Date::first_day_of_week()
}

#[doc(hidden)]
pub fn extract_isodow_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.weekday().num_days_from_monday() as i64) + 1
}

#[doc(hidden)]
pub fn extract_doy_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.ordinal().into()
}

#[doc(hidden)]
pub fn extract_epoch_Timestamp(t: Timestamp) -> i64 {
    t.milliseconds() / 1000
}

#[doc(hidden)]
pub fn extract_millisecond_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.second() * 1000 + date.timestamp_subsec_millis()).into()
}

#[doc(hidden)]
pub fn extract_microsecond_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    // This is now what you expect due to CALCITE-5919
    (date.second() * 1_000_000 + date.timestamp_subsec_millis() * 1000).into()
}

#[doc(hidden)]
pub fn extract_second_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.second().into()
}

#[doc(hidden)]
pub fn extract_minute_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.minute().into()
}

#[doc(hidden)]
pub fn extract_hour_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.hour().into()
}

some_polymorphic_function1!(extract_year, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_month, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_day, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_quarter, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_decade, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_century, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_millennium, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_isoyear, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_week, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_dow, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_isodow, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_doy, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_epoch, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_millisecond, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_microsecond, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_second, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_minute, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_hour, Timestamp, Timestamp, i64);

some_operator!(lt, Timestamp, Timestamp, bool);
some_operator!(gt, Timestamp, Timestamp, bool);
some_operator!(eq, Timestamp, Timestamp, bool);
some_operator!(neq, Timestamp, Timestamp, bool);
some_operator!(gte, Timestamp, Timestamp, bool);
some_operator!(lte, Timestamp, Timestamp, bool);
/*
some_operator!(lt, Timestamp, &Timestamp, bool);
some_operator!(gt, Timestamp, &Timestamp, bool);
some_operator!(eq, Timestamp, &Timestamp, bool);
some_operator!(neq, Timestamp, &Timestamp, bool);
some_operator!(gte, Timestamp, &Timestamp, bool);
some_operator!(lte, Timestamp, &Timestamp, bool);
*/

#[doc(hidden)]
pub fn floor_week_Timestamp(value: Timestamp) -> Timestamp {
    let wd = extract_dow_Timestamp(value) - Date::first_day_of_week();
    let ts = value.to_dateTime();
    let notime = ts
        .with_hour(0)
        .unwrap_or_else(|| panic!("Cannot clear hour of timestamp '{:?}'", value))
        .with_minute(0)
        .unwrap_or_else(|| panic!("Cannot clear minute of timestamp '{:?}'", value))
        .with_second(0)
        .unwrap_or_else(|| panic!("Cannot clear second of timestamp '{:?}'", value))
        .with_nanosecond(0)
        .unwrap_or_else(|| panic!("Cannot clear nanosecond of timestamp '{:?}'", value));
    let notimeTs = Timestamp::from_dateTime(notime);
    let interval = ShortInterval::new(wd * 86400 * 1000);

    minus_Timestamp_ShortInterval_Timestamp(notimeTs, interval)
}

some_polymorphic_function1!(floor_week, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn tumble_Timestamp_ShortInterval(ts: Timestamp, i: ShortInterval) -> Timestamp {
    let ts_ms = ts.milliseconds();
    let i_ms = i.milliseconds();
    let round = ts_ms - ts_ms % i_ms;
    Timestamp::new(round)
}

some_polymorphic_function2!(
    tumble,
    Timestamp,
    Timestamp,
    ShortInterval,
    ShortInterval,
    Timestamp
);

#[doc(hidden)]
pub fn tumble_Timestamp_ShortInterval_Time(ts: Timestamp, i: ShortInterval, t: Time) -> Timestamp {
    let t_ms = (t.nanoseconds() / 1000000) as i64;
    let ts_ms = ts.milliseconds() - t_ms;
    let i_ms = i.milliseconds();
    let round = ts_ms - ts_ms % i_ms;
    Timestamp::new(round + t_ms)
}

some_polymorphic_function3!(
    tumble,
    Timestamp,
    Timestamp,
    ShortInterval,
    ShortInterval,
    Time,
    Time,
    Timestamp
);

#[doc(hidden)]
pub fn tumble_Timestamp_ShortInterval_ShortInterval(
    ts: Timestamp,
    i: ShortInterval,
    t: ShortInterval,
) -> Timestamp {
    let t_ms = t.milliseconds();
    let ts_ms = ts.milliseconds() - t_ms;
    let i_ms = i.milliseconds();
    let round = ts_ms - ts_ms % i_ms;
    Timestamp::new(round + t_ms)
}

some_polymorphic_function3!(
    tumble,
    Timestamp,
    Timestamp,
    ShortInterval,
    ShortInterval,
    ShortInterval,
    ShortInterval,
    Timestamp
);

// Start of first interval which contains ts
#[doc(hidden)]
pub fn hop_start(
    ts: Timestamp,
    period: ShortInterval,
    size: ShortInterval,
    start: ShortInterval,
) -> i64 {
    let ts_ms = ts.milliseconds();
    let size_ms = size.milliseconds();
    let period_ms = period.milliseconds();
    let start_ms = start.milliseconds();
    ts_ms - ((ts_ms - start_ms) % period_ms) + period_ms - size_ms
}

// Helper function used by the monotonicity analysis for hop table functions
#[doc(hidden)]
pub fn hop_start_timestamp(
    ts: Timestamp,
    period: ShortInterval,
    size: ShortInterval,
    start: ShortInterval,
) -> Timestamp {
    Timestamp::new(hop_start(ts, period, size, start))
}

#[doc(hidden)]
pub fn hop_Timestamp_ShortInterval_ShortInterval_ShortInterval(
    ts: Timestamp,
    period: ShortInterval,
    size: ShortInterval,
    start: ShortInterval,
) -> Array<Timestamp> {
    let mut result = Vec::<Timestamp>::new();
    let round = hop_start(ts, period, size, start);
    let mut add = 0;
    while add < size.milliseconds() {
        result.push(Timestamp::new(round + add));
        add += period.milliseconds();
    }
    result.into()
}

#[doc(hidden)]
pub fn hop_TimestampN_ShortInterval_ShortInterval_ShortInterval(
    ts: Option<Timestamp>,
    period: ShortInterval,
    size: ShortInterval,
    start: ShortInterval,
) -> Array<Timestamp> {
    match ts {
        None => Vec::new().into(),
        Some(ts) => {
            hop_Timestamp_ShortInterval_ShortInterval_ShortInterval(ts, period, size, start)
        }
    }
}

#[doc(hidden)]
pub fn parse_timestamp__(format: String, st: String) -> Option<Timestamp> {
    let nt = NaiveDateTime::parse_from_str(&st, &format);
    match nt {
        Ok(nt) => Some(Timestamp::from_naiveDateTime(nt)),
        Err(e) => match e.kind() {
            ParseErrorKind::BadFormat | ParseErrorKind::NotEnough | ParseErrorKind::Impossible => {
                panic!("Invalid format in PARSE_TIMESTAMP")
            }
            _ => None,
        },
    }
}

pub fn parse_timestampN_(format: Option<String>, st: String) -> Option<Timestamp> {
    let format = format?;
    parse_timestamp__(format, st)
}

pub fn parse_timestamp_N(format: String, st: Option<String>) -> Option<Timestamp> {
    let st = st?;
    parse_timestamp__(format, st)
}

pub fn parse_timestampNN(format: Option<String>, st: Option<String>) -> Option<Timestamp> {
    let st = st?;
    let format = format?;
    parse_timestamp__(format, st)
}

//////////////////////////// Date

/// A representation of a Date in the Gregorian calendar.
/// The range of legal dates is 0001-01-01 to 9999-12-12.
#[derive(
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Date {
    // since unix epoch
    days: i32,
}

impl Date {
    /// Create a [Date] from a number of days since the Unix epoch
    /// (January 1st, 1970).
    pub const fn new(days: i32) -> Self {
        Self { days }
    }

    /// Create a [Date] from a chrono [NaiveDate]
    pub fn from_date(date: NaiveDate) -> Self {
        Self {
            days: (date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32,
        }
    }

    /// The number of days in this [Date] since the Unix epoch
    /// (January 1, 1970).  The result will be negative for dates
    /// before 1970.
    pub fn days(&self) -> i32 {
        self.days
    }

    /// Convert a [Date] to a [Timestamp].  The time in the
    /// [Timestamp] is midnight at the start of the date.
    pub fn to_timestamp(&self) -> Timestamp {
        Timestamp::new((self.days as i64) * 86400 * 1000)
    }

    /// Convert a [Date] to a chrono [DateTime] in the [Utc] timezone,
    /// representing the midnight at the start of the date.
    pub fn to_dateTime(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.days() as i64 * 86400, 0)
            .single()
            .unwrap()
    }

    /// Convert a [Date] to a chrono [NaiveDate]
    pub fn to_date(&self) -> NaiveDate {
        let nd = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        if self.days >= 0 {
            nd.checked_add_days(Days::new(self.days as u64)).unwrap()
        } else {
            nd.checked_sub_days(Days::new((-self.days) as u64)).unwrap()
        }
    }

    #[doc(hidden)]
    pub fn first_day_of_week() -> i64 {
        // This should depend on the SQL dialect, but the calcite
        // optimizer seems to imply this for all dialects.
        1
    }
}

impl fmt::Debug for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let millis = (self.days as i64) * 86400 * 1000;
        let ndt = DateTime::from_timestamp_millis(millis);
        match ndt {
            Some(ndt) => ndt.date_naive().fmt(f),
            _ => write!(f, "date value '{}' out of range", self.days),
        }
    }
}

impl<T> From<T> for Date
where
    i32: From<T>,
    T: PrimInt,
{
    /// Convert an integer representing the number of days since the
    /// Unix epoch (January 1st, 1970) to a [Date].
    fn from(value: T) -> Self {
        Self {
            days: i32::from(value),
        }
    }
}

#[doc(hidden)]
impl ToInteger<i32> for Date {
    #[doc(hidden)]
    fn to_integer(&self) -> i32 {
        self.days()
    }
}

#[doc(hidden)]
impl FromInteger<i32> for Date {
    #[doc(hidden)]
    fn from_integer(value: &i32) -> Self {
        Self { days: *value }
    }
}

impl SerializeWithContext<SqlSerdeConfig> for Date {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.date_format {
            DateFormat::String(format_string) => {
                let millis = (self.days as i64) * 86400 * 1000;
                let datetime = DateTime::from_timestamp_millis(millis).ok_or_else(|| {
                    S::Error::custom(format!("date value '{}' out of range", self.days))
                })?;

                serializer.serialize_str(&datetime.format(format_string).to_string())
            }
            DateFormat::DaysSinceEpoch => serializer.serialize_i32(self.days),
        }
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for Date {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match config.date_format {
            DateFormat::String(format) => {
                let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

                let date = NaiveDate::parse_from_str(str.trim(), format).map_err(|e| {
                    D::Error::custom(format!(
                        "invalid date string '{str}': {e} (expected format: '{format}')"
                    ))
                })?;
                Ok(Self::new(
                    (date.and_time(NaiveTime::default()).and_utc().timestamp() / 86400) as i32,
                ))
            }
            DateFormat::DaysSinceEpoch => Ok(Self {
                days: i32::deserialize(deserializer)?,
            }),
        }
    }
}

/// Deserialize date from the `YYYY-MM-DD` format.
impl<'de> Deserialize<'de> for Date {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        let date = NaiveDate::parse_from_str(str.trim(), "%Y-%m-%d").map_err(|e| {
            D::Error::custom(format!(
                "invalid date string '{str}': {e} (expected format: '%Y-%m-%d')"
            ))
        })?;
        Ok(Self::new(
            (date.and_time(NaiveTime::default()).and_utc().timestamp() / 86400) as i32,
        ))
    }
}

some_operator!(lt, Date, Date, bool);
some_operator!(gt, Date, Date, bool);
some_operator!(eq, Date, Date, bool);
some_operator!(neq, Date, Date, bool);
some_operator!(gte, Date, Date, bool);
some_operator!(lte, Date, Date, bool);
/*
some_operator!(lt, Date, &Date, bool);
some_operator!(gt, Date, &Date, bool);
some_operator!(eq, Date, &Date, bool);
some_operator!(neq, Date, &Date, bool);
some_operator!(gte, Date, &Date, bool);
some_operator!(lte, Date, &Date, bool);
*/

// right - left
#[doc(hidden)]
pub fn minus_Date_Date_LongInterval(left: Date, right: Date) -> LongInterval {
    // Logic adapted from https://github.com/mysql/mysql-server/blob/ea1efa9822d81044b726aab20c857d5e1b7e046a/sql/item_timefunc.cc
    let ld = left.to_dateTime();
    let rd = right.to_dateTime();

    let (beg, end, neg) = if ld > rd { (rd, ld, 1) } else { (ld, rd, -1) };

    let month_end = end.month() as i32;
    let month_beg = beg.month() as i32;
    let day_end = end.day();
    let day_beg = beg.day();

    // compute years
    let mut years = end.year() - beg.year();
    let adjust = month_end < month_beg || (month_end == month_beg && day_end < day_beg);
    if adjust {
        years -= 1;
    }

    // compute months
    let mut months = 12 * years;
    if adjust {
        months += 12 - (month_beg - month_end);
    } else {
        months += month_end - month_beg;
    }

    if day_end < day_beg {
        months -= 1;
    }

    LongInterval::from(months * neg)
}

polymorphic_return_function2!(minus, Date, Date, Date, Date, LongInterval, LongInterval);

#[doc(hidden)]
pub fn minus_Date_Date_ShortInterval(left: Date, right: Date) -> ShortInterval {
    let ld = left.days() as i64;
    let rd = right.days() as i64;
    ShortInterval::new((ld - rd) * 86400 * 1000)
}

polymorphic_return_function2!(minus, Date, Date, Date, Date, ShortInterval, ShortInterval);

#[doc(hidden)]
pub fn extract_year_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.year().into()
}

#[doc(hidden)]
pub fn extract_month_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.month().into()
}

#[doc(hidden)]
pub fn extract_day_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.day().into()
}

#[doc(hidden)]
pub fn extract_quarter_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    (date.month0() / 3 + 1).into()
}

#[doc(hidden)]
pub fn extract_decade_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    (date.year() / 10).into()
}

#[doc(hidden)]
pub fn extract_century_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    ((date.year() + 99) / 100).into()
}

#[doc(hidden)]
pub fn extract_millennium_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    ((date.year() + 999) / 1000).into()
}

#[doc(hidden)]
pub fn extract_isoyear_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.iso_week().year().into()
}

#[doc(hidden)]
pub fn extract_week_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.iso_week().week().into()
}

#[doc(hidden)]
pub fn extract_dow_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    (date.weekday().num_days_from_sunday() as i64) + Date::first_day_of_week()
}

#[doc(hidden)]
pub fn extract_isodow_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    (date.weekday().num_days_from_monday() as i64) + 1
}

#[doc(hidden)]
pub fn extract_doy_Date(value: Date) -> i64 {
    let date = value.to_dateTime();
    date.ordinal().into()
}

#[doc(hidden)]
pub fn extract_epoch_Date(value: Date) -> i64 {
    (value.days() as i64) * 86400
}

#[doc(hidden)]
pub fn extract_second_Date(_value: Date) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_minute_Date(_value: Date) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_hour_Date(_value: Date) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_millisecond_Date(_value: Date) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_microsecond_Date(_value: Date) -> i64 {
    0
}

some_polymorphic_function1!(extract_year, Date, Date, i64);
some_polymorphic_function1!(extract_month, Date, Date, i64);
some_polymorphic_function1!(extract_day, Date, Date, i64);
some_polymorphic_function1!(extract_quarter, Date, Date, i64);
some_polymorphic_function1!(extract_decade, Date, Date, i64);
some_polymorphic_function1!(extract_century, Date, Date, i64);
some_polymorphic_function1!(extract_millennium, Date, Date, i64);
some_polymorphic_function1!(extract_isoyear, Date, Date, i64);
some_polymorphic_function1!(extract_week, Date, Date, i64);
some_polymorphic_function1!(extract_dow, Date, Date, i64);
some_polymorphic_function1!(extract_isodow, Date, Date, i64);
some_polymorphic_function1!(extract_doy, Date, Date, i64);
some_polymorphic_function1!(extract_epoch, Date, Date, i64);
some_polymorphic_function1!(extract_millisecond, Date, Date, i64);
some_polymorphic_function1!(extract_microsecond, Date, Date, i64);
some_polymorphic_function1!(extract_second, Date, Date, i64);
some_polymorphic_function1!(extract_minute, Date, Date, i64);
some_polymorphic_function1!(extract_hour, Date, Date, i64);

#[doc(hidden)]
pub fn datediff_day_Date_Date(left: Date, right: Date) -> i32 {
    left.days() - right.days()
}

some_polymorphic_function2!(datediff_day, Date, Date, Date, Date, i32);

#[doc(hidden)]
pub fn format_date__(format: String, date: Date) -> String {
    date.to_dateTime().format(&format).to_string()
}

some_function2!(format_date, String, Date, String);

#[doc(hidden)]
pub fn parse_date__(format: String, st: String) -> Option<Date> {
    let nd = NaiveDate::parse_from_str(&st, &format);
    match nd {
        Ok(nd) => Some(Date::from_date(nd)),
        Err(e) => match e.kind() {
            ParseErrorKind::BadFormat | ParseErrorKind::NotEnough | ParseErrorKind::Impossible => {
                panic!("Invalid format in PARSE_DATE")
            }
            _ => None,
        },
    }
}

pub fn parse_dateN_(format: Option<String>, st: String) -> Option<Date> {
    let format = format?;
    parse_date__(format, st)
}

pub fn parse_date_N(format: String, st: Option<String>) -> Option<Date> {
    let st = st?;
    parse_date__(format, st)
}

pub fn parse_dateNN(format: Option<String>, st: Option<String>) -> Option<Date> {
    let st = st?;
    let format = format?;
    parse_date__(format, st)
}

#[doc(hidden)]
pub fn date_trunc_millennium_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_millennium_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_century_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_century_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_decade_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_decade_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_year_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_year_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_month_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_month_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_week_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_week_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_day_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_day_Date(dt);
    Timestamp::from_date(dt)
}

some_polymorphic_function1!(date_trunc_millennium, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_century, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_decade, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_year, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_month, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_week, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_day, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn timestamp_trunc_millennium_Timestamp(timestamp: Timestamp) -> Timestamp {
    date_trunc_millennium_Timestamp(timestamp)
}

#[doc(hidden)]
pub fn timestamp_trunc_century_Timestamp(timestamp: Timestamp) -> Timestamp {
    date_trunc_century_Timestamp(timestamp)
}

#[doc(hidden)]
pub fn timestamp_trunc_decade_Timestamp(timestamp: Timestamp) -> Timestamp {
    date_trunc_decade_Timestamp(timestamp)
}

#[doc(hidden)]
pub fn timestamp_trunc_year_Timestamp(timestamp: Timestamp) -> Timestamp {
    date_trunc_year_Timestamp(timestamp)
}

#[doc(hidden)]
pub fn timestamp_trunc_month_Timestamp(timestamp: Timestamp) -> Timestamp {
    date_trunc_month_Timestamp(timestamp)
}

#[doc(hidden)]
pub fn timestamp_trunc_week_Timestamp(timestamp: Timestamp) -> Timestamp {
    date_trunc_week_Timestamp(timestamp)
}

#[doc(hidden)]
pub fn timestamp_trunc_day_Timestamp(timestamp: Timestamp) -> Timestamp {
    date_trunc_day_Timestamp(timestamp)
}

#[doc(hidden)]
pub fn timestamp_trunc_hour_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.to_dateTime();
    let nd = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
        .unwrap()
        .and_hms_opt(dt.hour(), 0, 0)
        .unwrap();
    Timestamp::from_naiveDateTime(nd)
}

#[doc(hidden)]
pub fn timestamp_trunc_minute_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.to_dateTime();
    let nd = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
        .unwrap()
        .and_hms_opt(dt.hour(), dt.minute(), 0)
        .unwrap();
    Timestamp::from_naiveDateTime(nd)
}

#[doc(hidden)]
pub fn timestamp_trunc_second_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.to_dateTime();
    let nd = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
        .unwrap()
        .and_hms_opt(dt.hour(), dt.minute(), dt.second())
        .unwrap();
    Timestamp::from_naiveDateTime(nd)
}

some_polymorphic_function1!(timestamp_trunc_millennium, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_century, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_decade, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_year, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_month, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_week, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_day, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_hour, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_minute, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(timestamp_trunc_second, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn date_trunc_millennium_Date(date: Date) -> Date {
    let m = extract_millennium_Date(date) - 1;
    let rounded = NaiveDate::from_ymd_opt((m as i32) * 1000 + 1, 1, 1).unwrap();
    Date::from_date(rounded)
}

#[doc(hidden)]
pub fn date_trunc_century_Date(date: Date) -> Date {
    let c = extract_century_Date(date) - 1;
    let rounded = NaiveDate::from_ymd_opt((c as i32) * 100 + 1, 1, 1).unwrap();
    Date::from_date(rounded)
}

#[doc(hidden)]
pub fn date_trunc_decade_Date(date: Date) -> Date {
    let date = date.to_dateTime();
    let rounded = NaiveDate::from_ymd_opt((date.year() / 10) * 10, 1, 1).unwrap();
    Date::from_date(rounded)
}

#[doc(hidden)]
pub fn date_trunc_year_Date(date: Date) -> Date {
    let date = date.to_dateTime();
    let rounded = NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap();
    Date::from_date(rounded)
}

#[doc(hidden)]
pub fn date_trunc_month_Date(date: Date) -> Date {
    let date = date.to_dateTime();
    let rounded = NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap();
    Date::from_date(rounded)
}

#[doc(hidden)]
pub fn date_trunc_week_Date(date: Date) -> Date {
    let naive = date.to_date();
    let weekday = naive.weekday().num_days_from_sunday();
    let rounded = naive - Duration::days(weekday.into());
    Date::from_date(rounded)
}

#[doc(hidden)]
pub fn date_trunc_day_Date(date: Date) -> Date {
    date
}

some_polymorphic_function1!(date_trunc_millennium, Date, Date, Date);
some_polymorphic_function1!(date_trunc_century, Date, Date, Date);
some_polymorphic_function1!(date_trunc_decade, Date, Date, Date);
some_polymorphic_function1!(date_trunc_year, Date, Date, Date);
some_polymorphic_function1!(date_trunc_month, Date, Date, Date);
some_polymorphic_function1!(date_trunc_week, Date, Date, Date);
some_polymorphic_function1!(date_trunc_day, Date, Date, Date);

//////////////////////////// Time

/// A time within a day, with nanoseconds precision, without timezone
/// information.  The legal range of time values is 00:00:00 to
/// 23:59:59.999999999.  Times are always positive.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Time {
    nanoseconds: u64,
}

#[doc(hidden)]
impl ToInteger<u64> for Time {
    fn to_integer(&self) -> u64 {
        self.nanoseconds
    }
}

#[doc(hidden)]
impl ToInteger<i64> for Time {
    fn to_integer(&self) -> i64 {
        self.nanoseconds as i64
    }
}

impl FromInteger<u64> for Time {
    /// Create a [Time] from a number of nanoseconds since midnight.
    fn from_integer(value: &u64) -> Self {
        Self {
            nanoseconds: *value,
        }
    }
}

impl FromInteger<i64> for Time {
    /// Create a [Time] from a positive number of nanoseconds since
    /// midnight.
    fn from_integer(value: &i64) -> Self {
        Self {
            nanoseconds: *value as u64,
        }
    }
}

#[doc(hidden)]
const BILLION: u64 = 1_000_000_000;

impl Time {
    /// Create a [Time] from a number of nanoseconds since midnight.
    pub const fn new(nanoseconds: u64) -> Self {
        Self { nanoseconds }
    }

    #[doc(hidden)]
    pub fn nanoseconds(&self) -> u64 {
        self.nanoseconds
    }

    /// Create a [Time] from a chrono [NaiveTime].
    pub fn from_time(time: NaiveTime) -> Self {
        Self {
            nanoseconds: (time.num_seconds_from_midnight() as u64) * BILLION
                + (time.nanosecond() as u64),
        }
    }

    /// Convert a [Time] to a chrono [NaiveTime].
    pub fn to_time(&self) -> NaiveTime {
        NaiveTime::from_num_seconds_from_midnight_opt(
            (self.nanoseconds / BILLION) as u32,
            (self.nanoseconds % BILLION) as u32,
        )
        .unwrap()
    }

    /// Time that represents the duration of a day
    pub const ONE_DAY: Time = Time {
        nanoseconds: 86_400_000_000_000, // Total nanoseconds in a day
    };
}

impl Add<ShortInterval> for Time {
    type Output = Self;

    #[doc(hidden)]
    fn add(self, rhs: ShortInterval) -> Self::Output {
        let nanos_in_day = Time::ONE_DAY.nanoseconds() as i64;

        let time = self.nanoseconds() as i64 + nanos_in_day;
        let int = rhs.nanoseconds() % nanos_in_day;

        let rem = (time + int) % nanos_in_day;

        Time::new(rem.unsigned_abs())
    }
}

impl Sub<ShortInterval> for Time {
    type Output = Self;

    #[doc(hidden)]
    fn sub(self, rhs: ShortInterval) -> Self::Output {
        self + (ShortInterval::new(-rhs.milliseconds()))
    }
}

impl<T> From<T> for Time
where
    u64: From<T>,
    T: PrimInt,
{
    /// Create a [Time] from an unsigned number of nanoseconds since
    /// midnight.
    fn from(value: T) -> Self {
        Self {
            nanoseconds: u64::from(value),
        }
    }
}

impl SerializeWithContext<SqlSerdeConfig> for Time {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.time_format {
            TimeFormat::String(format_string) => {
                let time = self.to_time();
                serializer.serialize_str(&time.format(format_string).to_string())
            }
            TimeFormat::Nanos => serializer.serialize_u64(self.nanoseconds),
            TimeFormat::NanosSigned => serializer.serialize_i64(self.nanoseconds as i64),
            TimeFormat::Micros => serializer.serialize_u64(self.nanoseconds / 1_000),
            TimeFormat::Millis => serializer.serialize_u64(self.nanoseconds / 1_000_000),
        }
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for Time {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match config.time_format {
            TimeFormat::String(format) => {
                let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
                let time = NaiveTime::parse_from_str(str.trim(), format).map_err(|e| {
                    D::Error::custom(format!(
                        "invalid time string '{str}': {e} (expected format: '{format}')"
                    ))
                })?;
                Ok(Self::from_time(time))
            }
            TimeFormat::Nanos => Ok(Self {
                nanoseconds: u64::deserialize(deserializer)?,
            }),
            TimeFormat::NanosSigned => Ok(Self {
                nanoseconds: i64::deserialize(deserializer)? as u64,
            }),
            TimeFormat::Micros => Ok(Self {
                nanoseconds: u64::deserialize(deserializer)? * 1_000,
            }),
            TimeFormat::Millis => Ok(Self {
                nanoseconds: u64::deserialize(deserializer)? * 1_000_000,
            }),
        }
    }
}

impl<'de> Deserialize<'de> for Time {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: &'de str = Deserialize::deserialize(deserializer)?;
        let time = NaiveTime::parse_from_str(str.trim(), "%H:%M:%S%.f").map_err(|e| {
            D::Error::custom(format!(
                "invalid time string '{str}': {e} (expected format: '%H:%M:%S%.f')"
            ))
        })?;
        Ok(Self::from_time(time))
    }
}

some_operator!(lt, Time, Time, bool);
some_operator!(gt, Time, Time, bool);
some_operator!(eq, Time, Time, bool);
some_operator!(neq, Time, Time, bool);
some_operator!(gte, Time, Time, bool);
some_operator!(lte, Time, Time, bool);
/*
some_operator!(lt, Time, &Time, bool);
some_operator!(gt, Time, &Time, bool);
some_operator!(eq, Time, &Time, bool);
some_operator!(neq, Time, &Time, bool);
some_operator!(gte, Time, &Time, bool);
some_operator!(lte, Time, &Time, bool);
*/

#[doc(hidden)]
pub fn extract_millisecond_Time(value: Time) -> i64 {
    let time = value.to_time();
    (time.second() * 1000 + time.nanosecond() / 1_000_000).into()
}

#[doc(hidden)]
pub fn extract_microsecond_Time(value: Time) -> i64 {
    let time = value.to_time();
    // not what you expect due to CALCITE-5919
    (time.second() * 1_000_000 + (time.nanosecond() / 1000000) * 1000).into()
}

#[doc(hidden)]
pub fn extract_second_Time(value: Time) -> i64 {
    let time = value.to_time();
    time.second().into()
}

#[doc(hidden)]
pub fn extract_minute_Time(value: Time) -> i64 {
    let time = value.to_time();
    time.minute().into()
}

#[doc(hidden)]
pub fn extract_hour_Time(value: Time) -> i64 {
    let time = value.to_time();
    time.hour().into()
}

#[doc(hidden)]
pub fn parse_time__(format: String, st: String) -> Option<Time> {
    let nt = NaiveTime::parse_from_str(&st, &format);
    match nt {
        Ok(nt) => Some(Time::from_time(nt)),
        Err(e) => match e.kind() {
            ParseErrorKind::BadFormat | ParseErrorKind::NotEnough | ParseErrorKind::Impossible => {
                panic!("Invalid format in PARSE_TIME")
            }
            _ => None,
        },
    }
}

pub fn parse_timeN_(format: Option<String>, st: String) -> Option<Time> {
    let format = format?;
    parse_time__(format, st)
}

pub fn parse_time_N(format: String, st: Option<String>) -> Option<Time> {
    let st = st?;
    parse_time__(format, st)
}

pub fn parse_timeNN(format: Option<String>, st: Option<String>) -> Option<Time> {
    let st = st?;
    let format = format?;
    parse_time__(format, st)
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use super::{Date, Time, Timestamp};
    use feldera_types::format::json::JsonFlavor;
    use feldera_types::serde_with_context::{
        DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
    };
    use feldera_types::{deserialize_table_record, serialize_table_record};

    #[derive(Debug, Eq, PartialEq, serde::Serialize)]
    #[allow(non_snake_case)]
    struct TestStruct {
        date: Date,
        time: Time,
        timestamp: Timestamp,
    }

    deserialize_table_record!(TestStruct["TestStruct", 3] {
        (date, "date", false, Date, None),
        (time, "time", false, Time, None),
        (timestamp, "timestamp", false, Timestamp, None)
    });
    serialize_table_record!(TestStruct[3] {
        date["date"]: Date,
        time["time"]: Time,
        timestamp["timestamp"]:  Timestamp
    });

    static DEFAULT_CONFIG: LazyLock<SqlSerdeConfig> = LazyLock::new(SqlSerdeConfig::default);
    static DEBEZIUM_CONFIG: LazyLock<SqlSerdeConfig> =
        LazyLock::new(|| SqlSerdeConfig::from(JsonFlavor::DebeziumMySql));
    static SNOWFLAKE_CONFIG: LazyLock<SqlSerdeConfig> =
        LazyLock::new(|| SqlSerdeConfig::from(JsonFlavor::Snowflake));

    fn deserialize_with_default_config<'de, T>(json: &'de str) -> Result<T, serde_json::Error>
    where
        T: DeserializeWithContext<'de, SqlSerdeConfig>,
    {
        T::deserialize_with_context(
            &mut serde_json::Deserializer::from_str(json),
            &DEFAULT_CONFIG,
        )
    }

    fn deserialize_with_debezium_config<'de, T>(json: &'de str) -> Result<T, serde_json::Error>
    where
        T: DeserializeWithContext<'de, SqlSerdeConfig>,
    {
        T::deserialize_with_context(
            &mut serde_json::Deserializer::from_str(json),
            &DEBEZIUM_CONFIG,
        )
    }

    fn serialize_with_default_config<T>(val: &T) -> Result<String, serde_json::Error>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        let mut data = Vec::new();
        val.serialize_with_context(&mut serde_json::Serializer::new(&mut data), &DEFAULT_CONFIG)
            .unwrap();
        Ok(String::from_utf8(data).unwrap())
    }

    fn serialize_with_snowflake_config<T>(val: &T) -> Result<String, serde_json::Error>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        let mut data = Vec::new();
        val.serialize_with_context(
            &mut serde_json::Serializer::new(&mut data),
            &SNOWFLAKE_CONFIG,
        )
        .unwrap();
        Ok(String::from_utf8(data).unwrap())
    }

    #[test]
    fn debezium() {
        assert_eq!(
            deserialize_with_debezium_config::<TestStruct>(
                r#"{"date": 16816, "time": 10800000000, "timestamp": "2018-06-20T13:37:03Z"}"#
            )
            .unwrap(),
            TestStruct {
                date: Date::new(16816),
                time: Time::new(10800000000000),
                timestamp: Timestamp::new(1529501823000),
            }
        );
    }

    #[test]
    fn snowflake_json() {
        assert_eq!(
            serialize_with_snowflake_config(&TestStruct {
                date: Date::new(19628),
                time: Time::new(84075123000000),
                timestamp: Timestamp::new(1529501823000),
            })
            .unwrap(),
            r#"{"date":"2023-09-28","time":"23:21:15.123","timestamp":"2018-06-20T13:37:03+00:00"}"#
        );
    }

    #[test]
    fn default_json() {
        assert_eq!(
            deserialize_with_default_config::<TestStruct>(
                r#"{"date": "2023-09-28", "time": "23:21:15.123", "timestamp": "2018-06-20 13:37:03"}"#
            )
            .unwrap(),
            TestStruct {
                date: Date::new(19628),
                time: Time::new(84075123000000),
                timestamp: Timestamp::new(1529501823000),
            }
        );

        assert_eq!(
            serialize_with_default_config(&TestStruct {
                date: Date::new(19628),
                time: Time::new(84075123000000),
                timestamp: Timestamp::new(1529501823000),
            })
            .unwrap(),
            r#"{"date":"2023-09-28","time":"23:21:15.123","timestamp":"2018-06-20 13:37:03"}"#
        );
    }
}

num_entries_scalar! {
    Timestamp,
    Date,
    Time,
}

#[doc(hidden)]
pub fn time_trunc_hour_Time(time: Time) -> Time {
    let t = time.to_time();
    let n = NaiveTime::from_hms_opt(t.hour(), 0, 0).unwrap();
    Time::from_time(n)
}

#[doc(hidden)]
pub fn time_trunc_minute_Time(time: Time) -> Time {
    let t = time.to_time();
    let n = NaiveTime::from_hms_opt(t.hour(), t.minute(), 0).unwrap();
    Time::from_time(n)
}

#[doc(hidden)]
pub fn time_trunc_second_Time(time: Time) -> Time {
    let t = time.to_time();
    let n = NaiveTime::from_hms_opt(t.hour(), t.minute(), t.second()).unwrap();
    Time::from_time(n)
}

some_polymorphic_function1!(time_trunc_hour, Time, Time, Time);
some_polymorphic_function1!(time_trunc_minute, Time, Time, Time);
some_polymorphic_function1!(time_trunc_second, Time, Time, Time);

#[doc(hidden)]
#[inline(always)]
pub fn plus_Time_ShortInterval_Time(left: Time, right: ShortInterval) -> Time {
    left + right
}

polymorphic_return_function2!(plus, Time, Time, ShortInterval, ShortInterval, Time, Time);

#[doc(hidden)]
#[inline(always)]
pub fn minus_Time_ShortInterval_Time(left: Time, right: ShortInterval) -> Time {
    left - right
}

polymorphic_return_function2!(minus, Time, Time, ShortInterval, ShortInterval, Time, Time);

#[doc(hidden)]
#[inline(always)]
pub fn plus_Time_LongInterval_Time(left: Time, _: LongInterval) -> Time {
    left
}

polymorphic_return_function2!(plus, Time, Time, LongInterval, LongInterval, Time, Time);

#[doc(hidden)]
#[inline(always)]
pub fn minus_Time_LongInterval_Time(left: Time, _: LongInterval) -> Time {
    left
}

polymorphic_return_function2!(minus, Time, Time, LongInterval, LongInterval, Time, Time);
