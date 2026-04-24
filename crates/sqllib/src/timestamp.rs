//! Support for SQL Timestamp and Date data types.

use crate::error::{SqlResult, SqlRuntimeError};
use crate::{
    FromInteger, SqlString, ToInteger,
    array::Array,
    casts::*,
    interval::{LongInterval, ShortInterval},
    rfc3339::parse_timestamp_from_rfc3339,
};
use chrono::format::ParseErrorKind;
use chrono::{
    DateTime, Datelike, Days, Duration, FixedOffset, LocalResult, Months, NaiveDate, NaiveDateTime,
    NaiveTime, TimeZone, Timelike, Utc,
};
use chrono_tz::Tz;
use core::fmt::Formatter;
use dbsp::num_entries_scalar;
use feldera_macros::IsNone;
use feldera_types::serde_with_context::{
    DateFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig, TimeFormat,
    TimestampFormat,
};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serializer, de::Error as _, ser::Error as _};
use size_of::SizeOf;
use std::{
    borrow::Cow,
    fmt::{self, Debug},
    ops::{Add, Deref, Sub},
};

use crate::{
    operators::{eq, gt, gte, lt, lte, neq},
    some_existing_operator, some_function2, some_operator, some_polymorphic_function1,
    some_polymorphic_function2, some_polymorphic_function3,
};

/////////////////////////////////////////////////////////////////////////////////////////////

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
    serde::Serialize,
    IsNone,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Timestamp {
    // since unix epoch
    microseconds: i64,
}

#[doc(hidden)]
impl<D> ::rkyv::Deserialize<Timestamp, D> for ArchivedTimestamp
where
    D: ::rkyv::Fallible + ::core::any::Any,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<Timestamp, D::Error> {
        let value: i64 = self.microseconds.deserialize(deserializer)?;
        Ok(Timestamp::from_microseconds(value))
    }
}

#[doc(hidden)]
impl ToInteger<i64> for Timestamp {
    #[doc(hidden)]
    fn to_integer(&self) -> i64 {
        self.microseconds
    }
}

#[doc(hidden)]
impl FromInteger<i64> for Timestamp {
    #[doc(hidden)]
    fn from_integer(value: &i64) -> Self {
        Timestamp {
            microseconds: *value,
        }
    }
}

#[doc(hidden)]
impl From<TimestampTz> for Timestamp {
    fn from(value: TimestampTz) -> Self {
        Timestamp {
            microseconds: value.microseconds,
        }
    }
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let datetime = DateTime::from_timestamp_micros(self.microseconds).ok_or(fmt::Error)?;
        f.write_str(&datetime.format("%F %T%.f").to_string())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let dt = self.to_dateTime();
        let month = dt.month();
        let day = dt.day();
        let year = dt.year();
        let hr = dt.hour();
        let min = dt.minute();
        let sec = dt.second();
        let micro = dt.nanosecond() / 1000;
        if micro == 0 {
            write!(
                f,
                "{}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, month, day, hr, min, sec
            )
        } else {
            write!(
                f,
                "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                year, month, day, hr, min, sec, micro
            )
        }
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
            TimestampFormat::Default => {
                let datetime =
                    DateTime::from_timestamp_micros(self.microseconds).ok_or_else(|| {
                        S::Error::custom(format!(
                            "timestamp value '{}' out of range",
                            self.microseconds()
                        ))
                    })?;

                serializer.serialize_str(&datetime.format("%F %T%.f").to_string())
            }
            TimestampFormat::Rfc3339 => {
                let datetime =
                    DateTime::from_timestamp_micros(self.microseconds).ok_or_else(|| {
                        S::Error::custom(format!(
                            "timestamp value '{}' out of range",
                            self.microseconds()
                        ))
                    })?;

                serializer.serialize_str(&datetime.to_rfc3339())
            }
            TimestampFormat::String(format_string) => {
                let datetime =
                    DateTime::from_timestamp_micros(self.microseconds).ok_or_else(|| {
                        S::Error::custom(format!(
                            "timestamp value '{}' out of range",
                            self.microseconds()
                        ))
                    })?;

                serializer.serialize_str(&datetime.format(format_string).to_string())
            }
            TimestampFormat::MillisSinceEpoch => serializer.serialize_i64(self.milliseconds()),
            TimestampFormat::MicrosSinceEpoch => serializer.serialize_i64(self.microseconds),
        }
    }
}

impl<'de, AUX> DeserializeWithContext<'de, SqlSerdeConfig, AUX> for Timestamp {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match config.timestamp_format {
            TimestampFormat::Default | TimestampFormat::Rfc3339 => {
                let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

                let timestamp =
                    parse_timestamp_from_rfc3339(timestamp_str.trim()).map_err(|e| {
                        D::Error::custom(format!(
                            "invalid RFC3339 timestamp string '{timestamp_str}': {e}"
                        ))
                    })?;

                Ok(Self::from_microseconds(timestamp.timestamp_micros()))
            }
            TimestampFormat::String(format) => {
                // `timestamp_str: &'de` doesn't work for JSON, which escapes strings
                // and can only deserialize into an owned string.
                let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

                let timestamp = NaiveDateTime::parse_from_str(timestamp_str.trim(), format)
                    .map_err(|e| {
                        D::Error::custom(format!("invalid timestamp string '{timestamp_str}': {e} (expected format: '{format}')"))
                    })?;

                Ok(Self::from_microseconds(
                    timestamp.and_utc().timestamp_micros(),
                ))
            }
            TimestampFormat::MillisSinceEpoch => {
                let millis: i64 = Deserialize::deserialize(deserializer)?;
                Ok(Self::from_milliseconds(millis))
            }
            TimestampFormat::MicrosSinceEpoch => {
                let micros: i64 = Deserialize::deserialize(deserializer)?;
                Ok(Self::from_microseconds(micros))
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

        Ok(Self::from_microseconds(
            timestamp.and_utc().timestamp_micros(),
        ))
    }
}

impl Timestamp {
    /// Create a new [Timestamp] from a number of milliseconds since the
    /// Unix epoch (January 1, 1970)
    pub const fn from_milliseconds(milliseconds: i64) -> Self {
        Self {
            microseconds: milliseconds * 1000,
        }
    }

    /// Create a new [Timestamp] from a number of microseconds since the
    /// Unix epoch (January 1, 1970)
    pub const fn from_microseconds(microseconds: i64) -> Self {
        Self { microseconds }
    }

    /// Return the number of millliseconds elapsed since the Unix
    /// epoch (January 1st, 1970)
    pub fn milliseconds(&self) -> i64 {
        self.microseconds / 1000
    }

    /// Return the number of microseconds elapsed since the Unix
    /// epoch (January 1st, 1970)
    pub fn microseconds(&self) -> i64 {
        self.microseconds
    }

    /// Convert a [Timestamp] to a chrono [DateTime] in the [Utc]
    /// timezone.
    pub fn to_dateTime(&self) -> DateTime<Utc> {
        Utc.timestamp_micros(self.microseconds)
            .single()
            .unwrap_or_else(|| panic!("Could not convert timestamp {:?} to DateTime", self))
    }

    /// Convert a [Timestamp] to a chrono [NaiveDateTime]
    pub fn to_naiveDateTime(&self) -> NaiveDateTime {
        Utc.timestamp_micros(self.microseconds)
            .single()
            .unwrap_or_else(|| panic!("Could not convert timestamp {:?} to DateTime", self))
            .naive_utc()
    }

    /// Create a [Timestamp] from a chrono [DateTime] in the [Utc]
    /// timezone.
    // Cannot make this into a From trait because Rust reserved it
    pub fn from_dateTime(date: DateTime<Utc>) -> Self {
        Self {
            microseconds: date.timestamp_micros(),
        }
    }

    /// Create a [Timestamp] from a chrono [NaiveDateTime].
    pub fn from_naiveDateTime(date: NaiveDateTime) -> Self {
        Self {
            microseconds: date.and_utc().timestamp_micros(),
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
        let dt: DateTime<Utc> = match Utc.timestamp_micros(self.microseconds) {
            LocalResult::Single(dt) => dt,
            _ => panic!("{self} TIMESTAMP out of range"),
        };
        Date::from_date(dt.date_naive())
    }

    /// Check if the timestamp is legal; panic if it isn't
    pub fn check_legal(&self) {
        let date = self.to_naiveDateTime();
        if date.year() < 1 || date.year() > 9999 {
            panic!("Timestamp out of range {}", self);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////

/// Represents an instant, represented as a Timestamp in UTC timezone
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
    serde::Serialize,
    IsNone,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct TimestampTz {
    // since unix epoch
    microseconds: i64,
}

#[doc(hidden)]
impl From<Timestamp> for TimestampTz {
    fn from(value: Timestamp) -> Self {
        TimestampTz {
            microseconds: value.microseconds,
        }
    }
}

#[doc(hidden)]
impl<D> ::rkyv::Deserialize<TimestampTz, D> for ArchivedTimestampTz
where
    D: ::rkyv::Fallible + ::core::any::Any,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<TimestampTz, D::Error> {
        let value: i64 = self.microseconds.deserialize(deserializer)?;
        Ok(TimestampTz::from_microseconds(value))
    }
}

#[doc(hidden)]
impl ToInteger<i64> for TimestampTz {
    #[doc(hidden)]
    fn to_integer(&self) -> i64 {
        self.microseconds
    }
}

#[doc(hidden)]
impl FromInteger<i64> for TimestampTz {
    #[doc(hidden)]
    fn from_integer(value: &i64) -> Self {
        TimestampTz {
            microseconds: *value,
        }
    }
}

impl Debug for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let datetime = match Utc.timestamp_micros(self.microseconds) {
            LocalResult::Single(v) => v,
            LocalResult::None | LocalResult::Ambiguous(_, _) => Err(fmt::Error)?,
        };
        f.write_str(&datetime.format("%F %T%.f%:z").to_string())
    }
}

impl fmt::Display for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let dt = self.to_dateTime();
        let month = dt.month();
        let day = dt.day();
        let year = dt.year();
        let hr = dt.hour();
        let min = dt.minute();
        let sec = dt.second();
        let micro = dt.nanosecond() / 1000;
        if micro == 0 {
            write!(
                f,
                "{}-{:02}-{:02} {:02}:{:02}:{:02}+00:00",
                year, month, day, hr, min, sec
            )
        } else {
            write!(
                f,
                "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}+00:00",
                year, month, day, hr, min, sec, micro
            )
        }
    }
}

impl SerializeWithContext<SqlSerdeConfig> for TimestampTz {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.timestamp_tz_format {
            TimestampFormat::Default => {
                let datetime = self.to_dateTime();
                serializer.serialize_str(&datetime.format("%F %T%.f%:z").to_string())
            }
            TimestampFormat::Rfc3339 => {
                let datetime = self.to_dateTime();
                serializer.serialize_str(&datetime.to_rfc3339())
            }
            TimestampFormat::String(format_string) => {
                let datetime = self.to_dateTime();
                serializer.serialize_str(&datetime.format(format_string).to_string())
            }
            TimestampFormat::MillisSinceEpoch => serializer.serialize_i64(self.milliseconds()),
            TimestampFormat::MicrosSinceEpoch => serializer.serialize_i64(self.microseconds),
        }
    }
}

impl<'de, AUX> DeserializeWithContext<'de, SqlSerdeConfig, AUX> for TimestampTz {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match config.timestamp_tz_format {
            TimestampFormat::Default | TimestampFormat::Rfc3339 => {
                let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

                let dateTime = DateTime::parse_from_rfc3339(timestamp_str.trim()).map_err(|e| {
                    D::Error::custom(format!(
                        "invalid RFC3339 timestamp with timezone string '{timestamp_str}': {e}"
                    ))
                })?;

                Ok(Self::from_dateTime(dateTime.to_utc()))
            }
            TimestampFormat::String(format) => {
                // `timestamp_str: &'de` doesn't work for JSON, which escapes strings
                // and can only deserialize into an owned string.
                let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
                let timestamp = NaiveDateTime::parse_from_str(timestamp_str.trim(), format)
                    .map_err(|e| {
                        D::Error::custom(format!("invalid timestamp string '{timestamp_str}': {e} (expected format: '{format}')"))
                    })?;

                Ok(Self::from_microseconds(
                    timestamp.and_utc().timestamp_micros(),
                ))
            }
            TimestampFormat::MillisSinceEpoch => {
                let millis: i64 = Deserialize::deserialize(deserializer)?;
                Ok(Self::from_milliseconds(millis))
            }
            TimestampFormat::MicrosSinceEpoch => {
                let micros: i64 = Deserialize::deserialize(deserializer)?;
                Ok(Self::from_microseconds(micros))
            }
        }
    }
}

/// Deserialize timestamp from the `YYYY-MM-DD HH:MM:SS.fff offset` format.
/// For a flexible deserialization framework use deserialize_with_context.
impl<'de> Deserialize<'de> for TimestampTz {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `timestamp_str: &'de` doesn't work for JSON, which escapes strings
        // and can only deserialize into an owned string.
        let timestamp_str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;

        let datetime =
            DateTime::parse_from_str(timestamp_str.trim(), "%F %T%.f%:z").map_err(|e| {
                D::Error::custom(format!(
                    "invalid timestamp with time zone string '{timestamp_str}': {e} (expected format: '%F %T%.f%:z')"
                ))
            })?;

        Ok(Self::from_dateTime(datetime.to_utc()))
    }
}

impl TimestampTz {
    /// Create a new [TimestampTz] from a number of milliseconds since the
    /// Unix epoch (January 1, 1970)
    pub const fn from_milliseconds(milliseconds: i64) -> Self {
        Self {
            microseconds: milliseconds * 1000,
        }
    }

    /// Create a new [TimestampTz] from a number of microseconds since the
    /// Unix epoch (January 1, 1970)
    pub const fn from_microseconds(microseconds: i64) -> Self {
        Self { microseconds }
    }

    /// Return the number of millliseconds elapsed since the Unix
    /// epoch (January 1st, 1970)
    pub fn milliseconds(&self) -> i64 {
        self.microseconds / 1000
    }

    /// Return the number of microseconds elapsed since the Unix
    /// epoch (January 1st, 1970)
    pub fn microseconds(&self) -> i64 {
        self.microseconds
    }

    /// Convert a [TimestampTz] to a chrono [DateTime] in the [Utc]
    /// timezone.
    pub fn to_dateTime(&self) -> DateTime<Utc> {
        Utc.timestamp_micros(self.microseconds)
            .single()
            .unwrap_or_else(|| panic!("Could not convert timestamp {:?} to DateTime", self))
    }

    /// Create a [TimestampTz] from a chrono [DateTime] in the [Utc]
    /// timezone.
    // Cannot make this into a From trait because Rust reserved it
    pub fn from_dateTime(date: DateTime<Utc>) -> Self {
        Self {
            microseconds: date.timestamp_micros(),
        }
    }

    /// Convert a [Timestamp] to a [TimestampTz]
    pub fn from_timestamp(ts: Timestamp) -> Self {
        TimestampTz::from_microseconds(ts.microseconds)
    }

    /// Convert a [TimestampTz] to a chrono [NaiveDateTime]
    pub fn to_naiveDateTime(&self) -> NaiveDateTime {
        Utc.timestamp_micros(self.microseconds)
            .single()
            .unwrap_or_else(|| {
                panic!(
                    "Could not convert timestamp with time zone {:?} to DateTime",
                    self
                )
            })
            .naive_utc()
    }

    /// Check if the timestamp is legal; panic if it isn't
    pub fn check_legal(&self) {
        let date = self.to_naiveDateTime();
        if date.year() < 1 || date.year() > 9999 {
            panic!("TIMESTAMP WITH TIME ZONE out of range {}", self);
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum Zone {
    Iana(Tz),
    Offset(FixedOffset),
    // Timezone string
    Invalid(String),
}

impl Zone {
    #[doc(hidden)]
    /// Parse either an IANA timezone or a numeric offset like "+09:00".
    fn parse(s: &str) -> Self {
        // Try IANA zone first
        if let Ok(tz) = s.parse::<Tz>() {
            return Zone::Iana(tz);
        }

        // Try numeric offset: +HH, +HH:MM, +HH:MM:SS
        if let Some(offset) = Self::parse_fixed_offset(s) {
            return Zone::Offset(offset);
        }

        tracing::warn!("failed to parse timezone '{}'", s);
        Zone::Invalid(s.to_string())
    }

    #[doc(hidden)]
    /// Convert a string like +08:00 to a FixedOffset.
    // Apparently there is no library function to do this, since the format is not standard
    fn parse_fixed_offset(s: &str) -> Option<FixedOffset> {
        static RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
            Regex::new(r"^([+-])([0-9]{2})(?::([0-9]{2})(?::([0-9]{2}))?)?$").unwrap()
        });

        let caps = RE.captures(s)?;
        let sign = if &caps[1] == "+" { 1 } else { -1 };

        let h: i32 = caps[2].parse().ok()?;
        let m: i32 = caps
            .get(3)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let sec: i32 = caps
            .get(4)
            .and_then(|s| s.as_str().parse().ok())
            .unwrap_or(0);

        if h > 23 || m > 59 || sec > 59 {
            return None;
        }

        let total = sign * (h * 3600 + m * 60 + sec);
        FixedOffset::east_opt(total)
    }

    /// Interpret a NaiveDateTime in this zone.
    fn local_to_utc(&self, ts: NaiveDateTime) -> SqlResult<DateTime<Utc>> {
        match self {
            Zone::Iana(tz) => {
                let dt = tz.from_local_datetime(&ts).single().ok_or_else(|| {
                    SqlRuntimeError::from_string(format!(
                        "Cannot represent timestamp {} in timezone {:?}",
                        ts, self
                    ))
                })?;
                Ok(dt.with_timezone(&Utc))
            }
            Zone::Offset(off) => {
                let dt = off.from_local_datetime(&ts).single().ok_or_else(|| {
                    SqlRuntimeError::from_string(format!(
                        "Cannot represent timestamp {} in timezone {:?}",
                        ts, self
                    ))
                })?;
                Ok(dt.with_timezone(&Utc))
            }
            Zone::Invalid(tz) => Err(SqlRuntimeError::from_string(format!(
                "Cannot parse timezone {tz}"
            ))),
        }
    }

    /// Convert a UTC datetime into this zone.
    #[doc(hidden)]
    fn to_local(&self, utc: DateTime<Utc>) -> SqlResult<NaiveDateTime> {
        match self {
            Zone::Iana(tz) => Ok(utc.with_timezone(tz).naive_local()),
            Zone::Offset(off) => Ok(utc.with_timezone(off).naive_local()),
            Zone::Invalid(tz) => Err(SqlRuntimeError::from_string(format!(
                "Cannot parse timezone {tz}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::FixedOffset;

    #[test]
    fn test_valid_hours_only() {
        assert_eq!(
            Zone::parse_fixed_offset("+09"),
            Some(FixedOffset::east_opt(9 * 3600).unwrap())
        );
        assert_eq!(
            Zone::parse_fixed_offset("-05"),
            Some(FixedOffset::west_opt(5 * 3600).unwrap())
        );
    }

    #[test]
    fn test_valid_hours_minutes() {
        assert_eq!(
            Zone::parse_fixed_offset("+09:30"),
            Some(FixedOffset::east_opt(9 * 3600 + 30 * 60).unwrap())
        );
        assert_eq!(
            Zone::parse_fixed_offset("-02:45"),
            Some(FixedOffset::west_opt(2 * 3600 + 45 * 60).unwrap())
        );
    }

    #[test]
    fn test_valid_hours_minutes_seconds() {
        assert_eq!(
            Zone::parse_fixed_offset("+09:00:30"),
            Some(FixedOffset::east_opt(9 * 3600 + 30).unwrap())
        );
        assert_eq!(
            Zone::parse_fixed_offset("-01:02:03"),
            Some(FixedOffset::west_opt(3600 + 2 * 60 + 3).unwrap())
        );
    }

    #[test]
    fn test_zero_offset() {
        assert_eq!(
            Zone::parse_fixed_offset("+00"),
            Some(FixedOffset::east_opt(0).unwrap())
        );
        assert_eq!(
            Zone::parse_fixed_offset("-00:00"),
            Some(FixedOffset::east_opt(0).unwrap())
        );
    }

    #[test]
    fn test_invalid_missing_sign() {
        assert_eq!(Zone::parse_fixed_offset("09"), None);
        assert_eq!(Zone::parse_fixed_offset("09:00"), None);
    }

    #[test]
    fn test_invalid_bad_format() {
        assert_eq!(Zone::parse_fixed_offset("+9"), None);
        assert_eq!(Zone::parse_fixed_offset("+0900"), None);
        assert_eq!(Zone::parse_fixed_offset("+09:"), None);
        assert_eq!(Zone::parse_fixed_offset("+:"), None);
        assert_eq!(Zone::parse_fixed_offset("+09:0"), None);
        assert_eq!(Zone::parse_fixed_offset("+09:00:0"), None);
        assert_eq!(Zone::parse_fixed_offset("++09:00"), None);
    }

    #[test]
    fn test_invalid_out_of_range() {
        assert_eq!(Zone::parse_fixed_offset("+24"), None);
        assert_eq!(Zone::parse_fixed_offset("+23:60"), None);
        assert_eq!(Zone::parse_fixed_offset("+10:00:60"), None);
        assert_eq!(Zone::parse_fixed_offset("-25:00"), None);
    }

    #[test]
    fn test_valid_edge_boundaries() {
        assert_eq!(
            Zone::parse_fixed_offset("+23:59:59"),
            Some(FixedOffset::east_opt(23 * 3600 + 59 * 60 + 59).unwrap())
        );
        assert_eq!(
            Zone::parse_fixed_offset("-23:59:59"),
            Some(FixedOffset::west_opt(23 * 3600 + 59 * 60 + 59).unwrap())
        );
    }
}

#[doc(hidden)]
pub fn parse_timezone_(tz: SqlString) -> Zone {
    Zone::parse(tz.str())
}

#[doc(hidden)]
pub fn parse_timezoneN(tz: Option<SqlString>) -> Option<Zone> {
    let tz = tz?;
    Some(parse_timezone_(tz))
}

#[doc(hidden)]
pub fn convert_timezone___(src_tz: Zone, dst_tz: Zone, ts: Timestamp) -> Option<Timestamp> {
    let naive = ts.to_naiveDateTime();
    let utc = src_tz.local_to_utc(naive).ok()?;
    let result = dst_tz.to_local(utc).ok()?;
    Some(Timestamp::from_naiveDateTime(result))
}

#[doc(hidden)]
pub fn convert_timezoneN__(source: Option<Zone>, target: Zone, ts: Timestamp) -> Option<Timestamp> {
    let source = source?;
    convert_timezone___(source, target, ts)
}

#[doc(hidden)]
pub fn convert_timezone_N_(source: Zone, target: Option<Zone>, ts: Timestamp) -> Option<Timestamp> {
    let target = target?;
    convert_timezone___(source, target, ts)
}

#[doc(hidden)]
pub fn convert_timezoneNN_(
    source: Option<Zone>,
    target: Option<Zone>,
    ts: Timestamp,
) -> Option<Timestamp> {
    let source = source?;
    let target = target?;
    convert_timezone___(source, target, ts)
}

#[doc(hidden)]
pub fn convert_timezoneN_N(
    source: Option<Zone>,
    target: Zone,
    ts: Option<Timestamp>,
) -> Option<Timestamp> {
    let source = source?;
    let ts = ts?;
    convert_timezone___(source, target, ts)
}

#[doc(hidden)]
pub fn convert_timezone_NN(
    source: Zone,
    target: Option<Zone>,
    ts: Option<Timestamp>,
) -> Option<Timestamp> {
    let target = target?;
    let ts = ts?;
    convert_timezone___(source, target, ts)
}

#[doc(hidden)]
pub fn convert_timezone__N(source: Zone, target: Zone, ts: Option<Timestamp>) -> Option<Timestamp> {
    let ts = ts?;
    convert_timezone___(source, target, ts)
}

#[doc(hidden)]
pub fn convert_timezoneNNN(
    source: Option<Zone>,
    target: Option<Zone>,
    ts: Option<Timestamp>,
) -> Option<Timestamp> {
    let source = source?;
    let target = target?;
    let ts = ts?;
    convert_timezone___(source, target, ts)
}

#[doc(hidden)]
pub fn now() -> Timestamp {
    Timestamp::from_dateTime(Utc::now())
}

#[doc(hidden)]
pub fn plus_Timestamp_Timestamp_ShortInterval__(
    left: Timestamp,
    right: ShortInterval,
) -> Timestamp {
    Timestamp {
        microseconds: left.microseconds + right.microseconds(),
    }
}

some_function2!(
    plus_Timestamp_Timestamp_ShortInterval,
    Timestamp,
    ShortInterval,
    Timestamp
);

#[doc(hidden)]
pub fn plus_TimestampTz_TimestampTz_ShortInterval__(
    left: TimestampTz,
    right: ShortInterval,
) -> TimestampTz {
    TimestampTz {
        microseconds: left.microseconds + right.microseconds(),
    }
}

some_function2!(
    plus_TimestampTz_TimestampTz_ShortInterval,
    TimestampTz,
    ShortInterval,
    TimestampTz
);

#[doc(hidden)]
pub fn plus_Timestamp_Timestamp_LongInterval__(left: Timestamp, right: LongInterval) -> Timestamp {
    let dt = left.to_naiveDateTime();
    let months = right.months();
    let ndt = if months < 0 {
        dt.checked_sub_months(Months::new(-months as u32))
    } else {
        dt.checked_add_months(Months::new(months as u32))
    };
    Timestamp::from_naiveDateTime(ndt.unwrap())
}

some_function2!(
    plus_Timestamp_Timestamp_LongInterval,
    Timestamp,
    LongInterval,
    Timestamp
);

#[doc(hidden)]
pub fn plus_TimestampTz_TimestampTz_LongInterval__(
    left: TimestampTz,
    right: LongInterval,
) -> TimestampTz {
    let result = plus_Timestamp_Timestamp_LongInterval__(left.into(), right);
    result.into()
}

some_function2!(
    plus_TimestampTz_TimestampTz_LongInterval,
    TimestampTz,
    LongInterval,
    TimestampTz
);

#[doc(hidden)]
pub fn minus_Timestamp_Timestamp_LongInterval__(left: Timestamp, right: LongInterval) -> Timestamp {
    let dt = left.to_naiveDateTime();
    let months = right.months();
    let ndt = if months < 0 {
        dt.checked_add_months(Months::new(-months as u32))
    } else {
        dt.checked_sub_months(Months::new(months as u32))
    };
    Timestamp::from_naiveDateTime(ndt.unwrap())
}

some_function2!(
    minus_Timestamp_Timestamp_LongInterval,
    Timestamp,
    LongInterval,
    Timestamp
);

#[doc(hidden)]
pub fn minus_TimestampTz_TimestampTz_LongInterval__(
    left: TimestampTz,
    right: LongInterval,
) -> TimestampTz {
    let result = minus_Timestamp_Timestamp_LongInterval__(left.into(), right);
    result.into()
}

some_function2!(
    minus_TimestampTz_TimestampTz_LongInterval,
    TimestampTz,
    LongInterval,
    TimestampTz
);

#[doc(hidden)]
pub fn plus_Timestamp_Date_ShortInterval__(left: Date, right: ShortInterval) -> Timestamp {
    plus_Timestamp_Timestamp_ShortInterval__(cast_to_Timestamp_Date(left).unwrap(), right)
}

some_function2!(
    plus_Timestamp_Date_ShortInterval,
    Date,
    ShortInterval,
    Timestamp
);

#[doc(hidden)]
pub fn minus_ShortInterval_Timestamp_Timestamp__(
    left: Timestamp,
    right: Timestamp,
) -> ShortInterval {
    ShortInterval::from_microseconds(left.microseconds - right.microseconds)
}

some_function2!(
    minus_ShortInterval_Timestamp_Timestamp,
    Timestamp,
    Timestamp,
    ShortInterval
);

#[doc(hidden)]
pub fn minus_ShortInterval_TimestampTz_TimestampTz__(
    left: TimestampTz,
    right: TimestampTz,
) -> ShortInterval {
    ShortInterval::from_microseconds(left.microseconds - right.microseconds)
}

some_function2!(
    minus_ShortInterval_TimestampTz_TimestampTz,
    TimestampTz,
    TimestampTz,
    ShortInterval
);

#[doc(hidden)]
pub fn minus_ShortInterval_Time_Time__(left: Time, right: Time) -> ShortInterval {
    ShortInterval::from_microseconds(
        (left.nanoseconds() as i64 - right.nanoseconds() as i64) / 1000,
    )
}

some_function2!(minus_ShortInterval_Time_Time, Time, Time, ShortInterval);

#[doc(hidden)]
pub fn minus_Timestamp_Timestamp_ShortInterval__(
    left: Timestamp,
    right: ShortInterval,
) -> Timestamp {
    Timestamp::from_microseconds(left.microseconds() - right.microseconds())
}

some_function2!(
    minus_Timestamp_Timestamp_ShortInterval,
    Timestamp,
    ShortInterval,
    Timestamp
);

#[doc(hidden)]
pub fn minus_TimestampTz_TimestampTz_ShortInterval__(
    left: TimestampTz,
    right: ShortInterval,
) -> TimestampTz {
    TimestampTz::from_microseconds(left.microseconds() - right.microseconds())
}

some_function2!(
    minus_TimestampTz_TimestampTz_ShortInterval,
    TimestampTz,
    ShortInterval,
    TimestampTz
);

#[doc(hidden)]
pub fn minus_Timestamp_Date_ShortInterval__(left: Date, right: ShortInterval) -> Timestamp {
    minus_Timestamp_Timestamp_ShortInterval__(cast_to_Timestamp_Date(left).unwrap(), right)
}

some_function2!(
    minus_Timestamp_Date_ShortInterval,
    Date,
    ShortInterval,
    Timestamp
);

#[doc(hidden)]
pub fn plus_Date_Date_ShortInterval__(left: Date, right: ShortInterval) -> Date {
    let days = (right.microseconds() / (86400i64 * 1000 * 1000)) as i32;
    let diff = left.days() + days;
    Date::from_days(diff)
}

some_function2!(plus_Date_Date_ShortInterval, Date, ShortInterval, Date);

#[doc(hidden)]
pub fn minus_Date_Date_ShortInterval__(left: Date, right: ShortInterval) -> Date {
    let days = (right.microseconds() / (86400i64 * 1000 * 1000)) as i32;
    let diff = left.days() - days;
    Date::from_days(diff)
}

some_function2!(minus_Date_Date_ShortInterval, Date, ShortInterval, Date);

#[doc(hidden)]
pub fn plus_Date_Date_LongInterval__(left: Date, right: LongInterval) -> Date {
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

some_function2!(plus_Date_Date_LongInterval, Date, LongInterval, Date);

#[doc(hidden)]
pub fn minus_Date_Date_LongInterval__(left: Date, right: LongInterval) -> Date {
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

some_function2!(minus_Date_Date_LongInterval, Date, LongInterval, Date);

#[doc(hidden)]
pub fn minus_LongInterval_Timestamp_Timestamp__(left: Timestamp, right: Timestamp) -> LongInterval {
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
    LongInterval::from_months(if swap { -months } else { months })
}

some_function2!(
    minus_LongInterval_Timestamp_Timestamp,
    Timestamp,
    Timestamp,
    LongInterval
);

#[doc(hidden)]
pub fn minus_LongInterval_TimestampTz_TimestampTz__(
    left: TimestampTz,
    right: TimestampTz,
) -> LongInterval {
    minus_LongInterval_Timestamp_Timestamp__(left.into(), right.into())
}

#[doc(hidden)]
pub fn extract_year_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.year().into()
}

#[doc(hidden)]
pub fn extract_year_TimestampTz(value: TimestampTz) -> i64 {
    extract_year_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_month_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.month().into()
}

#[doc(hidden)]
pub fn extract_month_TimestampTz(value: TimestampTz) -> i64 {
    extract_month_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_day_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.day().into()
}

#[doc(hidden)]
pub fn extract_day_TimestampTz(value: TimestampTz) -> i64 {
    extract_day_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_quarter_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.month0() / 3 + 1).into()
}

#[doc(hidden)]
pub fn extract_quarter_TimestampTz(value: TimestampTz) -> i64 {
    extract_quarter_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_decade_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.year() / 10).into()
}

#[doc(hidden)]
pub fn extract_decade_TimestampTz(value: TimestampTz) -> i64 {
    extract_decade_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_century_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    ((date.year() + 99) / 100).into()
}

#[doc(hidden)]
pub fn extract_century_TimestampTz(value: TimestampTz) -> i64 {
    extract_century_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_millennium_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    ((date.year() + 999) / 1000).into()
}

#[doc(hidden)]
pub fn extract_millennium_TimestampTz(value: TimestampTz) -> i64 {
    extract_millennium_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_isoyear_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.iso_week().year().into()
}

#[doc(hidden)]
pub fn extract_isoyear_TimestampTz(value: TimestampTz) -> i64 {
    extract_isoyear_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_week_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.iso_week().week().into()
}

#[doc(hidden)]
pub fn extract_week_TimestampTz(value: TimestampTz) -> i64 {
    extract_week_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_dow_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.weekday().num_days_from_sunday() as i64) + Date::first_day_of_week()
}

#[doc(hidden)]
pub fn extract_dow_TimestampTz(value: TimestampTz) -> i64 {
    extract_dow_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_isodow_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.weekday().num_days_from_monday() as i64) + 1
}

#[doc(hidden)]
pub fn extract_isodow_TimestampTz(value: TimestampTz) -> i64 {
    extract_isodow_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_doy_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.ordinal().into()
}

#[doc(hidden)]
pub fn extract_doy_TimestampTz(value: TimestampTz) -> i64 {
    extract_doy_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_epoch_Timestamp(t: Timestamp) -> i64 {
    t.microseconds() / 1_000_000
}

#[doc(hidden)]
pub fn extract_epoch_TimestampTz(t: TimestampTz) -> i64 {
    extract_epoch_Timestamp(t.into())
}

#[doc(hidden)]
pub fn extract_millisecond_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.second() * 1000 + date.timestamp_subsec_millis()).into()
}

#[doc(hidden)]
pub fn extract_millisecond_TimestampTz(value: TimestampTz) -> i64 {
    extract_millisecond_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_microsecond_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.second() * 1_000_000 + date.timestamp_subsec_micros()).into()
}

#[doc(hidden)]
pub fn extract_microsecond_TimestampTz(value: TimestampTz) -> i64 {
    extract_microsecond_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_nanosecond_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    (date.second() as i64) * 1_000_000_000i64 + date.timestamp_subsec_nanos() as i64
}

#[doc(hidden)]
pub fn extract_nanosecond_TimestampTz(value: TimestampTz) -> i64 {
    extract_nanosecond_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_second_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.second().into()
}

#[doc(hidden)]
pub fn extract_second_TimestampTz(value: TimestampTz) -> i64 {
    extract_second_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_minute_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.minute().into()
}

#[doc(hidden)]
pub fn extract_minute_TimestampTz(value: TimestampTz) -> i64 {
    extract_minute_Timestamp(value.into())
}

#[doc(hidden)]
pub fn extract_hour_Timestamp(value: Timestamp) -> i64 {
    let date = value.to_dateTime();
    date.hour().into()
}

#[doc(hidden)]
pub fn extract_hour_TimestampTz(value: TimestampTz) -> i64 {
    extract_hour_Timestamp(value.into())
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
some_polymorphic_function1!(extract_nanosecond, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_second, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_minute, Timestamp, Timestamp, i64);
some_polymorphic_function1!(extract_hour, Timestamp, Timestamp, i64);

some_polymorphic_function1!(extract_year, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_month, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_day, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_quarter, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_decade, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_century, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_millennium, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_isoyear, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_week, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_dow, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_isodow, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_doy, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_epoch, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_millisecond, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_microsecond, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_nanosecond, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_second, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_minute, TimestampTz, TimestampTz, i64);
some_polymorphic_function1!(extract_hour, TimestampTz, TimestampTz, i64);

some_operator!(lt, Timestamp, Timestamp, bool);
some_operator!(gt, Timestamp, Timestamp, bool);
some_operator!(eq, Timestamp, Timestamp, bool);
some_operator!(neq, Timestamp, Timestamp, bool);
some_operator!(gte, Timestamp, Timestamp, bool);
some_operator!(lte, Timestamp, Timestamp, bool);

some_operator!(lt, TimestampTz, TimestampTz, bool);
some_operator!(gt, TimestampTz, TimestampTz, bool);
some_operator!(eq, TimestampTz, TimestampTz, bool);
some_operator!(neq, TimestampTz, TimestampTz, bool);
some_operator!(gte, TimestampTz, TimestampTz, bool);
some_operator!(lte, TimestampTz, TimestampTz, bool);

#[doc(hidden)]
pub fn floor_millennium_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    let naive = NaiveDate::from_ymd_opt((ts.year() / 1000) * 1000, 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(floor_millennium, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_millennium_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_millennium_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_millennium, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_century_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    let naive = NaiveDate::from_ymd_opt((ts.year() / 100) * 100, 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(floor_century, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_century_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_century_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_century, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_decade_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    let naive = NaiveDate::from_ymd_opt((ts.year() / 10) * 10, 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(floor_decade, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_decade_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_decade_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_decade, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_year_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    let naive = NaiveDate::from_ymd_opt(ts.year(), 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(floor_year, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_year_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_year_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_year, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_quarter_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    let naive = NaiveDate::from_ymd_opt(ts.year(), (((ts.month() - 1) / 3) * 3) + 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(floor_quarter, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_quarter_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_quarter_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_quarter, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_month_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    let naive = NaiveDate::from_ymd_opt(ts.year(), ts.month(), 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(floor_month, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_month_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_month_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_month, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_week_Timestamp(value: Timestamp) -> Timestamp {
    let wd = extract_dow_Timestamp(value) - Date::first_day_of_week();
    let notimeTs = floor_day_Timestamp(value);
    let interval = ShortInterval::from_seconds(wd * 86400);
    minus_Timestamp_Timestamp_ShortInterval__(notimeTs, interval)
}

some_polymorphic_function1!(floor_week, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_week_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_week_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_week, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_day_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    let naive = NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day()).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(floor_day, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_day_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_day_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_day, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_hour_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_naiveDateTime();
    let naive = NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day())
        .unwrap()
        .and_hms_opt(ts.hour(), 0, 0)
        .unwrap();
    Timestamp::from_naiveDateTime(naive)
}

some_polymorphic_function1!(floor_hour, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_hour_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_hour_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_hour, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_minute_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_naiveDateTime();
    let naive = NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day())
        .unwrap()
        .and_hms_opt(ts.hour(), ts.minute(), 0)
        .unwrap();
    Timestamp::from_naiveDateTime(naive)
}

some_polymorphic_function1!(floor_minute, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_minute_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_minute_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_minute, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_second_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_naiveDateTime();
    let naive = NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day())
        .unwrap()
        .and_hms_opt(ts.hour(), ts.minute(), ts.second())
        .unwrap();
    Timestamp::from_naiveDateTime(naive)
}

some_polymorphic_function1!(floor_second, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_second_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_second_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_second, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_millisecond_Timestamp(value: Timestamp) -> Timestamp {
    let floor = if value.microseconds < 0 {
        let md = -value.microseconds % 1000;
        if md == 0 {
            -((-value.microseconds) / 1000)
        } else {
            -((-value.microseconds) / 1000) - 1
        }
    } else {
        value.microseconds / 1000
    };
    Timestamp::from_milliseconds(floor)
}

some_polymorphic_function1!(floor_millisecond, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_millisecond_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_millisecond_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_millisecond, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_microsecond_Timestamp(value: Timestamp) -> Timestamp {
    // we currently don't store more than microseconds, so this is a no-op
    value
}

some_polymorphic_function1!(floor_microsecond, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_microsecond_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_microsecond_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_microsecond, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn floor_nanosecond_Timestamp(value: Timestamp) -> Timestamp {
    // we currently don't store more than milliseconds, so this is a no-op
    value
}

some_polymorphic_function1!(floor_nanosecond, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn floor_nanosecond_TimestampTz(value: TimestampTz) -> TimestampTz {
    floor_nanosecond_Timestamp(value.into()).into()
}

some_polymorphic_function1!(floor_nanosecond, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
fn is_midnight(value: DateTime<Utc>) -> bool {
    value.hour() == 0 && value.minute() == 0 && value.second() == 0 && value.nanosecond() == 0
}

#[doc(hidden)]
pub fn ceil_millennium_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    if ts.year() % 1000 == 0 && ts.month() == 1 && ts.day() == 1 && is_midnight(ts) {
        return value;
    }
    let naive = NaiveDate::from_ymd_opt((ts.year() / 1000 + 1) * 1000, 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(ceil_millennium, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_millennium_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_millennium_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_millennium, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_century_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    if ts.year() % 100 == 0 && ts.month() == 1 && ts.day() == 1 && is_midnight(ts) {
        return value;
    }
    let naive = NaiveDate::from_ymd_opt((ts.year() / 100 + 1) * 100, 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(ceil_century, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_century_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_century_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_century, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_decade_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    if ts.year() % 10 == 0 && ts.month() == 1 && ts.day() == 1 && is_midnight(ts) {
        return value;
    }
    let naive = NaiveDate::from_ymd_opt((ts.year() / 10 + 1) * 10, 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(ceil_decade, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_decade_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_decade_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_decade, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_year_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    if ts.month() == 1 && ts.day() == 1 && is_midnight(ts) {
        return value;
    }
    let naive = NaiveDate::from_ymd_opt(ts.year() + 1, 1, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(ceil_year, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_year_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_year_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_year, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_quarter_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    if (ts.month() - 1).is_multiple_of(3) && ts.day() == 1 && is_midnight(ts) {
        return value;
    }
    let next_quarter_month = match ts.month() {
        1..=3 => 4,
        4..=6 => 7,
        7..=9 => 10,
        10..=12 => 1,
        _ => unreachable!(),
    };
    let year = if next_quarter_month == 1 {
        ts.year() + 1
    } else {
        ts.year()
    };
    let naive = NaiveDate::from_ymd_opt(year, next_quarter_month, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(ceil_quarter, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_quarter_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_quarter_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_quarter, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_month_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    if ts.day() == 1 && is_midnight(ts) {
        return value;
    }
    let month = if ts.month() == 12 { 1 } else { ts.month() + 1 };
    let year = if month == 1 { ts.year() + 1 } else { ts.year() };
    let naive = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    Timestamp::from_naiveDate(naive)
}

some_polymorphic_function1!(ceil_month, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_month_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_month_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_month, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_week_Timestamp(value: Timestamp) -> Timestamp {
    let wd = extract_dow_Timestamp(value) - Date::first_day_of_week();
    let ts = value.to_dateTime();
    if wd == 0 && is_midnight(ts) {
        return value;
    }
    let notimeTs = floor_day_Timestamp(value);
    let interval = ShortInterval::from_seconds((7 - wd) * 86400);
    plus_Timestamp_Timestamp_ShortInterval__(notimeTs, interval)
}

some_polymorphic_function1!(ceil_week, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_week_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_week_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_week, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_day_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_dateTime();
    if is_midnight(ts) {
        return value;
    }
    let notimeTs = floor_day_Timestamp(value);
    let day = ShortInterval::from_seconds(86400);
    plus_Timestamp_Timestamp_ShortInterval__(notimeTs, day)
}

some_polymorphic_function1!(ceil_day, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_day_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_day_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_day, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_hour_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_naiveDateTime();
    if ts.minute() == 0 && ts.second() == 0 && ts.nanosecond() == 0 {
        return value;
    }
    let floored = floor_hour_Timestamp(value);
    let hour = ShortInterval::from_seconds(3600);
    plus_Timestamp_Timestamp_ShortInterval__(floored, hour)
}

some_polymorphic_function1!(ceil_hour, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_hour_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_hour_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_hour, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_minute_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_naiveDateTime();
    if ts.second() == 0 && ts.nanosecond() == 0 {
        return value;
    }
    let floored = floor_minute_Timestamp(value);
    let minute = ShortInterval::from_seconds(60);
    plus_Timestamp_Timestamp_ShortInterval__(floored, minute)
}

some_polymorphic_function1!(ceil_minute, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_minute_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_minute_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_minute, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_second_Timestamp(value: Timestamp) -> Timestamp {
    let ts = value.to_naiveDateTime();
    if ts.nanosecond() == 0 {
        return value;
    }
    let floored = floor_second_Timestamp(value);
    let second = ShortInterval::from_seconds(1);
    plus_Timestamp_Timestamp_ShortInterval__(floored, second)
}

some_polymorphic_function1!(ceil_second, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_second_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_second_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_second, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_millisecond_Timestamp(value: Timestamp) -> Timestamp {
    Timestamp::from_milliseconds((value.microseconds + 999) / 1000)
}

some_polymorphic_function1!(ceil_millisecond, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_millisecond_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_millisecond_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_millisecond, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_microsecond_Timestamp(value: Timestamp) -> Timestamp {
    // We currently do not store more than milliseconds, so this is a no-op
    value
}

some_polymorphic_function1!(ceil_microsecond, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_microsecond_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_microsecond_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_microsecond, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn ceil_nanosecond_Timestamp(value: Timestamp) -> Timestamp {
    // We currently do not store more than milliseconds, so this is a no-op
    value
}

some_polymorphic_function1!(ceil_nanosecond, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
pub fn ceil_nanosecond_TimestampTz(value: TimestampTz) -> TimestampTz {
    ceil_nanosecond_Timestamp(value.into()).into()
}

some_polymorphic_function1!(ceil_nanosecond, TimestampTz, TimestampTz, TimestampTz);

#[doc(hidden)]
pub fn tumble_Timestamp_ShortInterval(ts: Timestamp, i: ShortInterval) -> Timestamp {
    let ts_us = ts.microseconds();
    let i_us = i.microseconds();
    let round = ts_us - ts_us % i_us;
    Timestamp::from_microseconds(round)
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
pub fn tumble_TimestampTz_ShortInterval(ts: TimestampTz, i: ShortInterval) -> TimestampTz {
    tumble_Timestamp_ShortInterval(ts.into(), i).into()
}

some_polymorphic_function2!(
    tumble,
    TimestampTz,
    TimestampTz,
    ShortInterval,
    ShortInterval,
    TimestampTz
);

#[doc(hidden)]
pub fn tumble_Timestamp_ShortInterval_Time(ts: Timestamp, i: ShortInterval, t: Time) -> Timestamp {
    let t_us = (t.nanoseconds() / 1000) as i64;
    let ts_us = ts.microseconds() - t_us;
    let i_us = i.microseconds();
    let round = ts_us - ts_us % i_us;
    Timestamp::from_microseconds(round + t_us)
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
pub fn tumble_TimestampTz_ShortInterval_Time(
    ts: TimestampTz,
    i: ShortInterval,
    t: Time,
) -> TimestampTz {
    tumble_Timestamp_ShortInterval_Time(ts.into(), i, t).into()
}

some_polymorphic_function3!(
    tumble,
    TimestampTz,
    TimestampTz,
    ShortInterval,
    ShortInterval,
    Time,
    Time,
    TimestampTz
);

#[doc(hidden)]
pub fn tumble_Timestamp_ShortInterval_ShortInterval(
    ts: Timestamp,
    i: ShortInterval,
    t: ShortInterval,
) -> Timestamp {
    let t_us = t.microseconds();
    let ts_us = ts.microseconds() - t_us;
    let i_us = i.microseconds();
    let round = ts_us - ts_us % i_us;
    Timestamp::from_microseconds(round + t_us)
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

#[doc(hidden)]
pub fn tumble_TimestampTz_ShortInterval_ShortInterval(
    ts: TimestampTz,
    i: ShortInterval,
    t: ShortInterval,
) -> TimestampTz {
    tumble_Timestamp_ShortInterval_ShortInterval(ts.into(), i, t).into()
}

some_polymorphic_function3!(
    tumble,
    TimestampTz,
    TimestampTz,
    ShortInterval,
    ShortInterval,
    ShortInterval,
    ShortInterval,
    TimestampTz
);

// Start of first interval which contains ts, in microseconds
#[doc(hidden)]
pub fn hop_start(
    ts: Timestamp,
    period: ShortInterval,
    size: ShortInterval,
    start: ShortInterval,
) -> i64 {
    let ts_us = ts.microseconds();
    let size_us = size.microseconds();
    let period_us = period.microseconds();
    let start_us = start.microseconds();
    ts_us - ((ts_us - start_us) % period_us) + period_us - size_us
}

// Helper function used by the monotonicity analysis for hop table functions
#[doc(hidden)]
pub fn hop_start_timestamp(
    ts: Timestamp,
    period: ShortInterval,
    size: ShortInterval,
    start: ShortInterval,
) -> Timestamp {
    Timestamp::from_microseconds(hop_start(ts, period, size, start))
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
    while add < size.microseconds() {
        result.push(Timestamp::from_microseconds(round + add));
        add += period.microseconds();
    }
    result.into()
}

#[doc(hidden)]
pub fn hop_TimestampTz_ShortInterval_ShortInterval_ShortInterval(
    ts: TimestampTz,
    period: ShortInterval,
    size: ShortInterval,
    start: ShortInterval,
) -> Array<TimestampTz> {
    hop_Timestamp_ShortInterval_ShortInterval_ShortInterval(ts.into(), period, size, start)
        .deref()
        .iter()
        .map(|t: &Timestamp| (*t).into())
        .collect::<Vec<TimestampTz>>()
        .into()
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
pub fn hop_TimestampTzN_ShortInterval_ShortInterval_ShortInterval(
    ts: Option<TimestampTz>,
    period: ShortInterval,
    size: ShortInterval,
    start: ShortInterval,
) -> Array<TimestampTz> {
    match ts {
        None => Vec::new().into(),
        Some(ts) => {
            hop_TimestampTz_ShortInterval_ShortInterval_ShortInterval(ts, period, size, start)
        }
    }
}

#[doc(hidden)]
pub fn parse_timestamp__(format: SqlString, st: SqlString) -> Option<Timestamp> {
    let nt = NaiveDateTime::parse_from_str(st.str(), format.str());
    match nt {
        Ok(nt) => Some(Timestamp::from_naiveDateTime(nt)),
        Err(e) => match e.kind() {
            ParseErrorKind::BadFormat | ParseErrorKind::NotEnough | ParseErrorKind::Impossible => {
                panic!(
                    "Invalid format in PARSE_TIMESTAMP: '{}'\n{}",
                    format.str(),
                    e
                )
            }
            _ => None,
        },
    }
}

pub fn parse_timestampN_(format: Option<SqlString>, st: SqlString) -> Option<Timestamp> {
    let format = format?;
    parse_timestamp__(format, st)
}

pub fn parse_timestamp_N(format: SqlString, st: Option<SqlString>) -> Option<Timestamp> {
    let st = st?;
    parse_timestamp__(format, st)
}

pub fn parse_timestampNN(format: Option<SqlString>, st: Option<SqlString>) -> Option<Timestamp> {
    let st = st?;
    let format = format?;
    parse_timestamp__(format, st)
}

/////////////////////////////////////////////////////////////////////////////////////////////
// Date

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
    IsNone,
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
    pub const fn from_days(days: i32) -> Self {
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
        Timestamp::from_milliseconds((self.days as i64) * 86400 * 1000)
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

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let dt = self.to_date();
        let month = dt.month();
        let day = dt.day();
        let year = dt.year();
        write!(f, "{}-{:02}-{:02}", year, month, day)
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

impl<'de, AUX> DeserializeWithContext<'de, SqlSerdeConfig, AUX> for Date {
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
                Ok(Self::from_days(
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
        Ok(Self::from_days(
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

#[doc(hidden)]
pub fn floor_millennium_Date(value: Date) -> Date {
    let d = value.to_date();
    let naive = NaiveDate::from_ymd_opt((d.year() / 1000) * 1000, 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(floor_millennium, Date, Date, Date);

#[doc(hidden)]
pub fn floor_century_Date(value: Date) -> Date {
    let d = value.to_date();
    let naive = NaiveDate::from_ymd_opt((d.year() / 100) * 100, 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(floor_century, Date, Date, Date);

#[doc(hidden)]
pub fn floor_decade_Date(value: Date) -> Date {
    let d = value.to_date();
    let naive = NaiveDate::from_ymd_opt((d.year() / 10) * 10, 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(floor_decade, Date, Date, Date);

#[doc(hidden)]
pub fn floor_year_Date(value: Date) -> Date {
    let d = value.to_date();
    let naive = NaiveDate::from_ymd_opt(d.year(), 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(floor_year, Date, Date, Date);

#[doc(hidden)]
pub fn floor_quarter_Date(value: Date) -> Date {
    let d = value.to_date();
    let naive = NaiveDate::from_ymd_opt(d.year(), ((d.month() - 1) / 3) * 3 + 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(floor_quarter, Date, Date, Date);

#[doc(hidden)]
pub fn floor_month_Date(value: Date) -> Date {
    let d = value.to_date();
    let naive = NaiveDate::from_ymd_opt(d.year(), d.month(), 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(floor_month, Date, Date, Date);

#[doc(hidden)]
pub fn floor_week_Date(value: Date) -> Date {
    let wd = extract_dow_Date(value) - Date::first_day_of_week();
    let interval = ShortInterval::from_seconds(wd * 86400);
    minus_Date_Date_ShortInterval__(value, interval)
}

some_polymorphic_function1!(floor_week, Date, Date, Date);

#[doc(hidden)]
pub fn floor_day_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(floor_day, Date, Date, Date);

#[doc(hidden)]
pub fn floor_hour_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(floor_hour, Date, Date, Date);

#[doc(hidden)]
pub fn floor_minute_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(floor_minute, Date, Date, Date);

#[doc(hidden)]
pub fn floor_second_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(floor_second, Date, Date, Date);

#[doc(hidden)]
pub fn floor_millisecond_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(floor_millisecond, Date, Date, Date);

#[doc(hidden)]
pub fn floor_microsecond_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(floor_microsecond, Date, Date, Date);

#[doc(hidden)]
pub fn floor_nanosecond_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(floor_nanosecond, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_millennium_Date(value: Date) -> Date {
    let d = value.to_date();
    if d.year() % 1000 == 0 && d.month() == 1 && d.day() == 1 {
        return value;
    }
    let naive = NaiveDate::from_ymd_opt((d.year() / 1000 + 1) * 1000, 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(ceil_millennium, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_century_Date(value: Date) -> Date {
    let d = value.to_date();
    if d.year() % 100 == 0 && d.month() == 1 && d.day() == 1 {
        return value;
    }
    let naive = NaiveDate::from_ymd_opt((d.year() / 100 + 1) * 100, 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(ceil_century, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_decade_Date(value: Date) -> Date {
    let d = value.to_date();
    if d.year() % 10 == 0 && d.month() == 1 && d.day() == 1 {
        return value;
    }
    let naive = NaiveDate::from_ymd_opt((d.year() / 10 + 1) * 10, 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(ceil_decade, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_year_Date(value: Date) -> Date {
    let d = value.to_date();
    if d.month() == 1 && d.day() == 1 {
        return value;
    }
    let naive = NaiveDate::from_ymd_opt(d.year() + 1, 1, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(ceil_year, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_quarter_Date(value: Date) -> Date {
    let d = value.to_date();
    if (d.month() - 1).is_multiple_of(3) && d.day() == 1 {
        return value;
    }
    let next_quarter_month = match d.month() {
        1..=3 => 4,
        4..=6 => 7,
        7..=9 => 10,
        10..=12 => 1,
        _ => unreachable!(),
    };
    let year = if next_quarter_month == 1 {
        d.year() + 1
    } else {
        d.year()
    };
    let naive = NaiveDate::from_ymd_opt(year, next_quarter_month, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(ceil_quarter, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_month_Date(value: Date) -> Date {
    let d = value.to_date();
    if d.day() == 1 {
        return value;
    }
    let next_month = if d.month() == 12 { 1 } else { d.month() + 1 };
    let next_year = if next_month == 1 {
        d.year() + 1
    } else {
        d.year()
    };
    let naive = NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
    Date::from_date(naive)
}

some_polymorphic_function1!(ceil_month, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_week_Date(value: Date) -> Date {
    let wd = extract_dow_Date(value) - Date::first_day_of_week();
    if wd == 0 {
        return value;
    }
    let interval = ShortInterval::from_seconds((7 - wd) * 86400);
    plus_Date_Date_ShortInterval__(value, interval)
}

some_polymorphic_function1!(ceil_week, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_day_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(ceil_day, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_hour_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(ceil_hour, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_minute_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(ceil_minute, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_second_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(ceil_second, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_millisecond_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(ceil_millisecond, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_microsecond_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(ceil_microsecond, Date, Date, Date);

#[doc(hidden)]
pub fn ceil_nanosecond_Date(value: Date) -> Date {
    value
}

some_polymorphic_function1!(ceil_nanosecond, Date, Date, Date);

// right - left
#[doc(hidden)]
pub fn minus_LongInterval_Date_Date__(left: Date, right: Date) -> LongInterval {
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

    LongInterval::from_months(months * neg)
}

some_function2!(minus_LongInterval_Date_Date, Date, Date, LongInterval);

#[doc(hidden)]
pub fn minus_ShortInterval_Date_Date__(left: Date, right: Date) -> ShortInterval {
    let ld = left.days() as i64;
    let rd = right.days() as i64;
    ShortInterval::from_seconds((ld - rd) * 86400)
}

some_function2!(minus_ShortInterval_Date_Date, Date, Date, ShortInterval);

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

#[doc(hidden)]
pub fn extract_nanosecond_Date(_value: Date) -> i64 {
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
some_polymorphic_function1!(extract_nanosecond, Date, Date, i64);
some_polymorphic_function1!(extract_second, Date, Date, i64);
some_polymorphic_function1!(extract_minute, Date, Date, i64);
some_polymorphic_function1!(extract_hour, Date, Date, i64);

#[doc(hidden)]
pub fn datediff_day_Date_Date(left: Date, right: Date) -> i32 {
    left.days() - right.days()
}

some_polymorphic_function2!(datediff_day, Date, Date, Date, Date, i32);

#[doc(hidden)]
pub fn format_date__(format: SqlString, date: Date) -> SqlString {
    SqlString::from(date.to_dateTime().format(format.str()).to_string())
}

some_function2!(format_date, SqlString, Date, SqlString);

#[doc(hidden)]
pub fn format_time__(format: SqlString, date: Time) -> SqlString {
    SqlString::from(date.to_time().format(format.str()).to_string())
}

some_function2!(format_time, SqlString, Time, SqlString);

#[doc(hidden)]
pub fn format_timestamp__(format: SqlString, date: Timestamp) -> SqlString {
    SqlString::from(date.to_naiveDateTime().format(format.str()).to_string())
}

some_function2!(format_timestamp, SqlString, Timestamp, SqlString);

#[doc(hidden)]
pub fn parse_date__(format: SqlString, st: SqlString) -> Option<Date> {
    let nd = NaiveDate::parse_from_str(st.str(), format.str());
    match nd {
        Ok(nd) => Some(Date::from_date(nd)),
        Err(e) => match e.kind() {
            ParseErrorKind::BadFormat | ParseErrorKind::NotEnough | ParseErrorKind::Impossible => {
                panic!("Invalid format in PARSE_DATE: {}\n{}", format.str(), e)
            }
            _ => None,
        },
    }
}

pub fn parse_dateN_(format: Option<SqlString>, st: SqlString) -> Option<Date> {
    let format = format?;
    parse_date__(format, st)
}

pub fn parse_date_N(format: SqlString, st: Option<SqlString>) -> Option<Date> {
    let st = st?;
    parse_date__(format, st)
}

pub fn parse_dateNN(format: Option<SqlString>, st: Option<SqlString>) -> Option<Date> {
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
pub fn date_trunc_millennium_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_millennium_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn date_trunc_century_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_century_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_century_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_century_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn date_trunc_decade_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_decade_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_decade_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_decade_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn date_trunc_year_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_year_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_year_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_year_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn date_trunc_quarter_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_quarter_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_quarter_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_quarter_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn date_trunc_month_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_month_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_month_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_month_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn date_trunc_week_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_week_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_week_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_week_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn date_trunc_day_Timestamp(timestamp: Timestamp) -> Timestamp {
    let dt = timestamp.get_date();
    let dt = date_trunc_day_Date(dt);
    Timestamp::from_date(dt)
}

#[doc(hidden)]
pub fn date_trunc_day_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_day_Timestamp(timestamp.into()).into()
}

some_polymorphic_function1!(date_trunc_millennium, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_century, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_decade, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_year, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_quarter, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_month, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_week, Timestamp, Timestamp, Timestamp);
some_polymorphic_function1!(date_trunc_day, Timestamp, Timestamp, Timestamp);

some_polymorphic_function1!(date_trunc_millennium, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(date_trunc_century, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(date_trunc_decade, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(date_trunc_year, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(date_trunc_quarter, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(date_trunc_month, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(date_trunc_week, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(date_trunc_day, TimestampTz, TimestampTz, TimestampTz);

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
pub fn timestamp_trunc_quarter_Timestamp(timestamp: Timestamp) -> Timestamp {
    date_trunc_quarter_Timestamp(timestamp)
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
pub fn timestamp_trunc_millennium_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_millennium_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_century_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_century_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_decade_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_decade_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_year_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_year_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_quarter_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_quarter_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_month_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_month_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_week_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_week_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_day_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    date_trunc_day_Timestamp(timestamp.into()).into()
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

#[doc(hidden)]
pub fn timestamp_trunc_hour_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    timestamp_trunc_hour_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_minute_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    timestamp_trunc_minute_Timestamp(timestamp.into()).into()
}

#[doc(hidden)]
pub fn timestamp_trunc_second_TimestampTz(timestamp: TimestampTz) -> TimestampTz {
    timestamp_trunc_second_Timestamp(timestamp.into()).into()
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

some_polymorphic_function1!(
    timestamp_trunc_millennium,
    TimestampTz,
    TimestampTz,
    TimestampTz
);
some_polymorphic_function1!(
    timestamp_trunc_century,
    TimestampTz,
    TimestampTz,
    TimestampTz
);
some_polymorphic_function1!(
    timestamp_trunc_decade,
    TimestampTz,
    TimestampTz,
    TimestampTz
);
some_polymorphic_function1!(timestamp_trunc_year, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(timestamp_trunc_month, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(timestamp_trunc_week, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(timestamp_trunc_day, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(timestamp_trunc_hour, TimestampTz, TimestampTz, TimestampTz);
some_polymorphic_function1!(
    timestamp_trunc_minute,
    TimestampTz,
    TimestampTz,
    TimestampTz
);
some_polymorphic_function1!(
    timestamp_trunc_second,
    TimestampTz,
    TimestampTz,
    TimestampTz
);

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
pub fn date_trunc_quarter_Date(date: Date) -> Date {
    let date = date.to_dateTime();
    let month = match date.month() {
        1..=3 => 1,
        4..=6 => 4,
        7..=9 => 7,
        10..=12 => 10,
        _ => unreachable!(),
    };
    let rounded = NaiveDate::from_ymd_opt(date.year(), month, 1).unwrap();
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
some_polymorphic_function1!(date_trunc_quarter, Date, Date, Date);
some_polymorphic_function1!(date_trunc_month, Date, Date, Date);
some_polymorphic_function1!(date_trunc_week, Date, Date, Date);
some_polymorphic_function1!(date_trunc_day, Date, Date, Date);

/////////////////////////////////////////////////////////////////////////////////////////////
// Time

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
    IsNone,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Time {
    nanoseconds: u64,
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let dt = self.to_time();
        let hr = dt.hour();
        let min = dt.minute();
        let sec = dt.second();
        let nanos = dt.nanosecond();
        if nanos != 0 {
            write!(f, "{:02}:{:02}:{:02}.{:09}", hr, min, sec, nanos)
        } else {
            write!(f, "{:02}:{:02}:{:02}", hr, min, sec)
        }
    }
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
    pub const fn from_nanoseconds(nanoseconds: u64) -> Self {
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

        Time::from_nanoseconds(rem.unsigned_abs())
    }
}

impl Sub<ShortInterval> for Time {
    type Output = Self;

    #[doc(hidden)]
    fn sub(self, rhs: ShortInterval) -> Self::Output {
        self + (ShortInterval::from_microseconds(-rhs.microseconds()))
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

impl<'de, AUX> DeserializeWithContext<'de, SqlSerdeConfig, AUX> for Time {
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

#[doc(hidden)]
pub fn floor_millennium_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(floor_millennium, Time, Time, Time);

#[doc(hidden)]
pub fn floor_century_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(floor_century, Time, Time, Time);

#[doc(hidden)]
pub fn floor_decade_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(floor_decade, Time, Time, Time);

#[doc(hidden)]
pub fn floor_year_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(floor_year, Time, Time, Time);

#[doc(hidden)]
pub fn floor_quarter_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(floor_quarter, Time, Time, Time);

#[doc(hidden)]
pub fn floor_month_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(floor_month, Time, Time, Time);

#[doc(hidden)]
pub fn floor_week_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(floor_week, Time, Time, Time);

#[doc(hidden)]
pub fn floor_day_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(floor_day, Time, Time, Time);

#[doc(hidden)]
pub fn floor_hour_Time(value: Time) -> Time {
    time_trunc_hour_Time(value)
}

some_polymorphic_function1!(floor_hour, Time, Time, Time);

#[doc(hidden)]
pub fn floor_minute_Time(value: Time) -> Time {
    time_trunc_minute_Time(value)
}

some_polymorphic_function1!(floor_minute, Time, Time, Time);

#[doc(hidden)]
pub fn floor_second_Time(value: Time) -> Time {
    time_trunc_second_Time(value)
}

some_polymorphic_function1!(floor_second, Time, Time, Time);

#[doc(hidden)]
pub fn floor_millisecond_Time(value: Time) -> Time {
    time_trunc_millisecond_Time(value)
}

some_polymorphic_function1!(floor_millisecond, Time, Time, Time);

#[doc(hidden)]
pub fn floor_microsecond_Time(value: Time) -> Time {
    time_trunc_microsecond_Time(value)
}

some_polymorphic_function1!(floor_microsecond, Time, Time, Time);

#[doc(hidden)]
pub fn floor_nanosecond_Time(value: Time) -> Time {
    time_trunc_nanosecond_Time(value)
}

some_polymorphic_function1!(floor_nanosecond, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_millennium_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(ceil_millennium, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_century_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(ceil_century, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_decade_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(ceil_decade, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_year_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(ceil_year, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_quarter_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(ceil_quarter, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_month_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(ceil_month, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_week_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(ceil_week, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_day_Time(value: Time) -> Time {
    // This makes no sense, but this is the result from Calcite
    value
}

some_polymorphic_function1!(ceil_day, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_hour_Time(value: Time) -> Time {
    let t = value.to_time();
    if t.minute() == 0 && t.second() == 0 && t.nanosecond() == 0 {
        return value;
    }
    let result = NaiveTime::from_hms_opt((t.hour() + 1) % 24, 0, 0).unwrap();
    Time::from_time(result)
}

some_polymorphic_function1!(ceil_hour, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_minute_Time(value: Time) -> Time {
    let t = value.to_time();
    if t.second() == 0 && t.nanosecond() == 0 {
        return value;
    }
    let min = (t.minute() + 1) % 60;
    let hour = if min == 0 {
        (t.hour() + 1) % 24
    } else {
        t.hour()
    };
    let result = NaiveTime::from_hms_opt(hour, min, 0).unwrap();
    Time::from_time(result)
}

some_polymorphic_function1!(ceil_minute, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_second_Time(value: Time) -> Time {
    let t = value.to_time();
    if t.nanosecond() == 0 {
        return value;
    }
    let sec = (t.second() + 1) % 60;
    let min = if sec == 0 {
        (t.minute() + 1) % 60
    } else {
        t.minute()
    };
    let hour = if sec == 0 && min == 0 {
        (t.hour() + 1) % 24
    } else {
        t.hour()
    };
    let result = NaiveTime::from_hms_opt(hour, min, sec).unwrap();
    Time::from_time(result)
}

fn make_time(h: u32, m: u32, s: u32, n: u32) -> Time {
    let mut hour = h;
    let mut minute = m;
    let mut second = s;
    let mut nanos = n;
    if nanos >= 1_000_000_000 {
        nanos = 0;
        second += 1;
        if second >= 60 {
            second = 0;
            minute += 1;
            if minute >= 60 {
                minute = 0;
                hour += 1;
                if hour >= 24 {
                    hour = 0;
                }
            }
        }
    }

    let t = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos).unwrap();
    Time::from_time(t)
}

some_polymorphic_function1!(ceil_second, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_millisecond_Time(value: Time) -> Time {
    let t = value.to_time();
    let nanos = t.nanosecond();
    let millis = nanos / 1_000_000;
    let remainder = nanos % 1_000_000;
    let ceil_millis = if remainder > 0 { millis + 1 } else { millis };

    let nanos = ceil_millis * 1_000_000;
    make_time(t.hour(), t.minute(), t.second(), nanos)
}

some_polymorphic_function1!(ceil_millisecond, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_microsecond_Time(value: Time) -> Time {
    let t = value.to_time();
    let nanos = t.nanosecond();
    let micros = nanos / 1_000;
    let remainder = nanos % 1_000;
    let ceil_micros = if remainder > 0 { micros + 1 } else { micros };

    let nanos = ceil_micros * 1_000;
    make_time(t.hour(), t.minute(), t.second(), nanos)
}

some_polymorphic_function1!(ceil_microsecond, Time, Time, Time);

#[doc(hidden)]
pub fn ceil_nanosecond_Time(value: Time) -> Time {
    value
}

some_polymorphic_function1!(ceil_nanosecond, Time, Time, Time);

#[doc(hidden)]
pub fn extract_millisecond_Time(value: Time) -> i64 {
    let time = value.to_time();
    (time.second() * 1000 + time.nanosecond() / 1_000_000).into()
}

#[doc(hidden)]
pub fn extract_microsecond_Time(value: Time) -> i64 {
    let time = value.to_time();
    time.second() as i64 * 1_000_000i64 + (time.nanosecond() as i64 / 1000)
}

#[doc(hidden)]
pub fn extract_nanosecond_Time(value: Time) -> i64 {
    let time = value.to_time();
    (time.second() as i64) * 1_000_000_000i64 + time.nanosecond() as i64
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

some_polymorphic_function1!(extract_millisecond, Time, Time, i64);
some_polymorphic_function1!(extract_microsecond, Time, Time, i64);
some_polymorphic_function1!(extract_nanosecond, Time, Time, i64);
some_polymorphic_function1!(extract_second, Time, Time, i64);
some_polymorphic_function1!(extract_minute, Time, Time, i64);
some_polymorphic_function1!(extract_hour, Time, Time, i64);

#[doc(hidden)]
pub fn parse_time__(format: SqlString, st: SqlString) -> Option<Time> {
    let nt = NaiveTime::parse_from_str(st.str(), format.str());
    match nt {
        Ok(nt) => Some(Time::from_time(nt)),
        Err(e) => match e.kind() {
            ParseErrorKind::BadFormat | ParseErrorKind::NotEnough | ParseErrorKind::Impossible => {
                panic!("Invalid format in PARSE_TIME: {}\n{}", format.str(), e)
            }
            _ => None,
        },
    }
}

pub fn parse_timeN_(format: Option<SqlString>, st: SqlString) -> Option<Time> {
    let format = format?;
    parse_time__(format, st)
}

pub fn parse_time_N(format: SqlString, st: Option<SqlString>) -> Option<Time> {
    let st = st?;
    parse_time__(format, st)
}

pub fn parse_timeNN(format: Option<SqlString>, st: Option<SqlString>) -> Option<Time> {
    let st = st?;
    let format = format?;
    parse_time__(format, st)
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use super::{Date, Time, Timestamp, TimestampTz};
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
        timestampTz: TimestampTz,
    }

    deserialize_table_record!(TestStruct["TestStruct", 4] {
        (date, "date", false, Date, |_| None),
        (time, "time", false, Time, |_| None),
        (timestamp, "timestamp", false, Timestamp, |_| None),
        (timestampTz, "timestampTz", false, TimestampTz, |_| None)
    });
    serialize_table_record!(TestStruct[4] {
        date["date"]: Date,
        time["time"]: Time,
        timestamp["timestamp"]:  Timestamp,
        timestampTz["timestampTz"]:  TimestampTz
    });

    static DEFAULT_CONFIG: LazyLock<SqlSerdeConfig> = LazyLock::new(SqlSerdeConfig::default);
    static DEBEZIUM_CONFIG: LazyLock<SqlSerdeConfig> =
        LazyLock::new(|| SqlSerdeConfig::from(JsonFlavor::DebeziumMySql));
    static SNOWFLAKE_CONFIG: LazyLock<SqlSerdeConfig> =
        LazyLock::new(|| SqlSerdeConfig::from(JsonFlavor::Snowflake));

    fn deserialize_with_default_config<'de, T>(json: &'de str) -> Result<T, serde_json::Error>
    where
        T: DeserializeWithContext<'de, SqlSerdeConfig, ()>,
    {
        T::deserialize_with_context(
            &mut serde_json::Deserializer::from_str(json),
            &DEFAULT_CONFIG,
        )
    }

    fn deserialize_with_debezium_config<'de, T>(json: &'de str) -> Result<T, serde_json::Error>
    where
        T: DeserializeWithContext<'de, SqlSerdeConfig, ()>,
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
                r#"{"date": 16816, "time": 10800000000, "timestamp": "2018-06-20T13:37:03Z", "timestampTz": "2018-06-20T13:37:03Z" }"#
            )
            .unwrap(),
            TestStruct {
                date: Date::from_days(16816),
                time: Time::from_nanoseconds(10800000000000),
                timestamp: Timestamp::from_milliseconds(1529501823000),
                timestampTz: TimestampTz::from_milliseconds(1529501823000),
            }
        );
    }

    #[test]
    fn snowflake_json() {
        assert_eq!(
            serialize_with_snowflake_config(&TestStruct {
                date: Date::from_days(19628),
                time: Time::from_nanoseconds(84075123000000),
                timestamp: Timestamp::from_milliseconds(1529501823000),
                timestampTz: TimestampTz::from_milliseconds(1529501823000),
            })
            .unwrap(),
            r#"{"date":"2023-09-28","time":"23:21:15.123","timestamp":"2018-06-20T13:37:03+00:00","timestampTz":"2018-06-20T13:37:03+00:00"}"#
        );
    }

    #[test]
    fn default_json() {
        assert_eq!(
            deserialize_with_default_config::<TestStruct>(
                r#"{"date": "2023-09-28", "time": "23:21:15.123", "timestamp": "2018-06-20 13:37:03", "timestampTz": "2018-06-20 13:37:03+00:00"}"#
            )
            .unwrap(),
            TestStruct {
                date: Date::from_days(19628),
                time: Time::from_nanoseconds(84075123000000),
                timestamp: Timestamp::from_milliseconds(1529501823000),
                timestampTz: TimestampTz::from_milliseconds(1529501823000),
            }
        );

        assert_eq!(
            serialize_with_default_config(&TestStruct {
                date: Date::from_days(19628),
                time: Time::from_nanoseconds(84075123000000),
                timestamp: Timestamp::from_milliseconds(1529501823000),
                timestampTz: TimestampTz::from_milliseconds(1529501823000),
            })
            .unwrap(),
            r#"{"date":"2023-09-28","time":"23:21:15.123","timestamp":"2018-06-20 13:37:03","timestampTz":"2018-06-20 13:37:03+00:00"}"#
        );
    }
}

num_entries_scalar! {
    Timestamp,
    Date,
    Time,
    TimestampTz,
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

#[doc(hidden)]
pub fn time_trunc_millisecond_Time(time: Time) -> Time {
    let t = time.to_time();
    let n = NaiveTime::from_hms_nano_opt(
        t.hour(),
        t.minute(),
        t.second(),
        (t.nanosecond() / 1_000_000) * 1_000_000,
    )
    .unwrap();
    Time::from_time(n)
}

#[doc(hidden)]
pub fn time_trunc_microsecond_Time(time: Time) -> Time {
    let t = time.to_time();
    let n = NaiveTime::from_hms_nano_opt(
        t.hour(),
        t.minute(),
        t.second(),
        (t.nanosecond() / 1_000) * 1_000,
    )
    .unwrap();
    Time::from_time(n)
}

#[doc(hidden)]
pub fn time_trunc_nanosecond_Time(time: Time) -> Time {
    time
}

some_polymorphic_function1!(time_trunc_hour, Time, Time, Time);
some_polymorphic_function1!(time_trunc_minute, Time, Time, Time);
some_polymorphic_function1!(time_trunc_second, Time, Time, Time);
some_polymorphic_function1!(time_trunc_millisecond, Time, Time, Time);
some_polymorphic_function1!(time_trunc_microsecond, Time, Time, Time);
some_polymorphic_function1!(time_trunc_nanosecond, Time, Time, Time);

#[doc(hidden)]
#[inline(always)]
pub fn plus_Time_Time_ShortInterval__(left: Time, right: ShortInterval) -> Time {
    left + right
}

some_function2!(plus_Time_Time_ShortInterval, Time, ShortInterval, Time);

#[doc(hidden)]
#[inline(always)]
pub fn minus_Time_Time_ShortInterval__(left: Time, right: ShortInterval) -> Time {
    left - right
}

some_function2!(minus_Time_Time_ShortInterval, Time, ShortInterval, Time);

#[doc(hidden)]
#[inline(always)]
pub fn plus_Time_Time_LongInterval__(left: Time, _: LongInterval) -> Time {
    left
}

some_function2!(plus_Time_Time_LongInterval, Time, LongInterval, Time);

#[doc(hidden)]
#[inline(always)]
pub fn minus_Time_Time_LongInterval__(left: Time, _: LongInterval) -> Time {
    left
}

some_function2!(minus_Time_Time_LongInterval, Time, LongInterval, Time);
