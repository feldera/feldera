//! A human-readable duration type for the pipeline configuration.
//!
//! Every duration-shaped field in the pipeline configuration uses [`ConfigDuration`],
//! so that one format covers seconds, milliseconds, microseconds, and days
//! instead of a different unit baked into each field name.
//!
//! The syntax is the one Go's [`time.ParseDuration`] documents, which many
//! tools accept and users already know. Feldera extends it with a `d` (day)
//! unit, because configuration values such as checkpoint retention are
//! naturally expressed in days, and narrows it by rejecting a negative
//! duration, which no configuration setting means.
//!
//! [`time.ParseDuration`]: https://pkg.go.dev/time#ParseDuration

use std::{cmp::Ordering, fmt, str::FromStr, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// A duration written as a number and a unit, such as `500ms`, `30s`, `1h30m`
/// or `30d`.
///
/// Accepted units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, `h`, and `d`. A
/// value may chain several terms, as in `1h30m`, and the terms are summed. A
/// bare `0` is also accepted.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Default)]
pub struct ConfigDuration(Duration);

const EXPECTED_UNITS: &str = "expected one of 'ns', 'us', '\u{00b5}s', 'ms', 's', 'm', 'h', or 'd'";

/// Errors returned when parsing a [`ConfigDuration`] from a string.
///
/// Every variant quotes the text it rejected, so that a configuration error
/// tells the reader which value to correct.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ParseError {
    /// The string holds no number at all.
    #[error("empty duration: write `0`, or a number and a unit such as `500ms`")]
    Empty,

    /// The string carries a unit that does not exist.
    #[error("unknown unit `{unit}` in duration `{input}`: {EXPECTED_UNITS}")]
    InvalidUnit { unit: String, input: String },

    /// A term carries a unit but no number in front of it, as in `s`.
    #[error("`{unit}` in duration `{input}` has no number in front of it")]
    MissingNumber { unit: String, input: String },

    /// The string carries a number with no unit after it.
    #[error("`{number}` in duration `{input}` carries no unit: {EXPECTED_UNITS}")]
    NoUnit { number: String, input: String },

    /// The string carries a leading `-`. Configuration durations cannot be
    /// negative, unlike Go's and Kubernetes' durations.
    #[error("duration `{input}` cannot be negative")]
    Negative { input: String },

    /// The value does not fit in a [`Duration`].
    #[error("duration `{input}` is too large")]
    Overflow { input: String },
}

const NANOS_PER_SECOND: u128 = 1_000_000_000;

const SECOND: Duration = Duration::from_secs(1);
const MINUTE: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(60 * 60);
const DAY: Duration = Duration::from_secs(24 * 60 * 60);

impl ConfigDuration {
    pub const ZERO: ConfigDuration = ConfigDuration(Duration::ZERO);

    pub const fn from_nanos(nanos: u64) -> Self {
        Self(Duration::from_nanos(nanos))
    }

    pub const fn from_micros(micros: u64) -> Self {
        Self(Duration::from_micros(micros))
    }

    pub const fn from_millis(millis: u64) -> Self {
        Self(Duration::from_millis(millis))
    }

    pub const fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs))
    }

    /// A duration of `days` days, or the longest one there is when that many
    /// days do not fit in a `u64` of seconds.
    pub const fn from_days(days: u64) -> Self {
        Self(Duration::from_secs(days.saturating_mul(24 * 60 * 60)))
    }

    pub const fn as_micros(self) -> u128 {
        self.0.as_micros()
    }

    pub const fn as_millis(self) -> u128 {
        self.0.as_millis()
    }

    pub const fn as_secs(self) -> u64 {
        self.0.as_secs()
    }

    pub fn as_secs_f64(self) -> f64 {
        self.0.as_secs_f64()
    }

    pub const fn is_zero(self) -> bool {
        self.0.is_zero()
    }
}

impl From<Duration> for ConfigDuration {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl From<ConfigDuration> for Duration {
    fn from(ConfigDuration(duration): ConfigDuration) -> Self {
        duration
    }
}

impl PartialEq<Duration> for ConfigDuration {
    fn eq(&self, other: &Duration) -> bool {
        self.0 == *other
    }
}

impl PartialOrd for ConfigDuration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConfigDuration {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd<Duration> for ConfigDuration {
    fn partial_cmp(&self, other: &Duration) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

/// Every accepted unit, with its length in nanoseconds.
///
/// Longest first, so that a scan matching the table in order cannot take `m`
/// from the front of `ms`.
const UNITS: [(&str, u128); 9] = [
    ("ns", 1),
    ("us", 1_000),
    // U+00B5 is the "micro sign", U+03BC is "Greek small letter mu". Both are
    // written for microseconds in the wild, so both are accepted.
    ("\u{00b5}s", 1_000),
    ("\u{03bc}s", 1_000),
    ("ms", 1_000_000),
    ("s", NANOS_PER_SECOND),
    ("m", 60 * NANOS_PER_SECOND),
    ("h", 60 * 60 * NANOS_PER_SECOND),
    ("d", 24 * 60 * 60 * NANOS_PER_SECOND),
];

/// How many digits after the point are read.
///
/// A fraction scales as an exact ratio, so the digits and the power of ten
/// below them both have to fit in a `u128` alongside the unit. Eighteen digits
/// leave room to spare, and the first digit dropped is worth less than a
/// millionth of a nanosecond even for a term counted in days.
const MAX_FRACTION_DIGITS: usize = 18;

/// Reads a run of decimal digits, or `None` if they do not fit in a `u128`.
fn digits_to_u128(digits: &str) -> Option<u128> {
    digits.bytes().try_fold(0u128, |acc, byte| {
        acc.checked_mul(10)?.checked_add(u128::from(byte - b'0'))
    })
}

impl FromStr for ConfigDuration {
    type Err = ParseError;

    /// Reads the format described on [`ConfigDuration`]: a run of terms, each a
    /// number and a unit, summed together.
    ///
    /// The scan walks the string once. Each term contributes
    /// `whole * unit + round(fraction * unit)`, accumulated in nanoseconds as a
    /// `u128`, so no step goes through floating point and a value near the top
    /// of the representable range keeps every digit.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s;
        let overflow = || ParseError::Overflow {
            input: input.to_string(),
        };

        if input.starts_with('-') {
            return Err(ParseError::Negative {
                input: input.to_string(),
            });
        }
        // At most one sign, so a run of them is not a number.
        let body = input.strip_prefix('+').unwrap_or(input);
        if body.is_empty() {
            return Err(ParseError::Empty);
        }
        // A lone zero is the one value that carries no unit.
        if body == "0" {
            return Ok(ConfigDuration::ZERO);
        }

        let bytes = body.as_bytes();
        let mut at = 0;
        let mut total: u128 = 0;

        while at < bytes.len() {
            let number_at = at;

            // A number: digits, then at most one point and more digits.
            let whole_at = at;
            while at < bytes.len() && bytes[at].is_ascii_digit() {
                at += 1;
            }
            let whole = &body[whole_at..at];

            let mut fraction = "";
            let has_point = at < bytes.len() && bytes[at] == b'.';
            if has_point {
                at += 1;
                let fraction_at = at;
                while at < bytes.len() && bytes[at].is_ascii_digit() {
                    at += 1;
                }
                fraction = &body[fraction_at..at];
            }
            let number = &body[number_at..at];

            // The unit runs to whatever starts the next number. This always
            // advances: a byte that stops the unit scan starts a number.
            let unit_at = at;
            while at < bytes.len() && !bytes[at].is_ascii_digit() && bytes[at] != b'.' {
                at += 1;
            }
            let unit = &body[unit_at..at];

            if whole.is_empty() && fraction.is_empty() {
                // Nothing numeric here at all. Which half is wrong depends on
                // whether the rest is a unit: `s` is one and wants a number in
                // front of it, `forever` is not one at all.
                let known = UNITS.iter().any(|(name, _)| *name == unit);
                return Err(if known {
                    ParseError::MissingNumber {
                        unit: unit.to_string(),
                        input: input.to_string(),
                    }
                } else {
                    ParseError::InvalidUnit {
                        unit: unit.to_string(),
                        input: input.to_string(),
                    }
                });
            }
            // A point with no digits behind it, as in `5.s`, is a whole number,
            // as it is in Go. `1..2s` still fails below: the second point
            // starts a number where a unit belongs.
            if unit.is_empty() {
                return Err(ParseError::NoUnit {
                    number: number.to_string(),
                    input: input.to_string(),
                });
            }
            let Some(&(_, unit_nanos)) = UNITS.iter().find(|(name, _)| *name == unit) else {
                return Err(ParseError::InvalidUnit {
                    unit: unit.to_string(),
                    input: input.to_string(),
                });
            };

            let mut term = digits_to_u128(whole)
                .ok_or_else(overflow)?
                .checked_mul(unit_nanos)
                .ok_or_else(overflow)?;

            if !fraction.is_empty() {
                // An exact ratio rather than a floating-point product: `0.5s`
                // is `unit_nanos * 5 / 10`. The quotient is rounded to the
                // nearest nanosecond, halves away from zero.
                let kept = fraction.len().min(MAX_FRACTION_DIGITS);
                let numerator = digits_to_u128(&fraction[..kept]).ok_or_else(overflow)?;
                let denominator = 10u128.pow(kept as u32);
                let scaled = unit_nanos.checked_mul(numerator).ok_or_else(overflow)?;
                let rounded = (scaled + denominator / 2) / denominator;
                term = term.checked_add(rounded).ok_or_else(overflow)?;
            }

            total = total.checked_add(term).ok_or_else(overflow)?;
        }

        let seconds = u64::try_from(total / NANOS_PER_SECOND).map_err(|_| overflow())?;
        Ok(ConfigDuration(Duration::new(
            seconds,
            (total % NANOS_PER_SECOND) as u32,
        )))
    }
}

impl fmt::Display for ConfigDuration {
    /// Writes the `ConfigDuration` as a sequence of unit terms, largest first,
    /// e.g. `ConfigDuration::from_secs(100)` writes `1m40s`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut remain = self.0.as_nanos();
        if remain == 0 {
            return f.write_str("0s");
        }

        for (unit, base) in [
            ("d", DAY),
            ("h", HOUR),
            ("m", MINUTE),
            ("s", SECOND),
            ("ms", Duration::from_millis(1)),
            ("us", Duration::from_micros(1)),
            ("ns", Duration::from_nanos(1)),
        ] {
            let base = base.as_nanos();
            let unit_mult = remain / base;
            if unit_mult != 0 {
                remain %= base;
                write!(f, "{unit_mult}{unit}")?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for ConfigDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for ConfigDuration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ConfigDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = ConfigDuration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration such as \"500ms\", \"30s\", \"1h30m\" or \"30d\"")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                value.parse::<ConfigDuration>().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

// No `ToSchema` implementation. Each field of this type carries
// `#[schema(value_type = Option<String>)]` instead, which renders the field as
// an inline OpenAPI string and keeps the field's own description. A schema for
// this type would make every such field a reference to one shared schema, and
// `utoipa` drops a field's description once it becomes a reference. Each field's
// doc comment carries an example, and the accepted format is documented once
// under `docs/pipelines/configuration.mdx`.

/// The unit in which a setting's superseded numeric spelling was written.
///
/// Each duration setting once took a bare number, and the unit lived in the
/// field name (`clock_resolution_usecs`) or only in the documentation
/// (`retention_min_age`, which counted days). Those numbers still parse, so the
/// unit has to survive somewhere; it lives here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyUnit {
    Micros,
    Millis,
    Secs,
    Days,
}

impl LegacyUnit {
    /// The length one unit stands for, in nanoseconds.
    pub const fn nanos(self) -> u64 {
        match self {
            LegacyUnit::Micros => 1_000,
            LegacyUnit::Millis => 1_000_000,
            LegacyUnit::Secs => 1_000_000_000,
            LegacyUnit::Days => 24 * 60 * 60 * 1_000_000_000,
        }
    }
}

/// A [`ConfigDuration`] of `nanos` nanoseconds, or the longest one there is.
///
/// Saturating rather than failing, because every caller converts a value an
/// older release already accepted. `max_output_buffer_time_millis` defaulted to
/// `usize::MAX`, and that default was written into every output connector
/// configuration, so rejecting it would stop those pipelines from loading at
/// all.
fn duration_from_nanos_saturating(nanos: u128) -> ConfigDuration {
    const NANOS_PER_SECOND_U128: u128 = NANOS_PER_SECOND;
    match u64::try_from(nanos / NANOS_PER_SECOND_U128) {
        Ok(seconds) => ConfigDuration(Duration::new(
            seconds,
            (nanos % NANOS_PER_SECOND_U128) as u32,
        )),
        Err(_) => ConfigDuration(Duration::MAX),
    }
}

/// Warns, once per setting per process, that a value arrived as a bare number,
/// and returns the duration that number stands for.
///
/// The warning quotes the equivalent in the current format, so that the reader
/// can correct the configuration by copying the text out of the log.
///
/// Warning once rather than on every value is deliberate: this runs inside a
/// `deserialize_with` function, and the pipeline manager re-parses every
/// pipeline's stored configuration on each automaton poll, so warning per parse
/// would fill the log at the polling rate.
pub fn duration_from_legacy_number<E: de::Error>(
    warned: &std::sync::atomic::AtomicBool,
    field: &str,
    unit: LegacyUnit,
    value: u64,
) -> Result<ConfigDuration, E> {
    let duration = duration_from_nanos_saturating(u128::from(value) * u128::from(unit.nanos()));
    // `u64::MAX` is the "no limit" an older release wrote out on its own, as
    // `max_output_buffer_time_millis` in every output connector's stored and
    // checkpointed configuration. Warning about it would tell the reader to
    // write a duration they never chose.
    if value != u64::MAX && !warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
        log::warn!(
            "configuration field `{field}` was given the bare number {value}, which is \
             deprecated and will be removed at the 1.0 release; write \
             `\"{duration}\"` instead"
        );
    }
    Ok(duration)
}

/// The whole number a JSON or YAML number stands for, or `None` if it is not a
/// non-negative whole number that fits in a `u64`. JSON and YAML have one
/// number type, so a whole number may arrive as a float; a fraction of the
/// legacy unit never had a meaning.
fn whole_number(value: f64) -> Option<u64> {
    // `u64::MAX as f64` rounds up to 2^64, so compare against 2^64 itself with
    // `<`: `<=` would let 2^64 through and saturate it to `u64::MAX`.
    const TWO_TO_THE_64: f64 = 18_446_744_073_709_551_616.0;
    (value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value < TWO_TO_THE_64)
        .then_some(value as u64)
}

/// Deserializes a duration setting that also accepts the bare number its
/// earlier spelling took.
///
/// A string is parsed as a [`ConfigDuration`]; a number is read in `unit`. Both
/// spellings therefore reach one field, and the configuration written back out
/// always carries the current one.
pub fn duration_or_legacy_number<'de, D>(
    deserializer: D,
    warned: &std::sync::atomic::AtomicBool,
    field: &'static str,
    unit: LegacyUnit,
) -> Result<Option<ConfigDuration>, D::Error>
where
    D: Deserializer<'de>,
{
    struct Visitor<'a> {
        warned: &'a std::sync::atomic::AtomicBool,
        field: &'static str,
        unit: LegacyUnit,
    }

    impl Visitor<'_> {
        /// Says what was wrong and quotes it, so the reader can find the value.
        fn not_a_number<E: de::Error>(field: &str, value: &str) -> E {
            de::Error::custom(format!(
                "`{field}` expects a duration such as \"30s\", or a whole number of the unit \
                 its earlier spelling took; `{value}` is neither"
            ))
        }
    }

    impl<'de> de::Visitor<'de> for Visitor<'_> {
        type Value = Option<ConfigDuration>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a duration such as \"500ms\", \"30s\" or \"1h30m\"")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            value
                .parse::<ConfigDuration>()
                .map(Some)
                .map_err(de::Error::custom)
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
            duration_from_legacy_number(self.warned, self.field, self.unit, value).map(Some)
        }

        fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
            let value = u64::try_from(value)
                .map_err(|_| de::Error::custom(format!("`{}` cannot be negative", self.field)))?;
            self.visit_u64(value)
        }

        fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
            match whole_number(value) {
                Some(whole) => self.visit_u64(whole),
                None => Err(Self::not_a_number(self.field, &value.to_string())),
            }
        }

        /// A number that is not an integer, when it comes from `serde_json`.
        ///
        /// This workspace turns on `serde_json`'s `arbitrary_precision`, which
        /// makes such a number reach `deserialize_any` as a one-entry map
        /// holding the literal text rather than as an `f64`. Without this the
        /// value never reaches [`Self::visit_f64`] and the reader is told
        /// "invalid type: map", which names nothing they wrote.
        fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            const NUMBER: &str = "$serde_json::private::Number";

            let Some(key) = map.next_key::<String>()? else {
                return Err(Self::not_a_number(self.field, "{}"));
            };
            if key != NUMBER {
                return Err(Self::not_a_number(self.field, "an object"));
            }
            // Quote the literal as written: `10.50` reformatted as an `f64`
            // would read `10.5`, which is not what the reader wrote.
            let text = map.next_value::<String>()?;
            match text.parse::<f64>().ok().and_then(whole_number) {
                Some(whole) => self.visit_u64(whole),
                None => Err(Self::not_a_number(self.field, &text)),
            }
        }

        /// An explicit `null`. The nullable settings keep it distinct from an
        /// absent key; the rest never reach this, because a `null` on them is
        /// the same as leaving the key out.
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }

    deserializer.deserialize_any(Visitor {
        warned,
        field,
        unit,
    })
}

/// Deserializes a duration setting whose earlier spelling was a
/// [`Duration`], which serde writes as a `{secs, nanos}` object.
///
/// NATS's `max_expires` was the only setting shaped this way.
pub fn duration_or_legacy_struct<'de, D>(
    deserializer: D,
    warned: &std::sync::atomic::AtomicBool,
    field: &'static str,
) -> Result<Option<ConfigDuration>, D::Error>
where
    D: Deserializer<'de>,
{
    struct Visitor<'a> {
        warned: &'a std::sync::atomic::AtomicBool,
        field: &'static str,
    }

    impl<'de> de::Visitor<'de> for Visitor<'_> {
        type Value = Option<ConfigDuration>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // Serde builds its "invalid type" messages around this, and they
            // are what a reader sees for a value of the wrong shape. A bare
            // number is not listed, because this setting never had one.
            f.write_str("a duration such as \"30s\", or a {secs, nanos} object")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            value
                .parse::<ConfigDuration>()
                .map(Some)
                .map_err(de::Error::custom)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        /// The superseded spelling, a serde-written [`Duration`].
        ///
        /// `serde_json`'s `arbitrary_precision`, which this workspace enables,
        /// also delivers a non-integer number as a map, so the two are told
        /// apart by their key.
        fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            const NUMBER: &str = "$serde_json::private::Number";

            let (mut secs, mut nanos) = (0u64, 0u32);
            let mut saw_field = false;
            while let Some(key) = map.next_key::<String>()? {
                match key.as_str() {
                    "secs" => {
                        secs = map.next_value()?;
                        saw_field = true;
                    }
                    "nanos" => {
                        nanos = map.next_value()?;
                        saw_field = true;
                    }
                    NUMBER => {
                        let text = map.next_value::<String>()?;
                        return Err(de::Error::invalid_type(
                            de::Unexpected::Other(&format!("the number {text}")),
                            &self,
                        ));
                    }
                    other => {
                        return Err(de::Error::unknown_field(other, &["secs", "nanos"]));
                    }
                }
            }
            if !saw_field {
                return Err(de::Error::invalid_type(de::Unexpected::Map, &self));
            }

            let duration = duration_from_nanos_saturating(
                u128::from(secs) * NANOS_PER_SECOND + u128::from(nanos),
            );
            if !self.warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
                log::warn!(
                    "configuration field `{}` was given a `{{secs, nanos}}` object, which is \
                     deprecated and will be removed at the 1.0 release; write \
                     `\"{duration}\"` instead",
                    self.field
                );
            }
            Ok(Some(duration))
        }
    }

    // Written out rather than derived through `#[serde(untagged)]`: an untagged
    // enum reports every failure as "data did not match any variant of untagged
    // enum Either", which leaks an internal name and quotes nothing the reader
    // wrote. Serde's own "invalid type" messages, built from `expecting` above,
    // name the value instead.
    deserializer.deserialize_any(Visitor { warned, field })
}

/// Like [`duration_setting!`], for the setting whose earlier spelling was a
/// `{secs, nanos}` object rather than a bare number.
#[doc(hidden)]
#[macro_export]
macro_rules! duration_struct_setting {
    ($name:ident, $field:literal) => {
        fn $name<'de, D>(
            deserializer: D,
        ) -> ::std::result::Result<Option<$crate::duration::ConfigDuration>, D::Error>
        where
            D: ::serde::Deserializer<'de>,
        {
            static WARNED: ::std::sync::atomic::AtomicBool =
                ::std::sync::atomic::AtomicBool::new(false);
            $crate::duration::duration_or_legacy_struct(deserializer, &WARNED, $field)
        }
    };
}

/// Defines a `deserialize_with` function for a duration setting.
///
/// The setting accepts the current spelling, a duration string, and the bare
/// number its earlier spelling took, read in `unit`. Pair it with
/// `#[serde(default, alias = "<old key>", deserialize_with = "<name>")]`.
///
/// ```ignore
/// duration_setting!(clock_resolution, "clock_resolution", LegacyUnit::Micros);
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! duration_setting {
    ($name:ident, $field:literal, $unit:expr) => {
        fn $name<'de, D>(
            deserializer: D,
        ) -> ::std::result::Result<Option<$crate::duration::ConfigDuration>, D::Error>
        where
            D: ::serde::Deserializer<'de>,
        {
            static WARNED: ::std::sync::atomic::AtomicBool =
                ::std::sync::atomic::AtomicBool::new(false);
            $crate::duration::duration_or_legacy_number(deserializer, &WARNED, $field, $unit)
        }
    };
}

/// Like [`duration_setting!`], for a setting whose explicit `null` carries
/// meaning of its own, such as "never do this".
///
/// serde maps both a missing key and an explicit `null` to `None`, and these
/// settings need them apart. The generated function yields
/// `Option<Option<ConfigDuration>>`: `None` when the key is absent, `Some(None)` for
/// an explicit `null`, `Some(Some(value))` otherwise.
#[doc(hidden)]
#[macro_export]
macro_rules! nullable_duration_setting {
    ($name:ident, $field:literal, $unit:expr) => {
        fn $name<'de, D>(
            deserializer: D,
        ) -> ::std::result::Result<Option<Option<$crate::duration::ConfigDuration>>, D::Error>
        where
            D: ::serde::Deserializer<'de>,
        {
            static WARNED: ::std::sync::atomic::AtomicBool =
                ::std::sync::atomic::AtomicBool::new(false);
            $crate::duration::duration_or_legacy_number(deserializer, &WARNED, $field, $unit)
                .map(Some)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_same_as_go() {
        // Test cases from Go's `time.ParseDuration` test suite.
        for (input, expected) in [
            ("0", Duration::ZERO),
            ("5s", 5 * SECOND),
            ("30s", 30 * SECOND),
            ("1478s", 1478 * SECOND),
            ("100ns", Duration::from_nanos(100)),
            ("11us", Duration::from_micros(11)),
            ("11\u{00b5}s", Duration::from_micros(11)),
            ("11\u{03bc}s", Duration::from_micros(11)),
            ("1ms", Duration::from_millis(1)),
            // Go takes a point with digits on only one side.
            ("5.s", 5 * SECOND),
            (".5s", Duration::from_millis(500)),
            ("1h", HOUR),
            ("1m", MINUTE),
            ("3h30m", 3 * HOUR + 30 * MINUTE),
            (
                "10.5s4m",
                4 * MINUTE + 10 * SECOND + Duration::from_millis(500),
            ),
            (
                "1h2m3s4ms5us6ns",
                HOUR + 2 * MINUTE
                    + 3 * SECOND
                    + Duration::from_millis(4)
                    + Duration::from_micros(5)
                    + Duration::from_nanos(6),
            ),
            (
                "39h9m14.425s",
                39 * HOUR + 9 * MINUTE + 14 * SECOND + Duration::from_millis(425),
            ),
            ("0.3333333333333333333h", 20 * MINUTE),
        ] {
            assert_eq!(
                input.parse::<ConfigDuration>().unwrap(),
                ConfigDuration(expected),
                "parsing {input:?}"
            );
        }
    }

    #[test]
    fn parses_days() {
        assert_eq!(
            "30d".parse::<ConfigDuration>().unwrap(),
            ConfigDuration::from_days(30)
        );
        assert_eq!(
            "1d12h".parse::<ConfigDuration>().unwrap(),
            ConfigDuration(DAY + 12 * HOUR)
        );
    }

    #[test]
    fn rejects_bad_input() {
        for input in [
            // A sign with nothing behind it, and more signs than Go allows.
            "",
            "+",
            "++1s",
            "-1s", // A number with no unit, anywhere in the string.
            "10",
            "1s0",
            "1h30", // A unit that does not exist, or a stray separator.
            "10y",
            "5sec",
            "abc",
            "1 s",
            "5s ",
            "1..2s",
            // Too large for `Duration`, which once panicked here.
            "99999999999999999999999999s",
            "184467440737095516160s",
            "9223372036854775807d",
            "1000000000000000000000d",
        ] {
            assert!(
                input.parse::<ConfigDuration>().is_err(),
                "{input:?} should not parse, got {:?}",
                input.parse::<ConfigDuration>()
            );
        }
    }

    /// A term with a unit but no number says so, rather than reporting an
    /// unknown unit for a unit that plainly exists.
    #[test]
    fn a_unit_with_no_number_says_so() {
        for (input, expected) in [
            ("s", "`s` in duration `s` has no number in front of it"),
            ("ms", "`ms` in duration `ms` has no number in front of it"),
            ("forever", "unknown unit `forever`"),
            // The unit scan runs to the next digit, so a space belongs to the
            // unit and the whole token is what is unknown.
            ("1h ms", "unknown unit `h ms`"),
        ] {
            let message = input.parse::<ConfigDuration>().unwrap_err().to_string();
            assert!(
                message.contains(expected),
                "parsing {input:?} reported {message:?}"
            );
        }
    }

    /// Every rejection names the value it rejected, so that a configuration
    /// error tells the reader what to correct.
    #[test]
    fn rejection_messages_quote_the_input() {
        for (input, expected) in [
            ("-1s", "duration `-1s` cannot be negative"),
            ("10", "`10` in duration `10` carries no unit"),
            ("5sec", "unknown unit `sec` in duration `5sec`"),
            // The space lands inside the unit, and the message shows it.
            ("1 s", "unknown unit ` s` in duration `1 s`"),
            ("99999999999999999999999999s", "is too large"),
            ("", "empty duration"),
        ] {
            let message = input.parse::<ConfigDuration>().unwrap_err().to_string();
            assert!(
                message.contains(expected),
                "parsing {input:?} reported {message:?}, which omits {expected:?}"
            );
        }
    }

    /// The edge of the representable range: `u64::MAX` seconds and a
    /// fraction of a second below the next one parse and print back exactly,
    /// and one nanosecond more is rejected rather than wrapped.
    #[test]
    fn accepts_the_largest_representable_duration() {
        let max = format!("{}s999999999ns", u64::MAX);
        let parsed: ConfigDuration = max.parse().unwrap();
        assert_eq!(parsed, ConfigDuration::from(Duration::MAX));
        assert_eq!(
            parsed.to_string().parse::<ConfigDuration>().unwrap(),
            parsed
        );
        let over = format!("{}s1000000000ns", u64::MAX);
        assert!(matches!(
            over.parse::<ConfigDuration>(),
            Err(ParseError::Overflow { .. })
        ));
    }

    /// A superseded spelling accepts a whole number however JSON spells it,
    /// and says what is wrong with anything else.
    ///
    /// This workspace enables `serde_json`'s `arbitrary_precision`, which hands
    /// a non-integer literal to `deserialize_any` as a map rather than an
    /// `f64`. Without a `visit_map` the reader is told "invalid type: map",
    /// naming nothing they wrote, so each spelling below is pinned. YAML still
    /// delivers a real `f64`, and is covered too.
    #[test]
    fn a_superseded_spelling_reads_any_whole_number() {
        #[derive(Debug, serde::Deserialize)]
        struct Config {
            #[serde(default, alias = "nap_secs", deserialize_with = "duration_nap")]
            nap: Option<ConfigDuration>,
        }
        crate::duration_setting!(duration_nap, "nap", LegacyUnit::Secs);

        for json in [
            r#"{"nap_secs": 10}"#,
            r#"{"nap_secs": 10.0}"#,
            r#"{"nap_secs": 1e1}"#,
        ] {
            let config: Config = serde_json::from_str(json).unwrap();
            assert_eq!(
                config.nap,
                Some(ConfigDuration::from_secs(10)),
                "parsing {json}"
            );
        }
        // `u64::MAX` is the largest bare number; 2^64 and above are refused, even
        // though `u64::MAX as f64` rounds to exactly 2^64.
        let config: Config = serde_json::from_str(r#"{"nap_secs": 18446744073709551615}"#).unwrap();
        assert!(config.nap.is_some());
        for json in [
            r#"{"nap_secs": 18446744073709551616}"#,
            r#"{"nap_secs": 18446744073709551617}"#,
            r#"{"nap_secs": 1.8446744073709552e19}"#,
        ] {
            let error = serde_json::from_str::<Config>(json)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("is neither"),
                "parsing {json} reported {error}"
            );
        }

        let yaml: Config = serde_yaml::from_str("nap_secs: 10.0").unwrap();
        assert_eq!(yaml.nap, Some(ConfigDuration::from_secs(10)));

        for (bad, quoted) in [
            (r#"{"nap_secs": 10.5}"#, "`10.5`"),
            (r#"{"nap_secs": -5}"#, "cannot be negative"),
            (r#"{"nap_secs": {}}"#, "`{}`"),
            // Quoted as written, not as an `f64` would print it.
            (r#"{"nap_secs": 10.50}"#, "`10.50`"),
            (r#"{"nap_secs": {"secs": 1}}"#, "`an object`"),
        ] {
            let error = serde_json::from_str::<Config>(bad).unwrap_err().to_string();
            assert!(error.contains(quoted), "parsing {bad} reported {error}");
            assert!(
                !error.contains("invalid type: map"),
                "parsing {bad} reported {error}"
            );
        }
        let error = serde_yaml::from_str::<Config>("nap_secs: 10.5")
            .unwrap_err()
            .to_string();
        assert!(error.contains("`10.5`"), "{error}");
    }

    /// More days than a `u64` of seconds holds saturate rather than wrapping
    /// in release builds or panicking in debug ones.
    #[test]
    fn from_days_saturates() {
        assert_eq!(
            ConfigDuration::from_days(2),
            ConfigDuration::from_secs(2 * 86_400)
        );
        assert_eq!(
            ConfigDuration::from_days(u64::MAX),
            ConfigDuration::from_secs(u64::MAX)
        );
    }

    /// The "no limit" value an older release wrote out on its own is read
    /// without a deprecation warning, which would otherwise quote a duration of
    /// hundreds of millions of years the reader never wrote. Any other bare
    /// number still warns.
    #[test]
    fn the_no_limit_value_is_read_without_a_warning() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let warned = AtomicBool::new(false);
        let max = duration_from_legacy_number::<serde_json::Error>(
            &warned,
            "max_output_buffer_time",
            LegacyUnit::Millis,
            u64::MAX,
        )
        .unwrap();
        assert_eq!(max, ConfigDuration::from_millis(u64::MAX));
        assert!(!warned.load(Ordering::Relaxed));

        duration_from_legacy_number::<serde_json::Error>(
            &warned,
            "max_output_buffer_time",
            LegacyUnit::Millis,
            500,
        )
        .unwrap();
        assert!(warned.load(Ordering::Relaxed));
    }

    /// A fraction scales as an exact ratio, not as a floating-point product.
    /// `f64` carries about sixteen significant digits, so the value below lands
    /// a nanosecond low when it goes through one; the parser must not.
    #[test]
    fn fractions_scale_exactly() {
        const DAY_NANOS: u128 = 24 * 60 * 60 * 1_000_000_000;
        let digits = "718656392856533598";
        let exact = (DAY_NANOS * digits.parse::<u128>().unwrap()
            + 10u128.pow(digits.len() as u32) / 2)
            / 10u128.pow(digits.len() as u32);
        assert_eq!(exact, 62_091_912_342_805);

        let parsed: ConfigDuration = format!("0.{digits}d").parse().unwrap();
        assert_eq!(Duration::from(parsed).as_nanos(), exact);

        // Halves go away from zero, and a whole part adds on top of the ratio.
        assert_eq!(
            "0.5s".parse::<ConfigDuration>().unwrap(),
            ConfigDuration::from_millis(500)
        );
        assert_eq!(
            "1.5s".parse::<ConfigDuration>().unwrap(),
            ConfigDuration::from_millis(1500)
        );
        assert_eq!(
            "0.0000000005s".parse::<ConfigDuration>().unwrap(),
            ConfigDuration::from_nanos(1)
        );
    }

    /// Both spellings of a setting reach one field, so a configuration that
    /// writes both is rejected rather than silently keeping one. The message
    /// names the current spelling, which is the field serde knows.
    #[test]
    fn writing_both_spellings_is_rejected() {
        #[derive(Debug, serde::Deserialize)]
        struct Config {
            #[serde(
                default,
                alias = "nap_secs",
                deserialize_with = "duration_nap",
                skip_serializing_if = "Option::is_none"
            )]
            nap: Option<ConfigDuration>,
        }
        crate::duration_setting!(duration_nap, "nap", LegacyUnit::Secs);

        assert_eq!(
            serde_json::from_str::<Config>(r#"{"nap": "30s"}"#)
                .unwrap()
                .nap,
            Some(ConfigDuration::from_secs(30))
        );
        assert_eq!(
            serde_json::from_str::<Config>(r#"{"nap_secs": 30}"#)
                .unwrap()
                .nap,
            Some(ConfigDuration::from_secs(30))
        );
        let error = serde_json::from_str::<Config>(r#"{"nap": "30s", "nap_secs": 30}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("duplicate field `nap`"), "{error}");
    }

    /// A duration of more than 2^53 seconds survives being written out and read
    /// back. Scaling such a value through `f64` used to land four minutes away,
    /// which the pipeline manager saw as a stored configuration changing value
    /// on its own. The two spellings below differ only in how the parser must
    /// reach the same number of seconds.
    #[test]
    fn large_values_round_trip_exactly() {
        let written = "128644735584426d23h5m38s";
        let parsed: ConfigDuration = written.parse().unwrap();
        assert_eq!(parsed.to_string(), written);
        assert_eq!(
            parsed.as_secs(),
            128_644_735_584_426 * 24 * 60 * 60 + 23 * 60 * 60 + 5 * 60 + 38
        );
        assert_eq!(
            "11114905154494489538s".parse::<ConfigDuration>().unwrap(),
            parsed
        );
    }

    #[test]
    fn displays_in_decomposed_units() {
        for (duration, expected) in [
            (ConfigDuration::ZERO, "0s"),
            (ConfigDuration::from_days(30), "30d"),
            (ConfigDuration::from_secs(3600), "1h"),
            (ConfigDuration::from_secs(100), "1m40s"),
            (ConfigDuration::from_millis(500), "500ms"),
            (ConfigDuration::from_micros(1_000_000), "1s"),
            (ConfigDuration::from_nanos(1), "1ns"),
            (ConfigDuration(HOUR + 30 * MINUTE + 10 * SECOND), "1h30m10s"),
        ] {
            assert_eq!(duration.to_string(), expected);
        }
    }

    #[test]
    fn round_trips_through_json() {
        for text in [
            "0s", "30d", "1h", "1m30s", "500ms", "1ns", "1h30m10s", "1d12h",
        ] {
            let json = format!("\"{text}\"");
            let duration: ConfigDuration = serde_json::from_str(&json).unwrap();
            assert_eq!(serde_json::to_string(&duration).unwrap(), json);
        }
    }

    /// Property-based tests.
    ///
    /// The tests above enumerate cases by hand and therefore cover only the
    /// inputs somebody thought of. These explore the input space instead, with
    /// generators weighted towards the edges of the representable range, where
    /// the parser's floating-point arithmetic once panicked.
    mod properties {
        use super::*;
        use proptest::prelude::*;

        /// Every unit the parser accepts, with its length in nanoseconds.
        const UNITS: [(&str, u64); 7] = [
            ("ns", 1),
            ("us", 1_000),
            ("ms", 1_000_000),
            ("s", 1_000_000_000),
            ("m", 60 * 1_000_000_000),
            ("h", 60 * 60 * 1_000_000_000),
            ("d", 24 * 60 * 60 * 1_000_000_000),
        ];

        /// Durations spanning the whole representable range, as
        /// `(seconds, subsecond nanoseconds)`.
        ///
        /// [`nanos`] reaches only `u64::MAX` nanoseconds, about 213503 days,
        /// and a whole region above it went untested: the pipeline manager
        /// stores durations of billions of days, and those are exactly the ones
        /// the parser's arithmetic used to round. The strategy names the top of
        /// the range explicitly, because a uniform `u64` rarely lands there.
        fn seconds_and_nanos() -> impl Strategy<Value = (u64, u32)> {
            let seconds = prop_oneof![
                1 => Just(0u64),
                1 => Just(u64::MAX),
                1 => Just(11_114_905_154_334_422_338u64),
                2 => 0u64..1_000,
                2 => (1u64 << 53)..u64::MAX,
                5 => any::<u64>(),
            ];
            (
                seconds,
                prop_oneof![1 => Just(0u32), 3 => 0u32..1_000_000_000],
            )
        }

        /// Nanosecond counts spanning the range `ConfigDuration::from_nanos` covers.
        ///
        /// A uniform `u64` almost never lands on zero, on one nanosecond, on
        /// the largest value, or below a second, which is where the formatter
        /// and the parser are likeliest to disagree, so the strategy names
        /// those regions explicitly.
        fn nanos() -> impl Strategy<Value = u64> {
            const DAY_NS: u64 = 24 * 60 * 60 * 1_000_000_000;
            prop_oneof![
                1 => Just(0u64),
                1 => Just(1u64),
                1 => Just(u64::MAX),
                1 => Just(DAY_NS),
                2 => 0u64..1_000,
                2 => 0u64..1_000_000_000,
                2 => 0u64..DAY_NS,
                6 => any::<u64>(),
            ]
        }

        /// Text drawn from the parser's own alphabet, plus arbitrary unicode.
        ///
        /// A uniformly random string is rejected at its first character and
        /// exercises nothing; these reach the number parser, the unit table and
        /// the overflow checks. The long digit runs matter most: a magnitude
        /// far past `Duration` used to panic inside the standard
        /// library rather than return an error.
        fn duration_like_text() -> impl Strategy<Value = String> {
            prop_oneof![
                2 => any::<String>(),
                3 => "[-+0-9. nsumhd\u{00b5}\u{03bc}]{0,40}",
                3 => "[0-9]{1,40}(ns|us|\u{00b5}s|\u{03bc}s|ms|s|m|h|d)",
                2 => "[0-9]{1,25}\\.[0-9]{1,25}(ns|ms|s|h|d)",
                2 => "[+-]?[0-9]{0,8}[a-z]{0,4}([0-9]{0,8}[a-z]{0,4}){0,3}",
            ]
        }

        /// Terms of a multi-term duration, as `(number, unit index)` pairs.
        ///
        /// The magnitudes stay small so that every sum is representable, which
        /// keeps the summation property free of discarded cases.
        fn terms() -> impl Strategy<Value = Vec<(u32, usize)>> {
            prop::collection::vec((0u32..1_000, 0usize..UNITS.len()), 1..6)
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Formatting a duration and parsing the result returns the same
            /// value. This is the central promise of the type: a configuration
            /// written back out reloads unchanged.
            #[test]
            fn display_round_trips(nanos in nanos()) {
                let duration = ConfigDuration::from_nanos(nanos);
                let text = duration.to_string();
                prop_assert_eq!(
                    text.parse::<ConfigDuration>().map_err(|e| e.to_string()),
                    Ok(duration),
                    "formatted as {:?}",
                    text
                );
            }

            /// Formatting and parsing back returns the same value across the
            /// whole representable range, not just the part `from_nanos`
            /// reaches. Durations above 2^53 seconds are where the parser's
            /// arithmetic has to stay exact.
            #[test]
            fn display_round_trips_across_the_whole_range(
                (seconds, nanos) in seconds_and_nanos()
            ) {
                let duration = ConfigDuration::from(Duration::new(seconds, nanos));
                let text = duration.to_string();
                prop_assert_eq!(
                    text.parse::<ConfigDuration>().map_err(|e| e.to_string()),
                    Ok(duration),
                    "formatted as {:?}",
                    text
                );
            }

            /// JSON is the format configurations travel in, and serde must
            /// agree with `FromStr`: the JSON string is the `Display` form, and
            /// parsing that text directly yields the same value.
            #[test]
            fn serde_agrees_with_from_str(nanos in nanos()) {
                let duration = ConfigDuration::from_nanos(nanos);
                let json = serde_json::to_string(&duration).unwrap();
                prop_assert_eq!(&json, &format!("\"{duration}\""));
                prop_assert_eq!(serde_json::from_str::<ConfigDuration>(&json).unwrap(), duration);
                prop_assert_eq!(
                    json.trim_matches('"').parse::<ConfigDuration>().unwrap(),
                    duration
                );
            }

            /// Parsing answers `Ok` or `Err` for any input and never panics.
            /// Overflowing values reach `Duration`, whose arithmetic
            /// panics on overflow, so the parser must keep them inside its own
            /// checked forms.
            #[test]
            fn parsing_never_panics(text in duration_like_text()) {
                let parsed = text.parse::<ConfigDuration>();
                // Whatever the outcome, the error message names the input, so
                // that a rejected configuration value tells the reader which
                // one to correct.
                if let Err(error) = parsed {
                    prop_assert!(!error.to_string().is_empty());
                }
            }

            /// A value written as several terms equals the sum of its terms
            /// parsed on their own, which is what chaining `1h30m` means.
            #[test]
            fn terms_sum(terms in terms()) {
                let mut written = String::new();
                let mut sum = ConfigDuration::ZERO;
                for (value, unit) in &terms {
                    let (unit, _) = UNITS[*unit];
                    let term = format!("{value}{unit}");
                    let term_duration: Duration = term.parse::<ConfigDuration>().unwrap().into();
                    sum = ConfigDuration::from(Duration::from(sum) + term_duration);
                    written.push_str(&term);
                }
                prop_assert_eq!(written.parse::<ConfigDuration>().unwrap(), sum);
            }

            /// The same quantity written in different units parses to the same
            /// value, so no unit's conversion factor has drifted.
            #[test]
            fn units_are_equivalent(count in 0u64..1_000_000) {
                let same = |smaller: String, larger: String| -> Result<(), TestCaseError> {
                    prop_assert_eq!(
                        smaller.parse::<ConfigDuration>().unwrap(),
                        larger.parse::<ConfigDuration>().unwrap(),
                        "{} and {} differ",
                        smaller,
                        larger
                    );
                    Ok(())
                };
                same(format!("{}ns", count * 1_000), format!("{count}us"))?;
                same(format!("{}us", count * 1_000), format!("{count}ms"))?;
                same(format!("{}ms", count * 1_000), format!("{count}s"))?;
                same(format!("{}s", count * 60), format!("{count}m"))?;
                same(format!("{}m", count * 60), format!("{count}h"))?;
                same(format!("{}h", count * 24), format!("{count}d"))?;
                // All three spellings of the microsecond unit agree.
                same(format!("{count}us"), format!("{count}\u{00b5}s"))?;
                same(format!("{count}us"), format!("{count}\u{03bc}s"))?;
            }

            /// Ordering follows the underlying nanosecond counts, so that
            /// comparisons of parsed configuration values mean what they read.
            #[test]
            fn ordering_follows_nanoseconds(left in nanos(), right in nanos()) {
                let parse = |nanos: u64| {
                    ConfigDuration::from_nanos(nanos)
                        .to_string()
                        .parse::<ConfigDuration>()
                        .unwrap()
                };
                prop_assert_eq!(parse(left).cmp(&parse(right)), left.cmp(&right));
            }

            /// A leading `+` is accepted and changes nothing, while a leading
            /// `-` is always rejected: configuration durations cannot run
            /// backwards.
            #[test]
            fn signs_behave(nanos in nanos()) {
                let text = ConfigDuration::from_nanos(nanos).to_string();
                prop_assert_eq!(
                    format!("+{text}").parse::<ConfigDuration>().unwrap(),
                    text.parse::<ConfigDuration>().unwrap()
                );
                prop_assert_eq!(
                    format!("-{text}").parse::<ConfigDuration>().unwrap_err(),
                    ParseError::Negative { input: format!("-{text}") }
                );
            }

            /// A fractional term rounds to the nearest nanosecond, halves up,
            /// rather than truncating as Go's parser does. Four digits after
            /// the point of a microsecond reach below one nanosecond, so the
            /// rounding is exercised.
            #[test]
            fn fractional_terms_round(micros in 0u64..1_000_000, fraction in 0u64..10_000) {
                let text = format!("{micros}.{fraction:04}us");
                let expected = ConfigDuration::from_nanos(micros * 1_000 + (fraction + 5) / 10);
                prop_assert_eq!(text.parse::<ConfigDuration>().unwrap(), expected);
            }
        }
    }
}
