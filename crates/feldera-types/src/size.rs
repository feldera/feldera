//! A human-readable size type for the pipeline configuration.
//!
//! Every size-shaped field in the pipeline configuration uses [`ConfigSize`],
//! so that one format covers bytes, megabytes and mebibytes instead of a
//! different unit baked into each field name.
//!
//! The syntax is the one Kubernetes documents for a resource [quantity], which
//! users of the Enterprise edition already write for their pods: a number and
//! a unit, decimal (`500M`) or binary (`512Mi`). Feldera narrows it by taking
//! one term only, no exponent form, and no negative or fractional-unit (`m`)
//! quantities, which no configuration setting accepts.
//!
//! [quantity]: https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity/

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// A size in bytes written as a number and a unit, such as `500M`, `512Mi`,
/// `1Gi` or `4096`.
///
/// Decimal units are `k`, `M`, `G`, `T`, `P` and `E`, each a thousand times
/// the one before; binary units are `Ki`, `Mi`, `Gi`, `Ti`, `Pi` and `Ei`,
/// each 1024 times the one before. A unit may carry a trailing `B`, as in
/// `512MiB`, and a bare number is a number of bytes.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ConfigSize(u64);

/// The unit list quoted in error messages. [`units_are_listed_in_messages`]
/// keeps it in step with [`UNITS`].
const EXPECTED_UNITS: &str = "expected one of 'k', 'M', 'G', 'T', 'P', 'E', 'Ki', 'Mi', 'Gi', \
                              'Ti', 'Pi', 'Ei', each with or without a trailing 'B'";

/// Errors returned when parsing a [`ConfigSize`] from a string.
///
/// Every variant quotes the text it rejected, so that a configuration error
/// tells the reader which value to correct.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ParseError {
    /// The string holds no number at all.
    #[error("empty size: write a number of bytes, or a number and a unit such as `512Mi`")]
    Empty,

    /// The string carries a unit that does not exist.
    #[error("unknown unit `{unit}` in size `{input}`: {EXPECTED_UNITS}")]
    InvalidUnit { unit: String, input: String },

    /// The string carries a unit but no number in front of it, as in `Mi`.
    #[error("`{unit}` in size `{input}` has no number in front of it")]
    MissingNumber { unit: String, input: String },
    /// A point with no digit on either side, such as `.`.
    #[error("size `{input}` has no digits")]
    NoDigits { input: String },

    /// The string carries a leading `-`. Configuration sizes cannot be
    /// negative, unlike Kubernetes quantities.
    #[error("size `{input}` cannot be negative")]
    Negative { input: String },

    /// The value does not fit in a `u64` of bytes.
    #[error("size `{input}` is too large: at most {} bytes", u64::MAX)]
    Overflow { input: String },
}

impl ConfigSize {
    pub const ZERO: ConfigSize = ConfigSize(0);

    /// The largest size there is, which several settings use for "no limit".
    pub const MAX: ConfigSize = ConfigSize(u64::MAX);

    pub const fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    /// A size of `kib` kibibytes, or the largest one when that overflows.
    pub const fn from_kib(kib: u64) -> Self {
        Self(kib.saturating_mul(1 << 10))
    }

    /// A size of `mib` mebibytes, or the largest one when that overflows.
    pub const fn from_mib(mib: u64) -> Self {
        Self(mib.saturating_mul(1 << 20))
    }

    /// A size of `gib` gibibytes, or the largest one when that overflows.
    pub const fn from_gib(gib: u64) -> Self {
        Self(gib.saturating_mul(1 << 30))
    }

    /// A size of `mb` megabytes, or the largest one when that overflows.
    pub const fn from_mb(mb: u64) -> Self {
        Self(mb.saturating_mul(1_000_000))
    }

    pub const fn as_bytes(self) -> u64 {
        self.0
    }

    /// The size as a `usize`, or `usize::MAX` where it does not fit: a limit
    /// past the address space is no limit at all.
    pub const fn as_usize(self) -> usize {
        if self.0 > usize::MAX as u64 {
            usize::MAX
        } else {
            self.0 as usize
        }
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub const fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }
}

impl From<u64> for ConfigSize {
    fn from(bytes: u64) -> Self {
        Self(bytes)
    }
}

impl From<ConfigSize> for u64 {
    fn from(ConfigSize(bytes): ConfigSize) -> Self {
        bytes
    }
}

impl PartialEq<u64> for ConfigSize {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u64> for ConfigSize {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

/// Every accepted unit, with its size in bytes. The binary units come first,
/// so that a scan over the table prefers `Mi` to `M` when it writes a size out.
const UNITS: [(&str, u128); 13] = [
    ("Ki", 1 << 10),
    ("Mi", 1 << 20),
    ("Gi", 1 << 30),
    ("Ti", 1 << 40),
    ("Pi", 1 << 50),
    ("Ei", 1 << 60),
    ("k", 1_000),
    // Kubernetes writes a kilobyte as `k` only, but the capital is what the
    // other decimal units use, so it is read too. It is never written.
    ("K", 1_000),
    ("M", 1_000_000),
    ("G", 1_000_000_000),
    ("T", 1_000_000_000_000),
    ("P", 1_000_000_000_000_000),
    ("E", 1_000_000_000_000_000_000),
];

/// How many digits after the point are read.
///
/// A fraction scales as an exact ratio, so the digits and the power of ten
/// below them both have to fit in a `u128` alongside the unit. Eighteen digits
/// leave room to spare, and the first digit dropped is worth less than a
/// billionth of a byte even for a term counted in exbibytes.
const MAX_FRACTION_DIGITS: usize = 18;

impl FromStr for ConfigSize {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_with_unit(s).map(|(size, _has_unit)| size)
    }
}

impl ConfigSize {
    /// Reads the format described on [`ConfigSize`] and says whether the text
    /// carried a unit: `"16G"` and `"4096B"` did, `"16000"` and `"1.5"` did
    /// not.
    ///
    /// A setting that once counted megabytes refuses a string with no unit,
    /// since `"16000"` would be sixteen kilobytes where `16000` meant sixteen
    /// gigabytes; this is how such a setting tells the two apart.
    pub fn from_str_with_unit(s: &str) -> Result<(Self, bool), ParseError> {
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

        // A number: digits, then at most one point and more digits.
        let whole_end = body
            .bytes()
            .position(|b| !b.is_ascii_digit())
            .unwrap_or(body.len());
        let (whole, mut rest) = body.split_at(whole_end);
        let mut fraction = "";
        if let Some(after_point) = rest.strip_prefix('.') {
            let fraction_end = after_point
                .bytes()
                .position(|b| !b.is_ascii_digit())
                .unwrap_or(after_point.len());
            (fraction, rest) = after_point.split_at(fraction_end);
        }
        let unit = rest;

        if whole.is_empty() && fraction.is_empty() {
            // Nothing numeric here at all. Which half is wrong depends on
            // whether the rest is a unit: `Mi` is one and wants a number in
            // front of it, `lots` is not one at all. A lone point is neither.
            return Err(if unit.is_empty() {
                ParseError::NoDigits {
                    input: input.to_string(),
                }
            } else if unit_bytes(unit).is_some() {
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
        let Some(bytes_per_unit) = unit_bytes(unit) else {
            return Err(ParseError::InvalidUnit {
                unit: unit.to_string(),
                input: input.to_string(),
            });
        };

        // `.5Gi` has no whole part.
        let whole: u128 = if whole.is_empty() {
            0
        } else {
            whole.parse().map_err(|_| overflow())?
        };
        let mut total = whole.checked_mul(bytes_per_unit).ok_or_else(overflow)?;

        if !fraction.is_empty() {
            // An exact ratio rather than a floating-point product: `1.5Gi` is
            // `bytes_per_unit * 15 / 10`. The quotient is rounded to the
            // nearest byte, halves away from zero.
            let kept = fraction.len().min(MAX_FRACTION_DIGITS);
            let numerator: u128 = fraction[..kept].parse().map_err(|_| overflow())?;
            let denominator = 10u128.pow(kept as u32);
            let scaled = bytes_per_unit.checked_mul(numerator).ok_or_else(overflow)?;
            let rounded = (scaled + denominator / 2) / denominator;
            total = total.checked_add(rounded).ok_or_else(overflow)?;
        }

        let size = u64::try_from(total)
            .map(ConfigSize)
            .map_err(|_| overflow())?;
        Ok((size, !unit.is_empty()))
    }
}

/// The number of bytes `unit` stands for: one for the empty unit or a lone
/// `B`, else the table entry, with or without a trailing `B`.
fn unit_bytes(unit: &str) -> Option<u128> {
    let unit = unit.strip_suffix('B').unwrap_or(unit);
    if unit.is_empty() {
        return Some(1);
    }
    UNITS
        .iter()
        .find(|(name, _)| *name == unit)
        .map(|&(_, bytes)| bytes)
}

impl fmt::Display for ConfigSize {
    /// Writes the size in the unit that divides it exactly with the smallest
    /// count, so `1073741824` writes `1Gi` and `1000000` writes `1M`, and a
    /// size no unit divides writes its number of bytes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = u128::from(self.0);
        if bytes == 0 {
            return f.write_str("0");
        }
        let mut best: (u128, &str) = (bytes, "");
        // Binary units lead the table, so on a tie they win.
        for &(unit, base) in UNITS.iter().filter(|(name, _)| *name != "K") {
            if bytes % base == 0 && bytes / base < best.0 {
                best = (bytes / base, unit);
            }
        }
        write!(f, "{}{}", best.0, best.1)
    }
}

impl fmt::Debug for ConfigSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for ConfigSize {
    /// Writes the [`Display`](fmt::Display) form, with a trailing `B` when that
    /// form has no unit: `0B` and `1181116006B`, not `0` and `1181116006`. A
    /// setting whose earlier spelling counted megabytes refuses a string with
    /// no unit, so a configuration written back out has to carry one to reload.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let text = self.to_string();
        if text.bytes().all(|b| b.is_ascii_digit()) {
            serializer.collect_str(&format_args!("{text}B"))
        } else {
            serializer.serialize_str(&text)
        }
    }
}

impl<'de> Deserialize<'de> for ConfigSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = ConfigSize;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a size such as \"500M\", \"512Mi\" or \"4096\"")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                value.parse::<ConfigSize>().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

// No `ToSchema` implementation, for the reason `ConfigDuration` gives: each
// field carries `#[schema(value_type = Option<String>)]` so that it keeps its
// own description in the OpenAPI document.

/// The unit in which a setting's superseded numeric spelling was written.
///
/// Each size setting once took a bare number, and the unit lived in the field
/// name (`max_rss_mb`, `cache_mib`, `min_storage_bytes`). Those numbers still
/// parse, so the unit has to survive somewhere; it lives here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacySizeUnit {
    Bytes,
    /// A million bytes, which the `_mb` settings and Kubernetes' `M` mean.
    Megabytes,
    /// 2^20 bytes, which the `_mib` settings mean.
    Mebibytes,
}

impl LegacySizeUnit {
    /// The number of bytes one unit stands for.
    pub const fn bytes(self) -> u64 {
        match self {
            LegacySizeUnit::Bytes => 1,
            LegacySizeUnit::Megabytes => 1_000_000,
            LegacySizeUnit::Mebibytes => 1 << 20,
        }
    }
}

/// Warns, once per setting per process, that a value arrived as a bare number,
/// and returns the size that number stands for, or the largest size when it
/// overflows.
///
/// The warning quotes the equivalent in the current format, so that the reader
/// can correct the configuration by copying the text out of the log. It is
/// issued once rather than on every value for the reason
/// [`crate::duration::duration_from_legacy_number`] gives: the pipeline
/// manager re-parses every stored configuration on each automaton poll.
pub fn size_from_legacy_number<E: de::Error>(
    warned: &std::sync::atomic::AtomicBool,
    field: &str,
    unit: LegacySizeUnit,
    value: u64,
) -> Result<ConfigSize, E> {
    let size = ConfigSize(value.saturating_mul(unit.bytes()));
    // `u64::MAX` is a "no limit" that an older release may have written out on
    // its own. Warning about it would tell the reader to write a size they
    // never wrote.
    if value != u64::MAX && !warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
        log::warn!(
            "configuration field `{field}` was given the bare number {value}, which is \
             deprecated and is removed no earlier than release 0.369.0; write \
             `\"{size}\"` instead"
        );
    }
    Ok(size)
}

/// The whole number a JSON or YAML number stands for, or `None` if it is not a
/// non-negative whole number that fits in a `u64`.
fn whole_number(value: f64) -> Option<u64> {
    // `u64::MAX as f64` rounds up to 2^64, so compare against 2^64 itself with
    // `<`: `<=` would let 2^64 through and saturate it to `u64::MAX`.
    const TWO_TO_THE_64: f64 = 18_446_744_073_709_551_616.0;
    (value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value < TWO_TO_THE_64)
        .then_some(value as u64)
}

/// Deserializes a size setting that also accepts the bare number its earlier
/// spelling took.
///
/// A string is parsed as a [`ConfigSize`]; a number is read in `unit`. Both
/// spellings therefore reach one field, and the configuration written back out
/// always carries the current one.
///
/// A string with no unit is refused unless `unit` is bytes: `"16000"` would
/// be sixteen kilobytes where `16000` is sixteen gigabytes, and nothing but
/// the quotes tells the two apart, so the setting asks for the unit instead.
pub fn size_or_legacy_number<'de, D>(
    deserializer: D,
    warned: &std::sync::atomic::AtomicBool,
    field: &'static str,
    unit: LegacySizeUnit,
) -> Result<Option<ConfigSize>, D::Error>
where
    D: Deserializer<'de>,
{
    struct Visitor<'a> {
        warned: &'a std::sync::atomic::AtomicBool,
        field: &'static str,
        unit: LegacySizeUnit,
    }

    impl Visitor<'_> {
        /// Says what was wrong and quotes it, so the reader can find the value.
        fn not_a_number<E: de::Error>(field: &str, value: &str) -> E {
            de::Error::custom(format!(
                "`{field}` expects a size such as \"512Mi\", or a whole number of the unit \
                 its earlier spelling took; `{value}` is neither"
            ))
        }
    }

    impl<'de> de::Visitor<'de> for Visitor<'_> {
        type Value = Option<ConfigSize>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a size such as \"500M\", \"512Mi\" or \"4096\"")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            let (size, has_unit) =
                ConfigSize::from_str_with_unit(value).map_err(de::Error::custom)?;
            if !has_unit && self.unit != LegacySizeUnit::Bytes {
                let unit = match self.unit {
                    LegacySizeUnit::Bytes => "",
                    LegacySizeUnit::Megabytes => "M",
                    LegacySizeUnit::Mebibytes => "Mi",
                };
                return Err(de::Error::custom(format!(
                    "`{}` expects a size with a unit, such as \"{value}{unit}\"; \"{value}\" \
                     has none, and a bare number counts {} only as the number {value}, \
                     without quotes",
                    self.field,
                    match self.unit {
                        LegacySizeUnit::Bytes => "bytes",
                        LegacySizeUnit::Megabytes => "megabytes",
                        LegacySizeUnit::Mebibytes => "mebibytes",
                    }
                )));
            }
            Ok(Some(size))
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
            size_from_legacy_number(self.warned, self.field, self.unit, value).map(Some)
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

        /// A number that is not an integer, when it comes from `serde_json`
        /// with `arbitrary_precision` on, which this workspace enables: it
        /// arrives as a one-entry map holding the literal text.
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
                // A whole number past `u64::MAX` is a number, only too large.
                None if text
                    .parse::<f64>()
                    .is_ok_and(|v| v >= 18_446_744_073_709_551_616.0) =>
                {
                    Err(de::Error::custom(format!(
                        "`{}` is too large: at most {} of its unit",
                        self.field,
                        u64::MAX
                    )))
                }
                None => Err(Self::not_a_number(self.field, &text)),
            }
        }

        /// An explicit `null`, the same as leaving the key out.
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

/// Like [`size_or_legacy_number`], for a setting that is not optional: a
/// `null` is refused rather than read as "unset".
pub fn required_size_or_legacy_number<'de, D>(
    deserializer: D,
    warned: &std::sync::atomic::AtomicBool,
    field: &'static str,
    unit: LegacySizeUnit,
) -> Result<ConfigSize, D::Error>
where
    D: Deserializer<'de>,
{
    size_or_legacy_number(deserializer, warned, field, unit)?
        .ok_or_else(|| de::Error::custom(format!("`{field}` cannot be null")))
}

/// Defines a serde deserialization function for a size setting that also
/// accepts the bare number its earlier spelling took.
///
/// `$name` is the function, `$field` the setting's current name as the warning
/// and the error messages quote it, and `$unit` the [`LegacySizeUnit`] a bare
/// number is read in.
#[doc(hidden)]
#[macro_export]
macro_rules! size_setting {
    ($name:ident, $field:literal, $unit:expr) => {
        fn $name<'de, D>(
            deserializer: D,
        ) -> ::std::result::Result<Option<$crate::size::ConfigSize>, D::Error>
        where
            D: ::serde::Deserializer<'de>,
        {
            static WARNED: ::std::sync::atomic::AtomicBool =
                ::std::sync::atomic::AtomicBool::new(false);
            $crate::size::size_or_legacy_number(deserializer, &WARNED, $field, $unit)
        }
    };
}

/// Like [`size_setting!`], for a setting that is not optional.
#[doc(hidden)]
#[macro_export]
macro_rules! required_size_setting {
    ($name:ident, $field:literal, $unit:expr) => {
        fn $name<'de, D>(
            deserializer: D,
        ) -> ::std::result::Result<$crate::size::ConfigSize, D::Error>
        where
            D: ::serde::Deserializer<'de>,
        {
            static WARNED: ::std::sync::atomic::AtomicBool =
                ::std::sync::atomic::AtomicBool::new(false);
            $crate::size::required_size_or_legacy_number(deserializer, &WARNED, $field, $unit)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    #[test]
    fn parses_kubernetes_quantities() {
        for (input, expected) in [
            ("0", 0),
            ("4096", 4096),
            ("4096B", 4096),
            ("+4096", 4096),
            ("1k", 1_000),
            ("1K", 1_000),
            ("1kB", 1_000),
            ("1Ki", KIB),
            ("1KiB", KIB),
            ("500M", 500_000_000),
            ("512Mi", 512 * MIB),
            ("512MiB", 512 * MIB),
            ("1G", 1_000_000_000),
            ("1Gi", GIB),
            ("1T", 1_000_000_000_000),
            ("1Ti", 1 << 40),
            ("1P", 1_000_000_000_000_000),
            ("1Pi", 1 << 50),
            ("1E", 1_000_000_000_000_000_000),
            ("1Ei", 1 << 60),
            ("15Ei", 15 << 60),
            ("18446744073709551615", u64::MAX),
        ] {
            assert_eq!(
                input.parse::<ConfigSize>().map(ConfigSize::as_bytes),
                Ok(expected),
                "parsing {input}"
            );
        }
    }

    #[test]
    fn fractions_scale_exactly() {
        for (input, expected) in [
            ("1.5Gi", GIB + GIB / 2),
            ("0.5Mi", MIB / 2),
            ("2.5k", 2_500),
            ("1.Gi", GIB),
            (".5Gi", GIB / 2),
            // Rounded to the nearest byte, halves away from zero.
            ("0.5", 1),
            ("0.4", 0),
            ("1.0005k", 1_001),
            ("1.0004k", 1_000),
            // Digits past the eighteenth are dropped, not rounded.
            ("0.0000000000000000001Ei", 0),
            ("1.000000000000000001Ei", (1 << 60) + 1),
        ] {
            assert_eq!(
                input.parse::<ConfigSize>().map(ConfigSize::as_bytes),
                Ok(expected),
                "parsing {input}"
            );
        }
    }

    #[test]
    fn rejects_bad_input() {
        use ParseError::*;
        let input = |s: &str| s.to_string();
        for (text, expected) in [
            ("", Empty),
            ("+", Empty),
            (
                "-1Gi",
                Negative {
                    input: input("-1Gi"),
                },
            ),
            (
                "--1",
                Negative {
                    input: input("--1"),
                },
            ),
            (
                "++1",
                InvalidUnit {
                    unit: input("+1"),
                    input: input("++1"),
                },
            ),
            (
                "1m",
                InvalidUnit {
                    unit: input("m"),
                    input: input("1m"),
                },
            ),
            (
                "1g",
                InvalidUnit {
                    unit: input("g"),
                    input: input("1g"),
                },
            ),
            (
                "1b",
                InvalidUnit {
                    unit: input("b"),
                    input: input("1b"),
                },
            ),
            (
                "1MB ",
                InvalidUnit {
                    unit: input("MB "),
                    input: input("1MB "),
                },
            ),
            (
                "1 Mi",
                InvalidUnit {
                    unit: input(" Mi"),
                    input: input("1 Mi"),
                },
            ),
            (
                "1Gi512Mi",
                InvalidUnit {
                    unit: input("Gi512Mi"),
                    input: input("1Gi512Mi"),
                },
            ),
            (
                "1e3",
                InvalidUnit {
                    unit: input("e3"),
                    input: input("1e3"),
                },
            ),
            (
                "1..2",
                InvalidUnit {
                    unit: input(".2"),
                    input: input("1..2"),
                },
            ),
            (
                "lots",
                InvalidUnit {
                    unit: input("lots"),
                    input: input("lots"),
                },
            ),
            (
                "Mi",
                MissingNumber {
                    unit: input("Mi"),
                    input: input("Mi"),
                },
            ),
            (
                "B",
                MissingNumber {
                    unit: input("B"),
                    input: input("B"),
                },
            ),
            (".", NoDigits { input: input(".") }),
            (
                "18446744073709551616",
                Overflow {
                    input: input("18446744073709551616"),
                },
            ),
            (
                "16Ei",
                Overflow {
                    input: input("16Ei"),
                },
            ),
            (
                "19E",
                Overflow {
                    input: input("19E"),
                },
            ),
            (
                "1000000000000000000000000000000000000000000Ei",
                Overflow {
                    input: input("1000000000000000000000000000000000000000000Ei"),
                },
            ),
        ] {
            assert_eq!(
                text.parse::<ConfigSize>(),
                Err(expected),
                "parsing {text:?}"
            );
        }
    }

    #[test]
    fn rejection_messages_quote_the_input() {
        let error = "1g".parse::<ConfigSize>().unwrap_err().to_string();
        assert!(error.contains("`g`") && error.contains("`1g`"), "{error}");
        assert!(error.contains("'Gi'") && error.contains("'B'"), "{error}");
        let error = "Mi".parse::<ConfigSize>().unwrap_err().to_string();
        assert!(error.contains("no number in front"), "{error}");
        let error = "16Ei".parse::<ConfigSize>().unwrap_err().to_string();
        assert!(error.contains("too large"), "{error}");
    }

    /// The unit list in the error messages names every unit the parser reads,
    /// except `K`, which is only a spelling of `k`.
    #[test]
    fn units_are_listed_in_messages() {
        for (unit, _) in UNITS.iter().filter(|(name, _)| *name != "K") {
            assert!(EXPECTED_UNITS.contains(&format!("'{unit}'")), "{unit}");
        }
    }

    #[test]
    fn displays_in_the_largest_exact_unit() {
        for (bytes, expected) in [
            (0, "0"),
            (1, "1"),
            (999, "999"),
            (1_000, "1k"),
            (1_024, "1Ki"),
            (1_500, "1500"),
            (1_000_000, "1M"),
            (MIB, "1Mi"),
            (MIB + MIB / 2, "1536Ki"),
            (500_000_000, "500M"),
            (GIB, "1Gi"),
            // Divisible by `Ki` and by `M`; the smaller count wins.
            (1_024_000_000, "1024M"),
            (1 << 60, "1Ei"),
            (1_000_000_000_000_000_000, "1E"),
            (u64::MAX, "18446744073709551615"),
        ] {
            assert_eq!(
                ConfigSize::from_bytes(bytes).to_string(),
                expected,
                "{bytes} bytes"
            );
        }
    }

    #[test]
    fn round_trips_through_json() {
        for (text, written) in [
            ("0", "0B"),
            ("0B", "0B"),
            ("4095", "4095B"),
            ("1.1Gi", "1181116006B"),
            ("1k", "1k"),
            ("512Mi", "512Mi"),
            ("1Gi", "1Gi"),
            ("1536Ki", "1536Ki"),
            ("1024M", "1024M"),
            ("1Ei", "1Ei"),
        ] {
            let size: ConfigSize = serde_json::from_str(&format!("\"{text}\"")).unwrap();
            let json = serde_json::to_string(&size).unwrap();
            assert_eq!(json, format!("\"{written}\""), "{text}");
            assert_eq!(serde_json::from_str::<ConfigSize>(&json).unwrap(), size);
        }
        // A plain `ConfigSize` field takes the string form only.
        let error = serde_json::from_str::<ConfigSize>("4096")
            .unwrap_err()
            .to_string();
        assert!(error.contains("a size such as"), "{error}");
    }

    #[test]
    fn constructors_saturate() {
        assert_eq!(ConfigSize::from_mib(u64::MAX), ConfigSize::MAX);
        assert_eq!(ConfigSize::from_mb(u64::MAX), ConfigSize::MAX);
        assert_eq!(ConfigSize::from_gib(3).as_bytes(), 3 * GIB);
        assert_eq!(ConfigSize::from_mb(16_000).as_bytes(), 16_000_000_000);
        assert_eq!(ConfigSize::from_mib(256).as_bytes(), 256 << 20);
        assert_eq!(ConfigSize::MAX.as_usize(), usize::MAX);
    }

    /// Both spellings of a setting reach one field. The superseded one is a
    /// bare number in the unit the old name carried, and arrives as an
    /// integer, a float or, through `serde_json`'s `arbitrary_precision`, as
    /// a map holding the literal; every shape is read and a fraction of the
    /// unit is refused by name.
    #[test]
    fn a_superseded_spelling_reads_any_whole_number() {
        #[derive(Debug, serde::Deserialize)]
        struct Config {
            #[serde(default, alias = "cap_mb", deserialize_with = "size_cap")]
            cap: Option<ConfigSize>,
        }
        crate::size_setting!(size_cap, "cap", LegacySizeUnit::Megabytes);

        for json in [
            r#"{"cap_mb": 10}"#,
            r#"{"cap_mb": 10.0}"#,
            r#"{"cap_mb": 1e1}"#,
            r#"{"cap": "10M"}"#,
            r#"{"cap": "10000k"}"#,
            // A bare number under the current key reads in the older unit too.
            r#"{"cap": 10}"#,
        ] {
            let config: Config = serde_json::from_str(json).unwrap();
            assert_eq!(config.cap, Some(ConfigSize::from_mb(10)), "parsing {json}");
        }
        assert_eq!(serde_json::from_str::<Config>("{}").unwrap().cap, None);
        assert_eq!(
            serde_json::from_str::<Config>(r#"{"cap": null}"#)
                .unwrap()
                .cap,
            None
        );
        // The largest legacy number saturates rather than overflowing.
        let config: Config = serde_json::from_str(r#"{"cap_mb": 18446744073709551615}"#).unwrap();
        assert_eq!(config.cap, Some(ConfigSize::MAX));

        for (bad, quoted) in [
            (r#"{"cap_mb": 10.5}"#, "`10.5`"),
            (r#"{"cap_mb": -5}"#, "cannot be negative"),
            (r#"{"cap_mb": {}}"#, "`{}`"),
            (r#"{"cap_mb": 10.50}"#, "`10.50`"),
            (r#"{"cap_mb": {"bytes": 1}}"#, "`an object`"),
            (r#"{"cap": "10m"}"#, "unknown unit `m`"),
        ] {
            let error = serde_json::from_str::<Config>(bad).unwrap_err().to_string();
            assert!(error.contains(quoted), "parsing {bad} reported {error}");
        }
    }

    /// A quoted number is not read as bytes where the unquoted one counts
    /// megabytes: the string must say its unit. A byte-counting setting takes
    /// either, since both mean bytes.
    #[test]
    fn the_parser_says_whether_a_unit_was_written() {
        for (text, has_unit) in [
            ("16G", true),
            ("4096B", true),
            ("1.5Gi", true),
            ("16000", false),
            ("+16000", false),
            ("1.5", false),
            (".5", false),
            ("0", false),
        ] {
            let (size, written) = ConfigSize::from_str_with_unit(text).unwrap();
            assert_eq!(written, has_unit, "{text}");
            assert_eq!(size, text.parse::<ConfigSize>().unwrap(), "{text}");
        }
        assert!(ConfigSize::from_str_with_unit("1.2.3").is_err());
    }

    #[test]
    fn a_unitless_string_needs_a_unit_unless_the_setting_counts_bytes() {
        #[derive(Debug, serde::Deserialize)]
        struct Config {
            #[serde(default, alias = "cap_mb", deserialize_with = "size_cap")]
            cap: Option<ConfigSize>,
            #[serde(default, alias = "buffer_bytes", deserialize_with = "size_buffer")]
            buffer: Option<ConfigSize>,
        }
        crate::size_setting!(size_cap, "cap", LegacySizeUnit::Megabytes);
        crate::size_setting!(size_buffer, "buffer", LegacySizeUnit::Bytes);

        for json in [
            r#"{"cap": "16000"}"#,
            r#"{"cap_mb": "16000"}"#,
            r#"{"cap": "+1.5"}"#,
        ] {
            let error = serde_json::from_str::<Config>(json)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("expects a size with a unit"),
                "{json}: {error}"
            );
            assert!(error.contains("`cap`"), "{json}: {error}");
        }
        let error = serde_json::from_str::<Config>(r#"{"cap": "16000"}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("\"16000M\""), "{error}");
        for (json, expected) in [
            (r#"{"cap": "16000M"}"#, ConfigSize::from_mb(16_000)),
            (r#"{"cap": "16000B"}"#, ConfigSize::from_bytes(16_000)),
            (r#"{"cap": 16000}"#, ConfigSize::from_mb(16_000)),
            (r#"{"buffer": "4096"}"#, ConfigSize::from_bytes(4096)),
            (r#"{"buffer": 4096}"#, ConfigSize::from_bytes(4096)),
            (r#"{"buffer_bytes": 4096}"#, ConfigSize::from_bytes(4096)),
        ] {
            let config: Config = serde_json::from_str(json).unwrap();
            assert_eq!(
                config.cap.or(config.buffer),
                Some(expected),
                "parsing {json}"
            );
        }
    }

    /// Writing both spellings of one setting is an error, not a silent choice.
    #[test]
    fn writing_both_spellings_is_rejected() {
        #[derive(Debug, serde::Deserialize)]
        struct Config {
            #[serde(default, alias = "cap_mb", deserialize_with = "size_cap")]
            cap: Option<ConfigSize>,
        }
        crate::size_setting!(size_cap, "cap", LegacySizeUnit::Megabytes);

        let one: Config = serde_json::from_str(r#"{"cap": "30M"}"#).unwrap();
        assert_eq!(one.cap, Some(ConfigSize::from_mb(30)));
        let error = serde_json::from_str::<Config>(r#"{"cap": "30M", "cap_mb": 30}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("duplicate field `cap`"), "{error}");
    }

    /// A setting that is not optional takes both spellings too, and refuses a
    /// `null` by name.
    #[test]
    fn a_required_setting_refuses_null() {
        #[derive(Debug, serde::Deserialize)]
        struct Config {
            #[serde(
                default = "default_buffer",
                alias = "buffer_bytes",
                deserialize_with = "size_buffer"
            )]
            buffer: ConfigSize,
        }
        fn default_buffer() -> ConfigSize {
            ConfigSize::from_mib(1)
        }
        crate::required_size_setting!(size_buffer, "buffer", LegacySizeUnit::Bytes);

        assert_eq!(
            serde_json::from_str::<Config>("{}").unwrap().buffer,
            ConfigSize::from_mib(1)
        );
        assert_eq!(
            serde_json::from_str::<Config>(r#"{"buffer_bytes": 2048}"#)
                .unwrap()
                .buffer,
            ConfigSize::from_kib(2)
        );
        assert_eq!(
            serde_json::from_str::<Config>(r#"{"buffer": "2Ki"}"#)
                .unwrap()
                .buffer,
            ConfigSize::from_kib(2)
        );
        let error = serde_json::from_str::<Config>(r#"{"buffer": null}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("`buffer` cannot be null"), "{error}");
    }

    /// The bare `u64::MAX` an older release may have written as "no limit"
    /// is read without a warning. The warning itself is only observable in the
    /// log, so this pins the value.
    #[test]
    fn the_no_limit_value_is_read_without_a_warning() {
        let warned = std::sync::atomic::AtomicBool::new(false);
        let size: ConfigSize = size_from_legacy_number::<serde_json::Error>(
            &warned,
            "cap",
            LegacySizeUnit::Bytes,
            u64::MAX,
        )
        .unwrap();
        assert_eq!(size, ConfigSize::MAX);
        assert!(!warned.load(std::sync::atomic::Ordering::Relaxed));
        let _ = size_from_legacy_number::<serde_json::Error>(
            &warned,
            "cap",
            LegacySizeUnit::Mebibytes,
            3,
        );
        assert!(warned.load(std::sync::atomic::Ordering::Relaxed));
    }

    /// Property-based tests, in the spirit of the duration ones: the inputs
    /// somebody thought of are above, these explore the rest.
    mod properties {
        use super::*;
        use proptest::prelude::*;

        /// Byte counts weighted towards the edges, where the formatter and the
        /// parser are likeliest to disagree.
        fn bytes() -> impl Strategy<Value = u64> {
            prop_oneof![
                1 => Just(0u64),
                1 => Just(1u64),
                1 => Just(u64::MAX),
                1 => Just(1u64 << 60),
                1 => Just(1_000_000_000_000_000_000u64),
                2 => 0u64..1_000,
                2 => (0u64..1_000).prop_map(|k| k << 20),
                2 => (0u64..1_000).prop_map(|m| m * 1_000_000),
                6 => any::<u64>(),
            ]
        }

        /// Text drawn from the parser's own alphabet, plus arbitrary unicode.
        fn size_like_text() -> impl Strategy<Value = String> {
            prop_oneof![
                2 => any::<String>(),
                3 => "[-+0-9. kKMGTPEiB]{0,40}",
                3 => "[0-9]{1,40}(k|K|M|G|T|P|E|Ki|Mi|Gi|Ti|Pi|Ei)B?",
                2 => "[0-9]{1,25}\\.[0-9]{1,25}(k|M|Gi|Ei)",
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Formatting a size and parsing the result returns the same
            /// value: a configuration written back out reloads unchanged.
            #[test]
            fn display_round_trips(bytes in bytes()) {
                let size = ConfigSize::from_bytes(bytes);
                let text = size.to_string();
                prop_assert_eq!(
                    text.parse::<ConfigSize>().map_err(|e| e.to_string()),
                    Ok(size),
                    "formatted as {:?}",
                    text
                );
            }

            /// JSON is the format configurations travel in, and serde must
            /// agree with `FromStr`.
            #[test]
            fn serde_agrees_with_from_str(bytes in bytes()) {
                let size = ConfigSize::from_bytes(bytes);
                let json = serde_json::to_string(&size).unwrap();
                let text = size.to_string();
                let written = if text.bytes().all(|b| b.is_ascii_digit()) {
                    format!("\"{text}B\"")
                } else {
                    format!("\"{text}\"")
                };
                prop_assert_eq!(&json, &written);
                prop_assert_eq!(serde_json::from_str::<ConfigSize>(&json).unwrap(), size);
                prop_assert_eq!(json.trim_matches('"').parse::<ConfigSize>().unwrap(), size);
            }

            /// Parsing answers `Ok` or `Err` for any input and never panics.
            #[test]
            fn parsing_never_panics(text in size_like_text()) {
                let _ = text.parse::<ConfigSize>();
            }

            /// A whole number of any unit parses to that many bytes.
            #[test]
            fn whole_units_multiply(count in 0u64..1_000_000, unit in 0usize..UNITS.len()) {
                let (name, bytes) = UNITS[unit];
                let expected = u128::from(count) * bytes;
                let parsed = format!("{count}{name}").parse::<ConfigSize>();
                if let Ok(expected) = u64::try_from(expected) {
                    prop_assert_eq!(parsed.map(ConfigSize::as_bytes), Ok(expected));
                } else {
                    let overflowed = matches!(parsed, Err(ParseError::Overflow { .. }));
                    prop_assert!(overflowed, "parsed {:?}", parsed);
                }
            }
        }
    }
}
