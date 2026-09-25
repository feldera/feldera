use std::cmp::max;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use chrono::FixedOffset;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use utoipa::ToSchema;

use crate::{
    duration::{Duration, LegacyUnit},
    duration_setting,
};

duration_setting!(
    duration_clock_resolution,
    "clock_resolution",
    LegacyUnit::Micros
);

/// Fixed timezone offset for the pipeline clock.
///
/// Parsed from an ISO-8601 UTC offset string such as `"+05:30"` or
/// `"-08:00"` and serialized back to the same form.  Wraps
/// [`chrono::FixedOffset`], which accepts offsets strictly between -24 and
/// +24 hours.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockTimezoneOffset(FixedOffset);

impl ClockTimezoneOffset {
    /// The offset in milliseconds east of UTC.
    pub fn offset_ms(&self) -> i64 {
        i64::from(self.0.local_minus_utc()) * 1_000
    }
}

impl FromStr for ClockTimezoneOffset {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        FixedOffset::from_str(s).map(Self).map_err(|e| {
            format!(
                "invalid timezone offset {s:?} (expected a UTC offset such as \"+05:30\" or \"-08:00\"): {e}"
            )
        })
    }
}

impl Display for ClockTimezoneOffset {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for ClockTimezoneOffset {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ClockTimezoneOffset {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(de::Error::custom)
    }
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct ClockConfig {
    /// How often the clock ticks, for example `1s`.
    #[serde(
        default,
        alias = "clock_resolution_usecs",
        deserialize_with = "duration_clock_resolution",
        skip_serializing_if = "Option::is_none"
    )]
    pub clock_resolution: Option<Duration>,

    /// Constant offset added to every emitted `NOW()` value, in milliseconds
    /// east of UTC.  Populated from the `clock_timezone_offset` pipeline
    /// property; 0 means UTC.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub timezone_offset_ms: i64,

    /// Target value for `NOW()` at the worker's first emitted tick, in
    /// milliseconds since the Unix epoch.
    ///
    /// Populated verbatim from `DevTweaks::now_offset` at endpoint
    /// construction; the wall-clock delta is computed inside the
    /// connector's worker task from a single `SystemTime::now()`
    /// reading, so there is no drift between config construction and
    /// the first emitted tick.  `None` means no shift is applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_offset_ms: Option<i64>,

    /// If `true`, the clock does not advance on wall-clock cadence.
    /// `NOW()` is held at its current value and only advances when an
    /// external caller invokes the pipeline's `POST /clock/advance`
    /// endpoint.  Populated from `DevTweaks::now_http_driven`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub http_driven: bool,
}

impl ClockConfig {
    /// The resolution in effect: `clock_resolution`, falling back to
    /// the deprecated `clock_resolution_usecs` and then to one second.
    pub fn clock_resolution(&self) -> Duration {
        self.clock_resolution
            .unwrap_or(crate::config::DEFAULT_CLOCK_RESOLUTION)
    }

    pub fn clock_resolution_ms(&self) -> u64 {
        // Refuse to set 0 clock resolution.
        max(
            (self.clock_resolution().as_micros() as u64 + 500) / 1_000,
            1,
        )
    }
}

/// Body of `POST /clock/advance`.
///
/// `delta_ms` is unsigned; negative values fail JSON deserialization.
/// `Some(0)` reads the current `NOW()` without moving it or rounding
/// it; `Some(n)` advances by `n` ms; `None` (`null` or omitted)
/// advances by one `clock_resolution`.  Non-zero values round up to
/// the next `clock_resolution` boundary, so a sub-resolution delta
/// still moves the clock by one full tick.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ClockAdvanceRequest {
    #[serde(default)]
    pub delta_ms: Option<u64>,
}

/// Response of `POST /clock/advance`: the new `NOW()` value as both
/// milliseconds since epoch (signed; pre-1970 anchors yield negative
/// values) and an RFC 3339 string.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ClockAdvanceResponse {
    pub now_ms: i64,
    pub now: String,
}

#[cfg(test)]
mod test {
    use super::{ClockConfig, ClockTimezoneOffset};
    use crate::config::DEFAULT_CLOCK_RESOLUTION;
    use crate::duration::Duration;

    /// The clock resolution reads back from both spellings. They reach one
    /// field, so writing both is rejected as a duplicate.
    #[test]
    fn clock_resolution_accepts_both_spellings() {
        for (json, expected) in [
            (r#"{}"#, DEFAULT_CLOCK_RESOLUTION),
            (
                r#"{"clock_resolution": "250ms"}"#,
                Duration::from_millis(250),
            ),
            (
                r#"{"clock_resolution_usecs": 100000}"#,
                Duration::from_millis(100),
            ),
        ] {
            let config: ClockConfig = serde_json::from_str(json).unwrap();
            assert_eq!(config.clock_resolution(), expected, "parsing {json}");
        }

        // Both spellings reach one field, so writing both is a duplicate.
        let error = serde_json::from_str::<ClockConfig>(
            r#"{"clock_resolution": "250ms", "clock_resolution_usecs": 100000}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("duplicate field `clock_resolution`"),
            "{error}"
        );
    }

    #[test]
    fn timezone_offset_parses_and_round_trips() {
        const MINUTE_MS: i64 = 60_000;
        for (input, expected_ms) in [
            ("+05:30", (5 * 60 + 30) * MINUTE_MS),
            ("-08:00", -8 * 60 * MINUTE_MS),
            ("+00:00", 0),
            ("+14:00", 14 * 60 * MINUTE_MS),
        ] {
            let offset: ClockTimezoneOffset =
                serde_json::from_value(serde_json::json!(input)).unwrap();
            assert_eq!(offset.offset_ms(), expected_ms, "input {input}");
            assert_eq!(
                serde_json::to_value(offset).unwrap(),
                serde_json::json!(input),
                "round trip of {input}"
            );
        }
    }

    /// Configurations written before the offset existed must read back
    /// with no offset.
    #[test]
    fn clock_config_without_offset_defaults_to_zero() {
        let config: super::ClockConfig =
            serde_json::from_value(serde_json::json!({"clock_resolution_usecs": 1_000_000}))
                .unwrap();
        assert_eq!(config.timezone_offset_ms, 0);

        let runtime_config: crate::config::RuntimeConfig =
            serde_json::from_value(serde_json::json!({"workers": 4})).unwrap();
        assert_eq!(runtime_config.clock_timezone_offset, None);
    }

    #[test]
    fn timezone_offset_rejects_invalid_input() {
        // Missing sign, garbage, out of chrono's (-24h, +24h) range, and
        // non-string JSON must all fail deserialization.
        for input in [
            serde_json::json!("05:30"),
            serde_json::json!("banana"),
            serde_json::json!("+27:00"),
            serde_json::json!(330),
        ] {
            assert!(
                serde_json::from_value::<ClockTimezoneOffset>(input.clone()).is_err(),
                "input {input} should be rejected"
            );
        }
    }
}
