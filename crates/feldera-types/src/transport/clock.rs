use std::cmp::max;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct ClockConfig {
    pub clock_resolution_usecs: u64,

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
    pub fn clock_resolution_ms(&self) -> u64 {
        // Refuse to set 0 clock resolution.
        max((self.clock_resolution_usecs + 500) / 1_000, 1)
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
