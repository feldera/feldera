use std::cmp::max;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct ClockConfig {
    pub clock_resolution_usecs: u64,

    /// Signed offset, in milliseconds, added to wall-clock time before
    /// rounding to the clock resolution.
    ///
    /// Populated from `DevTweaks::now_offset` at endpoint construction.
    /// `None` means no shift is applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_offset_delta_ms: Option<i64>,
}

impl ClockConfig {
    pub fn clock_resolution_ms(&self) -> u64 {
        // Refuse to set 0 clock resolution.
        max((self.clock_resolution_usecs + 500) / 1_000, 1)
    }
}
