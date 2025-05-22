use std::cmp::max;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct ClockConfig {
    pub clock_resolution_usecs: u64,
}

impl ClockConfig {
    pub fn clock_resolution_ms(&self) -> u64 {
        // Refuse to set 0 clock resolution.
        max((self.clock_resolution_usecs + 500) / 1_000, 1)
    }
}
