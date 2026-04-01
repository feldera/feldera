use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const MODERATE_MEMORY_PRESSURE_THRESHOLD: f64 = 0.85;
pub const HIGH_MEMORY_PRESSURE_THRESHOLD: f64 = 0.90;
pub const CRITICAL_MEMORY_PRESSURE_THRESHOLD: f64 = 0.95;

/// Memory pressure level.
///
/// The current memory pressure level is computed as a function of the current process
/// resident set size (RSS) and the user-configured memory limit (`max_rss`).
///
/// As the memory pressure level increases, the system will apply increasing backpressure to
/// push state cached in memory to storage.
///
/// - `Low`: less than 85% of the user-configured memory limit has been allocated.
/// - `Moderate`: between 85% and 90% of the user-configured memory limit has been allocated.
/// - `High`: between 90% and 95% of the user-configured memory limit has been allocated.
/// - `Critical`: more than 95% of the user-configured memory limit has been allocated.
#[derive(
    Debug, Default, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, ToSchema,
)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPressure {
    /// Less than 85% of the user-configured memory limit has been allocated.
    #[default]
    Low = 0,
    /// Between 85% and 90% of the user-configured memory limit has been allocated.
    Moderate = 1,
    /// Between 90% and 95% of the user-configured memory limit has been allocated.
    High = 2,
    /// More than 95% of the user-configured memory limit has been allocated.
    Critical = 3,
}

impl TryFrom<u8> for MemoryPressure {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MemoryPressure::Low),
            1 => Ok(MemoryPressure::Moderate),
            2 => Ok(MemoryPressure::High),
            3 => Ok(MemoryPressure::Critical),
            _ => Err(()),
        }
    }
}
