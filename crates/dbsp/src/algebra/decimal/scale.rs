use size_of::SizeOf;
use std::fmt::Display;

// Should be kept in sync with the Java code in DBSPTypeDecimal.java
const MIN_DECIMAL_SCALE: u32 = 0;
const MAX_DECIMAL_SCALE: u32 = 10;

#[derive(
    Debug,
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
)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Scale(u32);

impl Scale {
    pub fn inner(&self) -> u32 {
        self.0
    }

    pub fn min() -> Self {
        Self(MIN_DECIMAL_SCALE)
    }

    pub const fn max() -> Self {
        Self(MAX_DECIMAL_SCALE)
    }
}

impl TryFrom<u32> for Scale {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            x if (MIN_DECIMAL_SCALE..=MAX_DECIMAL_SCALE).contains(&x) => Ok(Scale(x)),
            _ => Err(format!(
                "Numeric scale {value} must be between {MIN_DECIMAL_SCALE} and {MAX_DECIMAL_SCALE}"
            )),
        }
    }
}

impl Display for Scale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for Scale {
    fn default() -> Self {
        Self(MAX_DECIMAL_SCALE)
    }
}
