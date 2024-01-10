use rust_decimal::Decimal;
use size_of::SizeOf;
use std::fmt::Display;

// Should be kept in sync with the Java code in DBSPTypeDecimal.java
const MIN_DECIMAL_PRECISION: u32 = 1;
const MAX_DECIMAL_PRECISION: u32 = 38;

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
pub struct Precision(u32);

impl Precision {
    pub fn extract_precision_from_decimal(val: Decimal) -> Result<Self, String> {
        let precision = val
            .scale()
            .max(val.mantissa().checked_ilog10().unwrap_or(0) + 1);

        Precision::try_from(precision)
    }

    pub fn inner(&self) -> u32 {
        self.0
    }

    pub fn max() -> Self {
        Self(MAX_DECIMAL_PRECISION)
    }
}

impl TryFrom<u32> for Precision {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
                x if (MIN_DECIMAL_PRECISION..=MAX_DECIMAL_PRECISION).contains(&x) => Ok(Precision(x)),
                _ => Err(format!(
                    "Numeric precision {value} must be between {MIN_DECIMAL_PRECISION} and {MAX_DECIMAL_PRECISION}"
                )),
            }
    }
}

impl Display for Precision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for Precision {
    fn default() -> Self {
        Precision(MAX_DECIMAL_PRECISION)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use crate::algebra::decimal::Precision;

    #[test]
    fn precision_from_decimal() {
        let pairs = [
            ("100103123", 13, 4),
            ("1001", 6, 2),
            ("1001", 4, 0),
            ("1001", 5, 1),
            ("0.1001", 4, 4),
            (".1001", 4, 4),
            ("1001", 8, 4),
            ("1.1001", 5, 4),
        ];

        for (item, expected, scale) in pairs {
            let mut item = Decimal::from_str(item).unwrap();
            item.rescale(scale);

            assert_eq!(
                expected,
                Precision::extract_precision_from_decimal(item)
                    .unwrap()
                    .inner(),
                "{item}"
            )
        }
    }
}
