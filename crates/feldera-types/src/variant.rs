//! [`VariantValue`]: an owned, engine-neutral representation of SQL `VARIANT`.
//!
//! * Structurally equivalent to the Parquet variant type.
//! * Straightforward mapping to both sqllib representations of the Feldera SQL VARIANT.
//! * Mutable: new attributes can be added without reencoding the entire binary value.
//!
//! ```
//! use std::collections::BTreeMap;
//! use feldera_types::variant::{VariantDecimal8, VariantValue};
//!
//! let attributes = VariantValue::Object(BTreeMap::from([
//!     ("kafka_topic".to_string(), VariantValue::String("events".into())),
//!     ("kafka_offset".to_string(), VariantValue::Int64(17)),
//!     ("price".to_string(), VariantValue::Decimal8(VariantDecimal8::try_new(12345, 2)?)),
//! ]));
//! let VariantValue::Object(fields) = &attributes else {
//!     unreachable!()
//! };
//! assert_eq!(fields["kafka_offset"], VariantValue::Int64(17));
//! # Ok::<(), feldera_types::variant::VariantDecimalError>(())
//! ```

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use thiserror::Error as ThisError;
use uuid::Uuid;

/// An owned `VARIANT` value whose types mirror the Parquet variant encoding.
///
/// The arms match the encoding's type set one for one.
#[derive(Clone, Debug, PartialEq)]
pub enum VariantValue {
    /// The null variant, which is a present value, not a SQL NULL.
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Decimal4(VariantDecimal4),
    Decimal8(VariantDecimal8),
    Decimal16(VariantDecimal16),
    String(String),
    Binary(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    /// A zoned timestamp, microsecond precision.
    TimestampMicros(DateTime<Utc>),
    /// An unzoned timestamp, microsecond precision.
    TimestampNtzMicros(NaiveDateTime),
    /// A zoned timestamp, nanosecond precision.
    TimestampNanos(DateTime<Utc>),
    /// An unzoned timestamp, nanosecond precision.
    TimestampNtzNanos(NaiveDateTime),
    Uuid(Uuid),
    List(Vec<VariantValue>),
    /// Object field names are strings: that is what the encoding permits.
    Object(BTreeMap<String, VariantValue>),
}

/// Error creating a decimal value.
#[derive(Clone, Debug, PartialEq, Eq, ThisError)]
pub enum VariantDecimalError {
    #[error("the scale {scale} is larger than the maximum precision {max_precision}")]
    ScaleTooLarge { scale: u8, max_precision: u8 },
    #[error("{integer} has more digits than the maximum precision {max_precision}")]
    TooManyDigits { integer: i128, max_precision: u8 },
}

/// Defines one of the three decimal types of the Parquet variant encoding: `decimal4`,
/// `decimal8`, or `decimal16`. The encoding stores the value as a one-byte scale, then
/// the unscaled integer in 4, 8, or 16 little-endian bytes. The struct holds the same
/// two fields, with `$native` (`i32`, `i64`, or `i128`) as the integer type.
///
/// `$max_precision` is the maximum number of digits for the width: 9, 18, or 38. `try_new`
/// applies the same limits as `parquet_variant::VariantDecimal4::try_new` and the other two
/// widths: the scale is at most `$max_precision`, and the integer has at most that many
/// digits. Thus a value converts to the Parquet decimal of the same width, and back, without
/// an error. As in the Parquet types, there is no lower limit: a value that fits a narrower
/// width is also valid in a wider one.
macro_rules! variant_decimal {
    ($(#[$doc:meta])* $name:ident, $native:ty, $max_precision:expr) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            integer: $native,
            scale: u8,
        }

        impl $name {
            /// The most digits the unscaled integer can have.
            pub const MAX_PRECISION: u8 = $max_precision;

            /// The decimal `integer * 10^-scale`. Fails if `integer` has more than
            /// [`Self::MAX_PRECISION`] digits or `scale` is larger than it: the bounds the
            /// Parquet variant encoding sets, so the decimal always encodes.
            pub fn try_new(integer: $native, scale: u8) -> Result<Self, VariantDecimalError> {
                let max_precision = Self::MAX_PRECISION;
                if scale > max_precision {
                    return Err(VariantDecimalError::ScaleTooLarge {
                        scale,
                        max_precision,
                    });
                }
                let max_integer = <$native>::pow(10, max_precision as u32) - 1;
                if !(-max_integer..=max_integer).contains(&integer) {
                    return Err(VariantDecimalError::TooManyDigits {
                        integer: integer as i128,
                        max_precision,
                    });
                }
                Ok(Self { integer, scale })
            }

            /// The unscaled integer: `12345` for `123.45`.
            pub fn integer(&self) -> $native {
                self.integer
            }

            /// The number of digits after the decimal point: `2` for `123.45`.
            pub fn scale(&self) -> u8 {
                self.scale
            }
        }
    };
}

variant_decimal!(
    /// A decimal of at most 9 digits, the Parquet variant's 4-byte decimal.
    ///
    /// ```
    /// use feldera_types::variant::VariantDecimal4;
    ///
    /// let price = VariantDecimal4::try_new(12345, 2)?;
    /// assert_eq!((price.integer(), price.scale()), (12345, 2));
    /// assert!(VariantDecimal4::try_new(1_000_000_000, 0).is_err());
    /// assert!(VariantDecimal4::try_new(1, 10).is_err());
    /// # Ok::<(), feldera_types::variant::VariantDecimalError>(())
    /// ```
    VariantDecimal4,
    i32,
    9
);

variant_decimal!(
    /// A decimal of at most 18 digits, the Parquet variant's 8-byte decimal.
    ///
    /// ```
    /// use feldera_types::variant::VariantDecimal8;
    ///
    /// let price = VariantDecimal8::try_new(123_456_789_012, 6)?;
    /// assert_eq!((price.integer(), price.scale()), (123_456_789_012, 6));
    /// assert!(VariantDecimal8::try_new(1_000_000_000_000_000_000, 0).is_err());
    /// # Ok::<(), feldera_types::variant::VariantDecimalError>(())
    /// ```
    VariantDecimal8,
    i64,
    18
);

variant_decimal!(
    /// A decimal of at most 38 digits, the Parquet variant's 16-byte decimal.
    ///
    /// ```
    /// use feldera_types::variant::VariantDecimal16;
    ///
    /// let price = VariantDecimal16::try_new(12_345_678_901_234_567_890, 8)?;
    /// assert_eq!(price.scale(), 8);
    /// assert!(VariantDecimal16::try_new(i128::MAX, 0).is_err());
    /// # Ok::<(), feldera_types::variant::VariantDecimalError>(())
    /// ```
    VariantDecimal16,
    i128,
    38
);

#[cfg(test)]
mod tests {
    use super::*;

    /// Each decimal width accepts exactly the integers and scales its precision allows.
    #[test]
    fn decimals_keep_the_parquet_bounds() {
        assert!(VariantDecimal4::try_new(999_999_999, 9).is_ok());
        assert!(VariantDecimal4::try_new(-999_999_999, 0).is_ok());
        assert_eq!(
            VariantDecimal4::try_new(1_000_000_000, 0),
            Err(VariantDecimalError::TooManyDigits {
                integer: 1_000_000_000,
                max_precision: 9
            })
        );
        assert_eq!(
            VariantDecimal4::try_new(-1_000_000_000, 0),
            Err(VariantDecimalError::TooManyDigits {
                integer: -1_000_000_000,
                max_precision: 9
            })
        );
        assert_eq!(
            VariantDecimal4::try_new(1, 10),
            Err(VariantDecimalError::ScaleTooLarge {
                scale: 10,
                max_precision: 9
            })
        );

        assert!(VariantDecimal8::try_new(999_999_999_999_999_999, 18).is_ok());
        assert!(VariantDecimal8::try_new(1_000_000_000_000_000_000, 0).is_err());
        assert!(VariantDecimal8::try_new(1, 19).is_err());

        let max16 = 10i128.pow(38) - 1;
        assert!(VariantDecimal16::try_new(max16, 38).is_ok());
        assert!(VariantDecimal16::try_new(-max16, 0).is_ok());
        assert!(VariantDecimal16::try_new(max16 + 1, 0).is_err());
        assert!(VariantDecimal16::try_new(i128::MIN, 0).is_err());
        assert!(VariantDecimal16::try_new(1, 39).is_err());
    }

    /// The errors name the bound the decimal broke.
    #[test]
    fn decimal_errors_name_the_bound() {
        assert_eq!(
            VariantDecimal4::try_new(1, 10).unwrap_err().to_string(),
            "the scale 10 is larger than the maximum precision 9"
        );
        assert_eq!(
            VariantDecimal8::try_new(-1_000_000_000_000_000_000, 0)
                .unwrap_err()
                .to_string(),
            "-1000000000000000000 has more digits than the maximum precision 18"
        );
    }
}
