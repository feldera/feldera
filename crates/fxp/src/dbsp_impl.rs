use dbsp::algebra::{HasOne, HasZero, MulByRef, OptionWeightType};
use feldera_types::serde_with_context::{
    serde_config::DecimalFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use serde::{Deserializer, Serialize, Serializer};
use smallstr::SmallString;
use std::fmt::Write;

use crate::{Fixed, FixedInteger};

impl<const P: usize, const S: usize> OptionWeightType for Fixed<P, S> {}
impl<const P: usize, const S: usize> OptionWeightType for &Fixed<P, S> {}

impl<const P: usize, const S: usize> HasZero for Fixed<P, S> {
    fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }

    fn zero() -> Self {
        Self::ZERO
    }
}

impl<const P: usize, const S: usize> HasOne for Fixed<P, S> {
    /// This will panic if 1 can't be represented in this type (that is, if `S
    /// >= P`).
    fn one() -> Self {
        Self::ONE
    }
}

impl<const P: usize, const S: usize> MulByRef<isize> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &isize) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_isize(*other))
            .unwrap()
    }
}

impl<const P: usize, const S: usize> MulByRef<i64> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &i64) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_i64(*other))
            .unwrap()
    }
}

impl<const P: usize, const S: usize> MulByRef<i32> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &i32) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_i32(*other))
            .unwrap()
    }
}

impl<const P: usize, const S: usize> SerializeWithContext<SqlSerdeConfig> for Fixed<P, S> {
    fn serialize_with_context<Ser>(
        &self,
        serializer: Ser,
        context: &SqlSerdeConfig,
    ) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        match context.decimal_format {
            DecimalFormat::Numeric => self.serialize(serializer),
            DecimalFormat::String => {
                let mut string = SmallString::<[u8; 64]>::new();
                write!(&mut string, "{}", self).unwrap();
                serializer.serialize_str(&string)
            }
        }
    }
}

impl<'de, C, const P: usize, const S: usize> DeserializeWithContext<'de, C> for Fixed<P, S> {
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, _context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
    }
}
