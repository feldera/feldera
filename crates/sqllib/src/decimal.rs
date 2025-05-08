//! Operations on Decimal values

use crate::{some_function2, some_polymorphic_function1, some_polymorphic_function2};
use core::cmp::Ordering;
use dbsp::algebra::F64;
use num::ToPrimitive;
use num_traits::Signed;
use rust_decimal::{Decimal, MathematicalOps, RoundingStrategy};

// Runtime decimal type
pub type Dec = Decimal;

/***** decimals ***** */

#[doc(hidden)]
pub fn bround__(left: Decimal, right: i32) -> Decimal {
    if right.is_negative() {
        let right_unsigned = right.unsigned_abs();
        let pow_of_ten = Decimal::new(10_i64.pow(right_unsigned), 0);
        let rounded = ((left / pow_of_ten)
            .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven))
            * pow_of_ten;
        return rounded;
    }

    left.round_dp_with_strategy(
        u32::try_from(right).unwrap(),
        RoundingStrategy::MidpointNearestEven,
    )
}

some_function2!(bround, Decimal, i32, Decimal);

/*
#[doc(hidden)]
#[inline(always)]
pub fn new_decimal(s: &str, precision: u32, scale: u32) -> Option<Decimal> {
    let value = Decimal::from_str(s).ok()?;
    Some(unwrap_cast(cast_to_decimal_decimal(
        value, precision, scale,
    )))
}
*/

#[doc(hidden)]
#[inline(always)]
pub fn round_decimal_i32(left: Decimal, right: i32) -> Decimal {
    // Rust decimal doesn't support rounding with negative values
    // but Calcite does
    if right.is_negative() {
        let right_unsigned = right.unsigned_abs();
        let pow_of_ten = Decimal::new(10_i64.pow(right_unsigned), 0);
        let rounded = ((left / pow_of_ten).round()) * pow_of_ten;
        return rounded;
    }

    left.round_dp(u32::try_from(right).unwrap())
}

some_polymorphic_function2!(round, decimal, Decimal, i32, i32, Decimal);

#[doc(hidden)]
#[inline(always)]
pub fn truncate_decimal_i32(left: Decimal, right: i32) -> Decimal {
    // Rust decimal doesn't support rounding with negative values
    // but Calcite does
    if right.is_negative() {
        let right_unsigned = right.unsigned_abs();
        let pow_of_ten = Decimal::new(10_i64.pow(right_unsigned), 0);
        let truncated = (left / pow_of_ten).trunc() * pow_of_ten;
        return truncated;
    }

    left.trunc_with_scale(u32::try_from(right).unwrap())
}

some_polymorphic_function2!(truncate, decimal, Decimal, i32, i32, Decimal);

#[doc(hidden)]
pub fn power_i32_decimal(left: i32, right: Decimal) -> F64 {
    F64::new((left as f64).powf(right.to_f64().unwrap()))
}

some_polymorphic_function2!(power, i32, i32, decimal, Decimal, F64);

#[doc(hidden)]
pub fn power_decimal_i32(left: Decimal, right: i32) -> F64 {
    F64::new(left.powi(right.into()).to_f64().unwrap())
}

some_polymorphic_function2!(power, decimal, Decimal, i32, i32, F64);

#[doc(hidden)]
pub fn power_decimal_decimal(left: Decimal, right: Decimal) -> F64 {
    if right == Decimal::new(5, 1) {
        // special case for sqrt, has higher precision than pow
        F64::from(left.sqrt().unwrap().to_f64().unwrap())
    } else {
        F64::from(left.powd(right).to_f64().unwrap())
    }
}

some_polymorphic_function2!(power, decimal, Decimal, decimal, Decimal, F64);

#[doc(hidden)]
pub fn power_decimal_d(left: Decimal, right: F64) -> F64 {
    // Special case to match Java pow
    if right.into_inner().is_nan() {
        return right;
    }
    F64::new(left.powf(right.into_inner()).to_f64().unwrap())
}

some_polymorphic_function2!(power, decimal, Decimal, d, F64, F64);

#[doc(hidden)]
pub fn sqrt_decimal(left: Decimal) -> F64 {
    if left < Decimal::ZERO {
        return F64::new(f64::NAN);
    }

    F64::from(left.sqrt().unwrap().to_f64().unwrap())
}

some_polymorphic_function1!(sqrt, decimal, Decimal, F64);

#[inline(always)]
#[doc(hidden)]
pub fn floor_decimal(value: Decimal) -> Decimal {
    value.floor()
}

some_polymorphic_function1!(floor, decimal, Decimal, Decimal);

#[inline(always)]
#[doc(hidden)]
pub fn ceil_decimal(value: Decimal) -> Decimal {
    value.ceil()
}

some_polymorphic_function1!(ceil, decimal, Decimal, Decimal);

#[inline(always)]
#[doc(hidden)]
pub fn sign_decimal(value: Decimal) -> Decimal {
    value.signum()
}

some_polymorphic_function1!(sign, decimal, Decimal, Decimal);

#[doc(hidden)]
pub fn shift_left__(left: Decimal, amount: i32) -> Decimal {
    let mut result = left;
    // There should be a better way to do this
    match amount.cmp(&0) {
        Ordering::Equal => result,
        Ordering::Greater => {
            for _i in amount..0 {
                result *= Decimal::TEN
            }
            result
        }
        Ordering::Less => {
            for _i in 0..amount {
                result /= Decimal::TEN
            }
            result
        }
    }
}

#[doc(hidden)]
pub fn shift_leftN_(left: Option<Decimal>, amount: i32) -> Option<Decimal> {
    let left = left?;
    Some(shift_left__(left, amount))
}
