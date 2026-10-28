//! Operations on Decimal values

use crate::{
    some_polymorphic_function1, some_polymorphic_function2, some_polymorphic_null_function2,
};
pub use feldera_fxp::DynamicDecimal;
use feldera_fxp::Fixed;

pub type SqlDecimal<const P: usize, const S: usize> = Fixed<P, S>;

#[doc(hidden)]
pub fn plus_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    left.checked_add_generic::<P1, S1, P2, S2>(right).unwrap()
}

some_polymorphic_function2!(plus [const P0: usize, const S0: usize, const P1: usize, const S1: usize, const P2: usize, const S2: usize], SqlDecimal, SqlDecimal<P0, S0>, SqlDecimal, SqlDecimal<P1, S1>, SqlDecimal<P2, S2>);

#[doc(hidden)]
pub fn minus_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    left.checked_sub_generic::<P1, S1, P2, S2>(right).unwrap()
}

some_polymorphic_function2!(minus [const P0: usize, const S0: usize, const P1: usize, const S1: usize, const P2: usize, const S2: usize], SqlDecimal, SqlDecimal<P0, S0>, SqlDecimal, SqlDecimal<P1, S1>, SqlDecimal<P2, S2>);

#[doc(hidden)]
pub fn times_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    left.checked_mul_generic::<P1, S1, P2, S2>(right)
        .unwrap_or_else(|| {
            panic!(
                "Overflow in multiplication {}*{} cannot be represented as DECIMAL({P2}, {S2})",
                left, right
            )
        })
}

some_polymorphic_function2!(times [const P0: usize, const S0: usize, const P1: usize, const S1: usize, const P2: usize, const S2: usize], SqlDecimal, SqlDecimal<P0, S0>, SqlDecimal, SqlDecimal<P1, S1>, SqlDecimal<P2, S2>);

#[doc(hidden)]
pub fn div_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    left.checked_div_generic::<P1, S1, P2, S2>(right)
        .unwrap_or_else(|| panic!("Attempt to divide by zero: {}/{}", left, right))
}

some_polymorphic_function2!(div [const P0: usize, const S0: usize, const P1: usize, const S1: usize, const P2: usize, const S2: usize], SqlDecimal, SqlDecimal<P0, S0>, SqlDecimal, SqlDecimal<P1, S1>, SqlDecimal<P2, S2>);

#[doc(hidden)]
pub fn div_null_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> Option<SqlDecimal<P2, S2>> {
    left.checked_div_generic::<P1, S1, P2, S2>(right)
}

some_polymorphic_null_function2!(div_null [const P0: usize, const S0: usize, const P1: usize, const S1: usize, const P2: usize, const S2: usize], SqlDecimal, SqlDecimal<P0, S0>, SqlDecimal, SqlDecimal<P1, S1>, SqlDecimal<P2, S2>);

// Saturating forms of the DECIMAL operations.  The range of a DECIMAL type is symmetric, so a
// result that does not fit is MAX if it is positive and MIN if not.

/// MAX if `positive`, MIN otherwise.
fn saturated<const P: usize, const S: usize>(positive: bool) -> SqlDecimal<P, S> {
    if positive {
        SqlDecimal::<P, S>::MAX
    } else {
        SqlDecimal::<P, S>::MIN
    }
}

#[doc(hidden)]
pub fn plus_saturating_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    // The sum is positive when left > -right
    left.checked_add_generic::<P1, S1, P2, S2>(right)
        .unwrap_or_else(|| saturated(left > -right))
}

#[doc(hidden)]
pub fn minus_saturating_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    left.checked_sub_generic::<P1, S1, P2, S2>(right)
        .unwrap_or_else(|| saturated(left > right))
}

#[doc(hidden)]
pub fn times_saturating_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    // A product that does not fit is not 0, so it is positive when the signs are equal
    left.checked_mul_generic::<P1, S1, P2, S2>(right)
        .unwrap_or_else(|| saturated(left.is_negative() == right.is_negative()))
}

/// A divisor of 0 gives MAX for a dividend of at least 0 and MIN otherwise.
#[doc(hidden)]
pub fn div_saturating_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    left.checked_div_generic::<P1, S1, P2, S2>(right)
        .unwrap_or_else(|| saturated(left.is_negative() == right.is_negative()))
}

#[doc(hidden)]
pub fn modulo_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: SqlDecimal<P1, S1>,
) -> SqlDecimal<P2, S2> {
    left.checked_rem(right)
        .unwrap_or_else(|| panic!("Attempt to modulo by zero: {}%{}", left, right))
}

#[doc(hidden)]
pub fn modulo_SqlDecimalN_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: Option<SqlDecimal<P0, S0>>,
    right: SqlDecimal<P1, S1>,
) -> Option<SqlDecimal<P2, S2>> {
    let left = left?;
    Some(modulo_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn modulo_SqlDecimal_SqlDecimalN<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: SqlDecimal<P0, S0>,
    right: Option<SqlDecimal<P1, S1>>,
) -> Option<SqlDecimal<P2, S2>> {
    let right = right?;
    Some(modulo_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn modulo_SqlDecimalN_SqlDecimalN<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
    const P2: usize,
    const S2: usize,
>(
    left: Option<SqlDecimal<P0, S0>>,
    right: Option<SqlDecimal<P1, S1>>,
) -> Option<SqlDecimal<P2, S2>> {
    let left = left?;
    let right = right?;
    Some(modulo_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_SqlDecimal<const P: usize, const S: usize>(left: SqlDecimal<P, S>) -> SqlDecimal<P, S> {
    left.abs()
}

some_polymorphic_function1!(abs [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn floor_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, 0> {
    value.int_floor()
}

some_polymorphic_function1!(floor [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, 0>);

#[doc(hidden)]
pub fn ceil_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, 0> {
    value.int_ceil()
}

some_polymorphic_function1!(ceil [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, 0>);

#[doc(hidden)]
pub fn trunc_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, 0> {
    let trunc = value.trunc();
    SqlDecimal::new(trunc.internal_representation(), 0).unwrap()
}

some_polymorphic_function1!(trunc [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, 0>);

#[doc(hidden)]
pub fn round_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, 0> {
    value.convert_round_even::<P, 0>().unwrap()
}

some_polymorphic_function1!(round [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, 0>);

#[doc(hidden)]
pub fn round_SqlDecimal_i32<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    value.round(digits)
}

some_polymorphic_function2!(round [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, i32, i32, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn bround_SqlDecimal_i32<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    value.round_ties_even(digits)
}

some_polymorphic_function2!(bround [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, i32, i32, SqlDecimal<P, S>);

/// Saturating form of `round_SqlDecimal_i32`: rounding moves away from 0, so a result that
/// does not fit has the sign of `value`.
#[doc(hidden)]
pub fn round_SqlDecimal_i32_saturating<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    value
        .checked_round(digits)
        .unwrap_or_else(|| saturated(!value.is_negative()))
}

/// Saturating form of `bround_SqlDecimal_i32`, like [`round_SqlDecimal_i32_saturating`].
#[doc(hidden)]
pub fn bround_SqlDecimal_i32_saturating<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    value
        .checked_round_ties_even(digits)
        .unwrap_or_else(|| saturated(!value.is_negative()))
}

#[doc(hidden)]
pub fn trunc_SqlDecimal_i32<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    value.trunc_digits(digits)
}

some_polymorphic_function2!(trunc [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, i32, i32, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn truncate_SqlDecimal_i32<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    trunc_SqlDecimal_i32(value, digits)
}

some_polymorphic_function2!(truncate [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, i32, i32, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn sign_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, S> {
    // unwrap should always be safe
    value.sign().convert().unwrap()
}

some_polymorphic_function1!(sign [const P: usize, const S: usize], SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn decimal_to_integer_<const P: usize, const S: usize>(dec: SqlDecimal<P, S>) -> i128 {
    dec.internal_representation()
}

#[doc(hidden)]
pub fn decimal_to_integerN<const P: usize, const S: usize>(
    dec: Option<SqlDecimal<P, S>>,
) -> Option<i128> {
    let dec = dec?;
    Some(decimal_to_integer_(dec))
}

#[doc(hidden)]
pub fn integer_to_decimal_<const P: usize, const S: usize>(value: i128) -> SqlDecimal<P, S> {
    SqlDecimal::new(value, S as i32).unwrap()
}

#[doc(hidden)]
pub fn integer_to_decimalN<const P: usize, const S: usize>(
    value: Option<i128>,
) -> Option<SqlDecimal<P, S>> {
    let value = value?;
    Some(integer_to_decimal_(value))
}

#[cfg(test)]
mod test {
    use super::{
        SqlDecimal, bround_SqlDecimal_i32_saturating, div_saturating_SqlDecimal_SqlDecimal,
        minus_saturating_SqlDecimal_SqlDecimal, plus_saturating_SqlDecimal_SqlDecimal,
        round_SqlDecimal_i32_saturating, times_saturating_SqlDecimal_SqlDecimal,
    };

    type D3 = SqlDecimal<3, 1>;

    fn d3(tenths: i128) -> D3 {
        D3::new(tenths, 1).unwrap()
    }

    #[test]
    fn saturating_decimal_arithmetic() {
        // Operands of a different type than the result
        let big = SqlDecimal::<5, 1>::new(5000, 1).unwrap();
        let small = SqlDecimal::<5, 1>::new(-4999, 1).unwrap();
        let sum: D3 = plus_saturating_SqlDecimal_SqlDecimal(big, big);
        assert_eq!(sum, D3::MAX);
        let sum: D3 = plus_saturating_SqlDecimal_SqlDecimal(-big, -big);
        assert_eq!(sum, D3::MIN);
        // 500.0 - 499.9 fits
        let sum: D3 = plus_saturating_SqlDecimal_SqlDecimal(big, small);
        assert_eq!(sum, d3(1));
        // -499.9 - 500.0 is below the range
        let difference: D3 = minus_saturating_SqlDecimal_SqlDecimal(small, big);
        assert_eq!(difference, D3::MIN);
        let product: D3 = times_saturating_SqlDecimal_SqlDecimal(d3(-900), d3(900));
        assert_eq!(product, D3::MIN);
        let product: D3 = times_saturating_SqlDecimal_SqlDecimal(d3(-900), d3(-900));
        assert_eq!(product, D3::MAX);
        let quotient: D3 = div_saturating_SqlDecimal_SqlDecimal(d3(-900), d3(1));
        assert_eq!(quotient, D3::MIN);
        let quotient: D3 = div_saturating_SqlDecimal_SqlDecimal(d3(900), d3(20));
        assert_eq!(quotient, d3(450));
    }

    #[test]
    fn saturating_decimal_rounding() {
        // 99.95 rounds to 100.0, which does not fit in DECIMAL(4, 2)
        let value = SqlDecimal::<4, 2>::new(9995, 2).unwrap();
        assert_eq!(
            round_SqlDecimal_i32_saturating(value, 1),
            SqlDecimal::<4, 2>::MAX
        );
        assert_eq!(
            round_SqlDecimal_i32_saturating(-value, 1),
            SqlDecimal::<4, 2>::MIN
        );
        assert_eq!(
            bround_SqlDecimal_i32_saturating(-value, 1),
            SqlDecimal::<4, 2>::MIN
        );
        let fits = SqlDecimal::<4, 2>::new(1234, 2).unwrap();
        assert_eq!(
            round_SqlDecimal_i32_saturating(fits, 1),
            SqlDecimal::<4, 2>::new(1230, 2).unwrap()
        );
    }
}
