//! Operations on Decimal values

use crate::{some_polymorphic_function1, some_polymorphic_function2};
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

#[doc(hidden)]
pub fn plus_SqlDecimalN_SqlDecimal<
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
    Some(plus_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn plus_SqlDecimal_SqlDecimalN<
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
    Some(plus_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn plus_SqlDecimalN_SqlDecimalN<
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
    Some(plus_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

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

#[doc(hidden)]
pub fn minus_SqlDecimalN_SqlDecimal<
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
    Some(minus_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn minus_SqlDecimal_SqlDecimalN<
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
    Some(minus_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn minus_SqlDecimalN_SqlDecimalN<
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
    Some(minus_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

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

#[doc(hidden)]
pub fn times_SqlDecimalN_SqlDecimal<
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
    Some(times_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn times_SqlDecimal_SqlDecimalN<
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
    Some(times_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn times_SqlDecimalN_SqlDecimalN<
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
    Some(times_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

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

#[doc(hidden)]
pub fn div_SqlDecimalN_SqlDecimal<
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
    Some(div_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn div_SqlDecimal_SqlDecimalN<
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
    Some(div_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
}

#[doc(hidden)]
pub fn div_SqlDecimalN_SqlDecimalN<
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
    Some(div_SqlDecimal_SqlDecimal::<P0, S0, P1, S1, P2, S2>(
        left, right,
    ))
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

some_polymorphic_function1!(abs<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn floor_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, 0> {
    value.int_floor()
}

some_polymorphic_function1!(floor<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, 0>);

#[doc(hidden)]
pub fn ceil_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, 0> {
    value.int_ceil()
}

some_polymorphic_function1!(ceil<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, 0>);

#[doc(hidden)]
pub fn trunc_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, 0> {
    let trunc = value.trunc();
    SqlDecimal::new(trunc.internal_representation(), 0).unwrap()
}

some_polymorphic_function1!(trunc<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, 0>);

#[doc(hidden)]
pub fn round_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, 0> {
    value.convert_round_even::<P, 0>().unwrap()
}

some_polymorphic_function1!(round<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, 0>);

#[doc(hidden)]
pub fn round_SqlDecimal_i32<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    value.round(digits)
}

some_polymorphic_function2!(round<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, i32, i32, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn bround_SqlDecimal_i32<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    value.round_ties_even(digits)
}

some_polymorphic_function2!(bround<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, i32, i32, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn truncate_SqlDecimal_i32<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    digits: i32,
) -> SqlDecimal<P, S> {
    value.trunc_digits(digits)
}

some_polymorphic_function2!(truncate<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, i32, i32, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn sign_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlDecimal<P, S> {
    // unwrap should always be safe
    value.sign().convert().unwrap()
}

some_polymorphic_function1!(sign<const P: usize, const S: usize>, SqlDecimal, SqlDecimal<P, S>, SqlDecimal<P, S>);

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
