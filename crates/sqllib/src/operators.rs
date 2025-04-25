use std::ops::{Add, Div, Mul, Sub};

use dbsp::algebra::{HasZero, F32, F64};
use num::PrimInt;
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, ToPrimitive};
use std::cmp::Ordering;

use crate::{for_all_int_operator, some_existing_operator, some_operator};

use rust_decimal::Decimal;

macro_rules! for_all_compare {
    ($func_name: ident, $ret_type: ty, $t:ty where $($bounds:tt)*) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name __ >]<T: $($bounds)*>( arg0: T, arg1: T ) -> $ret_type {
                $func_name(arg0, arg1)
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _N_ >]<T: $($bounds)*>( arg0: Option<T>, arg1: T ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([< $func_name __ >](arg0, arg1))
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name __N >]<T: $($bounds)*>( arg0: T, arg1: Option<T> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([< $func_name __ >](arg0, arg1))
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _N_N >]<T: $($bounds)*>( arg0: Option<T>, arg1: Option<T> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([< $func_name __ >](arg0, arg1))
            }
        }
    };
}

/*
macro_rules! for_all_compare {
    ($func_name: ident, $ret_type: ty, $($bounds:tt)*) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name __ >]<T: $($bounds)*>( arg0: &T, arg1: &T ) -> $ret_type {
                $func_name(arg0, arg1)
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _N_ >]<T: $($bounds)*>( arg0: &Option<T>, arg1: &T ) -> Option<$ret_type> {
                arg0.as_ref().and_then(|arg0| Some([< $func_name __ >](arg0, arg1)))
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name __N >]<T: $($bounds)*>( arg0: &T, arg1: &Option<T> ) -> Option<$ret_type> {
                arg1.as_ref().and_then(|arg1| Some([< $func_name __ >](arg0, arg1)))
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _N_N >]<T: $($bounds)*>( arg0: &Option<T>, arg1: &Option<T> ) -> Option<$ret_type> {
                arg0.as_ref().and_then(|arg0|
                                       arg1.as_ref().and_then(|arg1| Some([< $func_name __ >](arg0, arg1))))
            }
        }
    };
}
*/

#[inline(always)]
#[doc(hidden)]
pub(crate) fn eq<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    left == right
}

for_all_compare!(eq, bool, T where Eq);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn neq<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    left != right
}

for_all_compare!(neq, bool, T where Eq);

#[doc(hidden)]
pub fn compareN<T>(
    left: &Option<T>,
    right: &Option<T>,
    ascending: bool,
    nullsFirst: bool,
) -> std::cmp::Ordering
where
    T: Ord,
{
    if nullsFirst {
        match (left, right) {
            (&None, &None) => Ordering::Equal,
            (&None, _) => Ordering::Less,
            (_, &None) => Ordering::Greater,
            (Some(left), Some(right)) => compare_(left, right, ascending, nullsFirst),
        }
    } else {
        match (left, right) {
            (&None, &None) => Ordering::Equal,
            (&None, _) => Ordering::Greater,
            (_, &None) => Ordering::Less,
            (Some(left), Some(right)) => compare_(left, right, ascending, nullsFirst),
        }
    }
}

#[doc(hidden)]
pub fn compare_<T>(
    left: &T,
    right: &T,
    ascending: bool,
    // There can be no nulls
    _nullsFirst: bool,
) -> std::cmp::Ordering
where
    T: Ord,
{
    let result = left.cmp(right);
    if ascending {
        result
    } else {
        result.reverse()
    }
}

#[doc(hidden)]
pub fn nullif__<T>(left: T, right: T) -> Option<T>
where
    T: Eq,
{
    if left == right {
        None
    } else {
        Some(left)
    }
}

#[doc(hidden)]
pub fn nullif_N_<T>(left: Option<T>, right: T) -> Option<T>
where
    T: Eq,
{
    match left {
        None => None,
        Some(left) => nullif__(left, right),
    }
}

#[doc(hidden)]
pub fn nullif__N<T>(left: T, right: Option<T>) -> Option<T>
where
    T: Eq,
{
    match right {
        None => Some(left),
        Some(right) => nullif__(left, right),
    }
}

#[doc(hidden)]
pub fn nullif_N_N<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Eq,
{
    if left == right {
        None
    } else {
        left
    }
}

#[doc(hidden)]
#[inline(always)]
pub(crate) fn lt<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left < right
}

for_all_compare!(lt, bool, T where Ord);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn gt<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left > right
}

for_all_compare!(gt, bool, T where Ord);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn lte<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left <= right
}

for_all_compare!(lte, bool, T where Ord);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn gte<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left >= right
}

for_all_compare!(gte, bool, T where Ord);

/*
#[inline(always)]
#[doc(hidden)]
pub(crate) fn eq<T>(left: &T, right: &T) -> bool
where
    T: Eq,
{
    left == right
}

for_all_compare!(eq, bool, Eq);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn neq<T>(left: &T, right: &T) -> bool
where
    T: Eq,
{
    left != right
}

for_all_compare!(neq, bool, Eq);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn lt<T>(left: &T, right: &T) -> bool
where
    T: Ord,
{
    left < right
}

for_all_compare!(lt, bool, Ord);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn gt<T>(left: &T, right: &T) -> bool
where
    T: Ord,
{
    left > right
}

for_all_compare!(gt, bool, Ord);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn lte<T>(left: &T, right: &T) -> bool
where
    T: Ord,
{
    left <= right
}

for_all_compare!(lte, bool, Ord);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn gte<T>(left: &T, right: &T) -> bool
where
    T: Ord,
{
    left >= right
}

for_all_compare!(gte, bool, Ord);
*/

#[doc(hidden)]
#[inline(always)]
fn plus<T>(left: T, right: T) -> T
where
    T: CheckedAdd,
{
    left.checked_add(&right)
        .expect("attempt to add with overflow")
}

for_all_int_operator!(plus);
some_operator!(plus, decimal, Decimal, Decimal);

#[doc(hidden)]
fn fp_plus<T>(left: T, right: T) -> T
where
    T: Add<Output = T>,
{
    left + right
}

some_operator!(fp_plus, plus, f, F32, F32);
some_operator!(fp_plus, plus, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
fn minus<T>(left: T, right: T) -> T
where
    T: CheckedSub,
{
    left.checked_sub(&right)
        .expect("attempt to subtract with overflow")
}

for_all_int_operator!(minus);
some_operator!(minus, decimal, Decimal, Decimal);

#[doc(hidden)]
fn fp_minus<T>(left: T, right: T) -> T
where
    T: Sub<Output = T>,
{
    left - right
}

some_operator!(fp_minus, minus, f, F32, F32);
some_operator!(fp_minus, minus, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
fn modulo<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    if Some(-1) == right.to_isize() {
        return T::zero();
    }

    left % right
}

for_all_int_operator!(modulo);

#[doc(hidden)]
fn f32_modulo(left: F32, right: F32) -> F32 {
    F32::new(left.into_inner() % right.into_inner())
}

some_operator!(f32_modulo, modulo, f, F32, F32);

#[doc(hidden)]
fn f64_modulo(left: F64, right: F64) -> F64 {
    F64::new(left.into_inner() % right.into_inner())
}

some_operator!(f64_modulo, modulo, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
fn decimal_modulo(left: Decimal, right: Decimal) -> Decimal {
    left % right
}

some_operator!(decimal_modulo, modulo, decimal, Decimal, Decimal);

#[inline(always)]
#[doc(hidden)]
fn times<T>(left: T, right: T) -> T
where
    T: CheckedMul,
{
    left.checked_mul(&right)
        .expect("attempt to multiply with overflow")
}

for_all_int_operator!(times);
some_operator!(times, decimal, Decimal, Decimal);

#[doc(hidden)]
fn fp_times<T>(left: T, right: T) -> T
where
    T: Mul<Output = T>,
{
    left * right
}

some_operator!(fp_times, times, f, F32, F32);
some_operator!(fp_times, times, d, F64, F64);

/*

TODO: shifts seem wrong

#[inline(always)]
fn shiftr<T, U>(left: T, right: U) -> T
where
    T: PrimInt,
    U: Into<usize>,
{ left >> right.into() }

for_all_int_operator!(shiftr);

#[inline(always)]
fn shiftl<T, U>(left: T, right: U) -> T
where
    T: PrimInt,
    U: Into<usize>,
{ left << right.into() }

for_all_int_operator!(shiftl);

*/

#[inline(always)]
#[doc(hidden)]
fn band<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left & right
}

for_all_int_operator!(band);

#[inline(always)]
#[doc(hidden)]
fn bor<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left | right
}

for_all_int_operator!(bor);

#[inline(always)]
#[doc(hidden)]
fn bxor<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left ^ right
}

for_all_int_operator!(bxor);

#[inline(always)]
#[doc(hidden)]
fn div<T>(left: T, right: T) -> T
where
    T: CheckedDiv + ToPrimitive,
{
    let panic_message = if Some(0) == right.to_isize() {
        "attempt to divide by zero"
    } else {
        "attempt to divide with overflow"
    };

    left.checked_div(&right).expect(panic_message)
}

for_all_int_operator!(div);
some_operator!(div, decimal, Decimal, Decimal);

#[doc(hidden)]
fn fp_div<T>(left: T, right: T) -> T
where
    T: Div<Output = T>,
{
    left / right
}

some_operator!(fp_div, div, f, F32, F32);
some_operator!(fp_div, div, d, F64, F64);

#[doc(hidden)]
pub fn plus_u_u(left: usize, right: usize) -> usize {
    left + right
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null__<T>(left: T, right: T) -> Option<T>
where
    T: Div<Output = T> + HasZero,
{
    if right.is_zero() {
        None
    } else {
        Some(left.div(right))
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn div_nullN_<T>(left: Option<T>, right: T) -> Option<T>
where
    T: Div<Output = T> + HasZero,
{
    let left = left?;
    div_null__::<T>(left, right)
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_N<T>(left: T, right: Option<T>) -> Option<T>
where
    T: Div<Output = T> + HasZero,
{
    let right = right?;
    div_null__(left, right)
}

#[inline(always)]
#[doc(hidden)]
pub fn div_nullNN<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Div<Output = T> + HasZero,
{
    let left = left?;
    let right = right?;
    div_null__(left, right)
}

#[inline(always)]
#[doc(hidden)]
fn max<T>(left: T, right: T) -> T
where
    T: Ord,
{
    left.max(right)
}

for_all_compare!(max, T, T where Ord);

#[inline(always)]
#[doc(hidden)]
fn min<T>(left: T, right: T) -> T
where
    T: Ord,
{
    left.min(right)
}

for_all_compare!(min, T, T where Ord);

/*
#[inline(always)]
#[doc(hidden)]
fn max<T>(left: &T, right: &T) -> T
where
    T: Ord + Clone,
{
    left.max(right).clone()
}

for_all_compare!(max, T, Ord + Clone);

#[inline(always)]
#[doc(hidden)]
fn min<T>(left: &T, right: &T) -> T
where
    T: Ord + Clone,
{
    left.min(right).clone()
}

for_all_compare!(min, T, Ord + Clone);
*/

#[doc(hidden)]
pub fn blackbox<T>(value: T) -> T {
    value
}
