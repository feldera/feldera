use std::ops::{Add, Div, Mul, Sub};

use dbsp::algebra::{HasZero, F32, F64};
use num::{PrimInt, Zero};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};
use std::cmp::Ordering;

use crate::{for_all_int_operator, some_existing_operator, some_operator};

macro_rules! for_all_compare {
    // Arguments with different types
    ($func_name: ident, $ret_type: ty, $lt: ty, $rt: ty where $($bounds:tt)*) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name __ >]<$lt, $rt>( arg0: $lt, arg1: $rt ) -> $ret_type
            where
                $($bounds)*
            {
                $func_name(arg0, arg1)
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _N_ >]<$lt, $rt>( arg0: Option<$lt>, arg1: $rt ) -> Option<$ret_type>
            where
                $($bounds)*
            {
                let arg0 = arg0?;
                Some([< $func_name __ >](arg0, arg1))
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name __N >]<$lt, $rt>( arg0: $lt, arg1: Option<$rt> ) -> Option<$ret_type>
            where
                $($bounds)*
            {
                let arg1 = arg1?;
                Some([< $func_name __ >](arg0, arg1))
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _N_N >]<$lt, $rt>( arg0: Option<$lt>, arg1: Option<$rt> ) -> Option<$ret_type>
            where
                $($bounds)*
            {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([< $func_name __ >](arg0, arg1))
            }
        }
    };

    // Both arguments same type
    ($func_name: ident, $ret_type: ty, $t: ty where $($bounds:tt)*) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name __ >]<$t>( arg0: $t, arg1: $t ) -> $ret_type
            where
                $($bounds)*
            {
                $func_name(arg0, arg1)
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _N_ >]<$t>( arg0: Option<$t>, arg1: $t ) -> Option<$ret_type>
            where
                $($bounds)*
            {
                let arg0 = arg0?;
                Some([< $func_name __ >](arg0, arg1))
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name __N >]<$t>( arg0: $t, arg1: Option<$t> ) -> Option<$ret_type>
            where
                $($bounds)*
            {
                let arg1 = arg1?;
                Some([< $func_name __ >](arg0, arg1))
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _N_N >]<$t>( arg0: Option<$t>, arg1: Option<$t> ) -> Option<$ret_type>
            where
                $($bounds)*
            {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([< $func_name __ >](arg0, arg1))
            }
        }
    };
}

#[inline(always)]
#[doc(hidden)]
pub(crate) fn eq<LT, RT>(left: LT, right: RT) -> bool
where
    LT: PartialEq<RT>,
{
    left == right
}

for_all_compare!(eq, bool, LT, RT where LT: PartialEq<RT>);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn neq<LT, RT>(left: LT, right: RT) -> bool
where
    LT: PartialEq<RT>,
{
    left != right
}

for_all_compare!(neq, bool, LT, RT where LT: PartialEq<RT>);

#[doc(hidden)]
pub fn compareN<T>(
    left: &Option<T>,
    right: &Option<T>,
    ascending: bool,
    nullsFirst: bool,
) -> std::cmp::Ordering
where
    T: PartialOrd<T>,
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
    T: PartialOrd<T>,
{
    let result = left.partial_cmp(right).unwrap();
    if ascending {
        result
    } else {
        result.reverse()
    }
}

#[doc(hidden)]
#[inline(always)]
pub(crate) fn lt<LT, RT>(left: LT, right: RT) -> bool
where
    LT: PartialOrd<RT>,
{
    left < right
}

for_all_compare!(lt, bool, LT, RT where LT: PartialOrd<RT>);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn gt<LT, RT>(left: LT, right: RT) -> bool
where
    LT: PartialOrd<RT>,
{
    left > right
}

for_all_compare!(gt, bool, LT, RT where LT: PartialOrd<RT>);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn lte<LT, RT>(left: LT, right: RT) -> bool
where
    LT: PartialOrd<RT>,
{
    left <= right
}

for_all_compare!(lte, bool, LT, RT where LT: PartialOrd<RT>);

#[doc(hidden)]
#[inline(always)]
pub(crate) fn gte<LT, RT>(left: LT, right: RT) -> bool
where
    LT: PartialOrd<RT>,
{
    left >= right
}

for_all_compare!(gte, bool, LT, RT where LT: PartialOrd<RT>);

#[doc(hidden)]
#[inline(always)]
fn plus<T>(left: T, right: T) -> T
where
    T: CheckedAdd + std::fmt::Display,
{
    match left.checked_add(&right) {
        Some(value) => value,
        None => panic!("'{left} + {right}' causes overflow"),
    }
}

for_all_int_operator!(plus);

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
    T: CheckedSub + std::fmt::Display,
{
    match left.checked_sub(&right) {
        Some(result) => result,
        None => panic!("'{left} - {right}' causes overflow"),
    }
}

for_all_int_operator!(minus);

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
fn times<T>(left: T, right: T) -> T
where
    T: CheckedMul + std::fmt::Display,
{
    match left.checked_mul(&right) {
        None => panic!("'{left} * {right}' causes overflow"),
        Some(value) => value,
    }
}

for_all_int_operator!(times);

#[doc(hidden)]
fn fp_times<T>(left: T, right: T) -> T
where
    T: Mul<Output = T>,
{
    left * right
}

some_operator!(fp_times, times, f, F32, F32);
some_operator!(fp_times, times, d, F64, F64);

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
    T: CheckedDiv + Zero + Eq + std::fmt::Display,
{
    match left.checked_div(&right) {
        None => panic!("'{left} / {right}' causes overflow"),
        Some(value) => value,
    }
}

for_all_int_operator!(div);

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

for_all_compare!(max, T, T where T: Ord);

#[inline(always)]
#[doc(hidden)]
fn min<T>(left: T, right: T) -> T
where
    T: Ord,
{
    left.min(right)
}

for_all_compare!(min, T, T where T: Ord);

#[doc(hidden)]
pub fn blackbox<T>(value: T) -> T {
    value
}
