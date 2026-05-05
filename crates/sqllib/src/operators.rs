use std::ops::{Add, Div, Mul, Sub};

use dbsp::algebra::{F32, F64};
use num::{Float, PrimInt, Zero};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};
use std::cmp::Ordering;

use crate::{some_existing_operator, some_operator, type_name};

macro_rules! for_all_int_operator {
    ($func_name: ident) => {
        some_operator!($func_name, i8, i8, i8);
        some_operator!($func_name, i16, i16, i16);
        some_operator!($func_name, i32, i32, i32);
        some_operator!($func_name, i64, i64, i64);
        some_operator!($func_name, i128, i128, i128);
        some_operator!($func_name, u8, u8, u8);
        some_operator!($func_name, u16, u16, u16);
        some_operator!($func_name, u32, u32, u32);
        some_operator!($func_name, u64, u64, u64);
    };
}

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
    if ascending { result } else { result.reverse() }
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
        None => panic!(
            "'{left} + {right}' causes overflow for type {}",
            type_name(std::any::type_name::<T>())
        ),
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
        None => panic!(
            "'{left} - {right}' causes overflow for type {}",
            type_name(std::any::type_name::<T>()),
        ),
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
        None => panic!(
            "'{left} * {right}' causes overflow for type {}",
            type_name(std::any::type_name::<T>()),
        ),
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
        None => panic!(
            "'{left} / {right}' causes overflow for type {}",
            type_name(std::any::type_name::<T>()),
        ),
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
pub fn div_null<T>(left: T, right: T) -> Option<T>
where
    T: PrimInt,
{
    left.checked_div(&right)
}

// An operator which always produces a nullable result
macro_rules! nullable_operator {
    ($func_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[inline(always)]
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name >] $(< $( const $var : $ty ),* >)? ( arg0: $arg_type, arg1: $arg_type ) -> Option<$ret_type> {
                $func_name $(:: < $($var),* >)? (arg0, arg1)
            }

            #[inline(always)]
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name N>] $(< $( const $var : $ty ),* >)? ( arg0: Option<$arg_type>, arg1: Option<$arg_type> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                [<$func_name _ $short_name _ $short_name>] $(:: < $($var),* >)? (arg0, arg1)
            }

            #[inline(always)]
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N>] $(< $( const $var : $ty ),* >)? ( arg0: $arg_type, arg1: Option<$arg_type> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                [<$func_name _ $short_name _ $short_name>] $(:: < $($var),* >)? (arg0, arg1)
            }

            #[inline(always)]
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name>] $(< $( const $var : $ty ),* >)? ( arg0: Option<$arg_type>, arg1: $arg_type ) -> Option<$ret_type> {
                let arg0 = arg0?;
                [<$func_name _ $short_name _ $short_name>] $(:: < $($var),* >)? (arg0, arg1)
            }
        }
    }
}

nullable_operator!(div_null, i8, i8, i8);
nullable_operator!(div_null, i16, i16, i16);
nullable_operator!(div_null, i32, i32, i32);
nullable_operator!(div_null, i64, i64, i64);
nullable_operator!(div_null, i128, i128, i128);
nullable_operator!(div_null, u8, u8, u8);
nullable_operator!(div_null, u16, u16, u16);
nullable_operator!(div_null, u32, u32, u32);
nullable_operator!(div_null, u64, u64, u64);
nullable_operator!(div_null, u128, u128, u128);

#[inline(always)]
#[doc(hidden)]
pub(crate) fn finite_or_null<T>(value: T) -> Option<T>
where
    T: Float,
{
    if value.is_nan() || value.is_infinite() {
        None
    } else {
        Some(value)
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_d_d(left: F64, right: F64) -> Option<F64> {
    let result = left.into_inner() / right.into_inner();
    finite_or_null(result).map(|x| x.into())
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_dN_d(left: Option<F64>, right: F64) -> Option<F64> {
    let left = left?;
    div_null_d_d(left, right)
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_d_dN(left: F64, right: Option<F64>) -> Option<F64> {
    let right = right?;
    div_null_d_d(left, right)
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64> {
    let left = left?;
    let right = right?;
    div_null_d_d(left, right)
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_f_f(left: F32, right: F32) -> Option<F32> {
    let result = left.into_inner() / right.into_inner();
    finite_or_null(result).map(|x| x.into())
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_fN_f(left: Option<F32>, right: F32) -> Option<F32> {
    let left = left?;
    div_null_f_f(left, right)
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_f_fN(left: F32, right: Option<F32>) -> Option<F32> {
    let right = right?;
    div_null_f_f(left, right)
}

#[inline(always)]
#[doc(hidden)]
pub fn div_null_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32> {
    let left = left?;
    let right = right?;
    div_null_f_f(left, right)
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

#[inline(always)]
#[doc(hidden)]
pub fn max_ignore_nulls__<T>(left: T, right: T) -> T
where
    T: Ord,
{
    left.max(right)
}

#[inline(always)]
#[doc(hidden)]
pub fn max_ignore_nulls_N_<T>(left: Option<T>, right: T) -> Option<T>
where
    T: Ord,
{
    match left {
        None => Some(right),
        Some(left) => Some(left.max(right)),
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn max_ignore_nulls__N<T>(left: T, right: Option<T>) -> Option<T>
where
    T: Ord,
{
    match right {
        None => Some(left),
        Some(right) => Some(left.max(right)),
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn max_ignore_nulls_N_N<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Ord,
{
    match left {
        None => right,
        _ => left.max(right),
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn min_ignore_nulls__<T>(left: T, right: T) -> T
where
    T: Ord,
{
    left.min(right)
}

#[inline(always)]
#[doc(hidden)]
pub fn min_ignore_nulls_N_<T>(left: Option<T>, right: T) -> Option<T>
where
    T: Ord,
{
    match left {
        None => Some(right),
        Some(left) => Some(left.min(right)),
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn min_ignore_nulls__N<T>(left: T, right: Option<T>) -> Option<T>
where
    T: Ord,
{
    match right {
        None => Some(left),
        Some(right) => Some(left.min(right)),
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn min_ignore_nulls_N_N<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Ord,
{
    match left {
        None => right,
        Some(left) => match right {
            None => Some(left),
            Some(right) => Some(left.min(right)),
        },
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn max_null_wins__<T>(left: T, right: T) -> T
where
    T: Ord,
{
    left.max(right)
}

#[inline(always)]
#[doc(hidden)]
pub fn max_null_wins_N_<T>(left: Option<T>, right: T) -> Option<T>
where
    T: Ord,
{
    left.map(|left| left.max(right))
}

#[inline(always)]
#[doc(hidden)]
pub fn max_null_wins__N<T>(left: T, right: Option<T>) -> Option<T>
where
    T: Ord,
{
    right.map(|right| left.max(right))
}

#[inline(always)]
#[doc(hidden)]
pub fn max_null_wins_N_N<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Ord,
{
    match (&left, &right) {
        (&None, _) => None,
        (_, &None) => None,
        (_, _) => left.max(right),
    }
}

#[doc(hidden)]
pub fn blackbox<T>(value: T) -> T {
    value
}
