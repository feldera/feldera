use std::ops::{Add, Div, Mul, Neg, Sub};

use dbsp::algebra::{F32, F64};
use num::{Float, PrimInt, Zero};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedSub};
use std::cmp::Ordering;

use crate::{
    LongInterval, ShortInterval, SqlDecimal, some_existing_operator, some_operator,
    some_polymorphic_null_function2, type_name,
};

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
fn neg<T>(value: T) -> T
where
    T: CheckedNeg + std::fmt::Display,
{
    match value.checked_neg() {
        Some(result) => result,
        None => panic!(
            "'-({value})' causes overflow for type {}",
            type_name(std::any::type_name::<T>())
        ),
    }
}

/// Defines the unary minus `neg_<short_name>_` and its nullable form `neg_<short_name>N_`.
macro_rules! neg_operator {
    ($short_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $type: ty, $implementation: expr) => {
        ::paste::paste! {
            #[inline(always)]
            #[doc(hidden)]
            pub fn [<neg_ $short_name _>] $(< $( const $var : $ty ),* >)? (value: $type) -> $type {
                $implementation(value)
            }

            #[inline(always)]
            #[doc(hidden)]
            pub fn [<neg_ $short_name N_>] $(< $( const $var : $ty ),* >)? (value: Option<$type>) -> Option<$type> {
                value.map(|value| [<neg_ $short_name _>](value))
            }
        }
    };
}

neg_operator!(i8, i8, neg);
neg_operator!(i16, i16, neg);
neg_operator!(i32, i32, neg);
neg_operator!(i64, i64, neg);
neg_operator!(i128, i128, neg);
neg_operator!(f, F32, Neg::neg);
neg_operator!(d, F64, Neg::neg);
neg_operator!(SqlDecimal<const P: usize, const S: usize>, SqlDecimal<P, S>, Neg::neg);
neg_operator!(ShortInterval, ShortInterval, Neg::neg);
neg_operator!(LongInterval, LongInterval, Neg::neg);

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

/// Saturating forms of the integer operators.  A result above the range of the type is the
/// maximum value of the type, and a result below it is the minimum.  A divisor of 0 still fails.
macro_rules! saturating_int_operator {
    ($func_name: ident, $method: ident) => {
        saturating_int_operator!($func_name, $method, i8, i16, i32, i64, i128, u8, u16, u32, u64);
    };
    ($func_name: ident, $method: ident, $($t: ident),*) => {
        ::paste::paste! {
            $(
                #[doc(hidden)]
                #[inline(always)]
                pub fn [<$func_name _saturating_ $t _ $t>](left: $t, right: $t) -> $t {
                    left.$method(right)
                }
            )*
        }
    };
}

saturating_int_operator!(plus, saturating_add);
saturating_int_operator!(minus, saturating_sub);
saturating_int_operator!(times, saturating_mul);
saturating_int_operator!(div, saturating_div);

/// Saturating forms of the floating-point operators.  These operators never fail: a result
/// too large for the type is an infinity.
macro_rules! saturating_fp_operator {
    ($func_name: ident, $op: tt) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _saturating_f_f>](left: F32, right: F32) -> F32 {
                left $op right
            }

            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _saturating_d_d>](left: F64, right: F64) -> F64 {
                left $op right
            }
        }
    };
}

saturating_fp_operator!(plus, +);
saturating_fp_operator!(minus, -);
saturating_fp_operator!(times, *);
saturating_fp_operator!(div, /);

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

some_polymorphic_null_function2!(div_null, d, F64, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn div_null_f_f(left: F32, right: F32) -> Option<F32> {
    let result = left.into_inner() / right.into_inner();
    finite_or_null(result).map(|x| x.into())
}

some_polymorphic_null_function2!(div_null, f, F32, f, F32, F32);

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

/// SQL COALESCE of two nullable values of the same type.  COALESCE is lazy:
/// the right operand is evaluated only when the left operand is NULL.
#[doc(hidden)]
#[macro_export]
macro_rules! coalesce_N_N {
    ($left:expr, $right:expr) => {
        match $left {
            None => $right,
            left => left,
        }
    };
}

#[cfg(test)]
mod tests {
    use super::{
        div_saturating_i32_i32, minus_saturating_u32_u32, neg_i8_, neg_i16N_, neg_i64_,
        plus_saturating_d_d, plus_saturating_i8_i8, times_saturating_i64_i64,
    };
    use dbsp::algebra::F64;
    use std::cell::Cell;

    #[test]
    fn neg() {
        assert_eq!(neg_i64_(i64::MAX), -i64::MAX);
        assert_eq!(neg_i16N_(None), None);
    }

    #[test]
    #[should_panic(expected = "'-(-128)' causes overflow for type TINYINT")]
    fn neg_of_min_overflows() {
        neg_i8_(i8::MIN);
    }

    #[test]
    fn coalesce_is_lazy() {
        let values = [Some(1), Some(2), None];
        for left in values {
            for right in values {
                let evaluations = Cell::new(0);
                let result = coalesce_N_N!(left, {
                    evaluations.set(evaluations.get() + 1);
                    right
                });
                let context = format!("coalesce_N_N({left:?}, {right:?})");
                assert_eq!(result, left.or(right), "{context}");
                let expected = if left.is_none() { 1 } else { 0 };
                assert_eq!(evaluations.get(), expected, "{context}");
            }
        }
    }

    #[test]
    fn saturating_integer_arithmetic() {
        assert_eq!(plus_saturating_i8_i8(100, 100), i8::MAX);
        assert_eq!(plus_saturating_i8_i8(-100, -100), i8::MIN);
        assert_eq!(plus_saturating_i8_i8(100, -100), 0);
        assert_eq!(minus_saturating_u32_u32(10, 20), 0);
        assert_eq!(times_saturating_i64_i64(i64::MAX, 2), i64::MAX);
        assert_eq!(times_saturating_i64_i64(i64::MAX, -2), i64::MIN);
        assert_eq!(div_saturating_i32_i32(i32::MIN, -1), i32::MAX);
        assert_eq!(div_saturating_i32_i32(7, 2), 3);
    }

    #[test]
    fn saturating_fp_arithmetic() {
        let max = F64::new(f64::MAX);
        assert_eq!(plus_saturating_d_d(max, max), F64::new(f64::INFINITY));
    }
}
