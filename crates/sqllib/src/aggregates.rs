#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(clippy::let_and_return)]
#![allow(clippy::unnecessary_cast)]

use crate::binary::ByteArray;
use crate::decimal::Dec;
use crate::{FromInteger, ToInteger, Weight};
use dbsp::algebra::{FirstLargeValue, HasOne, HasZero, SignedPrimInt, UnsignedPrimInt, F32, F64};
use num::PrimInt;
use num_traits::CheckedAdd;
use rust_decimal::Decimal;
use std::cmp::Ord;
use std::fmt::Debug;
use std::marker::Copy;

/// Holds some methods for wrapping values into unsigned values
/// This is used by partitioned_rolling_aggregate
#[doc(hidden)]
pub struct UnsignedWrapper {}

/// The conversion involves four types:
/// O: the original type which is being converted
/// S: a signed type that the original type can be converted to losslessly
/// I: an intermediate signed type wider than S
/// U: an unsigned type the same width as I
//  None of the 'unwrap' calls below should ever fail.
impl UnsignedWrapper {
    // Conversion from O to U
    // O cannot be nullable, so nullsLast is irrelevant
    #[doc(hidden)]
    pub fn from_signed<O, S, I, U>(value: O, ascending: bool, _nullsLast: bool) -> U
    where
        O: ToInteger<S> + Debug,
        S: PrimInt,
        I: SignedPrimInt + From<S>,
        U: UnsignedPrimInt + TryFrom<I> + Debug,
        <U as TryFrom<I>>::Error: std::fmt::Debug,
    {
        let s = <O as ToInteger<S>>::to_integer(&value);
        let i = <I as From<S>>::from(s);
        let i = if ascending { i } else { -i };
        // We reserve 0 for NULL, so we start at 1 in case !nullsLast
        let i = i - <I as From<S>>::from(S::min_value()) + <I as HasOne>::one();
        debug_assert!(i >= <I as HasZero>::zero());
        let result = <U as TryFrom<I>>::try_from(i).unwrap();
        // println!("Encoded {:?} as {:?}", value, result);
        result
    }

    // Conversion from O to U
    #[doc(hidden)]
    pub fn from_option<O, S, I, U>(value: Option<O>, ascending: bool, nullsLast: bool) -> U
    where
        O: ToInteger<S> + Debug,
        S: SignedPrimInt,
        I: SignedPrimInt + From<S>,
        U: UnsignedPrimInt + TryFrom<I> + HasZero + Debug,
        <U as TryFrom<I>>::Error: std::fmt::Debug,
    {
        match value {
            None => {
                if nullsLast {
                    U::large()
                } else {
                    <U as HasZero>::zero()
                }
            }
            Some(value) => UnsignedWrapper::from_signed::<O, S, I, U>(value, ascending, nullsLast),
        }
    }

    // Conversion from U to O
    // O cannot be nullable, so nullsLast is irrelevant
    // However, we still have it so that the signature of both
    // functions is the same.
    #[doc(hidden)]
    pub fn to_signed<O, S, I, U>(value: U, ascending: bool, _nullsLast: bool) -> O
    where
        O: FromInteger<S> + Debug,
        S: SignedPrimInt + TryFrom<I> + Debug,
        I: SignedPrimInt + From<S> + TryFrom<U>,
        U: UnsignedPrimInt + TryFrom<I>,
        <I as TryFrom<U>>::Error: std::fmt::Debug,
        <S as TryFrom<I>>::Error: std::fmt::Debug,
    {
        let i = <I as TryFrom<U>>::try_from(value).unwrap();
        let i = i + <I as From<S>>::from(S::min_value()) - <I as HasOne>::one();
        let i = if ascending { i } else { -i };
        let s = <S as TryFrom<I>>::try_from(i).unwrap();
        let result = <O as FromInteger<S>>::from_integer(&s);
        // println!("Decoded {:?} as {:?}", value, result);
        result
    }

    // Conversion from U to O where the 0 of U is converted to None
    #[doc(hidden)]
    pub fn to_signed_option<O, S, I, U>(value: U, ascending: bool, nullsLast: bool) -> Option<O>
    where
        O: FromInteger<S> + Debug,
        S: SignedPrimInt + TryFrom<U> + TryFrom<I>,
        I: SignedPrimInt + From<S> + TryFrom<U>,
        U: UnsignedPrimInt + TryFrom<I> + Debug,
        <I as TryFrom<U>>::Error: std::fmt::Debug,
        <S as TryFrom<I>>::Error: std::fmt::Debug,
    {
        if nullsLast {
            if <U as FirstLargeValue>::large() == value {
                return None;
            }
        } else if <U as HasZero>::is_zero(&value) {
            return None;
        }
        let o = UnsignedWrapper::to_signed::<O, S, I, U>(value, ascending, nullsLast);
        Some(o)
    }
}

// Macro to create variants of an aggregation function
// There must exist a function f(left: T, right: T) -> T ($base_name is f)
// This creates 4 more functions ($func_name = f)
// f_t_t(left: T, right: T) -> T
// f_tN_t(left: Option<T>, right: T) -> Option<T>
// etc.
// And 4 more functions:
// f_t_t_conditional(left: T, right: T, predicate: bool) -> T
macro_rules! some_aggregate {
    ($base_name: ident, $func_name:ident, $short_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name>]( left: $arg_type, right: $arg_type ) -> $arg_type {
                $base_name(left, right)
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name>]( left: Option<$arg_type>, right: $arg_type ) -> Option<$arg_type> {
                match left {
                    None => Some(right.clone()),
                    Some(left) => Some($base_name(left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N>]( left: $arg_type, right: Option<$arg_type> ) -> Option<$arg_type> {
                match right {
                    None => Some(left.clone()),
                    Some(right) => Some($base_name(left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name N>]( left: Option<$arg_type>, right: Option<$arg_type> ) -> Option<$arg_type> {
                match (left.clone(), right.clone()) {
                    (None, _) => right.clone(),
                    (_, None) => left.clone(),
                    (Some(left), Some(right)) => Some($base_name(left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name _conditional>]( left: $arg_type, right: $arg_type, predicate: bool ) -> $arg_type {
                if predicate {
                    $base_name(left, right)
                } else {
                    left.clone()
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name _conditional>]( left: Option<$arg_type>, right: $arg_type, predicate: bool ) -> Option<$arg_type> {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => left.clone(),
                    (None, _, _) => Some(right.clone()),
                    (Some(x), _, _) => Some($base_name(x, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N _conditional>]( left: $arg_type, right: Option<$arg_type>, predicate: bool ) -> Option<$arg_type> {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => Some(left.clone()),
                    (_, None, _) => Some(left.clone()),
                    (_, Some(y), _) => Some($base_name(left, y)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name N _conditional>]( left: Option<$arg_type>, right: Option<$arg_type>, predicate: bool ) -> Option<$arg_type> {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => left.clone(),
                    (None, _, _) => right.clone(),
                    (_, None, _) => left.clone(),
                    (Some(x), Some(y), _) => Some($base_name(x, y)),
                }
            }
        }
    };
}
pub(crate) use some_aggregate;

// Macro to create variants of an aggregation function
// There must exist a function f(left: T, right: T) -> T ($base_name is f)
// This creates 2 more functions ($func_name = f)
// f_t_t(left: T, right: T) -> T
// f_tN_t(left: Option<T>, right: T) -> T
// And 2 more functions:
// f_t_t_conditional(left: T, right: T, predicate: bool) -> T
// Only the right value can be null
macro_rules! some_aggregate_non_null {
    ($base_name: ident, $func_name:ident, $short_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name>]( left: $arg_type, right: $arg_type ) -> $arg_type {
                $base_name(left, right)
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N>]( left: $arg_type, right: Option<$arg_type> ) -> $arg_type {
                match right {
                    None => left.clone(),
                    Some(right) => $base_name(left, right),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name _conditional>]( left: $arg_type, right: $arg_type, predicate: bool ) -> $arg_type {
                if predicate {
                    $base_name(left, right)
                } else {
                    left.clone()
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N _conditional>]( left: $arg_type, right: Option<$arg_type>, predicate: bool ) -> $arg_type {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => left.clone(),
                    (_, None, _) => left.clone(),
                    (_, Some(y), _) => $base_name(left, y),
                }
            }
        }
    };
}
pub(crate) use some_aggregate_non_null;

macro_rules! for_all_int_aggregate {
    ($base_name: ident, $func_name: ident) => {
        some_aggregate!($base_name, $func_name, i8, i8);
        some_aggregate!($base_name, $func_name, i16, i16);
        some_aggregate!($base_name, $func_name, i32, i32);
        some_aggregate!($base_name, $func_name, i64, i64);
    };
}

macro_rules! for_all_int_aggregate_non_null {
    ($base_name: ident, $func_name: ident) => {
        some_aggregate_non_null!($base_name, $func_name, i8, i8);
        some_aggregate_non_null!($base_name, $func_name, i16, i16);
        some_aggregate_non_null!($base_name, $func_name, i32, i32);
        some_aggregate_non_null!($base_name, $func_name, i64, i64);
    };
}

// Macro to create variants of an aggregation function
// There must exist a function f__(left: T, right: T) -> T
// This creates 3 more functions
// f_N_(left: Option<T>, right: T) -> Option<T>
// etc.
// And 4 more functions:
// f_N_N_conditional(left: T, right: T, predicate: bool) -> T
macro_rules! universal_aggregate {
    ($func:ident) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func _N_ >]<T>( left: Option<T>, right: T ) -> Option<T>
                where T: Ord + Clone,
            {
                match left {
                    None => Some(right.clone()),
                    Some(left) => Some([<$func __>](left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func __N>]<T>( left: T, right: Option<T> ) -> Option<T>
                where T: Ord + Clone,
            {
                match right {
                    None => Some(left.clone()),
                    Some(right) => Some([<$func __>](left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N_N>]<T>( left: Option<T>, right: Option<T> ) -> Option<T>
                where T: Ord + Clone,
            {
                match (left.clone(), right.clone()) {
                    (None, _) => right.clone(),
                    (_, None) => left.clone(),
                    (Some(left), Some(right)) => Some([<$func __>](left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func ___conditional>]<T>( left: T, right: T, predicate: bool ) -> T
                where T: Ord + Clone,
            {
                if predicate {
                    [<$func __>](left, right)
                } else {
                    left.clone()
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N__conditional>]<T>( left: Option<T>, right: T, predicate: bool ) -> Option<T>
                where T: Ord + Clone,
            {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => left.clone(),
                    (None, _, _) => Some(right.clone()),
                    (Some(x), _, _) => Some([<$func __>](x, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func __N_conditional>]<T>( left: T, right: Option<T>, predicate: bool ) -> Option<T>
                where T: Ord + Clone,
            {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => Some(left.clone()),
                    (_, None, _) => Some(left.clone()),
                    (_, Some(y), _) => Some([<$func __>](left, y)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N_N_conditional>]<T>( left: Option<T>, right: Option<T>, predicate: bool ) -> Option<T>
                where T: Ord + Clone,
            {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => left.clone(),
                    (None, _, _) => right.clone(),
                    (_, None, _) => left.clone(),
                    (Some(x), Some(y), _) => Some([< $func __ >](x, y)),
                }
            }
        }
    };
}
pub(crate) use universal_aggregate;

#[doc(hidden)]
pub fn agg_max__<T>(left: T, right: T) -> T
where
    T: Ord + Clone,
{
    left.max(right)
}

universal_aggregate!(agg_max);

#[doc(hidden)]
pub fn agg_min__<T>(left: T, right: T) -> T
where
    T: Ord + Clone,
{
    left.min(right)
}

universal_aggregate!(agg_min);

#[doc(hidden)]
pub fn agg_plus<T>(left: T, right: T) -> T
where
    T: CheckedAdd + Copy,
{
    left.checked_add(&right).expect("Addition overflow")
}

#[doc(hidden)]
pub fn agg_plus_f32(left: F32, right: F32) -> F32 {
    left + right
}

#[doc(hidden)]
pub fn agg_plus_f64(left: F64, right: F64) -> F64 {
    left + right
}

#[doc(hidden)]
pub fn agg_plus_decimal(left: Decimal, right: Decimal) -> Decimal {
    left + right
}

#[doc(hidden)]
pub fn agg_plus_dec(left: Dec, right: Dec) -> Dec {
    left + right
}

some_aggregate!(agg_plus_f32, agg_plus, f, F32);
some_aggregate!(agg_plus_f64, agg_plus, d, F64);
some_aggregate!(agg_plus_decimal, agg_plus, decimal, Decimal);
some_aggregate!(agg_plus_dec, agg_plus, dec, Dec);

for_all_int_aggregate!(agg_plus, agg_plus);

#[doc(hidden)]
pub fn agg_plus_non_null<T>(left: T, right: T) -> T
where
    T: CheckedAdd + Copy,
{
    left.checked_add(&right).expect("Addition overflow")
}

#[doc(hidden)]
pub fn agg_plus_f32_non_null(left: F32, right: F32) -> F32 {
    left + right
}

#[doc(hidden)]
pub fn agg_plus_f64_non_null(left: F64, right: F64) -> F64 {
    left + right
}

#[doc(hidden)]
pub fn agg_plus_decimal_non_null(left: Decimal, right: Decimal) -> Decimal {
    left + right
}

some_aggregate_non_null!(agg_plus_f32_non_null, agg_plus_non_null, f, F32);
some_aggregate_non_null!(agg_plus_f64, agg_plus_non_null, d, F64);
some_aggregate_non_null!(
    agg_plus_decimal_non_null,
    agg_plus_non_null,
    decimal,
    Decimal
);

for_all_int_aggregate_non_null!(agg_plus_non_null, agg_plus_non_null);

#[doc(hidden)]
#[inline(always)]
fn agg_and<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left & right
}

for_all_int_aggregate!(agg_and, agg_and);

#[doc(hidden)]
#[inline(always)]
fn agg_or<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left | right
}

for_all_int_aggregate!(agg_or, agg_or);

#[doc(hidden)]
#[inline(always)]
fn agg_xor<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left ^ right
}

for_all_int_aggregate!(agg_xor, agg_xor);

// Helper function for XOR
#[doc(hidden)]
#[inline]
pub fn right_xor_weigh<T>(right: T, w: Weight) -> T
where
    T: PrimInt,
{
    if ((w as i64) % 2) == 0 {
        T::zero()
    } else {
        right
    }
}

#[doc(hidden)]
#[inline]
pub fn right_xor_weighN<T>(right: Option<T>, w: Weight) -> Option<T>
where
    T: PrimInt,
{
    let right = right?;
    Some(right_xor_weigh(right, w))
}

#[doc(hidden)]
pub fn agg_and_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.and(&right)
}

some_aggregate!(agg_and_bytes, agg_and, bytes, ByteArray);

#[doc(hidden)]
pub fn agg_or_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.or(&right)
}

some_aggregate!(agg_or_bytes, agg_or, bytes, ByteArray);

#[doc(hidden)]
pub fn agg_xor_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.xor(&right)
}

#[doc(hidden)]
#[inline]
pub fn right_xor_weigh_bytes(right: ByteArray, w: Weight) -> ByteArray {
    if ((w as i64) % 2) == 0 {
        ByteArray::zero(right.length())
    } else {
        right
    }
}

#[doc(hidden)]
#[inline]
pub fn right_xor_weigh_bytesN(right: Option<ByteArray>, w: Weight) -> Option<ByteArray> {
    let right = right?;
    Some(right_xor_weigh_bytes(right, w))
}

some_aggregate!(agg_xor_bytes, agg_xor, bytes, ByteArray);

// In this aggregate the left value is the current value
// while the right value is the accumulator
#[doc(hidden)]
pub fn agg_lte__<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left <= right
}

#[doc(hidden)]
pub fn agg_lte__N<T>(left: T, right: Option<T>) -> bool
where
    T: Ord,
{
    match right {
        None => true,
        Some(right) => left <= right,
    }
}

#[doc(hidden)]
pub fn agg_lte_N_<T>(left: Option<T>, right: T) -> bool
where
    T: Ord,
{
    match left {
        None => false,
        Some(left) => left <= right,
    }
}

#[doc(hidden)]
pub fn agg_lte_N_N<T>(left: Option<T>, right: Option<T>) -> bool
where
    T: Ord,
{
    match (left, right) {
        (None, None) => true,
        (None, _) => false,
        (_, None) => true,
        (Some(left), Some(right)) => left <= right,
    }
}

#[doc(hidden)]
pub fn agg_gte__<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left >= right
}

#[doc(hidden)]
pub fn agg_gte__N<T>(left: T, right: Option<T>) -> bool
where
    T: Ord,
{
    match right {
        None => true,
        Some(right) => left >= right,
    }
}

#[doc(hidden)]
pub fn agg_gte_N_<T>(left: Option<T>, right: T) -> bool
where
    T: Ord,
{
    match left {
        None => false,
        Some(left) => left >= right,
    }
}

#[doc(hidden)]
pub fn agg_gte_N_N<T>(left: Option<T>, right: Option<T>) -> bool
where
    T: Ord,
{
    match (left, right) {
        (None, None) => true,
        (None, _) => false,
        (_, None) => true,
        (Some(left), Some(right)) => left >= right,
    }
}

//////////////

#[doc(hidden)]
pub fn cf_compare_gte__<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left >= right
}

#[doc(hidden)]
pub fn cf_compare_gte__N<T>(left: T, right: Option<T>) -> bool
where
    T: Ord,
{
    match right {
        None => true,
        Some(right) => left >= right,
    }
}

#[doc(hidden)]
pub fn cf_compare_gte_N_<T>(left: Option<T>, right: T) -> bool
where
    T: Ord,
{
    match left {
        None => true,
        Some(left) => left >= right,
    }
}

#[doc(hidden)]
pub fn cf_compare_gte_N_N<T>(left: Option<T>, right: Option<T>) -> bool
where
    T: Ord,
{
    match (left, right) {
        (None, None) => true,
        (None, _) => true,
        (_, None) => true,
        (Some(left), Some(right)) => left >= right,
    }
}
