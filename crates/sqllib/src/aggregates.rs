#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(clippy::let_and_return)]
#![allow(clippy::unnecessary_cast)]

use crate::{ByteArray, FromInteger, SqlDecimal, ToInteger, Weight};
use dbsp::algebra::{F32, F64, FirstLargeValue, HasOne, HasZero, SignedPrimInt, UnsignedPrimInt};
use num::PrimInt;
use num_traits::CheckedAdd;
use std::cmp::Ord;
use std::fmt::{Debug, Display};
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
        <U as TryFrom<I>>::Error: Debug,
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
        <U as TryFrom<I>>::Error: Debug,
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
        <I as TryFrom<U>>::Error: Debug,
        <S as TryFrom<I>>::Error: Debug,
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
        <I as TryFrom<U>>::Error: Debug,
        <S as TryFrom<I>>::Error: Debug,
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
    ($base_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $func_name:ident, $short_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name>] $(< $( const $var : $ty ),* >)? ( left: $arg_type, right: $arg_type ) -> $arg_type {
                $base_name(left, right)
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name>] $(< $( const $var : $ty ),* >)? ( left: Option<$arg_type>, right: $arg_type ) -> Option<$arg_type> {
                match left {
                    None => Some(right.clone()),
                    Some(left) => Some($base_name(left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N>] $(< $( const $var : $ty ),* >)? ( left: $arg_type, right: Option<$arg_type> ) -> Option<$arg_type> {
                match right {
                    None => Some(left.clone()),
                    Some(right) => Some($base_name(left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name N>] $(< $( const $var : $ty ),* >)? ( left: Option<$arg_type>, right: Option<$arg_type> ) -> Option<$arg_type> {
                match (left.clone(), right.clone()) {
                    (None, _) => right.clone(),
                    (_, None) => left.clone(),
                    (Some(left), Some(right)) => Some($base_name(left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name _conditional>] $(< $( const $var : $ty ),* >)? ( left: $arg_type, right: $arg_type, predicate: bool ) -> $arg_type {
                if predicate {
                    $base_name(left, right)
                } else {
                    left.clone()
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name _conditional>] $(< $( const $var : $ty ),* >)? ( left: Option<$arg_type>, right: $arg_type, predicate: bool ) -> Option<$arg_type> {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => left.clone(),
                    (None, _, _) => Some(right.clone()),
                    (Some(x), _, _) => Some($base_name(x, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N _conditional>] $(< $( const $var : $ty ),* >)? ( left: $arg_type, right: Option<$arg_type>, predicate: bool ) -> Option<$arg_type> {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => Some(left.clone()),
                    (_, None, _) => Some(left.clone()),
                    (_, Some(y), _) => Some($base_name(left, y)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name N _conditional>] $(< $( const $var : $ty ),* >)? ( left: Option<$arg_type>, right: Option<$arg_type>, predicate: bool ) -> Option<$arg_type> {
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
    ($base_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $func_name:ident, $short_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name>] $(< $( const $var : $ty ),* >)? ( left: $arg_type, right: $arg_type ) -> $arg_type {
                $base_name(left, right)
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N>] $(< $( const $var : $ty ),* >)? ( left: $arg_type, right: Option<$arg_type> ) -> $arg_type {
                match right {
                    None => left.clone(),
                    Some(right) => $base_name(left, right),
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name _conditional>] $(< $( const $var : $ty ),* >)? ( left: $arg_type, right: $arg_type, predicate: bool ) -> $arg_type {
                if predicate {
                    $base_name(left, right)
                } else {
                    left.clone()
                }
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N _conditional>] $(< $( const $var : $ty ),* >)? ( left: $arg_type, right: Option<$arg_type>, predicate: bool ) -> $arg_type {
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
        some_aggregate!($base_name, $func_name, i128, i128);
    };
}

macro_rules! for_all_int_aggregate_non_null {
    ($base_name: ident, $func_name: ident) => {
        some_aggregate_non_null!($base_name, $func_name, i8, i8);
        some_aggregate_non_null!($base_name, $func_name, i16, i16);
        some_aggregate_non_null!($base_name, $func_name, i32, i32);
        some_aggregate_non_null!($base_name, $func_name, i64, i64);
        some_aggregate_non_null!($base_name, $func_name, i128, i128);
    };
}

// Macro to create variants of an aggregation function
// There must exist a function f__(left: T, right: T) -> T
// This creates 3 more functions
// f_N_<T>(left: Option<T>, right: T) -> Option<T>
// etc.
// And 4 more functions:
// f_N_N_conditional<T>(left: T, right: T, predicate: bool) -> T
macro_rules! universal_aggregate {
    ($func:ident, $t: ty where $($bounds:tt)*) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func _N_ >]<$t>( left: Option<$t>, right: $t ) -> Option<$t>
                where $($bounds)*
            {
                match left {
                    None => Some(right.clone()),
                    Some(left) => Some([<$func __>](left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func __N>]<$t>( left: $t, right: Option<$t> ) -> Option<$t>
                where $($bounds)*
            {
                match right {
                    None => Some(left.clone()),
                    Some(right) => Some([<$func __>](left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N_N>]<$t>( left: Option<$t>, right: Option<$t> ) -> Option<$t>
                where $($bounds)*
            {
                match (left.clone(), right.clone()) {
                    (None, right) => right,
                    (left, None) => left,
                    (Some(left), Some(right)) => Some([<$func __>](left, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func ___conditional>]<$t>( left: $t, right: $t, predicate: bool ) -> $t
                where $($bounds)*
            {
                if predicate {
                    [<$func __>](left, right)
                } else {
                    left.clone()
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N__conditional>]<$t>( left: Option<$t>, right: $t, predicate: bool ) -> Option<$t>
                where $($bounds)*
            {
                match (left.clone(), right.clone(), predicate) {
                    (left, _, false) => left,
                    (None, right, _) => Some(right),
                    (Some(x), _, _) => Some([<$func __>](x, right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func __N_conditional>]<$t>( left: $t, right: Option<$t>, predicate: bool ) -> Option<$t>
                where $($bounds)*
            {
                match (left.clone(), right.clone(), predicate) {
                    (left, _, false) => Some(left),
                    (left, None, _) => Some(left),
                    (_, Some(y), _) => Some([<$func __>](left, y)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N_N_conditional>]<$t>( left: Option<$t>, right: Option<$t>, predicate: bool ) -> Option<$t>
                where $($bounds)*
            {
                match (left.clone(), right.clone(), predicate) {
                    (left, _, false) => left,
                    (None, right, _) => right,
                    (left, None, _) => left,
                    (Some(x), Some(y), _) => Some([< $func __ >](x, y)),
                }
            }
        }
    };
}
pub(crate) use universal_aggregate;

#[doc(hidden)]
pub fn agg_min__<T>(left: T, right: T) -> T
where
    T: Ord + Clone + Debug,
{
    left.min(right)
}

universal_aggregate!(agg_min, T where T: Ord + Clone + Debug);

#[doc(hidden)]
pub fn agg_max__<T>(left: T, right: T) -> T
where
    T: Ord + Clone + Debug,
{
    left.max(right)
}

universal_aggregate!(agg_max, T where T: Ord + Clone + Debug);

fn o0<L, R>(t: (L, R)) -> (Option<L>, R) {
    (Some(t.0), t.1)
}

// Macro to create variants of an aggregation function
// There must exist a function f__(left: (L, R), right: (L, R)) -> (L, R)
// This creates 3 more functions
// f_N_<L, R>(left: (<Option<L>, R), right: (L, R)) -> (Option<L>, R)
// etc.
// And 4 more functions:
// f_N_N_conditional<L, R>(left: (L, R), right: (L, R), predicate: bool) -> (L, R)
macro_rules! universal_aggregate2 {
    ($func:ident, $l: ty, $r: ty where $($bounds:tt)*) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func _N_ >]<$l, $r>( left: (Option<$l>, $r), right: ($l, $r)) -> (Option<$l>, $r)
                where $($bounds)*
            {
                match left {
                    (None, _) => o0(right),
                    (Some(left), r) => o0([<$func __>]((left, r), right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func __N>]<$l, $r>( left: ($l, $r), right: (Option<$l>, $r) ) -> (Option<$l>, $r)
                where $($bounds)*
            {
                match right {
                    (None, _) => o0(left),
                    (Some(right), r) => o0([<$func __>](left, (right, r))),
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N_N>]<$l, $r>( left: (Option<$l>, $r), right: (Option<$l>, $r) ) -> (Option<$l>, $r)
                where $($bounds)*
            {
                match (left.clone(), right.clone()) {
                    ((None, _), right) => right,
                    (left, (None, _)) => left,
                    ((Some(left), l1), (Some(right), r1)) => o0([<$func __>]((left, l1), (right, r1))),
                }
            }

            #[doc(hidden)]
            pub fn [<$func ___conditional>]<$l, $r>( left: ($l, $r), right: ($l, $r), predicate: bool ) -> ($l, $r)
                where $($bounds)*
            {
                if predicate {
                    [<$func __>](left, right)
                } else {
                    left.clone()
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N__conditional>]<$l, $r>( left: (Option<$l>, $r), right: ($l, $r), predicate: bool ) -> (Option<$l>, $r)
                where $($bounds)*
            {
                match (left.clone(), right.clone(), predicate) {
                    (left, _, false) => left,
                    ((None, _), right, _) => o0(right),
                    ((Some(x), r), _, _) => o0([<$func __>]((x, r), right)),
                }
            }

            #[doc(hidden)]
            pub fn [<$func __N_conditional>]<$l, $r>( left: ($l, $r), right: (Option<$l>, $r), predicate: bool ) -> (Option<$l>, $r)
                where $($bounds)*
            {
                match (left.clone(), right.clone(), predicate) {
                    (left, _, false) => o0(left),
                    (left, (None, _), _) => o0(left),
                    (_, (Some(y), r), _) => o0([<$func __>](left, (y, r))),
                }
            }

            #[doc(hidden)]
            pub fn [<$func _N_N_conditional>]<$l, $r>( left: (Option<$l>, $r), right: (Option<$l>, $r), predicate: bool ) -> (Option<$l>, $r)
                where $($bounds)*
            {
                match (left.clone(), right.clone(), predicate) {
                    (left, _, false) => left,
                    ((None, _), right, _) => right,
                    (left, (None, _), _) => left,
                    ((Some(x), l), (Some(y), r), _) => o0([< $func __ >]((x, l), (y, r))),
                }
            }
        }
    };
}
pub(crate) use universal_aggregate2;

#[doc(hidden)]
pub fn agg_max1__<L, R>(left: (L, R), right: (L, R)) -> (L, R)
where
    L: Ord + Clone + Debug,
    R: Ord + Clone + Debug,
{
    left.max(right)
}

universal_aggregate2!(agg_max1, L, R where L: Ord + Clone + Debug, R: Ord + Clone + Debug);

#[doc(hidden)]
pub fn agg_min1__<L, R>(left: (L, R), right: (L, R)) -> (L, R)
where
    L: Ord + Clone + Debug,
    R: Ord + Clone + Debug,
{
    left.min(right)
}

universal_aggregate2!(agg_min1, L, R where L: Ord + Clone + Debug, R: Ord + Clone + Debug);

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

some_aggregate!(agg_plus_f32, agg_plus, f, F32);
some_aggregate!(agg_plus_f64, agg_plus, d, F64);
for_all_int_aggregate!(agg_plus, agg_plus);

#[doc(hidden)]
pub fn agg_plus_SqlDecimal<const P: usize, const S: usize>(
    left: SqlDecimal<P, S>,
    right: SqlDecimal<P, S>,
) -> SqlDecimal<P, S> {
    left.checked_add(&right).unwrap_or_else(|| {
        panic!(
            "Overflow during aggregation {}+{} cannot be represented as DECIMAL({P}, {S})",
            left, right
        )
    })
}

some_aggregate!(agg_plus_SqlDecimal<const P: usize, const S: usize>, agg_plus, SqlDecimal, SqlDecimal<P, S>);

#[doc(hidden)]
pub fn agg_plus_non_null<T>(left: T, right: T) -> T
where
    T: CheckedAdd + Copy + Display,
{
    left.checked_add(&right)
        .unwrap_or_else(|| panic!("Overflow during aggregation {}+{}", left, right))
}

#[doc(hidden)]
pub fn agg_plus_f32_non_null(left: F32, right: F32) -> F32 {
    left + right
}

#[doc(hidden)]
pub fn agg_plus_f64_non_null(left: F64, right: F64) -> F64 {
    left + right
}

some_aggregate_non_null!(agg_plus_f32_non_null, agg_plus_non_null, f, F32);
some_aggregate_non_null!(agg_plus_f64_non_null, agg_plus_non_null, d, F64);
for_all_int_aggregate_non_null!(agg_plus_non_null, agg_plus_non_null);

#[doc(hidden)]
pub fn agg_plus_SqlDecimal_non_null<const P: usize, const S: usize>(
    left: SqlDecimal<P, S>,
    right: SqlDecimal<P, S>,
) -> SqlDecimal<P, S> {
    left.checked_add(&right).unwrap_or_else(|| {
        panic!(
            "Overflow during aggregation {}+{} cannot be represented as DECIMAL({P}, {S})",
            left, right
        )
    })
}

some_aggregate_non_null!(agg_plus_SqlDecimal_non_null<const P: usize, const S: usize>, agg_plus_non_null, SqlDecimal, SqlDecimal<P, S>);

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

// ===== PERCENTILE_CONT / PERCENTILE_DISC =====
//
// These functions implement SQL PERCENTILE_CONT and PERCENTILE_DISC aggregate functions
// using an order-statistics multiset (OrderStatisticTree) that supports:
// - O(log n) insertion with positive/negative weights for incremental computation
// - O(log n) deletion via negative weights
// - O(n) k-th element selection
// - Proper multiset semantics

use crate::order_statistic_tree::OrderStatisticTree;

/// Collect values for percentile computation (non-nullable values)
///
/// Inserts the value into the OrderStatisticTree with the given weight.
/// Positive weights add occurrences, negative weights remove them.
#[doc(hidden)]
pub fn percentile_collect<T>(
    accumulator: &mut OrderStatisticTree<T>,
    value: T,
    weight: Weight,
    predicate: bool,
) where
    T: Ord + Clone,
{
    if !predicate {
        return;
    }
    accumulator.insert(value, weight);
}

/// Collect values for percentile computation (nullable values)
///
/// Ignores NULL values. Non-NULL values are inserted into the tree.
#[doc(hidden)]
pub fn percentile_collectN<T>(
    accumulator: &mut OrderStatisticTree<T>,
    value: Option<T>,
    weight: Weight,
    predicate: bool,
) where
    T: Ord + Clone,
{
    if !predicate {
        return;
    }
    // Ignore NULL values in percentile computation
    if let Some(v) = value {
        accumulator.insert(v, weight);
    }
}

/// Compute continuous percentile (with interpolation) for non-nullable result
///
/// Uses the OrderStatisticTree to efficiently compute the percentile.
/// For PERCENTILE_CONT, we interpolate between adjacent values when the
/// position falls between two elements.
#[doc(hidden)]
pub fn percentile_cont<T>(
    tree: &OrderStatisticTree<T>,
    percentile: f64,
    ascending: bool,
) -> Option<T>
where
    T: Clone + Debug + Ord,
{
    // Get the bounds for interpolation
    let (lower, upper, fraction) = tree.select_percentile_bounds(percentile, ascending)?;

    if fraction == 0.0 || lower == upper {
        return Some(lower.clone());
    }

    // For generic T, we cannot interpolate numerically.
    // Return the lower value. Type-specific implementations can provide interpolation.
    // Note: The SQL standard defines PERCENTILE_CONT for numeric types only,
    // so in practice T would be a numeric type where interpolation is possible.
    Some(lower.clone())
}

/// Compute continuous percentile (with interpolation) for nullable result
#[doc(hidden)]
pub fn percentile_contN<T>(
    tree: &OrderStatisticTree<T>,
    percentile: f64,
    ascending: bool,
) -> Option<T>
where
    T: Clone + Debug + Ord,
{
    // The tree already contains only non-null values (NULL values are filtered in percentile_collectN)
    percentile_cont(tree, percentile, ascending)
}

/// Compute discrete percentile (nearest value) for non-nullable result
///
/// Returns an actual value from the dataset (no interpolation).
#[doc(hidden)]
pub fn percentile_disc<T>(
    tree: &OrderStatisticTree<T>,
    percentile: f64,
    ascending: bool,
) -> Option<T>
where
    T: Clone + Debug + Ord,
{
    tree.select_percentile_disc(percentile, ascending).cloned()
}

/// Compute discrete percentile (nearest value) for nullable result
#[doc(hidden)]
pub fn percentile_discN<T>(
    tree: &OrderStatisticTree<T>,
    percentile: f64,
    ascending: bool,
) -> Option<T>
where
    T: Clone + Debug + Ord,
{
    // The tree already contains only non-null values
    percentile_disc(tree, percentile, ascending)
}

// ===== Type-specific PERCENTILE_CONT implementations with proper interpolation =====

/// Compute continuous percentile with interpolation for f64 (DOUBLE type)
#[doc(hidden)]
pub fn percentile_cont_f64(
    tree: &OrderStatisticTree<crate::F64>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    let (lower, upper, fraction) = tree.select_percentile_bounds(percentile, ascending)?;

    if fraction == 0.0 || lower == upper {
        return Some(*lower);
    }

    // Linear interpolation: lower + fraction * (upper - lower)
    let lower_val = lower.into_inner();
    let upper_val = upper.into_inner();
    let result = lower_val + fraction * (upper_val - lower_val);
    Some(crate::F64::new(result))
}

/// Compute continuous percentile with interpolation for f64 (nullable)
#[doc(hidden)]
pub fn percentile_cont_f64N(
    tree: &OrderStatisticTree<crate::F64>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    percentile_cont_f64(tree, percentile, ascending)
}

/// Compute continuous percentile with interpolation for f32 (REAL type)
#[doc(hidden)]
pub fn percentile_cont_f32(
    tree: &OrderStatisticTree<crate::F32>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F32> {
    let (lower, upper, fraction) = tree.select_percentile_bounds(percentile, ascending)?;

    if fraction == 0.0 || lower == upper {
        return Some(*lower);
    }

    // Linear interpolation
    let lower_val = lower.into_inner() as f64;
    let upper_val = upper.into_inner() as f64;
    let result = lower_val + fraction * (upper_val - lower_val);
    Some(crate::F32::new(result as f32))
}

/// Compute continuous percentile with interpolation for f32 (nullable)
#[doc(hidden)]
pub fn percentile_cont_f32N(
    tree: &OrderStatisticTree<crate::F32>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F32> {
    percentile_cont_f32(tree, percentile, ascending)
}

/// Compute continuous percentile with interpolation for i64 (BIGINT type)
/// Returns f64 as per SQL standard (interpolated values may not be integers)
#[doc(hidden)]
pub fn percentile_cont_i64(
    tree: &OrderStatisticTree<i64>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    let (lower, upper, fraction) = tree.select_percentile_bounds(percentile, ascending)?;

    if fraction == 0.0 || lower == upper {
        return Some(crate::F64::new(*lower as f64));
    }

    // Linear interpolation returning double
    let lower_val = *lower as f64;
    let upper_val = *upper as f64;
    let result = lower_val + fraction * (upper_val - lower_val);
    Some(crate::F64::new(result))
}

/// Compute continuous percentile with interpolation for i64 (nullable)
#[doc(hidden)]
pub fn percentile_cont_i64N(
    tree: &OrderStatisticTree<i64>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    percentile_cont_i64(tree, percentile, ascending)
}

/// Compute continuous percentile with interpolation for i32 (INTEGER type)
/// Returns f64 as per SQL standard
#[doc(hidden)]
pub fn percentile_cont_i32(
    tree: &OrderStatisticTree<i32>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    let (lower, upper, fraction) = tree.select_percentile_bounds(percentile, ascending)?;

    if fraction == 0.0 || lower == upper {
        return Some(crate::F64::new(*lower as f64));
    }

    let lower_val = *lower as f64;
    let upper_val = *upper as f64;
    let result = lower_val + fraction * (upper_val - lower_val);
    Some(crate::F64::new(result))
}

/// Compute continuous percentile with interpolation for i32 (nullable)
#[doc(hidden)]
pub fn percentile_cont_i32N(
    tree: &OrderStatisticTree<i32>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    percentile_cont_i32(tree, percentile, ascending)
}

/// Compute continuous percentile with interpolation for i16 (SMALLINT type)
/// Returns f64 as per SQL standard
#[doc(hidden)]
pub fn percentile_cont_i16(
    tree: &OrderStatisticTree<i16>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    let (lower, upper, fraction) = tree.select_percentile_bounds(percentile, ascending)?;

    if fraction == 0.0 || lower == upper {
        return Some(crate::F64::new(*lower as f64));
    }

    let lower_val = *lower as f64;
    let upper_val = *upper as f64;
    let result = lower_val + fraction * (upper_val - lower_val);
    Some(crate::F64::new(result))
}

/// Compute continuous percentile with interpolation for i16 (nullable)
#[doc(hidden)]
pub fn percentile_cont_i16N(
    tree: &OrderStatisticTree<i16>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    percentile_cont_i16(tree, percentile, ascending)
}

/// Compute continuous percentile with interpolation for i8 (TINYINT type)
/// Returns f64 as per SQL standard
#[doc(hidden)]
pub fn percentile_cont_i8(
    tree: &OrderStatisticTree<i8>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    let (lower, upper, fraction) = tree.select_percentile_bounds(percentile, ascending)?;

    if fraction == 0.0 || lower == upper {
        return Some(crate::F64::new(*lower as f64));
    }

    let lower_val = *lower as f64;
    let upper_val = *upper as f64;
    let result = lower_val + fraction * (upper_val - lower_val);
    Some(crate::F64::new(result))
}

/// Compute continuous percentile with interpolation for i8 (nullable)
#[doc(hidden)]
pub fn percentile_cont_i8N(
    tree: &OrderStatisticTree<i8>,
    percentile: f64,
    ascending: bool,
) -> Option<crate::F64> {
    percentile_cont_i8(tree, percentile, ascending)
}

/// Compute continuous percentile with interpolation for numeric types (DECIMAL, etc.)
/// Uses f64 conversion for interpolation, returning the original type.
#[doc(hidden)]
pub fn percentile_cont_numeric<T>(
    tree: &OrderStatisticTree<T>,
    percentile: f64,
    ascending: bool,
) -> Option<T>
where
    T: Clone + Debug + Ord + Into<f64>,
    T: TryFrom<f64>,
{
    let (lower, upper, fraction) = tree.select_percentile_bounds(percentile, ascending)?;

    if fraction == 0.0 || lower == upper {
        return Some(lower.clone());
    }

    // Linear interpolation using f64
    let lower_f64: f64 = lower.clone().into();
    let upper_f64: f64 = upper.clone().into();
    let result_f64 = lower_f64 + fraction * (upper_f64 - lower_f64);

    // Convert back to the original type
    T::try_from(result_f64).ok()
}

/// Compute continuous percentile with interpolation for numeric types (nullable)
#[doc(hidden)]
pub fn percentile_cont_numericN<T>(
    tree: &OrderStatisticTree<T>,
    percentile: f64,
    ascending: bool,
) -> Option<T>
where
    T: Clone + Debug + Ord + Into<f64>,
    T: TryFrom<f64>,
{
    percentile_cont_numeric(tree, percentile, ascending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::F64;

    /// Test that mimics the generated code pattern for PERCENTILE_CONT
    /// This tests the increment function pattern used in combined aggregates
    #[test]
    fn test_increment_function_pattern() {
        // Mimics the generated code structure:
        // Accumulator type: Tup2<Option<percentile_value>, OrderStatisticTree<F64>>
        type Acc = (Option<f64>, OrderStatisticTree<F64>);

        // Zero/initial value
        let mut acc: Acc = (None, OrderStatisticTree::new());

        // Values to insert (from the failing test)
        let values: Vec<f64> = vec![-10.0, 0.0, 10.0, 20.0, 30.0, 100.0, 200.0];
        let percentile_arg = 0.0_f64; // For p0

        // Simulate calling the increment function for each row
        for val in &values {
            let value = Some(F64::new(*val));
            let weight: Weight = 1;
            let predicate = true;

            // This mimics the generated increment block:
            // percentile_collectN(&mut acc.1, value, weight, predicate);
            // acc = (newPercentile, acc.1.clone())
            percentile_collectN(&mut acc.1, value, weight, predicate);
            let new_percentile = if acc.0.is_none() {
                Some(percentile_arg)
            } else {
                acc.0
            };
            acc = (new_percentile, acc.1.clone());
        }

        // After processing all 7 values, the tree should have all of them
        assert_eq!(acc.1.total_count(), 7, "Tree should have 7 elements");

        // Check percentile computations
        let p0 = percentile_cont_f64(&acc.1, 0.0, true);
        let p100 = percentile_cont_f64(&acc.1, 1.0, true);

        println!("acc.1 = {:?}", acc.1);
        println!("p0 = {:?}", p0);
        println!("p100 = {:?}", p100);

        assert_eq!(p0, Some(F64::new(-10.0)), "p0 should be -10");
        assert_eq!(p100, Some(F64::new(200.0)), "p100 should be 200");
    }

    /// Test that mimics the exact generated code with Tup2
    /// The generated code has: (*p0).0 = { ... Tup2::new(..., (*p0).0.1.clone()) }
    /// where p0 is &mut Tup4<Tup2<...>, Tup2<...>, ...>
    #[test]
    fn test_fold_style_increment() {
        use crate::Tup2;

        // Exact type from generated code (one element of the Tup4)
        type Acc = Tup2<Option<f64>, OrderStatisticTree<F64>>;

        // Zero/initial value
        let mut acc: Acc = Tup2::new(None, OrderStatisticTree::new());

        // Values to insert
        let values: Vec<f64> = vec![-10.0, 0.0, 10.0, 20.0, 30.0, 100.0, 200.0];
        let percentile_arg = 0.0_f64;

        // Simulate calling the increment function for each row
        // This mimics: (*p0).0 = { ... }
        for val in &values {
            let p1_1 = Some(F64::new(*val)); // The value being collected
            let p2: Weight = 1; // Weight

            // This exactly mimics the generated code pattern:
            // acc = {
            //     percentile_collectN(&mut acc.1, value, p2, true);
            //     Tup2::new(
            //         if acc.0.is_none() { Some(percentile_arg) } else { acc.0.clone() },
            //         acc.1.clone()
            //     )
            // };
            acc = {
                percentile_collectN(&mut acc.1, p1_1.as_ref().cloned(), p2, true);
                Tup2::new(
                    if acc.0.as_ref().cloned().is_none() {
                        Some(percentile_arg)
                    } else {
                        acc.0.as_ref().cloned()
                    },
                    acc.1.clone(),
                )
            };
        }

        // Check results
        assert_eq!(acc.1.total_count(), 7, "Tree should have 7 elements");

        let p0 = percentile_cont_f64(&acc.1, 0.0, true);
        let p100 = percentile_cont_f64(&acc.1, 1.0, true);

        println!("Tree contents: {:?}", acc.1);
        println!("p0 = {:?}, p100 = {:?}", p0, p100);

        assert_eq!(p0, Some(F64::new(-10.0)), "p0 should be -10");
        assert_eq!(p100, Some(F64::new(200.0)), "p100 should be 200");
    }
}
