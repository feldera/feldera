#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use crate::binary::ByteArray;
use crate::timestamp::*;
use core::ops::Add;
use dbsp::algebra::{F32, F64};
use num::PrimInt;
use rust_decimal::Decimal;
use std::cmp::Ord;
use std::marker::Copy;

// Macro to create variants of an aggregation function
// There must exist a function g(left: T, right: T) -> T ($base_name = g)
// This creates 4 more functions ($func_name = f)
// f_t_t(left: T, right: T) -> T
// f_tN_t(left: Option<T>, right: T) -> Option<T>
// etc.
// And 4 more functions:
// f_t_t_conditional(left: T, right: T, predicate: bool) -> T
#[macro_export]
macro_rules! some_aggregate {
    ($base_name: ident, $func_name:ident, $short_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            pub fn [<$func_name _ $short_name _ $short_name>]( left: $arg_type, right: $arg_type ) -> $arg_type {
                $base_name(left, right)
            }

            pub fn [<$func_name _ $short_name N _ $short_name>]( left: Option<$arg_type>, right: $arg_type ) -> Option<$arg_type> {
                match left {
                    None => Some(right.clone()),
                    Some(left) => Some($base_name(left, right)),
                }
            }

            pub fn [<$func_name _ $short_name _ $short_name N>]( left: $arg_type, right: Option<$arg_type> ) -> Option<$arg_type> {
                match right {
                    None => Some(left.clone()),
                    Some(right) => Some($base_name(left, right)),
                }
            }

            pub fn [<$func_name _ $short_name N _ $short_name N>]( left: Option<$arg_type>, right: Option<$arg_type> ) -> Option<$arg_type> {
                match (left.clone(), right.clone()) {
                    (None, _) => right.clone(),
                    (_, None) => left.clone(),
                    (Some(left), Some(right)) => Some($base_name(left, right)),
                }
            }

            pub fn [<$func_name _ $short_name _ $short_name _conditional>]( left: $arg_type, right: $arg_type, predicate: bool ) -> $arg_type {
                if predicate {
                    $base_name(left, right)
                } else {
                    left.clone()
                }
            }

            pub fn [<$func_name _ $short_name N _ $short_name _conditional>]( left: Option<$arg_type>, right: $arg_type, predicate: bool ) -> Option<$arg_type> {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => left.clone(),
                    (None, _, _) => Some(right.clone()),
                    (Some(x), _, _) => Some($base_name(x, right)),
                }
            }

            pub fn [<$func_name _ $short_name _ $short_name N _conditional>]( left: $arg_type, right: Option<$arg_type>, predicate: bool ) -> Option<$arg_type> {
                match (left.clone(), right.clone(), predicate) {
                    (_, _, false) => Some(left.clone()),
                    (_, None, _) => Some(left.clone()),
                    (_, Some(y), _) => Some($base_name(left, y)),
                }
            }

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

#[macro_export]
macro_rules! for_all_int_aggregate {
    ($base_name: ident, $func_name: ident) => {
        some_aggregate!($base_name, $func_name, i8, i8);
        some_aggregate!($base_name, $func_name, i16, i16);
        some_aggregate!($base_name, $func_name, i32, i32);
        some_aggregate!($base_name, $func_name, i64, i64);
    };
}

#[macro_export]
macro_rules! for_all_numeric_aggregate {
    ($base_name: ident, $func_name: ident) => {
        for_all_int_aggregate!($base_name, $func_name);
        some_aggregate!($base_name, $func_name, f, F32);
        some_aggregate!($base_name, $func_name, d, F64);
        some_aggregate!($base_name, $func_name, decimal, Decimal);
    };
}

pub fn agg_max<T>(left: T, right: T) -> T
where
    T: Ord,
{
    left.max(right)
}

for_all_numeric_aggregate!(agg_max, agg_max);
some_aggregate!(agg_max, agg_max, b, bool);
some_aggregate!(agg_max, agg_max, Timestamp, Timestamp);
some_aggregate!(agg_max, agg_max, Date, Date);
some_aggregate!(agg_max, agg_max, Time, Time);
some_aggregate!(agg_max, agg_max, s, String);

pub fn agg_min<T>(left: T, right: T) -> T
where
    T: Ord,
{
    left.min(right)
}

for_all_numeric_aggregate!(agg_min, agg_min);
some_aggregate!(agg_min, agg_min, b, bool);
some_aggregate!(agg_min, agg_min, Timestamp, Timestamp);
some_aggregate!(agg_min, agg_min, Date, Date);
some_aggregate!(agg_min, agg_min, Time, Time);
some_aggregate!(agg_min, agg_min, s, String);

pub fn agg_plus<T>(left: T, right: T) -> T
where
    T: Add<T, Output = T> + Copy,
{
    left + right
}

for_all_numeric_aggregate!(agg_plus, agg_plus);

#[inline(always)]
fn agg_and<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left & right
}

for_all_int_aggregate!(agg_and, agg_and);

#[inline(always)]
fn agg_or<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left | right
}

for_all_int_aggregate!(agg_or, agg_or);

#[inline(always)]
fn agg_xor<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left ^ right
}

for_all_int_aggregate!(agg_xor, agg_xor);

pub fn agg_and_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.and(&right)
}

some_aggregate!(agg_and_bytes, agg_and, bytes, ByteArray);

pub fn agg_or_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.or(&right)
}

some_aggregate!(agg_or_bytes, agg_or, bytes, ByteArray);

pub fn agg_xor_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.xor(&right)
}

some_aggregate!(agg_xor_bytes, agg_xor, bytes, ByteArray);
