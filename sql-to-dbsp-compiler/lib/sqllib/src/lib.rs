#![allow(non_snake_case)]

pub mod aggregates;
pub mod binary;
pub mod casts;
pub mod geopoint;
pub mod interval;
pub mod operators;
pub mod string;
pub mod timestamp;

use crate::interval::ShortInterval;
use dbsp::algebra::{Semigroup, SemigroupValue, ZRingValue, F32, F64};
use dbsp::trace::{Batch, BatchReader, Builder, Cursor};
use dbsp::{DBData, DBWeight, OrdZSet};
use geopoint::GeoPoint;
use num::{Signed, ToPrimitive};
use rust_decimal::{Decimal, MathematicalOps};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Index;

#[derive(Clone)]
pub struct DefaultOptSemigroup<T>(PhantomData<T>);

// Macro to create variants of a function with 1 argument
// If there exists a function is f_(x: T) -> S, this creates a function
// fN(x: Option<T>) -> Option<S>, defined as
// fN(x) { let x = x?; Some(f_(x)) }.
#[macro_export]
macro_rules! some_function1 {
    ($func_name:ident, $arg_type:ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name N>]( arg: Option<$arg_type> ) -> Option<$ret_type> {
                let arg = arg?;
                Some([<$func_name _>](arg))
            }
        }
    };
}

// Macro to create variants of a function with 1 argument
// If there exists a function is f_type(x: T) -> S, this creates a function
// f_typeN(x: Option<T>) -> Option<S>, defined as
// f_typeN(x) { let x = x?; Some(f_type(x)) }.
#[macro_export]
macro_rules! some_polymorphic_function1 {
    ($func_name:ident, $type_name: ident, $arg_type:ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name _ $type_name N>]( arg: Option<$arg_type> ) -> Option<$ret_type> {
                let arg = arg?;
                Some([<$func_name _ $type_name >](arg))
            }
        }
    };
}

// Macro to create variants of a function with 2 arguments
// If there exists a function is f__(x: T, y: S) -> U, this creates
// three functions:
// - f_N(x: T, y: Option<S>) -> Option<U>
// - fN_(x: Option<T>, y: S) -> Option<U>
// - fNN(x: Option<T>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_function2 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name NN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name __>](arg0, arg1))
            }

            pub fn [<$func_name _N>]( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name __>](arg0, arg1))
            }

            pub fn [<$func_name N_>]( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name __>](arg0, arg1))
            }
        }
    }
}

// Macro to create variants of a polymorphic function with 2 arguments
// If there exists a function is f_type1_type2(x: T, y: S) -> U, this creates
// three functions:
// - f_type1_type2N(x: T, y: Option<S>) -> Option<U>
// - f_type1N_type2(x: Option<T>, y: S) -> Option<U>
// - f_type1N_type2N(x: Option<T>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_polymorphic_function2 {
    ($func_name:ident, $type_name0: ident, $arg_type0:ty, $type_name1: ident, $arg_type1:ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name _$type_name0 _ $type_name1 N>]( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1>](arg0, arg1))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1>]( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $type_name0 _ $type_name1>](arg0, arg1))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1 N>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1>](arg0, arg1))
            }
        }
    }
}

// Macro to create variants of a function with 3 arguments
// If there exists a function is f___(x: T, y: S, z: V) -> U, this creates
// seven functions:
// - f__N(x: T, y: S, z: Option<V>) -> Option<U>
// - f_N_(x: T, y: Option<S>, z: V) -> Option<U>
// - etc.
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_function3 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2: ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name __N>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _N_>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _NN>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name N__>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name N_N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name NN_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name NNN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }
        }
    }
}

// Macro to create variants of a function with 4 arguments
// If there exists a function is f____(x: T, y: S, z: V, w: W) -> U, this
// creates fifteen functions:
// - f___N(x: T, y: S, z: V, w: Option<W>) -> Option<U>
// - f__N_(x: T, y: S, z: Option<V>, w: W) -> Option<U>
// - etc.
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_function4 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2: ty, $arg_type3: ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name ___N>]( arg0: $arg_type0, arg1: $arg_type1, arg2: $arg_type2, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name __N_>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name __NN>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name _N__>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name _N_N>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name _NN_>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name _NNN>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name N___>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name N__N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name N_N_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name N_NN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name NN__>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name NN_N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name NNN_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name NNNN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }
        }
    }
}

// Macro to create variants of a function with 2 arguments
// optimized for the implementation of arithmetic operators.
// Assuming there exists a function is f__(x: T, y: T) -> U, this creates
// three functions:
// - f_tN_t(x: T, y: Option<T>) -> Option<U>
// - f_t_tN(x: Option<T>, y: T) -> Option<U>
// - f_tN_tN(x: Option<T>, y: Option<T>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_existing_operator {
    ($func_name: ident, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[inline(always)]
            pub fn [<$func_name _ $short_name N _ $short_name N>]( arg0: Option<$arg_type>, arg1: Option<$arg_type> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $short_name _ $short_name>](arg0, arg1))
            }

            #[inline(always)]
            pub fn [<$func_name _ $short_name _ $short_name N>]( arg0: $arg_type, arg1: Option<$arg_type> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $short_name _ $short_name>](arg0, arg1))
            }

            #[inline(always)]
            pub fn [<$func_name _ $short_name N _ $short_name>]( arg0: Option<$arg_type>, arg1: $arg_type ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $short_name _ $short_name>](arg0, arg1))
            }
        }
    }
}

// Macro to create variants of a function with 2 arguments
// optimized for the implementation of arithmetic operators.
// Assuming there exists a function is f(x: T, y: T) -> U, this creates
// four functions:
// - f_t_t(x: T, y: T) -> U
// - f_tN_t(x: T, y: Option<T>) -> Option<U>
// - f_t_tN(x: Option<T>, y: T) -> Option<U>
// - f_tN_tN(x: Option<T>, y: Option<T>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_operator {
    ($func_name: ident, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[inline(always)]
            pub fn [<$func_name _ $short_name _ $short_name >]( arg0: $arg_type, arg1: $arg_type ) -> $ret_type {
                $func_name(arg0, arg1)
            }

            some_existing_operator!($func_name, $short_name, $arg_type, $ret_type);
        }
    }
}

#[macro_export]
macro_rules! for_all_int_compare {
    ($func_name: ident, $ret_type: ty) => {
        some_operator!($func_name, i8, i8, bool);
        some_operator!($func_name, i16, i16, bool);
        some_operator!($func_name, i32, i32, bool);
        some_operator!($func_name, i64, i64, bool);
    };
}

#[macro_export]
macro_rules! for_all_numeric_compare {
    ($func_name: ident, $ret_type: ty) => {
        for_all_int_compare!($func_name, bool);
        some_operator!($func_name, f, F32, bool);
        some_operator!($func_name, d, F64, bool);
        some_operator!($func_name, decimal, Decimal, bool);
    };
}

#[macro_export]
macro_rules! for_all_compare {
    ($func_name: ident, $ret_type: ty) => {
        for_all_numeric_compare!($func_name, bool);
        some_operator!($func_name, b, bool, bool);
        some_operator!($func_name, s, String, bool);
    };
}

#[macro_export]
macro_rules! for_all_int_operator {
    ($func_name: ident) => {
        some_operator!($func_name, i8, i8, i8);
        some_operator!($func_name, i16, i16, i16);
        some_operator!($func_name, i32, i32, i32);
        some_operator!($func_name, i64, i64, i64);
    };
}

#[macro_export]
macro_rules! for_all_numeric_operator {
    ($func_name: ident) => {
        for_all_int_operator!($func_name);
        some_operator!($func_name, f, F32, F32);
        some_operator!($func_name, d, F64, F64);
        some_operator!($func_name, decimal, Decimal, Decimal);
    };
}

impl<T> Semigroup<Option<T>> for DefaultOptSemigroup<T>
where
    T: SemigroupValue,
{
    fn combine(left: &Option<T>, right: &Option<T>) -> Option<T> {
        match (left, right) {
            (None, _) => None,
            (_, None) => None,
            (Some(x), Some(y)) => Some(x.add_by_ref(y)),
        }
    }
}

#[derive(Clone)]
pub struct PairSemigroup<T, R, TS, RS>(PhantomData<(T, R, TS, RS)>);

impl<T, R, TS, RS> Semigroup<(T, R)> for PairSemigroup<T, R, TS, RS>
where
    TS: Semigroup<T>,
    RS: Semigroup<R>,
{
    fn combine(left: &(T, R), right: &(T, R)) -> (T, R) {
        (
            TS::combine(&left.0, &right.0),
            RS::combine(&left.1, &right.1),
        )
    }
}

#[inline(always)]
pub fn wrap_bool(b: Option<bool>) -> bool {
    match b {
        Some(x) => x,
        _ => false,
    }
}

#[inline(always)]
pub fn or_b_b(left: bool, right: bool) -> bool {
    left || right
}

#[inline(always)]
pub fn or_bN_b(left: Option<bool>, right: bool) -> Option<bool> {
    match (left, right) {
        (Some(l), r) => Some(l || r),
        (_, true) => Some(true),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn or_b_bN(left: bool, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (l, Some(r)) => Some(l || r),
        (true, _) => Some(true),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn or_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(l), Some(r)) => Some(l || r),
        (Some(true), _) => Some(true),
        (_, Some(true)) => Some(true),
        (_, _) => None::<bool>,
    }
}

// OR and AND are special, they can't be generated by rules

#[inline(always)]
pub fn and_b_b(left: bool, right: bool) -> bool {
    left && right
}

#[inline(always)]
pub fn and_bN_b(left: Option<bool>, right: bool) -> Option<bool> {
    match (left, right) {
        (Some(l), r) => Some(l && r),
        (_, false) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn and_b_bN(left: bool, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (l, Some(r)) => Some(l && r),
        (false, _) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn and_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(l), Some(r)) => Some(l && r),
        (Some(false), _) => Some(false),
        (_, Some(false)) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn is_null<T>(value: Option<T>) -> bool {
    value.is_none()
}

#[inline(always)]
pub fn indicator<T>(value: Option<T>) -> i64 {
    match value {
        None => 0,
        Some(_) => 1,
    }
}

//////////////////////////////////////////

#[inline(always)]
pub fn abs_i8(left: i8) -> i8 {
    left.abs()
}

#[inline(always)]
pub fn abs_i16(left: i16) -> i16 {
    left.abs()
}

#[inline(always)]
pub fn abs_i32(left: i32) -> i32 {
    left.abs()
}

#[inline(always)]
pub fn abs_i64(left: i64) -> i64 {
    left.abs()
}

#[inline(always)]
pub fn abs_f(left: F32) -> F32 {
    left.abs()
}

#[inline(always)]
pub fn abs_d(left: F64) -> F64 {
    left.abs()
}

#[inline(always)]
pub fn abs_decimal(left: Decimal) -> Decimal {
    left.abs()
}

some_polymorphic_function1!(abs, i8, i8, i8);
some_polymorphic_function1!(abs, i16, i16, i16);
some_polymorphic_function1!(abs, i32, i32, i32);
some_polymorphic_function1!(abs, i64, i64, i64);
some_polymorphic_function1!(abs, f, F32, F32);
some_polymorphic_function1!(abs, d, F64, F64);
some_polymorphic_function1!(abs, decimal, Decimal, Decimal);

#[inline(always)]
pub fn ln_decimal(left: Decimal) -> F64 {
    F64::new(left.ln().to_f64().unwrap())
}

some_polymorphic_function1!(ln, decimal, Decimal, F64);

#[inline(always)]
pub fn log10_decimal(left: Decimal) -> F64 {
    F64::new(left.log10().to_f64().unwrap())
}

some_polymorphic_function1!(log10, decimal, Decimal, F64);

#[inline(always)]
pub fn is_true_b_(left: bool) -> bool {
    left
}

#[inline(always)]
pub fn is_true_bN_(left: Option<bool>) -> bool {
    left == Some(true)
}

#[inline(always)]
pub fn is_false_b_(left: bool) -> bool {
    !left
}

#[inline(always)]
pub fn is_false_bN_(left: Option<bool>) -> bool {
    left == Some(false)
}

#[inline(always)]
pub fn is_not_true_b_(left: bool) -> bool {
    !left
}

#[inline(always)]
pub fn is_not_true_bN_(left: Option<bool>) -> bool {
    match left {
        Some(true) => false,
        Some(false) => true,
        _ => true,
    }
}

#[inline(always)]
pub fn is_not_false_b_(left: bool) -> bool {
    left
}

#[inline(always)]
pub fn is_not_false_bN_(left: Option<bool>) -> bool {
    match left {
        Some(true) => true,
        Some(false) => false,
        _ => true,
    }
}

#[inline(always)]
pub fn is_distinct__<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    left != right
}

#[inline(always)]
pub fn is_distinct_N_N<T>(left: Option<T>, right: Option<T>) -> bool
where
    T: Eq,
{
    match (left, right) {
        (Some(a), Some(b)) => a != b,
        (None, None) => false,
        _ => true,
    }
}

#[inline(always)]
pub fn is_distinct__N<T>(left: T, right: Option<T>) -> bool
where
    T: Eq,
{
    match right {
        Some(b) => left != b,
        None => true,
    }
}

#[inline(always)]
pub fn is_distinct_N_<T>(left: Option<T>, right: T) -> bool
where
    T: Eq,
{
    match left {
        Some(a) => a != right,
        None => true,
    }
}

pub fn weighted_push<T, W>(vec: &mut Vec<T>, value: &T, weight: W)
where
    W: ZRingValue,
    T: Clone,
{
    let mut w = weight;
    let negone = W::one().neg();
    while w != W::zero() {
        vec.push(value.clone());
        w = w.add_by_ref(&negone);
    }
}

pub fn st_distance_geopoint_geopoint(left: GeoPoint, right: GeoPoint) -> F64 {
    left.distance(&right)
}

some_polymorphic_function2!(st_distance, geopoint, GeoPoint, geopoint, GeoPoint, F64);

pub fn times_ShortInterval_i64(left: ShortInterval, right: i64) -> ShortInterval {
    left * right
}

some_polymorphic_function2!(times, ShortInterval, ShortInterval, i64, i64, ShortInterval);

pub fn times_i64_ShortInterval(left: i64, right: ShortInterval) -> ShortInterval {
    right * left
}

some_polymorphic_function2!(times, i64, i64, ShortInterval, ShortInterval, ShortInterval);

pub fn times_ShortInterval_i32(left: ShortInterval, right: i32) -> ShortInterval {
    left * (right as i64)
}

some_polymorphic_function2!(times, ShortInterval, ShortInterval, i32, i32, ShortInterval);

pub fn times_i32_ShortInterval(left: i32, right: ShortInterval) -> ShortInterval {
    right * (left as i64)
}

some_polymorphic_function2!(times, i32, i32, ShortInterval, ShortInterval, ShortInterval);

/***** decimals ***** */

#[inline(always)]
pub fn round_decimal<T>(left: Decimal, right: T) -> Decimal
where
    u32: TryFrom<T>,
    <u32 as TryFrom<T>>::Error: Debug,
{
    left.round_dp(u32::try_from(right).unwrap())
}

#[inline(always)]
pub fn round_decimalN<T>(left: Option<Decimal>, right: T) -> Option<Decimal>
where
    u32: TryFrom<T>,
    <u32 as TryFrom<T>>::Error: Debug,
{
    left.map(|x| round_decimal(x, right))
}

#[inline(always)]
pub fn truncate_decimal<T>(left: Decimal, right: T) -> Decimal
where
    u32: TryFrom<T>,
    <u32 as TryFrom<T>>::Error: Debug,
{
    left.trunc_with_scale(u32::try_from(right).unwrap())
}

#[inline(always)]
pub fn truncate_decimalN<T>(left: Option<Decimal>, right: T) -> Option<Decimal>
where
    u32: TryFrom<T>,
    <u32 as TryFrom<T>>::Error: Debug,
{
    left.map(|x| truncate_decimal(x, right))
}

#[inline(always)]
pub fn div_decimal_decimal(left: Decimal, right: Decimal) -> Option<Decimal> {
    if right.is_zero() {
        None
    } else {
        Some(left / right)
    }
}

#[inline(always)]
pub fn div_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal> {
    let left = left?;
    div_decimal_decimal(left, right)
}

#[inline(always)]
pub fn div_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal> {
    let right = right?;
    div_decimal_decimal(left, right)
}

#[inline(always)]
pub fn div_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
    let left = left?;
    let right = right?;
    div_decimal_decimal(left, right)
}

#[inline(always)]
pub fn modulo_decimal_decimal(left: Decimal, right: Decimal) -> Decimal {
    left % right
}

some_polymorphic_function2!(modulo, decimal, Decimal, decimal, Decimal, Decimal);

pub fn element<T>(array: Vec<T>) -> Option<T>
where
    T: Copy,
{
    if array.len() == 1 {
        Some(array[0])
    } else {
        None
    }
}

pub fn elementN<T>(array: Vec<Option<T>>) -> Option<T>
where
    T: Copy,
{
    if array.len() == 1 {
        array[0]
    } else {
        None
    }
}

pub fn power_i32_d(left: i32, right: F64) -> F64 {
    F64::new((left as f64).powf(right.into_inner()))
}

some_polymorphic_function2!(power, i32, i32, d, F64, F64);

pub fn power_d_d(left: F64, right: F64) -> F64 {
    F64::new(left.into_inner().powf(right.into_inner()))
}

pub fn power_decimal_decimal(left: Decimal, right: Decimal) -> F64 {
    if right == Decimal::new(5, 1) {
        // special case for sqrt, has higher precision than pow
        F64::from(left.sqrt().unwrap().to_f64().unwrap())
    } else {
        F64::from(left.powd(right).to_f64().unwrap())
    }
}

some_polymorphic_function2!(power, decimal, Decimal, decimal, Decimal, F64);

pub fn sqrt_decimal(left: Decimal) -> F64 {
    F64::from(left.sqrt().unwrap().to_f64().unwrap())
}

some_polymorphic_function1!(sqrt, decimal, Decimal, F64);

//////////////////// floor /////////////////////

#[inline(always)]
pub fn floor_d(value: F64) -> F64 {
    F64::new(value.into_inner().floor())
}

#[inline(always)]
pub fn floor_f(value: F32) -> F32 {
    F32::new(value.into_inner().floor())
}

#[inline(always)]
pub fn floor_decimal(value: Decimal) -> Decimal {
    value.floor()
}

some_polymorphic_function1!(floor, f, F32, F32);
some_polymorphic_function1!(floor, d, F64, F64);
some_polymorphic_function1!(floor, decimal, Decimal, Decimal);

//////////////////// ceil /////////////////////

#[inline(always)]
pub fn ceil_d(value: F64) -> F64 {
    F64::new(value.into_inner().ceil())
}

#[inline(always)]
pub fn ceil_f(value: F32) -> F32 {
    F32::new(value.into_inner().ceil())
}

#[inline(always)]
pub fn ceil_decimal(value: Decimal) -> Decimal {
    value.ceil()
}

some_polymorphic_function1!(ceil, f, F32, F32);
some_polymorphic_function1!(ceil, d, F64, F64);
some_polymorphic_function1!(ceil, decimal, Decimal, Decimal);

///////////////////// sign //////////////////////

#[inline(always)]
pub fn sign_d(value: F64) -> F64 {
    // Rust signum never returns 0
    let x = value.into_inner();
    if x == 0f64 {
        value
    } else {
        F64::new(x.signum())
    }
}

#[inline(always)]
pub fn sign_f(value: F32) -> F32 {
    // Rust signum never returns 0
    let x = value.into_inner();
    if x == 0f32 {
        value
    } else {
        F32::new(x.signum())
    }
}

#[inline(always)]
pub fn sign_decimal(value: Decimal) -> Decimal {
    value.signum()
}

some_polymorphic_function1!(sign, f, F32, F32);
some_polymorphic_function1!(sign, d, F64, F64);
some_polymorphic_function1!(sign, decimal, Decimal, Decimal);

// PI
#[inline(always)]
pub fn pi() -> F64 {
    std::f64::consts::PI.into()
}

/////////// Trigonometric Fucntions //////////////

#[inline(always)]
pub fn sin_d(value: F64) -> F64 {
    value.into_inner().sin().into()
}

some_polymorphic_function1!(sin, d, F64, F64);

#[inline(always)]
pub fn cos_d(value: F64) -> F64 {
    value.into_inner().cos().into()
}

some_polymorphic_function1!(cos, d, F64, F64);

////////////////////////////////////////////////

pub fn cardinality_<T>(value: Vec<T>) -> i32 {
    value.len() as i32
}

pub fn cardinalityN<T>(value: Option<Vec<T>>) -> Option<i32> {
    let value = value?;
    Some(value.len() as i32)
}

pub fn index__<T>(value: &Vec<T>, index: usize) -> T
where
    T: Clone,
{
    (*value.index(index)).clone()
}

pub fn index__N<T>(value: &Vec<T>, index: Option<usize>) -> T
where
    T: Clone,
{
    index__(value, index.unwrap())
}

pub fn index_N_<T>(value: &Option<Vec<T>>, index: usize) -> Option<T>
where
    T: Clone,
{
    value.as_ref().map(|value| index__(value, index))
}

pub fn index_N_N<T>(value: &Option<Vec<T>>, index: Option<usize>) -> Option<T>
where
    T: Clone,
{
    value.as_ref().map(|value| index__N(value, index))
}

pub fn array<T>() -> Vec<T> {
    vec![]
}

pub fn limit<T>(vector: &[T], limit: usize) -> Vec<T>
where
    T: Clone,
{
    vector[0..limit].to_vec()
}

pub fn dump<T>(prefix: String, data: &T) -> T
where
    T: Debug + Clone,
{
    println!("{}: {:?}", prefix, data);
    data.clone()
}

pub fn print(str: String) {
    print!("{}", str)
}

pub fn print_opt(str: Option<String>) {
    match str {
        None => print!("NULL"),
        Some(x) => print!("{}", x),
    }
}

pub fn zset_map<D, T, W, F>(data: &OrdZSet<D, W>, mapper: F) -> OrdZSet<T, W>
where
    D: DBData + 'static,
    W: DBWeight + 'static,
    T: DBData + 'static,
    F: Fn(&D) -> T,
{
    let mut builder = <OrdZSet<T, W> as Batch>::Builder::with_capacity((), data.len());

    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let item = cursor.key();
        let data = mapper(item);
        builder.push((data, cursor.weight()));
        cursor.step_key();
    }
    builder.done()
}
