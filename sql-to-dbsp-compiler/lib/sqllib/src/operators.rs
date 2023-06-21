use dbsp::algebra::{F32, F64};

use crate::{
    for_all_compare,
    for_all_int_operator,
    for_all_int_compare,
    for_all_numeric_operator,
    for_all_numeric_compare,
    some_operator,
    some_existing_operator
};

use dbsp::algebra::Decimal;
use num::PrimInt;
use core::ops::{Add,Sub,Mul};

#[inline(always)]
pub(crate) fn eq<T>(left: T, right: T) -> bool
where T: Eq
{ left == right }

for_all_compare!(eq, bool);

#[inline(always)]
pub(crate) fn neq<T>(left: T, right: T) -> bool
where T: Eq
{ left != right }

for_all_compare!(neq, bool);

#[inline(always)]
pub(crate) fn lt<T>(left: T, right: T) -> bool
where T: Ord
{ left < right }

for_all_compare!(lt, bool);

#[inline(always)]
pub(crate) fn gt<T>(left: T, right: T) -> bool
where T: Ord
{ left > right }

for_all_compare!(gt, bool);

#[inline(always)]
pub(crate) fn lte<T>(left: T, right: T) -> bool
where T: Ord
{ left <= right }

for_all_compare!(lte, bool);

#[inline(always)]
pub(crate) fn gte<T>(left: T, right: T) -> bool
where T: Ord
{ left >= right }

for_all_compare!(gte, bool);

#[inline(always)]
fn plus<T>(left: T, right: T) -> T
where T: Add<Output = T>
{ left + right }

for_all_numeric_operator!(plus);

#[inline(always)]
fn minus<T>(left: T, right: T) -> T
where T: Sub<Output = T>
{ left - right }

for_all_numeric_operator!(minus);

#[inline(always)]
fn modulo<T>(left: T, right: T) -> T
where T: PrimInt
{ left % right }

for_all_int_operator!(modulo);

#[inline(always)]
fn times<T>(left: T, right: T) -> T
where T: Mul<Output = T>
{ left * right }

for_all_numeric_operator!(times);

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
fn band<T>(left: T, right: T) -> T
where T: PrimInt
{ left & right }

for_all_int_operator!(band);

#[inline(always)]
fn bor<T>(left: T, right: T) -> T
where T: PrimInt
{ left | right }

for_all_int_operator!(bor);

#[inline(always)]
fn bxor<T>(left: T, right: T) -> T
where T: PrimInt
{ left ^ right }

for_all_int_operator!(bxor);

/* div for integers is implemented manually, since it does not follow the
   pattern: the base function already returns an Option */
#[inline(always)]
pub fn div_i16_i16(left: i16, right: i16) -> Option<i16> {
    match right {
        0 => None,
        _ => Some(left / right),
    }
}

#[inline(always)]
pub fn div_i32_i32(left: i32, right: i32) -> Option<i32> {
    match right {
        0 => None,
        _ => Some(left / right),
    }
}

#[inline(always)]
pub fn div_i64_i64(left: i64, right: i64) -> Option<i64> {
    match right {
        0 => None,
        _ => Some(left / right),
    }
}

#[inline(always)]
pub fn div_i16N_i16(left: Option<i16>, right: i16) -> Option<i16> {
    let left = left?;
    div_i16_i16(left, right)
}

#[inline(always)]
pub fn div_i32N_i32(left: Option<i32>, right: i32) -> Option<i32> {
    let left = left?;
    div_i32_i32(left, right)
}

#[inline(always)]
pub fn div_i64N_i64(left: Option<i64>, right: i64) -> Option<i64> {
    let left = left?;
    div_i64_i64(left, right)
}

#[inline(always)]
pub fn div_i16_i16N(left: i16, right: Option<i16>) -> Option<i16> {
    let right = right?;
    div_i16_i16(left, right)
}

#[inline(always)]
pub fn div_i32_i32N(left: i32, right: Option<i32>) -> Option<i32> {
    let right = right?;
    div_i32_i32(left, right)
}

#[inline(always)]
pub fn div_i64_i64N(left: i64, right: Option<i64>) -> Option<i64> {
    let right = right?;
    div_i64_i64(left, right)
}

#[inline(always)]
pub fn div_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16> {
    let left = left?;
    let right = right?;
    div_i16_i16(left, right)
}

#[inline(always)]
pub fn div_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32> {
    let left = left?;
    let right = right?;
    div_i32_i32(left, right)
}

#[inline(always)]
pub fn div_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    let left = left?;
    let right = right?;
    div_i64_i64(left, right)
}

// TODO: does div F32 need to return Option?

#[inline(always)]
pub fn div_f_f(left: F32, right: F32) -> Option<F32> {
    Some(F32::new(left.into_inner() / right.into_inner()))
}

#[inline(always)]
pub fn div_f_fN(left: F32, right: Option<F32>) -> Option<F32> {
    right.map(|right| F32::new(left.into_inner() / right.into_inner()))
}

#[inline(always)]
pub fn div_fN_f(left: Option<F32>, right: F32) -> Option<F32> {
    left.map(|left| F32::new(left.into_inner() / right.into_inner()))
}

#[inline(always)]
pub fn div_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(left), Some(right)) => Some(F32::new(left.into_inner() / right.into_inner())),
    }
}

#[inline(always)]
pub fn div_d_d(left: F64, right: F64) -> Option<F64> {
    Some(F64::new(left.into_inner() / right.into_inner()))
}

#[inline(always)]
pub fn div_d_dN(left: F64, right: Option<F64>) -> Option<F64> {
    right.map(|right| F64::new(left.into_inner() / right.into_inner()))
}

#[inline(always)]
pub fn div_dN_d(left: Option<F64>, right: F64) -> Option<F64> {
    left.map(|left| F64::new(left.into_inner() / right.into_inner()))
}

#[inline(always)]
pub fn div_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(left), Some(right)) => Some(F64::new(left.into_inner() / right.into_inner())),
    }
}

pub fn plus_u_u(left: usize, right: usize) -> usize {
    left + right
}
