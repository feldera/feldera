use dbsp::algebra::{F32, F64};
use num::PrimInt;

use crate::{
    for_all_compare, for_all_int_compare, for_all_int_operator, for_all_numeric_compare,
    for_all_numeric_operator, some_existing_operator, some_operator,
};

use core::ops::{Add, Div, Mul, Sub};
use rust_decimal::Decimal;

#[inline(always)]
pub(crate) fn eq<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    left == right
}

for_all_compare!(eq, bool);

pub fn is_same__<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    left == right
}

pub fn is_distinct__<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    !(left == right)
}

#[inline(always)]
pub(crate) fn neq<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    left != right
}

for_all_compare!(neq, bool);

#[inline(always)]
pub(crate) fn lt<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left < right
}

for_all_compare!(lt, bool);

#[inline(always)]
pub(crate) fn gt<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left > right
}

for_all_compare!(gt, bool);

#[inline(always)]
pub(crate) fn lte<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left <= right
}

for_all_compare!(lte, bool);

#[inline(always)]
pub(crate) fn gte<T>(left: T, right: T) -> bool
where
    T: Ord,
{
    left >= right
}

for_all_compare!(gte, bool);

#[inline(always)]
fn plus<T>(left: T, right: T) -> T
where
    T: Add<Output = T>,
{
    left + right
}

for_all_numeric_operator!(plus);

#[inline(always)]
fn minus<T>(left: T, right: T) -> T
where
    T: Sub<Output = T>,
{
    left - right
}

for_all_numeric_operator!(minus);

#[inline(always)]
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

#[inline(always)]
fn times<T>(left: T, right: T) -> T
where
    T: Mul<Output = T>,
{
    left * right
}

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
where
    T: PrimInt,
{
    left & right
}

for_all_int_operator!(band);

#[inline(always)]
fn bor<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left | right
}

for_all_int_operator!(bor);

#[inline(always)]
fn bxor<T>(left: T, right: T) -> T
where
    T: PrimInt,
{
    left ^ right
}

for_all_int_operator!(bxor);

#[inline(always)]
fn div<T>(left: T, right: T) -> T
where
    T: Div<Output = T>,
{
    left / right
}

for_all_numeric_operator!(div);

pub fn plus_u_u(left: usize, right: usize) -> usize {
    left + right
}
