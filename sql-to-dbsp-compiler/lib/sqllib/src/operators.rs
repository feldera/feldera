use std::ops::{Add, Div, Mul, Sub};

use dbsp::algebra::{F32, F64};
use num::PrimInt;
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, ToPrimitive};

use crate::{
    for_all_compare, for_all_int_compare, for_all_int_operator, for_all_numeric_compare,
    some_existing_operator, some_operator,
};

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
    T: CheckedAdd,
{
    left.checked_add(&right)
        .expect("attempt to add with overflow")
}

for_all_int_operator!(plus);
some_operator!(plus, decimal, Decimal, Decimal);

fn fp_plus<T>(left: T, right: T) -> T
where
    T: Add<Output = T>,
{
    left + right
}

some_operator!(fp_plus, plus, f, F32, F32);
some_operator!(fp_plus, plus, d, F64, F64);

#[inline(always)]
fn minus<T>(left: T, right: T) -> T
where
    T: CheckedSub,
{
    left.checked_sub(&right)
        .expect("attempt to subtract with overflow")
}

for_all_int_operator!(minus);
some_operator!(minus, decimal, Decimal, Decimal);

fn fp_minus<T>(left: T, right: T) -> T
where
    T: Sub<Output = T>,
{
    left - right
}

some_operator!(fp_minus, minus, f, F32, F32);
some_operator!(fp_minus, minus, d, F64, F64);

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
    T: CheckedMul,
{
    left.checked_mul(&right)
        .expect("attempt to multiply with overflow")
}

for_all_int_operator!(times);
some_operator!(times, decimal, Decimal, Decimal);

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

fn fp_div<T>(left: T, right: T) -> T
where
    T: Div<Output = T>,
{
    left / right
}

some_operator!(fp_div, div, f, F32, F32);
some_operator!(fp_div, div, d, F64, F64);

pub fn plus_u_u(left: usize, right: usize) -> usize {
    left + right
}
