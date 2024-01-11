use dbsp::algebra::{F32, F64};
use num::PrimInt;
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};

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

pub fn plus_f_f(left: F32, right: F32) -> F32 {
    left + right
}

pub fn plus_fN_f(left: Option<F32>, right: F32) -> Option<F32> {
    let left = left?;
    Some(plus_f_f(left, right))
}

pub fn plus_f_fN(left: F32, right: Option<F32>) -> Option<F32> {
    let right = right?;
    Some(plus_f_f(left, right))
}

pub fn plus_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32> {
    let left = left?;
    let right = right?;
    Some(plus_f_f(left, right))
}

pub fn plus_d_d(left: F64, right: F64) -> F64 {
    left + right
}

pub fn plus_dN_d(left: Option<F64>, right: F64) -> Option<F64> {
    let left = left?;
    Some(plus_d_d(left, right))
}

pub fn plus_d_dN(left: F64, right: Option<F64>) -> Option<F64> {
    let right = right?;
    Some(plus_d_d(left, right))
}

pub fn plus_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64> {
    let left = left?;
    let right = right?;
    Some(plus_d_d(left, right))
}

pub fn plus_decimal_decimal(left: Decimal, right: Decimal) -> Decimal {
    left + right
}

pub fn plus_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal> {
    let left = left?;
    Some(plus_decimal_decimal(left, right))
}

pub fn plus_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal> {
    let right = right?;
    Some(plus_decimal_decimal(left, right))
}

pub fn plus_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
    let left = left?;
    let right = right?;
    Some(plus_decimal_decimal(left, right))
}

#[inline(always)]
fn minus<T>(left: T, right: T) -> T
where
    T: CheckedSub,
{
    left.checked_sub(&right)
        .expect("attempt to subtract with overflow")
}

for_all_int_operator!(minus);

pub fn minus_f_f(left: F32, right: F32) -> F32 {
    left - right
}

pub fn minus_fN_f(left: Option<F32>, right: F32) -> Option<F32> {
    let left = left?;
    Some(minus_f_f(left, right))
}

pub fn minus_f_fN(left: F32, right: Option<F32>) -> Option<F32> {
    let right = right?;
    Some(minus_f_f(left, right))
}

pub fn minus_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32> {
    let left = left?;
    let right = right?;
    Some(minus_f_f(left, right))
}

pub fn minus_d_d(left: F64, right: F64) -> F64 {
    left - right
}

pub fn minus_dN_d(left: Option<F64>, right: F64) -> Option<F64> {
    let left = left?;
    Some(minus_d_d(left, right))
}

pub fn minus_d_dN(left: F64, right: Option<F64>) -> Option<F64> {
    let right = right?;
    Some(minus_d_d(left, right))
}

pub fn minus_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64> {
    let left = left?;
    let right = right?;
    Some(minus_d_d(left, right))
}

pub fn minus_decimal_decimal(left: Decimal, right: Decimal) -> Decimal {
    left - right
}

pub fn minus_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal> {
    let left = left?;
    Some(minus_decimal_decimal(left, right))
}

pub fn minus_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal> {
    let right = right?;
    Some(minus_decimal_decimal(left, right))
}

pub fn minus_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
    let left = left?;
    let right = right?;
    Some(minus_decimal_decimal(left, right))
}

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

pub fn times_f_f(left: F32, right: F32) -> F32 {
    left * right
}

pub fn times_fN_f(left: Option<F32>, right: F32) -> Option<F32> {
    let left = left?;
    Some(times_f_f(left, right))
}

pub fn times_f_fN(left: F32, right: Option<F32>) -> Option<F32> {
    let right = right?;
    Some(times_f_f(left, right))
}

pub fn times_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32> {
    let left = left?;
    let right = right?;
    Some(times_f_f(left, right))
}

pub fn times_d_d(left: F64, right: F64) -> F64 {
    left * right
}

pub fn times_dN_d(left: Option<F64>, right: F64) -> Option<F64> {
    let left = left?;
    Some(times_d_d(left, right))
}

pub fn times_d_dN(left: F64, right: Option<F64>) -> Option<F64> {
    let right = right?;
    Some(times_d_d(left, right))
}

pub fn times_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64> {
    let left = left?;
    let right = right?;
    Some(times_d_d(left, right))
}

pub fn times_decimal_decimal(left: Decimal, right: Decimal) -> Decimal {
    left * right
}

pub fn times_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal> {
    let left = left?;
    Some(times_decimal_decimal(left, right))
}

pub fn times_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal> {
    let right = right?;
    Some(times_decimal_decimal(left, right))
}

pub fn times_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
    let left = left?;
    let right = right?;
    Some(times_decimal_decimal(left, right))
}

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
    T: CheckedDiv + PrimInt,
{
    let panic_message = if Some(0) == right.to_isize() {
        "attempt to divide by zero"
    } else {
        "attempt to divide with overflow"
    };

    left.checked_div(&right).expect(panic_message)
}

for_all_int_operator!(div);

pub fn div_f_f(left: F32, right: F32) -> F32 {
    left / right
}

pub fn div_fN_f(left: Option<F32>, right: F32) -> Option<F32> {
    let left = left?;
    Some(div_f_f(left, right))
}

pub fn div_f_fN(left: F32, right: Option<F32>) -> Option<F32> {
    let right = right?;
    Some(div_f_f(left, right))
}

pub fn div_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32> {
    let left = left?;
    let right = right?;
    Some(div_f_f(left, right))
}

pub fn div_d_d(left: F64, right: F64) -> F64 {
    left / right
}

pub fn div_dN_d(left: Option<F64>, right: F64) -> Option<F64> {
    let left = left?;
    Some(div_d_d(left, right))
}

pub fn div_d_dN(left: F64, right: Option<F64>) -> Option<F64> {
    let right = right?;
    Some(div_d_d(left, right))
}

pub fn div_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64> {
    let left = left?;
    let right = right?;
    Some(div_d_d(left, right))
}

pub fn div_decimal_decimal(left: Decimal, right: Decimal) -> Decimal {
    left / right
}

pub fn div_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal> {
    let left = left?;
    Some(div_decimal_decimal(left, right))
}

pub fn div_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal> {
    let right = right?;
    Some(div_decimal_decimal(left, right))
}

pub fn div_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
    let left = left?;
    let right = right?;
    Some(div_decimal_decimal(left, right))
}

pub fn plus_u_u(left: usize, right: usize) -> usize {
    left + right
}
