// Automatically-generated file
#![allow(unused_parens)]
#![allow(non_snake_case)]
use dbsp::algebra::{F32, F64};

#[inline(always)]
pub fn eq_i16_i16(left: i16, right: i16) -> bool
{
    (left == right)
}

#[inline(always)]
pub fn eq_i32_i32(left: i32, right: i32) -> bool
{
    (left == right)
}

#[inline(always)]
pub fn eq_i64_i64(left: i64, right: i64) -> bool
{
    (left == right)
}

#[inline(always)]
pub fn eq_i16N_i16(left: Option<i16>, right: i16) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_i32N_i32(left: Option<i32>, right: i32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_i64N_i64(left: Option<i64>, right: i64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_i16_i16N(left: i16, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_i32_i32N(left: i32, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_i64_i64N(left: i64, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i16_i16(left: i16, right: i16) -> bool
{
    (left != right)
}

#[inline(always)]
pub fn neq_i32_i32(left: i32, right: i32) -> bool
{
    (left != right)
}

#[inline(always)]
pub fn neq_i64_i64(left: i64, right: i64) -> bool
{
    (left != right)
}

#[inline(always)]
pub fn neq_i16N_i16(left: Option<i16>, right: i16) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i32N_i32(left: Option<i32>, right: i32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i64N_i64(left: Option<i64>, right: i64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i16_i16N(left: i16, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i32_i32N(left: i32, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i64_i64N(left: i64, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i16_i16(left: i16, right: i16) -> bool
{
    (left < right)
}

#[inline(always)]
pub fn lt_i32_i32(left: i32, right: i32) -> bool
{
    (left < right)
}

#[inline(always)]
pub fn lt_i64_i64(left: i64, right: i64) -> bool
{
    (left < right)
}

#[inline(always)]
pub fn lt_i16N_i16(left: Option<i16>, right: i16) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i32N_i32(left: Option<i32>, right: i32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i64N_i64(left: Option<i64>, right: i64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i16_i16N(left: i16, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i32_i32N(left: i32, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i64_i64N(left: i64, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i16_i16(left: i16, right: i16) -> bool
{
    (left > right)
}

#[inline(always)]
pub fn gt_i32_i32(left: i32, right: i32) -> bool
{
    (left > right)
}

#[inline(always)]
pub fn gt_i64_i64(left: i64, right: i64) -> bool
{
    (left > right)
}

#[inline(always)]
pub fn gt_i16N_i16(left: Option<i16>, right: i16) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i32N_i32(left: Option<i32>, right: i32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i64N_i64(left: Option<i64>, right: i64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i16_i16N(left: i16, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i32_i32N(left: i32, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i64_i64N(left: i64, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i16_i16(left: i16, right: i16) -> bool
{
    (left <= right)
}

#[inline(always)]
pub fn lte_i32_i32(left: i32, right: i32) -> bool
{
    (left <= right)
}

#[inline(always)]
pub fn lte_i64_i64(left: i64, right: i64) -> bool
{
    (left <= right)
}

#[inline(always)]
pub fn lte_i16N_i16(left: Option<i16>, right: i16) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i32N_i32(left: Option<i32>, right: i32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i64N_i64(left: Option<i64>, right: i64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i16_i16N(left: i16, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i32_i32N(left: i32, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i64_i64N(left: i64, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i16_i16(left: i16, right: i16) -> bool
{
    (left >= right)
}

#[inline(always)]
pub fn gte_i32_i32(left: i32, right: i32) -> bool
{
    (left >= right)
}

#[inline(always)]
pub fn gte_i64_i64(left: i64, right: i64) -> bool
{
    (left >= right)
}

#[inline(always)]
pub fn gte_i16N_i16(left: Option<i16>, right: i16) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i32N_i32(left: Option<i32>, right: i32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i64N_i64(left: Option<i64>, right: i64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i16_i16N(left: i16, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i32_i32N(left: i32, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i64_i64N(left: i64, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn plus_i16_i16(left: i16, right: i16) -> i16
{
    (left + right)
}

#[inline(always)]
pub fn plus_i32_i32(left: i32, right: i32) -> i32
{
    (left + right)
}

#[inline(always)]
pub fn plus_i64_i64(left: i64, right: i64) -> i64
{
    (left + right)
}

#[inline(always)]
pub fn plus_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l + r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn plus_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l + r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn plus_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l + r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn plus_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l + r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn plus_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l + r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn plus_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l + r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn plus_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l + r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn plus_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l + r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn plus_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l + r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn minus_i16_i16(left: i16, right: i16) -> i16
{
    (left - right)
}

#[inline(always)]
pub fn minus_i32_i32(left: i32, right: i32) -> i32
{
    (left - right)
}

#[inline(always)]
pub fn minus_i64_i64(left: i64, right: i64) -> i64
{
    (left - right)
}

#[inline(always)]
pub fn minus_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l - r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn minus_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l - r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn minus_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l - r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn minus_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l - r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn minus_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l - r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn minus_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l - r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn minus_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l - r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn minus_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l - r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn minus_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l - r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn mod_i16_i16(left: i16, right: i16) -> i16
{
    (left % right)
}

#[inline(always)]
pub fn mod_i32_i32(left: i32, right: i32) -> i32
{
    (left % right)
}

#[inline(always)]
pub fn mod_i64_i64(left: i64, right: i64) -> i64
{
    (left % right)
}

#[inline(always)]
pub fn mod_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l % r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn mod_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l % r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn mod_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l % r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn mod_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l % r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn mod_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l % r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn mod_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l % r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn mod_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l % r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn mod_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l % r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn mod_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l % r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn times_i16_i16(left: i16, right: i16) -> i16
{
    (left * right)
}

#[inline(always)]
pub fn times_i32_i32(left: i32, right: i32) -> i32
{
    (left * right)
}

#[inline(always)]
pub fn times_i64_i64(left: i64, right: i64) -> i64
{
    (left * right)
}

#[inline(always)]
pub fn times_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l * r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn times_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l * r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn times_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l * r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn times_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l * r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn times_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l * r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn times_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l * r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn times_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l * r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn times_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l * r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn times_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l * r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn shiftr_i16_i16(left: i16, right: i16) -> i16
{
    (left >> right)
}

#[inline(always)]
pub fn shiftr_i32_i32(left: i32, right: i32) -> i32
{
    (left >> right)
}

#[inline(always)]
pub fn shiftr_i64_i64(left: i64, right: i64) -> i64
{
    (left >> right)
}

#[inline(always)]
pub fn shiftr_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l >> r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn shiftr_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l >> r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn shiftr_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l >> r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn shiftr_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l >> r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn shiftr_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l >> r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn shiftr_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l >> r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn shiftr_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l >> r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn shiftr_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l >> r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn shiftr_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l >> r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn shiftl_i16_i16(left: i16, right: i16) -> i16
{
    (left << right)
}

#[inline(always)]
pub fn shiftl_i32_i32(left: i32, right: i32) -> i32
{
    (left << right)
}

#[inline(always)]
pub fn shiftl_i64_i64(left: i64, right: i64) -> i64
{
    (left << right)
}

#[inline(always)]
pub fn shiftl_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l << r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn shiftl_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l << r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn shiftl_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l << r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn shiftl_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l << r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn shiftl_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l << r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn shiftl_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l << r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn shiftl_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l << r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn shiftl_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l << r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn shiftl_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l << r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn band_i16_i16(left: i16, right: i16) -> i16
{
    (left & right)
}

#[inline(always)]
pub fn band_i32_i32(left: i32, right: i32) -> i32
{
    (left & right)
}

#[inline(always)]
pub fn band_i64_i64(left: i64, right: i64) -> i64
{
    (left & right)
}

#[inline(always)]
pub fn band_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l & r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn band_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l & r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn band_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l & r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn band_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l & r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn band_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l & r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn band_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l & r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn band_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l & r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn band_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l & r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn band_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l & r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn bor_i16_i16(left: i16, right: i16) -> i16
{
    (left | right)
}

#[inline(always)]
pub fn bor_i32_i32(left: i32, right: i32) -> i32
{
    (left | right)
}

#[inline(always)]
pub fn bor_i64_i64(left: i64, right: i64) -> i64
{
    (left | right)
}

#[inline(always)]
pub fn bor_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l | r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn bor_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l | r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn bor_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l | r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn bor_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l | r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn bor_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l | r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn bor_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l | r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn bor_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l | r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn bor_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l | r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn bor_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l | r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn bxor_i16_i16(left: i16, right: i16) -> i16
{
    (left ^ right)
}

#[inline(always)]
pub fn bxor_i32_i32(left: i32, right: i32) -> i32
{
    (left ^ right)
}

#[inline(always)]
pub fn bxor_i64_i64(left: i64, right: i64) -> i64
{
    (left ^ right)
}

#[inline(always)]
pub fn bxor_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l ^ r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn bxor_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l ^ r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn bxor_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l ^ r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn bxor_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l ^ r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn bxor_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l ^ r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn bxor_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l ^ r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn bxor_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l ^ r)),
        (_, _) => None::<i16>,
    })
}

#[inline(always)]
pub fn bxor_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l ^ r)),
        (_, _) => None::<i32>,
    })
}

#[inline(always)]
pub fn bxor_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l ^ r)),
        (_, _) => None::<i64>,
    })
}

#[inline(always)]
pub fn eq_b_b(left: bool, right: bool) -> bool
{
    (left == right)
}

#[inline(always)]
pub fn eq_bN_b(left: Option<bool>, right: bool) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_b_bN(left: bool, right: Option<bool>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_b_b(left: bool, right: bool) -> bool
{
    (left != right)
}

#[inline(always)]
pub fn neq_bN_b(left: Option<bool>, right: bool) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_b_bN(left: bool, right: Option<bool>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_s_s(left: String, right: String) -> bool
{
    (left == right)
}

#[inline(always)]
pub fn eq_sN_s(left: Option<String>, right: String) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_s_sN(left: String, right: Option<String>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_sN_sN(left: Option<String>, right: Option<String>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_s_s(left: String, right: String) -> bool
{
    (left != right)
}

#[inline(always)]
pub fn neq_sN_s(left: Option<String>, right: String) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_s_sN(left: String, right: Option<String>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_sN_sN(left: Option<String>, right: Option<String>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_d_d(left: F64, right: F64) -> bool
{
    (left == right)
}

#[inline(always)]
pub fn eq_f_f(left: F32, right: F32) -> bool
{
    (left == right)
}

#[inline(always)]
pub fn eq_dN_d(left: Option<F64>, right: F64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_fN_f(left: Option<F32>, right: F32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_d_dN(left: F64, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_f_fN(left: F32, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn eq_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l == r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_d_d(left: F64, right: F64) -> bool
{
    (left != right)
}

#[inline(always)]
pub fn neq_f_f(left: F32, right: F32) -> bool
{
    (left != right)
}

#[inline(always)]
pub fn neq_dN_d(left: Option<F64>, right: F64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_fN_f(left: Option<F32>, right: F32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_d_dN(left: F64, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_f_fN(left: F32, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn neq_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l != r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_d_d(left: F64, right: F64) -> bool
{
    (left < right)
}

#[inline(always)]
pub fn lt_f_f(left: F32, right: F32) -> bool
{
    (left < right)
}

#[inline(always)]
pub fn lt_dN_d(left: Option<F64>, right: F64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_fN_f(left: Option<F32>, right: F32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_d_dN(left: F64, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_f_fN(left: F32, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lt_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l < r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_d_d(left: F64, right: F64) -> bool
{
    (left > right)
}

#[inline(always)]
pub fn gt_f_f(left: F32, right: F32) -> bool
{
    (left > right)
}

#[inline(always)]
pub fn gt_dN_d(left: Option<F64>, right: F64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_fN_f(left: Option<F32>, right: F32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_d_dN(left: F64, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_f_fN(left: F32, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gt_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l > r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_d_d(left: F64, right: F64) -> bool
{
    (left <= right)
}

#[inline(always)]
pub fn lte_f_f(left: F32, right: F32) -> bool
{
    (left <= right)
}

#[inline(always)]
pub fn lte_dN_d(left: Option<F64>, right: F64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_fN_f(left: Option<F32>, right: F32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_d_dN(left: F64, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_f_fN(left: F32, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn lte_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l <= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_d_d(left: F64, right: F64) -> bool
{
    (left >= right)
}

#[inline(always)]
pub fn gte_f_f(left: F32, right: F32) -> bool
{
    (left >= right)
}

#[inline(always)]
pub fn gte_dN_d(left: Option<F64>, right: F64) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_fN_f(left: Option<F32>, right: F32) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_d_dN(left: F64, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_f_fN(left: F32, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn gte_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<bool>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l >= r)),
        (_, _) => None::<bool>,
    })
}

#[inline(always)]
pub fn plus_d_d(left: F64, right: F64) -> F64
{
    (left + right)
}

#[inline(always)]
pub fn plus_f_f(left: F32, right: F32) -> F32
{
    (left + right)
}

#[inline(always)]
pub fn plus_dN_d(left: Option<F64>, right: F64) -> Option<F64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l + r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn plus_fN_f(left: Option<F32>, right: F32) -> Option<F32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l + r)),
        (_, _) => None::<F32>,
    })
}

#[inline(always)]
pub fn plus_d_dN(left: F64, right: Option<F64>) -> Option<F64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l + r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn plus_f_fN(left: F32, right: Option<F32>) -> Option<F32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l + r)),
        (_, _) => None::<F32>,
    })
}

#[inline(always)]
pub fn plus_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l + r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn plus_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l + r)),
        (_, _) => None::<F32>,
    })
}

#[inline(always)]
pub fn minus_d_d(left: F64, right: F64) -> F64
{
    (left - right)
}

#[inline(always)]
pub fn minus_f_f(left: F32, right: F32) -> F32
{
    (left - right)
}

#[inline(always)]
pub fn minus_dN_d(left: Option<F64>, right: F64) -> Option<F64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l - r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn minus_fN_f(left: Option<F32>, right: F32) -> Option<F32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l - r)),
        (_, _) => None::<F32>,
    })
}

#[inline(always)]
pub fn minus_d_dN(left: F64, right: Option<F64>) -> Option<F64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l - r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn minus_f_fN(left: F32, right: Option<F32>) -> Option<F32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l - r)),
        (_, _) => None::<F32>,
    })
}

#[inline(always)]
pub fn minus_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l - r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn minus_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l - r)),
        (_, _) => None::<F32>,
    })
}

#[inline(always)]
pub fn times_d_d(left: F64, right: F64) -> F64
{
    (left * right)
}

#[inline(always)]
pub fn times_f_f(left: F32, right: F32) -> F32
{
    (left * right)
}

#[inline(always)]
pub fn times_dN_d(left: Option<F64>, right: F64) -> Option<F64>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l * r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn times_fN_f(left: Option<F32>, right: F32) -> Option<F32>
{
    (match (left, right, ) {
        (Some(l), r) => Some((l * r)),
        (_, _) => None::<F32>,
    })
}

#[inline(always)]
pub fn times_d_dN(left: F64, right: Option<F64>) -> Option<F64>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l * r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn times_f_fN(left: F32, right: Option<F32>) -> Option<F32>
{
    (match (left, right, ) {
        (l, Some(r)) => Some((l * r)),
        (_, _) => None::<F32>,
    })
}

#[inline(always)]
pub fn times_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l * r)),
        (_, _) => None::<F64>,
    })
}

#[inline(always)]
pub fn times_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32>
{
    (match (left, right, ) {
        (Some(l), Some(r)) => Some((l * r)),
        (_, _) => None::<F32>,
    })
}

