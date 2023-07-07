use dbsp::algebra::{F32, F64};

use crate::for_all_compare;
use crate::for_all_int;
use crate::for_all_int_compare;
use crate::for_all_numeric;
use crate::for_all_numeric_compare;
use crate::some_operator;

#[inline(always)]
pub fn eq_i16_i16(left: i16, right: i16) -> bool {
    left == right
}

#[inline(always)]
pub fn eq_i32_i32(left: i32, right: i32) -> bool {
    left == right
}

#[inline(always)]
pub fn eq_i64_i64(left: i64, right: i64) -> bool {
    left == right
}

#[inline(always)]
pub fn eq_b_b(left: bool, right: bool) -> bool {
    left == right
}

#[inline(always)]
pub fn eq_d_d(left: F64, right: F64) -> bool {
    left == right
}

#[inline(always)]
pub fn eq_f_f(left: F32, right: F32) -> bool {
    left == right
}

#[inline(always)]
pub fn eq_s_s(left: String, right: String) -> bool {
    left == right
}

for_all_compare!(eq, bool);

#[inline(always)]
pub fn neq_i16_i16(left: i16, right: i16) -> bool {
    left != right
}

#[inline(always)]
pub fn neq_i32_i32(left: i32, right: i32) -> bool {
    left != right
}

#[inline(always)]
pub fn neq_i64_i64(left: i64, right: i64) -> bool {
    left != right
}

#[inline(always)]
pub fn neq_d_d(left: F64, right: F64) -> bool {
    left != right
}

#[inline(always)]
pub fn neq_f_f(left: F32, right: F32) -> bool {
    left != right
}

#[inline(always)]
pub fn neq_b_b(left: bool, right: bool) -> bool {
    left != right
}

#[inline(always)]
pub fn neq_s_s(left: String, right: String) -> bool {
    left != right
}

for_all_compare!(neq, bool);

#[inline(always)]
pub fn lt_i16_i16(left: i16, right: i16) -> bool {
    left < right
}

#[inline(always)]
pub fn lt_i32_i32(left: i32, right: i32) -> bool {
    left < right
}

#[inline(always)]
pub fn lt_i64_i64(left: i64, right: i64) -> bool {
    left < right
}

#[inline(always)]
pub fn lt_d_d(left: F64, right: F64) -> bool {
    left < right
}

#[inline(always)]
pub fn lt_f_f(left: F32, right: F32) -> bool {
    left < right
}

for_all_numeric_compare!(lt, bool);

#[inline(always)]
pub fn gt_i16_i16(left: i16, right: i16) -> bool {
    left > right
}

#[inline(always)]
pub fn gt_i32_i32(left: i32, right: i32) -> bool {
    left > right
}

#[inline(always)]
pub fn gt_i64_i64(left: i64, right: i64) -> bool {
    left > right
}

#[inline(always)]
pub fn gt_d_d(left: F64, right: F64) -> bool {
    left > right
}

#[inline(always)]
pub fn gt_f_f(left: F32, right: F32) -> bool {
    left > right
}

for_all_numeric_compare!(gt, bool);

#[inline(always)]
pub fn lte_i16_i16(left: i16, right: i16) -> bool {
    left <= right
}

#[inline(always)]
pub fn lte_i32_i32(left: i32, right: i32) -> bool {
    left <= right
}

#[inline(always)]
pub fn lte_i64_i64(left: i64, right: i64) -> bool {
    left <= right
}

#[inline(always)]
pub fn lte_d_d(left: F64, right: F64) -> bool {
    left <= right
}

#[inline(always)]
pub fn lte_f_f(left: F32, right: F32) -> bool {
    left <= right
}

for_all_numeric_compare!(lte, bool);

#[inline(always)]
pub fn gte_i16_i16(left: i16, right: i16) -> bool {
    left >= right
}

#[inline(always)]
pub fn gte_i32_i32(left: i32, right: i32) -> bool {
    left >= right
}

#[inline(always)]
pub fn gte_i64_i64(left: i64, right: i64) -> bool {
    left >= right
}

#[inline(always)]
pub fn gte_d_d(left: F64, right: F64) -> bool {
    left >= right
}

#[inline(always)]
pub fn gte_f_f(left: F32, right: F32) -> bool {
    left >= right
}

for_all_numeric_compare!(gte, bool);

#[inline(always)]
pub fn plus_i16_i16(left: i16, right: i16) -> i16 {
    left + right
}

#[inline(always)]
pub fn plus_i32_i32(left: i32, right: i32) -> i32 {
    left + right
}

#[inline(always)]
pub fn plus_i64_i64(left: i64, right: i64) -> i64 {
    left + right
}

#[inline(always)]
pub fn plus_d_d(left: F64, right: F64) -> F64 {
    left + right
}

#[inline(always)]
pub fn plus_f_f(left: F32, right: F32) -> F32 {
    left + right
}

for_all_numeric!(plus);

#[inline(always)]
pub fn minus_i16_i16(left: i16, right: i16) -> i16 {
    left - right
}

#[inline(always)]
pub fn minus_i32_i32(left: i32, right: i32) -> i32 {
    left - right
}

#[inline(always)]
pub fn minus_i64_i64(left: i64, right: i64) -> i64 {
    left - right
}

#[inline(always)]
pub fn minus_d_d(left: F64, right: F64) -> F64 {
    left - right
}

#[inline(always)]
pub fn minus_f_f(left: F32, right: F32) -> F32 {
    left - right
}

for_all_numeric!(minus);

#[inline(always)]
pub fn mod_i16_i16(left: i16, right: i16) -> i16 {
    left % right
}

#[inline(always)]
pub fn mod_i32_i32(left: i32, right: i32) -> i32 {
    left % right
}

#[inline(always)]
pub fn mod_i64_i64(left: i64, right: i64) -> i64 {
    left % right
}

for_all_int!(mod);

#[inline(always)]
pub fn times_i16_i16(left: i16, right: i16) -> i16 {
    left * right
}

#[inline(always)]
pub fn times_i32_i32(left: i32, right: i32) -> i32 {
    left * right
}

#[inline(always)]
pub fn times_i64_i64(left: i64, right: i64) -> i64 {
    left * right
}

#[inline(always)]
pub fn times_d_d(left: F64, right: F64) -> F64 {
    left * right
}

#[inline(always)]
pub fn times_f_f(left: F32, right: F32) -> F32 {
    left * right
}

for_all_numeric!(times);

// TODO: the shift rules look wrong.

#[inline(always)]
pub fn shiftr_i16_i16(left: i16, right: i16) -> i16 {
    left >> right
}

#[inline(always)]
pub fn shiftr_i32_i32(left: i32, right: i32) -> i32 {
    left >> right
}

#[inline(always)]
pub fn shiftr_i64_i64(left: i64, right: i64) -> i64 {
    left >> right
}

for_all_int!(shiftr);

#[inline(always)]
pub fn shiftl_i16_i16(left: i16, right: i16) -> i16 {
    left << right
}

#[inline(always)]
pub fn shiftl_i32_i32(left: i32, right: i32) -> i32 {
    left << right
}

#[inline(always)]
pub fn shiftl_i64_i64(left: i64, right: i64) -> i64 {
    left << right
}

for_all_int!(shiftl);

#[inline(always)]
pub fn band_i16_i16(left: i16, right: i16) -> i16 {
    left & right
}

#[inline(always)]
pub fn band_i32_i32(left: i32, right: i32) -> i32 {
    left & right
}

#[inline(always)]
pub fn band_i64_i64(left: i64, right: i64) -> i64 {
    left & right
}

for_all_int!(band);

#[inline(always)]
pub fn bor_i16_i16(left: i16, right: i16) -> i16 {
    left | right
}

#[inline(always)]
pub fn bor_i32_i32(left: i32, right: i32) -> i32 {
    left | right
}

#[inline(always)]
pub fn bor_i64_i64(left: i64, right: i64) -> i64 {
    left | right
}

for_all_int!(bor);

#[inline(always)]
pub fn bxor_i16_i16(left: i16, right: i16) -> i16 {
    left ^ right
}

#[inline(always)]
pub fn bxor_i32_i32(left: i32, right: i32) -> i32 {
    left ^ right
}

#[inline(always)]
pub fn bxor_i64_i64(left: i64, right: i64) -> i64 {
    left ^ right
}

for_all_int!(bxor);
