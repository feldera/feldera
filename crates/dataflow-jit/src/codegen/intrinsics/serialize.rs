use crate::codegen::{intrinsics::decimal_from_parts, utils::str_from_raw_parts};
use chrono::{NaiveDate, TimeZone, Utc};
use std::{io::Write, slice};

pub(super) unsafe extern "C" fn byte_vec_push(buffer: &mut Vec<u8>, ptr: *const u8, len: usize) {
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };
    buffer.extend(bytes);
}

pub(super) unsafe extern "C" fn byte_vec_reserve(buffer: &mut Vec<u8>, additional: usize) {
    buffer.reserve(additional);
}

pub(super) unsafe extern "C" fn write_escaped_string_to_byte_vec(
    buffer: &mut Vec<u8>,
    ptr: *const u8,
    len: usize,
) {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    write!(buffer, "{string:?}").unwrap();
}

pub(super) unsafe extern "C" fn write_decimal_to_byte_vec(buffer: &mut Vec<u8>, lo: u64, hi: u64) {
    let decimal = decimal_from_parts(lo, hi);
    write!(buffer, "{decimal}").unwrap();
}

pub(super) unsafe extern "C" fn write_date_to_byte_vec(buffer: &mut Vec<u8>, date: i32) {
    let date = NaiveDate::from_num_days_from_ce_opt(date).unwrap();
    write!(buffer, "{}", date.format("%Y-%m-%d")).unwrap();
}

pub(super) unsafe extern "C" fn write_timestamp_to_byte_vec(buffer: &mut Vec<u8>, timestamp: i64) {
    let timestamp = Utc.timestamp_millis_opt(timestamp).unwrap();
    write!(buffer, "{}", timestamp.format("%+")).unwrap();
}

macro_rules! write_primitives_to_byte_vec {
    ($($primitive:ident),+ $(,)?) => {
        paste::paste! {
            $(
                pub(super) unsafe extern "C" fn [<write_ $primitive _to_byte_vec>](buffer: &mut Vec<u8>, value: $primitive) {
                    write!(buffer, "{value}").unwrap();
                }
            )+
        }
    };
}

write_primitives_to_byte_vec! {
    i8,  u8,
    u16, i16,
    u32, i32,
    u64, i64,
    // We can't use this for f32/f64 because JSON can't represent NaN or Inf
}

pub(super) unsafe extern "C" fn write_f64_to_byte_vec(buffer: &mut Vec<u8>, value: f64) {
    if value.is_finite() {
        write!(buffer, "{value}")
    } else if value.is_nan() {
        write!(buffer, "\"NaN\"")
    } else if value == f64::INFINITY {
        write!(buffer, "\"Inf\"")
    } else if value == f64::NEG_INFINITY {
        write!(buffer, "\"-Inf\"")
    } else {
        unreachable!()
    }
    .unwrap();
}

pub(super) unsafe extern "C" fn write_f32_to_byte_vec(buffer: &mut Vec<u8>, value: f32) {
    if value.is_finite() {
        write!(buffer, "{value}")
    } else if value.is_nan() {
        write!(buffer, "\"NaN\"")
    } else if value == f32::INFINITY {
        write!(buffer, "\"Inf\"")
    } else if value == f32::NEG_INFINITY {
        write!(buffer, "\"-Inf\"")
    } else {
        unreachable!()
    }
    .unwrap();
}
