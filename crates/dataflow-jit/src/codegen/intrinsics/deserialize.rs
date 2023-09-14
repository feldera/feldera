use crate::{codegen::utils::str_from_raw_parts, ThinStr};
use serde_json::Value;
use std::mem::MaybeUninit;

// TODO: We can precompile the json pointers into something faster

pub(super) extern "C" fn deserialize_json_string(
    place: &mut MaybeUninit<ThinStr>,
    json_pointer_ptr: *const u8,
    json_pointer_len: usize,
    map: &Value,
) -> bool {
    // The json pointer we're accessing the map with
    let json_pointer = unsafe { str_from_raw_parts(json_pointer_ptr, json_pointer_len) };

    if let Some(string) = map.pointer(json_pointer).and_then(Value::as_str) {
        place.write(ThinStr::from(string));
        false

    // Otherwise the value couldn't be found and is considered null
    } else {
        true
    }
}

pub(super) extern "C" fn deserialize_json_bool(
    place: &mut MaybeUninit<bool>,
    json_pointer_ptr: *const u8,
    json_pointer_len: usize,
    map: &Value,
) -> bool {
    // The json pointer we're accessing the map with
    let json_pointer = unsafe { str_from_raw_parts(json_pointer_ptr, json_pointer_len) };

    if let Some(boolean) = map.pointer(json_pointer).and_then(Value::as_bool) {
        place.write(boolean);
        false

    // Otherwise the value couldn't be found and is considered null
    } else {
        true
    }
}

pub(super) extern "C" fn deserialize_json_i64(
    place: &mut MaybeUninit<i64>,
    json_pointer_ptr: *const u8,
    json_pointer_len: usize,
    map: &Value,
) -> bool {
    // The json pointer we're accessing the map with
    let json_pointer = unsafe { str_from_raw_parts(json_pointer_ptr, json_pointer_len) };

    if let Some(int) = map.pointer(json_pointer).and_then(Value::as_i64) {
        place.write(int);
        false

    // Otherwise the value couldn't be found and is considered null
    } else {
        true
    }
}

pub(super) extern "C" fn deserialize_json_i32(
    place: &mut MaybeUninit<i32>,
    json_pointer_ptr: *const u8,
    json_pointer_len: usize,
    map: &Value,
) -> bool {
    // The json pointer we're accessing the map with
    let json_pointer = unsafe { str_from_raw_parts(json_pointer_ptr, json_pointer_len) };

    if let Some(int) = map.pointer(json_pointer).and_then(Value::as_i64) {
        place.write(int as i32);
        false

    // Otherwise the value couldn't be found and is considered null
    } else {
        true
    }
}

pub(super) extern "C" fn deserialize_json_f64(
    place: &mut MaybeUninit<f64>,
    json_pointer_ptr: *const u8,
    json_pointer_len: usize,
    map: &Value,
) -> bool {
    // The json pointer we're accessing the map with
    let json_pointer = unsafe { str_from_raw_parts(json_pointer_ptr, json_pointer_len) };

    if let Some(value) = map.pointer(json_pointer) {
        let float = value
            .as_f64()
            // JSON can't represent NaN/Inf/-Inf for floats so users
            // have to use a string, fun!
            .or_else(|| {
                let value = value.as_str()?;

                Some(if value.eq_ignore_ascii_case("nan") {
                    f64::NAN
                } else if value.eq_ignore_ascii_case("inf") {
                    f64::INFINITY
                } else if value.eq_ignore_ascii_case("-inf") {
                    f64::NEG_INFINITY
                } else {
                    return None;
                })
            });

        if let Some(float) = float {
            place.write(float);
            return false;
        }
    }

    // Otherwise the value couldn't be found and is considered null
    true
}

pub(super) extern "C" fn deserialize_json_f32(
    place: &mut MaybeUninit<f32>,
    json_pointer_ptr: *const u8,
    json_pointer_len: usize,
    map: &Value,
) -> bool {
    // The json pointer we're accessing the map with
    let json_pointer = unsafe { str_from_raw_parts(json_pointer_ptr, json_pointer_len) };

    if let Some(value) = map.pointer(json_pointer) {
        let float = value
            .as_f64()
            // TODO: Should we emit an error when the f64 is OOB for a f32
            // or just silently lose precision?
            .map(|float| float as f32)
            // JSON can't represent NaN/Inf/-Inf for floats so users
            // have to use a string, fun!
            .or_else(|| {
                let value = value.as_str()?;

                Some(if value.eq_ignore_ascii_case("nan") {
                    f32::NAN
                } else if value.eq_ignore_ascii_case("inf") {
                    f32::INFINITY
                } else if value.eq_ignore_ascii_case("-inf") {
                    f32::NEG_INFINITY
                } else {
                    return None;
                })
            });

        if let Some(float) = float {
            place.write(float);
            return false;
        }
    }

    // Otherwise the value couldn't be found and is considered null
    true
}
