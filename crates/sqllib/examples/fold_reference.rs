//! Independent runtime calls for the P0 fold experiment's edge vectors.
//! This executable uses the real sqllib and DBSP; the fold registry does not.

#[path = "fold_reference/decimal.rs"]
mod decimal;
#[path = "fold_reference/integer_binary.rs"]
mod integer_binary;
#[path = "fold_reference/maps.rs"]
mod maps;
#[path = "fold_reference/selection.rs"]
mod selection;
#[path = "fold_reference/spatial.rs"]
mod spatial;
#[path = "fold_reference/temporal.rs"]
mod temporal;
#[path = "fold_reference/text.rs"]
mod text;

use feldera_sqllib::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

#[derive(Deserialize)]
struct Request {
    id: String,
    operation: String,
    value: String,
}

fn interval_cast(target: &str, value: &str) -> Result<Value, String> {
    if matches!(target, "YEAR" | "MONTH" | "YEAR_MONTH") {
        let value = LongInterval::from_months(value.parse::<i32>().map_err(|e| e.to_string())?);
        let result = match target {
            "YEAR" => cast_to_LongInterval_YEARS_LongInterval(value),
            "MONTH" => cast_to_LongInterval_MONTHS_LongInterval(value),
            "YEAR_MONTH" => cast_to_LongInterval_YEARS_TO_MONTHS_LongInterval(value),
            _ => unreachable!(),
        }
        .map_err(|e| e.to_string())?;
        return Ok(json!({"type":"months", "value":result.months().to_string()}));
    }
    let function: fn(ShortInterval) -> SqlResult<ShortInterval> = match target {
        "DAY" => cast_to_ShortInterval_DAYS_ShortInterval,
        "HOUR" => cast_to_ShortInterval_HOURS_ShortInterval,
        "DAY_HOUR" => cast_to_ShortInterval_DAYS_TO_HOURS_ShortInterval,
        "MINUTE" => cast_to_ShortInterval_MINUTES_ShortInterval,
        "DAY_MINUTE" => cast_to_ShortInterval_DAYS_TO_MINUTES_ShortInterval,
        "HOUR_MINUTE" => cast_to_ShortInterval_HOURS_TO_MINUTES_ShortInterval,
        "SECOND" => cast_to_ShortInterval_SECONDS_ShortInterval,
        "DAY_SECOND" => cast_to_ShortInterval_DAYS_TO_SECONDS_ShortInterval,
        "HOUR_SECOND" => cast_to_ShortInterval_HOURS_TO_SECONDS_ShortInterval,
        "MINUTE_SECOND" => cast_to_ShortInterval_MINUTES_TO_SECONDS_ShortInterval,
        _ => return Err(format!("Unknown interval cast target {target}")),
    };
    let value = ShortInterval::from_microseconds(value.parse::<i64>().map_err(|e| e.to_string())?);
    let result = function(value).map_err(|e| e.to_string())?;
    Ok(json!({"type":"micros", "value":result.microseconds().to_string()}))
}

fn binary_value(operation: &str, value: &str) -> Result<Value, String> {
    if let Some(width) = operation.strip_prefix("uuid_") {
        let uuid = cast_to_Uuid_s(SqlString::from(value)).map_err(|e| e.to_string())?;
        let (precision, fixed) = match width {
            "all" => (-1, false),
            "fixed20" => (20, true),
            "var4" => (4, false),
            _ => return Err("unknown UUID binary cast width".into()),
        };
        let bytes = cast_to_bytes_Uuid(uuid, precision, fixed).map_err(|e| e.to_string())?;
        return Ok(json!({"type":"binary", "value":hex::encode(bytes.as_slice())}));
    }
    if operation == "utf8" {
        let bytes =
            cast_to_bytes_s(SqlString::from(value), -1, false).map_err(|e| e.to_string())?;
        return Ok(json!({"type":"binary", "value":hex::encode(bytes.as_slice())}));
    }
    let bytes = binary::ByteArray::new(&hex::decode(value).map_err(|e| e.to_string())?);
    let result = match operation {
        "to_uuid" => {
            return cast_to_Uuid_bytes(bytes)
                .map(|v| json!({"type":"uuid", "value":v.to_string()}))
                .map_err(|e| e.to_string());
        }
        "md5" => return Ok(json!({"type":"string", "value":binary::md5_bytes(bytes).str()})),
        "starts_ff" => {
            return Ok(
                json!({"type":"boolean", "value":binary::binary_starts_with__(bytes, binary::ByteArray::new(&[255]))}),
            );
        }
        "ends_ff" => {
            return Ok(
                json!({"type":"boolean", "value":binary::binary_ends_with__(bytes, binary::ByteArray::new(&[255]))}),
            );
        }
        "position_ff" => {
            return Ok(
                json!({"type":"integer", "value":binary::binary_position__(binary::ByteArray::new(&[255]), bytes).to_string()}),
            );
        }
        "position_empty" => {
            return Ok(
                json!({"type":"integer", "value":binary::binary_position__(binary::ByteArray::new(&[]), bytes).to_string()}),
            );
        }
        "fixed4" => cast_to_bytes_bytes(bytes, 4, true).map_err(|e| e.to_string())?,
        "var2" => cast_to_bytes_bytes(bytes, 2, false).map_err(|e| e.to_string())?,
        "left2" => binary::left_bytes_i32(bytes, 2),
        "right2" => binary::right_bytes_i32(bytes, 2),
        "substring0_3" => binary::binary_substring3___(bytes, 0, 3),
        "concat" => binary::concat_bytes_bytes(bytes, binary::ByteArray::new(&[0, 255])),
        "overlay3" => binary::binary_overlay3___(bytes, binary::ByteArray::new(&[170, 187]), 2),
        "overlay4" => binary::binary_overlay4____(bytes, binary::ByteArray::new(&[170, 187]), 2, 0),
        "hex" => return Ok(json!({"type":"string", "value":binary::to_hex_(bytes).str()})),
        _ => return Err(format!("Unknown binary operation {operation}")),
    };
    Ok(json!({"type":"binary", "value":hex::encode(result.as_slice())}))
}

fn evaluate(request: &Request) -> Result<Value, String> {
    if request.operation == "spatial/distance" {
        return spatial::distance(&request.value);
    }
    if let Some(operation) = request.operation.strip_prefix("map/") {
        return maps::evaluate(operation, &request.value);
    }
    if let Some(operation) = request.operation.strip_prefix("text/") {
        return text::evaluate(operation, &request.value);
    }
    if let Some(operation) = request.operation.strip_prefix("integer_binary/") {
        return integer_binary::evaluate(operation, &request.value);
    }
    if let Some(operation) = request.operation.strip_prefix("selection/") {
        return selection::evaluate(operation, &request.value);
    }
    if let Some(operation) = request.operation.strip_prefix("decimal_arithmetic/") {
        return decimal::evaluate(operation, &request.value);
    }
    if let Some(operation) = request.operation.strip_prefix("round/") {
        return temporal::evaluate(operation, &request.value);
    }
    if let Some(operation) = request.operation.strip_prefix("unsigned_") {
        let (target, source) = operation.split_once('_').ok_or("unsigned cast operation")?;
        macro_rules! cast {
            ($string:ident, $integer:ident, $boolean:ident) => {
                match source {
                    "s" => $string(SqlString::from(request.value.as_str())),
                    "i64" => $integer(request.value.parse::<i64>().map_err(|e| e.to_string())?),
                    "b" => $boolean(request.value.parse::<bool>().map_err(|e| e.to_string())?),
                    _ => return Err("unsupported unsigned cast source".into()),
                }
                .map_err(|e| e.to_string())?
                .to_string()
            };
        }
        let value = match target {
            "u8" => cast!(cast_to_u8_s, cast_to_u8_i64, cast_to_u8_b),
            "u16" => cast!(cast_to_u16_s, cast_to_u16_i64, cast_to_u16_b),
            "u32" => cast!(cast_to_u32_s, cast_to_u32_i64, cast_to_u32_b),
            "u64" => cast!(cast_to_u64_s, cast_to_u64_i64, cast_to_u64_b),
            _ => return Err("unsupported unsigned cast target".into()),
        };
        return Ok(json!({"type":"integer", "value":value}));
    }
    if let Some(operation) = request.operation.strip_prefix("binary_") {
        return binary_value(operation, &request.value);
    }
    if let Some(target) = request.operation.strip_prefix("interval_cast_") {
        return interval_cast(target, &request.value);
    }
    if let Some(name) = request.operation.strip_prefix("math_") {
        let value =
            dbsp::algebra::F64::new(request.value.parse::<f64>().map_err(|e| e.to_string())?);
        let function: fn(dbsp::algebra::F64) -> dbsp::algebra::F64 = match name {
            "sin" => sin_d,
            "cos" => cos_d,
            "tan" => tan_d,
            "cot" => cot_d,
            "coth" => coth_d,
            "csch" => csch_d,
            "sec" => sec_d,
            "csc" => csc_d,
            "asin" => asin_d,
            "acos" => acos_d,
            "atan" => atan_d,
            "ln" => ln_d,
            "log" => log_d,
            "log10" => log10_d,
            "exp" => exp_d,
            "degrees" => degrees_d,
            "radians" => radians_d,
            "log4" => |v| log_d_d(v, dbsp::algebra::F64::new(4.0)),
            "power2" => |v| power_d_d(v, dbsp::algebra::F64::new(2.0)),
            "atan2" => |v| atan2_d_d(v, dbsp::algebra::F64::new(2.0)),
            "neg" => |v| -v,
            "mulzero" => |v| v * dbsp::algebra::F64::new(0.0),
            "divzero" => |v| v / dbsp::algebra::F64::new(0.0),
            "floor" => floor_d,
            "ceil" => ceil_d,
            _ => return Err(format!("Unknown runtime math operation {name}")),
        };
        // These runtime APIs signal domain errors by panicking. Catch only at
        // the oracle process boundary so one vector cannot suppress later ones.
        let result = std::panic::catch_unwind(|| function(value))
            .map_err(|_| format!("runtime math error in {name}"))?;
        return Ok(
            json!({"type":"float", "value":format!("{:016x}",result.into_inner().to_bits())}),
        );
    }
    let text = SqlString::from(request.value.as_str());
    let result = match request.operation.as_str() {
        "uuid" => cast_to_Uuid_s(text).map(|v| json!({"type":"uuid", "value":v.to_string()})),
        "overlay3" => Ok(json!({"type":"string", "value":overlay3___(text, SqlString::from("é"), 2).str()})),
        "overlay4" => Ok(json!({"type":"string", "value":overlay4____(text, SqlString::from("ab"), 2, 0).str()})),
        "float_cast" => cast_to_d_s(text).map(|v| json!({"type":"float", "value":format!("{:016x}", v.into_inner().to_bits())})),
        "float_cast_real" => cast_to_f_s(text).map(|v| json!({"type":"float", "value":format!("{:016x}", f64::from(v.into_inner()).to_bits())})),
        "array_nan_equal" => {
            let left = vec![vec![Some(dbsp::algebra::F64::new(f64::NAN)), None]];
            let right = left.clone();
            Ok(json!({"type":"boolean", "value":left == right}))
        }
        "date" => {
            cast_to_Date_s(text).map(|v| json!({"type": "date", "value": v.to_date().to_string()}))
        }
        "time" => {
            cast_to_Time_s(text).map(|v| json!({"type": "time", "value": v.to_time().to_string()}))
        }
        "timestamp" => cast_to_Timestamp_s(text).map(
            |v| json!({"type": "timestamp", "value": v.to_dateTime().naive_utc().to_string()}),
        ),
        "decimal_4_2" => {
            let input = request
                .value
                .parse::<SqlDecimal<38, 18>>()
                .map_err(|e| e.to_string())?;
            cast_to_SqlDecimal_SqlDecimal::<4, 2, 38, 18>(input)
                .map(|v| json!({"type": "decimal", "value": v.to_string()}))
        }
        "upper" => Ok(json!({"type": "string", "value": upper_(text).str()})),
        "initcap" => Ok(json!({"type": "string", "value": initcap_(text).str()})),
        "lower" => Ok(json!({"type": "string", "value": lower_(text).str()})),
        "md5" => Ok(json!({"type": "string", "value": md5_s(text).str()})),
        "split_pipe" => Ok(
            json!({"type": "array", "value": split2__(text, SqlString::from("|")).iter().map(|s| json!({"type":"string", "value":s.str()})).collect::<Vec<_>>()}),
        ),
        "trim_x" => {
            Ok(json!({"type":"string", "value":trim_both_s_s(SqlString::from("x"), text).str()}))
        }
        "concat_ws_null" => Ok(
            json!({"type":"string", "value":concat_ws_N_(SqlString::from("|"), None, text).str()}),
        ),
        "array_distinct" | "array_min" | "array_max" => {
            let values: Vec<Option<i64>> =
                serde_json::from_str(&request.value).map_err(|e| e.to_string())?;
            let values = std::sync::Arc::new(values);
            let encode = |value: Option<i64>| {
                value.map_or_else(
                    || json!({"type":"null"}),
                    |v| json!({"type":"integer", "value":v.to_string()}),
                )
            };
            Ok(match request.operation.as_str() {
                "array_distinct" => {
                    json!({"type":"array", "value":array_distinct(values).iter().copied().map(encode).collect::<Vec<_>>()})
                }
                "array_min" => encode(array_min_N(values)),
                _ => encode(array_max_N(values)),
            })
        }
        "extract_date_century"
        | "extract_date_isoyear"
        | "extract_date_week"
        | "extract_date_dow" => {
            let value = cast_to_Date_s(text).map_err(|e| e.to_string())?;
            let result = match request.operation.as_str() {
                "extract_date_century" => extract_century_Date(value),
                "extract_date_isoyear" => extract_isoyear_Date(value),
                "extract_date_week" => extract_week_Date(value),
                _ => extract_dow_Date(value),
            };
            Ok(json!({"type":"integer", "value":result.to_string()}))
        }
        "extract_timestamp_epoch" | "extract_timestamp_microsecond" => {
            let value = cast_to_Timestamp_s(text).map_err(|e| e.to_string())?;
            let result = if request.operation.ends_with("epoch") {
                extract_epoch_Timestamp(value)
            } else {
                extract_microsecond_Timestamp(value)
            };
            Ok(json!({"type":"integer", "value":result.to_string()}))
        }
        "like_suffix" => {
            Ok(json!({"type": "boolean", "value": like2__(text, SqlString::from("%_"))}))
        }
        "like_escape" => Ok(
            json!({"type": "boolean", "value": like3___(text, SqlString::from("a!%_"), SqlString::from("!"))}),
        ),
        "ilike_h" => Ok(json!({"type": "boolean", "value": ilike2__(text, SqlString::from("h%"))})),
        "round_decimal1" | "truncate_decimal1" => {
            let value = request
                .value
                .parse::<SqlDecimal<12, 4>>()
                .map_err(|e| e.to_string())?;
            let result = if request.operation == "round_decimal1" {
                round_SqlDecimal_i32(value, 1)
            } else {
                truncate_SqlDecimal_i32(value, 1)
            };
            Ok(json!({"type": "decimal", "value": result.to_string()}))
        }
        "round_float1" | "truncate_float1" => {
            let value =
                dbsp::algebra::F64::new(request.value.parse::<f64>().map_err(|e| e.to_string())?);
            let result = if request.operation == "round_float1" {
                round_d_i32(value, 1)
            } else {
                truncate_d_i32(value, 1)
            };
            Ok(json!({"type": "float", "value": format!("{:016x}", result.into_inner().to_bits())}))
        }
        "length" => Ok(json!({"type": "integer", "value": char_length_(text).to_string()})),
        "left2" => Ok(json!({"type": "string", "value": left_s_i32(text, 2).str()})),
        "right2" => Ok(json!({"type": "string", "value": right_s_i32(text, 2).str()})),
        "substring_0_3" => Ok(json!({"type": "string", "value": substring3___(text, 0, 3).str()})),
        "repeat2" => Ok(json!({"type": "string", "value": repeat__(text, 2).str()})),
        "position_e" => Ok(
            json!({"type": "integer", "value": position__(SqlString::from("é"), text).to_string()}),
        ),
        "replace_e" => Ok(
            json!({"type": "string", "value": replace___(text, SqlString::from("é"), SqlString::from("E")).str()}),
        ),
        "float_lt_zero" => {
            let value = request.value.parse::<f64>().map_err(|e| e.to_string())?;
            Ok(
                json!({"type": "boolean", "value": dbsp::algebra::F64::new(value) < dbsp::algebra::F64::new(0.0)}),
            )
        }
        _ => return Err(format!("Unknown reference operation {}", request.operation)),
    };
    result.map_err(|e| e.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: fold_reference INPUT.jsonl")?;
    let mut output = std::io::stdout().lock();
    for line in BufReader::new(File::open(path)?).lines() {
        let request: Request = serde_json::from_str(&line?)?;
        let response = match evaluate(&request) {
            Ok(value) => json!({"id": request.id, "status": "value", "value": value}),
            Err(reason) => json!({"id": request.id, "status": "runtime_error", "reason": reason}),
        };
        serde_json::to_writer(&mut output, &response)?;
        writeln!(output)?;
    }
    Ok(())
}
