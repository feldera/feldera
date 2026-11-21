//! Integer-to-binary casts through the actual nullable sqllib functions.

use feldera_sqllib::*;
use serde_json::{Value, json};

pub(super) fn evaluate(operation: &str, input: &str) -> Result<Value, String> {
    let parts: Vec<_> = operation.split('/').collect();
    let [kind, target, precision] = parts.as_slice() else {
        return Err("expected integer type, binary type and precision".into());
    };
    let precision = precision.parse::<i32>().map_err(|e| e.to_string())?;
    let fixed = match *target {
        "BINARY" => true,
        "VARBINARY" => false,
        _ => return Err("unknown binary target".into()),
    };
    macro_rules! cast {
        ($ty:ty, $function:ident) => {{
            let value = if input == "null" {
                None
            } else {
                Some(input.parse::<$ty>().map_err(|e| e.to_string())?)
            };
            $function(value, precision, fixed).map_err(|e| e.to_string())?
        }};
    }
    let result = match *kind {
        "TINYINT" => cast!(i8, cast_to_bytesN_i8N),
        "SMALLINT" => cast!(i16, cast_to_bytesN_i16N),
        "INTEGER" => cast!(i32, cast_to_bytesN_i32N),
        "BIGINT" => cast!(i64, cast_to_bytesN_i64N),
        "UTINYINT" => cast!(u8, cast_to_bytesN_u8N),
        "USMALLINT" => cast!(u16, cast_to_bytesN_u16N),
        "UINTEGER" => cast!(u32, cast_to_bytesN_u32N),
        "UBIGINT" => cast!(u64, cast_to_bytesN_u64N),
        _ => return Err("unknown integer source".into()),
    };
    Ok(result.map_or_else(
        || json!({"type":"null"}),
        |bytes| json!({"type":"binary", "value":hex::encode(bytes.as_slice())}),
    ))
}
