//! Calls the actual runtime calendar functions, independently of the prototype.

use feldera_sqllib::*;
use serde_json::{Value, json};

pub(super) fn evaluate(operation: &str, text: &str) -> Result<Value, String> {
    let parts: Vec<_> = operation.split('/').collect();
    let [kind, operation, unit] = parts.as_slice() else {
        return Err("malformed temporal rounding request".into());
    };
    macro_rules! dispatch {
        ($value:expr, $ty:ident, $method:ident, $op:ident; $($unit:ident),+) => {{
            let function: fn($ty) -> $ty = paste::paste! {
                match *unit {
                    $(stringify!($unit) => [<$op _ $unit _ $ty>],)+
                    _ => return Err("unknown temporal rounding unit".into()),
                }
            };
            // Catch the documented legal-range panic only. An unexpected
            // runtime panic is a broken oracle, not a successful error vector.
            let result = std::panic::catch_unwind(|| function($value)).map_err(|error| {
                let message = error.downcast_ref::<String>().map(String::as_str)
                    .or_else(|| error.downcast_ref::<&str>().copied());
                if message.is_some_and(|m|
                    (m.starts_with("Rounding") && m.contains("range")) ||
                    (m.starts_with("Cannot subtract interval '") && m.contains("from date '")) ||
                    (m.starts_with("Cannot add interval '") && m.contains("to date '"))) {
                    message.unwrap().to_owned()
                } else {
                    std::panic::resume_unwind(error)
                }
            })?;
            Ok(json!({"type":kind, "value":result.$method().to_string()}))
        }};
    }
    macro_rules! rounded {
        ($value:expr, $ty:ident, $method:ident) => {
            match *operation {
                "floor" => dispatch!($value, $ty, $method, floor;
                    millennium, century, decade, year, quarter, month, week, day,
                    hour, minute, second, millisecond, microsecond, nanosecond),
                "ceil" => dispatch!($value, $ty, $method, ceil;
                    millennium, century, decade, year, quarter, month, week, day,
                    hour, minute, second, millisecond, microsecond, nanosecond),
                "trunc" => dispatch!($value, $ty, $method, date_trunc;
                    millennium, century, decade, year, quarter, month, week, day),
                _ => Err("unknown temporal rounding operation".into()),
            }
        };
    }
    match *kind {
        "date" => {
            let value = cast_to_Date_s(SqlString::from(text)).map_err(|e| e.to_string())?;
            rounded!(value, Date, to_date)
        }
        "timestamp" => {
            let value = cast_to_Timestamp_s(SqlString::from(text)).map_err(|e| e.to_string())?;
            rounded!(value, Timestamp, to_naiveDateTime)
        }
        _ => Err("unknown temporal rounding type".into()),
    }
}
