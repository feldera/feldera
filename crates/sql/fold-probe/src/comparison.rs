//! Scalar comparisons use the runtime's ordering: NaN equals NaN and sorts last;
//! signed zero compares equal. NULL handling belongs to the SQL operator caller.

use crate::model::{Result, Value, unsupported};
use std::cmp::Ordering;

pub(crate) fn compare(left: &Value, right: &Value) -> Result<Ordering> {
    let ordering = match (left, right) {
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => a.partial_cmp(b).expect("non-NaN floats are comparable"),
        },
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
        (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::Months(a), Value::Months(b)) => a.cmp(b),
        (Value::Micros(a), Value::Micros(b)) => a.cmp(b),
        _ => return Err(unsupported("comparison operand types")),
    };
    Ok(ordering)
}

pub(crate) fn equal(a: &Value, b: &Value) -> Result<bool> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => Ok(a == b),
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !equal(a, b)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(compare(a, b)?.is_eq()),
    }
}
