//! Array folds preserve element order and NULL elements. Allocation checks count
//! nested values and string payloads before repetition or concatenation.

use crate::model::{Refusal, Result, Value, unsupported};

const MAX_BYTES: usize = 1024 * 1024;
const MAX_ELEMENTS: usize = 4096;

pub(crate) fn registered(name: &str) -> bool {
    matches!(
        name,
        "ARRAY_REPEAT"
            | "ARRAY_APPEND"
            | "ARRAY_PREPEND"
            | "ARRAY_CONCAT"
            | "ARRAY_REVERSE"
            | "ARRAY_DISTINCT"
            | "ARRAY_SIZE"
            | "ARRAY_LENGTH"
            | "CARDINALITY"
            | "ELEMENT"
            | "ARRAY_MIN"
            | "ARRAY_MAX"
            | "SORT_ARRAY"
            | "ARRAY_TO_STRING"
    )
}

fn bytes(value: &Value) -> Result<usize> {
    let size = value.retained_bytes()?;
    if size > MAX_BYTES {
        return Err(Refusal::Budget);
    }
    Ok(size)
}

fn total_bytes(values: &[Value]) -> Result<usize> {
    if values.len() > MAX_ELEMENTS {
        return Err(Refusal::Budget);
    }
    values.iter().try_fold(0usize, |sum, value| {
        let next = sum.checked_add(bytes(value)?).ok_or(Refusal::Budget)?;
        if next > MAX_BYTES {
            return Err(Refusal::Budget);
        }
        Ok(next)
    })
}

pub(crate) fn evaluate(name: &str, values: &[Value]) -> Result<Value> {
    total_bytes(values)?;
    match (name, values) {
        ("ARRAY_TO_STRING", [_, Value::Null] | [Value::Null, _])
        | ("ARRAY_TO_STRING", [_, _, Value::Null] | [_, Value::Null, _] | [Value::Null, _, _]) => {
            Ok(Value::Null)
        }
        ("ARRAY_TO_STRING", [Value::Array(array), Value::String(separator)]) => {
            join_strings(array, separator, None)
        }
        (
            "ARRAY_TO_STRING",
            [
                Value::Array(array),
                Value::String(separator),
                Value::String(null_text),
            ],
        ) => join_strings(array, separator, Some(null_text)),
        ("SORT_ARRAY", [Value::Array(array)]) => sort(array, true),
        ("SORT_ARRAY", [Value::Null, Value::Bool(_)]) => Ok(Value::Null),
        ("SORT_ARRAY", [Value::Array(array), Value::Bool(ascending)]) => sort(array, *ascending),
        ("ARRAY_REPEAT", [_, Value::Null]) => Ok(Value::Null),
        ("ARRAY_REPEAT", [element, Value::Int(count)]) => {
            let count = usize::try_from((*count).max(0)).map_err(|_| Refusal::Budget)?;
            if count > MAX_ELEMENTS
                || bytes(element)?
                    .checked_mul(count)
                    .is_none_or(|n| n > MAX_BYTES)
            {
                return Err(Refusal::Budget);
            }
            Ok(Value::Array(vec![element.clone(); count]))
        }
        ("ARRAY_APPEND" | "ARRAY_PREPEND", [Value::Null, _]) => Ok(Value::Null),
        ("ARRAY_APPEND" | "ARRAY_PREPEND", [Value::Array(array), element]) => {
            if array.len() >= MAX_ELEMENTS {
                return Err(Refusal::Budget);
            }
            let mut result = array.clone();
            if name == "ARRAY_APPEND" {
                result.push(element.clone());
            } else {
                result.insert(0, element.clone());
            }
            Ok(Value::Array(result))
        }
        ("ARRAY_CONCAT", arrays) if !arrays.is_empty() => {
            if arrays.contains(&Value::Null) {
                return Ok(Value::Null);
            }
            let mut count = 0;
            for array in arrays {
                let Value::Array(array) = array else {
                    return Err(unsupported("ARRAY_CONCAT type"));
                };
                count += array.len();
            }
            if count > MAX_ELEMENTS {
                return Err(Refusal::Budget);
            }
            let mut result = Vec::with_capacity(count);
            for array in arrays {
                if let Value::Array(array) = array {
                    result.extend_from_slice(array);
                }
            }
            Ok(Value::Array(result))
        }
        (_, [Value::Null]) => Ok(Value::Null),
        ("ARRAY_SIZE" | "ARRAY_LENGTH" | "CARDINALITY", [Value::Array(array)]) => {
            Ok(Value::Int(array.len() as i128))
        }
        ("ELEMENT", [Value::Array(array)]) => match array.as_slice() {
            [] => Ok(Value::Null),
            [element] => Ok(element.clone()),
            _ => Err(Refusal::RuntimeError(
                "ELEMENT requires at most one element",
            )),
        },
        ("ARRAY_REVERSE", [Value::Array(array)]) => {
            Ok(Value::Array(array.iter().rev().cloned().collect()))
        }
        ("ARRAY_DISTINCT", [Value::Array(array)]) => {
            // Bound the quadratic work of this deliberately simple prototype.
            if array.len() > 256 {
                return Err(Refusal::Budget);
            }
            let mut result = Vec::new();
            for value in array {
                let mut present = false;
                for previous in &result {
                    if crate::comparison::equal(previous, value)? {
                        present = true;
                        break;
                    }
                }
                if !present {
                    result.push(value.clone());
                }
            }
            Ok(Value::Array(result))
        }
        ("ARRAY_MIN" | "ARRAY_MAX", [Value::Array(array)]) => {
            let mut best = None;
            for value in array.iter().filter(|v| **v != Value::Null) {
                if let Some(previous) = best {
                    let order = crate::comparison::compare(value, previous)?;
                    if (name == "ARRAY_MIN" && order.is_lt())
                        || (name == "ARRAY_MAX" && order.is_gt())
                    {
                        best = Some(value);
                    }
                } else {
                    best = Some(value);
                }
            }
            Ok(best.cloned().unwrap_or(Value::Null))
        }
        _ => Err(unsupported(format!("array function {name} argument types"))),
    }
}

fn join_strings(array: &[Value], separator: &str, null_text: Option<&str>) -> Result<Value> {
    let mut parts = Vec::with_capacity(array.len());
    let mut size = 0usize;
    for value in array {
        let part = match value {
            Value::String(text) => text.as_str(),
            Value::Null => match null_text {
                Some(text) => text,
                None => continue,
            },
            _ => return Err(unsupported("ARRAY_TO_STRING element type")),
        };
        size = size
            .checked_add(part.len())
            .and_then(|n| n.checked_add(if parts.is_empty() { 0 } else { separator.len() }))
            .filter(|n| *n <= MAX_BYTES)
            .ok_or(Refusal::Budget)?;
        parts.push(part);
    }
    Ok(Value::String(parts.join(separator)))
}

fn sort(array: &[Value], ascending: bool) -> Result<Value> {
    // A fallible insertion sort keeps unsupported comparisons as refusals,
    // without panicking inside an infallible sort comparator. Bound its work.
    if array.len() > 256 {
        return Err(Refusal::Budget);
    }
    let mut result = array.to_vec();
    for value in &mut result {
        if let Value::Float(value) = value {
            *value = crate::numeric::canonical_float(*value);
        }
    }
    for index in 1..result.len() {
        let mut position = index;
        while position > 0 {
            let order = match (&result[position - 1], &result[position]) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                (Value::Null, _) => std::cmp::Ordering::Less,
                (_, Value::Null) => std::cmp::Ordering::Greater,
                (left, right) => crate::comparison::compare(left, right)?,
            };
            if (ascending && !order.is_gt()) || (!ascending && !order.is_lt()) {
                break;
            }
            result.swap(position - 1, position);
            position -= 1;
        }
    }
    Ok(Value::Array(result))
}
