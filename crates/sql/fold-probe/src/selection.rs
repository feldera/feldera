//! Selection operations keep NULL policy separate from scalar ordering.

use crate::model::{Result, SqlType, Value, unsupported};

pub(crate) fn registered(kind: &str) -> bool {
    matches!(
        kind,
        "NULLIF" | "LEAST" | "GREATEST" | "LEAST_PG" | "GREATEST_PG"
    )
}

pub(crate) fn evaluate(kind: &str, values: &[Value], ty: &SqlType) -> Result<Value> {
    let selected = if kind == "NULLIF" {
        let [left, right] = values else {
            return Err(unsupported("NULLIF arity"));
        };
        if *left != Value::Null && *right != Value::Null && crate::comparison::equal(left, right)? {
            &Value::Null
        } else {
            left
        }
    } else {
        if values.is_empty() {
            return Err(unsupported("empty value selection"));
        }
        let ignore_nulls = matches!(kind, "LEAST_PG" | "GREATEST_PG");
        if !ignore_nulls && values.contains(&Value::Null) {
            return Ok(Value::Null);
        }
        let greatest = matches!(kind, "GREATEST" | "GREATEST_PG");
        let mut best = &Value::Null;
        for value in values.iter().filter(|v| **v != Value::Null) {
            if *best == Value::Null {
                best = value;
                continue;
            }
            let order = crate::comparison::compare(value, best)?;
            if (greatest && order.is_gt()) || (!greatest && order.is_lt()) {
                best = value;
            }
        }
        best
    };
    Ok(match selected {
        Value::Float(value) => crate::numeric::floating(*value, ty),
        value => value.clone(),
    })
}
