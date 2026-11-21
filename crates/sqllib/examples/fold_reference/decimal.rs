//! Decimal arithmetic oracle using the generated program's sqllib entry points.

use feldera_sqllib::*;
use serde_json::{Value, json};

pub(super) fn evaluate(operation: &str, input: &str) -> Result<Value, String> {
    let values: [Option<String>; 2] = serde_json::from_str(input).map_err(|e| e.to_string())?;
    let [Some(left), Some(right)] = values else {
        return Ok(json!({"type":"null"}));
    };
    let left: SqlDecimal<12, 3> = left.parse().map_err(|e| format!("{e}"))?;
    let right: SqlDecimal<12, 3> = right.parse().map_err(|e| format!("{e}"))?;
    let result = std::panic::catch_unwind(|| -> SqlDecimal<20, 3> {
        match operation {
            "plus" => plus_SqlDecimal_SqlDecimal(left, right),
            "minus" => minus_SqlDecimal_SqlDecimal(left, right),
            "times" => times_SqlDecimal_SqlDecimal(left, right),
            "divide" => div_SqlDecimal_SqlDecimal(left, right),
            "mod" => modulo_SqlDecimal_SqlDecimal(left, right),
            _ => panic!("unknown decimal oracle operation {operation}"),
        }
    });
    match result {
        Ok(value) => Ok(json!({"type":"decimal", "value":value.to_string()})),
        Err(_) if matches!(operation, "divide" | "mod") && right == SqlDecimal::<12, 3>::ZERO => {
            Err("decimal division or modulo by zero".into())
        }
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
